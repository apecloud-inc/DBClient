package com.apecloud.dbtester.tester;
import com.apecloud.dbtester.commons.BenchmarkUtils;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * Vault tester implementing the DatabaseTester interface.
 * Supported operations: read, write, delete, list.
 * Uses Java 11 HttpClient; no extra dependencies required.
 */
public class VaultTester implements DatabaseTester {
    private final List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private HttpClient sharedHttpClient; // Shared HttpClient with connection pooling.

    public VaultTester() {
        this.dbConfig = null;
    }

    public VaultTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        // Create the shared HttpClient if it does not already exist.
        if (sharedHttpClient == null) {
            sharedHttpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
        }

        VaultConnection connection = new VaultConnection(sharedHttpClient, dbConfig);
        connections.add(connection);
        return connection;
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        VaultConnection vaultConn = (VaultConnection) connection;
        String[] parts = command.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IOException("Invalid command format. Expected: <operation> <path> [key=value ...]");
        }

        String operation = parts[0].toLowerCase();
        String path = parts[1];
        // Build the full URL.
        String baseUrl = buildBaseUrl(dbConfig);
        String url = baseUrl + "/v1/" + path;

        // Build the request based on the operation.
        HttpRequest request;
        switch (operation) {
            case "read":
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("X-Vault-Token", dbConfig.getPassword()) // Use the password field to store the token.
                        .header("Accept", "application/json")
                        .GET()
                        .build();
                break;
            case "write":
                // Parse key=value pairs and construct the JSON body.
                Map<String, String> data = new HashMap<>();
                for (int i = 2; i < parts.length; i++) {
                    String[] kv = parts[i].split("=", 2);
                    if (kv.length == 2) {
                        data.put(kv[0], kv[1]);
                    } else {
                        throw new IOException("Invalid key=value pair: " + parts[i]);
                    }
                }
                String jsonBody;
                // Determine whether this is a KV v2 path (contains /data/).
                if (path.contains("/data/")) {
                    // KV v2 format: {"data": {"key":"value"}}.
                    jsonBody = "{\"data\":" + buildJsonBody(data) + "}";
                } else {
                    // KV v1 or others: directly {"key":"value"}.
                    jsonBody = buildJsonBody(data);
                }
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("X-Vault-Token", dbConfig.getPassword())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .build();
                break;
            case "delete":
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("X-Vault-Token", dbConfig.getPassword())
                        .DELETE()
                        .build();
                break;
            case "list":
                // Vault list uses GET with the list=true parameter.
                String listUrl = url + "?list=true";
                request = HttpRequest.newBuilder()
                        .uri(URI.create(listUrl))
                        .header("X-Vault-Token", dbConfig.getPassword())
                        .header("Accept", "application/json")
                        .GET()
                        .build();
                break;
            default:
                throw new IOException("Unsupported operation: " + operation);
        }

        try {
            HttpResponse<String> response = vaultConn.getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            if (statusCode < 200 || statusCode >= 300) {
                throw new IOException("Vault request failed with status " + statusCode + ": " + response.body());
            }

            // Build the QueryResult based on the operation type.
            if (operation.equals("read") || operation.equals("list")) {
                // Return the response body as the result set.
                return new VaultQueryResult(Collections.singletonList(response.body()), 0);
            } else {
                // write and delete return update count 1.
                return new VaultQueryResult(null, 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }
    }

    @Override
        public String bench(DatabaseConnection connection, String command, int iterations, int concurrency) {
        return BenchmarkUtils.run(iterations, concurrency, connection, c -> execute(c, command));
    }


    @Override
    public String connectionStress(int connections, int duration) {
        int successful = 0;
        int failed = 0;
        List<DatabaseConnection> tempConnections = new ArrayList<>();

        // Create the specified number of connection objects.
        for (int i = 0; i < connections; i++) {
            try {
                HttpClient independentClient = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .build();
                VaultConnection conn = new VaultConnection(independentClient, dbConfig);
                // Send a lightweight request to establish the actual connection.
                HttpRequest healthReq = HttpRequest.newBuilder()
                        .uri(URI.create(buildBaseUrl(dbConfig) + "/v1/sys/health"))
                        .header("X-Vault-Token", dbConfig.getPassword())
                        .GET()
                        .build();
                independentClient.send(healthReq, HttpResponse.BodyHandlers.discarding());
                tempConnections.add(conn);
                successful++;
            } catch (Exception e) {
                failed++;
            }
        }

        long createEnd = System.currentTimeMillis();
        long releaseTime = createEnd + duration * 1000L;

        // Wait for the configured duration in seconds.
        while (System.currentTimeMillis() < releaseTime) {
            try {
                Thread.sleep(100); // Avoid busy-waiting.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Release all connections.
        for (DatabaseConnection conn : tempConnections) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace(); // Can be replaced with logging if needed.
            }
        }
        tempConnections.clear();

        return String.format("Connection stress test results:\n" +
                        "Requested connections: %d\n" +
                        "Successful creations: %d\n" +
                        "Failed creations: %d\n" +
                        "Held for: %d seconds",
                connections, successful, failed, duration);
    }

    @Override
    public void releaseConnections() {
        // Clean up the shared HttpClient reference.
        if (sharedHttpClient != null) {
            // HttpClient has no explicit close method; managed by the JVM, but clear the reference.
            sharedHttpClient = null;
        }
        connections.clear();
    }

    @Override
    public String executeTest() throws IOException {
        DatabaseConnection connection = null;
        StringBuilder results = new StringBuilder();
        String testType = dbConfig.getTestType();

        try {
            connection = connect();
            String testCommand = "write secret/data/hello foo=bar"; // Default write operation.

            switch (testType) {
                case "query":
                    execute(connection, testCommand);
                    results.append("Basic write test: SUCCESS\n");
                    // Then perform a read.
                    String readCmd = "read secret/data/hello";
                    QueryResult readResult = execute(connection, readCmd);
                    results.append("Basic read test: SUCCESS, data: ").append(readResult.getRawResults()).append("\n");
                    break;

                case "connectionstress":
                    results.append("Connection stress test:\n")
                            .append(connectionStress(10, 5))
                            .append("\n");
                    break;

                case "benchmark": {
                    String benchQuery = (dbConfig.getQuery() != null && !dbConfig.getQuery().isEmpty()) ? dbConfig.getQuery() : testCommand;
                    results.append("Benchmark test:\n")
                           .append(bench(connection, benchQuery, dbConfig.getIterations(), dbConfig.getConcurrency()))
                           .append("\n");
                    break;
                }

                default:
                    results.append("Unknown test type\n");
            }
        } catch (Exception e) {
            results.append("Test failed: ").append(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
            releaseConnections();
        }

        return results.toString();
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        // Use the database value as the secret engine mount path.
        if (database == null || database.equals("")) {
            database = "executions_loop";
        }
        String mountPath = database;
        String baseUrl = buildBaseUrl(dbConfig);
        HttpClient httpClient = HttpClient.newHttpClient();
        // ---------- Reset the engine: delete and recreate ----------.
        try {
            // 1. Try to delete the existing engine if it exists.
            String deleteUrl = baseUrl + "/v1/sys/mounts/" + mountPath;
            HttpRequest deleteRequest = HttpRequest.newBuilder()
                    .uri(URI.create(deleteUrl))
                    .header("X-Vault-Token", dbConfig.getPassword())
                    .DELETE()
                    .build();
            HttpResponse<String> deleteResponse = httpClient.send(deleteRequest, HttpResponse.BodyHandlers.ofString());
            if (deleteResponse.statusCode() == 204) {
                System.out.println("Existing engine at '" + mountPath + "' deleted.");
            } else if (deleteResponse.statusCode() == 404) {
                System.out.println("No existing engine at '" + mountPath + "', will create new.");
            } else {
                System.err.println("Unexpected response when deleting engine: " + deleteResponse.statusCode());
                // Continue trying to create; may be due to insufficient permissions etc.
            }

            // 2. Create a new KV v2 engine.
            String enableUrl = baseUrl + "/v1/sys/mounts/" + mountPath;
            String enableBody = "{\"type\":\"kv-v2\"}";
            HttpRequest enableRequest = HttpRequest.newBuilder()
                    .uri(URI.create(enableUrl))
                    .header("X-Vault-Token", dbConfig.getPassword())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(enableBody))
                    .build();
            HttpResponse<String> enableResponse = httpClient.send(enableRequest, HttpResponse.BodyHandlers.ofString());
            if (enableResponse.statusCode() == 204) {
                System.out.println("KV v2 engine successfully mounted at '" + mountPath + "'.");
            } else {
                throw new IOException("Failed to mount KV v2 engine at '" + mountPath + "', status: " +
                        enableResponse.statusCode() + ", body: " + enableResponse.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Engine reset interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to reset Vault engine", e);
        }

        // ---------- Continuous write loop (auto-generate commands) ----------.
        StringBuilder result = new StringBuilder();
        QueryResult executeResult;
        int executeUpdateCount;
        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000L;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0; // 0: no need, 1: need to generate, 2: generating, 3: generated.

        // Determine whether a test command needs to be generated.
        if (query == null || query.isEmpty()) {
            genTestQuery = 1;
        }
        String baseValue = "";
        String baseKeyName = "";
        // Use table as the base name for secrets.
        if (table == null || table.equals("")) {
            baseKeyName = "executions_loop_key";
            baseValue = "executions_loop_value";
        } else {
            baseKeyName = table + "_key";
            baseValue = table + "_value";
        }

        System.out.println("Execution loop start: " + (query != null ? query : "auto-generate Vault writes to " + mountPath));
        while (System.currentTimeMillis() < endTime) {
            insertIndex++;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastOutputTime >= interval * 1000L) {
                outputPassTime += interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions)
                        + " successful: " + successfulExecutions + " failed: " + failedExecutions
                        + " disconnect: " + disconnectCounts);
            }

            try {
                if (executionError) {
                    Thread.sleep(1000);
                    // Reconnect (obtain a new connection instance while sharing the HttpClient).
                    connection = this.connect();
                }

                if (genTestQuery == 1) {
                    genTestQuery = 2;
                }

                // Auto-generate a test command in Vault format using database and table.
                if ((genTestQuery == 2 && (query == null || query.isEmpty())) || genTestQuery == 3) {
                    // Generated command: write <database>/data/<table>_<index> value=test_value_<index>.
                    String secretKey = baseKeyName + "_" + insertIndex;
                    String secretValue = baseValue + "_" + insertIndex;
                    query = "write " + mountPath + "/data/" + secretKey + " value=" + secretValue;
                    if (genTestQuery == 2) {
                        System.out.println("Generated test command: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                // For Vault, write/delete return updateCount=1; read returns 0 but is treated as success.
                if (executeUpdateCount != -1) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        Date recoveryDate = new Date(recoveryTime);
                        System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                        System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                        errorToRecoveryTime = recoveryTime - errorTime;
                        System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                        executionError = false;
                    }
                } else {
                    failedExecutions++;
                    insertIndex--;
                    executionError = true;
                }
            } catch (IOException e) {
                failedExecutions++;
                insertIndex--;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred: " + e.getMessage());
                    executionError = true;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections();

        result.append("Execution loop completed during ").append(duration).append(" seconds\n");
        return String.format("Total Executions: %d\n" +
                        "Successful Executions: %d\n" +
                        "Failed Executions: %d\n" +
                        "Disconnection Counts: %d",
                successfulExecutions + failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts);
    }

    /**
     * Build the base URL in the format scheme://host:port.
     */
    private String buildBaseUrl(DBConfig config) {
        String scheme = "http";
        return scheme + "://" + config.getHost() + ":" + config.getPort();
    }

    /**
     * Convert the map to a JSON string.
     */
    private String buildJsonBody(Map<String, String> data) {
        // Simple JSON construction without introducing a JSON library.
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
                    .append(escapeJson(entry.getValue())).append("\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Escape double quotes in the JSON string.
     */
    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Vault connection implementation holding the HttpClient and configuration.
     */
    private static class VaultConnection implements DatabaseConnection {
        private final HttpClient httpClient;
        private final DBConfig config;

        VaultConnection(HttpClient httpClient, DBConfig config) {
            this.httpClient = httpClient;
            this.config = config;
        }

        HttpClient getHttpClient() {
            return httpClient;
        }

        @Override
        public void close() throws IOException {
            // The HttpClient is managed by VaultTester; no need to close it here.
        }
    }

    /**
     * Vault query result implementation.
     */
    private static class VaultQueryResult implements QueryResult {
        private final List<String> results;
        private final int updateCount;

        VaultQueryResult(List<String> results, int updateCount) {
            this.results = results;
            this.updateCount = updateCount;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // JDBC ResultSet is not implemented.
        }

        @Override
        public List<String> getRawResults() {
            return results;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return results != null && !results.isEmpty();
        }
    }

    /**
     * Simple usage examples.
     */
    public static void main(String[] args) throws IOException {
        // Example 1: run a continuous loop test (set testType=executionloop and other parameters).
//        DBConfig config = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(8200)
//                .password("***")
//                .dbType("vault")
//                .testType("executionloop")
////                .database("myengine")           // Engine mount path.
////                .table("mysecret")               // Base secret name.
//                .duration(30)
//                .interval(1)
//                .build();
//
//        VaultTester tester = new VaultTester(config);
//        DatabaseConnection conn = tester.connect();
//        String result = tester.executionLoop(conn, null, config.getDuration(), config.getInterval(), config.getDatabase(), config.getTable());
//        System.out.println(result);

        // Example 2: perform a single read operation.
//        DBConfig config = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(8200)
//                .password("***")  // Use the password field to store the token.
//                .dbType("vault")
//                .testType("query")
//                .query("read executions_loop/data/executions_loop_key_1")
//                .build();
//
//        VaultTester tester = new VaultTester(config);
//        DatabaseConnection conn = null;
//        try {
//            conn = tester.connect();
//            QueryResult result = tester.execute(conn, "list executions_loop/metadata");
//            if (result.hasResultSet()) {
//                System.out.println("Read result: " + result.getRawResults());
//            } else {
//                System.out.println("Update count: " + result.getUpdateCount());
//            }
//        } finally {
//            if (conn != null) conn.close();
//            tester.releaseConnections();
//        }
        
        // Example 3: run a connection stress test (set testType=connectionstress and other parameters).
        DBConfig config = new DBConfig.Builder()
                .host("127.0.0.1")
                .port(8200)
                .password("***")
                .dbType("vault")
                .testType("connectionstress")
                .duration(30)
                .connectionCount(100)
                .build();

        VaultTester tester = new VaultTester(config);
        String result = tester.connectionStress(config.getConnectionCount(), config.getDuration());
        System.out.println(result);
    }

//    public static void main(String[] args) throws IOException {
//        // ---------- Configuration parameters ----------.
//        String host = "127.0.0.1";
//        int port = 8200;
//        String token = "***";  // Replace with a valid token.
//        String mountPath = "test";                     // Engine mount path.
//        String secretPath = "test/data/test";          // Full secret path (KV v2 format).
//
//        // ---------- Step 1: create DBConfig and VaultTester ----------.
//        DBConfig config = new DBConfig.Builder()
//                .host(host)
//                .port(port)
//                .password(token)
//                .dbType("vault")
//                .testType("query")
//                .query("read " + secretPath)
//                .build();
//
//        VaultTester tester = new VaultTester(config);
//        DatabaseConnection conn = null;
//
//        try {
//            // Step 2: establish the connection (your original approach).
//            conn = tester.connect();
//
//            // ---------- Step 3: reset the engine (delete then recreate) ----------.
//            HttpClient httpClient = HttpClient.newHttpClient();
//            String baseUrl = "http://" + host + ":" + port;
//
//            // 3.1 Try to delete the existing engine if it exists.
//            String deleteUrl = baseUrl + "/v1/sys/mounts/" + mountPath;
//            HttpRequest deleteRequest = HttpRequest.newBuilder()
//                    .uri(URI.create(deleteUrl))
//                    .header("X-Vault-Token", token)
//                    .DELETE()
//                    .build();
//
//            try {
//                HttpResponse<String> deleteResponse = httpClient.send(deleteRequest, HttpResponse.BodyHandlers.ofString());
//                if (deleteResponse.statusCode() == 204) {
//                    System.out.println("✅ Existing engine at '" + mountPath + "' deleted.");
//                } else if (deleteResponse.statusCode() == 404) {
//                    System.out.println("ℹ️ Engine '" + mountPath + "' does not exist; no need to delete.");
//                } else {
//                    System.err.println("⚠️ Unexpected status code while deleting engine: " + deleteResponse.statusCode() + ". Will continue trying to create.");
//                }
//            } catch (Exception e) {
//                System.err.println("❌ Engine deletion request failed: " + e.getMessage());
//                // Continue trying to create.
//            }
//
//            // 3.2 Create a new KV v2 engine.
//            String enableUrl = baseUrl + "/v1/sys/mounts/" + mountPath;
//            String enableBody = "{\"type\":\"kv-v2\"}";
//            HttpRequest enableRequest = HttpRequest.newBuilder()
//                    .uri(URI.create(enableUrl))
//                    .header("X-Vault-Token", token)
//                    .header("Content-Type", "application/json")
//                    .POST(HttpRequest.BodyPublishers.ofString(enableBody))
//                    .build();
//
//            try {
//                HttpResponse<String> enableResponse = httpClient.send(enableRequest, HttpResponse.BodyHandlers.ofString());
//                if (enableResponse.statusCode() == 204) {
//                    System.out.println("✅ Engine successfully mounted at '" + mountPath + "'.");
//                } else {
//                    System.err.println("❌ Failed to enable engine, HTTP " + enableResponse.statusCode() + ": " + enableResponse.body());
//                }
//            } catch (Exception e) {
//                System.err.println("❌ Engine enable request failed: " + e.getMessage());
//            }
//
//            // ---------- Step 4: write a test secret using the native API ----------.
//            String writeUrl = baseUrl + "/v1/" + secretPath;
//            String writeBody = "{\"data\": {\"foo\":\"bar\", \"baz\":\"qux\"}}";
//            HttpRequest writeRequest = HttpRequest.newBuilder()
//                    .uri(URI.create(writeUrl))
//                    .header("X-Vault-Token", token)
//                    .header("Content-Type", "application/json")
//                    .POST(HttpRequest.BodyPublishers.ofString(writeBody))
//                    .build();
//
//            try {
//                HttpResponse<String> writeResponse = httpClient.send(writeRequest, HttpResponse.BodyHandlers.ofString());
//                if (writeResponse.statusCode() == 200 || writeResponse.statusCode() == 204) {
//                    System.out.println("✅ Test secret written successfully.");
//                } else {
//                    System.err.println("❌ Write failed, HTTP " + writeResponse.statusCode() + ": " + writeResponse.body());
//                }
//            } catch (Exception e) {
//                System.err.println("❌ Write request failed: " + e.getMessage());
//            }
//
//            // ---------- Step 5: read the secret using your execute method for verification ----------.
//            QueryResult result = tester.execute(conn, "read " + secretPath);
//            if (result.hasResultSet()) {
//                System.out.println("🔍 Read result: " + result.getRawResults());
//            } else {
//                System.out.println("ℹ️ No result set; update count: " + result.getUpdateCount());
//            }
//
//        } finally {
//            if (conn != null) conn.close();
//            tester.releaseConnections();
//        }
//    }

}