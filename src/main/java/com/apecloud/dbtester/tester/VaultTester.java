package com.apecloud.dbtester.tester;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Vault 测试器，实现 DatabaseTester 接口
 * 支持操作: read, write, delete, list
 * 使用 Java 11 HttpClient，无需额外依赖
 */
public class VaultTester implements DatabaseTester {
    private final List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private HttpClient sharedHttpClient; // 共享的 HttpClient，支持连接池

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

        // 创建共享 HttpClient（若未创建）
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
        // 构建完整 URL
        String baseUrl = buildBaseUrl(dbConfig);
        String url = baseUrl + "/v1/" + path;

        // 根据操作构建请求
        HttpRequest request;
        switch (operation) {
            case "read":
                request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("X-Vault-Token", dbConfig.getPassword()) // 使用 password 字段存放 token
                        .header("Accept", "application/json")
                        .GET()
                        .build();
                break;
            case "write":
                // 解析键值对，构造 JSON 体
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
                // 判断是否为 KV v2 路径（包含 /data/）
                if (path.contains("/data/")) {
                    // KV v2 格式：{"data": {"key":"value"}}
                    jsonBody = "{\"data\":" + buildJsonBody(data) + "}";
                } else {
                    // KV v1 或其他：直接 {"key":"value"}
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
                // Vault list 操作使用 GET 并添加 list=true 参数
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

            // 根据操作类型构建 QueryResult
            if (operation.equals("read") || operation.equals("list")) {
                // 返回响应体作为结果集
                return new VaultQueryResult(Collections.singletonList(response.body()), 0);
            } else {
                // write, delete 返回更新计数 1
                return new VaultQueryResult(null, 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String command, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, command);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double qps = iterations / duration;

        result.append("Benchmark results:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(duration).append(" seconds\n")
                .append("Queries per second: ").append(String.format("%.2f", qps));

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        int successful = 0;
        int failed = 0;
        List<DatabaseConnection> tempConnections = new ArrayList<>();

        // 创建指定数量的连接对象
        for (int i = 0; i < connections; i++) {
            try {
                HttpClient independentClient = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .build();
                VaultConnection conn = new VaultConnection(independentClient, dbConfig);
                // 发送一个轻量请求以建立实际连接
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

        // 等待 duration 秒
        while (System.currentTimeMillis() < releaseTime) {
            try {
                Thread.sleep(100); // 避免忙等
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // 释放所有连接
        for (DatabaseConnection conn : tempConnections) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace(); // 可根据需要记录日志
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
        // 关闭共享 HttpClient
        if (sharedHttpClient != null) {
            // HttpClient 没有显式 close 方法，由 JVM 管理，但可以置空
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
            String testCommand = "write secret/data/hello foo=bar"; // 默认写操作

            switch (testType) {
                case "query":
                    execute(connection, testCommand);
                    results.append("Basic write test: SUCCESS\n");
                    // 再执行一次读
                    String readCmd = "read secret/data/hello";
                    QueryResult readResult = execute(connection, readCmd);
                    results.append("Basic read test: SUCCESS, data: ").append(readResult.getRawResults()).append("\n");
                    break;

                case "connectionstress":
                    results.append("Connection stress test:\n")
                            .append(connectionStress(10, 5))
                            .append("\n");
                    break;

                case "benchmark":
                    results.append("Benchmark test:\n")
                            .append(bench(connection, testCommand, 1000, 10))
                            .append("\n");
                    break;

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
        // 使用 database 作为引擎挂载点
        if (database == null || database.equals("")) {
            database = "executions_loop";
        }
        String mountPath = database;
        String baseUrl = buildBaseUrl(dbConfig);
        HttpClient httpClient = HttpClient.newHttpClient();
        // ---------- 重置引擎：删除并重新创建 ----------
        try {
            // 1. 尝试删除已有引擎（如果存在）
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
                // 继续尝试创建，可能权限不足等
            }

            // 2. 创建新的 KV v2 引擎
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

        // ---------- 持续写入循环（自动生成命令）----------
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
        int genTestQuery = 0; // 0: no need, 1: need generate, 2: generating, 3: generated

        // 判断是否需要自动生成测试命令
        if (query == null || query.isEmpty()) {
            genTestQuery = 1;
        }
        String baseValue = "";
        String baseKeyName = "";
        // 使用 table 作为密钥的基础名称
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
                    // 重新连接（获取新的连接实例，但共享 HttpClient）
                    connection = this.connect();
                }

                if (genTestQuery == 1) {
                    genTestQuery = 2;
                }

                // 自动生成测试命令（Vault 格式，使用 database 和 table）
                if ((genTestQuery == 2 && (query == null || query.isEmpty())) || genTestQuery == 3) {
                    // 生成命令：write <database>/data/<table>_<index> value=test_value_<index>
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
                // 对于 Vault，写/删返回 updateCount=1，读返回 0（但视为成功）
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
     * 构建基础 URL，格式：scheme://host:port
     */
    private String buildBaseUrl(DBConfig config) {
        String scheme = "http";
        return scheme + "://" + config.getHost() + ":" + config.getPort();
    }

    /**
     * 将 Map 转换为 JSON 字符串
     */
    private String buildJsonBody(Map<String, String> data) {
        // 简单 JSON 构建，不引入 JSON 库
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
     * 简单转义 JSON 字符串中的双引号
     */
    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Vault 连接实现，持有 HttpClient 和配置
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
            // HttpClient 由 VaultTester 管理，无需单独关闭
        }
    }

    /**
     * Vault 查询结果实现
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
            return null; // 不实现 JDBC ResultSet
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
     * 简单测试示例
     */
    public static void main(String[] args) throws IOException {
        // 示例1：执行持续循环测试（需在配置中设置 testType=executionloop 及其他参数）
//        DBConfig config = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(8200)
//                .password("***")
//                .dbType("vault")
//                .testType("executionloop")
////                .database("myengine")           // 引擎名称
////                .table("mysecret")               // 密钥基础名称
//                .duration(30)
//                .interval(1)
//                .build();
//
//        VaultTester tester = new VaultTester(config);
//        DatabaseConnection conn = tester.connect();
//        String result = tester.executionLoop(conn, null, config.getDuration(), config.getInterval(), config.getDatabase(), config.getTable());
//        System.out.println(result);

        // 示例2：执行一次读操作
//        DBConfig config = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(8200)
//                .password("***")  // 使用 password 字段存放 token
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
        
        // 示例3：执行创建连接测试（需在配置中设置 testType=connectionstress 及其他参数）
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
//        // ---------- 配置参数 ----------
//        String host = "127.0.0.1";
//        int port = 8200;
//        String token = "***";  // 请替换为有效 token
//        String mountPath = "test";                     // 引擎挂载路径
//        String secretPath = "test/data/test";          // 完整 secret 路径（KV v2 格式）
//
//        // ---------- 步骤1：创建 DBConfig 和 VaultTester ----------
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
//            // 步骤2：建立连接（您的原有方式）
//            conn = tester.connect();
//
//            // ---------- 步骤3：重置引擎（删除后重新创建）----------
//            HttpClient httpClient = HttpClient.newHttpClient();
//            String baseUrl = "http://" + host + ":" + port;
//
//            // 3.1 尝试删除已有引擎（如果存在）
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
//                    System.out.println("✅ 已删除原有引擎 '" + mountPath + "'");
//                } else if (deleteResponse.statusCode() == 404) {
//                    System.out.println("ℹ️ 引擎 '" + mountPath + "' 不存在，无需删除");
//                } else {
//                    System.err.println("⚠️ 删除引擎返回意外状态码 " + deleteResponse.statusCode() + "，将继续尝试创建");
//                }
//            } catch (Exception e) {
//                System.err.println("❌ 删除引擎请求异常: " + e.getMessage());
//                // 继续尝试创建
//            }
//
//            // 3.2 创建新的 KV v2 引擎
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
//                    System.out.println("✅ 引擎已成功挂载到 '" + mountPath + "'");
//                } else {
//                    System.err.println("❌ 启用引擎失败，HTTP " + enableResponse.statusCode() + ": " + enableResponse.body());
//                }
//            } catch (Exception e) {
//                System.err.println("❌ 启用引擎请求异常: " + e.getMessage());
//            }
//
//            // ---------- 步骤4：写入测试密钥（使用原生 API） ----------
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
//                    System.out.println("✅ 测试密钥写入成功");
//                } else {
//                    System.err.println("❌ 写入失败，HTTP " + writeResponse.statusCode() + ": " + writeResponse.body());
//                }
//            } catch (Exception e) {
//                System.err.println("❌ 写入请求异常: " + e.getMessage());
//            }
//
//            // ---------- 步骤5：使用您的 execute 方法读取密钥进行验证 ----------
//            QueryResult result = tester.execute(conn, "read " + secretPath);
//            if (result.hasResultSet()) {
//                System.out.println("🔍 读取结果: " + result.getRawResults());
//            } else {
//                System.out.println("ℹ️ 无结果集，更新计数: " + result.getUpdateCount());
//            }
//
//        } finally {
//            if (conn != null) conn.close();
//            tester.releaseConnections();
//        }
//    }

}