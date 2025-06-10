package com.apecloud.dbtester.tester;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QdrantTesterHttp implements DatabaseTester {
    private final List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public QdrantTesterHttp(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        OkHttpClient client = new OkHttpClient();
        return new QdrantConnection(client, dbConfig);
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String operation) throws IOException {
        QdrantConnection qdrantConnection = (QdrantConnection) connection;
        String[] parts = operation.split(":", 3);
        String operationType = parts[0].toLowerCase();
        String collectionName = parts.length > 1 ? parts[1] : "test_collection";
        String params = parts.length > 2 ? parts[2] : "";

        try {
            switch (operationType) {
                case "check_health":
                    boolean health = qdrantConnection.checkHealth();
                    return new QdrantQueryResult("Health: " + health);
                case "create_collection":
                    int vectorSize = Integer.parseInt(params);
                    boolean created = qdrantConnection.createCollection(collectionName, vectorSize);
                    return new QdrantQueryResult("Collection Created: " + created);
                case "delete_collection":
                    boolean deleted = qdrantConnection.deleteCollection(collectionName);
                    return new QdrantQueryResult("Collection Deleted: " + deleted);
                case "check_collection":
                    boolean exists = qdrantConnection.checkCollectionExists(collectionName);
                    return new QdrantQueryResult("Collection Exists: " + exists);
                case "upsert":
                    // upsert:test_collection:1,1.2,3.4,{"name":"item"}
                    String[] pointParts = params.split(",", 2); // Split only once
                    if (pointParts.length == 0) {
                        throw new IOException("Invalid upsert format: missing point ID");
                    }

                    int pointId;
                    try {
                        pointId = Integer.parseInt(pointParts[0].trim()); // 提取点ID
                    } catch (NumberFormatException e) {
                        throw new IOException("Invalid point ID: " + pointParts[0], e);
                    }

                    List<Double> vector = new ArrayList<>();
                    Map<String, Object> payload = new HashMap<>();

                    if (pointParts.length > 1) {
                        String rest = pointParts[1];
                        int jsonStart = rest.indexOf('{');
                        if (jsonStart == -1) {
                            // 无 payload，仅解析向量
                            String[] vectorStr = rest.trim().replaceAll("[{}]", "").split(",");
                            for (String s : vectorStr) {
                                try {
                                    vector.add(Double.parseDouble(s.trim()));
                                } catch (NumberFormatException e) {
                                    throw new IOException("Invalid vector value: " + s, e);
                                }
                            }
                        } else {
                            // 存在 JSON payload
                            String vectorPart = rest.substring(0, jsonStart).trim();
                            String payloadJson = rest.substring(jsonStart).trim();

                            // 解析向量部分
                            if (!vectorPart.isEmpty()) {
                                String[] vectorStr = vectorPart.replaceAll("[{}]", "").split(",");
                                for (String s : vectorStr) {
                                    try {
                                        vector.add(Double.parseDouble(s.trim()));
                                    } catch (NumberFormatException e) {
                                        throw new IOException("Invalid vector value: " + s, e);
                                    }
                                }
                            }

                            // 解析 payload
                            try {
                                payload = qdrantConnection.parseJsonToMap(payloadJson);
                            } catch (IOException e) {
                                throw new IOException("Failed to parse payload JSON: " + payloadJson, e);
                            }
                        }
                    }

                    boolean inserted = qdrantConnection.addPoint(collectionName, pointId, vector, payload);
                    return new QdrantQueryResult("Point Inserted: " + inserted);
                case "search":
                    // search:test_collection:1.2,3.4,limit=10
                    String[] searchParts = params.split(",", 1);
                    List<Double> queryVector = new ArrayList<>();
                    String limitParam = searchParts.length > 1 ? searchParts[1] : "limit=10";

                    String[] vectorStr = searchParts[0].trim().split(",");
                    for (String s : vectorStr) {
                        if (s.contains("limit=")) {
                            continue;
                        }
                        queryVector.add(Double.parseDouble(s.trim()));
                    }

                    int limit = 10;
                    if (limitParam.contains("limit=")) {
                        limit = Integer.parseInt(limitParam.split("=")[1]);
                    }

                    QdrantSearchResponse response = qdrantConnection.searchPoints(collectionName, queryVector, limit);
                    return QdrantQueryResult.forSearch(response);

                default:
                    throw new IOException("Unsupported operation: " + operationType);
            }
        } catch (Exception e) {
            throw new IOException("Error executing operation: " + operation, e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, query);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            System.out.println("Terminated: " + terminated);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;

        result.append("Benchmark completed:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(duration).append(" seconds\n")
                .append("Queries per second: ").append(iterations / duration);

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        int successfulConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                execute(connection, "check_health");
                successfulConnections++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(duration * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        result.append("Connection stress test results:\n")
                .append("Attempted connections: ").append(connections).append("\n")
                .append("Successful connections: ").append(successfulConnections).append("\n")
                .append("Test duration: ").append(duration).append(" seconds");

        return result.toString();
    }


    @Override
    public void releaseConnections() {
        for (DatabaseConnection connection : connections) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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

            switch (testType) {
                case "query":
                    QueryResult result = execute(connection, dbConfig.getQuery());
                    ResultSet resultSet = result.getResultSet();
                    while (resultSet.next()) {
                        if (resultSet.getString("result") != null) {
                            results.append(resultSet.getString("result"));
                        }
                    }
                    results.append("\nBasic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
                    break;

                case "benchmark":
                    results.append(bench(connection, dbConfig.getQuery(), 1000, 10)).append("\n");
                    break;

                default:
                    results.append("Unknown test type\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
        StringBuilder result = new StringBuilder();
        QdrantQueryResult qdrantQueryResult;
        QueryResult executeResult;
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
        int genTestQuery = 0;
        String genTestValue;

        // Check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_collection";
        }

        System.out.println("Execution loop start: " + query);
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
                    connection = this.connect();
                }
                
                if (genTestQuery == 1) {
                    // Check if collection exists, if not create it
                    qdrantQueryResult = (QdrantQueryResult) execute(connection, "check_collection:" + table);
                    if (!qdrantQueryResult.getMessage().contains("true")) {
                        System.out.println("Collection " + table + " does not exist. Creating collection...");
                        execute(connection, "create_collection:" + table + ":4");
                        System.out.println("Collection " + table + " created successfully.");
                    } else {
                        System.out.println("Collection " + table + " already exists.");
                        if (table.equals("executions_loop_collection")) {
                            // Delete collection
                            System.out.println("Delete collection " + table);
                            execute(connection, "delete_collection:" + table);
                            System.out.println("Collection " + table + " deleted successfully.");

                            System.out.println("Create collection " + table);
                            execute(connection, "create_collection:" + table + ":4");
                            System.out.println("Collection " + table + " created successfully.");
                        }
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    // Set test query
                    genTestValue = "executions_loop_" + insertIndex;
                    query = String.format("upsert:" + table + ":1,1,1,1,1,{\"name\":\"%s\"}", genTestValue);
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                if (!executeResult.toString().equals("Failed to insert point")) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        java.util.Date recoveryDate = new java.util.Date(recoveryTime);
                        System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                        System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                        errorToRecoveryTime = recoveryTime - errorTime;
                        System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                        executionError = false;
                    }
                } else {
                    failedExecutions++;
                    insertIndex = insertIndex - 1;
                    executionError = true;
                }
            } catch (Exception e) {
                System.out.println("Execution loop failed: " + e.getMessage());
                failedExecutions++;
                insertIndex--;

                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections();

        result.append("Execution loop completed during ").append(duration).append(" seconds");

        return String.format("Total Executions: %d\n" +
                        "Successful Executions: %d\n" +
                        "Failed Executions: %d\n" +
                        "Disconnection Counts: %d",
                successfulExecutions + failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts);
    }

    private static class QdrantConnection implements DatabaseConnection {
        private final OkHttpClient client;
        private final DBConfig dbConfig;

        QdrantConnection(OkHttpClient client, DBConfig dbConfig) {
            this.client = client;
            this.dbConfig = dbConfig;
        }

        @Override
        public void close() throws IOException {
            // No need to close OkHttpClient explicitly
        }

        private static final ObjectMapper objectMapper = new ObjectMapper();

        private Map parseJsonToMap(String json) throws IOException {
            return objectMapper.readValue(json, Map.class);
        }

        /**
         * Check health
         */
        public boolean checkHealth() throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/health";
            Request request = new Request.Builder()
                    .url(url)
                    .build();

            Response response = this.client.newCall(request).execute();
            return response.isSuccessful();
        }

        /**
         * Create Qdrant Collection
         */
        public boolean createCollection(String collectionName, int vectorSize) throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/collections/" + collectionName;

            String jsonBody = String.format(
                    "{\"vectors\":"
                            + "{\"size\": %d," + "\"distance\": \"Dot\""
                            + "}}", vectorSize);

            Request request = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(jsonBody, MediaType.get("application/json")))
                    .build();

            Response response = this.client.newCall(request).execute();
            return response.isSuccessful();
        }

        /**
         * Delete Qdrant Collection
         */
        public boolean deleteCollection(String collectionName) throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/collections/" + collectionName;

            Request request = new Request.Builder()
                    .url(url)
                    .delete()
                    .build();

            Response response = this.client.newCall(request).execute();
            return response.isSuccessful();
        }

        /**
         * Check Collection Exists
         */
        public boolean checkCollectionExists(String collectionName) throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/collections/" + collectionName;

            Request request = new Request.Builder()
                    .url(url)
                    .get()
                    .build();

            Response response = client.newCall(request).execute();
            return response.isSuccessful();
        }

        /**
         * Add Point To Collection
         */
        public boolean addPoint(String collectionName, int id, List<Double> vector, Map<String, Object> payload) throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/collections/" + collectionName + "/points";

            ObjectMapper mapper = new ObjectMapper();
            String payloadJson = payload != null ? mapper.writeValueAsString(payload) : "{}";

            String vectorJson = vector.toString(); // [1.2, 3.4, ...]

            String jsonBody = String.format(
                    "{"
                            + "\"points\": ["
                            + "{"
                            + "\"id\": %d,"
                            + "\"vector\": %s,"
                            + "\"payload\": %s"
                            + "}"
                            + "]"
                            + "}", id, vectorJson, payloadJson);

            Request request = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(jsonBody, MediaType.get("application/json")))
                    .build();

            Response response = this.client.newCall(request).execute();
            return response.isSuccessful();
        }

        public QdrantSearchResponse searchPoints(String collectionName, List<Double> vector, int limit) throws IOException {
            String url = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/collections/" + collectionName + "/points/search";

            String jsonBody = String.format(
                    "{\"vector\":%s,\"top\":%d}",
                    vector.toString(), limit);


            Request request = new Request.Builder()
                    .url(url)
                    .post(RequestBody.create(jsonBody, MediaType.get("application/json")))
                    .build();

            Response response = client.newCall(request).execute();
            if (!response.isSuccessful()) throw new IOException("Search failed: " + response.code());

            return objectMapper.readValue(response.body().string(), QdrantSearchResponse.class);
        }
    }

    public static class QdrantSearchResponse {
        private List<Map<String, Object>> result;
        private String status;
        private String time;

        public List<Map<String, Object>> getResult() {
            return result;
        }

        public void setResult(List<Map<String, Object>> result) {
            this.result = result;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }
    }

    public static class QdrantQueryResult implements QueryResult {
        private final ResultSet response;
        private final String message;
        private final boolean success;
        private final List<String> documents;

        public QdrantQueryResult(String message) {
            this.message = message;
            this.success = !message.startsWith("Failed");
            this.response = null;
            this.documents = Collections.emptyList();
        }

        public QdrantQueryResult(boolean success, String message, List<String> documents) {
            this.message = message;
            this.success = success;
            this.documents = documents;
            this.response = null;
        }

        public static QdrantQueryResult forSearch(QdrantSearchResponse response) {
            boolean status = response.getStatus().equals("ok");
            List<Map<String, Object>> results = response.getResult();
            List<String> docs = new ArrayList<>();
            for (Map<String, Object> result : results) {
                docs.add(result.toString());
            }
            return new QdrantQueryResult(status, "Search Completed", docs);
        }

        @Override
        public ResultSet getResultSet() {
            return response;
        }

        @Override
        public int getUpdateCount() {
            return -1;
        }

        @Override
        public boolean hasResultSet() {
            return true;
        }

        public boolean isSuccessful() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public List<String> getDocuments() {
            return documents;
        }

        public String toString() {
            return message;
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException {
//      DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .testType("query")
//                .dbType("qdrant").query("create_collection:test_collection:128")
//                .port(6333)
//                .build();
//
//        DatabaseTester tester = TesterFactory.createTester(dbConfig);
//
//        DatabaseConnection connection = tester.connect();
//        QueryResult result = tester.execute(connection, "delete_collection:test_collection");
//        System.out.println(result);
//
//        result = tester.execute(connection, "create_collection:test_collection:4");
//        System.out.println(result);
//
//        result = tester.execute(connection, "check_collection:test_collection");
//        System.out.println(result);
//
//        result = tester.execute(connection, "upsert:executions_loop_collection:1,1,1,1,1,{\"name\":\"executions_loop_1\"}");
//        System.out.println(result);
//
//        result = tester.execute(connection, "search:executions_loop_collection:1,1,1,1,limit=10");
//        QdrantQueryResult qdrantResult = (QdrantQueryResult) result;
//        if (qdrantResult.getDocuments() != null) {
//            List<String> rs = qdrantResult.getDocuments();
//            for (String r : rs) {
//                System.out.println(r);
//            }
//        }
//        System.out.println(result);

//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .testType("connectionstress")
//                .port(6333)
//                .duration(10)
//                .connectionCount(10)
//                .dbType("qdrant")
//                .build();
//
//        QdrantTesterHttp tester = new QdrantTesterHttp(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();

        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(6333)
                .dbType("qdrant")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .build();
        QdrantTesterHttp tester = new QdrantTesterHttp(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }

}