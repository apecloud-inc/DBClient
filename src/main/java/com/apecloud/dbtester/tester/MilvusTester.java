package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.sql.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.FloatVec;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;

public class MilvusTester implements DatabaseTester {
    private final List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private static final Gson gson = new Gson();

    public MilvusTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("http://" + dbConfig.getHost() + ":" + dbConfig.getPort())
                .build();

        MilvusClientV2 client = new MilvusClientV2(connectConfig);

        return new MilvusConnection(client, dbConfig);
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String operation) throws IOException {
        MilvusConnection milvusConnection = (MilvusConnection) connection;
        String[] parts = operation.split(":");
        String operationType = parts[0].toLowerCase();
        String collectionName = parts.length > 1 ? parts[1] : "";
        int vectorSize = parts.length > 2 ? Integer.parseInt(parts[2].trim()) : 4;
        String vectors = parts.length > 3 ? parts[3] : "";
        int pointId = parts.length > 4 ? Integer.parseInt(parts[4].trim()) : 0;
        String vectorName = parts.length > 5 ? parts[5] : "";

        try {
            switch (operationType) {
                case "check_health":
                    // check_health:test_collection"
                    boolean health = milvusConnection.checkHealth();
                    return new MilvusQueryResult("Health: " + health);
                case "create_collection":
                    // create_collection:test_collection:4
                    if ("".equals(collectionName)) {
                        throw new IOException("Invalid create_collection format: missing collection name");
                    }

                    if (vectorSize == 0) {
                        throw new IOException("Invalid create_collection format: missing dimension");
                    }
                    boolean created = milvusConnection.createCollection(collectionName, vectorSize);
                    return new MilvusQueryResult("Collection Created: " + created);
                case "delete_collection":
                    // delete_collection:test_collection
                    if ("".equals(collectionName)) {
                        throw new IOException("Invalid delete_collection format: missing collection name");
                    }
                    boolean deleted = milvusConnection.deleteCollection(collectionName);
                    return new MilvusQueryResult("Collection Deleted: " + deleted);
                case "check_collection":
                    // check_collection:test_collection"
                    if ("".equals(collectionName)) {
                        throw new IOException("Invalid check_collection format: missing collection name");
                    }
                    boolean exists = milvusConnection.checkCollectionExists(collectionName);
                    return new MilvusQueryResult("Collection Exists: " + exists);
                case "insert":
                    // "insert:test_collection:4:0.1,0.2,0.3,0.4:1:test"
                    if ("".equals(collectionName)) {
                        throw new IOException("Invalid insert format: missing collection name");
                    }

                    if (pointId == 0) {
                        throw new IOException("Invalid insert format: missing point ID");
                    }

                    if (vectorSize == 0 && "".equals(vectors)) {
                        throw new IOException("Invalid insert format: missing dimension or vectors");
                    }

                    if ("".equals(vectors) && vectorSize > 0) {
                        // generate random vectors
                        double[] randomVectors = VectorGenerator.generateRandomVector(vectorSize);
                        String randomVectorsStr = Arrays.toString(randomVectors);
                        vectors = randomVectorsStr.substring(1, randomVectorsStr.length() - 1);
                    }

                    List<Double> vector = new ArrayList<>();
                    String[] vectorStr = vectors.replaceAll("[{}]", "").split(",");
                    for (String s : vectorStr) {
                        try {
                            vector.add(Double.parseDouble(s.trim()));
                        } catch (NumberFormatException e) {
                            throw new IOException("Invalid vector value: " + s, e);
                        }
                    }

                    if ("".equals(vectorName)) {
                        throw new IOException("Invalid insert format: missing payload");
                    }

                    Map<String, Object> payload;
                    String payloadJson = "{\"name\":\"" + vectorName + "\"}";
                    try {
                        payload = milvusConnection.parseJsonToMap(payloadJson);
                    } catch (IOException e) {
                        throw new IOException("Failed to parse payload JSON: " + payloadJson, e);
                    }

                    boolean inserted = milvusConnection.insertData(collectionName, pointId, vector, payload);
                    return new MilvusQueryResult("Data Inserted: " + inserted);
                case "query":
                    // query:test_collection:4:0.1,0.2,0.3,0.4:10
                    if ("".equals(collectionName)) {
                        throw new IOException("Invalid query format: missing collection name");
                    }

                    List<Double> queryVector = new ArrayList<>();
                    if (vectorSize == 0 && "".equals(vectors)) {
                        throw new IOException("Invalid query format: missing dimension or vectors");
                    }

                    if ("".equals(vectors) && vectorSize > 0) {
                        // generate random vectors
                        double[] randomVectors = VectorGenerator.generateRandomVector(vectorSize);
                        String randomVectorsStr = Arrays.toString(randomVectors);
                        vectors = randomVectorsStr.substring(1, randomVectorsStr.length() - 1);
                    }

                    String[] allParts = vectors.split(",");
                    for (String part : allParts) {
                        queryVector.add(Double.parseDouble(part.trim()));
                    }

                    if (pointId == 0) {
                        throw new IOException("Invalid query format: missing limit");
                    }
                    int limit = pointId;

                    MilvusSearchResponse response = milvusConnection.queryPoints(collectionName, queryVector, limit);
                    return MilvusQueryResult.forSearch(response);
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
        MilvusQueryResult milvusQueryResult;
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

        int vectorSize = dbConfig.getSize();
        if (vectorSize == 0) {
            vectorSize = 10;
        }

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
                    milvusQueryResult = (MilvusQueryResult) execute(connection, "check_collection:" + table);
                    if (!milvusQueryResult.getMessage().contains("true")) {
                        System.out.println("Collection " + table + " does not exist. Creating collection...");
                        execute(connection, "create_collection:" + table + ":" + vectorSize);
                        System.out.println("Collection " + table + " created successfully.");
                    } else {
                        System.out.println("Collection " + table + " already exists.");
                        if (table.equals("executions_loop_collection")) {
                            // Delete collection
                            System.out.println("Delete collection " + table);
                            execute(connection, "delete_collection:" + table);
                            System.out.println("Collection " + table + " deleted successfully.");

                            System.out.println("Create collection " + table);
                            execute(connection, "create_collection:" + table + ":" + vectorSize);
                            System.out.println("Collection " + table + " created successfully.");
                        }
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    // Set test query
                    genTestValue = "executions_loop_" + insertIndex;
                    query = String.format("insert:" + table + ":" + vectorSize + "::" + insertIndex + ":%s", genTestValue);
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                if (!executeResult.toString().equals("Failed to insert data")) {
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

    private static class MilvusConnection implements DatabaseConnection {
        private final MilvusClientV2 client;
        private final DBConfig dbConfig;

        MilvusConnection(MilvusClientV2 client, DBConfig dbConfig) {
            this.client = client;
            this.dbConfig = dbConfig;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }

        private static final ObjectMapper objectMapper = new ObjectMapper();

        private Map parseJsonToMap(String json) throws IOException {
            return objectMapper.readValue(json, Map.class);
        }

        public boolean checkHealth() {
            try {
                client.listCollections();
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        public boolean createCollection(String collectionName, int vectorSize) throws IOException {
            try {
                // 构建创建集合请求
                CreateCollectionReq createReq = CreateCollectionReq.builder()
                        .collectionName(collectionName)
                        .dimension(vectorSize)
                        .build();

                client.createCollection(createReq);
                return true;
            } catch (Exception e) {
                throw new IOException("Failed to create collection: ", e);
            }
        }

        public boolean deleteCollection(String collectionName)  throws IOException {
            try {
                DropCollectionReq dropReq = DropCollectionReq.builder()
                        .collectionName(collectionName)
                        .build();

                client.dropCollection(dropReq);
                return true;
            } catch (Exception e) {
                throw new IOException("Failed to delete collection: ", e);
            }
        }

        public boolean checkCollectionExists(String collectionName) throws IOException {
            try {
                HasCollectionReq hasReq = HasCollectionReq.builder()
                        .collectionName(collectionName)
                        .build();

                return client.hasCollection(hasReq);
            } catch (Exception e) {
                throw new IOException("Failed to check collection existence: ", e);
            }
        }

        public boolean insertData(String collectionName, int id, List<Double> vector, Map<String, Object> payload) throws IOException {
            try {
                // 转换Double向量为Float
                List<Float> floatVector = new ArrayList<>();
                for (Double d : vector) {
                    floatVector.add(d.floatValue());
                }

                ObjectMapper mapper = new ObjectMapper();
                String payloadJson = payload != null ? mapper.writeValueAsString(payload) : "{}";

                JsonObject jsonObjectVector = new JsonObject();
                jsonObjectVector.add("vector", gson.toJsonTree(floatVector));
                jsonObjectVector.addProperty("id", id);
                jsonObjectVector.addProperty("payload", payloadJson);

                // 构建插入请求
                InsertReq insertReq = InsertReq.builder()
                        .collectionName(collectionName)
                        .data(Collections.singletonList(jsonObjectVector))
                        .build();

                client.insert(insertReq);
                return true;
            } catch (Exception e) {
                throw new IOException("Failed to insert data into collection: ", e);
            }
        }

        public MilvusSearchResponse queryPoints(String collectionName, List<Double> queryVector, int limit) throws IOException {
            try {
                // 转换查询向量
                List<Float> floatVector = new ArrayList<>();
                for (Double d : queryVector) {
                    floatVector.add(d.floatValue());
                }

                SearchReq searchReq = SearchReq.builder()
                        .collectionName(collectionName)
                        .data(Collections.singletonList(new FloatVec(floatVector)))
                        .topK(limit)
                        .metricType(IndexParam.MetricType.COSINE)
                        .build();

                // 执行搜索
                SearchResp response = client.search(searchReq);
                MilvusSearchResponse result = new MilvusSearchResponse();

                // 处理搜索结果
                List<Map<String, Object>> docs = new ArrayList<>();
                for (List<SearchResp.SearchResult> results : response.getSearchResults()) {
                    for (SearchResp.SearchResult res : results) {
                        Map<String, Object> doc = new HashMap<>();
                        doc.put("id", res.getId());
                        doc.put("score", res.getScore());
                        docs.add(doc);
                    }
                }

                result.setResult(docs);
                result.setStatus("ok");
                return result;
            } catch (Exception e) {
                throw new IOException("Failed to query points from collection: ", e);
            }
        }
    }

    public static class MilvusSearchResponse {
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

    public static class MilvusQueryResult implements QueryResult {
        private final ResultSet response;
        private final String message;
        private final boolean success;
        private final List<String> documents;

        public MilvusQueryResult(String message) {
            this.message = message;
            this.success = !message.startsWith("Failed");
            this.response = null;
            this.documents = Collections.emptyList();
        }

        public MilvusQueryResult(boolean success, String message, List<String> documents) {
            this.message = message;
            this.success = success;
            this.documents = documents;
            this.response = null;
        }

        public static MilvusQueryResult forSearch(MilvusSearchResponse response) {
            boolean status = response.getStatus().equals("ok");
            List<Map<String, Object>> results = response.getResult();
            List<String> docs = new ArrayList<>();
            for (Map<String, Object> result : results) {
                docs.add(result.toString());
            }
            return new MilvusQueryResult(status, "Search Completed", docs);
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

    public static void main(String[] args) throws IOException {
//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .testType("query")
//                .dbType("milvus")
//                .query("create_collection:test_collection:4")
//                .port(19530)
//                .build();

//        MilvusTester tester = new MilvusTester(dbConfig);
//        DatabaseConnection connection = tester.connect();

        // 测试全流程
//        QueryResult result = tester.execute(connection, "create_collection:test_collection:20");
//        System.out.println(result);
//
//        result = tester.execute(connection, "check_collection:test_collection");
//        System.out.println(result);
//
//        result = tester.execute(connection, "insert:test_collection:20::1:test");
//        System.out.println(result);
//
//        result = tester.execute(connection, "query:test_collection:20::10");
//        System.out.println(result);
//
//        if (result instanceof MilvusQueryResult) {
//            MilvusQueryResult milvusResult = (MilvusQueryResult) result;
//            if (milvusResult.getDocuments() != null) {
//                for (String doc : milvusResult.getDocuments()) {
//                    System.out.println(doc);
//                }
//            }
//        }
//
//        result = tester.execute(connection, "delete_collection:test_collection");
//        System.out.println(result);


//        dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .testType("connectionstress")
//                .port(19530)
//                .duration(10)
//                .connectionCount(60)
//                .dbType("milvus")
//                .build();
//
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);

        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(19530)
                .dbType("milvus")
                .duration(10)
                .interval(1)
                .size(4)
                .testType("executionloop")
                .build();

        MilvusTester tester = new MilvusTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result1 = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result1);

        QueryResult result = tester.execute(connection, "query:executions_loop_collection:4::1000");
        System.out.println(result);

        if (result instanceof MilvusQueryResult) {
            MilvusQueryResult milvusResult = (MilvusQueryResult) result;
            if (milvusResult.getDocuments() != null) {
                for (String doc : milvusResult.getDocuments()) {
                    System.out.println(doc);
                }
            }
        }

        connection.close();
    }
}



