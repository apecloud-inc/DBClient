package com.apecloud.dbtester.tester;

import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VictoriaMetricsTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;
    private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    public VictoriaMetricsTester() {
        this.dbConfig = null;
    }

    public VictoriaMetricsTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IOException("Database configuration is not set");
        }

        try {
            String hostname = dbConfig.getHost();
            int port = dbConfig.getPort();
            if (hostname == null || port == 0) {
                throw new IOException("Hostname and port are required");
            }

            // VictoriaMetrics URL
            String baseUrl = "http://" + hostname + ":" + port;
            
            // 创建 OkHttpClient
            OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();

            return new VMConnection(client, baseUrl);
        } catch (Exception e) {
            throw new IOException("Failed to connect to VictoriaMetrics: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        VMConnection vmConnection = (VMConnection) connection;
        OkHttpClient client = vmConnection.client;
        String baseUrl = vmConnection.baseUrl;
        
        try {
            String[] parts = command.split(" ", 2);
            if (parts.length < 2) {
                throw new IOException("Invalid command format");
            }

            String operation = parts[0].toUpperCase();
            String content = parts[1];

            switch (operation) {
                case "QUERY":
                    // VictoriaMetrics 支持 PromQL 查询
                    Request queryRequest = new Request.Builder()
                        .url(baseUrl + "/api/v1/query?query=" + content)
                        .get()
                        .build();

                    try (Response response = client.newCall(queryRequest).execute()) {
                        if (!response.isSuccessful()) {
                            throw new IOException("Query failed: " + response.body().string());
                        }
                        return new VMQueryResult(operation, response.body().string());
                    }
                    
                case "WRITE":
                    // VictoriaMetrics 支持 InfluxDB 行协议
                    RequestBody body = RequestBody.create(content, MEDIA_TYPE_TEXT);
                    Request writeRequest = new Request.Builder()
                        .url(baseUrl + "/write")
                        .post(body)
                        .build();

                    try (Response response = client.newCall(writeRequest).execute()) {
                        if (!response.isSuccessful()) {
                            throw new IOException("Write failed: " + response.body().string());
                        }
                        return new VMQueryResult(operation, "Write successful");
                    }
                    
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute command", e);
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
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        result.append("Benchmark completed:\n")
              .append("Iterations: ").append(iterations).append("\n")
              .append("Concurrency: ").append(concurrency).append("\n")
              .append("Total time: ").append(duration).append("ms\n")
              .append("Average time per operation: ").append(duration / iterations).append("ms");

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                // 执行一个简单的查询来验证连接
                execute(connection, "QUERY up");
            } catch (IOException e) {
                result.append("Failed to establish connection ").append(i).append(": ").append(e.getMessage()).append("\n");
            }
        }

        try {
            Thread.sleep(duration * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        result.append("Connection stress test completed:\n")
              .append("Successful connections: ").append(this.connections.size()).append("\n")
              .append("Duration: ").append((endTime - startTime) / 1000).append(" seconds");

        return result.toString();
    }

    @Override
    public void releaseConnections() {
        connections.forEach(connection -> {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        connections.clear();
    }

    @Override
    public String executeTest() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String testType = dbConfig.getTestType();
        if (testType == null || testType.isEmpty()) {
            throw new IllegalArgumentException("Test type not specified in DBConfig");
        }

        DatabaseConnection connection = null;
        StringBuilder result = new StringBuilder();

        try {
            connection = connect();

            switch (testType.toLowerCase()) {
                case "connectionstress":
                    result.append(connectionStress(
                        dbConfig.getConnectionCount(),
                        dbConfig.getDuration()
                    ));
                    break;

                case "query":
                    String query = dbConfig.getQuery();
                    if (query == null || query.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified in DBConfig");
                    }
                    QueryResult queryResult = execute(connection, query);
                    result.append(formatQueryResult(queryResult));
                    break;

                case "benchmark":
                    String benchQuery = dbConfig.getQuery();
                    if (benchQuery == null || benchQuery.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified for benchmark");
                    }
                    result.append(bench(
                        connection,
                        benchQuery,
                        dbConfig.getIterations(),
                        dbConfig.getConcurrency()
                    ));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported test type: " + testType);
            }

            return result.toString();

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval) {
        return null;
    }

    private String formatQueryResult(QueryResult result) {
        if (!(result instanceof VMQueryResult)) {
            return "Invalid result type";
        }
    
        VMQueryResult vmResult = (VMQueryResult) result;
        if (!vmResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", vmResult.getMessage());
        }
    
        StringBuilder sb = new StringBuilder();
        String operation = vmResult.getOperation();
    
        switch (operation) {
            case "WRITE":
                sb.append("Write Operation:\n");
                sb.append("Status: Success\n");
                break;
    
            case "QUERY":
                sb.append("Query Operation:\n");
                sb.append(vmResult.getMessage());
                break;
    
            default:
                return String.format("Unknown operation: %s\n", operation);
        }
    
        return sb.toString();
    }

    private static class VMConnection implements DatabaseConnection {
        private final OkHttpClient client;
        private final String baseUrl;

        VMConnection(OkHttpClient client, String baseUrl) {
            this.client = client;
            this.baseUrl = baseUrl;
        }

        @Override
        public void close() throws IOException {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }

    private static class VMQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;

        public VMQueryResult(String operation, String message) {
            this.operation = operation;
            this.message = message;
            this.success = true;
        }

        public boolean isSuccessful() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public String getOperation() {
            return operation;
        }

        @Override
        public boolean hasResultSet() {
            return success && message != null && !message.isEmpty();
        }

        @Override
        public ResultSet getResultSet() {
            return null; // VictoriaMetrics 不使用 JDBC ResultSet
        }

        @Override
        public int getUpdateCount() {
            try {
                JSONObject json = new JSONObject(message);
                JSONArray results = json.getJSONObject("data").getJSONArray("result");
                return results.length();
            } catch (Exception e) {
                return 0;
            }
        }

        @Override
        public String toString() {
            return String.format("%s operation: %s", operation, message);
        }
    }
}