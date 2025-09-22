package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VictoriaMetricsTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
            // 处理可能的空命令
            if (command == null || command.trim().isEmpty()) {
                throw new IOException("Command is null or empty");
            }

            String[] parts = command.split(" ", 2);
            if (parts.length < 2) {
                throw new IOException("Invalid command format. Expected: OPERATION CONTENT");
            }

            String operation = parts[0].toUpperCase();
            String content = parts[1];

            switch (operation) {
                case "QUERY":
                    // VictoriaMetrics 支持 PromQL 查询
                    String encodedQuery = java.net.URLEncoder.encode(content, "UTF-8");
                    Request queryRequest = new Request.Builder()
                            .url(baseUrl + "/select/0/prometheus/api/v1/query?query=" + encodedQuery)
                            .get()
                            .build();

                    try (Response response = client.newCall(queryRequest).execute()) {
                        if (!response.isSuccessful()) {
                            throw new IOException("Query failed with code " + response.code() + ": " + response.body().string());
                        }
                        return new VMQueryResult(operation, response.body().string());
                    }

                case "WRITE":
                    // VictoriaMetrics 集群版本写入接口
                    RequestBody body = RequestBody.create(content, MEDIA_TYPE_TEXT);
                    Request writeRequest = new Request.Builder()
                            .url(baseUrl + "/insert/0/influx/write")
                            .post(body)
                            .build();

                    try (Response response = client.newCall(writeRequest).execute()) {
                        if (!response.isSuccessful()) {
                            throw new IOException("Write failed with code " + response.code() + ": " + response.body().string());
                        }
                        return new VMQueryResult(operation, "Write successful");
                    }

                default:
                    throw new IOException("Unsupported operation: " + operation + ". Supported operations: QUERY, WRITE");
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute command: " + command + ". Error: " + e.getMessage(), e);
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
                // 执行一个简单的写入操作来验证连接，而不是查询
                // 使用一个简单的指标写入操作
                String testMetric = "test_connection_metric,instance=test value=1 " + System.currentTimeMillis();
                execute(connection, "WRITE " + testMetric);
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        StringBuilder result = new StringBuilder();
        QueryResult executeResult;
        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        java.util.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTestValue;

        // Check gen test query
        if (query == null || query.equals("")) {
            genTestQuery = 1;
        }

        System.out.println("Execution loop start: " + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex++;
            long currentTime = System.currentTimeMillis();

            if (currentTime - lastOutputTime >= interval * 1000) {
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
                    // For VictoriaMetrics, we'll create a default query if none provided
                    System.out.println("Setting up default VictoriaMetrics query for execution loop");
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue = "executions_loop_metric_" + insertIndex;
                    // Set test query - write metric data to VictoriaMetrics
                    // Using InfluxDB line protocol format
                    query = "WRITE " + genTestValue + ",type=test value=" + insertIndex + " " + System.currentTimeMillis();
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                if (executeResult != null && ((VMQueryResult) executeResult).isSuccessful()) {
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
                e.printStackTrace(); // 打印完整的异常堆栈信息以便调试
                failedExecutions++;
                insertIndex = insertIndex - 1;

                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.util.Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections();

        result.append("Execution loop completed during ").append(duration).append(" seconds\n");

        result.append(String.format("Total Executions: %d\n" +
                        "Successful Executions: %d\n" +
                        "Failed Executions: %d\n" +
                        "Disconnection Counts: %d\n",
                successfulExecutions + failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts));

        return result.toString();
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

    public static void main(String[] args) throws IOException {
        // Use example
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(8480)  // REST Port
                .user("")
                .password("")
                .dbType("victoriametrics")
                .duration(30)
                .interval(1)
                .testType("executionloop")
//                .testType("connectionstress")
                .connectionCount(50)
                .build();

        VictoriaMetricsTester tester = new VictoriaMetricsTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
        System.out.println(result);
        connection.close();
    }
}