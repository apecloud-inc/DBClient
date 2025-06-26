package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LokiTester implements DatabaseTester {
    private final DBConfig dbConfig;
    private final OkHttpClient httpClient;
    private List<OkHttpClient> connections = new ArrayList<>();
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public LokiTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        String baseUrl = String.format("http://%s:%d", dbConfig.getHost(), dbConfig.getPort());
        return new LokiConnection(httpClient, baseUrl, dbConfig.getUser(), dbConfig.getPassword());
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String operation) throws IOException {
        LokiConnection lokiConn = (LokiConnection) connection;
        String[] parts = operation.split(":");
        String operationType = parts[0].toLowerCase();
        String query = parts.length > 1 ? parts[1] : "";

        switch (operationType) {
            case "push":
                return pushLogs(lokiConn, query);
            case "query":
                return queryLogs(lokiConn, query);
            case "query_range":
                return queryLogsRange(lokiConn, query);
            default:
                throw new IOException("Unsupported operation: " + operationType);
        }
    }

    private QueryResult pushLogs(LokiConnection conn, String logData) throws IOException {
        JSONObject pushData = new JSONObject();
        JSONArray streams = new JSONArray();
        
        JSONObject stream = new JSONObject();
        JSONObject labels = new JSONObject();
        labels.put("job", "test_job");
        labels.put("level", "info");
        
        JSONArray values = new JSONArray();
        values.put(new JSONArray()
                .put(String.valueOf(Instant.now().toEpochMilli() * 1000000))
                .put(logData));
        
        stream.put("stream", labels);
        stream.put("values", values);
        streams.put(stream);
        pushData.put("streams", streams);

        Request request = new Request.Builder()
                .url(conn.getBaseUrl() + "/loki/api/v1/push")
                .post(RequestBody.create(pushData.toString(), JSON))
                .build();

        try (Response response = conn.getClient().newCall(request).execute()) {
            return new LokiQueryResult(response.isSuccessful(), response.body().string());
        }
    }

    private QueryResult queryLogs(LokiConnection conn, String queryStr) throws IOException {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(conn.getBaseUrl() + "/loki/api/v1/query").newBuilder();
        urlBuilder.addQueryParameter("query", queryStr);
        urlBuilder.addQueryParameter("time", String.valueOf(Instant.now().getEpochSecond()));

        Request request = new Request.Builder()
                .url(urlBuilder.build())
                .get()
                .build();

        try (Response response = conn.getClient().newCall(request).execute()) {
            return new LokiQueryResult(response.isSuccessful(), response.body().string());
        }
    }

    private QueryResult queryLogsRange(LokiConnection conn, String queryStr) throws IOException {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(conn.getBaseUrl() + "/loki/api/v1/query_range").newBuilder();
        long end = Instant.now().getEpochSecond();
        long start = end - 3600; // 查询最近一小时的数据

        urlBuilder.addQueryParameter("query", queryStr);
        urlBuilder.addQueryParameter("start", String.valueOf(start));
        urlBuilder.addQueryParameter("end", String.valueOf(end));
        urlBuilder.addQueryParameter("limit", "100");

        Request request = new Request.Builder()
                .url(urlBuilder.build())
                .get()
                .build();

        try (Response response = conn.getClient().newCall(request).execute()) {
            return new LokiQueryResult(response.isSuccessful(), response.body().string());
        }
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
                    String queryStr = dbConfig.getQuery();
                    if (queryStr == null || queryStr.isEmpty()) {
                        queryStr = "{job=\"test_job\"}";
                    }
                    QueryResult queryResult = execute(connection, "query:" + queryStr);
                    result.append(queryResult.toString());
                    break;

                case "benchmark":
                    result.append(bench(
                        connection,
                        dbConfig.getQuery(),
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
        return null;
    }

    @Override
    public String bench(DatabaseConnection connection, String operation, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, operation);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double qps = iterations / duration;

        result.append(String.format("Benchmark completed:\n"))
              .append(String.format("Total iterations: %d\n", iterations))
              .append(String.format("Concurrency: %d\n", concurrency))
              .append(String.format("Total time: %.2f seconds\n", duration))
              .append(String.format("QPS: %.2f\n", qps));

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        for (int i = 0; i < connections; i++) {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .build();
            this.connections.add(client);
        }
        return String.format("Created %d connections", connections);
    }

    @Override
    public void releaseConnections() {
        for (OkHttpClient client : connections) {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
        connections.clear();
    }

    private static class LokiConnection implements DatabaseConnection {
        private final OkHttpClient client;
        private final String baseUrl;
        private final String username;
        private final String password;

        public LokiConnection(OkHttpClient client, String baseUrl, String username, String password) {
            this.client = client;
            this.baseUrl = baseUrl;
            this.username = username;
            this.password = password;
        }

        public OkHttpClient getClient() {
            return client;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        @Override
        public void close() {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }

    private static class LokiQueryResult implements QueryResult {
        private final boolean success;
        private final String message;

        public LokiQueryResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        @Override
        public boolean hasResultSet() {
            return success;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null;
        }

        @Override
        public int getUpdateCount() {
            return 0;
        }

        @Override
        public String toString() {
            return message;
        }
    }
}