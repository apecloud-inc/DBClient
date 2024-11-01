package com.apecloud.dbtester.tester;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QdrantTesterHttp implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public QdrantTesterHttp() {
        this.dbConfig = null;
    }

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
    public QdrantQueryResult execute(DatabaseConnection connection, String query) throws IOException {
        QdrantConnection qdrantConnection = (QdrantConnection) connection;
        try {
            Request request = new Request.Builder()
                    .url(qdrantConnection.buildQueryUrl())
                    .build();

            Response response = qdrantConnection.client.newCall(request).execute();
            if (response.isSuccessful()) {
                return new QdrantQueryResult(response.body().string());
            } else {
                throw new IOException("Failed to execute query: " + response.code());
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute query", e);
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
        long startTime = System.currentTimeMillis();
        int successfulConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);

                // 执行健康检查请求以验证连接有效性
                execute(connection, "{\"method\": \"\"}");

                successfulConnections++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(duration * 1000);
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
            String testQuery = "{\"method\": \"health_check\"}";

            switch (testType) {
                case "query":
                    QdrantQueryResult result = execute(connection, testQuery);
                    results.append(result.getResponse());
                    results.append("\nBasic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    results.append(connectionStress(10, 5)).append("\n");
                    break;

                case "benchmark":
                    results.append(bench(connection, testQuery, 1000, 10)).append("\n");
                    break;

                default:
                    results.append("Unknown test type\n");
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
            releaseConnections();
        }

        return results.toString();
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

        public String buildHealthCheckUrl() {
            return "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/health";
        }

        public String buildQueryUrl() {
            String queryUrl = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() ;
            String testType = dbConfig.getTestType();
            String testQuery = dbConfig.getQuery();
            if (testType == "query" && testQuery != null) {
                queryUrl = queryUrl + "/" + dbConfig.getQuery();
            }
            return queryUrl;
        }
    }

    private static class QdrantQueryResult implements QueryResult {
        private final String response;

        QdrantQueryResult(String response) {
            this.response = response;
        }

        @Override
        public ResultSet getResultSet() {
            return null;
        }

        public String getResponse() {
            return response;
        }

        @Override
        public int getUpdateCount() {
            return 0;
        }

        @Override
        public boolean hasResultSet() {
            return response != null;
        }
    }

    public static void main(String[] args) {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .testType("connectionstress")
                .port(6333)
                .build();
//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .testType("query")
//                .query("collections/collection_mytest")
//                .port(6333)
//                .build();

        QdrantTesterHttp tester = new QdrantTesterHttp(dbConfig);

        try {
            String testResults = tester.executeTest();
            System.out.println(testResults);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}