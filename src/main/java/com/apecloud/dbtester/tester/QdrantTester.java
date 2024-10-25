package com.apecloud.dbtester.tester;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.QdrantOuterClass;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QdrantTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public QdrantTester() {
        this.dbConfig = null;
    }

    public QdrantTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            QdrantGrpcClient client = QdrantGrpcClient.newBuilder(
                Grpc.newChannelBuilder(
                    dbConfig.getHost() + ":" + dbConfig.getPort(),
                    InsecureChannelCredentials.create()
                ).build()
            ).build();
            
            return new QdrantConnection(client);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Qdrant", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        QdrantConnection qdrantConnection = (QdrantConnection) connection;
        try {
            QdrantOuterClass.HealthCheckReply response = 
                qdrantConnection.client.qdrant()
                    .healthCheck(QdrantOuterClass.HealthCheckRequest.getDefaultInstance())
                    .get();
            return new QdrantQueryResult(response);
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
            String testQuery = "health_check";
            
            switch (testType) {
                case "query":
                    execute(connection, testQuery);
                    results.append("Basic query test: SUCCESS\n");
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
        private final QdrantGrpcClient client;

        QdrantConnection(QdrantGrpcClient client) {
            this.client = client;
        }

        @Override
        public void close() throws IOException {
            try {
                client.close();
            } catch (Exception e) {
                throw new IOException("Failed to close Qdrant connection", e);
            }
        }
    }

    private static class QdrantQueryResult implements QueryResult {
        private final QdrantOuterClass.HealthCheckReply response;

        QdrantQueryResult(QdrantOuterClass.HealthCheckReply response) {
            this.response = response;
        }

        @Override
        public ResultSet getResultSet() {
            return null;
        }

        public QdrantOuterClass.HealthCheckReply getQdrantResponse() {
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
}