package com.apecloud.dbtester.tester;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MongoDBTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public MongoDBTester() {
        this.dbConfig = null;
    }

    public MongoDBTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            MongoCredential credential = MongoCredential.createCredential(
                    dbConfig.getUser(),
                    dbConfig.getDatabase(),
                    dbConfig.getPassword().toCharArray()
            );

            MongoClientSettings settings = MongoClientSettings.builder()
                    .credential(credential)
                    .applyToClusterSettings(builder ->
                            builder.hosts(Arrays.asList(
                                    new ServerAddress(dbConfig.getHost(), dbConfig.getPort())
                            )))
                    .build();

            MongoClient mongoClient = MongoClients.create(settings);
            return new MongoDBConnection(mongoClient, dbConfig.getDatabase());
        } catch (Exception e) {
            throw new IOException("Failed to connect to MongoDB", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        MongoDBConnection mongoConnection = (MongoDBConnection) connection;
        try {
            Document queryDoc = Document.parse(query);
            String operation = queryDoc.getString("operation");
            String collection = queryDoc.getString("collection");
            Document data = queryDoc.get("data", Document.class);

            MongoCollection<Document> coll = mongoConnection.getDatabase().getCollection(collection);

            switch (operation.toLowerCase()) {
                case "find":
                    FindIterable<Document> result = coll.find(data);
                    return new MongoDBQueryResult(result);
                case "insert":
                    coll.insertOne(data);
                    return new MongoDBQueryResult(1);
                case "update":
                    Document filter = queryDoc.get("filter", Document.class);
                    UpdateResult updateResult = coll.updateOne(filter, new Document("$set", data));
                    return new MongoDBQueryResult((int) updateResult.getModifiedCount());
                case "delete":
                    DeleteResult deleteResult = coll.deleteOne(data);
                    return new MongoDBQueryResult((int) deleteResult.getDeletedCount());
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute MongoDB query", e);
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
        long startTime = System.currentTimeMillis();
        int successfulConnections = 0;
        int failedConnections = 0;

        while ((System.currentTimeMillis() - startTime) < duration * 1000) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                successfulConnections++;
            } catch (IOException e) {
                failedConnections++;
            }
        }

        return String.format("Connection stress test results:\n" +
                "Duration: %d seconds\n" +
                "Successful connections: %d\n" +
                "Failed connections: %d",
                duration, successfulConnections, failedConnections);
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
            String testQuery = "{\"operation\": \"find\", \"collection\": \"test\", \"data\": {}}";
            
            switch (testType) {
                case "query":
                    execute(connection, testQuery);
                    results.append("Basic query test: SUCCESS\n");
                    break;
                    
                case "connectionstress":
                    results.append("Connection stress test:\n")
                           .append(connectionStress(10, 5))
                           .append("\n");
                    break;
                    
                case "benchmark":
                    results.append("Benchmark test:\n")
                           .append(bench(connection, testQuery, 1000, 10))
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
        return null;
    }


    private static class MongoDBConnection implements DatabaseConnection {
        private final MongoClient client;
        private final MongoDatabase database;

        MongoDBConnection(MongoClient client, String databaseName) {
            this.client = client;
            this.database = client.getDatabase(databaseName);
        }

        public MongoDatabase getDatabase() {
            return database;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private static class MongoDBQueryResult implements QueryResult {
        private final FindIterable<Document> mongoResultSet;
        private final int updateCount;
        private final List<Document> documents;
    
        MongoDBQueryResult(FindIterable<Document> resultSet) {
            this.mongoResultSet = resultSet;
            this.updateCount = 0;
            this.documents = new ArrayList<>();
            if (resultSet != null) {
                resultSet.into(this.documents);
            }
        }
    
        MongoDBQueryResult(int updateCount) {
            this.mongoResultSet = null;
            this.updateCount = updateCount;
            this.documents = null;
        }
    
        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // 保持与接口一致，返回null
        }
    
        // 新增方法用于获取MongoDB特有的结果
        public List<Document> getDocuments() {
            return documents;
        }
    
        public FindIterable<Document> getMongoResultSet() {
            return mongoResultSet;
        }
    
        @Override
        public int getUpdateCount() {
            return updateCount;
        }
    
        @Override
        public boolean hasResultSet() {
            return mongoResultSet != null;
        }
    }

    public static void main(String[] args) throws IOException {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(27017)
                .database("test")
                .user("root")
                .password("password")
                .build();

        MongoDBTester tester = new MongoDBTester(dbConfig);
        String testResults = tester.executeTest();
        System.out.println(testResults);
    }
}