package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import java.io.IOException;
import java.sql.Date;
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
            String databaseConnection = "admin";
            if (dbConfig.getDatabase() != null && !dbConfig.getDatabase().equals("")) {
                databaseConnection = dbConfig.getDatabase();
            }

            MongoCredential credential = MongoCredential.createCredential(
                    dbConfig.getUser(),
                    databaseConnection,
                    dbConfig.getPassword().toCharArray()
            );



            MongoClientSettings settings;

            if ("executionloop".equals(dbConfig.getTestType())){
                long serverSelectionTimeout = 1000;


                // 创建 ClusterSettings
                ClusterSettings clusterSettings = ClusterSettings.builder()
                        .serverSelectionTimeout(serverSelectionTimeout, TimeUnit.MILLISECONDS)
                        .build();

                // 创建 MongoClientSettings
               settings = MongoClientSettings.builder()
                       .applyToClusterSettings(builder -> builder.applySettings(clusterSettings))
                       .credential(credential)
                       .applyToClusterSettings(builder ->
                            builder.hosts(Arrays.asList(
                                new ServerAddress(dbConfig.getHost(), dbConfig.getPort())
                            )))
                       .build();

            } else {
                settings = MongoClientSettings.builder()
                    .credential(credential)
                    .applyToClusterSettings(builder ->
                            builder.hosts(Arrays.asList(
                                    new ServerAddress(dbConfig.getHost(), dbConfig.getPort())
                            )))
                    .build();
            }

            MongoClient mongoClient = MongoClients.create(settings);
            return new MongoDBConnection(mongoClient, databaseConnection);
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
        MongoDBConnection mongoConnection;
        StringBuilder result = new StringBuilder();
        InsertOneResult insert_result;
        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        java.sql.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTestValue;

        // check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_table";
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex = insertIndex + 1;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastOutputTime >= interval * 1000) {
                outputPassTime = outputPassTime + interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions)
                        + " successful: " + successfulExecutions + " failed: " + failedExecutions
                        + " disconnect: " + disconnectCounts);
            }

            try {
                if (executionError) {
//                    Thread.sleep(1000);
                    connection = this.connect();
                }
                mongoConnection = (MongoDBConnection) connection;
                if (genTestQuery == 1) {
                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        mongoConnection.getDatabase().getCollection(table).drop();
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue = "executions_loop_test_" + insertIndex;
                    // set test query
                    query = "{ value: '" + genTestValue + "' }";
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }

                // Execute the query (insert operation in MongoDB)
                insert_result = mongoConnection.getDatabase().getCollection(table).insertOne(Document.parse(query));
                System.out.println("Inserted document: " + insert_result.getInsertedId());
                if (insert_result.getInsertedId() != null) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        java.sql.Date recoveryDate = new java.sql.Date(recoveryTime);
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
                insertIndex = insertIndex - 1;
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
                .host("127.0.0.1")
                .port(27017)
//                .table("test")
                .user("root")
                .password("08xN3j826yV2va1r")
                .dbType("mongodb")
                .duration(60)
                .interval(1)
                .testType("executionloop")
                .build();

        MongoDBTester tester = new MongoDBTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}