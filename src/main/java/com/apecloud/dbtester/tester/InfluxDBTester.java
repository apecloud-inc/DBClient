package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InfluxDBTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public InfluxDBTester() {
        this.dbConfig = null;
    }

    public InfluxDBTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IOException("Configuration is not set");
        }

        try {
            String hostname = dbConfig.getHost();
            int port = dbConfig.getPort();
            if (hostname == null || port == 0) {
                throw new IOException("Hostname and port are required");
            }

            // 拼接 URL
            String url = "http://" + hostname + ":" + port;
            String token = dbConfig.getPassword();
            String org = dbConfig.getOrg();
            if (org == null || org.equals("")) {
                org = dbConfig.getDatabase();
            }
            String bucket = dbConfig.getBucket();
            if (bucket == null || bucket.equals("")) {
                bucket = dbConfig.getTable();
            }

            // 使用 InfluxDBClientOptions 构建客户端选项
            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(url)
                    .authenticateToken(token.toCharArray())
                    .org(org != null ? org : "primary")
                    .bucket(bucket != null ? bucket : "primary")
                    .build();

            // 使用选项创建客户端
            InfluxDBClient client = InfluxDBClientFactory.create(options);
            return new InfluxDBConnection(client);
        } catch (Exception e) {
            throw new IOException("Failed to connect to InfluxDB: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        InfluxDBConnection influxConnection = (InfluxDBConnection) connection;
        InfluxDBClient client = influxConnection.client;
        
        try {
            String[] parts = command.split(" ", 2);
            if (parts.length < 2) {
                throw new IOException("Invalid command format");
            }

            String operation = parts[0].toUpperCase();
            String content = parts[1];

            switch (operation) {
                case "QUERY":
                    QueryApi queryApi = client.getQueryApi();
                    List<FluxTable> tables = queryApi.query(content);
                    System.out.println("Query result:" + tables.toString());
                    return new InfluxDBQueryResult(operation, tables);
                    
                case "WRITE":
                    WriteApi writeApi = client.makeWriteApi();
                    writeApi.writeRecord(dbConfig.getTable(), "default", WritePrecision.NS, content);
                    writeApi.flush();
                    return new InfluxDBQueryResult(operation, null);
                    
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute command " + e.getMessage(), e);
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
                String bucket = dbConfig.getBucket();
                if (bucket == null || bucket.equals("")) {
                    bucket = dbConfig.getTable();
                }

                this.connections.add(connection);
                // 执行一个简单的查询来验证连接
                execute(connection, "QUERY from(bucket:\"" + bucket + "\") |> range(start: -1h)");
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
        InfluxDBConnection influxConnection;
        StringBuilder result = new StringBuilder();
        WriteApiBlocking writeApiBlocking;
        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTest;
        String genTestValue;
        Organization organization = null;
        String bucketId = null;

        // Check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.equals("")) {
            database = dbConfig.getOrg();
            if (database == null || database.equals("")) {
                database = "primary";
            }
        }

        if (table == null || table.equals("")) {
            table = dbConfig.getBucket();
            if (table == null || table.equals("")) {
                table = "executions_loop_bucket";
            }
        }

        System.out.println("Execution loop start: " + query);
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
                    Thread.sleep(1000);
                    connection = this.connect();
                }
                influxConnection = (InfluxDBConnection) connection;
                InfluxDBClient client = influxConnection.client;
                if (genTestQuery == 1) {
                    OrganizationsApi orgApi = client.getOrganizationsApi();
                    List<Organization> organizations = orgApi.findOrganizations();
                    for (Organization org : organizations) {
                        if (org.getName().equals(database)) {
                            organization = org;
                            break;
                        }
                    }


                    // Check if bucket exists, if not create it
                    BucketsApi bucketsApi = client.getBucketsApi();
                    List<Bucket> buckets = bucketsApi.findBucketsByOrgName(database);

                    boolean bucketExists = false;
                    for (Bucket b : buckets) {
                        if (b.getName().equals(table)) {
                            bucketExists = true;
                            bucketId = b.getId();
                            break;
                        }
                    }

                    if (!bucketExists) {
                        System.out.println("Bucket " + table + " does not exist. Creating bucket...");
                        bucketsApi.createBucket(table, organization);
                        System.out.println("Bucket " + table + " created successfully.");
                    } else {
                        System.out.println("Bucket " + table + " already exists.");
                        if (table.equals("executions_loop_bucket")) {
                            // delete bucket
                            System.out.println("Delete bucket " + table);
                            bucketsApi.deleteBucket(bucketId);
                            System.out.println("Bucket " + table + " deleted successfully.");

                            System.out.println("Create bucket " + table);
                            bucketsApi.createBucket(table, organization);
                            System.out.println("Bucket " + table + " created successfully.");
                        }
                    }

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue =  "executions_loop executions_loop=" + insertIndex;
                    // Set test query
                    query = genTestValue;
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                // Execute the query (write operation in InfluxDB)
                writeApiBlocking = client.getWriteApiBlocking();
                writeApiBlocking.writeRecord(table, database, WritePrecision.NS, query);
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


    private String formatQueryResult(QueryResult result) {
        if (!(result instanceof InfluxDBQueryResult)) {
            return "Invalid result type";
        }
    
        InfluxDBQueryResult influxResult = (InfluxDBQueryResult) result;
        if (!influxResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", influxResult.getMessage());
        }
    
        StringBuilder sb = new StringBuilder();
        String operation = influxResult.getOperation();
    
        switch (operation) {
            case "WRITE":
                sb.append("Write Operation:\n");
                sb.append("Status: Success\n");
                break;
    
            case "QUERY":
                sb.append("Query Operation:\n");
                List<FluxTable> tables = influxResult.getTables();
                if (tables == null || tables.isEmpty()) {
                    sb.append("No results found\n");
                } else {
                    sb.append(String.format("Found %d tables:\n", tables.size()));
                    for (int i = 0; i < tables.size(); i++) {
                        FluxTable table = tables.get(i);
                        sb.append(String.format("Table %d records:\n", i + 1));
                        for (FluxRecord record : table.getRecords()) {
                            sb.append(formatRecord(record)).append("\n");
                        }
                    }
                }
                break;
    
            default:
                return String.format("Unknown operation: %s\n", operation);
        }
    
        return sb.toString();
    }

    private String formatRecord(FluxRecord record) {
        return String.format("Time: %s, Values: %s",
                record.getTime(),
                record.getValues());
    }

    private static class InfluxDBConnection implements DatabaseConnection {
        private final InfluxDBClient client;

        InfluxDBConnection(InfluxDBClient client) {
            this.client = client;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private static class InfluxDBQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;
        private final List<FluxTable> tables;

        public InfluxDBQueryResult(String operation, List<FluxTable> tables) {
            this.operation = operation;
            this.tables = tables;
            
            if ("WRITE".equals(operation)) {
                this.success = true;
                this.message = "Write operation successful";
            } else {
                this.success = tables != null;
                this.message = this.success ? "Query operation successful" : "No results found";
            }
        }

        public List<FluxTable> getTables() {
            return tables != null ? tables : Collections.emptyList();
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
            return success && tables != null && !tables.isEmpty();
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // InfluxDB 不使用 JDBC ResultSet
        }

        @Override
        public int getUpdateCount() {
            if (tables == null) return 0;
            return tables.stream()
                    .mapToInt(table -> table.getRecords().size())
                    .sum();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s operation: %s\n", operation, message));

            if (tables != null && !tables.isEmpty()) {
                sb.append(String.format("Number of tables: %d\n", tables.size()));
                for (int i = 0; i < tables.size(); i++) {
                    FluxTable table = tables.get(i);
                    sb.append(String.format("Table %d: %d records\n",
                            i + 1,
                            table.getRecords().size()));
                }
            }

            return sb.toString();
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用示例
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(8086)
                .user("admin")
                .password("3S#9&Rb!8*")
                .dbType("influxdb")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .org("primary")
                .bucket("executions_loop_bucket")
                .build();
        InfluxDBTester tester = new InfluxDBTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();

//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .port(8086)
//                .user("admin")
//                .password("3S#9&Rb!8*")
//                .dbType("influxdb")
//                .duration(10)
//                .connectionCount(10)
//                .testType("connectionStress")
//                .org("primary")
//                .bucket("executions_loop_bucket")
//                .build();
//
//        InfluxDBTester tester = new InfluxDBTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();
    }
}