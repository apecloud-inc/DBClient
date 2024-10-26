package com.apecloud.dbtester.tester;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApi;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InfluxDBTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
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
            throw new IOException("Database configuration is not set");
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
            String bucket = dbConfig.getDatabase();

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
                    return new InfluxDBQueryResult(operation, tables);
                    
                case "WRITE":
                    WriteApi writeApi = client.getWriteApi();
                    writeApi.writeRecord(dbConfig.getDatabase(), "default", WritePrecision.NS, content);
                    writeApi.flush();
                    return new InfluxDBQueryResult(operation, null);
                    
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
                execute(connection, "QUERY from(bucket:\"" + dbConfig.getDatabase() + "\") |> range(start: -1m)");
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
            if (connection != null && !"connectionstress".equalsIgnoreCase(testType)) {
                connection.close();
            }
        }
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
}