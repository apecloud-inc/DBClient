
package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TDEngineTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public TDEngineTester() {
        this.dbConfig = null;
    }

    public TDEngineTester(DBConfig dbConfig) {
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
            String user = dbConfig.getUser();
            String password = dbConfig.getPassword();
            //String database = dbConfig.getDatabase();

            if (hostname == null || port == 0 || user == null || password == null) {
                throw new IOException("Hostname, port, user, password are required");
            }

            // JDBC URL 格式
            String jdbcUrl = "jdbc:TAOS-RS://" + hostname + ":" + port;

            // 设置时区为UTC，避免时间戳转换问题
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("timezone", "UTC");

            Connection connection = DriverManager.getConnection(jdbcUrl, properties);
            return new TDEngineConnection(connection);
        } catch (Exception e) {
            throw new IOException("Failed to connect to TDEngine: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        TDEngineConnection tdengineConnection = (TDEngineConnection) connection;
        Connection conn = tdengineConnection.getConnection();

        try {
            Statement stmt = conn.createStatement();

            // 判断是查询还是写入
            command = command.trim();
            if (command.toLowerCase().startsWith("select") ||
                    command.toLowerCase().startsWith("describe") ||
                    command.toLowerCase().startsWith("show")) {

                // 执行查询
                ResultSet rs = stmt.executeQuery(command);
                return new TDEngineQueryResult("QUERY", rs);
            } else {
                // 执行写入
                boolean isQuery = stmt.execute(command);
                int updateCount = stmt.getUpdateCount();
                return new TDEngineQueryResult("WRITE", isQuery, updateCount);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute command: " + e.getMessage(), e);
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
        DatabaseConnection connection;
        for (int i = 0; i < connections; i++) {
            try {
                connection = connect();
                execute(connection, "SHOW DATABASES");
                this.connections.add(connection);
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

        releaseConnections();
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
        TDEngineConnection tdengineConnection;
        StringBuilder result = new StringBuilder();
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
        Statement stmt = null;

        // Check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.equals("")) {
            database = "executions_loop";
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_table";
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
                tdengineConnection = (TDEngineConnection) connection;
                Connection conn = tdengineConnection.getConnection();

                if (genTestQuery == 1) {
                    stmt = conn.createStatement();

                    // 创建数据库（如果不存在）
                    stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
                    System.out.println("Ensured database " + database + " exists");

                    // 使用数据库
                    stmt.execute("USE " + database);

                    // 删除表（如果存在）
                    stmt.execute("DROP TABLE IF EXISTS " + table);
                    System.out.println("Dropped table " + table + " if it existed");

                    // 创建表（如果不存在）
                    stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                            " (ts TIMESTAMP, executions_loop INT)");
                    System.out.println("Ensured table " + table + " exists");

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    long timestamp = System.currentTimeMillis() * 1000000; // 纳秒级时间戳
                    genTestValue = "INSERT INTO " + table + " VALUES (now + " + insertIndex + "s, " + insertIndex + ")";
                    // Set test query
                    query = genTestValue;
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                // 执行写入操作
                stmt = conn.createStatement();
                stmt.execute(query);
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
            } catch (Exception e) {
                System.out.println("Execution loop failed: " + e.getMessage());
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.util.Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        // Ignore
                    }
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
        if (!(result instanceof TDEngineQueryResult)) {
            return "Invalid result type";
        }

        TDEngineQueryResult tdengineResult = (TDEngineQueryResult) result;
        if (!tdengineResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", tdengineResult.getMessage());
        }

        StringBuilder sb = new StringBuilder();
        String operation = tdengineResult.getOperation();

        switch (operation) {
            case "WRITE":
                sb.append("Write Operation:\n");
                sb.append("Status: Success\n");
                sb.append("Update count: ").append(tdengineResult.getUpdateCount()).append("\n");
                break;

            case "QUERY":
                sb.append("Query Operation:\n");
                ResultSet rs = tdengineResult.getResultSet();
                if (rs == null) {
                    sb.append("No results found\n");
                } else {
                    try {
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();

                        // 输出列名
                        for (int i = 1; i <= columnCount; i++) {
                            sb.append(metaData.getColumnName(i)).append("\t");
                        }
                        sb.append("\n");

                        // 输出数据
                        while (rs.next()) {
                            for (int i = 1; i <= columnCount; i++) {
                                sb.append(rs.getString(i)).append("\t");
                            }
                            sb.append("\n");
                        }
                    } catch (SQLException e) {
                        sb.append("Error formatting result: ").append(e.getMessage()).append("\n");
                    }
                }
                break;

            default:
                return String.format("Unknown operation: %s\n", operation);
        }

        return sb.toString();
    }

    private static class TDEngineConnection implements DatabaseConnection {
        private final Connection connection;

        TDEngineConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new IOException("Failed to close connection: " + e.getMessage(), e);
            }
        }

        public Connection getConnection() {
            return connection;
        }
    }

    private static class TDEngineQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;
        private final ResultSet resultSet;
        private final int updateCount;

        public TDEngineQueryResult(String operation, ResultSet resultSet) {
            this.operation = operation;
            this.resultSet = resultSet;
            this.updateCount = -1;

            if ("WRITE".equals(operation)) {
                this.success = true;
                this.message = "Write operation successful";
            } else {
                this.success = resultSet != null;
                this.message = this.success ? "Query operation successful" : "No results found";
            }
        }

        public TDEngineQueryResult(String operation, boolean isQuery, int updateCount) {
            this.operation = operation;
            this.resultSet = isQuery ? null : null;
            this.updateCount = updateCount;
            this.success = true;
            this.message = "Write operation successful";
        }

        public ResultSet getResultSet() {
            return resultSet;
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
            return resultSet != null;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s operation: %s\n", operation, message));

            if ("QUERY".equals(operation) && resultSet != null) {
                try {
                    resultSet.last();
                    int rowCount = resultSet.getRow();
                    sb.append(String.format("Number of rows: %d\n", rowCount));
                    resultSet.beforeFirst();
                } catch (SQLException e) {
                    sb.append("Error getting row count: ").append(e.getMessage()).append("\n");
                }
            } else if ("WRITE".equals(operation)) {
                sb.append(String.format("Update count: %d\n", updateCount));
            }

            return sb.toString();
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用示例
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(6041)  // REST 端口
                .user("root")
                .password("8H4y8f2BW7Kj")
                .dbType("tdengine")
                .duration(1)
                .interval(1)
                .testType("connectionstress")
                .connectionCount(2)
                .build();

        TDEngineTester tester = new TDEngineTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
//                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
        System.out.println(result);
//        connection.close();
    }
}