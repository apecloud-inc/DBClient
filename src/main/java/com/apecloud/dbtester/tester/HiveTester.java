
package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HiveTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "default";

    // 默认构造函数
    public HiveTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public HiveTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Hive JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:hive2://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:hive2://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new HiveConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to Hive database: " + e );
            System.err.println("Trying with database Hive.");
            try {
                return new HiveConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                System.err.println("Failed to connect to Hive database: " + e2);
                throw new IOException("Failed to connect to Hive database: ", e2);
            }
        }
    }

    @Override
    public String executeTest() throws IOException {
        return TestExecutor.executeTest(this, dbConfig);
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
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
        java.sql.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTest;
        String genTestValue;
        QueryResult queryResult;
        StringBuilder resultDb = new StringBuilder();
        String tableCount = "0";
        int maxId;

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals("")) || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.equals("")) {
            database = "executions_loop";
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
                    Thread.sleep(1000);
                    connection = this.connect();
                }

                if (genTestQuery == 1) {
                    // Check if database exists
                    genTest = "SHOW DATABASES LIKE '" + database + "'";
                    queryResult = execute(connection, genTest);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        if (rs.getMetaData() != null) {
                            ResultSetMetaData metaData = rs.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    resultDb.append(rs.getString(i));
                                }
                            }
                        }
                    }

                    if (resultDb.toString().equals("")) {
                        // Create test database
                        System.out.println("Creating database " + database);
                        genTest = "CREATE DATABASE IF NOT EXISTS " + database;
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    // Use the database
                    genTest = "USE " + database;
                    execute(connection, genTest);

                    // Check if table exists
                    genTest = "SHOW TABLES LIKE '" + table + "'";
                    queryResult = execute(connection, genTest);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        while (rs.next()) {
                            tableCount = "1";
                        }
                    }

                    if (table.equals("executions_loop_table") && !tableCount.equals("0")) {
                        // Drop test table
                        System.out.println("Dropping table " + table);
                        genTest = "DROP TABLE IF EXISTS " + table;
                        System.out.println(genTest);
                        execute(connection, genTest);

                        // Create test table
                        genTest = "CREATE TABLE " + table + " (id INT, value STRING, created_at TIMESTAMP) "
                                + "STORED AS TEXTFILE";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    } else if (!tableCount.equals("0")) {
                        genTest = "SELECT MAX(id) FROM " + table;
                        queryResult = execute(connection, genTest);
                        if (queryResult.hasResultSet()) {
                            ResultSet rs = queryResult.getResultSet();
                            while (rs.next()) {
                                String maxIdStr = rs.getString(1);
                                if (maxIdStr != null) {
                                    System.out.println("Max ID: " + maxIdStr);
                                    maxId = Integer.parseInt(maxIdStr);
                                    insertIndex += maxId;
                                }
                            }
                        }
                    } else {
                        // Create test table
                        genTest = "CREATE TABLE " + table + " (id INT, value STRING, created_at TIMESTAMP) "
                                + "STORED AS TEXTFILE";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals(""))) || genTestQuery == 3) {
                    Random random = new Random();
                    genTestValue = "executions_loop_test_" + insertIndex;

                    // Set test query
                    query = "INSERT INTO " + database + "." + table + " VALUES ("
                            + insertIndex + ", '"
                            + genTestValue + "', '"
                            + new Timestamp(System.currentTimeMillis()) + "')";

                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }

                // Execute the query
                execute(connection, query);
                successfulExecutions++;
            } catch (Exception e) {
                failedExecutions++;
                System.err.println("Error executing query: " + e.getMessage());
                executionError = true;

                if (errorTime == 0) {
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.sql.Date(errorTime);
                }

                try {
                    connection = this.connect();
                    executionError = false;
                } catch (Exception connectionException) {
                    disconnectCounts++;
                    System.err.println("Reconnection failed: " + connectionException.getMessage());
                }
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        result.append("\n").append(sdf.format(new java.util.Date())).append(" - ").append("Execution Loop completed.\n");
        result.append("Total time: ").append(totalTime).append(" ms\n");
        result.append("Successful executions: ").append(successfulExecutions).append("\n");
        result.append("Failed executions: ").append(failedExecutions).append("\n");
        result.append("Disconnect counts: ").append(disconnectCounts).append("\n");

        if (errorTime > 0) {
            recoveryTime = System.currentTimeMillis();
            errorToRecoveryTime = recoveryTime - errorTime;
            result.append("Error occurred at: ").append(errorDate).append("\n");
            result.append("Recovered at: ").append(sdf.format(new java.util.Date(recoveryTime))).append("\n");
            result.append("Time from error to recovery: ").append(errorToRecoveryTime).append(" ms\n");
        }

        return result.toString();
    }

    @Override
    public void releaseConnections() {
        for (DatabaseConnection connection : connections) {
            try {
                connection.close();
            } catch (IOException e) {
                System.err.println("Error closing Hive connection: " + e.getMessage());
            }
        }
        connections.clear();
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String sql) throws IOException {
        HiveConnection hiveConnection = (HiveConnection) connection;
        Connection conn = hiveConnection.getConnection();

        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            boolean hasResultSet = stmt.execute();

            if (hasResultSet) {
                return new HiveQueryResult(stmt.getResultSet(), -1);
            } else {
                return new HiveQueryResult(null, stmt.getUpdateCount());
            }
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + sql, e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        return null;
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        ExecutorService executor = Executors.newFixedThreadPool(connections);

        // Shared counters for all threads
        int[] successfulConnections = {0};
        int[] failedConnections = {0};
        int[] disconnectCounts = {0};

        result.append("Starting connection stress test with ")
                .append(connections)
                .append(" connections for ")
                .append(duration)
                .append(" seconds\n");

        // Start the worker threads
        for (int i = 0; i < connections; i++) {
            executor.submit(() -> {
                DatabaseConnection localConnection = null;
                boolean connected = false;

                while (System.currentTimeMillis() < endTime) {
                    try {
                        // Try to establish a new connection
                        localConnection = this.connect();
                        synchronized (successfulConnections) {
                            successfulConnections[0]++;
                        }
                        connected = true;

                        // Hold the connection for a short period
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }

                    } catch (Exception e) {
                        synchronized (failedConnections) {
                            failedConnections[0]++;
                        }
                    } finally {
                        // Close the connection if it was established
                        if (localConnection != null && connected) {
                            try {
                                localConnection.close();
                                synchronized (disconnectCounts) {
                                    disconnectCounts[0]++;
                                }
                            } catch (IOException e) {
                                // Ignore close errors in stress test
                            }
                            connected = false;
                        }
                    }
                }
            });
        }

        // Shutdown the executor and wait for termination
        executor.shutdown();
        try {
            if (!executor.awaitTermination(duration + 5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        result.append("\nConnection stress test completed.\n");
        result.append("Total time: ").append(totalTime).append(" ms\n");
        result.append("Successful connections: ").append(successfulConnections[0]).append("\n");
        result.append("Failed connections: ").append(failedConnections[0]).append("\n");
        result.append("Connections closed: ").append(disconnectCounts[0]).append("\n");

        return result.toString();
    }

    private static class HiveConnection implements DatabaseConnection {
        private final Connection connection;

        HiveConnection(Connection connection) {
            this.connection = connection;
        }

        public Connection getConnection() {
            return connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Hive connection", e);
            }
        }
    }

    public static class HiveQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        HiveQueryResult(ResultSet resultSet, int updateCount) {
            this.resultSet = resultSet;
            this.updateCount = updateCount;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return resultSet;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return resultSet != null;
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用示例
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(10000)
                .user("admin")
                .password("2K43h2N0kb")
                .dbType("hive")
                .duration(40)
                .interval(1)
//                .query("INSERT INTO test_table VALUES (1, 'test')")
                .testType("executionloop")
//                .database("test_db")
//                .table("test_table")
                .build();
        HiveTester tester = new HiveTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}