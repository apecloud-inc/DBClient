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

public class StarRocksTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "sys";

    public StarRocksTester() {
        this.dbConfig = null;
    }

    public StarRocksTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("StarRocks JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new StarRocksTester.StarRocksConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to StarRocks database: " + e );
            System.err.println("Trying with database StarRocks.");
            try {
                return new StarRocksTester.StarRocksConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to StarRocks database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        StarRocksConnection starRocksConnection = (StarRocksConnection) connection;
        try {
            Statement statement = starRocksConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new StarRocksQueryResult(statement.getResultSet(), statement.getUpdateCount());
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e, e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

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

        result.append("Benchmark completed with ").append(iterations).append(" iterations and ").append(concurrency).append(" concurrency");
        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        int successfulConnections = 0;
        int failedConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                successfulConnections++;
            } catch (IOException e) {
                failedConnections++;
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(duration * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            releaseConnections();
        }

        return String.format("Connection stress test results:\n" +
                        "Duration: %d seconds\n" +
                        "Successful connections: %d\n" +
                        "Failed connections: %d",
                duration, successfulConnections, failedConnections);
    }

    @Override
    public void releaseConnections() {
        // 释放所有连接
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
        return TestExecutor.executeTest(this, dbConfig);
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        StringBuilder result = new StringBuilder();
        QueryResult executeResult;
        int executeUpdateCount;
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

        byte[] binaryData = new byte[10];
        byte[] varbinaryData = new byte[155];

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
                    // create test databases
                    System.out.println("create databases " + database);
                    genTest = "CREATE DATABASE IF NOT EXISTS " + database + ";";
                    System.out.println(genTest);
                    execute(connection, genTest);

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        genTest = "DROP TABLE IF EXISTS " + database + "." + table + ";";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    // create test table with more field types
                    System.out.println("create table " + table);
                    genTest = "CREATE TABLE IF NOT EXISTS " + database + "." + table + " ("
                            + "id INT, "
                            + "value VARCHAR(255), "
                            + "tinyint_col TINYINT, "
                            + "smallint_col SMALLINT, "
                            + "int_col INT, "
                            + "bigint_col BIGINT, "
                            + "float_col FLOAT, "
                            + "double_col DOUBLE, "
                            + "decimal_col DECIMAL(10, 2), "
                            + "date_col DATE, "
                            + "datetime_col DATETIME, "
                            + "char_col CHAR(10), "
                            + "text_col TEXT, "
                            + "binary_col BINARY(30), "
                            + "varbinary_col VARBINARY(255), "
                            + "enum_col VARCHAR(20), "
                            + "set_col VARCHAR(50) "
                            + ") ENGINE=OLAP "
                            + "DUPLICATE KEY(id) "
                            + "DISTRIBUTED BY HASH(id) BUCKETS 3 "
                            + "PROPERTIES ( 'replication_num' = '1' );";
                    System.out.println(genTest);
                    execute(connection, genTest);

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    Random random = new Random();

                    // Generate random values
                    genTestValue = "executions_loop_test_" + insertIndex;

                    random.nextBytes(binaryData);
                    random.nextBytes(varbinaryData);

                    // set test query
                    query = "INSERT INTO " + database + "." + table + " (value, tinyint_col, smallint_col, "
                            + "int_col, bigint_col, float_col, double_col, decimal_col, "
                            + "date_col, datetime_col, char_col, text_col, "
                            + "binary_col, varbinary_col, enum_col, set_col) "
                            + "VALUES ("
                            + "'" + genTestValue + "', "
                            + random.nextInt(128) + ", " // TINYINT
                            + random.nextInt(32768) + ", " // SMALLINT
                            + random.nextInt() + ", " // INT
                            + random.nextLong() + ", " // BIGINT
                            + random.nextFloat() + ", " // FLOAT
                            + random.nextDouble() + ", " // DOUBLE
                            + random.nextDouble() * 100 + ", " // DECIMAL
                            + "'" + new java.sql.Date(System.currentTimeMillis()) + "', " // DATE
                            + "'" + new java.sql.Timestamp(System.currentTimeMillis()) + "', " // DATETIME
                            + "'" + randomString(10) + "', " // CHAR
                            + "'" + randomString(255) + "', " // TEXT
                            + "TO_BINARY('base64," + bytesToBase64(binaryData) + "'), " // BINARY
                            + "TO_BINARY('base64," + bytesToBase64(varbinaryData) + "'), " // VARBINARY
                            + "'Option" + (random.nextInt(3) + 1) + "', " // ENUM
                            + "'Value" + (random.nextInt(3) + 1) + "' " // SET
                            + ");";
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                if (executeUpdateCount != -1) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        Date recoveryDate = new Date(recoveryTime);
                        System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                        System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                        errorToRecoveryTime = recoveryTime - errorTime;
                        System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                        executionError=false;
                    }
                } else {
                    failedExecutions++;
                    insertIndex = insertIndex - 1;
                    executionError = true;
                }
            } catch (IOException | InterruptedException e) {
                System.out.println(e);
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError=true;
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
                successfulExecutions+failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts);
    }

    private static class StarRocksConnection implements DatabaseConnection {
        private final Connection connection;

        StarRocksConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close StarRocks connection", e);
            }
        }
    }

    public static class StarRocksQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        StarRocksQueryResult(ResultSet resultSet, int updateCount) {
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

    // Helper method to generate random string
    private String randomString(int length) {
        return TestUtils.randomString(length);
    }

    // Helper method to convert bytes to hex string
    private String bytesToBase64(byte[] bytes) {
        return java.util.Base64.getEncoder().encodeToString(bytes);
    }

    public static void main(String[] args) throws IOException {
        // 使用 DBConfig 方式
//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .port(9030)
//                .user("root")
//                .password("kfj30E29M6")
//                .dbType("starrocks")
//                .duration(60)
//                .connectionCount(100)
//                .testType("connectionstress")
//                .database("sys")
//                .build();
//        StarRocksTester tester = new StarRocksTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();

        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
            .host("localhost")
            .port(9030)
            .user("root")
            .password("Lc94v24bM3")
            .dbType("starrocks")
            .duration(10)
            .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
            .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
            .build();
        StarRocksTester tester = new StarRocksTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
