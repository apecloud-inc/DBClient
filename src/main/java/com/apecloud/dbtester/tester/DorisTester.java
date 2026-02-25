package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DorisTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "sys";

    public DorisTester() {
        this.dbConfig = null;
    }

    public DorisTester(DBConfig dbConfig) {
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
            throw new RuntimeException("Doris JDBC Driver not found, please try again..", e);
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
            return new DorisTester.DorisConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to Doris database: " + e);
            System.err.println("Trying with database Doris.");
            try {
                return new DorisTester.DorisConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to Doris database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        DorisConnection dorisConnection = (DorisConnection) connection;
        try {
            Statement statement = dorisConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new DorisTester.DorisQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        // 建立多个连接
        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
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
                            + "text_col TEXT "
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
                    query = "INSERT INTO " + database + "." + table + " (id, value, tinyint_col, smallint_col, "
                            + "int_col, bigint_col, float_col, double_col, decimal_col, "
                            + "date_col, datetime_col, char_col, text_col) "
                            + "VALUES ("
                            + insertIndex + ", "
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
                            + "'" + randomString(255) + "' " // TEXT
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
                        executionError = false;
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

    private static class DorisConnection implements DatabaseConnection {
        private final Connection connection;

        DorisConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Doris connection", e);
            }
        }
    }

    public static class DorisQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        DorisQueryResult(ResultSet resultSet, int updateCount) {
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
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9030)
                .user("root")
                .password("***")
                .dbType("doris")
                .duration(10)
                .interval(1)
//                .query("INSERT INTO test_table (id, value) VALUES (1, 'test');")
                .testType("connectionStress")
//                .testType("executionloop")
//                .database("test_db")
//                .table("test_table")
                .connectionCount(100)
                .build();
        DorisTester tester = new DorisTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
//                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
//        connection.close();
    }
}
