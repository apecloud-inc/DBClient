package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClickHouseTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "default";
    private String databaseCluster = "default";

    public ClickHouseTester() {
        this.dbConfig = null;
    }

    public ClickHouseTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }


    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:clickhouse://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:clickhouse://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new ClickHouseConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to ClickHouse database: " + e);
            System.err.println("Trying with default database.");
            try {
                return new ClickHouseConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to ClickHouse database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        ClickHouseConnection clickHouseConnection = (ClickHouseConnection) connection;
        try {
            Statement statement = clickHouseConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new ClickHouseQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        String cluster = dbConfig.getCluster();
        if (cluster != null && !cluster.equals("")) {
            databaseCluster = cluster;
        }
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
                    // create test database
                    System.out.println("create database " + database);
                    genTest = "CREATE DATABASE IF NOT EXISTS " + database + " ON CLUSTER " + databaseCluster + ";";
                    System.out.println(genTest);
                    execute(connection, genTest);

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        genTest = "DROP TABLE IF EXISTS " + database + "." + table + " ON CLUSTER " + databaseCluster + ";";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    // create test table
                    System.out.println("create table " + table);
                    genTest = "CREATE TABLE IF NOT EXISTS " + database + "." + table + " ON CLUSTER " + databaseCluster
                            + " (id UInt32, value String) ENGINE = ReplicatedMergeTree() ORDER BY id;";
                    System.out.println(genTest);
                    execute(connection, genTest);

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue = "executions_loop_test_" + insertIndex;
                    // set test query
                    query = "INSERT INTO " + database + "." + table + " (id, value) VALUES (" + insertIndex + ", '" + genTestValue + "');";
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
            } catch (IOException e) {
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
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


    private static class ClickHouseConnection implements DatabaseConnection {
        private final Connection connection;

        ClickHouseConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close ClickHouse connection", e);
            }
        }
    }

    public static class ClickHouseQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        ClickHouseQueryResult(ResultSet resultSet, int updateCount) {
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
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(8123)
                .user("admin")
//                .database("default")
                .password("8hH1rueHB79pIePT")
                .dbType("clickhouse")
                .duration(10)
                .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .testType("executionloop")
//            .database("test_db")
                .table("test_table")
                .build();
        ClickHouseTester tester = new ClickHouseTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}