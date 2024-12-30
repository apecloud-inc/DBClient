package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MySQLTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    // 增加默认构造函数
    public MySQLTester() {
        this.dbConfig = null;
    }

    // 增加接收 DBConfig 的构造函数
    public MySQLTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 增加使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC Driver not found,please try again..", e);
        }

        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        try {
            return new MySQLConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to MySQL database", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        MySQLConnection mysqlConnection = (MySQLConnection) connection;
        try {
            Statement statement = mysqlConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new MySQLQueryResult(statement.getResultSet(), statement.getUpdateCount());
        } catch (SQLException e) {
            throw new IOException("Failed to execute query", e);
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval) {
        StringBuilder result = new StringBuilder();
        int successfulExecutions = 0;
        int failedExecutions = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insert_index = 0;
        int gen_test_query = 0;
        String query_test;
        String gen_test_values;
        if (query == null || query.equals("")) {
            gen_test_query = 1;
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insert_index = insert_index + 1;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastOutputTime >= interval * 1000) {
                outputPassTime = outputPassTime + interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions) + " successful: " + successfulExecutions + " failed: " + failedExecutions);
            }

            try {
                if (executionError) {
                    Thread.sleep(1000);
                    connection = this.connect();
                }

                if (gen_test_query == 1) {
                    // create test databases
                    System.out.println("create databases executions_loop");
                    query_test = "CREATE DATABASE IF NOT EXISTS executions_loop;";
                    execute(connection, query_test);

                    // drop test table
                    query_test = "DROP TABLE IF EXISTS executions_loop.executions_loop_table;";
                    execute(connection, query_test);

                    // create test table
                    System.out.println("create table executions_loop_table");
                    query_test = "CREATE TABLE IF NOT EXISTS executions_loop.executions_loop_table (id INT PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255));";
                    execute(connection, query_test);

                    gen_test_query = 2;
                }

                if (gen_test_query == 2) {
                    gen_test_values = "executions_loop_test_" + insert_index;
                    // set test query
                    query = "INSERT INTO executions_loop.executions_loop_table (value) VALUES ('" + gen_test_values + "');";
                }

                execute(connection, query);
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
            } catch (IOException e) {
                failedExecutions++;
                if (!executionError) {
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError=true;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions) + " successful: " + successfulExecutions + " failed: " + failedExecutions);
        releaseConnections();

        result.append("Execution loop completed during ").append(duration).append(" seconds");

        return String.format("Total Executions: %d\n" +
                "Successful Executions: %d\n" +
                "Failed Executions: %d",
                successfulExecutions+failedExecutions,
                successfulExecutions,
                failedExecutions);
    }

    private static class MySQLConnection implements DatabaseConnection {
        private final Connection connection;

        MySQLConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close MySQL connection", e);
            }
        }
    }

    public static class MySQLQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        MySQLQueryResult(ResultSet resultSet, int updateCount) {
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
            .port(3306)
            .database("mysql")
            .user("root")
            .password("JIbCA8k769351ei3")
            .dbType("mysql")
            .duration(30)
            .interval(1)
            //.query("select 1;")
            .testType("executionloop")
            .build();
        MySQLTester tester = new MySQLTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(), dbConfig.getInterval());
        System.out.println(result);
        connection.close();
    }
}