package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KingbaseTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    // 默认构造函数
    public KingbaseTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public KingbaseTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("com.kingbase8.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Kingbase JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:kingbase8://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());
        try {
            return new KingbaseConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to Kingbase database", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        KingbaseConnection kingbaseConnection = (KingbaseConnection) connection;
        try {
            Statement statement = kingbaseConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new KingbaseQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        return null;
    }

    private static class KingbaseConnection implements DatabaseConnection {
        private final Connection connection;

        KingbaseConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Kingbase connection", e);
            }
        }
    }

    public static class KingbaseQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        KingbaseQueryResult(ResultSet resultSet, int updateCount) {
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
            .port(54321)  // Kingbase 默认端口
            .database("kingbase")
            .user("system")
            .password("password")
            .build();
        KingbaseTester tester = new KingbaseTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        QueryResult result = tester.execute(connection, "SELECT * FROM system_user");
        connection.close();
    }
}