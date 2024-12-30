package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OracleTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    // 默认构造函数
    public OracleTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public OracleTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle JDBC Driver not found, please try again..", e);
        }

        // Oracle的连接URL格式：jdbc:oracle:thin:@hostname:port:SID
        String url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        try {
            return new OracleConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to Oracle database", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        OracleConnection oracleConnection = (OracleConnection) connection;
        try {
            Statement statement = oracleConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new OracleQueryResult(statement.getResultSet(), statement.getUpdateCount());
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

    private static class OracleConnection implements DatabaseConnection {
        private final Connection connection;

        OracleConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Oracle connection", e);
            }
        }
    }

    public static class OracleQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        OracleQueryResult(ResultSet resultSet, int updateCount) {
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
            .port(1521)  // Oracle 默认端口
            .database("ORCL")  // Oracle SID
            .user("system")
            .password("password")
            .build();
        OracleTester tester = new OracleTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        QueryResult result = tester.execute(connection, "SELECT * FROM users");
        connection.close();
    }
}
