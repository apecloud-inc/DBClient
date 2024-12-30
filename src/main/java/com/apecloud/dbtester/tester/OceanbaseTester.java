package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.oceanbase.jdbc.*;

public class OceanbaseTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    // 默认构造函数
    public OceanbaseTester() {
        this.dbConfig = null;
    }

    // 带配置的构造函数
    public OceanbaseTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            // 根据访问模式加载不同的驱动
            switch (dbConfig.getAccessMode()) {
                case MYSQL:
                    Class.forName("com.oceanbase.jdbc.Driver");
                    break;
                case ORACLE:
                    Class.forName("com.oceanbase.jdbc.Oracle.Driver");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported access mode for OceanBase: " + dbConfig.getAccessMode());
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("OceanBase JDBC Driver not found", e);
        }

        String url = buildConnectionUrl();
        try {
            return new OceanbaseConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to OceanBase database", e);
        }
    }

    private String buildConnectionUrl() {
        switch (dbConfig.getAccessMode()) {
            case MYSQL:
                return String.format("jdbc:oceanbase://%s:%d/%s?useSSL=false",
                    dbConfig.getHost(),
                    dbConfig.getPort(),
                    dbConfig.getDatabase());
            case ORACLE:
                return String.format("jdbc:oceanbase:oracle://%s:%d/%s",
                    dbConfig.getHost(),
                    dbConfig.getPort(),
                    dbConfig.getDatabase());
            default:
                throw new IllegalArgumentException("Unsupported access mode for OceanBase: " + dbConfig.getAccessMode());
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        OceanbaseConnection obConnection = (OceanbaseConnection) connection;
        try {
            Statement statement = obConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new OceanbaseQueryResult(statement.getResultSet(), statement.getUpdateCount());
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

        result.append("Benchmark completed with ")
              .append(iterations)
              .append(" iterations and ")
              .append(concurrency)
              .append(" concurrency");
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
        return String.format("Created %d connections", connections);
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval) {
        return null;
    }

    private static class OceanbaseConnection implements DatabaseConnection {
        private final Connection connection;

        OceanbaseConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close OceanBase connection", e);
            }
        }
    }

    public static class OceanbaseQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        OceanbaseQueryResult(ResultSet resultSet, int updateCount) {
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
        // MySQL模式示例
        DBConfig mysqlConfig = new DBConfig.Builder()
                .host("localhost")
                .port(2881)
                .database("test")
                .user("root")
                .password("password")
                .accessMode(DBConfig.AccessMode.MYSQL)
                .build();
        OceanbaseTester mysqlTester = new OceanbaseTester(mysqlConfig);
        DatabaseConnection mysqlConnection = mysqlTester.connect();
        QueryResult mysqlResult = mysqlTester.execute(mysqlConnection, "SELECT * FROM users");
        mysqlConnection.close();

        // Oracle模式示例
        DBConfig oracleConfig = new DBConfig.Builder()
                .host("localhost")
                .port(2881)
                .database("test")
                .user("sys")
                .password("password")
                .accessMode(DBConfig.AccessMode.ORACLE)
                .build();
        OceanbaseTester oracleTester = new OceanbaseTester(oracleConfig);
        DatabaseConnection oracleConnection = oracleTester.connect();
        QueryResult oracleResult = oracleTester.execute(oracleConnection, "SELECT * FROM users");
        oracleConnection.close();
    }
}