package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClickHouseTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public ClickHouseTester() {
        this.dbConfig = null;
    }

    public ClickHouseTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

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

        try {
            return new ClickHouseConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to ClickHouse database", e);
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
        DBConfig dbConfig = new DBConfig.Builder()
            .host("localhost")
            .port(8123)
            .database("default")
            .user("default")
            .password("")
            .build();
        ClickHouseTester tester = new ClickHouseTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        QueryResult result = tester.execute(connection, "SHOW DATABASES");
        connection.close();
    }
}