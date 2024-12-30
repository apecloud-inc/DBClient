package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DamengTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public DamengTester() {
        this.dbConfig = null;
    }

    public DamengTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("dm.jdbc.driver.DmDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Dameng JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:dm://%s:%d",
                dbConfig.getHost(),
                dbConfig.getPort());
        
        if (dbConfig.getDatabase() != null && !dbConfig.getDatabase().isEmpty()) {
            url += "/" + dbConfig.getDatabase();
        }

        try {
            return new DamengConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to Dameng database", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        DamengConnection damengConnection = (DamengConnection) connection;
        try {
            Statement statement = damengConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new DamengQueryResult(statement.getResultSet(), statement.getUpdateCount());
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

        result.append("Benchmark completed with ").append(iterations)
              .append(" iterations and ").append(concurrency).append(" concurrency");
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        return null;
    }

    private static class DamengConnection implements DatabaseConnection {
        private final Connection connection;

        DamengConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Dameng connection", e);
            }
        }
    }

    public static class DamengQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        DamengQueryResult(ResultSet resultSet, int updateCount) {
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
        // 测试代码
        DBConfig dbConfig = new DBConfig.Builder()
            .host("localhost")
            .port(5236)
            .database("DAMENG")
            .user("SYSDBA")
            .password("SYSDBA")
            .build();
            
        DamengTester tester = new DamengTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        
        // 测试查询
        QueryResult result = tester.execute(connection, "SELECT * FROM USER");
        
        try {
            if (result.hasResultSet()) {
                ResultSet rs = result.getResultSet();
                while (rs.next()) {
                    String user = rs.getString(1);
                    System.out.println("User: " + user);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }
}