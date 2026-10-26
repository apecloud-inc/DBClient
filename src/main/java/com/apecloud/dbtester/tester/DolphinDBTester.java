package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.BenchmarkUtils;
import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.apecloud.dbtester.commons.TestExecutor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DolphinDBTester implements DatabaseTester {
    private final List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public DolphinDBTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    static String jdbcUrl(String host, int port) {
        return "jdbc:dolphindb://" + host + ":" + port;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }
        if (dbConfig.getHost() == null || dbConfig.getHost().isEmpty() || dbConfig.getPort() <= 0) {
            throw new IOException("Hostname and port are required");
        }
        try {
            Class.forName("com.dolphindb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new IOException("DolphinDB JDBC Driver not found", e);
        }

        Properties properties = new Properties();
        if (dbConfig.getUser() != null) {
            properties.setProperty("user", dbConfig.getUser());
        }
        if (dbConfig.getPassword() != null) {
            properties.setProperty("password", dbConfig.getPassword());
        }
        // ponytail: single node only. Add highAvailabilitySites when cluster failover tests need it.
        try {
            Connection connection = DriverManager.getConnection(
                    jdbcUrl(dbConfig.getHost(), dbConfig.getPort()), properties);
            return new DolphinDBConnection(connection);
        } catch (SQLException e) {
            throw new IOException("Failed to connect to DolphinDB: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        Connection conn = ((DolphinDBConnection) connection).connection;
        try {
            Statement statement = conn.createStatement();
            boolean hasResultSet = statement.execute(query);
            return new DolphinDBQueryResult(hasResultSet ? statement.getResultSet() : null,
                    hasResultSet ? 0 : statement.getUpdateCount());
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e.getMessage(), e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        return BenchmarkUtils.run(iterations, concurrency, this::connect, c -> executeBenchmark(c, query));
    }

    @Override
    public String connectionStress(int connectionCount, int duration) {
        int successful = 0;
        int failed = 0;
        for (int i = 0; i < connectionCount; i++) {
            try {
                connections.add(connect());
                successful++;
            } catch (IOException e) {
                failed++;
            }
        }
        try {
            Thread.sleep(duration * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            releaseConnections();
        }
        return "Connection stress test results:\n"
                + "Duration: " + duration + " seconds\n"
                + "Successful connections: " + successful + "\n"
                + "Failed connections: " + failed;
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval,
                                String database, String table) {
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Query not specified for execution loop");
        }
        int successful = 0;
        int failed = 0;
        long end = System.currentTimeMillis() + duration * 1000L;
        long nextReport = System.currentTimeMillis();
        DatabaseConnection current = connection;
        while (System.currentTimeMillis() < end) {
            try {
                execute(current, query);
                successful++;
            } catch (IOException e) {
                failed++;
                if (current != connection) {
                    try {
                        current.close();
                    } catch (IOException ignored) {
                        // reconnect below
                    }
                }
                try {
                    current = connect();
                } catch (IOException ignored) {
                    // counted on the next iteration
                }
            }
            long now = System.currentTimeMillis();
            if (now >= nextReport) {
                System.out.println("[executionloop] successful: " + successful + " failed: " + failed);
                nextReport = now + Math.max(interval, 1) * 1000L;
            }
        }
        return "Execution loop completed:\nSuccessful: " + successful + "\nFailed: " + failed;
    }

    private void executeBenchmark(DatabaseConnection connection, String query) throws IOException {
        Connection conn = ((DolphinDBConnection) connection).connection;
        try (Statement statement = conn.createStatement()) {
            statement.execute(query);
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e.getMessage(), e);
        }
    }

    private static class DolphinDBConnection implements DatabaseConnection {
        private final Connection connection;

        DolphinDBConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close DolphinDB connection", e);
            }
        }
    }

    private static class DolphinDBQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        DolphinDBQueryResult(ResultSet resultSet, int updateCount) {
            this.resultSet = resultSet;
            this.updateCount = updateCount;
        }

        @Override
        public ResultSet getResultSet() {
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
}
