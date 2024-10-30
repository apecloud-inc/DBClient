package com.apecloud.dbtester.tester;

import com.taosdata.jdbc.TSDBDriver;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TDEngineTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public TDEngineTester() {
        this.dbConfig = null;
    }

    public TDEngineTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String url = String.format("jdbc:TAOS://%s:%d/%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        try {
            Connection conn;
            if (dbConfig.getUser() != null && dbConfig.getPassword() != null) {
                conn = DriverManager.getConnection(url,
                        dbConfig.getUser(),
                        dbConfig.getPassword());
            } else {
                conn = DriverManager.getConnection(url);
            }
            return new TDEngineConnection(conn);
        } catch (SQLException e) {
            throw new IOException("Failed to connect to TDEngine server", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        TDEngineConnection tdConnection = (TDEngineConnection) connection;
        Connection conn = tdConnection.connection;

        try {
            String[] parts = command.split(" ", 2);
            if (parts.length < 2) {
                throw new IOException("Invalid command format");
            }

            String operation = parts[0].toUpperCase();
            String content = parts[1];

            Statement stmt = conn.createStatement();
            boolean hasResultSet = stmt.execute(content);
            
            if (hasResultSet) {
                ResultSet rs = stmt.getResultSet();
                return new TDEngineQueryResult(operation, rs, true, "Query executed successfully");
            } else {
                int updateCount = stmt.getUpdateCount();
                return new TDEngineQueryResult(operation, null, true, 
                    String.format("Update completed, affected rows: %d", updateCount));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to execute command", e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

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

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        result.append("Benchmark completed:\n")
              .append("Iterations: ").append(iterations).append("\n")
              .append("Concurrency: ").append(concurrency).append("\n")
              .append("Total time: ").append(duration).append("ms\n")
              .append("Average time per operation: ").append(duration / iterations).append("ms");

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                // Execute a simple query to verify the connection
                execute(connection, "QUERY SHOW DATABASES");
            } catch (IOException e) {
                result.append("Failed to establish connection ").append(i).append(": ").append(e.getMessage()).append("\n");
            }
        }

        try {
            Thread.sleep(duration * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        result.append("Connection stress test completed:\n")
              .append("Successful connections: ").append(this.connections.size()).append("\n")
              .append("Duration: ").append((endTime - startTime) / 1000).append(" seconds");

        return result.toString();
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
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String testType = dbConfig.getTestType();
        if (testType == null || testType.isEmpty()) {
            throw new IllegalArgumentException("Test type not specified in DBConfig");
        }

        DatabaseConnection connection = null;
        StringBuilder result = new StringBuilder();

        try {
            connection = connect();

            switch (testType.toLowerCase()) {
                case "connectionstress":
                    result.append(connectionStress(
                        dbConfig.getConnectionCount(),
                        dbConfig.getDuration()
                    ));
                    break;

                case "query":
                    String query = dbConfig.getQuery();
                    if (query == null || query.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified in DBConfig");
                    }
                    QueryResult queryResult = execute(connection, query);
                    result.append(formatQueryResult(queryResult));
                    break;

                case "benchmark":
                    String benchQuery = dbConfig.getQuery();
                    if (benchQuery == null || benchQuery.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified for benchmark");
                    }
                    result.append(bench(
                        connection,
                        benchQuery,
                        dbConfig.getIterations(),
                        dbConfig.getConcurrency()
                    ));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported test type: " + testType);
            }

            return result.toString();

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private String formatQueryResult(QueryResult result) throws IOException {
        if (!(result instanceof TDEngineQueryResult)) {
            return "Invalid result type";
        }

        TDEngineQueryResult tdResult = (TDEngineQueryResult) result;
        if (!tdResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", tdResult.getMessage());
        }

        StringBuilder sb = new StringBuilder();
        String operation = tdResult.getOperation();

        switch (operation) {
            case "QUERY":
                sb.append("Query Operation:\n");
                ResultSet rs = tdResult.getResultSet();
                if (rs == null) {
                    sb.append("No results found\n");
                } else {
                    try {
                        while (rs.next()) {
                            formatResultSetRow(rs, sb);
                        }
                    } catch (SQLException e) {
                        throw new IOException("Error reading query results", e);
                    }
                }
                break;

            default:
                sb.append(String.format("%s Operation:\n", operation));
                sb.append(String.format("Status: %s\n", tdResult.getMessage()));
                break;
        }

        return sb.toString();
    }

    private void formatResultSetRow(ResultSet rs, StringBuilder sb) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) sb.append(", ");
            sb.append(rs.getMetaData().getColumnName(i))
              .append(": ")
              .append(rs.getString(i));
        }
        sb.append("\n");
    }

    private static class TDEngineConnection implements DatabaseConnection {
        private final Connection connection;

        TDEngineConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close connection", e);
            }
        }
    }

    private static class TDEngineQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;
        private final ResultSet resultSet;

        public TDEngineQueryResult(String operation, ResultSet resultSet, boolean success, String message) {
            this.operation = operation;
            this.resultSet = resultSet;
            this.success = success;
            this.message = message;
        }

        public ResultSet getResultSet() {
            return resultSet;
        }

        public boolean isSuccessful() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public String getOperation() {
            return operation;
        }

        @Override
        public boolean hasResultSet() {
            return success && resultSet != null;
        }

        @Override
        public int getUpdateCount() {
            try {
                return resultSet != null ? 0 : -1;
            } catch (Exception e) {
                return -1;
            }
        }

        @Override
        public String toString() {
            return String.format("%s operation: %s", operation, message);
        }
    }
}