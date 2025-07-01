package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.vesoft.nebula.Row;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.SessionPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

public class NebulaTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public NebulaTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }
        String host = dbConfig.getHost();
        int port = dbConfig.getPort();
        List<HostAddress> addresses = Arrays.asList(new HostAddress(host, port));
        String user = dbConfig.getUser();
        String password = dbConfig.getPassword();
        String spaceName = dbConfig.getDatabase();
        if (spaceName == null || spaceName.isEmpty()) {
            spaceName = "default";
        }
        // config sessionPool
        SessionPoolConfig sessionPoolConfig = new SessionPoolConfig(addresses, spaceName, user, password);
        sessionPoolConfig.setMaxSessionSize(10);
        sessionPoolConfig.setMinSessionSize(2);
        SessionPool sessionPool;
        try {
            sessionPool = new SessionPool(sessionPoolConfig);
            return new NebulaDatabaseConnection(sessionPool);
        } catch (RuntimeException e) {
            System.err.println("Failed to connect to Nebula space: " + e.getMessage() );
            System.err.println("Trying with space Nebula.");
            try {
                // create default space
                NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
                nebulaPoolConfig.setMaxConnSize(10);
                NebulaPool pool = new NebulaPool();
                pool.init(addresses, nebulaPoolConfig);
                String createSpaceQuery = "CREATE SPACE IF NOT EXISTS " + spaceName + "(vid_type=FIXED_STRING(20));";
                try {
                    Session session = pool.getSession(user, password, false);
                    session.execute(createSpaceQuery);
                    System.out.println("CREATE SPACE " + spaceName + " Successfully");
                    Thread.sleep(10000);
                } catch (Exception e3) {
                    e3.printStackTrace();
                } finally {
                    pool.close();
                }
                sessionPool = new SessionPool(sessionPoolConfig);
                return new NebulaDatabaseConnection(sessionPool);
            } catch (RuntimeException e2) {
                throw new IOException("Failed to connect to Nebula space: " + e2.getMessage(), e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        NebulaDatabaseConnection nebulaConnection = (NebulaDatabaseConnection) connection;
        try {
            ResultSet result = nebulaConnection.sessionPool.execute(query);
            return new NebulaQueryResult(result);
        } catch (Exception e) {
            throw new IOException("Failed to execute query: " + e, e);
        } finally {
            if (nebulaConnection != null && !connections.contains(connection)) {
                connections.add(nebulaConnection);
            }
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

        for (int i = 0; i < iterations; i++) {
            executor.submit(() -> {
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
        releaseConnections();
        return null;
    }

    @Override
    public void releaseConnections() {
        NebulaDatabaseConnection nebulaConnection;
        System.out.println("Releasing connections...");
        for (DatabaseConnection connection : connections) {
            nebulaConnection = (NebulaDatabaseConnection) connection;
            try {
                nebulaConnection.sessionPool.close();
                nebulaConnection.close();
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
        NebulaDatabaseConnection nebulaConnection = (NebulaDatabaseConnection) connection;
        SessionPool sessionPool = nebulaConnection.sessionPool;
        ResultSet executeResult;
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

        // check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_table";
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex++;
            long currentTime = System.currentTimeMillis();

            if (currentTime - lastOutputTime >= interval * 1000) {
                outputPassTime += interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions)
                        + " successful: " + successfulExecutions + " failed: " + failedExecutions
                        + " disconnect: " + disconnectCounts);
            }

            try {
                if (executionError) {
                    if (sessionPool != null) {
                        sessionPool.close();
                    }
                    Thread.sleep(1000);
                    connection = this.connect();
                    nebulaConnection = (NebulaDatabaseConnection) connection;
                    sessionPool = nebulaConnection.sessionPool;
                }

                if (genTestQuery == 1) {
                    String dropVertexQuery = "DROP TAG IF EXISTS " + table + ";";
                    ResultSet resultDrop = sessionPool.execute(dropVertexQuery);
                    if (!resultDrop.isSucceeded()) {
                        System.err.println("Failed to create tag: " + resultDrop.getErrorMessage());
                    } else {
                        System.out.println("Drop tag " + table + " successful.");
                    }
                    String createVertexQuery = "CREATE TAG IF NOT EXISTS " + table + "(name string, age int);";
                    ResultSet resultCreate = sessionPool.execute(createVertexQuery);
                    if (!resultCreate.isSucceeded()) {
                        System.err.println("Failed to create tag: " + resultCreate.getErrorMessage());
                    } else {
                        System.out.println("Create tag " + table + " successful.");
                    }

                    String describeQuery = "DESCRIBE TAG " + table + ";";
                    ResultSet describeResult = sessionPool.execute(describeQuery);

                    if (describeResult.isSucceeded()) {
                        System.out.println("Tag exists and metadata retrieved successfully.");
                    } else {
                        System.err.println("Tag not found or not synchronized yet: " + describeResult.getErrorMessage());
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    Random random = new Random();
                    String vertexId = String.format("vertex_%d", insertIndex);
                    String name = String.format("person_%d", insertIndex);
                    int age = random.nextInt(100);

                    query = String.format("INSERT VERTEX " + table + "(name, age) VALUES \"%s\":(\"%s\", %d);",
                            vertexId, name, age);

                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = sessionPool.execute(query);
                if (executeResult.isSucceeded()) {
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
                    insertIndex--;
                    executionError = true;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                }
            } catch (Exception e) {
                System.out.println(e);
                failedExecutions++;
                insertIndex--;
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

        if (sessionPool != null) {
            sessionPool.close();
        }

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

    private static class NebulaDatabaseConnection implements DatabaseConnection {
        private final SessionPool sessionPool;

        NebulaDatabaseConnection(SessionPool sessionPool) {
            this.sessionPool = sessionPool;
        }

        @Override
        public void close() throws IOException {
            try {
                System.out.println("Closing Nebula connection...");
                sessionPool.close();
            } catch (Exception e) {
                throw new IOException("Failed to close Nebula connection", e);
            }
        }

        public ResultSet execute(String query) throws Exception {
            return sessionPool.execute(query);
        }
    }

    public static class NebulaQueryResult implements QueryResult {
        private final ResultSet resultSet;

        NebulaQueryResult(ResultSet resultSet) {
            this.resultSet = resultSet;
        }


        @Override
        public java.sql.ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public int getUpdateCount() {
            return resultSet.rowsSize();
        }

        @Override
        public boolean hasResultSet() {
            return resultSet != null && resultSet.rowsSize() > 0;
        }
    }

    public static void main(String[] args) throws Exception {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9669)
                .user("root")
                .password("5&K2193@Ezg70p9!")
                .dbType("nebula")
                .duration(60)
                .interval(1)
                .testType("executionloop")
                .database("default")
                .build();

        dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9669)
                .user("root")
                .password("5&K2193@Ezg70p9!")
                .dbType("nebula")
                .duration(1)
                .connectionCount(300)
                .testType("connectionstress")
                .database("default")
                .build();

        DBConfig dbConfig2 = new DBConfig.Builder()
                .host("localhost")
                .port(9669)
                .user("root")
                .password("5&K2193@Ezg70p9!")
                .dbType("nebula")
                .testType("query")
                .database("default")
                .query("SHOW SESSIONS;")
                .build();

        NebulaTester tester = new NebulaTester(dbConfig);
        NebulaTester tester2 = new NebulaTester(dbConfig2);
        DatabaseConnection connection = tester2.connect();

        String resultStr = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
        System.out.println(resultStr);

//        String result2 = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
//                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
//        System.out.println(result2);
//        connection.close();

        // kill session
        NebulaDatabaseConnection nebulaConnection = (NebulaDatabaseConnection) connection;
        ResultSet result = nebulaConnection.sessionPool.execute(dbConfig2.getQuery());
        List<Row> rowList = result.getRows();
        System.out.println("Total rows: " + rowList.size());
        for (Row row : rowList) {
            String killSessionQuery = "KILL SESSION " + row.getValues().get(0).getFieldValue();
            System.out.println(killSessionQuery);
            nebulaConnection.sessionPool.execute(killSessionQuery);
        }
        nebulaConnection.sessionPool.close();

    }
}
