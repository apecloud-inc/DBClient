package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class ZookeeperTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    
    public ZookeeperTester() {
        this.dbConfig = null;
    }

    public ZookeeperTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String connectString = String.format("%s:%d", dbConfig.getHost(), dbConfig.getPort());
        int sessionTimeout = 5000;
        try {
            CountDownLatch connectionLatch = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            });
            
            // 等待连接建立
            connectionLatch.await(sessionTimeout, TimeUnit.MILLISECONDS);
            return new ZookeeperConnection(zk);
        } catch (Exception e) {
            throw new IOException("Failed to connect to ZooKeeper server", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        ZookeeperConnection zkConnection = (ZookeeperConnection) connection;
        ZooKeeper zk = zkConnection.zooKeeper;
        
        try {
            String[] parts = command.split(" ", 3);
            if (parts.length < 2) {
                throw new IOException("Invalid command format");
            }

            String operation = parts[0].toUpperCase();
            String path = parts[1];

            switch (operation) {
                case "EXISTS":
                    Stat existsStat = zk.exists(path, false);
                    return new ZookeeperQueryResult("EXISTS", true, path, null, existsStat);

                case "GET":
                    Stat getStat = new Stat();
                    byte[] data = zk.getData(path, false, getStat);
                    return new ZookeeperQueryResult("GET", true, path, data, getStat);
                    
                case "PUT":
                    if (parts.length < 3) {
                        throw new IOException("PUT command requires value");
                    }
                    String value = parts[2];
                    Stat putStat = zk.exists(path, false);
                    if (putStat == null) {
                        String createdPath = zk.create(path, value.getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        return new ZookeeperQueryResult("PUT", true, createdPath, value.getBytes(), null);
                    } else {
                        Stat updatedStat = zk.setData(path, value.getBytes(), putStat.getVersion());
                        return new ZookeeperQueryResult("PUT", true, path, value.getBytes(), updatedStat);
                    }
                    
                case "DELETE":
                    Stat deleteStat = zk.exists(path, false);
                    if (deleteStat != null) {
                        zk.delete(path, deleteStat.getVersion());
                        return new ZookeeperQueryResult("DELETE", true, path, null, deleteStat);
                    }
                    return new ZookeeperQueryResult("DELETE", false, path, null, null);
                    
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
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
        String path = "/connection_stress_path";
        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                if (i == 0) {
                    execute(connection, "PUT " + path + " \"connection_stress_initial_value\"");
                }

                this.connections.add(connection);
                // 执行一个简单的GET操作来验证连接
                execute(connection, "GET /connection_stress_path");
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

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        StringBuilder result = new StringBuilder();
        ZookeeperQueryResult executeResult;
        int executeUpdateCount;

        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000L;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        java.util.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;

        // check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_path";  // 默认路径
        }

        String path = "/" + table;
        System.out.println("Execution loop start: " + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex++;
            long currentTime = System.currentTimeMillis();

            // 定期输出统计信息
            if (currentTime - lastOutputTime >= interval * 1000L) {
                outputPassTime += interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions)
                        + " successful: " + successfulExecutions + " failed: " + failedExecutions
                        + " disconnect: " + disconnectCounts);
            }

            try {
                if (executionError) {
                    Thread.sleep(1000);  // 等待一秒后尝试重连
                    connection = connect();  // 重新建立连接
                }

                if (genTestQuery == 1) {
                    // 检查节点是否存在，若不存在则创建
                    executeResult = (ZookeeperQueryResult) execute(connection, "EXISTS " + path);
                    if (executeResult.getStat() == null) {
                        System.out.println("Path " + path + " does not exist. Creating path...");
                        execute(connection, "PUT " + path + " \"executions_loop_initial_value\"");
                        System.out.println("Path " + path + " created successfully.");
                    } else {
                        System.out.println("Path " + path + " already exists.");
                        if (table.equals("executions_loop_path")) {
                            // Delete path
                            System.out.println("Delete path " + path);
                            execute(connection, "DELETE " + path);
                            System.out.println("Path " + path + " deleted successfully.");

                            System.out.println("Create path " + path);
                            execute(connection, "PUT " + path + " \"executions_loop_initial_value\"");
                            System.out.println("Path " + path + " created successfully.");
                        }
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.isEmpty())) || genTestQuery == 3) {
                    String value = "executions_loop_value_" + insertIndex;
                    query = "PUT " + path + " \"" + value + "\"";
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = (ZookeeperQueryResult) execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                if (executeUpdateCount != -1 && executeUpdateCount != 0) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        java.util.Date recoveryDate = new java.util.Date(recoveryTime);
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
                }
            } catch (IOException e) {
                failedExecutions++;
                insertIndex--;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.util.Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // 重新设置中断状态
                e.printStackTrace();
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections();  // 释放所有连接资源

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


    private String formatQueryResult(QueryResult result) {
        if (!(result instanceof ZookeeperQueryResult)) {
            return "Invalid result type";
        }
    
        ZookeeperQueryResult zkResult = (ZookeeperQueryResult) result;
        if (!zkResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", zkResult.getMessage());
        }
    
        StringBuilder sb = new StringBuilder();
        String operation = zkResult.getOperation();
    
        switch (operation.toUpperCase()) {
            case "PUT":
                sb.append("PUT Operation:\n");
                sb.append(String.format("Path: %s\n", zkResult.getPath()));
                sb.append(String.format("Value: %s\n", zkResult.getValue()));
                if (zkResult.getStat() != null) {
                    sb.append(String.format("Version: %d\n", zkResult.getStat().getVersion()));
                }
                break;

            case "EXISTS":
                sb.append("EXISTS Operation:\n");
                if (zkResult.getValue() == null) {
                    sb.append("No data found\n");
                } else {
                    sb.append(String.format("Path: %s\n", zkResult.getPath()));
                    sb.append(String.format("Value: %s\n", zkResult.getValue()));
                    if (zkResult.getStat() != null) {
                        sb.append(String.format("Version: %d\n", zkResult.getStat().getVersion()));
                    }
                }
                break;
    
            case "GET":
                sb.append("GET Operation:\n");
                if (zkResult.getValue() == null) {
                    sb.append("No data found\n");
                } else {
                    sb.append(String.format("Path: %s\n", zkResult.getPath()));
                    sb.append(String.format("Value: %s\n", zkResult.getValue()));
                    if (zkResult.getStat() != null) {
                        sb.append(String.format("Version: %d\n", zkResult.getStat().getVersion()));
                    }
                }
                break;
    
            case "DELETE":
                sb.append("DELETE Operation:\n");
                sb.append(String.format("Path: %s\n", zkResult.getPath()));
                if (zkResult.isSuccessful()) {
                    sb.append("Node deleted successfully\n");
                } else {
                    sb.append("Node not found\n");
                }
                break;
    
            default:
                return String.format("Unknown operation: %s\n", operation);
        }
    
        return sb.toString();
    }

    private static class ZookeeperConnection implements DatabaseConnection {
        private final ZooKeeper zooKeeper;

        ZookeeperConnection(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
        }

        @Override
        public void close() throws IOException {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                throw new IOException("Failed to close ZooKeeper connection", e);
            }
        }
    }

    private static class ZookeeperQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;
        private final String path;
        private final byte[] data;
        private final Stat stat;

        public ZookeeperQueryResult(String operation, boolean success, String path, byte[] data, Stat stat) {
            this.operation = operation;
            this.success = success;
            this.path = path;
            this.data = data;
            this.stat = stat;
            this.message = success ? "Operation successful" : "Operation failed";
        }

        public String getPath() {
            return path;
        }

        public String getValue() {
            return data != null ? new String(data, StandardCharsets.UTF_8) : null;
        }

        public Stat getStat() {
            return stat;
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
            return success && data != null;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // ZooKeeper 不使用 JDBC ResultSet
        }

        @Override
        public int getUpdateCount() {
            return success ? 1 : 0;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s operation: %s\n", operation, message));
            sb.append(String.format("Path: %s\n", path));
            
            if (data != null) {
                sb.append(String.format("Value: %s\n", getValue()));
            }
            
            if (stat != null) {
                sb.append(String.format("Version: %d\n", stat.getVersion()));
            }

            return sb.toString();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(2181)
                .user("zookeeper")
                .password("zookeeper")
                .dbType("zookeeper")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .build();
        ZookeeperTester tester = new ZookeeperTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();

//        dbConfig = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .testType("query")
//                .query("search")
//                .dbType("zookeeper")
//                .port(2181)
//                .user("zookeeper")
//                .password("zookeeper")
//                .build();
//
//        String path = "/executions_loop_path";
//
//        System.out.println(tester.execute(connection, "PUT " + path + " \"initial_value\""));
//
//        ZookeeperQueryResult executeResult = (ZookeeperQueryResult) tester.execute(connection, "GET " + path);
//
//        if (executeResult.isSuccessful()) {
//            System.out.println(tester.execute(connection, "Delete " + path));
//        }

//        dbConfig = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(2181)
//                .user("zookeeper")
//                .password("zookeeper")
//                .testType("connectionstress")
//                .duration(10)
//                .org("zookeeper")
//                .build();
//
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();
    }
}