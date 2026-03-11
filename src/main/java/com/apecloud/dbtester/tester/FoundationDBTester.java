package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;
import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FoundationDBTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    // 默认构造函数
    public FoundationDBTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public FoundationDBTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String libPath = "/usr/local/lib/libfdb_c.dylib";
        try {
            // 检查文件是否存在
            java.io.File libFile = new java.io.File(libPath);
            if (libFile.exists()) {
                System.load(libPath); // 直接加载绝对路径的库
                System.out.println("Successfully loaded native library from: " + libPath);
            } else {
                // 尝试常见备选路径
                String[] alternativePaths = {
                        "/opt/homebrew/lib/libfdb_c.dylib",
                        "/usr/local/lib/libfdb_c.dylib",
                        "/usr/lib/libfdb_c.dylib"
                };

                boolean loaded = false;
                for (String path : alternativePaths) {
                    if (new java.io.File(path).exists()) {
                        System.load(path);
                        System.out.println("Successfully loaded native library from: " + path);
                        loaded = true;
                        break;
                    }
                }

                if (!loaded) {
                    throw new IOException("Cannot find libfdb_c.dylib in any of the expected locations");
                }
            }
        } catch (UnsatisfiedLinkError e) {
            throw new IOException("Failed to load native library: " + e.getMessage(), e);
        }

        try {
            // FoundationDB 默认使用 /usr/local/etc/foundationdb/fdb.cluster
            // 如果需要指定集群文件，可以通过系统属性设置
            String clusterFile = System.getProperty("fdb.cluster");
            FDB fdb = FDB.selectAPIVersion(710); // 使用 API 版本 710
            if (clusterFile != null && !clusterFile.isEmpty()) {
                return new FoundationDBConnection(fdb.open(clusterFile));
            } else {
                return new FoundationDBConnection(fdb.open());
            }
        } catch (Exception e) {
            throw new IOException("Failed to connect to FoundationDB: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        FoundationDBConnection fdbConnection = (FoundationDBConnection) connection;
        try {
            // 解析查询命令，支持基本的 CRUD 操作
            // 查询格式：{"operation": "get|set|delete", "key": "xxx", "value": "xxx"}
            String[] parts = query.split("\\|");
            String operation = parts[0].trim().toLowerCase();

            switch (operation) {
                case "get":
                    String getKey = parts[1].trim();
                    byte[] getValue = fdbConnection.database.runAsync(tr -> tr.get(Tuple.from(getKey).pack())).join();
                    String result = getValue != null ? new String(getValue, StandardCharsets.UTF_8) : null;
                    return new FoundationDBQueryResult(result != null ? List.of(result) : List.of());

                case "set":
                    String setKey = parts[1].trim();
                    String setValue = parts[2].trim();
                    System.out.println("Setting key: " + setKey + ", value: " + setValue);
                    fdbConnection.database.run(tr -> {
                        tr.set(Tuple.from(setKey).pack(), setValue.getBytes(StandardCharsets.UTF_8));
                        return null;
                    });
                    return new FoundationDBQueryResult(1);

                case "delete":
                    String deleteKey = parts[1].trim();
                    fdbConnection.database.run(tr -> {
                        tr.clear(Tuple.from(deleteKey).pack());
                        return null;
                    });
                    return new FoundationDBQueryResult(1);

                case "range":
                    // 范围查询：range|startKey|endKey
                    String startKey = parts[1].trim();
                    String endKey = parts[2].trim();
                    List<String> rangeResults = new ArrayList<>();
                    fdbConnection.database.run(tr -> {
                        for (KeyValue kv : tr.getRange(
                                Tuple.from(startKey).pack(),
                                Tuple.from(endKey).pack())) {
                            rangeResults.add(new String(kv.getValue(), StandardCharsets.UTF_8));
                        }
                        return null;
                    });
                    return new FoundationDBQueryResult(rangeResults);

                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute FoundationDB query: " + e.getMessage(), e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(iterations);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, query);
                    successCount.incrementAndGet();
                } catch (IOException e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        executor.shutdown();
        try {
            latch.await(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double qps = iterations / duration;

        result.append("Benchmark results:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(String.format("%.2f", duration)).append(" seconds\n")
                .append("Queries per second: ").append(String.format("%.2f", qps)).append("\n")
                .append("Successful: ").append(successCount.get()).append("\n")
                .append("Failed: ").append(failCount.get());

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        long startTime = System.currentTimeMillis();
        int successfulConnections = 0;
        int failedConnections = 0;

        while ((System.currentTimeMillis() - startTime) < duration * 1000) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                successfulConnections++;
            } catch (IOException e) {
                failedConnections++;
            }
        }

        return String.format("Connection stress test results:\n" +
                        "Duration: %d seconds\n" +
                        "Successful connections: %d\n" +
                        "Failed connections: %d",
                duration, successfulConnections, failedConnections);
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
        StringBuilder result = new StringBuilder();
        int successfulExecutions = 0;
        int failedExecutions = 0;
        int disconnectCounts = 0;
        boolean executionError = false;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        long errorTime = 0;
        long recoveryTime;
        long errorToRecoveryTime;
        java.sql.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTestValue;

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.equals("")) {
            database = "executions_loop";
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex = insertIndex + 1;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastOutputTime >= interval * 1000) {
                outputPassTime = outputPassTime + interval;
                lastOutputTime = currentTime;
                System.out.println("[ " + outputPassTime + "s ] executions total: " + (successfulExecutions + failedExecutions)
                        + " successful: " + successfulExecutions + " failed: " + failedExecutions
                        + " disconnect: " + disconnectCounts);
            }

            try {
                if (executionError) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    connection = this.connect();
                }

                if (genTestQuery == 1) {
                    System.out.println("Initialize test data in FoundationDB");
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue = "executions_loop_test_" + insertIndex;
                    // set test query: set|key|value
                    query = "set|" + database + ":" + genTestValue + "|" + genTestValue + "_" + System.currentTimeMillis();
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }
                System.out.println("1");
                execute(connection, query);
                System.out.println("2");
                successfulExecutions++;
                if (executionError) {
                    recoveryTime = System.currentTimeMillis();
                    java.sql.Date recoveryDate = new java.sql.Date(recoveryTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                    errorToRecoveryTime = recoveryTime - errorTime;
                    System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                    executionError = false;
                }
            } catch (IOException e) {
                System.out.println("Execution loop failed: " + e.getMessage());
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.sql.Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections();

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

    private static class FoundationDBConnection implements DatabaseConnection {
        private final com.apple.foundationdb.Database database;

        FoundationDBConnection(com.apple.foundationdb.Database database) {
            this.database = database;
        }

        public com.apple.foundationdb.Database getDatabase() {
            return database;
        }

        @Override
        public void close() throws IOException {
            try {
                database.close();
            } catch (Exception e) {
                throw new IOException("Failed to close FoundationDB connection", e);
            }
        }
    }

    private static class FoundationDBQueryResult implements QueryResult {
        private final List<String> results;
        private final int updateCount;

        FoundationDBQueryResult(List<String> results) {
            this.results = results;
            this.updateCount = 0;
        }

        FoundationDBQueryResult(int updateCount) {
            this.results = new ArrayList<>();
            this.updateCount = updateCount;
        }

        @Override
        public List<String> getRawResults() {
            return results;
        }

        @Override
        public java.sql.ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return !results.isEmpty();
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("127.0.0.1")
                .port(4500)
                .user("foundationdb_cluster")
                .password("a0Zl7537")
                .dbType("foundationdb")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .database("test_db")
                .build();

        FoundationDBTester tester = new FoundationDBTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), null);
        System.out.println(result);
        connection.close();
    }
}
