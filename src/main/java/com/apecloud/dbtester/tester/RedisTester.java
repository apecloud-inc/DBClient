package com.apecloud.dbtester.tester;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RedisTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public RedisTester() {
        this.dbConfig = null;
    }

    public RedisTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(800); // 设置最大连接数
            poolConfig.setMaxIdle(100); // 设置最大空闲连接数
            poolConfig.setMinIdle(10); // 设置最小空闲连接数
            poolConfig.setMaxWaitMillis(3000); // 设置最大等待时间
            JedisPool pool = new JedisPool(poolConfig, dbConfig.getHost(), dbConfig.getPort(), 2000, dbConfig.getPassword());

            return new RedisConnection(pool);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Redis", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        RedisConnection redisConnection = (RedisConnection) connection;
        try {
            String[] parts = command.trim().split("\\s+");
            if (parts.length == 0) {
                throw new IOException("Empty command");
            }

            Jedis jedis = redisConnection.getResource();
            String operation = parts[0].toLowerCase();

            switch (operation) {
                case "get":
                    if (parts.length != 2) {
                        throw new IOException("GET command requires one key");
                    }
                    String result = jedis.get(parts[1]);
                    return new RedisQueryResult(Collections.singletonList(result));

                case "set":
                    if (parts.length != 3) {
                        throw new IOException("SET command requires key and value");
                    }
                    jedis.set(parts[1], parts[2]);
                    return new RedisQueryResult(1);

                case "del":
                    if (parts.length != 2) {
                        throw new IOException("DEL command requires one key");
                    }
                    Long delResult = jedis.del(parts[1]);
                    return new RedisQueryResult(delResult.intValue());

                case "keys":
                    if (parts.length != 2) {
                        throw new IOException("KEYS command requires pattern");
                    }
                    Set<String> keys = jedis.keys(parts[1]);
                    return new RedisQueryResult(new ArrayList<>(keys));

                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute Redis command", e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String command, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, command);
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
        double duration = (endTime - startTime) / 1000.0;
        double qps = iterations / duration;

        result.append("Benchmark results:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(duration).append(" seconds\n")
                .append("Queries per second: ").append(String.format("%.2f", qps));

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
        DatabaseConnection connection = null;
        StringBuilder results = new StringBuilder();
        String testType = dbConfig.getTestType();

        try {
            connection = connect();
            String testCommand = "set test_key test_value";

            switch (testType) {
                case "query":
                    execute(connection, testCommand);
                    results.append("Basic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    results.append("Connection stress test:\n")
                            .append(connectionStress(10, 5))
                            .append("\n");
                    break;

                case "benchmark":
                    results.append("Benchmark test:\n")
                            .append(bench(connection, testCommand, 1000, 10))
                            .append("\n");
                    break;

                default:
                    results.append("Unknown test type\n");
            }
        } catch (Exception e) {
            results.append("Test failed: ").append(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
            releaseConnections();
        }

        return results.toString();
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        StringBuilder result = new StringBuilder();
        QueryResult executeResult;
        int executeUpdateCount;
        String key = dbConfig.getKey();
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

        int insert_index = 0;
        int gen_test_query = 0;
        String gen_test_values;

        // check gen test query
        if (query == null || query.equals("") || (key != null && !key.equals(""))) {
            gen_test_query = 1;
        }

        if (key != null && !key.equals("")) {
            table = key;
        } else {
            table = "executions_loop_key";
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insert_index = insert_index + 1;
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
                    Thread.sleep(1000);
                    connection = this.connect();
                }

                if (gen_test_query == 1) {
                    gen_test_query = 2;
                }

                if ((gen_test_query == 2 && (query == null || query.equals("")) || gen_test_query == 3 )) {
                    gen_test_values = "executions_loop_test_" + insert_index;
                    // set test query
                    query = "set " + table + " " + gen_test_values;
                    if (gen_test_query == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    gen_test_query = 3;
                }

                executeResult = execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                if (executeUpdateCount != -1) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        java.sql.Date recoveryDate = new java.sql.Date(recoveryTime);
                        System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                        System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                        errorToRecoveryTime = recoveryTime - errorTime;
                        System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                        executionError=false;
                    }
                } else {
                    failedExecutions++;
                    insert_index = insert_index - 1;
                    executionError = true;
                }
            } catch (IOException e) {
                failedExecutions++;
                insert_index = insert_index - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError=true;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
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
                successfulExecutions+failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts);
    }

    private static class RedisConnection implements DatabaseConnection {
        private final JedisPool pool;

        RedisConnection(JedisPool pool) {
            this.pool = pool;
        }

        public Jedis getResource() {
            return pool.getResource();
        }

        @Override
        public void close() throws IOException {
            pool.close();
        }
    }

    private static class RedisQueryResult implements QueryResult {
        private final List<String> results;
        private final int updateCount;

        RedisQueryResult(List<String> results) {
            this.results = results;
            this.updateCount = 0;
        }

        RedisQueryResult(int updateCount) {
            this.results = null;
            this.updateCount = updateCount;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null;
        }

        public List<String> getResults() {
            return results;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return results != null;
        }
    }

    public static void main(String[] args) throws IOException {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(6379) // Change port to default Redis port
                .user("default")
                .password("9AT1200pTA")
                .dbType("redis")
                .duration(10)
                .interval(1)
                .testType("executionloop")
//                .key("test")
                .build();

        RedisTester tester = new RedisTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();

//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .port(6379) // Change port to default Redis port
//                .user("default")
//                .password("9AT1200pTA")
//                .dbType("redis")
//                .duration(10)
//                .connectionCount(100)
//                .testType("connectionStress")
//                .key("test")
//                .build();
//
//        RedisTester tester = new RedisTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();
    }
}
