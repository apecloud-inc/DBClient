package com.apecloud.dbtester.tester;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SentinelRedisTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public SentinelRedisTester() {
        this.dbConfig = null;
    }

    public SentinelRedisTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Set<String> sentinels = new HashSet<>(Arrays.asList(
                    dbConfig.getHost() + ":" + dbConfig.getPort()
            ));

            JedisSentinelPool pool = new JedisSentinelPool(
                    dbConfig.getMaster(),
                    sentinels,
                    dbConfig.getPassword(),
                    dbConfig.getSentinelPassword()
            );

            return new RedisConnection(pool);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Redis Sentinel", e);
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
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval) {
        return null;
    }

    private static class RedisConnection implements DatabaseConnection {
        private final JedisSentinelPool pool;

        RedisConnection(JedisSentinelPool pool) {
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
                .port(26379)
                .password("password")
                .testType("query")
                .build();

        SentinelRedisTester tester = new SentinelRedisTester(dbConfig);
        String testResults = tester.executeTest();
        System.out.println(testResults);
    }
}