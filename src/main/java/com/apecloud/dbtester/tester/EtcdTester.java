package com.apecloud.dbtester.tester;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.DeleteResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

public class EtcdTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private final DBConfig dbConfig;

    public EtcdTester() {
        this.dbConfig = null;
    }

    public EtcdTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String url = String.format("http://%s:%d", dbConfig.getHost(), dbConfig.getPort());
        Client client;
        try {
            if (dbConfig.getUser() != null && dbConfig.getPassword() != null) {
                client = Client.builder()
                    .endpoints(url)
                    .user(ByteSequence.from(dbConfig.getUser().getBytes()))
                    .password(ByteSequence.from(dbConfig.getPassword().getBytes()))
                    .build();
            } else {
                client = Client.builder().endpoints(url).build();
            }
            return new EtcdConnection(client);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Etcd server", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String command) throws IOException {
        EtcdConnection etcdConnection = (EtcdConnection) connection;
        KV kvClient = etcdConnection.client.getKVClient();
        
        try {
            String[] parts = command.split(" ", 3);
            if (parts.length < 2) {
                throw new IOException("Invalid command format");
            }

            String operation = parts[0].toUpperCase();
            String key = parts[1];
            ByteSequence keyBytes = ByteSequence.from(key.getBytes());

            switch (operation) {
                case "GET":
                    GetResponse getResponse = kvClient.get(keyBytes).get();
                    return new EtcdQueryResult(getResponse);
                case "PUT":
                    if (parts.length < 3) {
                        throw new IOException("PUT command requires value");
                    }
                    String value = parts[2];
                    ByteSequence valueBytes = ByteSequence.from(value.getBytes());
                    PutResponse putResponse = kvClient.put(keyBytes, valueBytes).get();
                    return new EtcdQueryResult(putResponse);
                case "DELETE":
                    DeleteResponse deleteResponse = kvClient.delete(keyBytes).get();
                    return new EtcdQueryResult(deleteResponse);
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

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                // 执行一个简单的GET操作来验证连接
                execute(connection, "GET test_key");
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
            EtcdConnection etcdConnection = (EtcdConnection) connection;

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
                    
                    // etcd 支持的基本操作：PUT、GET、DELETE
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

    private String formatQueryResult(QueryResult result) {
        if (!(result instanceof EtcdQueryResult)) {
            return "Invalid result type";
        }
    
        EtcdQueryResult etcdResult = (EtcdQueryResult) result;
        if (!etcdResult.isSuccessful()) {
            return String.format("Operation failed: %s\n", etcdResult.getMessage());
        }
    
        StringBuilder sb = new StringBuilder();
        String operation = etcdResult.getOperation();
        List<KeyValue> kvList = etcdResult.getKvList();
    
        switch (operation.toUpperCase()) {
            case "PUT":
                sb.append("PUT Operation:\n");
                sb.append(String.format("Revision: %d\n", etcdResult.getRevision()));
                if (!kvList.isEmpty()) {
                    KeyValue kv = kvList.get(0);
                    sb.append(String.format("Key: %s\n", 
                        kv.getKey().toString(StandardCharsets.UTF_8)));
                    sb.append(String.format("Previous Value: %s\n", 
                        kv.getValue().toString(StandardCharsets.UTF_8)));
                }
                break;
    
            case "GET":
                sb.append("GET Operation:\n");
                if (kvList.isEmpty()) {
                    sb.append("No results found\n");
                } else {
                    sb.append(String.format("Found %d results:\n", kvList.size()));
                    for (int i = 0; i < kvList.size(); i++) {
                        KeyValue kv = kvList.get(i);
                        sb.append(String.format("[%d] Key: %s, Value: %s\n",
                            i + 1,
                            kv.getKey().toString(StandardCharsets.UTF_8),
                            kv.getValue().toString(StandardCharsets.UTF_8)));
                    }
                }
                break;
    
            case "DELETE":
                sb.append("DELETE Operation:\n");
                if (kvList.isEmpty()) {
                    sb.append("No previous values\n");
                } else {
                    sb.append(String.format("Deleted %d keys:\n", kvList.size()));
                    for (int i = 0; i < kvList.size(); i++) {
                        KeyValue kv = kvList.get(i);
                        sb.append(String.format("[%d] Key: %s, Previous Value: %s\n",
                            i + 1,
                            kv.getKey().toString(StandardCharsets.UTF_8),
                            kv.getValue().toString(StandardCharsets.UTF_8)));
                    }
                }
                break;
    
            default:
                return String.format("Unknown operation: %s\n", operation);
        }
    
        return sb.toString();
    }

    private static class EtcdConnection implements DatabaseConnection {
        private final Client client;

        EtcdConnection(Client client) {
            this.client = client;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private static class EtcdQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final String operation;
        private final List<KeyValue> kvList; // 存储多个 KV 结果
        private final long revision;

        public EtcdQueryResult(Object response) {
            if (response == null) {
                this.success = false;
                this.message = "Operation failed: null response";
                this.operation = "UNKNOWN";
                this.kvList = Collections.emptyList();
                this.revision = 0;
                return;
            }

            if (response instanceof PutResponse) {
                PutResponse putResponse = (PutResponse) response;
                this.success = true;
                this.operation = "PUT";
                this.message = "Put operation successful";
                this.revision = putResponse.getHeader().getRevision();

                List<KeyValue> kvs = new ArrayList<>();
                if (putResponse.hasPrevKv()) {
                    kvs.add(putResponse.getPrevKv());
                }
                this.kvList = Collections.unmodifiableList(kvs);
            } else if (response instanceof GetResponse) {
                GetResponse getResponse = (GetResponse) response;
                this.success = getResponse.getCount() > 0;
                this.operation = "GET";
                this.revision = getResponse.getHeader().getRevision();

                if (this.success) {
                    this.message = "Get operation successful";
                    this.kvList = Collections.unmodifiableList(getResponse.getKvs());
                } else {
                    this.message = "Key not found";
                    this.kvList = Collections.emptyList();
                }
            } else if (response instanceof DeleteResponse) {
                DeleteResponse deleteResponse = (DeleteResponse) response;
                this.success = deleteResponse.getDeleted() > 0;
                this.operation = "DELETE";
                this.revision = deleteResponse.getHeader().getRevision();

                if (this.success) {
                    this.message = "Delete operation successful";
                    this.kvList = Collections.unmodifiableList(deleteResponse.getPrevKvs());
                } else {
                    this.message = "Key not found";
                    this.kvList = Collections.emptyList();
                }
            } else {
                this.success = false;
                this.message = "Unsupported response type: " + response.getClass().getName();
                this.operation = "UNKNOWN";
                this.kvList = Collections.emptyList();
                this.revision = 0;
            }
        }

        // 获取所有键值对
        public List<KeyValue> getKvList() {
            return kvList;
        }

        // 获取第一个键值对的键（如果存在）
        public String getKey() {
            if (!kvList.isEmpty()) {
                return kvList.get(0).getKey().toString(StandardCharsets.UTF_8);
            }
            return "";
        }

        // 获取第一个键值对的值（如果存在）
        public String getValue() {
            if (!kvList.isEmpty()) {
                return kvList.get(0).getValue().toString(StandardCharsets.UTF_8);
            }
            return "";
        }

        // 格式化单个 KeyValue 为字符串
        private String formatKeyValue(KeyValue kv) {
            return String.format("Key: %s, Value: %s, Version: %d",
                    kv.getKey().toString(StandardCharsets.UTF_8),
                    kv.getValue().toString(StandardCharsets.UTF_8),
                    kv.getVersion());
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

        public long getRevision() {
            return revision;
        }

        @Override
        public boolean hasResultSet() {
            return success && !kvList.isEmpty();
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // etcd 不使用 JDBC ResultSet
        }

        @Override
        public int getUpdateCount() {
            return kvList.size();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s operation: %s (revision: %d)\n",
                    operation,
                    message,
                    revision));

            if (!kvList.isEmpty()) {
                sb.append("Results:\n");
                for (int i = 0; i < kvList.size(); i++) {
                    sb.append(String.format("[%d] %s\n",
                            i + 1,
                            formatKeyValue(kvList.get(i))));
                }
            }

            return sb.toString();
        }
    }
}