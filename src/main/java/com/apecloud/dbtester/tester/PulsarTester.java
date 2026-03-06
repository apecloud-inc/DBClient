package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Pulsar 测试器，实现 DatabaseTester 接口
 * 支持操作：produce, consume, create_topic, delete_topic, list_topics
 */
public class PulsarTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    // Pulsar HTTP 服务默认端口（可根据实际情况修改或通过 dbConfig 传入）
    private static final int DEFAULT_HTTP_PORT = 8080;

    public PulsarTester() {
        this.dbConfig = null;
    }

    public PulsarTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            // 构建 Pulsar 服务 URL（二进制端口）
            String serviceUrl = "pulsar://" + dbConfig.getHost() + ":" + dbConfig.getPort();

            // 创建客户端
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();

            // 构建 Admin 客户端 URL（HTTP 端口，通常为 8080）
            // 注意：如果实际 HTTP 端口不是 8080，请修改此处或从 dbConfig 中获取
            String adminUrl = "http://" + dbConfig.getHost() + ":" + DEFAULT_HTTP_PORT;
            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .build();

            return new PulsarConnection(client, admin);
        } catch (Exception e) {
            // 打印详细异常信息以便调试
            e.printStackTrace();
            throw new IOException("Failed to connect to Pulsar", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        PulsarConnection pulsarConn = (PulsarConnection) connection;
        try {
            Map<String, Object> queryMap = parseJsonQuery(query);
            String operation = (String) queryMap.get("operation");
            String topic = (String) queryMap.get("topic");

            switch (operation.toLowerCase()) {
                case "produce":
                    return handleProduce(pulsarConn, topic, queryMap);
                case "consume":
                    return handleConsume(pulsarConn, topic, queryMap);
                case "create_topic":
                    return handleCreateTopic(pulsarConn, topic, queryMap);
                case "delete_topic":
                    return handleDeleteTopic(pulsarConn, topic);
                case "list_topics":
                    return handleListTopics(pulsarConn);
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            // 打印详细异常信息
            e.printStackTrace();
            throw new IOException("Failed to execute Pulsar operation", e);
        }
    }

    private Map<String, Object> parseJsonQuery(String query) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(query, new TypeReference<HashMap<String, Object>>() {});
    }

    private QueryResult handleProduce(PulsarConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        String key = (String) queryMap.get("key");
        String value = (String) queryMap.get("value");

        // 创建 Producer（可考虑复用，但为简单起见每次新建）
        Producer<byte[]> producer = conn.getClient().newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        // 发送消息
        MessageId messageId = producer.send(value.getBytes());

        producer.close();

        return new PulsarQueryResult(String.format("Message sent to topic=%s, messageId=%s",
                topic, messageId.toString()));
    }

    private QueryResult handleConsume(PulsarConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        int timeout = (int) queryMap.getOrDefault("timeout", 5000);
        int maxMessages = (int) queryMap.getOrDefault("maxMessages", 10);

        // 创建 Consumer
        Consumer<byte[]> consumer = conn.getClient().newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("pulsar-tester-subscription")
                .subscribe();

        List<String> results = new ArrayList<>();
        long endTime = System.currentTimeMillis() + timeout;
        int receivedCount = 0;

        while (System.currentTimeMillis() < endTime && receivedCount < maxMessages) {
            Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                results.add(String.format("Received: key=%s, value=%s, topic=%s, messageId=%s",
                        message.getKey() != null ? message.getKey() : "null",
                        new String(message.getValue()),
                        message.getTopicName(),
                        message.getMessageId().toString()));
                consumer.acknowledge(message);
                receivedCount++;
            }
        }

        consumer.close();

        return new PulsarQueryResult(results);
    }

    private QueryResult handleCreateTopic(PulsarConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        int partitions = (int) queryMap.getOrDefault("partitions", 1);

        if (partitions > 1) {
            // 创建分区主题
            conn.getAdmin().topics().createPartitionedTopic(topic, partitions);
        } else {
            // 创建非分区主题
            conn.getAdmin().topics().createNonPartitionedTopic(topic);
        }

        return new PulsarQueryResult("Topic " + topic + " created successfully");
    }

    private QueryResult handleDeleteTopic(PulsarConnection conn, String topic) throws Exception {
        // 先尝试删除分区主题，若失败则删除非分区主题
        try {
            conn.getAdmin().topics().deletePartitionedTopic(topic);
        } catch (Exception e) {
            conn.getAdmin().topics().delete(topic);
        }
        return new PulsarQueryResult("Topic " + topic + " deleted successfully");
    }

    private QueryResult handleListTopics(PulsarConnection conn) throws Exception {
        // 获取默认命名空间（public/default）的所有主题
        String tenant = "public";
        String namespace = "default";
        List<String> topics = conn.getAdmin().topics().getList(tenant + "/" + namespace);
        return new PulsarQueryResult(topics);
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        CountDownLatch latch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, query);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double qps = iterations / duration;

        result.append("Benchmark results:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(duration).append(" seconds\n")
                .append("Operations per second: ").append(String.format("%.2f", qps));

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        int successfulConnections = 0;
        int failedConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                successfulConnections++;
            } catch (IOException e) {
                e.printStackTrace();
                failedConnections++;
            }
        }

        try {
            Thread.sleep(duration * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            releaseConnections();
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
            String testQuery = "{\"operation\":\"produce\",\"topic\":\"test\",\"key\":\"testKey\",\"value\":\"testValue\"}";

            switch (testType) {
                case "query":
                    execute(connection, testQuery);
                    results.append("Basic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    results.append("Connection stress test:\n")
                            .append(connectionStress(10, 5))
                            .append("\n");
                    break;

                case "benchmark":
                    results.append("Benchmark test:\n")
                            .append(bench(connection, testQuery, 1000, 10))
                            .append("\n");
                    break;

                default:
                    results.append("Unknown test type\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        String topic = dbConfig.getTopic();

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
        String genTest;
        String genTestKey;
        String genTestValue;
        QueryResult queryResult;
        PulsarQueryResult pulsarQueryResult;
        int tableCount = 0;

        // check gen test query
        if (query == null || query.equals("") || (topic != null && !topic.equals(""))) {
            genTestQuery = 1;
        }

        if (topic != null && !topic.equals("")) {
            table = topic;
        } else {
            table = "executions_loop_topic";
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
                    Thread.sleep(1000);
                    connection = this.connect();
                }

                if (genTestQuery == 1) {
                    // check topics exists
                    genTest = "{\"operation\":\"list_topics\"}";
                    queryResult = execute(connection, genTest);
                    pulsarQueryResult = (PulsarQueryResult) queryResult;
                    if (queryResult.hasResultSet()) {
                        List<String> topicsList = pulsarQueryResult.getResults();
                        for (String topicTmp : topicsList) {
                            // 提取简单主题名（去除完整路径前缀）
                            String simpleTopicName = topicTmp.substring(topicTmp.lastIndexOf("/") + 1);
                            if (simpleTopicName.equals(table)) {
                                tableCount = 1;
                                break;
                            }
                        }
                    }

                    if (tableCount == 0) {
                        // create test topic
                        System.out.println("create topic " + table);
                        genTest = "{\"operation\":\"create_topic\",\"topic\":\"" + table + "\"}";
                        execute(connection, genTest);
                    }

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestKey = "executions_loop_key_" + insertIndex;
                    genTestValue = "executions_loop_value_" + insertIndex;
                    // set test query
                    query = "{\"operation\":\"produce\",\"topic\":\"" + table + "\",\"key\":\""
                            + genTestKey + "\",\"value\":\"" + genTestValue + "\"}";
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                if (executeUpdateCount != -1) {
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
                    insertIndex = insertIndex - 1;
                    executionError = true;
                }
            } catch (IOException e) {
                e.printStackTrace(); // 添加异常打印
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace(); // 捕获其他异常
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred: " + e.getMessage());
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

    private static class PulsarConnection implements DatabaseConnection {
        private final PulsarClient client;
        private final PulsarAdmin admin;

        PulsarConnection(PulsarClient client, PulsarAdmin admin) {
            this.client = client;
            this.admin = admin;
        }

        public PulsarClient getClient() {
            return client;
        }

        public PulsarAdmin getAdmin() {
            return admin;
        }

        @Override
        public void close() throws IOException {
            try {
                if (client != null) {
                    client.close();
                }
                if (admin != null) {
                    admin.close();
                }
            } catch (PulsarClientException e) {
                throw new IOException("Failed to close Pulsar client", e);
            }
        }
    }

    private static class PulsarQueryResult implements QueryResult {
        private final String message;
        private final List<String> results;

        PulsarQueryResult(String message) {
            this.message = message;
            this.results = null;
        }

        PulsarQueryResult(List<String> results) {
            this.message = null;
            this.results = results;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null;
        }

        @Override
        public int getUpdateCount() {
            return 0;
        }

        @Override
        public boolean hasResultSet() {
            return results != null;
        }

        public String getMessage() {
            return message;
        }

        public List<String> getResults() {
            return results;
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(6650)  // Pulsar 二进制端口
                .dbType("pulsar")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .build();

        PulsarTester tester = new PulsarTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}