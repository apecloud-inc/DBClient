package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class RocketMQTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public RocketMQTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String nameServer = dbConfig.getHost() + ":" + dbConfig.getPort();
        DefaultMQProducer producer = createProducer(nameServer);
        return new RocketMQConnection(producer, null);
    }

    private DefaultMQProducer createProducer(String nameServer) {
        String producerGroup = dbConfig.getOrg();
        if (producerGroup == null || producerGroup.isEmpty()) {
            producerGroup = "executions_loop_producer_group";
        }
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServer);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("Failed to start RocketMQ producer", e);
        }
        return producer;
    }

    private DefaultMQPushConsumer createConsumer(String nameServer) throws MQClientException {
        String consumerGroup = dbConfig.getOrg();
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            consumerGroup = "executions_loop_consumer_group";
        }

        String topic = dbConfig.getTopic();
        if (topic == null || topic.isEmpty()) {
            topic = "executions_loop_topic";
        }

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt msg : messages) {
                System.out.printf("Received message: %s%n", new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        return consumer;
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        RocketMQConnection conn = (RocketMQConnection) connection;
        try {
            Map<String, Object> queryMap = parseJsonQuery(query);
            String operation = (String) queryMap.get("operation");
            String topic = (String) queryMap.get("topic");
            switch (operation.toLowerCase()) {
                case "produce":
                    return handleProduce(conn, topic, queryMap);
                case "create_topic":
                    return handleCreateTopic(topic, queryMap);
                case "list_topics":
                    return handleListTopics();
                case "delete_topic":
                    return handleDeleteTopic(topic);
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute RocketMQ operation", e);
        }
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
        RocketMQQueryResult rocketMQQueryResult;
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
                    if ("executions_loop_topic".equals(table)) {
                        // check topics exists
                        genTest = "{\"operation\":\"list_topics\"}";
                        queryResult = execute(connection, genTest);
                        rocketMQQueryResult = (RocketMQQueryResult) queryResult;
                        List<String> topicsList = rocketMQQueryResult.getResults();
                        for (String topicTmp : topicsList) {
                            if (topicTmp == table || topicTmp.equals(table)) {
                                tableCount = 1;
                                break;
                            }
                        }

                        if (tableCount == 0) {
                            System.out.println("create topic " + table);
                        } else {
                            System.out.println("delete topic " + table);
                            genTest = "{\"operation\":\"delete_topic\",\"topic\":\"" + table + "\"}";
                            queryResult = execute(connection, genTest);
                            rocketMQQueryResult = (RocketMQQueryResult) queryResult;
                            System.out.println(rocketMQQueryResult.getMessage());

                        }
                        genTest = "{\"operation\":\"create_topic\",\"topic\":\"" + table + "\"}";
                        queryResult = execute(connection, genTest);
                        rocketMQQueryResult = (RocketMQQueryResult) queryResult;
                        System.out.println(rocketMQQueryResult.getMessage());
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
            } catch (IOException | InterruptedException e) {
                failedExecutions++;
                insertIndex = insertIndex - 1;
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

    private Map<String, Object> parseJsonQuery(String query) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(query, new TypeReference<HashMap<String, Object>>() {});
    }

    private QueryResult handleProduce(RocketMQConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        String key = (String) queryMap.get("key");
        String value = (String) queryMap.get("value");

        Message msg = new Message(topic, key, value.getBytes());
        conn.getProducer().send(msg);
        return new RocketMQQueryResult("Message sent to topic=" + topic);
    }

    private QueryResult handleCreateTopic(String topic, Map<String, Object> queryMap) {
        String nameServer = dbConfig.getHost() + ":" + dbConfig.getPort();
        int readQueueNums = ((Number) queryMap.getOrDefault("read_queue_nums", 4)).intValue();
        int writeQueueNums = ((Number) queryMap.getOrDefault("write_queue_nums", 4)).intValue();
        String cluster = dbConfig.getCluster();
        if (cluster == null || cluster.isEmpty()) {
            throw new RuntimeException("Cluster name must be provided to create a topic.");
        }
        DefaultMQAdminExt mqAdmin = new DefaultMQAdminExt();
        mqAdmin.setNamesrvAddr(nameServer);

        try {
            mqAdmin.start();
            mqAdmin.createTopic(cluster, topic, readQueueNums, writeQueueNums);
            return new RocketMQQueryResult("Topic " + topic + " created successfully.");
        } catch (MQClientException e) {
            throw new RuntimeException("Failed to create topic: " + topic, e);
        } finally {
            mqAdmin.shutdown();
        }
    }

    private QueryResult handleListTopics() {
        String nameServer = dbConfig.getHost() + ":" + dbConfig.getPort();
        DefaultMQAdminExt mqAdmin = new DefaultMQAdminExt();
        mqAdmin.setNamesrvAddr(nameServer);
        try {
            mqAdmin.start();
            Set<String> topicList = mqAdmin.fetchAllTopicList().getTopicList();
            System.out.println(topicList.toString());
            return new RocketMQQueryResult(new ArrayList<>(topicList));
        } catch (MQClientException | InterruptedException e) {
            throw new RuntimeException("Failed to list topics", e);
        } catch (RemotingException e) {
            e.printStackTrace();
        } finally {
            mqAdmin.shutdown();
        }
        return null;
    }

    private QueryResult handleDeleteTopic(String topic) {
        String nameServer = dbConfig.getHost() + ":" + dbConfig.getPort();
        String cluster = dbConfig.getCluster();
        DefaultMQAdminExt mqAdmin = new DefaultMQAdminExt();
        mqAdmin.setNamesrvAddr(nameServer);
        try {
            mqAdmin.start();
            ClusterInfo clusterInfo = mqAdmin.examineBrokerClusterInfo();
            HashMap<String, BrokerData> brokerDataMap = clusterInfo.getBrokerAddrTable();
            Set<String> masterAddresses = new HashSet<>();

            if (brokerDataMap != null) {
                for (BrokerData brokerData : brokerDataMap.values()) {
                    // 只取 Master（即 brokerId == 0）
                    String masterAddress = brokerData.selectBrokerAddr();
                    if (masterAddress != null) {
                        masterAddresses.add(masterAddress);
                    }
                }
            }
            if ( masterAddresses != null) {
                System.out.println(masterAddresses);
                mqAdmin.deleteTopicInBroker(masterAddresses, topic);
            }
            mqAdmin.deleteTopicInNameServer(Collections.singleton(nameServer), topic, cluster);
            return new RocketMQQueryResult("Topic '" + topic + "' deleted successfully.");
        } catch (MQClientException | InterruptedException e) {
            throw new RuntimeException("Failed to delete topic: " + topic, e);
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } finally {
            mqAdmin.shutdown();
        }
        return null;
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

    private static class RocketMQConnection implements DatabaseConnection {
        private final DefaultMQProducer producer;
        private final DefaultMQPushConsumer consumer;

        RocketMQConnection(DefaultMQProducer producer, DefaultMQPushConsumer consumer) {
            this.producer = producer;
            this.consumer = consumer;
        }

        public DefaultMQProducer getProducer() {
            return producer;
        }

        public DefaultMQPushConsumer getConsumer() {
            return consumer;
        }

        @Override
        public void close() throws IOException {
            producer.shutdown();
            if (consumer != null) {
                consumer.shutdown();
            }
        }
    }

    private static class RocketMQQueryResult implements QueryResult {
        private final String message;
        private final List<String> results;

        RocketMQQueryResult(String message) {
            this.message = message;
            this.results = null;
        }

        RocketMQQueryResult(List<String> results) {
            this.message = null;
            this.results = results;
        }

        @Override
        public boolean hasResultSet() {
            return false;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public int getUpdateCount() {
            return 1;
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
                .port(9876)
                .user("")
                .password("")
                .dbType("rocketmq")
                .duration(3)
                .interval(1)
                .connectionCount(100)
                .query("{\"operation\":\"produce\",\"topic\":\"test_topic\",\"key\":\"testKey\",\"value\":\"testValue\"}")
                .testType("connectionStress")
                .cluster("rocketmq-dewxsh")
                .build();
        RocketMQTester tester = new RocketMQTester(dbConfig);
        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
        System.out.println(result);

//        DatabaseConnection connection = tester.connect();
//        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
//                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
//        System.out.println(result);
//        connection.close();

//        String genTest = "{\"operation\":\"create_topic\",\"topic\":\"executions_loop_topic\"}";
//        QueryResult result = tester.execute(connection, genTest);
//        System.out.println(((RocketMQQueryResult)result).getMessage());

//        String genTest = "{\"operation\":\"list_topics\"}";
//        QueryResult result = tester.execute(connection, genTest);
//        if (result instanceof RocketMQQueryResult) {
//            RocketMQQueryResult mqResult = (RocketMQQueryResult) result;
//            if (mqResult.getResults() != null) {
//                for (String topic : mqResult.getResults()) {
//                    System.out.println(topic);
//                }
//            } else {
//                System.out.println(mqResult.getMessage());
//            }
//        }

//        String produceQuery = "{\"operation\":\"produce\",\"topic\":\"executions_loop_topic\",\"key\":\"executions_loop_key\",\"value\":\"executions_loop_value\"}";
//        QueryResult result = tester.execute(connection, produceQuery);
//        System.out.println(((RocketMQQueryResult)result).getMessage());

//        String genTest = "{\"operation\":\"delete_topic\",\"topic\":\"executions_loop_topic\"}";
//        QueryResult result = tester.execute(connection, genTest);
//        System.out.println(((RocketMQQueryResult)result).getMessage());

//        connection.close();
    }
}
