package com.apecloud.dbtester.tester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class KafkaTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public KafkaTester() {
        this.dbConfig = null;
    }

    public KafkaTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, dbConfig.getHost() + ":" + dbConfig.getPort());
            props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            
            if (dbConfig.getUser() != null && !dbConfig.getUser().isEmpty()) {
                props.put("sasl.mechanism", "PLAIN");
                props.put("sasl.jaas.config", 
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        dbConfig.getUser(), dbConfig.getPassword()));
            }

            AdminClient adminClient = AdminClient.create(props);
            KafkaProducer<String, String> producer = createProducer(props);
            KafkaConsumer<String, String> consumer = createConsumer(props);

            return new KafkaConnection(adminClient, producer, consumer);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Kafka", e);
        }
    }

    private KafkaProducer<String, String> createProducer(Properties baseProps) {
        Properties props = new Properties();
        props.putAll(baseProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(Properties baseProps) {
        Properties props = new Properties();
        props.putAll(baseProps);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-tester-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        KafkaConnection kafkaConn = (KafkaConnection) connection;
        try {
            Map<String, Object> queryMap = parseJsonQuery(query);
            String operation = (String) queryMap.get("operation");
            String topic = (String) queryMap.get("topic");
            switch (operation.toLowerCase()) {
                case "produce":
                    return handleProduce(kafkaConn, topic, queryMap);
                case "consume":
                    return handleConsume(kafkaConn, topic, queryMap);
                case "create_topic":
                    return handleCreateTopic(kafkaConn, topic, queryMap);
                case "delete_topic":
                    return handleDeleteTopic(kafkaConn, topic);
                case "list_topics":
                    return handleListTopics(kafkaConn);
                case "describe_topic":
                    return handleDescribeTopic(kafkaConn, topic);
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute Kafka operation", e);
        }
    }

    private Map<String, Object> parseJsonQuery(String query) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(query, new TypeReference<HashMap<String, Object>>() {});
    }

    private QueryResult handleProduce(KafkaConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        String key = (String) queryMap.get("key");
        String value = (String) queryMap.get("value");
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = conn.getProducer().send(record);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        return new KafkaQueryResult(String.format("Message sent to topic=%s, partition=%d, offset=%d",
                metadata.topic(), metadata.partition(), metadata.offset()));
    }

    private QueryResult handleConsume(KafkaConnection conn, String topic, Map<String, Object> queryMap) {
        int timeout = (int) queryMap.getOrDefault("timeout", 5000);
        conn.getConsumer().subscribe(Collections.singletonList(topic));
        
        ConsumerRecords<String, String> records = conn.getConsumer().poll(Duration.ofMillis(timeout));
        List<String> results = new ArrayList<>();
        
        for (ConsumerRecord<String, String> record : records) {
            results.add(String.format("Received: key=%s, value=%s, topic=%s, partition=%d, offset=%d",
                    record.key(), record.value(), record.topic(), record.partition(), record.offset()));
        }
        
        return new KafkaQueryResult(results);
    }

    private QueryResult handleCreateTopic(KafkaConnection conn, String topic, Map<String, Object> queryMap) throws Exception {
        int partitions = (int) queryMap.getOrDefault("partitions", 1);
        short replicationFactor = ((Number) queryMap.getOrDefault("replication_factor", 1)).shortValue();
        
        NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
        conn.getAdminClient().createTopics(Collections.singleton(newTopic)).all().get();
        
        return new KafkaQueryResult("Topic " + topic + " created successfully");
    }

    private QueryResult handleDeleteTopic(KafkaConnection conn, String topic) throws Exception {
        conn.getAdminClient().deleteTopics(Collections.singleton(topic)).all().get();
        return new KafkaQueryResult("Topic " + topic + " deleted successfully");
    }

    private QueryResult handleListTopics(KafkaConnection conn) throws Exception {
        Set<String> topics = conn.getAdminClient().listTopics().names().get();
        return new KafkaQueryResult(new ArrayList<>(topics));
    }

    private QueryResult handleDescribeTopic(KafkaConnection conn, String topic) throws Exception {
        DescribeTopicsResult result = conn.getAdminClient().describeTopics(Collections.singleton(topic));
        TopicDescription description = result.values().get(topic).get();
        return new KafkaQueryResult("Topic Description: " + description.toString());
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
        String gen_test_key;
        String genTestValue;
        QueryResult queryResult;
        KafkaQueryResult kafkaQueryResult;
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
                    kafkaQueryResult = (KafkaQueryResult) queryResult;
                    if (queryResult.hasResultSet()) {
                        List<String> topicsList = kafkaQueryResult.getResults();
                        for (String topicTmp:topicsList) {
                            if (topicTmp == table || topicTmp.equals(table) ) {
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
                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3 )) {
                    gen_test_key = "executions_loop_key_" + insertIndex;
                    genTestValue = "executions_loop_value_" + insertIndex;
                    // set test query
                    query = "{\"operation\":\"produce\",\"topic\":\"" + table + "\",\"key\":\""
                            + gen_test_key + "\",\"value\":\"" + genTestValue + "\"}";
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

    private static class KafkaConnection implements DatabaseConnection {
        private final AdminClient adminClient;
        private final KafkaProducer<String, String> producer;
        private final KafkaConsumer<String, String> consumer;

        KafkaConnection(AdminClient adminClient, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
            this.adminClient = adminClient;
            this.producer = producer;
            this.consumer = consumer;
        }

        public AdminClient getAdminClient() {
            return adminClient;
        }

        public KafkaProducer<String, String> getProducer() {
            return producer;
        }

        public KafkaConsumer<String, String> getConsumer() {
            return consumer;
        }

        @Override
        public void close() throws IOException {
            producer.close();
            consumer.close();
            adminClient.close();
        }
    }

    private static class KafkaQueryResult implements QueryResult {
        private final String message;
        private final List<String> results;

        KafkaQueryResult(String message) {
            this.message = message;
            this.results = null;
        }

        KafkaQueryResult(List<String> results) {
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
//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .port(9092)
//                .build();
//
//        KafkaTester tester = new KafkaTester(dbConfig);
//        String testResults = tester.executeTest();
//        System.out.println(testResults);

        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9092)
                .user("admin")
                .password("test")
                .dbType("kafka")
                .duration(2)
                .interval(1)
//            .query("{\"operation\":\"list_topics\"}")
                .testType("executionloop")
//                .topic("test_table")
                .build();
        KafkaTester tester = new KafkaTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();

//        String genTest = "{\"operation\":\"delete_topic\",\"topic\":\"test\"}";
//        tester.execute(connection, genTest);
//
//        genTest = "{\"operation\":\"create_topic\",\"topic\":\"test\"}";
//        tester.execute(connection, genTest);
//
//        genTest = "{\"operation\":\"list_topics\"}";
//        QueryResult queryResult = tester.execute(connection, genTest);
//        KafkaQueryResult kafkaQueryResult = (KafkaQueryResult)queryResult;
//        if (queryResult.hasResultSet()) {
//            List<String> rs = kafkaQueryResult.getResults();
//            System.out.println(rs);
//            for (String r:rs) {
//                System.out.println(r);
//            }
//        }
    }
}