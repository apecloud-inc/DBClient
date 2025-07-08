package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.sql.ResultSet;

import com.rabbitmq.client.*;


public class RabbitMQTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public RabbitMQTester() {
        this.dbConfig = null;
    }

    public RabbitMQTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(dbConfig.getHost());
            factory.setPort(dbConfig.getPort());
            if (dbConfig.getUser() != null && !dbConfig.getUser().isEmpty()) {
                factory.setUsername(dbConfig.getUser());
                factory.setPassword(dbConfig.getPassword());
            }
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            return new RabbitMQConnection(connection, channel);
        } catch (Exception e) {
            throw new IOException("Failed to connect to RabbitMQ", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        RabbitMQConnection mqConn = (RabbitMQConnection) connection;
        Channel channel = mqConn.getChannel();

        try {
            Map<String, Object> queryMap = parseJsonQuery(query);
            String operation = (String) queryMap.get("operation");
            String queue = (String) queryMap.get("queue");

            switch (operation.toLowerCase()) {
                case "declare_queue":
                    return handleDeclareQueue(channel, queue);
                case "delete_queue":
                    return handleDeleteQueue(channel, queue);
                case "publish":
                    return handlePublish(channel, queue, queryMap);
                case "consume":
                    return handleConsume(channel, queue, queryMap);
                case "check_queue":
                    String checkQueue = (String) queryMap.get("queue");
                    boolean exists = checkQueueExist(channel, checkQueue);
                    return new RabbitMQQueryResult("exists: " + exists);
                case "get_message_count":
                    String queueName = (String) queryMap.get("queue");
                    int messageCount = getMessageCount(channel, queueName);
                    return new RabbitMQQueryResult("Queue '" + queueName + "' has " + messageCount + " messages.");
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute RabbitMQ operation", e);
        }
    }

    private Map<String, Object> parseJsonQuery(String query) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(query, new TypeReference<HashMap<String, Object>>() {});
    }

    private QueryResult handleDeclareQueue(Channel channel, String queueName) throws IOException {
        boolean durable = true;
        channel.queueDeclare(queueName, durable, false, false, null);
        return new RabbitMQQueryResult("Queue " + queueName + " declared successfully");
    }

    private QueryResult handleDeleteQueue(Channel channel, String queueName) throws IOException {
        channel.queueDelete(queueName);
        return new RabbitMQQueryResult("Queue " + queueName + " deleted successfully");
    }

    private QueryResult handlePublish(Channel channel, String queueName, Map<String, Object> payload) throws IOException {
        String message = (String) payload.get("message");
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2) // 2 表示消息持久化
                .build();
        channel.basicPublish("", queueName, props, message.getBytes());
        return new RabbitMQQueryResult("Message sent to queue: " + queueName);
    }

    private QueryResult handleConsume(Channel channel, String queueName, Map<String, Object> queryMap) throws IOException {
        boolean noAck = (boolean) queryMap.getOrDefault("no_ack", true);

        List<String> results = new ArrayList<>();
        GetResponse response = channel.basicGet(queueName, noAck);
        if (response != null) {
            String message = new String(response.getBody(), "UTF-8");
            results.add("Received: " + message);
            if (!noAck) {
                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            }
        }

        return new RabbitMQQueryResult(results);
    }

    private boolean checkQueueExist(Channel channel, String queueName) throws IOException {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queueName);
            String queue = declareOk.getQueue();
            if (queue == null || queue.isEmpty()) {
                System.out.println("Queue does not exist: " + queueName);
                return false;
            }
            return true;
    }

    private int getMessageCount(Channel channel, String queueName) throws IOException {
        try {
            // 被动声明队列以获取其状态信息
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queueName);
            return declareOk.getMessageCount(); // 获取当前队列中未被确认的消息数
        } catch (IOException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ShutdownSignalException) {
                ShutdownSignalException sse = (ShutdownSignalException) cause;
                if (sse.getMessage().contains("404")) {
                    throw new IOException("Queue does not exist: " + queueName);
                }
            }
            throw e;
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
            String testQuery = "{\"operation\":\"declare_queue\",\"queue\":\"test\"}";

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
        String queue = dbConfig.getTopic();

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
        String genTestValue;
        QueryResult queryResult;
        RabbitMQQueryResult rmqQueryResult;

        // check gen test query
        if (query == null || query.equals("") || (queue != null && !queue.equals(""))) {
            genTestQuery = 1;
        }

        if (queue != null && !queue.equals("")) {
            table = queue;
        } else {
            table = "executions_loop_queue";
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
                   try {
                       // check queue exists
                       genTest = "{\"operation\":\"check_queue\",\"queue\":\"" + table + "\"}";
                       queryResult = execute(connection, genTest);
                        rmqQueryResult = (RabbitMQQueryResult) queryResult;
                        if (!rmqQueryResult.getMessage().contains("true")) {
                            // create test queue
                            System.out.println("Queue " + table + " does not exist. Creating queue...");
                            genTest = "{\"operation\":\"declare_queue\",\"queue\":\"" + table + "\"}";
                            execute(connection, genTest);
                            System.out.println("Queue " + table + " created successfully.");
                        } else {
                            System.out.println("Queue " + table + " already exists.");
                            if ("executions_loop_queue".equals(table)) {
                                genTest = "{\"operation\":\"delete_queue\",\"queue\":\"" + table + "\"}";
                                execute(connection, genTest);
                                System.out.println("Queue " + table + " deleted successfully.");
                                genTest = "{\"operation\":\"declare_queue\",\"queue\":\"" + table + "\"}";
                                execute(connection, genTest);
                                System.out.println("Queue " + table + " created successfully.");
                            }
                        }
                    } catch (IOException e) {
                        // 如果 channel 已关闭，重新连接
                        System.err.println("Channel closed, reconnecting...");
                        connection = this.connect();
                        genTest = "{\"operation\":\"declare_queue\",\"queue\":\"" + table + "\"}";
                        execute(connection, genTest);
                        System.out.println("Queue " + table + " created after reconnection.");
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3 )) {
                    genTestValue = "executions_loop_message_" + insertIndex;
                    // set test query
                    query = "{\"operation\":\"publish\",\"queue\":\"" + table + "\",\"message\":\"" + genTestValue + "\"}";
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

    private static class RabbitMQConnection implements DatabaseConnection {
        private final Connection connection;
        private final Channel channel;

        public RabbitMQConnection(Connection connection, Channel channel) {
            this.connection = connection;
            this.channel = channel;
        }

        public Channel getChannel() {
            return channel;
        }

        @Override
        public void close() throws IOException {
            try {
                channel.close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            connection.close();
        }
    }

    private static class RabbitMQQueryResult implements QueryResult {
        private final String message;
        private final List<String> results;

        public RabbitMQQueryResult(String message) {
            this.message = message;
            this.results = null;
        }

        public RabbitMQQueryResult(List<String> results) {
            this.message = null;
            this.results = results;
        }

        @Override
        public ResultSet getResultSet() {
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
                .port(5672)
                .user("root")
                .password("YZ9F255pO8SjM522")
                .dbType("rabbitmq")
                .duration(3)
                .interval(1)
                .connectionCount(100)
                .testType("connectionstress")
                .build();
        RabbitMQTester tester = new RabbitMQTester(dbConfig);
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);

//        String queue = "executions_loop_queue";
//        String genTest = "{\"operation\":\"declare_queue\",\"queue\":\"" + queue + "\"}";
//        tester.execute(connection, genTest);
//        System.out.println("Queue " + queue + " created successfully.");
//
//        genTest = "{\"operation\":\"publish\",\"queue\":\"" + queue + "\",\"message\":\"Hello RabbitMQ!\"}";
//        tester.execute(connection, genTest);
//        System.out.println("Message published successfully.");
//
//        genTest = "{\"operation\":\"consume\",\"queue\":\"" + queue + "\"}";
//        QueryResult queryResult = tester.execute(connection, genTest);
//        RabbitMQQueryResult rmqQueryResult = (RabbitMQQueryResult)queryResult;
//        if (queryResult.hasResultSet()) {
//            List<String> rs = rmqQueryResult.getResults();
//            System.out.println(rs);
//            for (String r:rs) {
//                System.out.println(r);
//            }
//        }

//        genTest = "{\"operation\":\"get_message_count\",\"queue\":\"" + queue + "\"}";
//        queryResult = tester.execute(connection, genTest);
//        rmqQueryResult = (RabbitMQQueryResult)queryResult;
//        System.out.println(rmqQueryResult.getMessage());

        connection.close();
    }
}
