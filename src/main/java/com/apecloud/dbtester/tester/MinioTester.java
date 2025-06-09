package com.apecloud.dbtester.tester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MinioTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public MinioTester() {
        this.dbConfig = null;
    }

    public MinioTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            MinioClient minioClient = MinioClient.builder()
                    .endpoint(dbConfig.getHost(), dbConfig.getPort(), false)
                    .credentials(dbConfig.getUser(), dbConfig.getPassword())
                    .build();

            return new MinioConnection(minioClient);
        } catch (Exception e) {
            throw new IOException("Failed to connect to MinIO", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        MinioConnection minioConn = (MinioConnection) connection;
        try {
            Map<String, Object> queryMap = parseJsonQuery(query);
            String operation = (String) queryMap.get("operation");
            String bucketName = (String) queryMap.get("bucket");
            switch (operation.toLowerCase()) {
                case "create_bucket":
                    return handleCreateBucket(minioConn, bucketName);
                case "delete_bucket":
                    return handleDeleteBucket(minioConn, bucketName);
                case "list_bucket":
                    return handleListBuckets(minioConn);
                case "upload_file":
                    return handleUploadFile(minioConn, bucketName, queryMap);
                case "download_file":
                    return handleDownloadFile(minioConn, bucketName, queryMap);
                case "write_data":
                    return handleWriteData(minioConn, bucketName, queryMap);
                case "empty_bucket":
                    return handleEmptyBucket(minioConn, bucketName);
                default:
                    throw new IOException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute MinIO operation", e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        return null;
    }

    private Map<String, Object> parseJsonQuery(String query) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(query, new TypeReference<HashMap<String, Object>>() {});
    }

    private QueryResult handleCreateBucket(MinioConnection conn, String bucketName) throws Exception {
        boolean found = conn.getMinioClient().bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        if (!found) {
            conn.getMinioClient().makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            System.out.println("Bucket " + bucketName + " created successfully");
            return new MinioQueryResult("Bucket " + bucketName + " created successfully");
        } else {
            return new MinioQueryResult("Bucket " + bucketName + " already exists");
        }
    }

    private QueryResult handleDeleteBucket(MinioConnection conn, String bucketName) throws Exception {
        conn.getMinioClient().removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
        System.out.println("Bucket " + bucketName + " deleted successfully");
        return new MinioQueryResult("Bucket " + bucketName + " deleted successfully");
    }

    private QueryResult handleListBuckets(MinioConnection conn) throws Exception {
        List<Bucket> buckets = conn.getMinioClient().listBuckets();
        List<String> bucketNames = new ArrayList<>();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.creationDate() + ", " + bucket.name());
            bucketNames.add(bucket.name());
        }
        return new MinioQueryResult(bucketNames);
    }

    private QueryResult handleUploadFile(MinioConnection conn, String bucketName, Map<String, Object> queryMap) throws Exception {
        String objectName = (String) queryMap.get("object_name");
        String filePath = (String) queryMap.get("file_path");

        File file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + filePath);
        }

        conn.getMinioClient().putObject(PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(new FileInputStream(file), file.length(), -1)
                .build());

        return new MinioQueryResult("File uploaded successfully to bucket=" + bucketName + ", object=" + objectName);
    }

    private QueryResult handleDownloadFile(MinioConnection conn, String bucketName, Map<String, Object> queryMap) throws Exception {
        String objectName = (String) queryMap.get("object_name");
        String downloadPath = (String) queryMap.get("download_path");

        File downloadFile = new File(downloadPath);
        Files.createDirectories(Paths.get(downloadFile.getParent()));

        try (InputStream stream = conn.getMinioClient().getObject(GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build())) {
            Files.copy(stream, Paths.get(downloadPath));
        }

        return new MinioQueryResult("File downloaded successfully from bucket=" + bucketName + ", object=" + objectName + " to " + downloadPath);
    }

    private QueryResult handleWriteData(MinioConnection conn, String bucketName, Map<String, Object> queryMap) throws Exception {
        String objectName = (String) queryMap.get("object_name");
        String content = (String) queryMap.get("content");

        if (content == null || content.isEmpty()) {
            throw new IllegalArgumentException("Content cannot be null or empty");
        }

        ByteArrayInputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        conn.getMinioClient().putObject(PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(stream, content.length(), -1)
                .contentType("text/plain")
                .build());

        return new MinioQueryResult("Data written successfully to bucket=" + bucketName + ", object=" + objectName);
    }


    private QueryResult handleEmptyBucket(MinioConnection conn, String bucketName) throws Exception {
        MinioClient minioClient = conn.getMinioClient();
        Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucketName)
                .recursive(true)
                .build());

        for (Result<Item> result : results) {
            Item item = result.get();
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(item.objectName())
                    .build());
            System.out.println("Deleted object: " + item.objectName());
        }

        return new MinioQueryResult("Bucket " + bucketName + " has been emptied.");
    }

    @Override
    public String connectionStress(int connections, int duration) {
        String queryJson="{\"operation\": \"list_bucket\"}";
        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                execute(connection, queryJson);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                releaseConnections();
            }
        }

        return null;
    }


    @Override
    public void releaseConnections() {
        MinioConnection minioConn;
        for (DatabaseConnection connection : connections) {
            try {
                connection.close();
                minioConn = (MinioConnection) connection;
                minioConn.close();
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
            String testQuery = "{\"operation\":\"create_bucket\",\"bucket\":\"test-bucket\"}";

            switch (testType) {
                case "query":
                    execute(connection, testQuery);
                    results.append("Basic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    results.append("Connection stress test:\n")
                            .append(connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration()))
                            .append("\n");
                    break;

                case "benchmark":
                    results.append("Benchmark test:\n")
                            .append("Benchmark not implemented yet\n");
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
        MinioConnection minioConn = null;
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
        Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTest;
        String genTestValue;
        ByteArrayInputStream stream = null;

        // Check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = dbConfig.getBucket();
            if (table == null || table.equals("")) {
                table = "executions-loop-bucket";
            }
        }

        System.out.println("Execution loop start: " + query);
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
                minioConn = (MinioConnection) connection;
                if (genTestQuery == 1) {
                    // Check if bucket exists, if not create it
                    boolean bucketExists = minioConn.getMinioClient().bucketExists(BucketExistsArgs.builder().bucket(table).build());
                    if (!bucketExists) {
                        System.out.println("Bucket " + table + " does not exist. Creating bucket...");
                        minioConn.getMinioClient().makeBucket(MakeBucketArgs.builder().bucket(table).build());
                        System.out.println("Bucket " + table + " created successfully.");
                    } else {
                        System.out.println("Bucket " + table + " already exists.");
                        if (table.equals("executions-loop-bucket")) {

                            // delete bucket object
                            System.out.println("Deleted bucket object");
                            MinioClient minioClient = minioConn.getMinioClient();
                            Iterable<Result<Item>> objectResults = minioClient.listObjects(ListObjectsArgs.builder()
                                    .bucket(table)
                                    .recursive(true)
                                    .build());

                            for (Result<Item> objectResult : objectResults) {
                                Item item = objectResult.get();
                                minioClient.removeObject(RemoveObjectArgs.builder()
                                        .bucket(table)
                                        .object(item.objectName())
                                        .build());
                                System.out.println("Bucket " + table + " object " + item.objectName() + "  deleted successfully.");
                            }

                            // delete bucket
                            System.out.println("Delete bucket " + table);
                            minioConn.getMinioClient().removeBucket(RemoveBucketArgs.builder().bucket(table).build());
                            System.out.println("Bucket " + table + " deleted successfully.");

                            System.out.println("Create bucket " + table);
                            minioConn.getMinioClient().makeBucket(MakeBucketArgs.builder().bucket(table).build());
                            System.out.println("Bucket " + table + " created successfully.");
                        }
                    }
                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    genTestValue =  "executions_loop_" + insertIndex;
                    // Set test query
                    query = genTestValue;
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                stream = new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8));
                minioConn.getMinioClient().putObject(PutObjectArgs.builder()
                        .bucket(table)
                        .object("executions_loop")
                        .stream(stream, query.length(), -1)
                        .contentType("text/plain")
                        .build());
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
            } catch (Exception e) {
                System.out.println("Execution loop failed: " + e.getMessage());
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } finally {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
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

    private static class MinioConnection implements DatabaseConnection {
        private final MinioClient minioClient;

        MinioConnection(MinioClient minioClient) {
            this.minioClient = minioClient;
        }

        public MinioClient getMinioClient() {
            return minioClient;
        }
        @Override
        public void close() throws IOException {
            try {
                if (minioClient instanceof AutoCloseable) {
                    ((AutoCloseable) minioClient).close();
                }
            } catch (Exception e) {
                throw new IOException("Failed to close MinIO client", e);
            }
        }
    }

    private static class MinioQueryResult implements QueryResult {
        private final String message;
        private final List<String> results;

        MinioQueryResult(String message) {
            this.message = message;
            this.results = null;
        }

        MinioQueryResult(List<String> results) {
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
//                .dbType("minio")
//                .testType("connectionstress")
//                .host("127.0.0.1")
//                .port(9000)
//                .user("root")
//                .password("8IkOu64ALX19202y")
//                .connectionCount(10)
//                .duration(1)
//                .build();
//
//        MinioTester tester = new MinioTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();


        System.setProperty("io.minio.net.Client.SHUTDOWN_TIMEOUT_MILLIS", "100");
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9000)
                .user("root")
                .password("8IkOu64ALX19202y")
                .dbType("minio")
                .duration(10)
                .interval(1)
                .testType("executionloop")
                .build();
        MinioTester tester = new MinioTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
