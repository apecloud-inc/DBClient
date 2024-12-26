package com.apecloud.dbtester.tester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.messages.Bucket;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MinioTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
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

    @Override
    public String connectionStress(int connections, int duration) {
        int successfulConnections = 0;
        int failedConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
                String query_json="{\"operation\": \"create_bucket\", \"bucket\": \"my-bucket-" + i + "\"}";
                execute(connection, query_json);

                query_json="{\"operation\": \"list_bucket\"}";
                execute(connection, query_json);

                query_json="{\"operation\": \"delete_bucket\", \"bucket\": \"my-bucket-" + i + "\"}";
                execute(connection, query_json);

                successfulConnections++;
            } catch (IOException e) {
                e.printStackTrace();
                failedConnections++;
            }finally {
                releaseConnections();
            }
        }

        return String.format("Successful connections: %d\n" +
                             "Failed connections: %d",
                             successfulConnections, failedConnections);
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
            // MinioClient does not have a close method, but we can leave it open
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
        DBConfig dbConfig = new DBConfig.Builder()
                .dbType("minio")
                .testType("connectionstress")
                .host("127.0.0.1")
                .port(9000)
                .user("root")
                .password("7V23k04Rd6x21tAG")
                .connectionCount(100)
                .duration(1)
                .build();

        MinioTester tester = new MinioTester(dbConfig);
        String testResults = tester.executeTest();
        System.out.println(testResults);
    }
}
