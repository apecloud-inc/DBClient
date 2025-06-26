package com.apecloud.dbtester.tester;

// Elasticsearch Core Clients
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseConnection;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.commons.QueryResult;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

// Elasticsearch Query & Search
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;

// Elasticsearch Mapping & Index
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;

// JSON Processing
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.json.Json;
import jakarta.json.stream.JsonParser;

// Java Utils
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

public class ElasticSearchTester implements DatabaseTester {
    private final DBConfig dbConfig;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private List<RestClient> connections = new ArrayList<>();
    private ElasticsearchClient esClient;
    private RestClient restClient;

    public ElasticSearchTester() {
        this.dbConfig = null;
    }

    public ElasticSearchTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 创建 ES 连接
    private void createESConnection() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String serverUrl = String.format("http://%s:%d",
            dbConfig.getHost(),
            dbConfig.getPort());

        restClient = RestClient.builder(HttpHost.create(serverUrl))
            .setHttpClientConfigCallback(httpClientBuilder -> {
                if (dbConfig.getUser() != null && dbConfig.getPassword() != null) {
                    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(dbConfig.getUser(), dbConfig.getPassword())
                    );
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
                return httpClientBuilder;
            })
            .build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient,
            new JacksonJsonpMapper()
        );

        esClient = new ElasticsearchClient(transport);
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        createESConnection();
        return new ElasticSearchConnection(restClient);
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String operation) throws IOException {
        String[] parts = operation.split(":");
        String operationType = parts[0].toLowerCase();
        String indexName = parts.length > 1 ? parts[1] : "test_index";
        String queryParams = parts.length > 2 ? String.join(":", Arrays.asList(parts).subList(2, parts.length)) : "";
        switch (operationType) {
            case "create_index":
                return createIndex(indexName);
            case "delete_index":
                return deleteIndex(indexName);
            case "check_index":
                return checkIndex(indexName);
            case "insert":
                return insertDocument(indexName, queryParams);
            case "search":
                return searchDocuments(indexName, queryParams);
            case "get":
                return getDocument(indexName, queryParams);
            case "delete_doc":
                return deleteDocument(indexName, queryParams);
            case "update":
                return updateDocument(indexName, queryParams);
            default:
                throw new IOException("Unsupported operation: " + operationType);
        }
    }

    private QueryResult insertDocument(String indexName, String documentJson) throws IOException {
        if (documentJson.isEmpty()) {
            documentJson = createSampleDocument();
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode document = (ObjectNode) mapper.readTree(documentJson);

        IndexResponse response = esClient.index(i -> i
            .index(indexName)
            .document(document)
        );

        return new ElasticSearchQueryResult(true,
            String.format("Document inserted with ID: %s", response.id()));
    }

    private String createSampleDocument() {
        return "{"
            + "\"id\": \"" + System.currentTimeMillis() + "\","
            + "\"name\": \"Test Document\","
            + "\"value\": " + (int)(Math.random() * 100)
            + "}";
    }

    private QueryResult searchDocuments(String indexName, String searchQuery) throws IOException {
        Query query;
        if (searchQuery.isEmpty()) {
            // 如果没有指定查询条件，使用 match_all
            query = new Query.Builder()
                .matchAll(new MatchAllQuery.Builder().build())
                .build();
        } else {
            // 解析查询条件
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode queryNode = (ObjectNode) mapper.readTree(searchQuery);
            query = Query.of(q -> q
                .match(m -> m
                    .field(queryNode.get("field").asText())
                    .query(queryNode.get("value").asText())
                )
            );
        }

        SearchResponse<ObjectNode> response = esClient.search(s -> s
                .index(indexName)
                .query(query),
            ObjectNode.class
        );

        StringBuilder results = new StringBuilder();
        results.append("Found ").append(response.hits().total().value()).append(" documents:\n");

        for (Hit<ObjectNode> hit : response.hits().hits()) {
            results.append("ID: ").append(hit.id())
                   .append(", Score: ").append(hit.score())
                   .append(", Source: ").append(hit.source())
                   .append("\n");
        }

        return new ElasticSearchQueryResult(true, results.toString());
    }

    private QueryResult getDocument(String indexName, String documentId) throws IOException {
        GetResponse<ObjectNode> response = esClient.get(g -> g
                .index(indexName)
                .id(documentId),
            ObjectNode.class
        );

        if (response.found()) {
            return new ElasticSearchQueryResult(true,
                String.format("Document found: %s", response.source()));
        } else {
            return new ElasticSearchQueryResult(false,
                String.format("Document not found with ID: %s", documentId));
        }
    }

    private QueryResult deleteDocument(String indexName, String documentId) throws IOException {
        DeleteResponse response = esClient.delete(d -> d
            .index(indexName)
            .id(documentId)
        );

        return new ElasticSearchQueryResult(true,
            String.format("Document deleted with ID: %s", documentId));
    }

    private QueryResult updateDocument(String indexName, String updateInfo) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode updateNode = (ObjectNode) mapper.readTree(updateInfo);
        String documentId = updateNode.get("id").asText();
        ObjectNode doc = (ObjectNode) updateNode.get("doc");

        UpdateResponse<ObjectNode> response = esClient.update(u -> u
                .index(indexName)
                .id(documentId)
                .doc(doc),
            ObjectNode.class
        );

        return new ElasticSearchQueryResult(true,
            String.format("Document updated with ID: %s", documentId));
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
            ElasticSearchConnection esConnection = (ElasticSearchConnection) connection;

            switch (testType.toLowerCase()) {
                case "connectionstress":
                    result.append(connectionStress(
                        dbConfig.getConnectionCount(),
                        dbConfig.getDuration()
                    ));
                    break;

                case "search":
                    String searchQuery = dbConfig.getQuery();
                    if (searchQuery == null || searchQuery.isEmpty()) {
                        throw new IllegalArgumentException("Search query not specified in DBConfig");
                    }
                    // 执行搜索操作
                    QueryResult searchResult = execute(connection, String.format(
                        "SEARCH %s", searchQuery
                    ));
                    result.append(formatSearchResult(searchResult));
                    break;

                case "index":
                    String indexOperation = dbConfig.getQuery();
                    if (indexOperation == null || indexOperation.isEmpty()) {
                        throw new IllegalArgumentException("Index operation not specified");
                    }
                    // 执行索引操作
                    QueryResult indexResult = execute(connection, String.format(
                        "INDEX %s", indexOperation
                    ));
                    result.append(formatIndexResult(indexResult));
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

    @Override
    public String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table) {
        StringBuilder result = new StringBuilder();
        QueryResult executeResult;
        int executeUpdateCount;
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
        String genTestValue;
        ElasticSearchQueryResult queryResult;

        // Check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_index";
        }

        System.out.println("Execution loop start: " + query);
        while (System.currentTimeMillis() < endTime) {
            insertIndex++;
            long currentTime = System.currentTimeMillis();

            if (currentTime - lastOutputTime >= interval * 1000) {
                outputPassTime += interval;
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
                    // Check if index exists, if not create it
                    queryResult = (ElasticSearchQueryResult) execute(connection, "check_index:" + table);
                    if (!queryResult.getMessage().contains("true")) {
                        System.out.println("Index " + table + " does not exist. Creating index...");
                        execute(connection, "create_index:" + table);
                        System.out.println("Index " + table + " created successfully.");
                    } else {
                        System.out.println("Index " + table + " already exists.");
                        if (table.equals("executions_loop_index")) {
                            // Delete index
                            System.out.println("Delete index " + table);
                            execute(connection, "delete_index:" + table);
                            System.out.println("Index " + table + " deleted successfully.");

                            System.out.println("Create index " + table);
                            execute(connection, "create_index:" + table);
                            System.out.println("Index " + table + " created successfully.");
                        }
                    }

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    // Set test query
                    genTestValue = "executions_loop_" + insertIndex;
                    String document = String.format(
                            "{\"id\": \"%d\", \"name\": \"%s\", \"value\": \"%s\"}",
                            System.currentTimeMillis(),
                            genTestValue,
                            insertIndex
                    );
                    query = String.format("insert:%s:%s", table, document);
                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start: " + query);
                    }
                    genTestQuery = 3;
                }

                executeResult = execute(connection, query);
                executeUpdateCount = executeResult.getUpdateCount();
                if (executeUpdateCount != -1) {
                    successfulExecutions++;
                    if (executionError) {
                        recoveryTime = System.currentTimeMillis();
                        java.util.Date recoveryDate = new java.util.Date(recoveryTime);
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
            } catch (Exception e) {
                System.out.println("Execution loop failed: " + e.getMessage());
                failedExecutions++;
                insertIndex--;

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


    private String formatSearchResult(QueryResult result) {
        if (result instanceof ElasticSearchQueryResult) {
            ElasticSearchQueryResult esResult = (ElasticSearchQueryResult) result;
            return String.format(
                "Search Results:\nTotal Hits: %d\nDocuments: %s\n",
                esResult.getTotalHits(),
                esResult.getDocuments()
            );
        }
        return "No results found";
    }

    private String formatIndexResult(QueryResult result) {
        if (result instanceof ElasticSearchQueryResult) {
            ElasticSearchQueryResult esResult = (ElasticSearchQueryResult) result;
            return String.format(
                "Index Operation Result:\nSuccess: %b\nDocument ID: %s\nVersion: %d\n",
                esResult.isSuccessful(),
                esResult.getDocumentId(),
                esResult.getVersion()
            );
        }
        return "Index operation failed";
    }

    private QueryResult createIndex(String indexName) throws IOException {
        // 定义示例映射
        String mappings = "{\n" +
                "  \"properties\" : {\n" +
                "    \"id\" : { \"type\" : \"keyword\" },\n" +
                "    \"name\" : { \"type\" : \"text\" },\n" +
                "    \"value\" : { \"type\" : \"long\" }\n" +
                "  }\n" +
                "}";

        JsonParser parser = Json.createParser(new StringReader(mappings));
        CreateIndexRequest request = new CreateIndexRequest.Builder()
            .index(indexName)
            .mappings(TypeMapping._DESERIALIZER.deserialize(
                parser,
                esClient._transport().jsonpMapper()
            ))
            .build();

        esClient.indices().create(request);
        return new ElasticSearchQueryResult(true, "Index created: " + indexName);
    }

    private QueryResult deleteIndex(String indexName) throws IOException {
        esClient.indices().delete(builder -> builder.index(indexName));
        return new ElasticSearchQueryResult(true, "Index deleted: " + indexName);
    }

    private QueryResult checkIndex(String indexName) throws IOException {
        boolean exists = esClient.indices().exists(builder -> 
            builder.index(indexName)
        ).value();
        return new ElasticSearchQueryResult(true, "Index exists: " + exists);
    }

    @Override
    public String bench(DatabaseConnection connection, String operation, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                try {
                    execute(connection, operation);
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

        result.append("Benchmark completed with ")
              .append(iterations)
              .append(" iterations and ")
              .append(concurrency)
              .append(" concurrency");
        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        for (int i = 0; i < connections; i++) {
            try {
                String serverUrl = String.format("http://%s:%d",
                    dbConfig.getHost(),
                    dbConfig.getPort());

                RestClient client = RestClient.builder(HttpHost.create(serverUrl)).build();
                this.connections.add(client);

                Request request = new Request("GET", "/_cluster/health");
                request.addParameter("pretty", "true");
                client.performRequest(request);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                releaseConnections();
            }
        }
        return String.format("Created %d connections", connections);
    }

    @Override
    public void releaseConnections() {
        for (RestClient client : connections) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        connections.clear();
    }

    private static class ElasticSearchConnection implements DatabaseConnection {
        private final RestClient client;

        ElasticSearchConnection(RestClient client) {
            this.client = client;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    private static class ElasticSearchQueryResult implements QueryResult {
        private final boolean success;
        private final String message;
        private final long totalHits;
        private final List<String> documents;
        private final String documentId;
        private final long version;
        private final Map<String, Object> metadata;

        // 基础构造函数
        ElasticSearchQueryResult(boolean success, String message) {
            this(success, message, 0, Collections.emptyList(), null, 0, Collections.emptyMap());
        }

        // 完整构造函数
        ElasticSearchQueryResult(boolean success,
                String message,
                long totalHits,
                List<String> documents,
                String documentId,
                long version,
                Map<String, Object> metadata) {
            this.success = success;
            this.message = message;
            this.totalHits = totalHits;
            this.documents = documents != null ? documents : Collections.emptyList();
            this.documentId = documentId;
            this.version = version;
            this.metadata = metadata != null ? metadata : Collections.emptyMap();
        }

        // 搜索结果构造器
        public static ElasticSearchQueryResult forSearch(long totalHits, List<String> documents) {
            return new ElasticSearchQueryResult(true,
                    "Search completed successfully",
                    totalHits,
                    documents,
                    null,
                    0,
                    Collections.emptyMap());
        }

        // 索引操作结果构造器
        public static ElasticSearchQueryResult forIndex(String documentId, long version) {
            return new ElasticSearchQueryResult(true,
                    "Document indexed successfully",
                    0,
                    Collections.emptyList(),
                    documentId,
                    version,
                    Collections.emptyMap());
        }

        // 错误结果构造器
        public static ElasticSearchQueryResult error(String errorMessage) {
            return new ElasticSearchQueryResult(false,
                    errorMessage,
                    0,
                    Collections.emptyList(),
                    null,
                    0,
                    Collections.emptyMap());
        }

        @Override
        public boolean hasResultSet() {
            return success;
        }

        @Override
        public java.sql.ResultSet getResultSet() {
            return null; // ES 不使用 JDBC ResultSet
        }

        @Override
        public int getUpdateCount() {
            return 0;
        }

        @Override
        public String toString() {
            return message;
        }

        // Elasticsearch 特定的方法
        public boolean isSuccessful() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public long getTotalHits() {
            return totalHits;
        }

        public List<String> getDocuments() {
            return Collections.unmodifiableList(documents);
        }

        public String getDocumentId() {
            return documentId;
        }

        public long getVersion() {
            return version;
        }

        public Map<String, Object> getMetadata() {
            return Collections.unmodifiableMap(metadata);
        }

        // 用于检查特定操作类型的方法
        public boolean isSearchResult() {
            return totalHits > 0 || !documents.isEmpty();
        }

        public boolean isIndexResult() {
            return documentId != null && version > 0;
        }

        public boolean hasError() {
            return !success;
        }

        // 用于构建复杂结果的内部构建器
        public static class Builder {
            private boolean success = true;
            private String message = "";
            private long totalHits = 0;
            private List<String> documents = new ArrayList<>();
            private String documentId;
            private long version = 0;
            private Map<String, Object> metadata = new HashMap<>();

            public Builder success(boolean success) {
                this.success = success;
                return this;
            }

            public Builder message(String message) {
                this.message = message;
                return this;
            }

            public Builder totalHits(long totalHits) {
                this.totalHits = totalHits;
                return this;
            }

            public Builder documents(List<String> documents) {
                this.documents = new ArrayList<>(documents);
                return this;
            }

            public Builder documentId(String documentId) {
                this.documentId = documentId;
                return this;
            }

            public Builder version(long version) {
                this.version = version;
                return this;
            }

            public Builder addMetadata(String key, Object value) {
                this.metadata.put(key, value);
                return this;
            }

            public ElasticSearchQueryResult build() {
                return new ElasticSearchQueryResult(success,
                        message,
                        totalHits,
                        documents,
                        documentId,
                        version,
                        metadata);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(9200)
                .user("elastic")
                .password("4335f9Zxyf")
                .dbType("elasticsearch")
                .duration(10)
                .interval(1)
                .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .build();
        ElasticSearchTester tester = new ElasticSearchTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();

        Thread.sleep(2000);
        dbConfig = new DBConfig.Builder()
            .host("127.0.0.1")
            .testType("query")
            .query("search")
            .dbType("elasticsearch")
            .port(9200)
            .user("elastic")
            .password("4335f9Zxyf")
            .build();

        tester = new ElasticSearchTester(dbConfig);
        connection = tester.connect();

        // 测试索引操作
        // System.out.println(tester.execute(connection, "create_index:test_index"));

        // 测试文档插入
        // QueryResult qr = tester.execute(connection, "insert:test_index");
        // System.out.println(qr);
        // String[] qrs = qr.toString().split(":");
        // String documentId = qrs[1].trim();

        // 测试文档搜索（match_all查询）
        System.out.println(tester.execute(connection, "search:executions_loop_index"));

        // 测试条件搜索
        // System.out.println(tester.execute(connection, "get:test_index:"+documentId));

        // 清理测试数据
        // System.out.println(tester.execute(connection, "delete_index:test_index"));

        connection.close();

//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("127.0.0.1")
//                .port(9200)
//                .user("elastic")
//                .password("4335f9Zxyf")
//                .testType("connectionstress")
//                .duration(60)
//                .table("default")
//                .org("elastic")
//                .database("elastic")
//                .build();
//
//        ElasticSearchTester tester = new ElasticSearchTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        String result = tester.connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
//        System.out.println(result);
//        connection.close();
    }
}