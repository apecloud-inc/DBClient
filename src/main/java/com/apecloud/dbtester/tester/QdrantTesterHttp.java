package com.apecloud.dbtester.tester;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QdrantTesterHttp implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public QdrantTesterHttp() {
        this.dbConfig = null;
    }

    public QdrantTesterHttp(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        OkHttpClient client = new OkHttpClient();
        return new QdrantConnection(client, dbConfig);
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        QdrantConnection qdrantConnection = (QdrantConnection) connection;
        try {
            Request request = new Request.Builder()
                    .url(qdrantConnection.buildQueryUrl())
                    .build();

            Response response = qdrantConnection.client.newCall(request).execute();

            if (response.isSuccessful()) {
                ResultSet result = null;
                if ( response != null && response.body() != null) {
                    String resultBody = response.body().string();
                    result = convertJsonToResultSet(resultBody);
                }
                return new QdrantQueryResult(result);
            } else {
                throw new IOException("Failed to execute query: " + response.code());
            }
        } catch (Exception e) {
            throw new IOException("Failed to execute query", e);
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
        double duration = (endTime - startTime) / 1000.0;

        result.append("Benchmark completed:\n")
                .append("Total iterations: ").append(iterations).append("\n")
                .append("Concurrency level: ").append(concurrency).append("\n")
                .append("Total time: ").append(duration).append(" seconds\n")
                .append("Queries per second: ").append(iterations / duration);

        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        int successfulConnections = 0;

        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);

                // 执行健康检查请求以验证连接有效性
                execute(connection, "");

                successfulConnections++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(duration * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        result.append("Connection stress test results:\n")
                .append("Attempted connections: ").append(connections).append("\n")
                .append("Successful connections: ").append(successfulConnections).append("\n")
                .append("Test duration: ").append(duration).append(" seconds");

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
        DatabaseConnection connection = null;
        StringBuilder results = new StringBuilder();
        String testType = dbConfig.getTestType();

        try {
            connection = connect();

            switch (testType) {
                case "query":
                    QueryResult result = execute(connection, dbConfig.getQuery());
                    ResultSet resultSet = result.getResultSet();
                    while (resultSet.next()) {
                        if (resultSet.getString("result") != null) {
                            results.append(resultSet.getString("result"));
                        }
                    }
                    results.append("\nBasic query test: SUCCESS\n");
                    break;

                case "connectionstress":
                    connectionStress(dbConfig.getConnectionCount(), dbConfig.getDuration());
                    break;

                case "benchmark":
                    results.append(bench(connection, dbConfig.getQuery(), 1000, 10)).append("\n");
                    break;

                default:
                    results.append("Unknown test type\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
        return null;
    }

    private static class QdrantConnection implements DatabaseConnection {
        private final OkHttpClient client;
        private final DBConfig dbConfig;

        QdrantConnection(OkHttpClient client, DBConfig dbConfig) {
            this.client = client;
            this.dbConfig = dbConfig;
        }

        @Override
        public void close() throws IOException {
            // No need to close OkHttpClient explicitly
        }

        public String buildHealthCheckUrl() {
            return "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/health";
        }

        public String buildQueryUrl() {
            String queryUrl = "http://" + dbConfig.getHost() + ":" + dbConfig.getPort() ;
            String testType = dbConfig.getTestType();
            String testQuery = dbConfig.getQuery();
            if (testType.toLowerCase().equals("query") && testQuery != null && !testQuery.isEmpty()) {
                queryUrl = queryUrl + "/" + dbConfig.getQuery();
            }
            return queryUrl;
        }
    }

    private static class QdrantQueryResult implements QueryResult {
        private final ResultSet response;

        QdrantQueryResult(ResultSet response) {
            this.response = response;
        }

        @Override
        public ResultSet getResultSet() {
            return response;
        }

        @Override
        public int getUpdateCount() {
            return 0;
        }

        @Override
        public boolean hasResultSet() {
            return response != null;
        }
    }

    public static ResultSet convertJsonToResultSet(String json) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        QdrantResult response = objectMapper.readValue(json, QdrantResult.class);

        List<Map<String, Object>> rows = new ArrayList<>();

        // Convert the HealthCheckResponse to a list of maps
        Map<String, Object> row = new HashMap<>();
        if (response != null) {
            if (response.getStatus() != null) {
                row.put("status", response.getStatus());
            }

            if (response.getTime() != 0) {
                row.put("time", response.getTime());
            }

            if (response.getResult() != null) {
                row.put("result", response.getResult().toString());
            }

            if (response.getTitle() != null) {
                row.put("title", response.getTitle());
            }

            if (response.getVersion() != null) {
                row.put("version", response.getVersion());
            }
        }
        rows.add(row);

        return new MockResultSet(rows);
    }

    public static class QdrantResult {
        @JsonProperty("result")
        private Map<String, Object> result;

        @JsonProperty("status")
        private String status;

        @JsonProperty("time")
        private double time;

        @JsonProperty("title")
        private String title;

        @JsonProperty("version")
        private String version;

        // Getters and Setters
        public Map<String, Object> getResult() {
            return result;
        }

        public void setResult(Map<String, Object> result) {
            this.result = result;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public double getTime() {
            return time;
        }

        public void setTime(double time) {
            this.time = time;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    public static class MockResultSet implements ResultSet {
        private List<Map<String, Object>> rows;
        private int currentIndex = -1;

        public MockResultSet(List<Map<String, Object>> rows) {
            this.rows = rows;
        }

        @Override
        public boolean next() throws SQLException {
            currentIndex++;
            return currentIndex < rows.size();
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
            Map<String, Object> row = rows.get(currentIndex);
            Object value = row.get(columnLabel);
            if (value == null) {
                return null;
            }
            return type.cast(value);
        }

        @Override
        public boolean wasNull() throws SQLException {
            return false;
        }

        @Override
        public String getString(int i) throws SQLException {
            return null;
        }

        @Override
        public boolean getBoolean(int i) throws SQLException {
            return false;
        }

        @Override
        public byte getByte(int i) throws SQLException {
            return 0;
        }

        @Override
        public short getShort(int i) throws SQLException {
            return 0;
        }

        @Override
        public int getInt(int i) throws SQLException {
            return 0;
        }

        @Override
        public long getLong(int i) throws SQLException {
            return 0;
        }

        @Override
        public float getFloat(int i) throws SQLException {
            return 0;
        }

        @Override
        public double getDouble(int i) throws SQLException {
            return 0;
        }

        @Override
        public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
            return null;
        }

        @Override
        public byte[] getBytes(int i) throws SQLException {
            return new byte[0];
        }

        @Override
        public Date getDate(int i) throws SQLException {
            return null;
        }

        @Override
        public Time getTime(int i) throws SQLException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(int i) throws SQLException {
            return null;
        }

        @Override
        public InputStream getAsciiStream(int i) throws SQLException {
            return null;
        }

        @Override
        public InputStream getUnicodeStream(int i) throws SQLException {
            return null;
        }

        @Override
        public InputStream getBinaryStream(int i) throws SQLException {
            return null;
        }

        @Override
        public String getString(String columnLabel) throws SQLException {
            return (String) getObject(columnLabel, String.class);
        }

        @Override
        public int getInt(String columnLabel) throws SQLException {
            return (int) getObject(columnLabel, Integer.class);
        }

        @Override
        public long getLong(String s) throws SQLException {
            return 0;
        }

        @Override
        public float getFloat(String s) throws SQLException {
            return 0;
        }

        @Override
        public double getDouble(String columnLabel) throws SQLException {
            return (double) getObject(columnLabel, Double.class);
        }

        @Override
        public BigDecimal getBigDecimal(String s, int i) throws SQLException {
            return null;
        }

        @Override
        public byte[] getBytes(String s) throws SQLException {
            return new byte[0];
        }

        @Override
        public Date getDate(String s) throws SQLException {
            return null;
        }

        @Override
        public Time getTime(String s) throws SQLException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(String s) throws SQLException {
            return null;
        }

        @Override
        public InputStream getAsciiStream(String s) throws SQLException {
            return null;
        }

        @Override
        public InputStream getUnicodeStream(String s) throws SQLException {
            return null;
        }

        @Override
        public InputStream getBinaryStream(String s) throws SQLException {
            return null;
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public String getCursorName() throws SQLException {
            return null;
        }

        @Override
        public boolean getBoolean(String columnLabel) throws SQLException {
            return (boolean) getObject(columnLabel, Boolean.class);
        }

        @Override
        public byte getByte(String s) throws SQLException {
            return 0;
        }

        @Override
        public short getShort(String s) throws SQLException {
            return 0;
        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            // Implement as needed
            return null;
        }

        @Override
        public Object getObject(int i) throws SQLException {
            return null;
        }

        @Override
        public Object getObject(String s) throws SQLException {
            return null;
        }

        @Override
        public int findColumn(String s) throws SQLException {
            return 0;
        }

        @Override
        public Reader getCharacterStream(int i) throws SQLException {
            return null;
        }

        @Override
        public Reader getCharacterStream(String s) throws SQLException {
            return null;
        }

        @Override
        public BigDecimal getBigDecimal(int i) throws SQLException {
            return null;
        }

        @Override
        public BigDecimal getBigDecimal(String s) throws SQLException {
            return null;
        }

        @Override
        public boolean isBeforeFirst() throws SQLException {
            return false;
        }

        @Override
        public boolean isAfterLast() throws SQLException {
            return false;
        }

        @Override
        public boolean isFirst() throws SQLException {
            return false;
        }

        @Override
        public boolean isLast() throws SQLException {
            return false;
        }

        @Override
        public void beforeFirst() throws SQLException {

        }

        @Override
        public void afterLast() throws SQLException {

        }

        @Override
        public boolean first() throws SQLException {
            return false;
        }

        @Override
        public boolean last() throws SQLException {
            return false;
        }

        @Override
        public int getRow() throws SQLException {
            return 0;
        }

        @Override
        public boolean absolute(int i) throws SQLException {
            return false;
        }

        @Override
        public boolean relative(int i) throws SQLException {
            return false;
        }

        @Override
        public boolean previous() throws SQLException {
            return false;
        }

        @Override
        public void setFetchDirection(int i) throws SQLException {

        }

        @Override
        public int getFetchDirection() throws SQLException {
            return 0;
        }

        @Override
        public void setFetchSize(int i) throws SQLException {

        }

        @Override
        public int getFetchSize() throws SQLException {
            return 0;
        }

        @Override
        public int getType() throws SQLException {
            return 0;
        }

        @Override
        public int getConcurrency() throws SQLException {
            return 0;
        }

        @Override
        public boolean rowUpdated() throws SQLException {
            return false;
        }

        @Override
        public boolean rowInserted() throws SQLException {
            return false;
        }

        @Override
        public boolean rowDeleted() throws SQLException {
            return false;
        }

        @Override
        public void updateNull(int i) throws SQLException {

        }

        @Override
        public void updateBoolean(int i, boolean b) throws SQLException {

        }

        @Override
        public void updateByte(int i, byte b) throws SQLException {

        }

        @Override
        public void updateShort(int i, short i1) throws SQLException {

        }

        @Override
        public void updateInt(int i, int i1) throws SQLException {

        }

        @Override
        public void updateLong(int i, long l) throws SQLException {

        }

        @Override
        public void updateFloat(int i, float v) throws SQLException {

        }

        @Override
        public void updateDouble(int i, double v) throws SQLException {

        }

        @Override
        public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

        }

        @Override
        public void updateString(int i, String s) throws SQLException {

        }

        @Override
        public void updateBytes(int i, byte[] bytes) throws SQLException {

        }

        @Override
        public void updateDate(int i, Date date) throws SQLException {

        }

        @Override
        public void updateTime(int i, Time time) throws SQLException {

        }

        @Override
        public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

        }

        @Override
        public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {

        }

        @Override
        public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {

        }

        @Override
        public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {

        }

        @Override
        public void updateObject(int i, Object o, int i1) throws SQLException {

        }

        @Override
        public void updateObject(int i, Object o) throws SQLException {

        }

        @Override
        public void updateNull(String s) throws SQLException {

        }

        @Override
        public void updateBoolean(String s, boolean b) throws SQLException {

        }

        @Override
        public void updateByte(String s, byte b) throws SQLException {

        }

        @Override
        public void updateShort(String s, short i) throws SQLException {

        }

        @Override
        public void updateInt(String s, int i) throws SQLException {

        }

        @Override
        public void updateLong(String s, long l) throws SQLException {

        }

        @Override
        public void updateFloat(String s, float v) throws SQLException {

        }

        @Override
        public void updateDouble(String s, double v) throws SQLException {

        }

        @Override
        public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {

        }

        @Override
        public void updateString(String s, String s1) throws SQLException {

        }

        @Override
        public void updateBytes(String s, byte[] bytes) throws SQLException {

        }

        @Override
        public void updateDate(String s, Date date) throws SQLException {

        }

        @Override
        public void updateTime(String s, Time time) throws SQLException {

        }

        @Override
        public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {

        }

        @Override
        public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {

        }

        @Override
        public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {

        }

        @Override
        public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {

        }

        @Override
        public void updateObject(String s, Object o, int i) throws SQLException {

        }

        @Override
        public void updateObject(String s, Object o) throws SQLException {

        }

        @Override
        public void insertRow() throws SQLException {

        }

        @Override
        public void updateRow() throws SQLException {

        }

        @Override
        public void deleteRow() throws SQLException {

        }

        @Override
        public void refreshRow() throws SQLException {

        }

        @Override
        public void cancelRowUpdates() throws SQLException {

        }

        @Override
        public void moveToInsertRow() throws SQLException {

        }

        @Override
        public void moveToCurrentRow() throws SQLException {

        }

        @Override
        public Statement getStatement() throws SQLException {
            return null;
        }

        @Override
        public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public Ref getRef(int i) throws SQLException {
            return null;
        }

        @Override
        public Blob getBlob(int i) throws SQLException {
            return null;
        }

        @Override
        public Clob getClob(int i) throws SQLException {
            return null;
        }

        @Override
        public Array getArray(int i) throws SQLException {
            return null;
        }

        @Override
        public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public Ref getRef(String s) throws SQLException {
            return null;
        }

        @Override
        public Blob getBlob(String s) throws SQLException {
            return null;
        }

        @Override
        public Clob getClob(String s) throws SQLException {
            return null;
        }

        @Override
        public Array getArray(String s) throws SQLException {
            return null;
        }

        @Override
        public Date getDate(int i, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public Date getDate(String s, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public Time getTime(int i, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public Time getTime(String s, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
            return null;
        }

        @Override
        public URL getURL(int i) throws SQLException {
            return null;
        }

        @Override
        public URL getURL(String s) throws SQLException {
            return null;
        }

        @Override
        public void updateRef(int i, Ref ref) throws SQLException {

        }

        @Override
        public void updateRef(String s, Ref ref) throws SQLException {

        }

        @Override
        public void updateBlob(int i, Blob blob) throws SQLException {

        }

        @Override
        public void updateBlob(String s, Blob blob) throws SQLException {

        }

        @Override
        public void updateClob(int i, Clob clob) throws SQLException {

        }

        @Override
        public void updateClob(String s, Clob clob) throws SQLException {

        }

        @Override
        public void updateArray(int i, Array array) throws SQLException {

        }

        @Override
        public void updateArray(String s, Array array) throws SQLException {

        }

        @Override
        public RowId getRowId(int i) throws SQLException {
            return null;
        }

        @Override
        public RowId getRowId(String s) throws SQLException {
            return null;
        }

        @Override
        public void updateRowId(int i, RowId rowId) throws SQLException {

        }

        @Override
        public void updateRowId(String s, RowId rowId) throws SQLException {

        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public void updateNString(int i, String s) throws SQLException {

        }

        @Override
        public void updateNString(String s, String s1) throws SQLException {

        }

        @Override
        public void updateNClob(int i, NClob nClob) throws SQLException {

        }

        @Override
        public void updateNClob(String s, NClob nClob) throws SQLException {

        }

        @Override
        public NClob getNClob(int i) throws SQLException {
            return null;
        }

        @Override
        public NClob getNClob(String s) throws SQLException {
            return null;
        }

        @Override
        public SQLXML getSQLXML(int i) throws SQLException {
            return null;
        }

        @Override
        public SQLXML getSQLXML(String s) throws SQLException {
            return null;
        }

        @Override
        public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {

        }

        @Override
        public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {

        }

        @Override
        public String getNString(int i) throws SQLException {
            return null;
        }

        @Override
        public String getNString(String s) throws SQLException {
            return null;
        }

        @Override
        public Reader getNCharacterStream(int i) throws SQLException {
            return null;
        }

        @Override
        public Reader getNCharacterStream(String s) throws SQLException {
            return null;
        }

        @Override
        public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {

        }

        @Override
        public void updateClob(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateClob(String s, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateNClob(int i, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateNClob(String s, Reader reader, long l) throws SQLException {

        }

        @Override
        public void updateNCharacterStream(int i, Reader reader) throws SQLException {

        }

        @Override
        public void updateNCharacterStream(String s, Reader reader) throws SQLException {

        }

        @Override
        public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateCharacterStream(int i, Reader reader) throws SQLException {

        }

        @Override
        public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateCharacterStream(String s, Reader reader) throws SQLException {

        }

        @Override
        public void updateBlob(int i, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateBlob(String s, InputStream inputStream) throws SQLException {

        }

        @Override
        public void updateClob(int i, Reader reader) throws SQLException {

        }

        @Override
        public void updateClob(String s, Reader reader) throws SQLException {

        }

        @Override
        public void updateNClob(int i, Reader reader) throws SQLException {

        }

        @Override
        public void updateNClob(String s, Reader reader) throws SQLException {

        }

        @Override
        public <T> T getObject(int i, Class<T> aClass) throws SQLException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> aClass) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> aClass) throws SQLException {
            return false;
        }

        // Implement other methods as needed
    }


    public static void main(String[] args) {
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .testType("connectionstress")
                .port(6333)
                .duration(10)
                .connectionCount(10)
                .dbType("qdrant")
                .build();
         dbConfig = new DBConfig.Builder()
                .host("localhost")
                .testType("query")
                .dbType("qdrant")
                .query("collections/collection_mytest")
                .port(6333)
                .build();
        DatabaseTester tester = TesterFactory.createTester(dbConfig);

        try {
            DatabaseConnection connection = tester.connect();
            QueryResult result = tester.execute(connection, dbConfig.getQuery());
            if (result.hasResultSet()) {
                ResultSet rs = result.getResultSet();

                while (rs.next()) {
                    if (rs.getString("result") != null) {
                        System.out.println(rs.getString("result"));
                    }
                }
            }

            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}