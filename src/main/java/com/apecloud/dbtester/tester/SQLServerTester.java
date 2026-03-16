package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SQLServerTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "master";

    // 默认构造函数
    public SQLServerTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public SQLServerTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("SQL Server JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=true;trustServerCertificate=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=true;trustServerCertificate=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new SQLServerConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to SQLServer database: " + e );
            System.err.println("Trying with database SQLServer.");
            try {
                return new SQLServerConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to SQLServer: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        SQLServerConnection sqlServerConnection = (SQLServerConnection) connection;
        try {
            Statement statement = sqlServerConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new SQLServerQueryResult(statement.getResultSet(), statement.getUpdateCount());
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e, e);
        }
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        StringBuilder result = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

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

        result.append("Benchmark completed with ").append(iterations).append(" iterations and ").append(concurrency).append(" concurrency");
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
                failedConnections++;
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
        // 释放所有连接
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
        return TestExecutor.executeTest(this, dbConfig);
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
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        java.util.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTest;
        String genTestValue;

        byte[] blobData = new byte[10];
        byte[] binaryData = new byte[10];
        byte[] varbinaryData = new byte[255];
        int randomYear;

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals("")) || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.isEmpty()) {
            database = "executions_loop";
        }
        if (table == null || table.isEmpty()) {
            table = "executions_loop_table";
        }

        System.out.println("Execution loop start: " + query);

        while (System.currentTimeMillis() < endTime) {
            insertIndex = insertIndex + 1;
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
                    connection = this.connect(); // 重新连接
                }

                if (genTestQuery == 1) {
                    // create test databases
                    System.out.println("create databases " + database);
                    genTest = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '" + database + "') CREATE DATABASE " + database;
                    System.out.println(genTest);
                    execute(connection, genTest);

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        genTest = "IF OBJECT_ID('" + database + ".." + table + "', 'U') IS NOT NULL DROP TABLE " + database + ".." + table;
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    // create test table with more field types
                    System.out.println("create table " + table);
                    genTest = "IF OBJECT_ID('" + database + ".." + table + "', 'U') IS NULL CREATE TABLE " + database + ".." + table + " ("
                            + "id INT IDENTITY(1,1) PRIMARY KEY, "
                            + "value VARCHAR(255), "
                            + "tinyint_col TINYINT, "
                            + "smallint_col SMALLINT, "
                            + "int_col INT, "
                            + "bigint_col BIGINT, "
                            + "float_col FLOAT, "
                            + "double_col FLOAT(53), "
                            + "decimal_col DECIMAL(10,2), "
                            + "date_col DATE, "
                            + "time_col TIME, "
                            + "datetime_col DATETIME, "
                            + "timestamp_col DATETIME2, "
                            + "year_col CHAR(4), "
                            + "char_col CHAR(10), "
                            + "text_col TEXT, "
                            + "blob_col VARBINARY(MAX), "
                            + "binary_col BINARY(10), "
                            + "varbinary_col VARBINARY(255), "
                            + "enum_col VARCHAR(10), "
                            + "set_col VARCHAR(50) "
                            + ")";
                    execute(connection, genTest);

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.isEmpty())) || genTestQuery == 3) {
                    Random random = new Random();

                    // 生成随机值
                    genTestValue = "executions_loop_test_" + insertIndex;
                    random.nextBytes(blobData);
                    random.nextBytes(binaryData);
                    random.nextBytes(varbinaryData);
                    randomYear = random.nextInt(255) + 1901;

                    // 构造 INSERT 查询
                    query = "INSERT INTO " + database + ".." + table + " (value, tinyint_col, smallint_col, " +
                            "int_col, bigint_col, float_col, double_col, decimal_col, date_col, time_col, " +
                            "datetime_col, timestamp_col, year_col, char_col, text_col, blob_col, binary_col, " +
                            "varbinary_col, enum_col, set_col) VALUES ("
                            + "'" + genTestValue + "', "
                            + random.nextInt(256) + ", "  // TINYINT (0-255)
                            + random.nextInt(32768) + ", " // SMALLINT
                            + random.nextInt() + ", "
                            + random.nextLong() + ", "
                            + random.nextDouble() + ", "
                            + random.nextDouble() + ", "
                            + (random.nextDouble() * 100) + ", "
                            + "'" + new java.sql.Date(System.currentTimeMillis()) + "', "
                            + "'" + new java.sql.Time(System.currentTimeMillis()) + "', "
                            + "'" + new java.sql.Timestamp(System.currentTimeMillis()) + "', "
                            + "SYSUTCDATETIME(), "
                            + "'" + randomYear + "', "
                            + "'" + randomString(10) + "', "
                            + "'" + randomString(255) + "', "
                            + "0x" + bytesToHex(blobData) + ", "
                            + "0x" + bytesToHex(binaryData) + ", "
                            + "0x" + bytesToHex(varbinaryData) + ", "
                            + "'Option" + (random.nextInt(3) + 1) + "', "
                            + "'Value" + (random.nextInt(3) + 1) + "'"
                            + ")";

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
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getMessage());
                failedExecutions++;
                insertIndex = insertIndex - 1;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.util.Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            }
        }

        System.out.println("[ " + duration + "s ] executions total: " + (successfulExecutions + failedExecutions)
                + " successful: " + successfulExecutions + " failed: " + failedExecutions
                + " disconnect: " + disconnectCounts);

        releaseConnections(); // 释放所有连接

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

    private static class SQLServerConnection implements DatabaseConnection {
        private final Connection connection;

        SQLServerConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close SQL Server connection", e);
            }
        }
    }

    public static class SQLServerQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        SQLServerQueryResult(ResultSet resultSet, int updateCount) {
            this.resultSet = resultSet;
            this.updateCount = updateCount;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return resultSet;
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return resultSet != null;
        }
    }

    // Helper method to generate random string
    private String randomString(int length) {
        return TestUtils.randomString(length);
    }

    // Helper method to convert bytes to hex string
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        // 使用示例
//        DBConfig dbConfig = new DBConfig.Builder()
//            .host("localhost")
//            .port(1433)  // SQL Server 默认端口
//            .database("master")  // SQL Server 默认数据库
//            .user("sa")  // SQL Server 默认管理员账户
//            .password("3Ff5yb3Qgb")
//            .dbType("sqlserver")
//            .testType("query")
//            .query("SELECT * FROM users")
//            .build();
//        SQLServerTester tester = new SQLServerTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//        QueryResult result = tester.execute(connection, "SELECT * FROM sysusers");
//        System.out.println(result);
//        connection.close();

//        DBConfig dbConfig = new DBConfig.Builder()
//                .host("localhost")
//                .port(1433)
//                .database("master")
//                .user("sa")
//                .password("3Ff5yb3Qgb")
//                .dbType("sqlserver")
//                .testType("connectionstress")
//                .duration(60)
//                .connectionCount(32767)
//                .build();
//        SQLServerTester tester = new SQLServerTester(dbConfig);
//        String result = TestExecutor.executeTest(tester, dbConfig);
//        System.out.println(result);

        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(1433)
                .user("sa")
                .password("3Ff5yb3Qgb")
                .dbType("sqlserver")
                .duration(10)
                .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
                .build();
        SQLServerTester tester = new SQLServerTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
