package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MySQLTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "mysql";

    // 增加默认构造函数
    public MySQLTester() {
        this.dbConfig = null;
    }

    // 增加接收 DBConfig 的构造函数
    public MySQLTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 增加使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new MySQLConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to MySQL database: " + e );
            System.err.println("Trying with database MySQL.");
            try {
                return new MySQLConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to MySQL database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        MySQLConnection mysqlConnection = (MySQLConnection) connection;
        try {
            Statement statement = mysqlConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new MySQLQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        // 建立多个连接
        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
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

        int insert_index = 0;
        int gen_test_query = 0;
        String query_test;
        String gen_test_values;

        byte[] blobData = new byte[10];
        byte[] binaryData = new byte[10];
        byte[] varbinaryData = new byte[255];
        int randomYear;

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals("")) || (table != null && !table.equals(""))) {
            gen_test_query = 1;
        }

        if (database == null || database.equals("")) {
            database = "executions_loop";
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_table";
        }

        System.out.println("Execution loop start:" + query);
        while (System.currentTimeMillis() < endTime) {
            insert_index = insert_index + 1;
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

                if (gen_test_query == 1) {
                    // create test databases
                    System.out.println("create databases " + database);
                    query_test = "CREATE DATABASE IF NOT EXISTS " + database + ";";
                    execute(connection, query_test);

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        query_test = "DROP TABLE IF EXISTS " + database + "." + table + ";";
                        execute(connection, query_test);
                    }

                    // create test table with more field types
                    System.out.println("create table " + table);
                    query_test = "CREATE TABLE IF NOT EXISTS " + database + "." + table + " ("
                            + "id INT PRIMARY KEY AUTO_INCREMENT, "
                            + "value VARCHAR(255), "
                            + "tinyint_col TINYINT, "
                            + "smallint_col SMALLINT, "
                            + "mediumint_col MEDIUMINT, "
                            + "int_col INT, "
                            + "bigint_col BIGINT, "
                            + "float_col FLOAT, "
                            + "double_col DOUBLE, "
                            + "decimal_col DECIMAL(10, 2), "
                            + "date_col DATE, "
                            + "time_col TIME, "
                            + "datetime_col DATETIME, "
                            + "timestamp_col TIMESTAMP, "
                            + "year_col YEAR, "
                            + "char_col CHAR(10), "
                            + "text_col TEXT, "
                            + "blob_col BLOB, "
                            + "binary_col BINARY(10), "
                            + "varbinary_col VARBINARY(255), "
                            + "enum_col ENUM('Option1', 'Option2', 'Option3'), "
                            + "set_col SET('Value1', 'Value2', 'Value3') "
                            + ");";
                    execute(connection, query_test);

                    gen_test_query = 2;
                }

                if ((gen_test_query == 2 && (query == null || query.equals("")) || gen_test_query == 3)) {
                    Random random = new Random();

                    // Generate random values
                    gen_test_values = "executions_loop_test_" + insert_index;

                    random.nextBytes(blobData);
                    random.nextBytes(binaryData);
                    random.nextBytes(varbinaryData);
                    randomYear = random.nextInt(255) + 1901;

                    // set test query
                    query = "INSERT INTO " + database + "." + table + " (value, tinyint_col, smallint_col, "
                            + "mediumint_col, int_col, bigint_col, float_col, double_col, decimal_col, "
                            + "date_col, time_col, datetime_col, timestamp_col, year_col, char_col, text_col, "
                            + "blob_col, binary_col, varbinary_col, enum_col, set_col) "
                            + "VALUES ("
                            + "'" + gen_test_values + "', "
                            + random.nextInt(128) + ", " // TINYINT
                            + random.nextInt(32768) + ", " // SMALLINT
                            + random.nextInt(8388608) + ", " // MEDIUMINT
                            + random.nextInt() + ", " // INT
                            + random.nextLong() + ", " // BIGINT
                            + random.nextFloat() + ", " // FLOAT
                            + random.nextDouble() + ", " // DOUBLE
                            + random.nextDouble() * 100 + ", " // DECIMAL
                            + "'" + new java.sql.Date(System.currentTimeMillis()) + "', " // DATE
                            + "'" + new java.sql.Time(System.currentTimeMillis()) + "', " // TIME
                            + "'" + new java.sql.Timestamp(System.currentTimeMillis()) + "', " // DATETIME
                            + "CURRENT_TIMESTAMP, " // TIMESTAMP
                            + randomYear + ", " // YEAR
                            + "'" + randomString(10) + "', " // CHAR
                            + "'" + randomString(255) + "', " // TEXT
                            + "UNHEX(REPLACE('" + bytesToHex(blobData) + "', ' ', '')), " // BLOB
                            + "UNHEX(REPLACE('" + bytesToHex(binaryData) + "', ' ', '')), " // BINARY
                            + "UNHEX(REPLACE('" + bytesToHex(varbinaryData) + "', ' ', '')), " // VARBINARY
                            + "'Option" + (random.nextInt(3) + 1) + "', " // ENUM
                            + "'Value" + (random.nextInt(3) + 1) + "' " // SET
                            + ");";
                    if (gen_test_query == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    gen_test_query = 3;
                }

                execute(connection, query);
                successfulExecutions++;
                if (executionError) {
                    recoveryTime = System.currentTimeMillis();
                    Date recoveryDate = new Date(recoveryTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    System.out.println("[" + sdf.format(recoveryDate) + "] Connection successfully recovered!");
                    errorToRecoveryTime = recoveryTime - errorTime;
                    System.out.println("The connection was restored in " + errorToRecoveryTime + " milliseconds.");
                    executionError=false;
                }
            } catch (IOException e) {
                System.out.println(e);
                failedExecutions++;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError=true;
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
                successfulExecutions+failedExecutions,
                successfulExecutions,
                failedExecutions,
                disconnectCounts);
    }

    private static class MySQLConnection implements DatabaseConnection {
        private final Connection connection;

        MySQLConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close MySQL connection", e);
            }
        }
    }

    public static class MySQLQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        MySQLQueryResult(ResultSet resultSet, int updateCount) {
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
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
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
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
            .host("localhost")
            .port(3306)
            .user("root")
            .password("l8Ose3g5TJ5489L0")
            .dbType("mysql")
            .duration(10)
            .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
            .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
            .build();
        MySQLTester tester = new MySQLTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}