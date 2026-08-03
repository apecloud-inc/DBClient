package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class He3SQLTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "postgres";

    // 默认构造函数
    public He3SQLTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public He3SQLTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // connect() method using DBConfig
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:postgresql://%s:%d/%s?useSSL=false",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:postgresql://%s:%d/%s?useSSL=false",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new He3SQLConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to He3SQL database: " + e );
            System.err.println("Trying with database He3SQL.");
            try {
                return new He3SQLConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to He3SQL database: ", e2);
            }
        }
    }


    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        He3SQLConnection postgresConnection = (He3SQLConnection) connection;
        try {
            Statement statement = postgresConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new He3SQLQueryResult(statement.getResultSet(), statement.getUpdateCount());
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e, e);
        }
    }

    @Override
        public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        return BenchmarkUtils.run(iterations, concurrency, this::connect, c -> executeBenchmark(c, query));
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
        StringBuilder resultDb = new StringBuilder();
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

        byte[] blobData = new byte[10];
        byte[] binaryData = new byte[10];
        byte[] varbinaryData = new byte[255];

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals("")) || (table != null && !table.equals(""))) {
            genTestQuery = 1;
        }

        if (database == null || database.equals("")) {
            database = "executions_loop";
        }

        if (table == null || table.equals("")) {
            table = "executions_loop_table";
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
                    // check if database exists
                    genTest = "SELECT datname FROM pg_database WHERE datname = '" + database + "';";
                    QueryResult queryResult = execute(connection, genTest);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        if (rs.getMetaData() != null) {
                            ResultSetMetaData metaData = rs.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    resultDb.append(rs.getString(i));
                                }
                            }
                        }
                    }

                    if (resultDb.toString().equals("")) {
                        // create test databases
                        System.out.println("create databases " + database);
                        genTest = "CREATE DATABASE " + database + ";";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    if (!databaseConnection.equals(database)) {
                        System.out.println("reconnect connection " + database);
                        databaseConnection = database;
                        connection = this.connect();
                    }

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        genTest = "DROP TABLE IF EXISTS " + table + ";";
                        System.out.println(genTest);
                        execute(connection, genTest);
                    }

                    // create test table with more field types
                    System.out.println("create table " + table);
                    genTest = "CREATE TABLE IF NOT EXISTS " + table + " ("
                            + "id SERIAL PRIMARY KEY, "
                            + "value TEXT, "
                            + "tinyint_col SMALLINT, " // PostgreSQL does not have TINYINT, using SMALLINT instead
                            + "smallint_col SMALLINT, "
                            + "integer_col INTEGER, "
                            + "bigint_col BIGINT, "
                            + "real_col REAL, "
                            + "double_col DOUBLE PRECISION, "
                            + "numeric_col NUMERIC(10, 2), "
                            + "date_col DATE, "
                            + "time_col TIME, "
                            + "timestamp_col TIMESTAMP, "
                            + "timestamptz_col TIMESTAMP WITH TIME ZONE, "
                            + "interval_col INTERVAL, "
                            + "boolean_col BOOLEAN, "
                            + "char_col CHAR(10), "
                            + "varchar_col VARCHAR(255), "
                            + "text_col TEXT, "
                            + "bytea_col BYTEA, "
                            + "uuid_col UUID, "
                            + "json_col JSON, "
                            + "jsonb_col JSONB, "
                            + "enum_col VARCHAR(10) CHECK (enum_col IN ('Option1', 'Option2', 'Option3')), "
                            + "set_col VARCHAR(255) CHECK (set_col IN ('Value1', 'Value2', 'Value3')), "
                            + "int_array_col INTEGER[], "
                            + "text_array_col TEXT[], "
                            + "cidr_col CIDR, "
                            + "inet_col INET, "
                            + "macaddr_col MACADDR, "
                            + "macaddr8_col MACADDR8, "
                            + "bit_col BIT(8), "
                            + "bit_var_col BIT VARYING(8), "
                            + "varbit_col BIT VARYING(8), "
                            + "money_col MONEY "
                            + ");";
                    System.out.println(genTest);
                    execute(connection, genTest);

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3)) {
                    Random random = new Random();

                    // Generate random values
                    genTestValue = "executions_loop_test_" + insertIndex;

                    random.nextBytes(blobData);
                    random.nextBytes(binaryData);
                    random.nextBytes(varbinaryData);

                    // set test query
                    query = "INSERT INTO " + table + " (value, tinyint_col, smallint_col, "
                            + "integer_col, bigint_col, real_col, double_col, numeric_col, "
                            + "date_col, time_col, timestamp_col, timestamptz_col, interval_col, "
                            + "boolean_col, char_col, varchar_col, text_col, bytea_col, "
                            + "uuid_col, json_col, jsonb_col, enum_col, set_col, "
                            + "int_array_col, text_array_col, cidr_col, inet_col, "
                            + "macaddr_col, macaddr8_col, bit_col, bit_var_col, varbit_col, "
                            + "money_col) "
                            + "VALUES ("
                            + "'" + genTestValue + "', "
                            + random.nextInt(128) + ", " // TINYINT (using SMALLINT)
                            + random.nextInt(32768) + ", " // SMALLINT
                            + random.nextInt() + ", " // INTEGER
                            + random.nextLong() + ", " // BIGINT
                            + random.nextFloat() + ", " // REAL
                            + random.nextDouble() + ", " // DOUBLE PRECISION
                            + random.nextDouble() * 100 + ", " // NUMERIC
                            + "'" + new Date(System.currentTimeMillis()) + "', " // DATE
                            + "'" + new Time(System.currentTimeMillis()) + "', " // TIME
                            + "'" + new Timestamp(System.currentTimeMillis()) + "', " // TIMESTAMP
                            + "CURRENT_TIMESTAMP, " // TIMESTAMP WITH TIME ZONE
                            + "'" + random.nextInt(24) + " hours " + random.nextInt(60) + " minutes " + random.nextInt(60) + " seconds', " // INTERVAL
                            + (random.nextBoolean() ? "TRUE" : "FALSE") + ", " // BOOLEAN
                            + "'" + randomString(10) + "', " // CHAR
                            + "'" + randomString(255) + "', " // VARCHAR
                            + "'" + randomString(255) + "', " // TEXT
                            + "decode('" + bytesToHex(blobData) + "', 'hex'), " // BYTEA
                            + "'" + UUID.randomUUID() + "', " // UUID
                            + "'" + randomJson() + "', " // JSON
                            + "'" + randomJson() + "', " // JSONB
                            + "'Option" + (random.nextInt(3) + 1) + "', " // ENUM
                            + "'Value" + (random.nextInt(3) + 1) + "', " // SET
                            + "ARRAY[" + random.nextInt(100) + ", " + random.nextInt(100) + ", " + random.nextInt(100) + "], " // INT ARRAY
                            + "ARRAY['" + randomString(10) + "', '" + randomString(10) + "', '" + randomString(10) + "'], " // TEXT ARRAY
                            + "'192.168." + random.nextInt(256) + ".0" + "/24', " // CIDR
                            + "'192.168." + random.nextInt(256) + "." + random.nextInt(256) + "', " // INET
                            + "'08:00:2b:01:02:03', " // MACADDR
                            + "'08:00:2b:01:02:03:04:05', " // MACADDR8
                            + "B'10101010', " // BIT(8)
                            + "B'10101010', " // BIT VARYING(8)
                            + "B'10101010', " // VARBIT(8)
                            + "'$" + random.nextDouble() * 1000 + "'" // MONEY
                            + ");";
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
                System.out.println(e);
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
            } catch (SQLException e) {
                System.out.println(e);
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

    // Helper method to generate random JSON
    private String randomJson() {
        Random random = new Random();
        return "{\"key1\": \"" + randomString(10) + "\", \"key2\": " + random.nextInt(100) + "}";
    }

    /**
     * Execute query for benchmark mode and close Statement/ResultSet immediately.
     */
    private void executeBenchmark(DatabaseConnection connection, String query) throws IOException {
        He3SQLConnection conn = (He3SQLConnection) connection;
        try (Statement stmt = conn.connection.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new IOException("Failed to execute query: " + e, e);
        }
    }


    private static class He3SQLConnection implements DatabaseConnection {
        private final Connection connection;

        He3SQLConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close PostgreSQL connection", e);
            }
        }
    }

    public static class He3SQLQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        He3SQLQueryResult(ResultSet resultSet, int updateCount) {
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

    public static void main(String[] args) throws IOException {
        // 使用示例
        DBConfig dbConfig = new DBConfig.Builder()
            .host("localhost")
            .port(5432)
            .user("postgres")
            .password("q9N4SAdV%5")
            .dbType("postgresql")
            .duration(10)
            .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
            .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
            .build();
        He3SQLTester tester = new He3SQLTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
