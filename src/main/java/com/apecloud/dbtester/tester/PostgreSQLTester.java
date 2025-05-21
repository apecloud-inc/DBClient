package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PostgreSQLTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "postgres";

    // 默认构造函数
    public PostgreSQLTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public PostgreSQLTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
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
            return new PostgreSQLConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to PostgreSQL database: " + e );
            System.err.println("Trying with database PostgreSQL.");
            try {
                return new PostgreSQLConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to PostgreSQL database: ", e2);
            }
        }
    }


    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        PostgreSQLConnection postgresConnection = (PostgreSQLConnection) connection;
        try {
            Statement statement = postgresConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new PostgreSQLQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        QueryResult executeResult;
        int executeUpdateCount;
        StringBuilder result_db = new StringBuilder();
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
                    // check if database exists
                    query_test = "SELECT datname FROM pg_database WHERE datname = '" + database + "';";
                    QueryResult queryResult = execute(connection, query_test);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        if (rs.getMetaData() != null) {
                            ResultSetMetaData metaData = rs.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    result_db.append(rs.getString(i));
                                }
                            }
                        }
                    }

                    if (result_db.toString().equals("")) {
                        // create test databases
                        System.out.println("create databases " + database);
                        query_test = "CREATE DATABASE " + database + ";";
                        System.out.println(query_test);
                        execute(connection, query_test);
                    }

                    if (!databaseConnection.equals(database)) {
                        System.out.println("reconnect connection " + database);
                        databaseConnection = database;
                        connection = this.connect();
                    }

                    if (table.equals("executions_loop_table")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        query_test = "DROP TABLE IF EXISTS " + table + ";";
                        System.out.println(query_test);
                        execute(connection, query_test);
                    }

                    // create test table with more field types
                    System.out.println("create table " + table);
                    query_test = "CREATE TABLE IF NOT EXISTS " + table + " ("
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
                            + "xml_col XML, "
                            + "enum_col VARCHAR(10) CHECK (enum_col IN ('Option1', 'Option2', 'Option3')), "
                            + "set_col VARCHAR(255) CHECK (set_col IN ('Value1', 'Value2', 'Value3')), "
                            + "int_array_col INTEGER[], "
                            + "text_array_col TEXT[], "
                            + "point_col POINT, "
                            + "line_col LINE, "
                            + "lseg_col LSEG, "
                            + "box_col BOX, "
                            + "path_col PATH, "
                            + "polygon_col POLYGON, "
                            + "circle_col CIRCLE, "
                            + "cidr_col CIDR, "
                            + "inet_col INET, "
                            + "macaddr_col MACADDR, "
                            + "macaddr8_col MACADDR8, "
                            + "bit_col BIT(8), "
                            + "bit_var_col BIT VARYING(8), "
                            + "varbit_col BIT VARYING(8), "
                            + "money_col MONEY, "
                            + "oid_col OID, "
                            + "regproc_col REGPROC, "
                            + "regprocedure_col REGPROCEDURE, "
                            + "regoper_col REGOPER, "
                            + "regoperator_col REGOPERATOR, "
                            + "regclass_col REGCLASS, "
                            + "regtype_col REGTYPE, "
                            + "regrole_col REGROLE, "
                            + "regnamespace_col REGNAMESPACE, "
                            + "regconfig_col REGCONFIG, "
                            + "regdictionary_col REGDICTIONARY "
                            + ");";
                    System.out.println(query_test);
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

                    // set test query
                    query = "INSERT INTO " + table + " (value, tinyint_col, smallint_col, "
                            + "integer_col, bigint_col, real_col, double_col, numeric_col, "
                            + "date_col, time_col, timestamp_col, timestamptz_col, interval_col, "
                            + "boolean_col, char_col, varchar_col, text_col, bytea_col, "
                            + "uuid_col, json_col, jsonb_col, xml_col, enum_col, set_col, "
                            + "int_array_col, text_array_col, point_col, line_col, lseg_col, "
                            + "box_col, path_col, polygon_col, circle_col, cidr_col, inet_col, "
                            + "macaddr_col, macaddr8_col, bit_col, bit_var_col, varbit_col, "
                            + "money_col, oid_col, regproc_col, regprocedure_col, regoper_col, "
                            + "regoperator_col, regclass_col, regtype_col, regrole_col, "
                            + "regnamespace_col, regconfig_col, regdictionary_col) "
                            + "VALUES ("
                            + "'" + gen_test_values + "', "
                            + random.nextInt(128) + ", " // TINYINT (using SMALLINT)
                            + random.nextInt(32768) + ", " // SMALLINT
                            + random.nextInt() + ", " // INTEGER
                            + random.nextLong() + ", " // BIGINT
                            + random.nextFloat() + ", " // REAL
                            + random.nextDouble() + ", " // DOUBLE PRECISION
                            + random.nextDouble() * 100 + ", " // NUMERIC
                            + "'" + new java.sql.Date(System.currentTimeMillis()) + "', " // DATE
                            + "'" + new java.sql.Time(System.currentTimeMillis()) + "', " // TIME
                            + "'" + new java.sql.Timestamp(System.currentTimeMillis()) + "', " // TIMESTAMP
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
                            + "'" + randomXml() + "', " // XML
                            + "'Option" + (random.nextInt(3) + 1) + "', " // ENUM
                            + "'Value" + (random.nextInt(3) + 1) + "', " // SET
                            + "ARRAY[" + random.nextInt(100) + ", " + random.nextInt(100) + ", " + random.nextInt(100) + "], " // INT ARRAY
                            + "ARRAY['" + randomString(10) + "', '" + randomString(10) + "', '" + randomString(10) + "'], " // TEXT ARRAY
                            + "'(" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + ")', " // POINT
                            + "'{" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "}', " // LINE
                            + "'[(" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + ")]', " // LSEG
                            + "'((" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "))', " // BOX
                            + "'((" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "))', " // PATH
                            + "'((" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "), (" + random.nextDouble() * 100 + ", " + random.nextDouble() * 100 + "))', " // POLYGON
                            + "'" + get_circle_string() + "', " // CIRCLE
                            + "'192.168." + random.nextInt(256) + ".0" + "/24', " // CIDR
                            + "'192.168." + random.nextInt(256) + "." + random.nextInt(256) + "', " // INET
                            + "'08:00:2b:01:02:03', " // MACADDR
                            + "'08:00:2b:01:02:03:04:05', " // MACADDR8
                            + "B'10101010', " // BIT(8)
                            + "B'10101010', " // BIT VARYING(8)
                            + "B'10101010', " // VARBIT(8)
                            + "'$" + random.nextDouble() * 1000 + "', " // MONEY
                            + random.nextInt() + ", " // OID
                            + "'acos', " // REGPROC
                            + "abs(1), " // REGPROCEDURE
                            + "'#-', " // REGOPER
                            + "+1, " // REGOPERATOR
                            + "'pg_class', " // REGCLASS
                            + "'integer', " // REGTYPE
                            + "'postgres', " // REGROLE
                            + "'pg_catalog', " // REGNAMESPACE
                            + "'simple', " // REGCONFIG
                            + "'english_stem' " // REGDICTIONARY
                            + ");";
                    if (gen_test_query == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    gen_test_query = 3;
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
                    insert_index = insert_index - 1;
                    executionError = true;
                }
            } catch (IOException e) {
                System.out.println(e);
                failedExecutions++;
                insert_index = insert_index - 1;
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

    private String get_circle_string() {
        Random random = new Random();

        // 生成随机圆心坐标和半径
        double centerX = random.nextDouble() * 100;
        double centerY = random.nextDouble() * 100;
        double radius = random.nextDouble() * 10; // 假设最大半径为 10

        String circleValue = String.format("<(%f, %f), %f>", centerX, centerY, radius);
        return circleValue;
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

    // Helper method to generate random JSON
    private String randomJson() {
        Random random = new Random();
        return "{\"key1\": \"" + randomString(10) + "\", \"key2\": " + random.nextInt(100) + "}";
    }

    // Helper method to generate random XML
    private String randomXml() {
        Random random = new Random();
        return "<root><element>" + randomString(10) + "</element><value>" + random.nextInt(100) + "</value></root>";
    }

    private static class PostgreSQLConnection implements DatabaseConnection {
        private final Connection connection;

        PostgreSQLConnection(Connection connection) {
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

    public static class PostgreSQLQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        PostgreSQLQueryResult(ResultSet resultSet, int updateCount) {
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
            .port(1640)
            .user("postgres")
            .password("Mj10827fwU")
            .dbType("postgresql")
            .duration(10)
            .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
            .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
            .build();
        PostgreSQLTester tester = new PostgreSQLTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
