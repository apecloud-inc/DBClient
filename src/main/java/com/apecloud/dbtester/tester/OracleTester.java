package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OracleTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "ORCLCDB";

    // 默认构造函数
    public OracleTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public OracleTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle JDBC Driver not found, please try again..", e);
        }

        // Oracle的连接URL格式：jdbc:oracle:thin:@hostname:port:SID
        String url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                dbConfig.getDatabase());

        String url2 = String.format("jdbc:oracle:thin:@%s:%d:%s",
                dbConfig.getHost(),
                dbConfig.getPort(),
                databaseConnection);

        Properties props = new Properties();
        props.setProperty("user", dbConfig.getUser());
        props.setProperty("password", dbConfig.getPassword());
        if (dbConfig.getUser().equalsIgnoreCase("sys")) {
            props.setProperty("internal_logon", "sysdba"); // or "sysoper"
        }

        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
            url = url2;
        }

        try {
            return new OracleConnection(DriverManager.getConnection(url, props));
        } catch (SQLException e) {
            System.err.println("Failed to connect to Oracle database: " + e );
            System.err.println("Trying with database Oracle.");
            try {
                return new OracleConnection(DriverManager.getConnection(url2, props));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to Oracle database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        OracleConnection oracleConnection = (OracleConnection) connection;
        try {
            Statement statement = oracleConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new OracleQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        QueryResult queryResult;
        String table_count = "0";
        int max_id;

        if (database == null || database.equals("")) {
            database = dbConfig.getUser();
        }

        // check gen test query
        if (query == null || query.equals("") || (table != null && !table.equals(""))) {
            gen_test_query = 1;
        }

        if (table == null || table.equals("")) {
            table = "EXECUTIONS_LOOP_TABLE";
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
                    query_test = "SELECT COUNT(*) FROM DBA_TABLES WHERE OWNER = '" + database.toUpperCase() + "' AND TABLE_NAME ='" + table.toUpperCase() + "'";
                    queryResult = execute(connection, query_test);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        while (rs.next()) {
                            String count = rs.getString(1);
                            System.out.println("table_count: " + count);
                            table_count = count;
                        }
                    }

                    if (table.equals("EXECUTIONS_LOOP_TABLE") && !table_count.equals("0")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        query_test = "DROP TABLE " + database + "." + table;
                        execute(connection, query_test);
                        table_count = "0";
                    }else if (!table_count.equals("0")){
                        query_test = "SELECT MAX(ID) FROM " + database + "." + table;
                        queryResult = execute(connection, query_test);
                        if (queryResult.hasResultSet()) {
                            ResultSet rs = queryResult.getResultSet();
                            while (rs.next()) {
                                String max_id_str = rs.getString(1);
                                System.out.println("max_id: " + max_id_str);
                                max_id = Integer.parseInt(max_id_str);
                                insert_index += max_id;
                            }
                        }
                    }

                    if (table_count.equals("0")) {
                        // create test table
                        System.out.println("create table " + table);
                        query_test = "CREATE TABLE " + database + "." + table + "(ID NUMBER PRIMARY KEY, VALUE VARCHAR2(255))";
                        execute(connection, query_test);
                    }
                    gen_test_query = 2;
                }

                if ((gen_test_query == 2 && (query == null || query.equals("")) || gen_test_query == 3 )) {
                    gen_test_values = "executions_loop_test_" + insert_index;
                    // set test query
                    query = "INSERT INTO " + database + "." + table + " (ID, VALUE) VALUES (" + insert_index + ", '" + gen_test_values + "')";
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
            } catch (SQLException e) {
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


    private static class OracleConnection implements DatabaseConnection {
        private final Connection connection;

        OracleConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Oracle connection", e);
            }
        }
    }

    public static class OracleQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        OracleQueryResult(ResultSet resultSet, int updateCount) {
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
        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(1521)
                .user("c##testuser")
                .password("testpassword")
                .dbType("oracle")
                .duration(10)
                .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .testType("executionloop")
            .table("test_table")
                .build();
        OracleTester tester = new OracleTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
