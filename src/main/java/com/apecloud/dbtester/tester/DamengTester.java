package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DamengTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;

    public DamengTester() {
        this.dbConfig = null;
    }

    public DamengTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            Class.forName("dm.jdbc.driver.DmDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Dameng JDBC Driver not found, please try again..", e);
        }

        String url = String.format("jdbc:dm://%s:%d",
                dbConfig.getHost(),
                dbConfig.getPort());
        
        if (dbConfig.getDatabase() != null && !dbConfig.getDatabase().isEmpty()) {
            url += "/" + dbConfig.getDatabase();
        }

        try {
            return new DamengConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            throw new IOException("Failed to connect to Dameng database", e);
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        DamengConnection damengConnection = (DamengConnection) connection;
        try {
            Statement statement = damengConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new DamengQueryResult(statement.getResultSet(), statement.getUpdateCount());
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

        result.append("Benchmark completed with ").append(iterations)
              .append(" iterations and ").append(concurrency).append(" concurrency");
        return result.toString();
    }

    @Override
    public String connectionStress(int connections, int duration) {
        for (int i = 0; i < connections; i++) {
            try {
                DatabaseConnection connection = connect();
                this.connections.add(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return String.format("Created %d connections", connections);
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

        // check gen test query
        if (query == null || query.equals("") || (database != null && !database.equals("")) || (table != null && !table.equals(""))) {
            gen_test_query = 1;
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
                    query_test = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME ='" + table + "';";
                    queryResult = this.execute(connection, query_test);
                    if (queryResult.hasResultSet()) {
                        ResultSet rs = queryResult.getResultSet();
                        while (rs.next()) {
                            String count = rs.getString(1);
                            System.out.println("table_count: " + count);
                            table_count = count;
                        }
                    }

                    if (table.equals("executions_loop_table") && !table_count.equals("0")) {
                        // drop test table
                        System.out.println("drop table " + table);
                        query_test = "DROP TABLE IF EXISTS " + table + ";";
                        execute(connection, query_test);
                        table_count = "0";
                    }

                    if (table_count.equals("0")) {
                        // create test table
                        System.out.println("create table " + table);
                        query_test = "CREATE TABLE IF NOT EXISTS " + table + " (id INT PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255));";
                        execute(connection, query_test);
                    }

                    gen_test_query = 2;
                }

                if ((gen_test_query == 2 && (query == null || query.equals("")) || gen_test_query == 3)) {
                    gen_test_values = "executions_loop_test_" + insert_index;
                    // set test query
                    query = "INSERT INTO " + table + " (value) VALUES ('" + gen_test_values + "');";
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
                    executionError = false;
                }
            } catch (IOException e) {
                failedExecutions++;
                if (!executionError) {
                    disconnectCounts++;
                    errorTime = System.currentTimeMillis();
                    errorDate = new Date(errorTime);
                    System.out.println("[" + sdf.format(errorDate) + "] Connection error occurred!");
                    executionError = true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
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

    private static class DamengConnection implements DatabaseConnection {
        private final Connection connection;

        DamengConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close Dameng connection", e);
            }
        }
    }

    public static class DamengQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        DamengQueryResult(ResultSet resultSet, int updateCount) {
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
        // 测试代码
//        DBConfig dbConfig = new DBConfig.Builder()
//            .host("localhost")
//            .port(5236)
//            .database("DAMENG")
//            .user("sysdba")
//            .password("A2x0hM4JZV")
//            .dbType("dameng")
//            .testType("query")
//            .query("SELECT COUNT(*) FROM USER_TABLES;")
//            .build();
//
//        DamengTester tester = new DamengTester(dbConfig);
//        DatabaseConnection connection = tester.connect();
//
//        // 测试查询
//        QueryResult result = tester.execute(connection, dbConfig.getQuery());
//
//        try {
//            if (result.hasResultSet()) {
//                ResultSet rs = result.getResultSet();
//                while (rs.next()) {
//                    String count = rs.getString(1);
//                    System.out.println("count: " + count);
//                }
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            connection.close();
//        }

        // 使用 DBConfig 方式
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(5236)
                .user("sysdba")
                .password("A2x0hM4JZV")
                .dbType("dameng")
                .duration(10)
                .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .testType("executionloop")
//            .database("test_db")
            .table("test_table2")
                .build();
        DamengTester tester = new DamengTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}