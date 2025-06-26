package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OceanbaseTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "mysql";

    // 默认构造函数
    public OceanbaseTester() {
        this.dbConfig = null;
    }

    // 带配置的构造函数
    public OceanbaseTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }
        String accessMode = dbConfig.getAccessMode().getMode();
        String url;
        String url2;
        try {
            // 根据访问模式加载不同的驱动
            switch (accessMode.toLowerCase()) {
                case "mysql":
                    Class.forName("com.oceanbase.jdbc.Driver");
                    databaseConnection = "mysql";
                    url = String.format("jdbc:oceanbase://%s:%d/%s?useSSL=false",
                            dbConfig.getHost(),
                            dbConfig.getPort(),
                            dbConfig.getDatabase());

                    url2 = String.format("jdbc:oceanbase://%s:%d/%s?useSSL=false",
                            dbConfig.getHost(),
                            dbConfig.getPort(),
                            databaseConnection);

                    if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
                        url = url2;
                    }
                    break;
                case "oracle":
                    Class.forName("com.oceanbase.jdbc.Driver");
                    databaseConnection = "SYS";
                    url = String.format("jdbc:oceanbase:oracle://%s:%d/%s?serverTimezone=Asia/Shanghai",
                            dbConfig.getHost(),
                            dbConfig.getPort(),
                            dbConfig.getDatabase());

                    url2 = String.format("jdbc:oceanbase:oracle://%s:%d/%s?serverTimezone=Asia/Shanghai",
                            dbConfig.getHost(),
                            dbConfig.getPort(),
                            databaseConnection);

                    if (dbConfig.getDatabase() == null || dbConfig.getDatabase().equals("")) {
                        url = url2;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported access mode for OceanBase: " + accessMode);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("OceanBase JDBC Driver not found", e);
        }

        try {
            return new OceanbaseConnection(DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword()));
        } catch (SQLException e) {
            System.err.println("Failed to connect to OceanBase database: " + e );
            System.err.println("Trying with database OceanBase.");
            try {
                return new OceanbaseConnection(DriverManager.getConnection(url2, dbConfig.getUser(), dbConfig.getPassword()));
            } catch (SQLException e2) {
                throw new IOException("Failed to connect to OceanBase database: ", e2);
            }
        }
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        OceanbaseConnection obConnection = (OceanbaseConnection) connection;
        try {
            Statement statement = obConnection.connection.createStatement();
            boolean isResultSet = statement.execute(query);
            return new OceanbaseQueryResult(statement.getResultSet(), statement.getUpdateCount());
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
        String accessMode = dbConfig.getAccessMode().getMode();
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
        String genTest;
        String genTestValue;
        QueryResult queryResult;
        String tableCount = "0";
        int maxId;

        switch (accessMode.toLowerCase()) {
            case "mysql":
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
                break;
            case "oracle":
                if (database == null || database.equals("")) {
                    database = dbConfig.getUser();
                    int atIndex = database.indexOf('@');
                    if (atIndex != -1) {
                        database = database.substring(0, atIndex);
                    }
                }

                // check gen test query
                if (query == null || query.equals("") || (table != null && !table.equals(""))) {
                    genTestQuery = 1;
                }

                if (table == null || table.equals("")) {
                    table = "EXECUTIONS_LOOP_TABLE";
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported access mode for OceanBase: " + accessMode);
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
                switch (accessMode.toLowerCase()) {
                    case "mysql":
                        if (genTestQuery == 1) {
                            // create test databases
                            System.out.println("create databases " + database);
                            genTest = "CREATE DATABASE IF NOT EXISTS " + database + ";";
                            System.out.println(genTest);
                            execute(connection, genTest);

                            if (table.equals("executions_loop_table")) {
                                // drop test table
                                System.out.println("drop table " + table);
                                genTest = "DROP TABLE IF EXISTS " + database + "." + table + ";";
                                System.out.println(genTest);
                                execute(connection, genTest);
                            }

                            // create test table
                            System.out.println("create table " + table);
                            genTest = "CREATE TABLE IF NOT EXISTS " + database + "." + table + " (id INT PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255));";
                            System.out.println(genTest);
                            execute(connection, genTest);

                            genTestQuery = 2;
                        }

                        if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3 )) {
                            genTestValue = "executions_loop_test_" + insertIndex;
                            // set test query
                            query = "INSERT INTO " + database + "." + table + " (value) VALUES ('" + genTestValue + "');";
                            if (genTestQuery == 2) {
                                System.out.println("Execution loop start:" + query);
                            }
                            genTestQuery = 3;
                        }

                        break;
                    case "oracle":
                        if (genTestQuery == 1) {
                            genTest = "SELECT COUNT(*) FROM DBA_TABLES WHERE OWNER = '" + database.toUpperCase() + "' AND TABLE_NAME ='" + table.toUpperCase() + "'";
                            queryResult = execute(connection, genTest);
                            if (queryResult.hasResultSet()) {
                                ResultSet rs = queryResult.getResultSet();
                                while (rs.next()) {
                                    String count = rs.getString(1);
                                    System.out.println("tableCount: " + count);
                                    tableCount = count;
                                }
                            }

                            if (table.equals("EXECUTIONS_LOOP_TABLE") && !tableCount.equals("0")) {
                                // drop test table
                                System.out.println("drop table " + table);
                                genTest = "DROP TABLE " + database + "." + table;
                                System.out.println(genTest);
                                execute(connection, genTest);
                                tableCount = "0";
                            }else if (!tableCount.equals("0")){
                                genTest = "SELECT MAX(ID) FROM " + database + "." + table;
                                queryResult = execute(connection, genTest);
                                if (queryResult.hasResultSet()) {
                                    ResultSet rs = queryResult.getResultSet();
                                    while (rs.next()) {
                                        String maxIdStr = rs.getString(1);
                                        System.out.println("maxId: " + maxIdStr);
                                        maxId = Integer.parseInt(maxIdStr);
                                        insertIndex += maxId;
                                    }
                                }
                            }

                            if (tableCount.equals("0")) {
                                // create test table
                                System.out.println("create table " + table);
                                genTest = "CREATE TABLE " + database + "." + table + "(ID NUMBER PRIMARY KEY, VALUE VARCHAR2(255))";
                                execute(connection, genTest);
                            }
                            genTestQuery = 2;
                        }

                        if ((genTestQuery == 2 && (query == null || query.equals("")) || genTestQuery == 3 )) {
                            genTestValue = "executions_loop_test_" + insertIndex;
                            // set test query
                            query = "INSERT INTO " + database + "." + table + " (ID, VALUE) VALUES (" + insertIndex + ", '" + genTestValue + "')";
                            if (genTestQuery == 2) {
                                System.out.println("Execution loop start:" + query);
                            }
                            genTestQuery = 3;
                        }

                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported access mode for OceanBase: " + accessMode);
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
            }  catch (SQLException e) {
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

    private static class OceanbaseConnection implements DatabaseConnection {
        private final Connection connection;

        OceanbaseConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close OceanBase connection", e);
            }
        }
    }

    public static class OceanbaseQueryResult implements QueryResult {
        private final ResultSet resultSet;
        private final int updateCount;

        OceanbaseQueryResult(ResultSet resultSet, int updateCount) {
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
        // MySQL模式示例
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(2881)
//                .user("root@tenant2")
//                .accessMode(DBConfig.AccessMode.MYSQL)
//                .port(2882)
                .user("sys@tenant2")
                .accessMode(DBConfig.AccessMode.ORACLE)
                .password("")
                .dbType("oceanbase")
                .duration(10)
                .interval(1)
//            .query("INSERT INTO test_table (value) VALUES ('1');")
                .testType("executionloop")
//            .database("test_db")
//            .table("test_table")
                .build();
        OceanbaseTester tester = new OceanbaseTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(),dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }

}