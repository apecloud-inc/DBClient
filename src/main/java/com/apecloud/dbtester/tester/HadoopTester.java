package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HadoopTester implements DatabaseTester {
    private List<DatabaseConnection> connections = new ArrayList<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DBConfig dbConfig;
    private String databaseConnection = "default";

    // 默认构造函数
    public HadoopTester() {
        this.dbConfig = null;
    }

    // 接收 DBConfig 的构造函数
    public HadoopTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    // 使用 DBConfig 的 connect 方法
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        try {
            // 使用Hadoop FileSystem API
            Configuration conf = new Configuration();
            String hdfsUri = "hdfs://" + dbConfig.getHost() + ":" + dbConfig.getPort();
            conf.set("fs.defaultFS", hdfsUri);

            // 如果需要认证，可以设置相关配置
            if (dbConfig.getUser() != null && !dbConfig.getUser().isEmpty()) {
                System.setProperty("HADOOP_USER_NAME", dbConfig.getUser());
            }

            FileSystem fs = FileSystem.get(conf);
            return new HadoopFSConnection(fs, hdfsUri);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Hadoop HDFS", e);
        }
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
        java.sql.Date errorDate = null;
        long lastOutputTime = System.currentTimeMillis();
        int outputPassTime = 0;

        int insertIndex = 0;
        int genTestQuery = 0;
        String genTestValue;

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

                HadoopFSConnection fsConnection = (HadoopFSConnection) connection;
                FileSystem fs = fsConnection.getFileSystem();

                if (genTestQuery == 1) {
                    // Check if database (directory) exists
                    Path dbPath = new Path("/" + database);
                    if (!fs.exists(dbPath)) {
                        // Create test database (directory)
                        System.out.println("Creating database (directory) " + database);
                        fs.mkdirs(dbPath);
                    }

                    // Check if table (directory) exists
                    Path tablePath = new Path("/" + database + "/" + table);
                    if (fs.exists(tablePath)) {
                        // Clear existing table data
                        fs.delete(tablePath, true);
                    }

                    // Create test table (directory)
                    System.out.println("Creating table (directory) " + table);
                    fs.mkdirs(tablePath);

                    genTestQuery = 2;
                }

                if ((genTestQuery == 2 && (query == null || query.equals(""))) || genTestQuery == 3) {
                    Random random = new Random();
                    genTestValue = "executions_loop_test_" + insertIndex;

                    // Write data to file in the table directory
                    Path dataFile = new Path("/" + database + "/" + table + "/data_" + insertIndex + ".txt");
                    FSDataOutputStream out = fs.create(dataFile, true);
                    String data = insertIndex + "," + genTestValue + "," + System.currentTimeMillis() + "\n";
                    out.writeUTF(data);
                    out.close();

                    // Set test query description
                    query = "Write data to " + dataFile.toString();

                    if (genTestQuery == 2) {
                        System.out.println("Execution loop start:" + query);
                    }
                    genTestQuery = 3;
                }

                // Execute the file operation
                // In this case, we've already performed the write operation above
                successfulExecutions++;

            } catch (Exception e) {
                failedExecutions++;
                System.err.println("Error executing operation: " + e.getMessage());
                e.printStackTrace();
                executionError = true;

                if (errorTime == 0) {
                    errorTime = System.currentTimeMillis();
                    errorDate = new java.sql.Date(errorTime);
                }

                try {
                    connection = this.connect();
                    executionError = false;
                } catch (Exception connectionException) {
                    disconnectCounts++;
                    System.err.println("Reconnection failed: " + connectionException.getMessage());
                }
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        result.append("\n").append(sdf.format(new java.util.Date())).append(" - ").append("Execution Loop completed.\n");
        result.append("Total time: ").append(totalTime).append(" ms\n");
        result.append("Successful executions: ").append(successfulExecutions).append("\n");
        result.append("Failed executions: ").append(failedExecutions).append("\n");
        result.append("Disconnect counts: ").append(disconnectCounts).append("\n");

        if (errorTime > 0) {
            recoveryTime = System.currentTimeMillis();
            errorToRecoveryTime = recoveryTime - errorTime;
            result.append("Error occurred at: ").append(errorDate).append("\n");
            result.append("Recovered at: ").append(sdf.format(new java.util.Date(recoveryTime))).append("\n");
            result.append("Time from error to recovery: ").append(errorToRecoveryTime).append(" ms\n");
        }

        return result.toString();
    }

    @Override
    public void releaseConnections() {
        for (DatabaseConnection connection : connections) {
            try {
                connection.close();
            } catch (IOException e) {
                System.err.println("Error closing Hadoop connection: " + e.getMessage());
            }
        }
        connections.clear();
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String sql) throws IOException {
        // For HDFS, we don't execute SQL. This is a placeholder implementation.
        // In a real implementation, you might parse the "sql" string to determine
        // what file operations to perform.
        throw new UnsupportedOperationException("SQL execution not supported for HDFS. Use file operations instead.");
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        return null;
    }

    @Override
    public String connectionStress(int connections, int duration) {
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        ExecutorService executor = Executors.newFixedThreadPool(connections);

        // Shared counters for all threads
        int[] successfulConnections = {0};
        int[] failedConnections = {0};
        int[] disconnectCounts = {0};

        result.append("Starting connection stress test with ")
                .append(connections)
                .append(" connections for ")
                .append(duration)
                .append(" seconds\n");

        // Start the worker threads
        for (int i = 0; i < connections; i++) {
            executor.submit(() -> {
                DatabaseConnection localConnection = null;
                boolean connected = false;

                while (System.currentTimeMillis() < endTime) {
                    try {
                        // Try to establish a new connection
                        localConnection = this.connect();
                        synchronized (successfulConnections) {
                            successfulConnections[0]++;
                        }
                        connected = true;

                        // Hold the connection for a short period
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }

                    } catch (Exception e) {
                        synchronized (failedConnections) {
                            failedConnections[0]++;
                        }
                    } finally {
                        // Close the connection if it was established
                        if (localConnection != null && connected) {
                            try {
                                localConnection.close();
                                synchronized (disconnectCounts) {
                                    disconnectCounts[0]++;
                                }
                            } catch (IOException e) {
                                // Ignore close errors in stress test
                            }
                            connected = false;
                        }
                    }
                }
            });
        }

        // Shutdown the executor and wait for termination
        executor.shutdown();
        try {
            if (!executor.awaitTermination(duration + 5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        result.append("\nConnection stress test completed.\n");
        result.append("Total time: ").append(totalTime).append(" ms\n");
        result.append("Successful connections: ").append(successfulConnections[0]).append("\n");
        result.append("Failed connections: ").append(failedConnections[0]).append("\n");
        result.append("Connections closed: ").append(disconnectCounts[0]).append("\n");

        return result.toString();
    }

    private static class HadoopFSConnection implements DatabaseConnection {
        private final FileSystem fileSystem;
        private final String hdfsUri;

        HadoopFSConnection(FileSystem fileSystem, String hdfsUri) {
            this.fileSystem = fileSystem;
            this.hdfsUri = hdfsUri;
        }

        public FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        public void close() throws IOException {
            try {
                fileSystem.close();
            } catch (IOException e) {
                throw new IOException("Failed to close Hadoop FileSystem connection", e);
            }
        }
    }

    public static class HadoopQueryResult implements QueryResult {
        private final org.apache.hadoop.fs.Path path;
        private final int updateCount;

        HadoopQueryResult(org.apache.hadoop.fs.Path path, int updateCount) {
            this.path = path;
            this.updateCount = updateCount;
        }

        @Override
        public java.sql.ResultSet getResultSet() throws java.sql.SQLException {
            return null; // HDFS doesn't return ResultSet
        }

        @Override
        public int getUpdateCount() {
            return updateCount;
        }

        @Override
        public boolean hasResultSet() {
            return false; // HDFS doesn't have ResultSet
        }
    }

    public static void main(String[] args) throws IOException {
        // 使用示例
        DBConfig dbConfig = new DBConfig.Builder()
                .host("localhost")
                .port(8020) // HDFS默认端口
                .user("hadoop")
                .password("")
                .dbType("hadoop")
                .duration(10)
                .interval(1)
//                .query("Write test data")
                .testType("executionloop")
//                .database("test_db")
//                .table("test_table")
                .build();
        HadoopTester tester = new HadoopTester(dbConfig);
        DatabaseConnection connection = tester.connect();
        String result = tester.executionLoop(connection, dbConfig.getQuery(), dbConfig.getDuration(),
                dbConfig.getInterval(), dbConfig.getDatabase(), dbConfig.getTable());
        System.out.println(result);
        connection.close();
    }
}
