import org.apache.commons.cli.*;
import com.apecloud.dbtester.tester.*;

public class OneClient {
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        // 基本数据库配置选项
        options.addOption("h", "host", true, "Database host")
               .addOption("u", "user", true, "Database user name")
               .addOption("o", "org", true, "Org name for some databases")
               .addOption("p", "password", true, "Database password")
               .addOption("P", "port", true, "Database port number")
               .addOption("d", "database", true, "Database name")
               .addOption("e", "dbtype", true, "Database type (mysql/postgresql/oceanbase/etc)")
               .addOption("t", "table", true, "Table name")
               .addOption("a", "accessmode", true, "Access mode (mysql/postgresql/oracle/redis/influxdb/prometheus)");

        // 测试相关选项
        options.addOption("t", "test", true, "Test type (query/connectionstress/benchmark/executionloop)")
               .addOption("q", "query", true, "SQL query to execute")
               .addOption("c", "connections", true, "Number of connections")
               .addOption("s", "duration", true, "Test duration in seconds")
               .addOption("I", "interval", true, "Periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports")
               .addOption("i", "iterations", true, "Number of iterations for benchmark")
               .addOption("m", "concurrency", true, "Concurrency level for benchmark")
               .addOption("M", "master", true, "Redis sentinel master")
               .addOption("S", "sentinelPassword", true, "Redis sentinel password")
               .addOption("k", "key", true, "Database key");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            DBConfig config = createConfig(cmd);
            executeTest(config);
        } catch (ParseException e) {
            System.err.println("Failed to parse command line arguments: " + e.getMessage());
            new HelpFormatter().printHelp("OneClient", options);
            return;
        }
    }

    private static DBConfig createConfig(CommandLine cmd) {
        // 处理访问模式
        DBConfig.AccessMode accessMode;
        try {
            String accessModeStr = cmd.getOptionValue("accessmode", "mysql").toUpperCase();
            accessMode = DBConfig.AccessMode.valueOf(accessModeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid access mode. Supported modes: " +
                String.join(", ", java.util.Arrays.stream(DBConfig.AccessMode.values())
                    .map(mode -> mode.getMode())
                    .toArray(String[]::new)));
        }

        DBConfig.Builder builder = new DBConfig.Builder()
            .host(cmd.getOptionValue("host", "127.0.0.1"))
            .user(cmd.getOptionValue("user", ""))
            .org(cmd.getOptionValue("org", ""))
            .password(cmd.getOptionValue("password", ""))
            .database(cmd.getOptionValue("database", ""))
            .dbType(cmd.getOptionValue("dbtype", ""))
            .accessMode(accessMode)
            .table(cmd.getOptionValue("table", ""))
            .query(cmd.getOptionValue("query", ""))
            .testType(cmd.getOptionValue("test", ""))
            .master(cmd.getOptionValue("master", ""))
            .sentinelPassword(cmd.getOptionValue("sentinelPassword", ""))
            .key(cmd.getOptionValue("key", ""));

        // 处理数值类型的参数
        try {
            // 必需的数值参数
            builder.port(Integer.parseInt(cmd.getOptionValue("port", "")));
            builder.connectionCount(Integer.parseInt(cmd.getOptionValue("connections", "100")));

            // 可选的数值参数
            if (cmd.hasOption("duration")) {
                builder.duration(Integer.parseInt(cmd.getOptionValue("duration", "60")));
            }
            if (cmd.hasOption("iterations")) {
                builder.iterations(Integer.parseInt(cmd.getOptionValue("iterations")));
            }
            if (cmd.hasOption("interval")) {
                builder.interval(Integer.parseInt(cmd.getOptionValue("interval", "1")));
            }
            if (cmd.hasOption("concurrency")) {
                builder.concurrency(Integer.parseInt(cmd.getOptionValue("concurrency")));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric parameter: " + e.getMessage());
        }

        return builder.build();
    }

    private static void executeTest(DBConfig config) {
        try {
            DatabaseTester tester = TesterFactory.createTester(config);
            String result = TestExecutor.executeTest(tester, config);

            // 打印测试结果
            System.out.println("Test Result:");
            System.out.println(result);

            // 打印配置信息
            System.out.println("\nConnection Information:");
            System.out.printf("Database Type: %s%n", config.getDbType());
            System.out.printf("Access Mode: %s%n", config.getAccessMode().getMode());
            System.out.printf("Host: %s%n", config.getHost());
            System.out.printf("Port: %d%n", config.getPort());
            System.out.printf("Database: %s%n", config.getDatabase());
            System.out.printf("Table: %s%n", config.getTable());
            System.out.printf("User: %s%n", config.getUser());
            System.out.printf("Org: %s%n", config.getOrg());
            System.out.printf("Test Type: %s%n", config.getTestType());

            // 根据测试类型打印额外信息
            switch (config.getTestType().toLowerCase()) {
                case "connectionstress":
                    System.out.printf("Connection Count: %d%n", config.getConnectionCount());
                    System.out.printf("Duration: %d seconds%n", config.getDuration());
                    break;
                case "benchmark":
                    System.out.printf("Iterations: %d%n", config.getIterations());
                    System.out.printf("Concurrency: %d%n", config.getConcurrency());
                    System.out.printf("Query: %s%n", config.getQuery());
                    break;
                case "query":
                    System.out.printf("Query: %s%n", config.getQuery());
                    break;
                case "executionloop":
                    System.out.printf("Query: %s%n", config.getQuery());
                    System.out.printf("Duration: %d seconds%n", config.getDuration());
                    System.out.printf("Interval: %d seconds%n", config.getInterval());
                    System.out.printf("Key: %s%n", config.getKey());
                    break;
            }
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public Object getGreeting() {
        return "Hello";
    }
}