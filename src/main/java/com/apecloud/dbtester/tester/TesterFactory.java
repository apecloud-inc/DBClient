package com.apecloud.dbtester.tester;

public class TesterFactory {
    public static DatabaseTester createTester(DBConfig config) {
        switch(config.getDbType().toLowerCase()) {
            case "mysql":
                return new MySQLTester(config);
            case "pg":
            case "postgres":
            case "postgresql":
                return new PostgreSQLTester(config);
            case "oracle":
                return new OracleTester(config);
            case "sqlserver":
                return new SQLServerTester(config);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.getDbType());
        }
    }
}