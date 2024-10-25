package com.apecloud.dbtester.tester;

public class TesterFactory {
    public static DatabaseTester createTester(DBConfig config) {
        switch(config.getDbType().toLowerCase()) {
            case "mysql":
            case "foxlake":
            case "polardbx":
            case "starrocks":
            case "sr":
                return new MySQLTester(config);
            case "pg":
            case "postgres":
            case "postgresql":
            case "opentenbase":
                return new PostgreSQLTester(config);
            case "oracle":
                return new OracleTester(config);
            case "sqlserver":
                return new SQLServerTester(config);
            case "clickhouse":
            case "ck":
                return new ClickHouseTester(config);
            case "dm":
            case "dmdb":
            case "dameng":
            case "damengdb":
                return new DamengTester(config);
            case "es":
            case "elastic":
            case "elasticsearch":
            case "opensearch":
                return new ElasticSearchTester(config);
            case "etcd":
                return new EtcdTester(config);
            case "mongo":
            case "mongodb":
                return new MongoDBTester(config);
            case "opengauss":
                return new OpenGaussTester(config);
            case "qdrant":
                return new QdrantTester(config);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.getDbType());
        }
    }
}