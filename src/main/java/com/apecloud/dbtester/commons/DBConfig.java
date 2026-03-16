package com.apecloud.dbtester.commons;

public class DBConfig {
    // 支持的数据库访问模式枚举
    public enum AccessMode {
        EMPTY(""),
        MYSQL("mysql"),
        POSTGRESQL("postgresql"),
        ORACLE("oracle"),
        REDIS("redis"),
        MONGODB("mongodb"),
        INFLUXDB("influxdb"),
        PROMETHEUS("prometheus");

        private final String mode;

        AccessMode(String mode) {
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }
    }

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String org;
    private final String password;
    private final String dbType;        // 数据库类型
    private final AccessMode accessMode; // 数据库访问语法 mode
    private final String testType;      // 测试类型
    private final String query;         // SQL查询语句
    private final String table;         // 数据表名
    private final int connectionCount;  // 连接数量
    private final int duration;         // 测试持续时间(秒)
    private final int interval;         // 报告输出间隔(秒)
    private final int iterations;       // 基准测试迭代次数
    private final int concurrency;      // 基准测试并发数
    private final String master;        // Redis sentinel master
    private final String sentinelPassword;        // Redis sentinel password
    private final String key;           // Database key
    private final String topic;           // Database topic
    private final String bucket;           // Database bucket
    private final String cluster;           // Database cluster
    private final int size;

    private DBConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.user = builder.user;
        this.org = builder.org;
        this.password = builder.password;
        this.dbType = builder.dbType;
        this.accessMode = builder.accessMode;
        this.testType = builder.testType;
        this.query = builder.query;
        this.table = builder.table;
        this.connectionCount = builder.connectionCount;
        this.duration = builder.duration;
        this.interval = builder.interval;
        this.iterations = builder.iterations;
        this.concurrency = builder.concurrency;
        this.master = builder.master;
        this.sentinelPassword = builder.sentinelPassword;
        this.key = builder.key;
        this.topic = builder.topic;
        this.bucket = builder.bucket;
        this.cluster = builder.cluster;
        this.size = builder.size;
    }

    public String getTable() {
        return table;
    }

    public String getDbType() {
        return dbType;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public String getOrg() {
        return org;
    }

    public String getPassword() {
        return password;
    }

    public String getTestType() {
        return testType;
    }

    public String getQuery() {
        return query;
    }

    public int getConnectionCount() {
        return connectionCount;
    }

    public int getDuration() {
        return duration;
    }

    public int getInterval() {
        return interval;
    }

    public int getIterations() {
        return iterations;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public String getMaster() {
        return master;
    }

    public String getSentinelPassword() {
        return sentinelPassword;
    }

    public String getKey() {
        return key;
    }

    public String getTopic() {
        return topic;
    }

    public String getBucket() {
        return bucket;
    }

    public String getCluster() {
        return cluster;
    }

    public int getSize() {
        return size;
    }

    // Builder class
    public static class Builder {
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String user;
        private String org;
        private String password;
        private String dbType = "mysql";     // 默认数据库类型
        private String testType;
        private String query;
        private String table;                // 数据表名
        private int connectionCount = 10;    // 默认值
        private int duration = 60;           // 默认60秒
        private int interval = 1;            // 默认1秒
        private int iterations = 1000;       // 默认值
        private int concurrency = 10;        // 默认值
        private AccessMode accessMode = AccessMode.EMPTY; // 默认访问模式
        private String master;
        private String sentinelPassword;
        private String key;
        private String topic;
        private String bucket;
        private String cluster;
        private int size = 0;


        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder dbType(String dbType) {
            this.dbType = dbType;
            return this;
        }

        public Builder accessMode(AccessMode accessMode) {
            this.accessMode = accessMode;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder org(String org) {
            this.org = org;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder testType(String testType) {
            this.testType = testType;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder connectionCount(int connectionCount) {
            this.connectionCount = connectionCount;
            return this;
        }

        public Builder duration(int duration) {
            this.duration = duration;
            return this;
        }

        public Builder interval(int interval) {
            this.interval = interval;
            return this;
        }

        public Builder iterations(int iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder master(String master) {
            this.master = master;
            return this;
        }

        public Builder sentinelPassword(String sentinelPassword) {
            this.sentinelPassword = sentinelPassword;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public DBConfig build() {
            validate();
            return new DBConfig(this);
        }

        private void validate() {
//            if (database == null || database.isEmpty()) {
//                throw new IllegalStateException("Database must be specified");
//            }
//            if (user == null || user.isEmpty()) {
//                throw new IllegalStateException("User must be specified");
//            }
//            if (org == null || org.isEmpty()) {
//                throw new IllegalStateException("Org must be specified");
//            }
//            if (password == null || password.isEmpty()) {
//                throw new IllegalStateException("Password must be specified");
//            }
            if (testType == null || testType.isEmpty()) {
                throw new IllegalStateException("Test type must be specified");
            }
            if (dbType == null || dbType.isEmpty()) {
                throw new IllegalStateException("Database type must be specified");
            }
//            if (table == null || table.isEmpty()) {
//                throw new IllegalStateException("Table must be specified");
//            }

            // 根据测试类型验证必要参数
            switch (testType.toLowerCase()) {
                case "query":
                case "benchmark":
                    if (query == null || query.isEmpty()) {
                        throw new IllegalStateException("Query must be specified for query/benchmark test");
                    }
                    break;
                case "connectionstress":
                    if (connectionCount <= 0) {
                        throw new IllegalStateException("Connection count must be positive");
                    }
                    if (duration <= 0) {
                        throw new IllegalStateException("Duration must be positive");
                    }
                    break;
                case "executionloop":
                    if (duration <= 0) {
                        throw new IllegalStateException("Duration must be positive");
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported test type: " + testType);
            }

            // 验证数据库类型
            switch (dbType.toLowerCase()) {
                case "mysql":
                case "foxlake":
                case "polardbx":
                case "starrocks":
                case "sr":
                case "greatsql":
                case "greatdb":
                case "greptime":
                case "greptimedb":
                case "pg":
                case "postgres":
                case "postgresql":
                case "opentenbase":
                case "gaussdb":
                case "vastbase":
                case "gbase":
                case "gbase8c":
                case "oracle":
                case "ob":
                case "oceanbase":
                case "sqlserver":
                case "clickhouse":
                case "ck":
                case "dm":
                case "dmdb":
                case "dameng":
                case "damengdb":
                case "es":
                case "elastic":
                case "elasticsearch":
                case "elasticsearch7":
                case "elasticsearch8":
                case "opensearch":
                case "loki":
                case "etcd":
                case "kafka":
                case "influx":
                case "influxdb":
                case "vm":
                case "victoriametrics":
                case "victoria-metrics":
                case "taos":
                case "tdengine":
                case "td":
                case "mongo":
                case "mongodb":
                case "opengauss":
                case "qdrant":
                case "redis":
                case "sentinelredis":
                case "zk":
                case "zookeeper":
                case "kingbase":
                case "minio":
                case "mogdb":
                case "milvus":
                case "nebula":
                case "tidb":
                case "rabbitmq":
                case "rocketmq":
                case "doris":
                case "mariadb":
                case "hadoop":
                case "hive":
                case "vault":
                case "pulsar":
                case "selectdb":
                    break;
                default:
                    throw new IllegalStateException("Unsupported database type: " + dbType);
            }
        }
    }

    @Override
    public String toString() {
        return "DBConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", user='" + user + '\'' +
                ", org='" + org + '\'' +
                ", dbType='" + dbType + '\'' +
                ", accessMode=" + accessMode +
                ", testType='" + testType + '\'' +
                ", query='" + query + '\'' +
                ", table='" + table + '\'' +
                ", connectionCount=" + connectionCount +
                ", duration=" + duration +
                ", interval=" + interval +
                ", iterations=" + iterations +
                ", concurrency=" + concurrency +
                ", master=" + master +
                ", sentinelPassword=" + sentinelPassword +
                ", key=" + key +
                ", topic=" + topic +
                ", bucket=" + bucket +
                ", cluster=" + cluster +
                ", size=" + size +
                '}';
    }
}