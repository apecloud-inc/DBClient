package com.apecloud.dbtester.commons;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.tester.*;

public class TesterFactory {
    public static DatabaseTester createTester(DBConfig config) {
        switch(config.getDbType().toLowerCase()) {
            // ClickHouse
            case "ck":
            case "clickhouse":
                return new ClickHouseTester(config);
            // Dameng
            case "dameng":
            case "damengdb":
            case "dm":
            case "dmdb":
                return new DamengTester(config);
            // Doris
            case "doris":
                return new DorisTester(config);
            // ElasticSearch
            case "elastic":
            case "elasticsearch":
            case "elasticsearch8":
            case "es":
                return new ElasticSearchTester(config);
            // ElasticSearch7 / OpenSearch
            case "elasticsearch7":
            case "opensearch":
                return new OpenSearchTester(config);
            // Etcd
            case "etcd":
                return new EtcdTester(config);
            // Gbase8c
            case "gbase":
            case "gbase8c":
                return new Gbase8cTester(config);
            // GaussDB
            case "gaussdb":
                return new GaussdbTester(config);
            // Hadoop
            case "hadoop":
                return new HadoopTester(config);
            // Hive
            case "hive":
                return new HiveTester(config);
            // InfluxDB
            case "influx":
            case "influxdb":
                return new InfluxDBTester(config);
            // Kafka
            case "kafka":
                return new KafkaTester(config);
            // KingBase
            case "kingbase":
                return new KingbaseTester(config);
            // Loki
            case "loki":
                return new LokiTester(config);
            // Milvus
            case "milvus":
                return new MilvusTester(config);
            // Minio
            case "minio":
                return new MinioTester(config);
            // MogDB
            case "mogdb":
                return new MogDBTester(config);
            // MongoDB
            case "mongo":
            case "mongodb":
                return new MongoDBTester(config);
            // MySQL (包括各种兼容 MySQL 协议的数据库)
            case "foxlake":
            case "greatdb":
            case "greatsql":
            case "greptime":
            case "greptimedb":
            case "mariadb":
            case "mysql":
            case "polardbx":
            case "tidb":
                return new MySQLTester(config);
            // Nebula
            case "nebula":
                return new NebulaTester(config);
            // OceanBase
            case "ob":
            case "oceanbase":
                return new OceanbaseTester(config);
            // OpenGauss
            case "opengauss":
                return new OpenGaussTester(config);
            // Oracle
            case "oracle":
                return new OracleTester(config);
            // PostgreSQL
            case "pg":
            case "postgres":
            case "postgresql":
            case "opentenbase":
                return new PostgreSQLTester(config);
            // Pulsar
            case "pulsar":
                return new PulsarTester(config);
            // Qdrant
            case "qdrant":
                return new QdrantTester(config);
            // RabbitMQ
            case "rabbitmq":
                return new RabbitMQTester(config);
            // Redis
            case "camellia-proxy":
            case "camellia-redis-proxy":
            case "redis":
            case "sentinelredis":
                return new RedisTester(config);
            // RocketMQ
            case "rocketmq":
                return new RocketMQTester(config);
            // SelectDB
            case "selectdb":
                return new SelectDBTester(config);
            // SQLServer
            case "mssql":
            case "sqlserver":
                return new SQLServerTester(config);
            // StarRocks
            case "sr":
            case "starrocks":
                return new StarRocksTester(config);
            // TDEngine
            case "taos":
            case "td":
            case "tdengine":
                return new TDEngineTester(config);
            // VastBase
            case "vastbase":
                return new VastbaseTester(config);
            // VictoriaLogs
            case "victoria-logs":
            case "victorialogs":
                return new VictoriaLogsTester(config);
            // VictoriaMetrics
            case "vm":
            case "victoria-metrics":
            case "victoriametrics":
                return new VictoriaMetricsTester(config);
            // Vault
            case "vault":
                return new VaultTester(config);
            // Zookeeper
            case "zk":
            case "zookeeper":
                return new ZookeeperTester(config);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.getDbType());
        }
    }
}