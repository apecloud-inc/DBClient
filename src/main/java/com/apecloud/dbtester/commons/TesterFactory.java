package com.apecloud.dbtester.commons;

import com.apecloud.dbtester.commons.DBConfig;
import com.apecloud.dbtester.commons.DatabaseTester;
import com.apecloud.dbtester.tester.*;

public class TesterFactory {
    public static DatabaseTester createTester(DBConfig config) {
        switch(config.getDbType().toLowerCase()) {
            case "mysql":
            case "foxlake":
            case "polardbx":
            case "greatsql":
            case "greatdb":
            case "greptime":
            case "greptimedb":
            case "tidb":
            case "mariadb":
                return new MySQLTester(config);
            case "pg":
            case "postgres":
            case "postgresql":
            case "opentenbase":
                return new PostgreSQLTester(config);
            case "vastbase":
                return new VastbaseTester(config);
            case "gaussdb":
                return new GaussdbTester(config);
            case "gbase":
            case "gbase8c":
                return new Gbase8cTester(config);
            case "oracle":
                return new OracleTester(config);
            case "ob":
            case "oceanbase":
                return new OceanbaseTester(config);
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
            case "elasticsearch8":
                return new ElasticSearchTester(config);
            case "elasticsearch7":
            case "opensearch":
                return new OpenSearchTester(config);
            case "loki":
                return new LokiTester(config);
            case "etcd":
                return new EtcdTester(config);
            case "kafka":
                return new KafkaTester(config);
            case "influx":
            case "influxdb":
                return new InfluxDBTester(config);
            case "vm":
            case "victoriametrics":
            case "victoria-metrics":
                return new VictoriaMetricsTester(config);
            case "taos":
            case "tdengine":
            case "td":
                return new TDEngineTester(config);
            case "mongo":
            case "mongodb":
                return new MongoDBTester(config);
            case "opengauss":
                return new OpenGaussTester(config);
            case "qdrant":
                return new QdrantTester(config);
            case "redis":
            case "sentinelredis":
                return new RedisTester(config);
            case "zk":
            case "zookeeper":
                return new ZookeeperTester(config);
            case "kingbase":
                return new KingbaseTester(config);
            case "minio":
                return new MinioTester(config);
            case "mogdb":
                return new MogDBTester(config);
            case "starrocks":
            case "sr":
                return new StarRocksTester(config);
            case "milvus":
                return new MilvusTester(config);
            case "nebula":
                return new NebulaTester(config);
            case "rabbitmq":
                return new RabbitMQTester(config);
            case "rocketmq":
                return new RocketMQTester(config);
            case "doris":
                return new DorisTester(config);
            case "hadoop":
                return new HadoopTester(config);
            case "hive":
                return new HiveTester(config);
            case "vault":
                return new VaultTester(config);
            case "pulsar":
                return new PulsarTester(config);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.getDbType());
        }
    }
}