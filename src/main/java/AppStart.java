import com.apecloud.dbtester.example.*;

public class AppStart {

    public static void main(String[] args) throws Exception {
        String engine=System.getenv("engine");

        if (engine==null||engine.isEmpty()) {
            System.out.println("No engine provided");
            return;
        }
        int engine_type=Integer.parseInt(engine);
        switch (engine_type) {
            case 1:           //mysql
                MySQLExample.doTest();
                break;
            case 2:           //postgresql
                PostgreSQLExample.doTest();
                break;
            case 6:           //mongodb
                MongoDBExample.doTest();
                break;
            case 7:          //kafka
                KafkaProducerExample.doTest();
                break;
            case 8:          //pulsar
                PulsarProducerExample.doTest();
                break;
            case 9:           //weaviate
                WeaviateExample.doTest();
                break;
            case 10:          //qdrant
                QdrantHttpExample.doTest();
                break;
            case 11:          //greptimedb
                GreptimeDBExample.doTest();
                break;
            case 12:          //nebula
                NebulaExample.doTest();
                break;
            case 14:          //starrocks
                StarRocksExample.doTest();
                break;
            case 15:          //etcd
                EtcdExample.doTest();
                break;
            case 17:          //foxlake
                FoxLakeExample.doTest();
                break;
            case 22:          //openldap
                OpenldapExample.doTest();
                break;
            case 23:          //polardbx
                PolarDBXExample.doTest();
                break;
            case 24:          //opensearch
                OpenSearchExample.doTest();
                break;
            case 25:          //elasticsearch
                ElasticSearchExample.doTest();
                break;
            case 26:          //vllm
                VllmExample.doTest();
                break;
            case 27:          //tdengine
                TdengineExample.doTest();
                break;
            case 29:          //clickhouse
                ClickHouseExample.doTest();
                break;
            case 31:          //ggml
                GgmlExample.doTest();
                break;
            case 32:          //zookeeper
                ZookeeperExample.doTest();
                break;
            case 33:          //mariadb
                MariadbExample.doTest();
                break;
            case 35:          //xinference
                XinferenceExample.doTest();
                break;
            case 36:          //oracle
                OracleExample.doTest();
                break;
            case 37:          //opengauss
                OpengaussExample.doTest();
                break;
            case 38:          //influxdb
                InfluxDBExample.doTest();
                break;
            case 39:         //flink
                FlinkExample.doTest();
                break;
            case 40:         //solr
                SolrHttpExample.doTest();
                break;
            case 43:          //mogdb
                MogDBExample.doTest();
                break;
            case 47:          //yashandb
                YashanDBExample.doTest();
                break;
            case 48:          //redis-cluster(sentinel)
                SentinelRedisExample.doTest();
                break;
            case 50:          //dmdb
                DmDBExample.doTest();
                break;
            case 51:        //minio
                MinioExample.doTest();
                break;
            default:
                System.out.println("Invalid engine: " + engine);
        }
    }
}
