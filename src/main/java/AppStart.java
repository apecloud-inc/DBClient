
public class AppStart {

    public static void main(String[] args) throws Exception {
            String engine=System.getenv("engine");
            if (engine.isEmpty()) {
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
                case 22:          //openldap
                    OpenldapExample.doTest();
                    break;
                case 24:          //opensearch
                    OpenSearchExample.doTest();
                    break;
                case 29:          //clickhouse
                    ClickHouseExample.doTest();
                    break;
                case 33:          //mariadb
                    MariadbExample.doTest();
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
                default:
                    System.out.println("Invalid engine: " + engine);
            }
    }
}
