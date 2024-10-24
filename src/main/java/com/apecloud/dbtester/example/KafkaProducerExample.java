import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaProducerExample {
    public static void doTest() {
        // 配置Kafka生产者
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("host");
        String port = System.getenv("port");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", hostname+":"+Integer.parseInt(port)); // Kafka 集群地址
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建Kafka生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        try {
            producer.send(new ProducerRecord<String, String>("test-topic", "Hello, Kafka!")); // 主题名和消息
            System.out.println("Message sent");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭生产者
            producer.close();
        }
    }
}


