import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarProducerExample {

    public static void doTest() throws PulsarClientException {
        // Pulsar 服务地址，可以是一个或多个 broker 的地址，用逗号分隔
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("host");
        String port = System.getenv("port");
        String serviceUrl = "pulsar://"+hostname+":"+port;

        // 创建 Pulsar 客户端实例  
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        // 定义要发送到的 topic 名称  
        String topic = "persistent://public/default/my-topic";

        // 创建一个生产者  
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .create();

        // 发送消息  
        String message = "Hello, Pulsar!";
        producer.send(message.getBytes());

        // 关闭生产者和客户端  
        producer.close();
        client.close();
    }
}

