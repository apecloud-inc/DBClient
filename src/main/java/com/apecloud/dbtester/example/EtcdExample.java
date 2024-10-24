import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;

import java.nio.charset.StandardCharsets;

public class EtcdExample {
    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "http://"+hostname+":"+port;
        try (Client client = Client.builder().endpoints(url).build()) {
            // 写入键值对  
            client.getKVClient().put(
                    io.etcd.jetcd.ByteSequence.from("key", StandardCharsets.UTF_8),
                    io.etcd.jetcd.ByteSequence.from("value", StandardCharsets.UTF_8)
            ).get();

            // 读取键值对  
            GetResponse getResponse = client.getKVClient().get(
                    io.etcd.jetcd.ByteSequence.from("key", StandardCharsets.UTF_8)
            ).get();
            System.out.println("Connect Successfully");
            System.out.println(getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}