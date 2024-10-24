import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class XinferenceExample {
    public static void doTest() {
        String host = System.getenv("host");
        String port = System.getenv("port");
        String url = "http://"+ host +":"+ port;
        String inputText = "";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 发送空报文，测试连通性
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "application/json");
            StringEntity entity = new StringEntity(inputText, StandardCharsets.UTF_8);
            httpPost.setEntity(entity);
            HttpResponse response= httpClient.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println("response: " + responseBody);
            System.out.println("连接成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
