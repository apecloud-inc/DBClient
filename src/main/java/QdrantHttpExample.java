import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class QdrantHttpExample {


    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("host");
        String port = System.getenv("port");
        String QDRANT_URL = "http://"+hostname+":"+port+"/collections/my_collection/points/insert";
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            // 创建一个HTTP POST请求
            HttpPost httpPost = new HttpPost(QDRANT_URL);
            // 设置请求体（例如，一个包含向量的JSON对象）
            // 序列化点列表到 JSON

            String json = "{\"ids\": [123], \"vector\": [0.1, 0.2, 0.3, 0.4, 0.5]}"; // 注意这里是一个数组

            StringEntity entity = new StringEntity(json, "UTF-8");
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");

            // 执行请求并获取响应
            HttpResponse response = client.execute(httpPost);

            // 处理响应
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null) {
                String responseString = EntityUtils.toString(responseEntity, "UTF-8");
                System.out.println("Response: " + responseString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

