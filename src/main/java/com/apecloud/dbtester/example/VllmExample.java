package com.apecloud.dbtester.example;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class VllmExample {
    public static void doTest() {
//        String url = "http://localhost:8000/v1/completions";
        String host = System.getenv("host");
        String port = System.getenv("port");
        String url = "http://" + host + ":" + port + "/v1/completions";
        String jsonInputString = "{\"prompt\": \"世界第二高峰是哪座\", \"temperature\": 0, \"max_tokens\": 64}";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 测试连通性
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "application/json");
            StringEntity entity = new StringEntity(jsonInputString, StandardCharsets.UTF_8);
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
