

import org.apache.http.HttpHost;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;

public class OpenSearchExample {
    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        // 创建RestHighLevelClient实例  
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, Integer.parseInt(port),"http")))) {

            // 创建一个索引请求  
            IndexRequest indexRequest = new IndexRequest("test_index1")
                    .source(XContentType.JSON, "field11", "value11", "field22", "value22");

            // 执行索引请求  
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

            // 输出响应结果  

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED || indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("索引成功");
            } else {
                System.out.println("索引操作未成功创建或更新文档");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}