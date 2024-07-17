import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.StringReader;

public class ElasticSearchExample {

    public static void doTest() throws IOException {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String serverUrl="http://"+hostname+":"+port;
        RestClient restClient=RestClient
                .builder(HttpHost
                        .create(serverUrl))
                .build();
        ElasticsearchTransport transport=new RestClientTransport(restClient,new JacksonJsonpMapper());
        ElasticsearchClient esClient=new ElasticsearchClient(transport);

        // 定义索引映射（这里使用简单的JSON字符串，实际使用中可能需要更复杂的结构）
        String mappings = "{\n" +
                "  \"properties\" : {\n" +
                "    \"id\" : {\n" +
                "      \"type\" : \"keyword\" \n" +
                "    },\n"+
                "    \"name\" : {\n" +
                "      \"type\" : \"text\",\n" +
                "      \"fields\" : {\n" +
                "        \"keyword\" : {\n" +
                "          \"type\" : \"keyword\",\n" +
                "          \"ignore_above\" : 256 \n" +
                "        }\n" +
                "      } \n" +
                "    }, \n" +
                "    \"price\" : { \n" +
                "      \"type\" : \"long\" \n" +
                "     } \n" +
                "  }\n" +
                "}\n";

        // 创建索引请求

        JsonpMapper mapper=esClient._transport().jsonpMapper();
        JsonParser parser= Json.createParser(new StringReader(mappings));
        CreateIndexRequest createIndexRequest=new CreateIndexRequest.Builder().index("test")
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser,mapper)).build();
        try {
            esClient.indices().create(createIndexRequest);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //查看索引是否创建成功

        try {
            BooleanResponse test = esClient.indices().exists(existsIndexRequest -> existsIndexRequest.index("test"));
            System.out.println(test.value());
            if (test.value()) {
                System.out.println("Connect Successfully");
            }
            else{
                System.out.println("Connect Unsuccessfully");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //删除索引

        try {
            DeleteIndexResponse response;
            response= esClient.indices().delete(deleteIndexRequest->deleteIndexRequest.index("test"));
            System.out.println(response.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        restClient.close();


    }



}

