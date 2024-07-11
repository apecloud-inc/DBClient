import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.OrganizationsApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.time.Instant;
import java.util.List;

public class InfluxDBExample {


    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url="http://"+hostname+":"+port;
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .authenticateToken(password.toCharArray())
                .org("primary")
                .bucket("primary")
                .build();
        InfluxDBClient client = InfluxDBClientFactory.create(options);
        OrganizationsApi organizationsApi = client.getOrganizationsApi();

        System.out.println(organizationsApi.findOrganizations());
        Instant now = Instant.now(); // 当前时间
        Point dataPoint = Point.measurement("temperature")
                .addTag("location", "New York")
                .addField("value", "123")
                .time(now.toEpochMilli(), WritePrecision.MS); // 使用毫秒精度
        List<Bucket> list =  client.getBucketsApi().findBuckets();
        for (Bucket bucket : list) {
            System.out.println(bucket.getName());
        }
        client.getWriteApiBlocking().writePoint(dataPoint);
        System.out.println();
    }

}

