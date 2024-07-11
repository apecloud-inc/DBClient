import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class SolrHttpExample {
    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String solr_url="http://"+hostname+":"+port+"/solr/admin/info/system";
        try {
            URL url = new URL(solr_url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            System.out.println("Response Code : " + responseCode);

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                // 打印结果
                System.out.println("Connect successfully");
                System.out.println(response.toString());
            } else {
                System.out.println("Solr server did not respond with HTTP 200");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}