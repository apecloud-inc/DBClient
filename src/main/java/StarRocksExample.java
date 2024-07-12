import java.sql.*;


public class StarRocksExample {
    public static void doTest() {
/*
        String url = "jdbc:mysql://127.0.0.1:9030/?useSSL=false";
        String username = "root";
        String password = "";
*/
        String host = System.getenv("host");
        String port = System.getenv("port");
        String username = System.getenv("username");
        String password = System.getenv("password");
        String url = "jdbc:mysql://" + host + ":" + port + "/?useSSL=false";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立连接
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to database successfully!");
            String query = "SHOW PROC '/frontends';";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 遍历输出数据
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + ": ");
                    System.out.println(resultSet.getString(i));
                }
                System.out.println();
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
