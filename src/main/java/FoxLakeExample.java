import java.sql.*;


public class FoxLakeExample {
    public static void doTest() {
//        String url = "jdbc:mysql://127.0.0.1:11288/?useSSL=false";
//        String username = "foxlake_root";
//        String password = "g2lcz89g";
        try {
            // 加载 JDBC 驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            String username = System.getenv("username");
            String password = System.getenv("password");
            String hostname = System.getenv("host");
            String port = System.getenv("port");
            String url = "jdbc:mysql://"+hostname+":"+port+"/?useSSL=false";
            // 建立连接
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to database successfully!");
            String query = "SHOW DATABASES;";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 遍历结果集的每一行
            while (resultSet.next()) {
                // 输出每一行的数据
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
