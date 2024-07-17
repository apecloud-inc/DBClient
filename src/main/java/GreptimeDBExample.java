import java.sql.*;


public class GreptimeDBExample {

    public static void doTest() throws Exception {
       /* String database = "public";
        String[] endpoints = {"127.0.0.1:4001"};
        AuthInfo authInfo = new AuthInfo("username", "password");
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
                // 如果数据库不需要鉴权，我们可以使用 AuthInfo.noAuthorization() 作为参数。
                .authInfo(authInfo)
                // 如果服务配置了 TLS ，设置 TLS 选项来启用安全连接
                //.tlsOptions(new TlsOptions())
                .build();

        GreptimeDB client = GreptimeDB.create(opts);*/

        try {
            Class.forName("com.mysql.cj.jdbc.Driver"); // MySQL Connector/J 8.0及以上版本
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Driver not found,please try again..", e);
        }
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:mysql://"+hostname+":"+port+"/public?serverTimezone=UTC";


        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("Connected to database!");
            // 3. 执行 SQL 查询
            String query = "SHOW TABLES";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                // 4. 处理查询结果
                while (resultSet.next()) {
                    String result = resultSet.getString(1);
                    System.out.println("TABLES: "+result);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}