import java.sql.*;


public class ClickHouseExample {

    public static void doTest() {

     /*   String url = "jdbc:clickhouse://127.0.0.1:8123/default";
        String user = "admin";
        String password = "rjzc5qm4";
        String database = "";*/
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("host");
        String port = System.getenv("port");
        String database = System.getenv("database");
        String url = "jdbc:clickhouse://"+hostname+":"+port+"/"+database;

        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
             System.out.println("Connect Successful");
             while (resultSet.next()) {
                String tableName = resultSet.getString(1); // 假设表名是第一列，索引从1开始
                System.out.println("table name :"+tableName);
             }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}