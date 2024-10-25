package com.apecloud.dbtester.example;

import java.sql.*;

public class MariadbExample {
    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:mariadb://"+hostname+":"+port+"/"; // 替换为你的数据库URL
        //String username = "root"; // 替换为你的数据库用户名
        //String password = "b79mhj4r"; // 替换为你的数据库密码

        // 加载MariaDB JDBC驱动
        try {
            Class.forName("org.mariadb.jdbc.Driver");
        }catch (ClassNotFoundException e){
            throw new RuntimeException(e);
        }
        // 建立连接
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("Connected to database!");
            // 3. 执行 SQL 查询
            String query = "SHOW DATABASES";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                // 4. 处理查询结果
                while (resultSet.next()) {
                    String database = resultSet.getString(1);
                    System.out.println("database: " + database );
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}