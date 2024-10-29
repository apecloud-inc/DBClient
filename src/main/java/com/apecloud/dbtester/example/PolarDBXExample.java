package com.apecloud.dbtester.example;

import java.sql.*;

public class PolarDBXExample {

    public static void doTest(){
            /*String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/mysql?useSSL=false";
            String username = "polardbx_root";
            String password = "xpj89drg"; */

        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:mysql://"+hostname+":"+port+"/mysql?useSSL=false";
        String query = "SELECT * FROM  user"; // 替换为你的表名和查询条件

        try (Connection connection = DriverManager.getConnection(url, username, password);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            // 执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            // 处理查询结果
            while (resultSet.next()) {
                String host = resultSet.getString("Host");
                String user = resultSet.getString("User");
                System.out.println("Host: " + host + ", User: " + user);
            }
            // resultSet.close();

            System.out.println("Query completed successfully.");


        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Connection Failed! Check output console");
        }
    }
}
