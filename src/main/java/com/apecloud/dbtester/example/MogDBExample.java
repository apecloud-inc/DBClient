package com.apecloud.dbtester.example;

import java.sql.*;

public class MogDBExample {
    public static void doTest()  {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:mogdb://"+hostname+":"+port+"/mogdb?useSSL=false&serverTimezone=UTC";
        //String username = "kbadmin";
        //String password = "p@ssW0rd1";

        try {
            Class.forName("io.mogdb.Driver");
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("Connected to database!");
            // 3. 执行 SQL 查询
            String query = "SELECT * FROM user";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                // 4. 处理查询结果
                while (resultSet.next()) {
                    String user = resultSet.getString(1);
                    System.out.println( "User: " + user);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}