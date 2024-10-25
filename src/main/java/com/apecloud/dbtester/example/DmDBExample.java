package com.apecloud.dbtester.example;

import  java.sql.*;
public class DmDBExample {
    public static void doTest() {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:dm://"+hostname+":"+port;
        //String username = "MONITOR";
        //String password = "nft95wr2";
        try {
            Class.forName("dm.jdbc.driver.DmDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver not found,please try again..", e);
        }
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String query = "SELECT * FROM user";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                // 4. 处理查询结果
                while (resultSet.next()) {
                    String user = resultSet.getString(1);
                    System.out.println(" User: " + user);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}