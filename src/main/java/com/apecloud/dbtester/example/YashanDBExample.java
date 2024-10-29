package com.apecloud.dbtester.example;

import java.sql.*;

public class YashanDBExample {
    //创建数据库连接。
    public static void doTest() throws SQLException {
        String driver = "com.yashandb.jdbc.Driver";
        //String url = "jdbc:yasdb://localhost:1688/yasdb";
        //String username="sys";
        //String password="yasdb_123";
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String database=System.getenv("database");
        String url = "jdbc:yasdb://"+hostname+":"+port+"/"+database;
        try {
            //加载数据库驱动。
            Class.forName(driver);
        } catch (Exception e) {
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
                    System.out.println(" User: " + user);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
}