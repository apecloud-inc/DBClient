package com.apecloud.dbtester.example;

import java.sql.*;

public class OpengaussExample {
    public static void doTest()  {
        // 数据库URL，格式为 jdbc:opengauss://主机名:端口/数据库名

        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:opengauss://"+hostname+":"+port+"/opengauss";

        // 数据库用户和密码
        // String username = "kbadmin";
        //String password = "p@ssW0rd1";

        try {
            // 加载并注册JDBC驱动
            Class.forName("org.opengauss.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        // 建立数据库连接
        try(Connection conn = DriverManager.getConnection(url, username, password)){
            System.out.println("Connected to database!");

            try(Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM user")) {

                // 处理结果
                while (rs.next()) {
                    // 根据你的表结构，获取并打印字段值
                    System.out.println(rs.getString(1));
                }
            }
            // 关闭结果集、Statement和连接
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
