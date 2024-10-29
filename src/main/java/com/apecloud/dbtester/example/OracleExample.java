package com.apecloud.dbtester.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class OracleExample {

    public static void doTest() {
        //String url = "jdbc:oracle:thin:@localhost:1521:ORCLCDB"; // 替换为你的数据库URL
        // String username = "sys"; // 替换为你的数据库用户名
        //String password = "nrws4n8x"; // 替换为你的数据库密码

        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "jdbc:oracle:thin:@"+hostname+":"+port+":ORCLCDB";

        try {
            // 加载Oracle JDBC驱动  
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // 建立数据库连接  
            Connection conn = DriverManager.getConnection(url, username, password);

            // 创建Statement对象  
            Statement stmt = conn.createStatement();

            // 执行查询  
            String sql = "SELECT * FROM user"; // 替换为你的查询语句和表名
            ResultSet rs = stmt.executeQuery(sql);

            // 处理查询结果  
            while (rs.next()) {
                // 假设你的表有一个名为"column_name"的列  
                String columnName = rs.getString(1);
                System.out.println(columnName);
            }

            // 关闭资源  
            rs.close();
            stmt.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}