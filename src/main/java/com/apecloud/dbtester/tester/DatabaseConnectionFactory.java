package com.apecloud.dbtester.tester;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnectionFactory {
    public static Connection getConnection(String dbType, String host, String user, String database, String password, int port) {
        Connection connection = null;
        try {
            // 加载驱动程序
            registerDriver(dbType);

            // 构建 JDBC URL
            String url = getJDBCUrl(dbType, host, database, port);

            // 获取连接
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private static void registerDriver(String dbType) throws ClassNotFoundException {
        switch (dbType.toLowerCase()) {
            case "mysql":
            case "polardbx":
            case "foxlake":
            case "starrocks":
            case "sr":
                Class.forName("com.mysql.cj.jdbc.Driver");
                break;
            case "mysql 5.1":
                Class.forName("com.mysql.jdbc.Driver");
                break;
            case "pg":
            case "postgresql":
                Class.forName("org.postgresql.Driver");
                break;
            case "gaussdb":
                Class.forName("com.huawei.opengauss.jdbc.Driver");
                break;
            case "ck":
            case "clickhouse":
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                break;
            case "dm":
            case "dameng":
                Class.forName("dm.jdbc.driver.DmDriver");
                break;
            case "greptime":
                Class.forName("com.mysql.cj.jdbc.Driver");
                break;
            case "mariadb":
                Class.forName("org.mariadb.jdbc.Driver");
                break;
            case "mogdb":
                Class.forName("io.mogdb.Driver");
                break;
            case "opengauss":
                Class.forName("org.opengauss.Driver");
                break;
            case "oracle":
                Class.forName("oracle.jdbc.driver.OracleDriver");
                break;
            case "taos":
            case "tdengine":
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
                break;
            case "yanshan":
                Class.forName("com.yashandb.jdbc.Driver");
                break;
            case "kingbase":
                Class.forName("com.kingbase8.Driver");
                break;
            // 可以添加其他数据库驱动程序
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private static String getJDBCUrl(String dbType, String host, String database, int port) {
        String url = null;
        switch (dbType.toLowerCase()) {
            case "mysql":
            case "mysql 5.1":
            case "foxlake":
            case "polardbx":
            case "starrocks":
            case "sr":
                url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false";
                break;
            case "pg":
            case "postgresql":
                url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
                break;
            case "gaussdb":
                url = "jdbc:opengauss://" + host + ":" + port + "/" + database;
                break;
            case "ck":
            case "clickhouse":
                url = "jdbc:clickhouse://"+ host + ":" + port + "/" + database;
                break;
            case "dm":
            case "dameng":
                url = "jdbc:dm://" + host + ":" + port;
                break;
//            case "greptime":
//                url = "jdbc:mysql://" + host + ":" + port + "/public?serverTimezone=UTC";
//                break;
            case "mariadb":
                url = "jdbc:mariadb://" + host + ":" + port + "/";
                break;
            case "mogdb":
                url = "jdbc:mogdb://" + host + ":" + port + "/mogdb?useSSL=false&serverTimezone=UTC";
                break;
            case "opengauss":
                url = "jdbc:opengauss://" + host + ":" + port + "/opengauss";
                break;
            case "oracle":
                url = "jdbc:oracle:thin:@" + host + ":" + port + ":ORCLCDB";
                break;
//            case "taos":
//            case "tdengine":
//                url = "jdbc:TAOS-RS://" + host + ":" + port;
//                break;
//            case "yashan":
//                url = "jdbc:yasdb://" + host + ":" + port + "/" + database;
//                break;
            // 可以添加其他数据库 JDBC URL 格式
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        return url;
    }

}