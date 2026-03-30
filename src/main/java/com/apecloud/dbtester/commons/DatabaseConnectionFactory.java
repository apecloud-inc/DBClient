package com.apecloud.dbtester.commons;

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
            // ClickHouse
            case "ck":
            case "clickhouse":
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                break;
            // Dameng
            case "dameng":
            case "dm":
                Class.forName("dm.jdbc.driver.DmDriver");
                break;
            // GaussDB
            case "gaussdb":
                Class.forName("com.huawei.opengauss.jdbc.Driver");
                break;
            // KingBase
            case "kingbase":
                Class.forName("com.kingbase8.Driver");
                break;
            // MariaDB
            case "mariadb":
                Class.forName("org.mariadb.jdbc.Driver");
                break;
            // MogDB
            case "mogdb":
                Class.forName("io.mogdb.Driver");
                break;
            // MySQL (包括各种兼容 MySQL 协议的数据库)
            case "doris":
            case "foxlake":
            case "greatdb":
            case "greatsql":
            case "greptime":
            case "polardbx":
            case "selectdb":
            case "sr":
            case "starrocks":
                Class.forName("com.mysql.cj.jdbc.Driver");
                break;
            // MySQL 5.1
            case "mysql 5.1":
                Class.forName("com.mysql.jdbc.Driver");
                break;
            // OpenGauss
            case "opengauss":
                Class.forName("org.opengauss.Driver");
                break;
            // Oracle
            case "oracle":
                Class.forName("oracle.jdbc.driver.OracleDriver");
                break;
            // PostgreSQL
            case "pg":
            case "postgresql":
                Class.forName("org.postgresql.Driver");
                break;
            // TDEngine
            case "taos":
            case "tdengine":
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
                break;
            // YashanDB
            case "yanshan":
                Class.forName("com.yashandb.jdbc.Driver");
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private static String getJDBCUrl(String dbType, String host, String database, int port) {
        String url = null;
        switch (dbType.toLowerCase()) {
            // ClickHouse
            case "ck":
            case "clickhouse":
                url = "jdbc:clickhouse://"+ host + ":" + port + "/" + database;
                break;
            // Dameng
            case "dameng":
            case "dm":
                url = "jdbc:dm://" + host + ":" + port;
                break;
            // GaussDB / OpenGauss
            case "gaussdb":
            case "opengauss":
                url = "jdbc:opengauss://" + host + ":" + port + "/" + database;
                break;
            // MogDB
            case "mogdb":
                url = "jdbc:mogdb://" + host + ":" + port + "/mogdb?useSSL=false&serverTimezone=UTC";
                break;
            // MySQL (包括各种兼容 MySQL 协议的数据库)
            case "doris":
            case "foxlake":
            case "greatdb":
            case "greatsql":
            case "mariadb":
            case "mysql":
            case "mysql 5.1":
            case "polardbx":
            case "selectdb":
            case "sr":
            case "starrocks":
                url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false";
                break;
            // Oracle
            case "oracle":
                url = "jdbc:oracle:thin:@" + host + ":" + port + ":ORCLCDB";
                break;
            // PostgreSQL
            case "pg":
            case "postgresql":
                url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        return url;
    }

}