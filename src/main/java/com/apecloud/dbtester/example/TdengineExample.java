import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class TdengineExample {
    public static void doTest(){
        // TDengine JDBC 连接字符串，格式为 "jdbc:TAOS://{host}:{port}?user={username}&password={password}"
        //String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/GmT6KYeB?user=root&password=taosdata";

        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");

            // 加载并注册 JDBC 驱动
            //Class.forName("com.taosdata.jdbc.TSDBDriver");
        }catch (Exception e) {
            e.printStackTrace();
        }
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String jdbcUrl = "jdbc:TAOS-RS://"+hostname+":"+port+"?user="+username+"&password="+password;
        // 建立连接
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE DATABASE power KEEP 3650");
                stmt.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                        "TAGS (location BINARY(64), groupId INT)");
                String sql = getSQL();
                int rowCount = stmt.executeUpdate(sql);
                System.out.println("rowCount=" + rowCount); // rowCount=8
                System.out.println("Connect Successfully");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // 创建Statement对象来执行SQL语句


    }
    public static String getSQL() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (String line : getRawData()) {
            String[] ps = line.split(",");
            sb.append("power." + ps[0]).append(" USING power.meters TAGS(")
                    .append(ps[5]).append(", ") // tag: location
                    .append(ps[6]) // tag: groupId
                    .append(") VALUES(")
                    .append('\'').append(ps[1]).append('\'').append(",") // ts
                    .append(ps[2]).append(",") // current
                    .append(ps[3]).append(",") // voltage
                    .append(ps[4]).append(") "); // phase
        }
        return sb.toString();
    }
    private static List<String> getRawData() {
        return Arrays.asList(
                "d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2",
                "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2",
                "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2",
                "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3",
                "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2",
                "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2",
                "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3",
                "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3"
        );
    }
}
