package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetPrinter {

    /**
     * 打印给定的 ResultSet 数据
     *
     * @param resultSet 要打印的 ResultSet 对象
     * @throws IOException 如果发生 I/O 错误或其他异常
     */
    public static void printResultSet(ResultSet resultSet) throws IOException {
        try {
            // 获取结果集的元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 打印列名
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();

            // 遍历结果集并打印每行数据
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            throw new IOException("Failed to print result set", e);
        }
    }
}
