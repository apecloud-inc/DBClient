package com.apecloud.dbtester.tester;

import java.io.IOException;
import java.sql.*;

public class TestExecutor {
    /**
     * 通用的测试执行函数
     *
     * @param tester   数据库测试器实例
     * @param dbConfig 数据库配置
     * @return 测试结果
     * @throws IOException 如果执行出错
     */
    public static String executeTest(DatabaseTester tester, DBConfig dbConfig) throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }

        String testType = dbConfig.getTestType();
        if (testType == null || testType.isEmpty()) {
            throw new IllegalArgumentException("Test type not specified in DBConfig");
        }

        DatabaseConnection connection = null;
        String result;

        try {
            connection = tester.connect();

            switch (testType.toLowerCase()) {
                case "connectionstress":
                    result = tester.connectionStress(
                        dbConfig.getConnectionCount(),
                        dbConfig.getDuration()
                    );
                    break;

                case "query":
                    String query = dbConfig.getQuery();
                    if (query == null || query.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified in DBConfig");
                    }
                    QueryResult queryResult = tester.execute(connection, query);
                    result = formatQueryResult(queryResult, dbConfig.getDbType());
                    break;

                case "benchmark":
                    String benchQuery = dbConfig.getQuery();
                    if (benchQuery == null || benchQuery.isEmpty()) {
                        throw new IllegalArgumentException("Query not specified for benchmark");
                    }
                    result = tester.bench(
                        connection,
                        benchQuery,
                        dbConfig.getIterations(),
                        dbConfig.getConcurrency()
                    );
                    break;

                case "executionloop":
                    String loopQuery = dbConfig.getQuery();
                    result = tester.executionLoop(
                        connection,
                        loopQuery,
                        dbConfig.getDuration(),
                        dbConfig.getInterval()
                    );
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported test type: " + testType);
            }

            return result;

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * 格式化查询结果
     */
    private static String formatQueryResult(QueryResult queryResult, String dbType) throws IOException {
        StringBuilder sb = new StringBuilder();
        try {
            if (queryResult.hasResultSet()) {
                ResultSet rs = queryResult.getResultSet();
                if (rs.getMetaData() != null) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    // 添加列名
                    for (int i = 1; i <= columnCount; i++) {
                        sb.append(metaData.getColumnName(i)).append("\t");
                    }
                    sb.append("\n");

                    // 添加数据行
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            sb.append(rs.getString(i)).append("\t");
                        }
                        sb.append("\n");
                    }
                }else{
                    if (dbType.toLowerCase().equals("qdrant")){
                        while (rs.next()) {
                            if (rs.getString("result") != null) {
                                sb.append(rs.getString("result"));
                            }
                        }
                        sb.append("\n");
                    }
                }
            } else {
                sb.append("Update count: ").append(queryResult.getUpdateCount());
            }
            return sb.toString();
        } catch (SQLException e) {
            throw new IOException("Failed to format query result", e);
        }
    }
}
