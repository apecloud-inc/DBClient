package com.apecloud.dbtester.commons;

import java.sql.ResultSet;
import java.sql.SQLException;

// 查询结果接口
public interface QueryResult {
    // 根据不同的数据库类型定义不同的结果格式

    /**
     * 获取查询结果集
     *
     * @return 查询结果集, 如果查询不返回结果集则为 null
     * @throws SQLException 如果发生 SQL 异常
     */
    ResultSet getResultSet() throws SQLException;

    /**
     * 获取更新计数
     *
     * @return 更新计数, 如果查询返回结果集则为 0
     */
    int getUpdateCount();

    /**
     * 检查查询是否返回结果集
     *
     * @return true 如果查询返回结果集,否则为 false
     */
    boolean hasResultSet();
}
