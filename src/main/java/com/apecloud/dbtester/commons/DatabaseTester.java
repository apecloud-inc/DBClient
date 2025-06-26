package com.apecloud.dbtester.commons;

import java.io.IOException;
import java.sql.*;

public interface DatabaseTester {

    /**
     * 连接到数据库
     *
     * @return 数据库连接对象
     * @throws IOException 如果连接失败则抛出此异常
     */
    DatabaseConnection connect() throws IOException;

    /**
     * 执行SQL语句或HTTP请求
     *
     * @param connection 数据库连接对象
     * @param query      SQL语句或HTTP请求
     * @return 执行结果
     * @throws IOException 如果执行失败则抛出此异常
     */
    QueryResult execute(DatabaseConnection connection, String query) throws IOException;

    /**
     * 运行基准测试
     *
     * @param connection  数据库连接对象
     * @param query       SQL语句或HTTP请求
     * @param iterations  迭代次数
     * @param concurrency 并发数
     * @return 包含基准测试结果的字符串
     */
    String bench(DatabaseConnection connection, String query, int iterations, int concurrency);

    /**
     * 执行连接压力测试
     *
     * @param connections  并发连接数
     * @param duration     测试持续时间(秒)
     * @return 包含压力测试结果的字符串
     */
    String connectionStress(int connections, int duration);

    /**
     * 根据配置执行相应的测试
     * 
     * @return 测试结果字符串
     * @throws IOException 如果执行测试过程中发生IO异常
     * @throws IllegalStateException 如果DBConfig未提供
     * @throws IllegalArgumentException 如果测试类型不支持或参数无效
     */
    String executeTest() throws IOException;

    /**
     * 运行循环测试
     *
     * @param connection   数据库连接对象
     * @param query        SQL语句或HTTP请求
     * @param duration     测试持续时间(秒)
     * @param interval     报告输出间隔(秒)
     * @param database     测试数据库名称
     * @param table        测试表名称
     * @return 包含循环测试结果的字符串
     */
    String executionLoop(DatabaseConnection connection, String query, int duration, int interval, String database, String table);

    /**
     * 释放所有数据库连接
     */
    void releaseConnections();
}

