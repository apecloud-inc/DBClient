package com.apecloud.dbtester.commons;

import java.io.IOException;

// 数据库连接对象接口
public interface DatabaseConnection {
    void close() throws IOException;
}
