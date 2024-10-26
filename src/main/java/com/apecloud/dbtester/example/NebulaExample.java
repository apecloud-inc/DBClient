package com.apecloud.dbtester.example;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.SessionPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.*;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaExample {

    public static void doTest() throws Exception {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(10);
        List<HostAddress> addresses = Arrays.asList(new HostAddress(hostname, Integer.parseInt(port)));

        NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
        try (Session session = pool.getSession(username, password, false)) {
            ResultSet resultSet = session.execute("SHOW HOSTS;");
            // 处理resultSet...
            System.out.println(resultSet.toString());
            System.out.println("Connect Successfully");


        } catch (Exception e) {
            // 处理其他可能的异常，如连接池问题
            e.printStackTrace();
        }
        pool.close();
    }

}