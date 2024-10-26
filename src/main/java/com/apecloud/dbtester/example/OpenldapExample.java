package com.apecloud.dbtester.example;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.Hashtable;

public class OpenldapExample {

    public static void doTest() {
        // 设置LDAP连接的环境信息
        Hashtable<String, String> env = new Hashtable<>();
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");
        String url = "ldap://"+hostname+":"+port;
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url); // 修改为你的LDAP服务器地址和端口
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        // 如果LDAP服务器需要认证，请取消以下两行的注释并填写正确的DN和密码
        // env.put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=example,dc=com");
        // env.put(Context.SECURITY_CREDENTIALS, "yourpassword");

        try {
            // 初始化DirContext对象
            DirContext ctx = new InitialDirContext(env);

            // 搜索根DSE来测试连通性
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.OBJECT_SCOPE);

            NamingEnumeration<SearchResult> results = ctx.search("", "(objectclass=*)", searchControls);

            if (results.hasMore()) {
                SearchResult result = results.next();
                Attributes attrs = result.getAttributes();
                System.out.println("Connected to LDAP server successfully. Root DSE attributes:");
                for (NamingEnumeration<?> ae = attrs.getAll(); ae.hasMore();) {
                    Attribute attr = (Attribute) ae.next();
                    System.out.println(attr.getID() + ": " + attr.get());
                }
            } else {
                System.out.println("No entries found, but connection seems to be working.");
            }

            // 关闭DirContext
            ctx.close();

        } catch (NamingException e) {
            e.printStackTrace();
            System.out.println("Failed to connect to LDAP server.");
        }
    }
}