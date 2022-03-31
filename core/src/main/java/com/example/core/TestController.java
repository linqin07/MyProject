package com.example.core;

import org.example.EsClient;
import org.example.EsClientFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @Description:
 * @author: LinQin
 * @date: 2021/03/28
 */
@RestController
public class TestController {

    @RequestMapping
    public boolean testConnect() {
        EsClient builder = EsClientFactory.builder("192.168.0.196", 9200, "datacenter", "es6");
        boolean b = builder.testConnect("192.168.0.196", 9200, "datacenter", "es6");
        return b;
    }

    @RequestMapping("/classLoader")
    public void classLoader() throws Exception {
        String path = "d:/mysql-driver.jar";
        Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        URL url = new URL("file:" + path);
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        method.setAccessible(true);
        method.invoke(systemClassLoader, url);

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("url", "root", "pwd");
        if (conn == null) {
            System.out.println("连接失败");
        }

    }
}
