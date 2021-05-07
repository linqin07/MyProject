package com.example.core;

import org.example.EsClient;
import org.example.EsClientFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
}
