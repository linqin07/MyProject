package org.example;

import java.lang.reflect.Method;

/**
 * @Description: 静态代理
 * @author: LinQin
 * @date: 2021/03/28
 */
public class EsClientProxy implements EsClient {

    private Class<?> aClass;

    public EsClientProxy(Class<?> aClass) {
        this.aClass = aClass;
    }

    @Override
    public boolean testConnect(String ip, Integer port, String clusterName, String esVersion) {
        try {
            Method testConnect = aClass
                    .getMethod("testConnect", String.class, Integer.class, String.class, String.class);
            Object invoke = testConnect.invoke(aClass.newInstance(), ip, port, clusterName, esVersion);
            return (boolean) invoke;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }
}
