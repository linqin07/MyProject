package org.example;

/**
 * @Description: 工厂类构建不同版本客户端
 * @author: LinQin
 * @date: 2021/03/28
 */
public class EsClientFactory {
    public static EsClient builder(String ip, Integer port, String clusterName,String esVersion) {
        try {
            JarVersionClassLoader JarVersionClassLoader = new JarVersionClassLoader("");
            Class<?> aClass = JarVersionClassLoader.loadClass("org.example.EsClientImpl6");
            EsClientProxy esClientProxy = new EsClientProxy(aClass);
            return esClientProxy;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
