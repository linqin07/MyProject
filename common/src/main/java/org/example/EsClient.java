package org.example;

/**
 * Description: es 客户段要做的事情，注意返回值不能有和es相关的任何东西。
 *              因为一旦有了，你这里就必须引入es相关依赖，没法做到解耦
 * author: 林钦
 * date: 2021/03/28
 */
public interface EsClient {
    boolean testConnect(String ip, Integer port, String clusterName, String esVersion);
}
