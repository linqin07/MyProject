package org.example;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;

/**
 * @Description:
 * @author: LinQin
 * @date: 2021/03/28
 */
public class EsClientImpl6 implements EsClient {
    @Override
    public boolean testConnect(String ip, Integer port, String clusterName, String esVersion) {
        Settings settings = Settings.builder()
                                    .put("cluster.name", clusterName)
                                    .put("client.transport.ping_timeout", "120s")
                                    .put("client.transport.nodes_sampler_interval", "5s")
                                    .put("client.transport.sniff", true)
                                    .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(new InetSocketAddress(ip, port)));
        int size = client.connectedNodes().size();

        return size > 0;
    }

    public static void main(String[] args) {
        boolean b = new EsClientImpl6().testConnect("192.168.0.196", 9200, "datacenter", "es6");
        System.out.println(b);
    }
}
