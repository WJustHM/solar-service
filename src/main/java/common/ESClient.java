package common;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by root on 17-1-5.
 */
public class ESClient {
    public static Client client()  {
        try {
            Settings settings = Settings.builder().put("cluster.name", "myApp").build();
            // client.transport.sniff为true，使客户端嗅探整个集群状态，把集群中的其他机器IP加入到客户端中
            Client client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.18.21.142"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.18.21.140"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.18.21.141"), 9300));
            return client;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }
}
