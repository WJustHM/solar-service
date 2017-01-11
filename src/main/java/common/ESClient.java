package common;

import com.sksamuel.elastic4s.ElasticsearchClientUri;
import common.es.EsConnectionPool;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by root on 17-1-5.
 */
public class ESClient {
    private static final String ESPREFIX = "merge.es";

    public static Client client() {
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

//
//    public static Client getClientPool() {
//        PoolConfig poolconfig = new PoolConfig();
//        poolconfig.setMaxTotal(30);
//        poolconfig.setMaxIdle(20);
//        poolconfig.setMaxWaitMillis(10000);
//        poolconfig.setTestOnBorrow(true);
//        Object[] ob = sett();
//
//        HashMap<String, Object> doc = new HashMap();
//        HashMap<String, String> geo = new HashMap<String, String>();
//        doc.put("rowkey", "fdsf");
//        doc.put("time", "sdf");
//        doc.put("device", "sdfsd");
//        doc.put("licencey", "sfsdf512D");
//        doc.put("vechilType", "dsf");
//        doc.put("vechilSpeed", "sdf");
//        doc.put("illegalId", "sdf");
//        geo.put("lat", "30.654758309411054");
//        geo.put("lon", "104.11306414117914");
//        doc.put("geo", geo);
//
//        EsConnectionPool pool = new EsConnectionPool(poolconfig, (Settings) ob[0], (ElasticsearchClientUri) ob[1]);
////          pool.getConnection().client().prepareIndex("traffic1", "vehicle2").setSource(doc).execute().actionGet();
//        return pool.getConnection().client();
//
//    }
//
//    public static Object[] sett() {
//        CompositeConfiguration conf = new CompositeConfiguration();
//        try {
//            conf.addConfiguration(new PropertiesConfiguration(System.getProperty("user.dir") + "/src/main/resources/solar.conf"));
//            String url = StringUtils.join(conf.getStringArray(ESPREFIX + "." + "addrees"), ",");
//            ElasticsearchClientUri uri = ElasticsearchClientUri.apply("elasticsearch://" + url);
//            Object[] array = new Object[2];
//
//            Settings.Builder settingBuilder = Settings.builder();
//            Iterator<String> esKeys = conf.getKeys(ESPREFIX);
//            while (esKeys.hasNext()) {
//                String temKey = esKeys.next().toString();
//                String key = temKey.replace(ESPREFIX + ".", "");
//                //termkey就是key，getStringArray得到value值，StringUtils.join把数组用逗号分隔
//                String value = StringUtils.join(conf.getStringArray(temKey), ",");
//                settingBuilder.put(key, value);
//            }
//            array[0] = settingBuilder.build();
//            array[1] = uri;
//            System.out.println(array[0]);
//            System.out.println(array[1]);
//
//            return array;
//
//        } catch (ConfigurationException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

}
