package common;

import common.Ipool.PoolConfig;
import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import redis.clients.jedis.JedisPool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by xuefei_wang on 17-3-27.
 */
public class InternalPools extends Pools {

    public InternalPools(PoolConfig poolConfig, Map<String,String> paramters){
        super(poolConfig,paramters);
    }

    public InternalPools( Map<String,String> paramters){
       this(null,paramters);
    }

    @Override
    public HbaseConnectionPool getHbaseConnectionPool() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",paramters.getOrDefault("hbase.zookeeper.quorum","datanode1,datanode2,datanode3"));
        configuration.set("hbase.zookeeper.property.clientPort",paramters.getOrDefault("hbase.zookeeper.property.clientPort","2181"));
        configuration.set("zookeeper.znode.parent",paramters.getOrDefault("zookeeper.znode.parent","/hbase-unsecure"));
        return new HbaseConnectionPool(getPoolConfig(),configuration);
    }

    @Override
    public EsConnectionPool getEsConnectionPool()  {
        Settings.Builder settings = Settings.builder();
        settings.put("cluster.name",paramters.getOrDefault("cluster.name",""));

        LinkedList<InetSocketTransportAddress>  address = new LinkedList<InetSocketTransportAddress>();
        String[] hosts = paramters.get("es.url").split(",");
        for(String host : hosts){
           String[] hp = host.split(":");
            try {
                address.add(new InetSocketTransportAddress(InetAddress.getByName(hp[0]), Integer.valueOf(hp[1])));
            }catch (Exception e){
                e.printStackTrace();
            }

        }
            return new EsConnectionPool(getPoolConfig(),settings.build(),address);
    }


    @Override
    public JdbcConnectionPool getJdbcConnectionPool() {
        return null;
    }

    @Override
    public KafkaConnectionPool getKafkaConnectionPool() {
        return null;
    }

    @Override
    public JedisPool getRedisPool() {
        return null;
    }


}
