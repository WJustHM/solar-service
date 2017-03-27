package common;

import common.Ipool.PoolConfig;
import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import redis.clients.jedis.JedisPool;

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
        System.out.println("*************************************");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",paramters.getOrDefault("hbase.zookeeper.quorum","datanode1,datanode2,datanode3"));
        configuration.set("hbase.zookeeper.property.clientPort",paramters.getOrDefault("hbase.zookeeper.property.clientPort","2181"));
        configuration.set("zookeeper.znode.parent",paramters.getOrDefault("zookeeper.znode.parent","/hbase-unsecure"));
        return new HbaseConnectionPool(getPoolConfig(),configuration);
    }

    @Override
    public EsConnectionPool getEsConnectionPool() {
        return null;
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
