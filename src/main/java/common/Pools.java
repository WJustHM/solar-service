package common;

import common.Ipool.Ipools;
import common.Ipool.PoolConfig;
import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by xuefei_wang on 17-3-24.
 */
abstract public class Pools implements Ipools {

    protected HbaseConnectionPool hbaseConnectionPool;

    protected EsConnectionPool esConnectionPool;

    protected JdbcConnectionPool jdbcConnectionPool;

    protected KafkaConnectionPool kafkaConnectionPool;

    protected JedisPool jedisPool;

    protected Map paramters ;


    public PoolConfig getPoolConfig(){
        PoolConfig poolConfig = new PoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(100);
        poolConfig.setMaxWaitMillis(1000000);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestOnCreate(true);
        return poolConfig;
    }

    public Connection getHbaseConnection(){
        synchronized(hbaseConnectionPool){
            if (hbaseConnectionPool == null || hbaseConnectionPool.isClosed()){
                hbaseConnectionPool = getHbaseConnectionPool(getPoolConfig(),paramters);
            }
            return hbaseConnectionPool.getConnection();
        }
    }

    public void returnHbaseConnection(Connection connection){

        synchronized(hbaseConnectionPool){
            hbaseConnectionPool.returnConnection(connection);
        }
    }


}
