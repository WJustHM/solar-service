package common;

import common.Ipool.PoolConfig;
import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by xuefei_wang on 17-3-27.
 */
class InternalPools extends Pools {

    public InternalPools(PoolConfig poolConfig, Map paramters){
        super(poolConfig,paramters);
    }

    public InternalPools( Map paramters){
       this(null,paramters);
    }


    @Override
    public HbaseConnectionPool getHbaseConnectionPool() {


        return null;
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
