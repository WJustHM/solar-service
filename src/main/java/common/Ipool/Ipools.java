package common.Ipool;

import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by xuefei_wang on 17-3-27.
 */
public interface Ipools {

    public HbaseConnectionPool getHbaseConnectionPool(final PoolConfig poolConfig ,final Map paramters);

    public JdbcConnectionPool getJdbcConnectionPool(final PoolConfig poolConfig ,final Map paramters);

    public KafkaConnectionPool getKafkaConnectionPool(final PoolConfig poolConfig , final Map paramters);

    public EsConnectionPool getEsConnectionPool(final PoolConfig poolConfig , final Map paramters);

    public JedisPool getJedisPool(final PoolConfig poolConfig ,final Map paramters);

}
