package common;

import common.Ipool.PoolConfig;
import common.es.EsConnectionPool;
import common.hbase.HbaseConnectionPool;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.producer.Producer;
import org.elasticsearch.client.transport.TransportClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by xuefei_wang on 17-3-24.
 */
abstract public class Pools {

    protected HbaseConnectionPool hbaseConnectionPool;

    protected EsConnectionPool esConnectionPool;

    protected JdbcConnectionPool jdbcConnectionPool;

    protected KafkaConnectionPool kafkaConnectionPool;

    protected JedisPool jedisPool;

    protected Map paramters ;

    protected PoolConfig poolConfig;


    public Pools(PoolConfig poolConfig, Map paramters){
        this.poolConfig = poolConfig;
        this.paramters = paramters;
    }

    public PoolConfig getPoolConfig(){
        if (poolConfig == null){
            poolConfig = new PoolConfig();
            poolConfig.setMaxTotal(100);
            poolConfig.setMaxIdle(100);
            poolConfig.setMaxWaitMillis(1000000);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestOnCreate(true);
        }
        return poolConfig;
    }

    public Connection getHbaseConnection(){
        synchronized(hbaseConnectionPool){
            if (hbaseConnectionPool == null || hbaseConnectionPool.isClosed()){
                hbaseConnectionPool = getHbaseConnectionPool();
            }
            return hbaseConnectionPool.getConnection();
        }
    }

    public void returnHbaseConnection(Connection connection){
        synchronized(hbaseConnectionPool){
            hbaseConnectionPool.returnConnection(connection);
        }
    }

    public TransportClient getEsConnection(){
        synchronized(esConnectionPool){
            if (esConnectionPool == null || esConnectionPool.isClosed()){
                esConnectionPool = getEsConnectionPool();
            }
            return esConnectionPool.getConnection();
        }
    }

    public void returnEsConnection(TransportClient connection){
        synchronized(esConnectionPool){
            esConnectionPool.returnConnection(connection);
        }
    }


    public java.sql.Connection getMysqlConnection(){
        synchronized(jdbcConnectionPool){
            if (jdbcConnectionPool == null || jdbcConnectionPool.isClosed()){
                jdbcConnectionPool = getJdbcConnectionPool();
            }
            return jdbcConnectionPool.getConnection();
        }
    }

    public void returnMysqlConnection(java.sql.Connection connection){
        synchronized(jdbcConnectionPool){
            jdbcConnectionPool.returnConnection(connection);
        }
    }


    public Producer getKafkaProducer(){
        synchronized(kafkaConnectionPool){
            if (kafkaConnectionPool == null || kafkaConnectionPool.isClosed()){
                kafkaConnectionPool = getKafkaConnectionPool();
            }
            return kafkaConnectionPool.getConnection();
        }
    }

    public void returnKafkaProducer(Producer producer){
        synchronized(kafkaConnectionPool){
            kafkaConnectionPool.returnConnection(producer);
        }
    }


    public Jedis getRedis() {
        synchronized (jedisPool){
            if (jedisPool == null || jedisPool.isClosed()){
                jedisPool = getRedisPool();
            }
            return jedisPool.getResource();
        }
    }

    public void returnRedis(Jedis client){
        synchronized (jedisPool){
            jedisPool.returnResource(client);
        }
    }



   abstract public HbaseConnectionPool getHbaseConnectionPool();
   abstract public EsConnectionPool getEsConnectionPool();
   abstract public JdbcConnectionPool getJdbcConnectionPool();
   abstract public KafkaConnectionPool getKafkaConnectionPool();
   abstract  public  JedisPool getRedisPool();
}
