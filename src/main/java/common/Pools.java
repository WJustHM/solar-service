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

import java.net.UnknownHostException;
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

    public Map<String,String> paramters ;

    public PoolConfig poolConfig;


    public Pools(PoolConfig poolConfig, Map<String,String> paramters){
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

    public synchronized Connection getHbaseConnection(){

            if (hbaseConnectionPool == null || hbaseConnectionPool.isClosed()){
                hbaseConnectionPool = getHbaseConnectionPool();
            }

        return hbaseConnectionPool.getConnection();
    }

    public synchronized void returnHbaseConnection(Connection connection){

            hbaseConnectionPool.returnConnection(connection);
    }

    public synchronized TransportClient getEsConnection(){

            if (esConnectionPool == null || esConnectionPool.isClosed()){
                esConnectionPool = getEsConnectionPool();
            }
            return esConnectionPool.getConnection();

    }

    public synchronized void returnEsConnection(TransportClient connection){

            esConnectionPool.returnConnection(connection);

    }


    public synchronized java.sql.Connection getMysqlConnection(){

            if (jdbcConnectionPool == null || jdbcConnectionPool.isClosed()){
                jdbcConnectionPool = getJdbcConnectionPool();
            }
            return jdbcConnectionPool.getConnection();

    }

    public synchronized void returnMysqlConnection(java.sql.Connection connection){

            jdbcConnectionPool.returnConnection(connection);

    }


    public synchronized Producer getKafkaProducer(){

            if (kafkaConnectionPool == null || kafkaConnectionPool.isClosed()){
                kafkaConnectionPool = getKafkaConnectionPool();
            }
            return kafkaConnectionPool.getConnection();

    }

    public synchronized void returnKafkaProducer(Producer producer){

            kafkaConnectionPool.returnConnection(producer);

    }


    public synchronized Jedis getRedis() {

            if (jedisPool == null || jedisPool.isClosed()){
                jedisPool = getRedisPool();
            }
            return jedisPool.getResource();

    }

    public synchronized void returnRedis(Jedis client){
            jedisPool.returnResource(client);
    }



   abstract public HbaseConnectionPool getHbaseConnectionPool();
   abstract public EsConnectionPool getEsConnectionPool() ;
   abstract public JdbcConnectionPool getJdbcConnectionPool();
   abstract public KafkaConnectionPool getKafkaConnectionPool();
   abstract  public  JedisPool getRedisPool();
}
