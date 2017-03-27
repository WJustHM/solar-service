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
import java.util.Properties;

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
        String MYSQL_DRIVER = paramters.getOrDefault("mysql.driver","");
        String MYSQL_JDBC_URL = paramters.getOrDefault("mysql.jdbc.url","");
        String MYSQL_USER_NAME = paramters.getOrDefault("mysql.user.name","");
        String MYSQL_USER_PASSWORD = paramters.getOrDefault("mysql.user.password","");
        JdbcConnectionPool mysqlPool = new JdbcConnectionPool(
                getPoolConfig(),
                MYSQL_DRIVER,
                MYSQL_JDBC_URL,
                MYSQL_USER_NAME,
                MYSQL_USER_PASSWORD
        );
        return mysqlPool;
    }

    @Override
    public KafkaConnectionPool getKafkaConnectionPool() {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", paramters.getOrDefault("bootstrap.servers",""));
        kafkaConfig.setProperty("producer.type", paramters.getOrDefault("producer.type",""));
        kafkaConfig.setProperty("key.serializer", paramters.getOrDefault("key.serializer",""));
        kafkaConfig.setProperty("value.serializer", paramters.getOrDefault("value.serializer",""));
        kafkaConfig.setProperty("batch.num.messages", paramters.getOrDefault("batch.num.messages",""));
        kafkaConfig.setProperty("max.request.size", paramters.getOrDefault("max.request.size",""));
        kafkaConfig.setProperty("enable.auto.commit", paramters.getOrDefault("enable.auto.commit",""));
        kafkaConfig.setProperty("auto.offset.reset", paramters.getOrDefault("auto.offset.reset",""));
        KafkaConnectionPool kafkaPool = new KafkaConnectionPool(getPoolConfig(), kafkaConfig);
        return kafkaPool;
    }

    @Override
    public JedisPool getRedisPool() {
        String REDIS_HOST = paramters.getOrDefault("redis.host","");
        String REDIS_PORT = paramters.getOrDefault("redis.port","");
        JedisPool jedisPool = new JedisPool(
                getPoolConfig(),
                REDIS_HOST,
                Integer.valueOf(REDIS_PORT));
        return jedisPool;
    }
}
