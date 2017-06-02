package service;

import common.Pools;
import common.hbase.HbaseConnectionPool;
import traffic.TrafficResource;

import javax.ws.rs.core.Application;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by xuefei_wang on 16-12-13.
 */
public class TrafficApplication extends Application {

    @Override
    public Set<Object> getSingletons() {
        HashSet<Object> objects = new HashSet<Object>();
        objects.add(new TrafficResource(getParamters()));

        return objects;
    }


   public Map getParamters(){
       Map paramters = new HashMap();
       paramters.put("hbase.zookeeper.quorum","datanode4,datanode5,datanode6");
       paramters.put("hbase.zookeeper.property.clientPort","2181");
       paramters.put("zookeeper.znode.parent","/hbase-unsecure");

       paramters.put("cluster.name","handge-cloud");
       paramters.put("es.url","datanode4:9300,datanode5:9300,datanode6:9300");

       paramters.put("mysql.driver","com.mysql.jdbc.Driver");
       paramters.put("mysql.jdbc.url","jdbc:mysql://172.20.31.127:3306/solar");
       paramters.put("mysql.user.name","root");
       paramters.put("mysql.user.password","mysql");

       paramters.put("bootstrap.servers","datanode4:6667,datanode5:6667,datanode6:6667");
       paramters.put("producer.type","async");
       paramters.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
       paramters.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
       paramters.put("batch.num.messages","1000");
       paramters.put("max.request.size","1000973460");
       paramters.put("enable.auto.commit","true");
       paramters.put("auto.offset.reset","latest");

       paramters.put("redis.host","172.20.31.127");
       paramters.put("redis.port","6379");
       return paramters;
    }

}
