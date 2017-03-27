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
    public  Set<Object> getSingletons() {
        HashSet<Object> objects = new HashSet<Object>();
        objects.add(new TrafficResource(getParamters()));
        return  objects;
    }


   public Map getParamters(){
        Map paramters = new HashMap();
       paramters.put("hbase.zookeeper.quorum","datanode1,datanode2,datanode3");
       paramters.put("hbase.zookeeper.property.clientPort","2181");
       paramters.put("zookeeper.znode.parent","/hbase-unsecure");
        return paramters;
    }

}
