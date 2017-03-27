package service;

import common.Pools;
import common.hbase.HbaseConnectionPool;
import traffic.TrafficResource;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xuefei_wang on 16-12-13.
 */
public class TrafficApplication extends Application {

    @Override
    public  Set<Object> getSingletons() {
        HashSet<Object> objects = new HashSet<Object>();
        objects.add(new TrafficResource());
        return  objects;
    }

}
