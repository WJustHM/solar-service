package traffic;


import common.InternalPools;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.client.transport.TransportClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource extends InternalPools {

    public TrafficResource(Map paramters) {
       super(paramters);
    }

    @GET
    @Path("/hbase")
    public Response testHbase() throws IOException{
        Connection hbase = getHbaseConnection();
        List<HRegionInfo> regions = hbase.getAdmin().getTableRegions(TableName.valueOf("Traffic"));
        for (HRegionInfo info : regions){
            System.out.println(info.getRegionId());
        }
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("OK").build();
    }

    @GET
    @Path("/es")
    public Response testES() throws IOException{
        TransportClient es = getEsConnection();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("OK").build();
    }

}
