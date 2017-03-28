package traffic;


import common.InternalPools;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource extends InternalPools {

    private final ObjectMapper mapper = new ObjectMapper();

    public TrafficResource(Map paramters) {
        super(paramters);
    }

    @GET
    @Path("/info")
    @Produces("text/html;charset=utf8")
    @Consumes("text/html;charset=utf8")
    public Response infoQuery(@QueryParam("PlateLicens") final String PlateLicense,
                              @QueryParam("start") final String start,
                              @QueryParam("end") final String end) throws IOException {
        TransportClient conn = getEsConnection();
        StringWriter writer = new StringWriter();
        HashMap content = new HashMap();
        SearchResponse response = conn.prepareSearch().setIndices("traffic").setTypes("traffic")
                .setQuery(QueryBuilders.termQuery("Plate_License.keyword", PlateLicense))
                .setPostFilter(QueryBuilders.rangeQuery("Time.keyword").gte(start.replace("\"", "")).lte(end.replace("\"", "")))
                .setSize(10000)
                .execute().actionGet();

        for (SearchHit i : response.getHits().getHits()) {
            HashMap metadata = new HashMap();
            metadata.put("SBBH",i.getSource().get("SBBH").toString());
            metadata.put("Vehicle_ChanIndex", i.getSource().get("Vehicle_ChanIndex").toString());
            metadata.put("Plate_License", i.getSource().get("Plate_License").toString());
            metadata.put("Vehicle_Speed", i.getSource().get("Vehicle_Speed").toString());
            metadata.put("IllegalType", i.getSource().get("IllegalType").toString());
            metadata.put("Dir", i.getSource().get("Dir").toString());
            metadata.put("Vehicle_Color", i.getSource().get("Vehicle_Color").toString());
            metadata.put("Plate_PlateType", i.getSource().get("Plate_PlateType").toString());
            content.put(i.getSource().get("Time").toString(), metadata);
        }
        mapper.writeValue(writer, content);
        returnEsConnection(conn);
//        Connection hbase = getHbaseConnection();
//
//        List<HRegionInfo> regions = hbase.getAdmin().getTableRegions(TableName.valueOf("Traffic"));
//        for (HRegionInfo info : regions) {
//            System.out.println(info.getRegionId());
//        }
//        returnHbaseConnection(hbase);
//        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
        return Response.status(200).header("Access-Control-Allow-Origin", "*").header("sessionstatus",writer.toString()).entity("ok").build();
    }

    @GET
    @Path("/track")
    public Response trackQuery(@QueryParam("start") final String start,
                           @QueryParam("end") final String end,
                           @QueryParam("PlateLicense") final String PlateLicense) throws Exception {
        TransportClient conn = getEsConnection();
        StringWriter writer = new StringWriter();
        HashMap content = new HashMap();

        SearchResponse response = conn.prepareSearch().setIndices("traffic").setTypes("traffic")
                .setQuery(QueryBuilders.termQuery("Plate_License.keyword", PlateLicense))
                .setPostFilter(QueryBuilders.rangeQuery("Time.keyword").gte(start.replace("\"", "")).lte(end.replace("\"", "")))
                .setSize(10000)
                .execute().actionGet();

        for (SearchHit i : response.getHits().getHits()) {
            content.put(i.getSource().get("Time").toString(), i.getSource().get("SBBH").toString());
        }
        mapper.writeValue(writer, content);
        returnEsConnection(conn);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

}
