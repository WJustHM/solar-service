package traffic;


import common.InternalPools;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;

import java.util.ArrayList;
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
    @Path("/image")
    public Response imageQuery(@QueryParam("rowkey") final String rowkey) throws IOException {
        StringWriter writer = new StringWriter();
        HashMap metadata = new HashMap();

        Connection hbase = getHbaseConnection();
        HTable table = (HTable)hbase.getTable(TableName.valueOf("TrafficImage"));
        Result rs = table.get(new Get(rowkey.getBytes()));
        byte[] image = rs.getValue("Image".getBytes(), "Data".getBytes());
        String redLightTime = new String(rs.getValue("Image".getBytes(), "RedLightTime".getBytes()));
        String recogMode = new String(rs.getValue("Image".getBytes(), "RecogMode".getBytes()));
        metadata.put("RedLightTime", redLightTime);
        metadata.put("RecogMode", recogMode);
        mapper.writeValue(writer, metadata);

        returnHbaseConnection(hbase);
        return Response.status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Content-Type","image/jpeg")
                .header("metadata",writer.toString())
                .entity(image).build();
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
