package traffic;


import common.InternalPools;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortOrder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource extends InternalPools {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SearchRequestBuilder request = getEsConnection().prepareSearch().setIndices("djj").setTypes("djj");

    public TrafficResource(Map paramters) {
        super(paramters);
    }

    @GET
    @Path("/hbase")
    public Response testHbase() throws IOException {
        Connection hbase = getHbaseConnection();
        List<HRegionInfo> regions = hbase.getAdmin().getTableRegions(TableName.valueOf("Traffic"));
        for (HRegionInfo info : regions) {
            System.out.println(info.getRegionId());
        }
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("OK").build();
    }

    @GET
    @Path("/es")
    public Response testES(@QueryParam("start") final String start,
                           @QueryParam("end") final String end,
                           @QueryParam("PlateLicense") final String PlateLicense) throws Exception {
        StringWriter writer = new StringWriter();
        HashMap content = new HashMap();

        SearchResponse response = request
                .setQuery(QueryBuilders.termQuery("HPHM", "川H16357"))
                .setPostFilter(QueryBuilders.rangeQuery("JGSJ").gte("2017-03-16 16:08:28").lte("2017-03-20 16:08:28"))
                .setSize(10000)
                .execute().actionGet();

//        SearchResponse respons=request
//                .setQuery(QueryBuilders.termQuery("HPHM","川H16357"))
//                .addAggregation(
//                AggregationBuilders.dateRange("JGSJ").addRange("2017-03-16 16:08:28","2017-04-00 16:08:28"))
//                .execute().actionGet();
//        Map<String, Aggregation> aggMap = respons.getAggregations().asMap();


        for (SearchHit i : response.getHits().getHits()) {
            content.put(i.getSource().get("JGSJ").toString(), i.getSource().get("KKBH").toString());
        }
        mapper.writeValue(writer, content);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

}
