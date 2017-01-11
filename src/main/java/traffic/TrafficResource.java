package traffic;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import common.ESClient;
import common.jdbc.JdbcConfig;
import common.jdbc.JdbcConnectionPool;
import common.util.RequestParamter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource {

    private final JedisCluster jc;
    private static AtomicLong c = new AtomicLong();
    private static DecimalFormat df = new DecimalFormat("#.###");
    private JdbcConnectionPool pool;
    private final Client client = ESClient.client();
    private final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    public TrafficResource() {
        System.out.println("Init resouce");
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("suna", 7000));
        jedisClusterNodes.add(new HostAndPort("sunb", 7000));
        jedisClusterNodes.add(new HostAndPort("sunc", 7000));
        jedisClusterNodes.add(new HostAndPort("suna", 7001));
        jedisClusterNodes.add(new HostAndPort("sunb", 7001));
        jedisClusterNodes.add(new HostAndPort("sunc", 7001));
        this.jc = new JedisCluster(jedisClusterNodes);
        pool = new JdbcConnectionPool("com.mysql.jdbc.Driver", JdbcConfig.DEFAULT_JDBC_URL, JdbcConfig.DEFAULT_JDBC_USERNAME, JdbcConfig.DEFAULT_JDBC_PASSWORD);
    }


    @GET
    @Path("/device")
    @Produces("application/json")
    public Response vehicleDynamicCount() {
        HashMap content = new HashMap();
        Set<String> set = jc.hkeys("trafficD");
        for (String ss : set) {
            HashMap carType = new HashMap();
            String[] data = jc.hget("trafficD", ss).split("\\|");
            for (String d : data) {
                String[] da = d.split("\\:");
                carType.put(da[0], da[1]);
            }
            content.put(ss, carType);
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(content)).build();
    }

    @GET
    @Path("/device/history")
    @Produces("application/json")
    public Response vehicleStaticCount() {
        HashMap content = new HashMap();
        Set<String> set = jc.hkeys("trafficS");
        for (String ss : set) {
            content.put(ss, jc.hget("trafficS", ss));
        }


        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(content)).build();
    }


    @GET
    @Path("/sichuan")
    @Produces("application/json")
    public Response vehicleSiChuanCount() {
        String query = "{\"filtered\": {\"filter\": {\"prefix\": {\"licence\": \"川\"}}}}";

        String aggs = "{ \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"licence\",\n" +
                "        \"include\": \"[\\\"a\\\",\\\"b\\\",\\\"c\\\",\\\"d\\\",\\\"e\\\",\\\"f\\\",\\\"g\\\",\\\"h\\\",\\\"i\\\",\\\"j\\\",\\\"k\\\",\\\"l\\\",\\\"m\\\",\\\"n\\\",\\\"o\\\",\\\"p\\\",\\\"q\\\",\\\"r\\\"\\\"s\\\",\\\"t\\\",\\\"u\\\",\\\"v\\\",\\\"w\\\",\\\"x\\\",\\\"y\\\",\\\"z\\\"]\",\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      }\n" +
                "    }}";

        SearchResponse scrollResp = client.prepareSearch().setIndices("solar").setTypes("traffic").setQuery(query).setAggregations(aggs.getBytes())
                .setSize(0).get();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();

        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> map1 = new HashMap();
        Map<Object, Object> map2 = new HashMap();
        while (entry.hasNext()) {
            Terms.Bucket bb = entry.next();
            map2.put(bb.getKey(), bb.getDocCount());
        }
        map1.put("川", map2);
        ESClient.client().close();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(map1)).build();
    }


    @POST
    @Produces("application/json")
    @Consumes("application/json")
    public Response provincice(RequestParamter requestparamter) {
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String start = requestparamter.getStart();
        String end = requestparamter.getEnd();

        long starttime = 0;
        long endtime = 0;

        try {
            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String query = "{    \"filtered\": {\n" +
                "      \"query\": {\n" +
                "        \"query_string\": {\n" +
                "          \"query\": \"*\",\n" +
                "          \"analyze_wildcard\": true\n" +
                "        }\n" +
                "      },\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            {\n" +
                "              \"range\": {\n" +
                "                \"time\": {\n" +
                "                  \"gte\":" + starttime + ",\n" +
                "                  \"lte\":" + endtime + ",\n" +
                "                  \"format\": \"epoch_millis\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          ],\n" +
                "          \"must_not\": []\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        String aggssub = "{    \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"licence\",\n" +
                "        \"include\": {\n" +
                "          \"pattern\": \"[\\\"川\\\",\\\"甘\\\",\\\"黑\\\",\\\"津\\\", \\\"辽\\\", \\\"闽\\\", \\\"琼\\\", \\\"晋\\\", \\\"新\\\", \\\"粤\\\", \\\"浙\\\", \\\"鄂\\\", \\\"贵\\\", \\\"沪\\\", \\\"京\\\", \\\"鲁\\\",\\\"宁\\\", \\\"陕\\\", \\\"皖\\\", \\\"豫\\\", \\\"云\\\", \\\"赣\\\", \\\"桂\\\", \\\"冀\\\",\\\"吉\\\", \\\"蒙\\\", \\\"青\\\", \\\"苏\\\", \\\"湘\\\", \\\"渝\\\", \\\"藏\\\"]\"\n" +
                "        },\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"3\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"vechilType\",\n" +
                "            \"include\": {\n" +
                "              \"pattern\": \"[\\\"0\\\",\\\"1\\\",\\\"2\\\",\\\"3\\\",\\\"4\\\"]\"\n" +
                "            },\n" +
                "            \"size\": 0,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        SearchResponse scrollResp = client.prepareSearch().setIndices("solar").setTypes("traffic").setQuery(query).setAggregations(aggssub.getBytes())
                .setSize(0).execute().actionGet();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();
        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> map1 = new LinkedHashMap();
        while (entry.hasNext()) {
            Map<Object, Long> map2 = new LinkedHashMap();
            Terms.Bucket bb = entry.next();
            Iterator<Terms.Bucket> classBucketIt = ((StringTerms) bb.getAggregations().asMap().get("3")).getBuckets().iterator();
            while (classBucketIt.hasNext()) {
                Terms.Bucket classBucket = classBucketIt.next();
                map2.put(classBucket.getKey(), classBucket.getDocCount());
            }
            map1.put(bb.getKey(), map2);
        }
        ESClient.client().close();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(map1)).build();
    }


    @POST
    @Path("/{province}")
    @Produces("application/json;charset=UTF-8")
    @Consumes("application/json;charset=UTF-8")
    public Response vehicleprovincice(@PathParam("province") String provinces, RequestParamter requestparamter) {
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String start = requestparamter.getStart();
        String end = requestparamter.getEnd();

        long starttime = 0;
        long endtime = 0;

        try {
            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String query = "{    \"filtered\": {\n" +
                "      \"query\": {\n" +
                "        \"query_string\": {\n" +
                "          \"query\": \"*\",\n" +
                "          \"analyze_wildcard\": true\n" +
                "        }\n" +
                "      },\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            {\n" +
                "              \"range\": {\n" +
                "                \"time\": {\n" +
                "                  \"gte\":" + starttime + ",\n" +
                "                  \"lte\":" + endtime + ",\n" +
                "                  \"format\": \"epoch_millis\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          ],\n" +
                "          \"must_not\": []\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        String aggssub = "{ \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"licence\",\n" +
                "        \"include\": {\n" +
                "          \"pattern\": \"[\\\"" + provinces + "\\\"]\"\n" +
                "        },\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"3\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"licence\",\n" +
                "            \"include\": {\n" +
                "              \"pattern\": \"[\\\"a\\\",\\\"b\\\",\\\"c\\\",\\\"d\\\",\\\"e\\\",\\\"f\\\",\\\"g\\\",\\\"h\\\",\\\"i\\\",\\\"j\\\",\\\"k\\\",\\\"l\\\",\\\"m\\\",\\\"n\\\",\\\"o\\\",\\\"p\\\",\\\"q\\\",\\\"r\\\"\\\"s\\\",\\\"t\\\",\\\"u\\\",\\\"v\\\",\\\"w\\\",\\\"x\\\",\\\"y\\\",\\\"z\\\"]\"\n" +
                "            },\n" +
                "            \"size\": 0,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"4\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"vechilType\",\n" +
                "                \"include\": {\n" +
                "                  \"pattern\": \"[\\\"0\\\",\\\"1\\\",\\\"2\\\",\\\"3\\\",\\\"4\\\"]\"\n" +
                "                },\n" +
                "                \"size\": 0\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        SearchResponse scrollResp = client.prepareSearch().setIndices("solar").setTypes("traffic").setQuery(query).setAggregations(aggssub.getBytes())
                .setSize(0).execute().actionGet();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();

        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> map1 = new LinkedHashMap();
        Map<Object, Object> map2 = new LinkedHashMap();
        Map<Object, Long> map3 = new LinkedHashMap();
        while (entry.hasNext()) {
            Terms.Bucket bb = entry.next();
            Iterator<Terms.Bucket> classBucketIt = ((StringTerms) bb.getAggregations().asMap().get("3")).getBuckets().iterator();
            Terms.Bucket classBucket;
            while (classBucketIt.hasNext()) {
                classBucket = classBucketIt.next();
                Iterator<Terms.Bucket> classBucketIt4 = ((StringTerms) classBucket.getAggregations().asMap().get("4")).getBuckets().iterator();
                Terms.Bucket classBucket4;
                while (classBucketIt4.hasNext()) {
                    classBucket4 = classBucketIt4.next();
                    map3.put(classBucket4.getKey(), classBucket4.getDocCount());
                }
                map2.put(classBucket.getKey(), map3);
            }
            map1.put(bb.getKey(), map2);
        }
        ESClient.client().close();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(map1)).build();
    }


    @POST
    @Path("/{provinces}/{city}")
    @Produces("application/json;charset=utf-8")
    @Consumes("application/json;charset=utf-8")
    public Response vehicleprovincicecity(@PathParam("provinces") String provinces, @PathParam("city") String city, RequestParamter requestparamter) {
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String start = requestparamter.getStart();
        String end = requestparamter.getEnd();
        long starttime = 0;
        long endtime = 0;

        try {
            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String query = "{    \"filtered\": {\n" +
                "      \"query\": {\n" +
                "        \"query_string\": {\n" +
                "          \"query\": \"*\",\n" +
                "          \"analyze_wildcard\": true\n" +
                "        }\n" +
                "      },\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            {\n" +
                "              \"range\": {\n" +
                "                \"time\": {\n" +
                "                  \"gte\":" + starttime + ",\n" +
                "                  \"lte\":" + endtime + ",\n" +
                "                  \"format\": \"epoch_millis\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          ],\n" +
                "          \"must_not\": []\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        String aggssub = "{ \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"licence\",\n" +
                "        \"include\": {\n" +
                "          \"pattern\": \"[\\\"" + provinces + "\\\"]\"\n" +
                "        },\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"3\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"licence\",\n" +
                "            \"include\": {\n" +
                "              \"pattern\": \"[\\\"" + city + "\\\"]\"\n" +
                "            },\n" +
                "            \"size\": 0,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"4\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"vechilType\",\n" +
                "                \"include\": {\n" +
                "                  \"pattern\": \"[\\\"0\\\",\\\"1\\\",\\\"2\\\",\\\"3\\\",\\\"4\\\"]\"\n" +
                "                },\n" +
                "                \"size\": 0\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        SearchResponse scrollResp = client.prepareSearch().setIndices("solar").setTypes("traffic").setQuery(query).setAggregations(aggssub.getBytes())
                .setSize(0).execute().actionGet();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();

        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> map1 = new LinkedHashMap();
        Map<Object, Object> map2 = new LinkedHashMap();
        Map<Object, Long> map3 = new LinkedHashMap();
        while (entry.hasNext()) {
            Terms.Bucket bb = entry.next();
            Iterator<Terms.Bucket> classBucketIt = ((StringTerms) bb.getAggregations().asMap().get("3")).getBuckets().iterator();
            Terms.Bucket classBucket;
            while (classBucketIt.hasNext()) {
                classBucket = classBucketIt.next();
                Iterator<Terms.Bucket> classBucketIt4 = ((StringTerms) classBucket.getAggregations().asMap().get("4")).getBuckets().iterator();
                Terms.Bucket classBucket4;
                while (classBucketIt4.hasNext()) {
                    classBucket4 = classBucketIt4.next();
                    map3.put(classBucket4.getKey(), classBucket4.getDocCount());
                }
                map2.put(classBucket.getKey(), map3);
            }
            map1.put(bb.getKey(), map2);
        }
        ESClient.client().close();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(map1)).build();
    }


    @GET
    @Path("/map")
    @Produces("application/json")
    public Response vehicleMap() {

        String aggs = "{ \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"device\",\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      }\n" +
                "    }}";

        SearchResponse scrollResp = client.prepareSearch().setIndices("solar").setTypes("traffic").setAggregations(aggs.getBytes())
                .setSize(0).execute().actionGet();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();

        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> map1 = new LinkedHashMap();
        Connection connn = pool.getConnection();
        String sql = null;
        while (entry.hasNext()) {
            Map<String, Object> map2 = new LinkedHashMap();
            Terms.Bucket bb = entry.next();
            sql = "select longitude,latitude from device where deviceId=" + bb.getKey();
            try {
                ResultSet rs = connn.prepareCall(sql).executeQuery();
                while (rs.next()) {
                    map2.put("count", bb.getDocCount());
                    map2.put("Lon", rs.getString("longitude"));
                    map2.put("Lat", rs.getString("latitude"));
                    rs.getString("longitude");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            map1.put(bb.getKey(), map2);
        }
        ESClient.client().close();

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(gson.toJson(map1)).build();
    }

}
