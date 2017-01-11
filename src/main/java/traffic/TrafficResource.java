package traffic;


import common.jdbc.JdbcConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource {

    private final JedisCluster jc;
    private final DecimalFormat df = new DecimalFormat("#.###");
    private final JdbcConnectionPool pool;
    private final SearchRequestBuilder search;
    private final ObjectMapper mapper = new ObjectMapper();


    public TrafficResource() {
        this.jc = initRedis();
        this.pool = initMysqlPool();
        this.search = initSearchRequestBuilder();
    }

    private SearchRequestBuilder initSearchRequestBuilder(){
        Settings   setting =  Settings.builder().put("cluster.name", "myApp").build();
        TransportClient  client =  TransportClient.builder().settings(setting).build();
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("172.18.21.142",9300)));
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("172.18.21.140", 9300)));
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("172.18.21.141", 9300)));
        SearchRequestBuilder  search = client.prepareSearch().setIndices("solar").setTypes("traffic");
        return search;
    }

    private  JedisCluster initRedis(){
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("suna", 7000));
        jedisClusterNodes.add(new HostAndPort("sunb", 7000));
        jedisClusterNodes.add(new HostAndPort("sunc", 7000));
        jedisClusterNodes.add(new HostAndPort("suna", 7001));
        jedisClusterNodes.add(new HostAndPort("sunb", 7001));
        jedisClusterNodes.add(new HostAndPort("sunc", 7001));
        return new JedisCluster(jedisClusterNodes);
    }

    private JdbcConnectionPool initMysqlPool(){
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://master-1:3306/solar";
        String user = "root";
        String passwd = "mysql";
        return new JdbcConnectionPool(driver, url, user, passwd);

    }



    @GET
    @Path("/device")
    @Produces("application/json")
    public Response vehicleDynamicCount() throws IOException {
        StringWriter writer = new StringWriter();

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
        mapper.writeValue(writer,content);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @GET
    @Path("/device/history")
    @Produces("application/json")
    public Response vehicleStaticCount() throws IOException {
        StringWriter writer = new StringWriter();

        HashMap content = new HashMap();
        Set<String> set = jc.hkeys("trafficS");
        for (String ss : set) {
            content.put(ss, jc.hget("trafficS", ss));
        }

        mapper.writeValue(writer,content);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }


    @GET
    @Path("/provinces")
    @Produces("application/json")
    @Consumes("application/json")
    public Response provincices(@QueryParam("start") final String start ,
                                @QueryParam("end") final String end)throws IOException {
        StringWriter writer = new StringWriter();
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long starttime = 0;
        long endtime = 0;
        try {

            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (Exception e) {
            starttime = 0;
            endtime = System.currentTimeMillis();
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



        SearchResponse scrollResp = search.setQuery(query).setAggregations(aggssub.getBytes())
                .setSize(0).execute().actionGet();

        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();
        Iterator<Terms.Bucket> entry = ((StringTerms) aggMap.get("2")).getBuckets().iterator();
        Map<Object, Object> content = new LinkedHashMap();
        while (entry.hasNext()) {
            Map<Object, Long> m = new LinkedHashMap();
            Terms.Bucket bb = entry.next();
            Iterator<Terms.Bucket> classBucketIt = ((StringTerms) bb.getAggregations().asMap().get("3")).getBuckets().iterator();
            while (classBucketIt.hasNext()) {
                Terms.Bucket classBucket = classBucketIt.next();
                m.put(classBucket.getKey(), classBucket.getDocCount());
            }
            content.put(bb.getKey(), m);
        }

        mapper.writeValue(writer,content);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }


    @GET
    @Path("/provinces/{province}")
    @Produces("application/json;charset=UTF-8")
    @Consumes("application/json;charset=UTF-8")

    public Response vehicleMap(@PathParam("province") String province,
                               @QueryParam("start") final String start ,
                               @QueryParam("end") final String end)throws IOException {
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long starttime = 0;
        long endtime = 0;

        try {
            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (Exception e) {
            starttime = 0;
            endtime =  System.currentTimeMillis();
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
                "          \"pattern\": \"[\\\"" + province + "\\\"]\"\n" +
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


        SearchResponse scrollResp = search.setQuery(query).setAggregations(aggssub.getBytes())
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


        StringWriter writer = new StringWriter();
        mapper.writeValue(writer,map1);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();

    }


    @GET
    @Path("/provinces/{provinces}/{city}")
    @Produces("application/json;charset=utf-8")
    @Consumes("application/json;charset=utf-8")
    public Response vehicleMap2(@PathParam("provinces") String provinces, @PathParam("city") String city,
                                @QueryParam("start") final String start ,
                                @QueryParam("end") final String end) throws IOException  {
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long starttime = 0;
        long endtime = 0;

        try {
            starttime = simple.parse(start).getTime();
            endtime = simple.parse(end).getTime();
        } catch (Exception e) {

             starttime = 0;
             endtime =  System.currentTimeMillis();
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



        SearchResponse scrollResp = search.setQuery(query).setAggregations(aggssub.getBytes())
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

        StringWriter writer = new StringWriter();
        mapper.writeValue(writer,map1);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }


    @GET
    @Path("/hotmap")
    @Produces("application/json")

    public Response vehicleMining()  throws IOException {

        String aggs = "{ \"2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"device\",\n" +
                "        \"size\": 0,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      }\n" +
                "    }}";


        SearchResponse scrollResp = search.setAggregations(aggs.getBytes())
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
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer,map1);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();

    }

    static class Contact{



    }
}
