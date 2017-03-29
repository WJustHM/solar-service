package traffic;


import common.InternalPools;
import common.jdbc.JdbcConnectionPool;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/traffic")
public class TrafficResource extends InternalPools {

    private final ObjectMapper mapper = new ObjectMapper();
    private HashMap<String, String> mysqlmap = new HashMap<String, String>();

    public TrafficResource(Map paramters) {
        super(paramters);
    }

    @GET
    @Path("/image")
    public Response imageQuery(@QueryParam("rowkey") final String rowkey) throws IOException {
        StringWriter writer = new StringWriter();
        HashMap metadata = new HashMap();

        Connection hbase = getHbaseConnection();
        HTable table = (HTable) hbase.getTable(TableName.valueOf("TrafficImage"));
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
                .header("Content-Type", "image/jpeg")
                .header("metadata", writer.toString())
                .entity(image).build();
    }

    @GET
    @Path("/track")
    public Response trackQuery(
            @QueryParam("start") final String start,
            @QueryParam("end") final String end,
            @QueryParam("PlateLicense") final String PlateLicense) throws Exception {
        TransportClient conn = getEsConnection();
        StringWriter writer = new StringWriter();
        Map<String, HashMap> map = new LinkedHashMap<String, HashMap>();
        if (mysqlmap == null) {
            java.sql.Connection connmysql = getMysqlConnection();
            String query = "SELECT id,lonlat FROM gate";
            ResultSet rs = connmysql.prepareStatement(query).executeQuery();
            while (rs.next()) {
                mysqlmap.put(rs.getString(1), rs.getString(2));
            }
            returnMysqlConnection(connmysql);
        }

        SearchResponse response = conn.prepareSearch().setIndices("traffic").setTypes("traffic")
                .setQuery(QueryBuilders.termQuery("Plate_License.keyword", PlateLicense))
                .setPostFilter(QueryBuilders.rangeQuery("Time.keyword").gte(start.replace("\"", "")).lte(end.replace("\"", "")))
                .addSort("Time.keyword", SortOrder.ASC)
                .setSize(10000)
                .execute().actionGet();
        for (SearchHit i : response.getHits().getHits()) {
            HashMap content = new HashMap();
            content.put("SBBH", i.getSource().get("SBBH").toString());
            content.put("RowKey", i.getSource().get("RowKey").toString());
            content.put("Vehicle_Speed", i.getSource().get("Vehicle_Speed").toString());
            content.put("lonlat", mysqlmap.get(i.getSource().get("SBBH").toString()));
            map.put(i.getSource().get("Time").toString(), content);
        }
        mapper.writeValue(writer, map);
        returnEsConnection(conn);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @GET
    @Path("/statistics")
    public Response statisticsQuery(
            @QueryParam("by") final String by,
            @QueryParam("start") final String start,
            @QueryParam("end") final String end,
            @QueryParam("deviceId") final String deviceId
    ) throws IOException, ParseException {
        switch (by) {
            case "month":
                return HbasequeryTrafficStatisticsMonth(start, end, deviceId);
            case "day":
                return HbasequeryTrafficStatisticsDay(start, end, deviceId);
            case "hour":
                return HbasequeryTrafficStatisticsHour(start, end, deviceId);
            case "minute":
                return HbasequeryTrafficStatisticsMinute(start, end, deviceId);
            default:
                return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("search by are not found!").build();
        }
    }

    public Response HbasequeryTrafficStatisticsMonth(String start, String end, String deviceId) throws IOException {
        Connection hbase = getHbaseConnection();
        Table table = hbase.getTable(TableName.valueOf("TrafficInfo"));
        Map<String, Integer> typetotal = new LinkedHashMap<String, Integer>();
        Map<String, Integer> monthcount = new LinkedHashMap<String, Integer>();

        int regionnum = Integer.parseInt(deviceId.substring(deviceId.length() - 1)) % 3;
        String startrow = regionnum + "|" + deviceId + "_" + start.replace("\"", "").split(" ")[0];
        String endrow = regionnum + "|" + deviceId + "_" + end.replace("\"", "").split(" ")[0] + "1";
        StringWriter writer = new StringWriter();

        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = new String(r.getRow());
            String date = row.split("\\_")[1];
            String year = date.split("\\-")[0];
            String month = date.split("\\-")[1];
            String value = Bytes.toString(r.getValue("trafficinfo".getBytes(), "D".getBytes()));
            String[] vehicleTypes = value.split("\\|");
            for (String str : vehicleTypes) {
                String[] typenum = str.split("\\:");
                //2017-02|0,100
                if (monthcount.containsKey(year + "-" + month + "|" + typenum[0])) {
                    monthcount.put(year + "-" + month + "|" + typenum[0], monthcount.get(year + "-" + month + "|" + typenum[0]) + Integer.parseInt(typenum[1]));
                } else {
                    monthcount.put(year + "-" + month + "|" + typenum[0], Integer.parseInt(typenum[1]));
                }

                typetotal.put("total", typetotal.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                if (typetotal.containsKey(typenum[0])) {
                    typetotal.put(typenum[0], typetotal.get(typenum[0]) + Integer.parseInt(typenum[1]));
                } else {
                    typetotal.put(typenum[0], Integer.parseInt(typenum[1]));
                }
            }
        }
        mapper.writeValue(writer, typetotal);
        mapper.writeValue(writer, monthcount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }


    public Response HbasequeryTrafficStatisticsDay(String start, String end, String deviceId
    ) throws IOException {
        Connection hbase = getHbaseConnection();
        Table table = hbase.getTable(TableName.valueOf("TrafficInfo"));
        Map<String, Integer> typetotal = new LinkedHashMap<String, Integer>();
        Map<String, Integer> daycount = new LinkedHashMap<String, Integer>();

        int regionnum = Integer.parseInt(deviceId.substring(deviceId.length() - 1)) % 3;
        String startrow = regionnum + "|" + deviceId + "_" + start.replace("\"", "").split(" ")[0];
        String endrow = regionnum + "|" + deviceId + "_" + end.replace("\"", "").split(" ")[0] + "1";
        StringWriter writer = new StringWriter();

        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = new String(r.getRow());
            String date = row.split("\\_")[1];
            String value = Bytes.toString(r.getValue("trafficinfo".getBytes(), "D".getBytes()));
            String[] vehicleTypes = value.split("\\|");
            for (String str : vehicleTypes) {
                String[] typenum = str.split("\\:");
                daycount.put(date + "|" + typenum[0], daycount.getOrDefault(date + "|" + typenum[0], 0) + Integer.parseInt(typenum[1]));

                typetotal.put("total", typetotal.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                if (typetotal.containsKey(typenum[0])) {
                    typetotal.put(typenum[0], typetotal.get(typenum[0]) + Integer.parseInt(typenum[1]));
                } else {
                    typetotal.put(typenum[0], Integer.parseInt(typenum[1]));
                }
            }
        }
        mapper.writeValue(writer, typetotal);
        mapper.writeValue(writer, daycount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    public Response HbasequeryTrafficStatisticsHour(String start, String end, String deviceId
    ) throws IOException, ParseException {
        Connection hbase = getHbaseConnection();
        Table table = hbase.getTable(TableName.valueOf("TrafficInfo"));
        Map<String, Integer> typetotal = new LinkedHashMap<String, Integer>();
        Map<String, Integer> hourcount = new LinkedHashMap<String, Integer>();

        String startsplit = start.replace("\"", "").split(" ")[0];
        String endsplit = end.replace("\"", "").split(" ")[0];
        int regionnum = Integer.parseInt(deviceId.substring(deviceId.length() - 1)) % 3;
        String startrow = regionnum + "|" + deviceId + "_" + startsplit;
        String endrow = regionnum + "|" + deviceId + "_" + endsplit + "1";
        StringWriter writer = new StringWriter();

        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        ResultScanner rs = table.getScanner(scan);
        int starthour;
        int endhour;
        for (Result r : rs) {
            String row = new String(r.getRow());
            String date = row.split("\\_")[1];
            if (date.equals(startsplit)) {
                starthour = Integer.parseInt(start.replace("\"", "").split(" ")[1].split("\\:")[0]);
                endhour = 23;
            } else if (date.equals(endsplit)) {
                starthour = 0;
                endhour = Integer.parseInt(end.replace("\"", "").split(" ")[1].split("\\:")[0]);
            } else {
                starthour = 0;
                endhour = 23;
            }
            for (int hour = starthour; hour <= endhour; hour++) {
                String value = Bytes.toString(r.getValue("trafficinfo".getBytes(), (hour + "h").getBytes()));
                String rowhour = date + " " + String.format("%02d", hour);
                if (value != null) {
                    String[] vehicleTypes = value.split("\\|");
                    for (String str : vehicleTypes) {
                        String[] typenum = str.split("\\:");
                        hourcount.put(rowhour + "|" + typenum[0], hourcount.getOrDefault(rowhour + "|" + typenum[0], 0) + Integer.parseInt(typenum[1]));

                        typetotal.put("total", typetotal.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                        if (typetotal.containsKey(typenum[0])) {
                            typetotal.put(typenum[0], typetotal.get(typenum[0]) + Integer.parseInt(typenum[1]));
                        } else {
                            typetotal.put(typenum[0], Integer.parseInt(typenum[1]));
                        }
                    }
                }
            }
        }
        mapper.writeValue(writer, typetotal);
        mapper.writeValue(writer, hourcount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    public Response HbasequeryTrafficStatisticsMinute(String start, String end, String deviceId
    ) throws IOException, ParseException {
        Connection hbase = getHbaseConnection();
        Table table = hbase.getTable(TableName.valueOf("TrafficInfo"));
        Map<String, Integer> typetotal = new LinkedHashMap<String, Integer>();
        Map<String, Integer> minutecount = new LinkedHashMap<String, Integer>();

        SimpleDateFormat simplehms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat simpleymd = new SimpleDateFormat("yyyy-MM-dd");
        String startsplit = start.replace("\"", "").split(" ")[0];
        String endsplit = end.replace("\"", "").split(" ")[0];
        int regionnum = Integer.parseInt(deviceId.substring(deviceId.length() - 1)) % 3;
        String startrow = regionnum + "|" + deviceId + "_" + startsplit;
        String endrow = regionnum + "|" + deviceId + "_" + endsplit + "1";
        StringWriter writer = new StringWriter();

        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        ResultScanner rs = table.getScanner(scan);

        int startminute;
        int endminute;
        for (Result r : rs) {
            String row = new String(r.getRow());
            String date = row.split("\\_")[1];
            if (date.equals(startsplit)) {
                if (date.equals(endsplit)) {
                    startminute = (int) ((simplehms.parse(start.replace("\"", "")).getTime() - simpleymd.parse(date).getTime()) / 60000);
                    endminute = (int) ((simplehms.parse(end.replace("\"", "")).getTime() - simpleymd.parse(date).getTime()) / 60000);
                } else {
                    startminute = (int) ((simplehms.parse(start.replace("\"", "")).getTime() - simpleymd.parse(date).getTime()) / 60000);
                    endminute = 1439;
                }
            } else if (date.equals(endsplit)) {
                startminute = 0;
                endminute = (int) ((simplehms.parse(end.replace("\"", "")).getTime() - simpleymd.parse(date).getTime()) / 60000);
            } else {
                startminute = 0;
                endminute = 1439;
            }
            for (int minute = startminute; minute <= endminute; minute++) {
                String value = Bytes.toString(r.getValue("trafficinfo".getBytes(), String.valueOf(minute).getBytes()));
                int hour = minute / 60;
                int minutes = minute % 60;
                String rowhour = date + " " + String.format("%02d", hour) + ":" + String.format("%02d", minutes);
                if (value != null) {
                    String[] vehicleTypes = value.split("\\|");
                    for (String str : vehicleTypes) {
                        String[] typenum = str.split("\\:");
                        minutecount.put(rowhour + "|" + typenum[0], minutecount.getOrDefault(rowhour + "|" + typenum[0], 0) + Integer.parseInt(typenum[1]));
                        typetotal.put("total", typetotal.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                        if (typetotal.containsKey(typenum[0])) {
                            typetotal.put(typenum[0], typetotal.get(typenum[0]) + Integer.parseInt(typenum[1]));
                        } else {
                            typetotal.put(typenum[0], Integer.parseInt(typenum[1]));
                        }
                    }
                }
            }
        }
        mapper.writeValue(writer, typetotal);
        mapper.writeValue(writer, minutecount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @POST
    @Path("test")
    @Produces("application/json; charset=utf-8")
    public Map<String, Object> getName(String data)
            throws JsonGenerationException, JsonMappingException, IOException {
        String name = null;
        String password = null;
        //解析传入json数据
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> maps;
        try {
            maps = objectMapper.readValue(data, Map.class);
            name = (String) maps.get("name");
            password = (String) maps.get("password");
        } catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //输出map
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", name);
        map.put("password", password);
        return map;
    }

}
