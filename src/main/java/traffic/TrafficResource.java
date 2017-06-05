package traffic;


import common.InternalPools;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.StringWriter;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static traffic.RSA_Encrypt.decrypt;
import static traffic.RSA_Encrypt.encrypt;

/**
 * Created by xuefei_wang on 16-12-13.
 */

@Path("/traffic")
public class TrafficResource extends InternalPools {

    private final ObjectMapper mapper = new ObjectMapper();
    private HashMap<String, String> mysqlmap = null;
    private List deviceList = null;
    TableName tablename = TableName.valueOf("Result");
    Table gettable = null;
    String TASK = "TASK";
    String CAMERA = "CAMERA";
    String DATASOURCE = "DATASOURCE";
    JedisCluster redis = initredis();

    public TrafficResource(Map paramters) {
        super(paramters);
    }


    @OPTIONS
    @Path("/image/{rowkey}")
    public Response imageOptions() {
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, x-token")
                .build();
    }

    @GET
    @Path("/image/{rowkey}")
    public Response imageQuery(@PathParam("rowkey") final String rowkey,
                               @HeaderParam("X-TOKEN") final String token) throws Exception {
        if (checkToken(token) < 0)
            return Response
                    .status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
        StringWriter writer = new StringWriter();
        HashMap metadata = new HashMap();
        int stat = 200;
        Connection hbase = getHbaseConnection();
        HTable table;
        byte[] image = null;
        try {
            table = (HTable) hbase.getTable(TableName.valueOf("TrafficImage"));
            Result rs = table.get(new Get(rowkey.getBytes()));
            image = rs.getValue("Image".getBytes(), "Data".getBytes());
            String redLightTime = new String(rs.getValue("Image".getBytes(), "RedLightTime".getBytes()));
            String recogMode = new String(rs.getValue("Image".getBytes(), "RecogMode".getBytes()));
            metadata.put("RedLightTime", redLightTime);
            metadata.put("RecogMode", recogMode);
            mapper.writeValue(writer, metadata);
        } catch (Exception e) {
            stat = 404;
            e.printStackTrace();
        } finally {
            returnHbaseConnection(hbase);
            return Response.status(stat)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Content-Type", "image/png")
                    .header("metadata", writer.toString())
                    .entity(image).build();
        }
    }

    @OPTIONS
    @Path("/track")
    public Response trackOptions() {
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, x-token")
                .build();
    }

    @GET
    @Path("/track")
    @Produces("application/json; charset=utf-8")
    public Response trackQuery(
            @QueryParam("start") final String start,
            @QueryParam("end") final String end,
            @QueryParam("PlateLicense") final String PlateLicense,
            @HeaderParam("x-token") final String token)
            throws Exception {
        if (checkToken(token) < 0)
            return Response.status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
        TransportClient conn = getEsConnection();
        StringWriter writer = new StringWriter();
        Map<String, HashMap> map = new LinkedHashMap<String, HashMap>();
        if (mysqlmap == null) {
            mysqlmap = new HashMap<String, String>();
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
            content.put("Lonlat", mysqlmap.get(i.getSource().get("SBBH").toString()));
            map.put(i.getSource().get("Time").toString(), content);
        }
        mapper.writeValue(writer, map);
        returnEsConnection(conn);
        return Response.status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Credentials", "true")
                .entity(writer.toString())
                .build();
    }

    @OPTIONS
    @Path("/statistics")
    public Response statisticsOptions() {
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, x-token")
                .build();
    }

    @GET
    @Path("/statistics")
    @Produces("application/json; charset=utf-8")
    public Response statisticsQuery(
            @QueryParam("by") final String by,
            @QueryParam("start") final String start,
            @QueryParam("end") final String end,
            @QueryParam("deviceId") final String deviceId,
            @HeaderParam("X-TOKEN") final String token)
            throws Exception {
        if (checkToken(token) < 0)
            return Response
                    .status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
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
                return Response
                        .status(200)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity("{\"error\":\"method by are not found!\"}")
                        .build();
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
                monthcount.put(year + "-" + month + "|" + typenum[0], monthcount.getOrDefault(year + "-" + month + "|" + typenum[0], 0) + Integer.parseInt(typenum[1]));
                monthcount.put("total", monthcount.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                monthcount.put(typenum[0], monthcount.getOrDefault(typenum[0], 0) + Integer.parseInt(typenum[1]));

            }
        }
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
                daycount.put("total", daycount.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                daycount.put(typenum[0], daycount.getOrDefault(typenum[0], 0) + Integer.parseInt(typenum[1]));
            }
        }
        mapper.writeValue(writer, daycount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    public Response HbasequeryTrafficStatisticsHour(String start, String end, String deviceId
    ) throws IOException, ParseException {
        Connection hbase = getHbaseConnection();
        Table table = hbase.getTable(TableName.valueOf("TrafficInfo"));
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
                        hourcount.put("total", hourcount.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                        hourcount.put(typenum[0], hourcount.getOrDefault(typenum[0], 0) + Integer.parseInt(typenum[1]));

                    }
                }
            }
        }
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
                        minutecount.put("total", minutecount.getOrDefault("total", 0) + Integer.parseInt(typenum[1]));
                        minutecount.put(typenum[0], minutecount.getOrDefault(typenum[0], 0) + Integer.parseInt(typenum[1]));

                    }
                }
            }
        }
        mapper.writeValue(writer, minutecount);
        returnHbaseConnection(hbase);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @POST
    @Path("/authenticate")
    @Produces("application/json; charset=utf-8")
    public Response login(String data) throws Exception {
        StringWriter writer = new StringWriter();
        String username;
        String password;
        String department;
        Map<String, Object> maps;
        maps = mapper.readValue(data, Map.class);
        username = (String) maps.get("username");
        password = (String) maps.get("password");
        department = (String) maps.get("department");
        if (!username.equals("admin") || !password.equals("admin123") || !department.equals("traffic")) {
            Map<String, String> res = new LinkedHashMap<>();
            res.put("result", "0");
            res.put("error", "Failed to authentication");
            mapper.writeValue(writer, res);
            return Response.status(401)
                    .entity(writer.toString())
                    .header("Access-Control-Allow-Origin", "*")
                    .build();
        } else {
            Map<String, Map<String, String>> resp = new LinkedHashMap<>();
            Map<String, String> tokens = new LinkedHashMap<>();
            String token = "123456";
            tokens.put("token", encrypt(token));
            tokens.put("endpoint", "/solar/traffic");
            resp.put("access", tokens);
            mapper.writeValue(writer, resp);
            return Response.status(200)
                    .entity(writer.toString())
                    .header("Access-Control-Allow-Origin", "*")
                    .build();
        }
    }

    @POST
    @Path("/authenticate2")
    @Produces("application/json; charset=utf-8")
    public Response login(@FormParam("username") String username,
                          @FormParam("password") String password,
                          @FormParam("department") String department) throws Exception {
        StringWriter writer = new StringWriter();
        if (!username.equals("admin") || !password.equals("admin123") || !department.equals("traffic")) {
            Map<String, String> res = new LinkedHashMap<>();
            res.put("result", "0");
            res.put("error", "Failed to authentication");
            mapper.writeValue(writer, res);
            return Response.status(401)
                    .entity(writer.toString())
                    .header("Access-Control-Allow-Origin", "*")
                    .build();
        } else {
            Map<String, Map<String, String>> resp = new LinkedHashMap<>();
            Map<String, String> tokens = new LinkedHashMap<>();
            String token = "123456";
            tokens.put("token", encrypt(token));
            tokens.put("endpoint", "/solar/traffic");
            resp.put("access", tokens);
            mapper.writeValue(writer, resp);
            return Response.status(200)
                    .entity(writer.toString())
                    .header("Access-Control-Allow-Origin", "*")
                    .build();
        }
    }

    public int checkToken(String myToken) {
        String token = "123456";
        try {
            if (decrypt(myToken).equals(token)) {
                return 1;
            } else {
                return -1;
            }
        } catch (Exception e) {
            return -1;
        }
    }

    @OPTIONS
    @Path("/devices")
    public Response devicesOptions() {
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, x-token")
                .build();
    }

    @GET
    @Path("/devices")
    @Produces("application/json; charset=utf-8")
    public Response devicesQuery(@HeaderParam("X-TOKEN") final String token) throws Exception {
        if (checkToken(token) < 0)
            return Response.status(401).header("Access-Control-Allow-Origin", "*").entity("{\"error\":\"Failed to authentication.\"}").build();
        StringWriter writer = new StringWriter();
        if (deviceList == null) {
            deviceList = new LinkedList();
            java.sql.Connection conn = getMysqlConnection();
            String query = "SELECT id,lonlat,type FROM gate";
            ResultSet rs = conn.prepareStatement(query).executeQuery();

            while (rs.next()) {
                if (!rs.getString(1).equals("0")) {
                    HashMap coordinateMap = new HashMap();
                    coordinateMap.put("longitude", rs.getString(2).split(",")[0]);
                    coordinateMap.put("latitude", rs.getString(2).split(",")[1]);
                    Map deviceMap = new LinkedHashMap();
                    deviceMap.put("id", rs.getString(1));
                    deviceMap.put("coordinate", coordinateMap);
                    deviceMap.put("monitortype", rs.getString(3));
                    deviceList.add(deviceMap);
                }
            }
            returnMysqlConnection(conn);
        }
        HashMap content = new HashMap();
        content.put("devices", deviceList);
        mapper.writeValue(writer, content);
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .entity(writer.toString()).build();
    }

    @OPTIONS
    @Path("/preyinfo")
    public Response preyOptions() {
        return Response
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, x-token")
                .build();
    }

    @GET
    @Path("/preyinfo")
    @Produces("application/json; charset=utf-8")
    public Response preyListQuery(@QueryParam("BKXW") final String BKXW,
                                  @HeaderParam("x-token") final String token) throws Exception {
        if (checkToken(token) < 0)
            return Response.status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
        StringWriter writer = new StringWriter();
        List prayList = new LinkedList();
        java.sql.Connection conn = getMysqlConnection();
        String query = "SELECT BKXXBH,HPHM,HPYS,CLPP1,CLPP2,CSYS,CLLX,HPZL FROM PreyInfo WHERE BKXW=?";
        PreparedStatement pst = conn.prepareStatement(query);
        pst.setString(1, BKXW);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            Map singleMap = new LinkedHashMap();
            singleMap.put("BKXXBH", rs.getString(1));
            singleMap.put("HPHH", rs.getString(2));
            singleMap.put("HPYS", rs.getString(3));
            singleMap.put("CLPP1", rs.getString(4));
            singleMap.put("CLPP2", rs.getString(5));
            singleMap.put("CSYS", rs.getString(6));
            singleMap.put("CLLX", rs.getString(7));
            singleMap.put("HPZL", rs.getString(8));
            prayList.add(singleMap);
        }
        Map content = new HashMap();
        content.put("PreyList", prayList);
        mapper.writeValue(writer, content);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @PUT
    @Path("/preyinfo")
    public Response preyListAdd(final String list,
                                @HeaderParam("x-token") final String token) throws SQLException, IOException {
        if (checkToken(token) < 0)
            return Response.status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
        java.sql.Connection conn = getMysqlConnection();
        String query = "INSERT INTO PreyInfo (BKXXBH,HPHM,HPYS,CLPP1,CLPP2,CSYS,CLLX,HPZL,CLZP,BKXW,BKJB,BKDWDM," +
                "BKDWMC,BKDWZBDH,BKR,BKRZJHM,BKLXRSJ,BKSZ,BKLX,BKSJ,BKFKSJ,BKJZRQ,AJMS,BKZT,CKDW,CKDWMC,CKR," +
                "CKRZJHM,CKSJ,CKYY,BKCZR,CKCZR) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Map<String, String> maps;
        System.out.println(list);
        maps = mapper.readValue(list, Map.class);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String BKXXBH = maps.get("HPHM") + sdf.format(new Date());

        PreparedStatement pst = conn.prepareStatement(query);
        pst.setString(1, BKXXBH);
        pst.setString(2, maps.containsKey("HPHM") ? maps.get("HPHM") : "");
        pst.setString(3, maps.containsKey("HPYS") ? maps.get("HPYS") : "");
        pst.setString(4, maps.containsKey("CLPP1") ? maps.get("CLPP1") : "");
        pst.setString(5, maps.containsKey("CLPP2") ? maps.get("CLPP2") : "");
        pst.setString(6, maps.containsKey("CSYS") ? maps.get("CSYS") : "");
        pst.setString(7, maps.containsKey("CLLX") ? maps.get("CLLX") : "");
        pst.setString(8, maps.containsKey("HPZL") ? maps.get("HPZL") : "");
        pst.setString(9, maps.containsKey("CLZP") ? maps.get("CLZP") : "");
        pst.setString(10, maps.containsKey("BKXW") ? maps.get("BKXW") : "");
        pst.setString(11, maps.containsKey("BKJB") ? maps.get("BKJB") : "");
        pst.setString(12, maps.containsKey("BKDWDM") ? maps.get("BKDWDM") : "");
        pst.setString(13, maps.containsKey("BKDWMC") ? maps.get("BKDWMC") : "");
        pst.setString(14, maps.containsKey("BKDWZBDH") ? maps.get("BKDWZBDH") : "");
        pst.setString(15, maps.containsKey("BKR") ? maps.get("BKR") : "");
        pst.setString(16, maps.containsKey("BKRZJHM") ? maps.get("BKRZJHM") : "");
        pst.setString(17, maps.containsKey("BKLXRSJ") ? maps.get("BKLXRSJ") : "");
        pst.setString(18, maps.containsKey("BKSZ") ? maps.get("BKSZ") : "");
        pst.setString(19, maps.containsKey("BKLX") ? maps.get("BKLX") : "");
        pst.setString(20, maps.containsKey("BKSJ") ? maps.get("BKSJ") : "");
        pst.setString(21, maps.containsKey("BKFKSJ") ? maps.get("BKFKSJ") : "");
        pst.setString(22, maps.containsKey("BKJZRQ") ? maps.get("BKJZRQ") : "");
        pst.setString(23, maps.containsKey("AJMS") ? maps.get("AJMS") : "");
        pst.setString(24, maps.containsKey("BKZT") ? maps.get("BKZT") : "");
        pst.setString(25, maps.containsKey("CKDW") ? maps.get("CKDW") : "");
        pst.setString(26, maps.containsKey("CKDWMC") ? maps.get("CKDWMC") : "");
        pst.setString(27, maps.containsKey("CKR") ? maps.get("CKR") : "");
        pst.setString(28, maps.containsKey("CKRZJHM") ? maps.get("CKRZJHM") : "");
        pst.setString(29, maps.containsKey("CKSJ") ? maps.get("CKSJ") : "");
        pst.setString(30, maps.containsKey("CKYY") ? maps.get("CKYY") : "");
        pst.setString(31, maps.containsKey("BKCZR") ? maps.get("BKCZR") : "");
        pst.setString(32, maps.containsKey("CKCZR") ? maps.get("CKCZR") : "");
        if (pst.executeUpdate() > 0) {
            return Response
                    .status(200)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                    .entity("Add record success")
                    .build();
        } else {
            return Response
                    .status(503)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS")
                    .entity("Add failure")
                    .build();
        }
    }

    @POST
    @Path("/preyinfo/{BKXXBH}")
    public Response preyListUpdate(@PathParam("BKXXBH") final String BKXXBH,
                                   @HeaderParam("x-token") final String token,
                                   final String data) throws SQLException, IOException {
        if (checkToken(token) < 0)
            return Response.status(401)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("{\"error\":\"Failed to authentication.\"}")
                    .build();
        java.sql.Connection conn = getMysqlConnection();
        String query1 = "SELECT COUNT(*) FROM PreyInfo WHERE BKXXBH=?";
        PreparedStatement pst1 = conn.prepareStatement(query1);
        pst1.setString(1, BKXXBH);
        ResultSet rs1 = pst1.executeQuery();
        if (rs1.next())
            if (rs1.getInt(1) < 1) {
                return Response
                        .status(404)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity("BKXXBH:" + BKXXBH + " not exist")
                        .build();
            }
        String querySet = "";
        String queryHead = "UPDATE PreyInfo SET ";
        String queryTail = " WHERE BKXXBH=?";
        Map<String, Object> maps;
        maps = mapper.readValue(data, Map.class);
        for (Map.Entry entry : maps.entrySet()) {
            querySet += entry.getKey() + "=" + entry.getValue() + ",";
        }
        querySet = querySet.substring(0, querySet.length() - 1);
        String queryALL = queryHead + querySet + queryTail;
        PreparedStatement pst2 = conn.prepareStatement(queryALL);
        pst2.setString(1, BKXXBH);
        System.out.println(queryALL);
        if (pst2.executeUpdate() > 0)
            return Response
                    .ok()
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("Update is successful")
                    .build();
        else
            return Response.status(503)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("update is failure")
                    .build();
    }


    @POST
    @Path("/preyinfo2/{BKXXBH}")
    public Response preyListUpdate2(@PathParam("BKXXBH") final String BKXXBH,
                                    @FormParam("list") final String list) throws SQLException, IOException {
        java.sql.Connection conn = getMysqlConnection();
        String query1 = "SELECT COUNT(*) FROM PreyInfo WHERE BKXXBH=?";
        PreparedStatement pst1 = conn.prepareStatement(query1);
        pst1.setString(1, BKXXBH);
        ResultSet rs1 = pst1.executeQuery();
        if (rs1.next())
            if (rs1.getInt(1) < 1) {
                return Response
                        .status(404)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity("BKXXBH:" + BKXXBH + " not exist")
                        .build();
            }
        String querySet = "";
        String queryHead = "UPDATE PreyInfo SET ";
        String queryTail = " WHERE BKXXBH=?";
        Map<String, Object> maps;
        maps = mapper.readValue(list, Map.class);
        for (Map.Entry entry : maps.entrySet()) {
            querySet += entry.getKey() + "=" + entry.getValue() + ",";
        }
        querySet = querySet.substring(0, querySet.length() - 1);
        String queryALL = queryHead + querySet + queryTail;
        PreparedStatement pst2 = conn.prepareStatement(queryALL);
        pst2.setString(1, BKXXBH);
        System.out.println(queryALL);
        if (pst2.executeUpdate() > 0)
            return Response
                    .ok()
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("Update is successful")
                    .build();
        else
            return Response.status(503)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity("update is failure")
                    .build();
    }


    @GET
    @Path("/help")
    public Response help() {
        StringBuffer helper = new StringBuffer();
        helper.append("查询 一: 指定时间段内的过车车辆记录 \n");
        helper.append("请求方式：GET \n");
        helper.append("条件参数：starttime、endtime\n");
        helper.append("示例：\n");
        helper.append("http://172.20.31.7:8001/solar/traffic/record?starttime=\"2017-04-11 17:32:45\"&endtime=\"2017-04-20 17:32:45\"\n");
        helper.append("\n\n");
        helper.append("查询 二: 指定时间段、车牌省份和车牌最后一位数字匹配车辆记录 \n");
        helper.append("请求方式：GET \n");
        helper.append("条件参数：starttime、endtime、province、number\n");
        helper.append("示例：\n");
        helper.append("http://172.20.31.7:8001/solar/traffic/platerecord?starttime=\"2017-04-11 17:32:45\"&province=粤&number=2\n");
        helper.append("\n\n");
        helper.append("查询 三: 指定时间段、车辆各种基本属性匹配车辆记录 \n");
        helper.append("请求方式：GET \n");
        helper.append("条件参数：starttime、endtime、vehicleBrand、PlateColor、Direction、tag、paper" +
                "、sun、drop、secondBelt、crash、danger\n");
        helper.append("示例：\n");
        helper.append("http://172.20.31.7:8001/solar/traffic/allrecord?starttime=\"2017-04-11 17:32:45\"&endtime=\"2017-04-20 17:32:45\"&vehicleBrand=斯柯达&PlateColor=蓝&Direction=3&tag=true&paper=false&sun=false&drop=true&secondBelt=true&crash=true&danger=false");

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(helper.toString()).build();
    }


    @GET
    @Path("/record")
    public Response searchElasticHBase(
            @QueryParam("starttime") final String start,
            @QueryParam("endtime") final String end)
            throws Exception {
        TransportClient esclient = getEsConnection();
        StringWriter writer = new StringWriter();
        SearchRequestBuilder request = esclient.prepareSearch().setIndices("vehicle").setTypes("result");
        String que = "{\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "       \"range\": {\n" +
                "         \"ResultTime\": {\n" +
                "           \"gte\": \"" + start.replace("\"", "") + "\",\n" +
                "           \"lte\": \"" + end.replace("\"", "") + "\"\n" +
                "         }\n" +
                "       }\n" +
                "      }\n" +
                "    }\n" +
                "  }";

        //执行查询语句
        SearchResponse response = request.setQuery(QueryBuilders.wrapperQuery(que)).execute().actionGet();

        Map<String, Long> map = new HashMap<>();
        map.put("total", response.getHits().getTotalHits());
        mapper.writeValue(writer, map);
        returnEsConnection(esclient);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
    }

    @GET
    @Path("/platerecord")
    public Response searchlicense(
            @QueryParam("starttime") final String start,
            @QueryParam("province") final String province,
            @QueryParam("number") final String regexnumber)
            throws Exception {
        StringWriter writer = new StringWriter();
        TransportClient esclient = getEsConnection();
        SearchRequestBuilder request = esclient.prepareSearch().setIndices("vehicle").setTypes("result");
        //执行查询语句
        SearchResponse response = request.setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("ResultTime").gte("2017-04-11 17:32:45")
                .lte("2017-04-20 17:32:45")).must(QueryBuilders.regexpQuery("Plate.keyword", province + ".{5}" + regexnumber)))).setSize(50)
                .setScroll(new TimeValue(5000)).execute().actionGet();
        int num = 0;
        do {
            List<String> TASKS = new ArrayList<>();
            List<String> CAMERAS = new ArrayList<>();
            List<String> DATASOURCES = new ArrayList<>();
            for (SearchHit rs : response.getHits().getHits()) {
                TASKS.add(rs.getSource().get("taskId").toString());
                CAMERAS.add(rs.getSource().get("cameraId").toString());
                DATASOURCES.add(rs.getSource().get("dataSourceId").toString());
                num++;
            }
            HashMap<String, String> task = TASKSE(TASKS);
            HashMap<String, String> camera = CAMERASE(CAMERAS);
            HashMap<String, String> dataSource = DATASOURCESE(DATASOURCES);
            LinkedList<HashMap<String, String>> res = joinResult(task, camera, dataSource);
            if (num == 50) {
                mapper.writeValue(writer, res);
                returnEsConnection(esclient);
                return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
            }
            response = esclient.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(5000)).execute().actionGet();
        } while (response.getHits().getHits().length != 0);

        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("").build();
    }

    public HashMap<String, String> TASKSE(List<String> TASK) {
        HashMap<String, String> map = new HashMap<>();
        Set<String> lis = new HashSet<>();
        TransportClient esclient = getEsConnection();
        MultiGetRequestBuilder muli = esclient.prepareMultiGet();
        lis.addAll(TASK);
        MultiGetResponse multiGetItemResponses = muli.add("vehicle", "task", lis).get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                map.put(response.getId(), json);
            }
        }
        returnEsConnection(esclient);
        return map;
    }

    public HashMap<String, String> CAMERASE(List<String> CAMERA) {
        HashMap<String, String> map = new HashMap<>();
        Set<String> lis = new HashSet<>();
        TransportClient esclient = getEsConnection();
        MultiGetRequestBuilder muli = esclient.prepareMultiGet();
        lis.addAll(CAMERA);
        MultiGetResponse multiGetItemResponses = muli.add("vehicle", "camera", lis).get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                map.put(response.getId(), json);
            }
        }
        returnEsConnection(esclient);
        return map;
    }

    public HashMap<String, String> DATASOURCESE(List<String> DATASOURCE) {
        HashMap<String, String> map = new HashMap<>();
        Set<String> lis = new HashSet<>();
        TransportClient esclient = getEsConnection();
        MultiGetRequestBuilder muli = esclient.prepareMultiGet();
        lis.addAll(DATASOURCE);
        MultiGetResponse multiGetItemResponses = muli.add("vehicle", "datasource", lis).get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                map.put(response.getId(), json);
            }
        }
        returnEsConnection(esclient);
        return map;
    }

    @GET
    @Path("/allrecord")
    public Response searchelasticHBase(
            @QueryParam("starttime") final String starttime,
            @QueryParam("endtime") final String endtime,
            @QueryParam("vehicleBrand") final String vehicleBrand,
            @QueryParam("PlateColor") final String PlateColor,
            @QueryParam("Direction") final String Direction,
            @QueryParam("tag") final String tag,
            @QueryParam("paper") final String paper,
            @QueryParam("sun") final String sun,
            @QueryParam("drop") final String drop,
            @QueryParam("secondBelt") final String secondBelt,
            @QueryParam("crash") final String crash,
            @QueryParam("danger") final String danger)
            throws Exception {
        StringWriter writer = new StringWriter();
        TransportClient esclient = getEsConnection();
        Connection hbase = getHbaseConnection();
        gettable = hbase.getTable(tablename);
        SearchRequestBuilder request = esclient.prepareSearch().setIndices("vehicle").setTypes("result");
        //ES查询Json代码
        String que = "{\n" +
                "    \"bool\": {\n" +
                "      \"filter\": [\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"vehicleBrand.keyword\": \"" + vehicleBrand + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"PlateColor\": \"" + PlateColor + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"Direction\": \"" + Direction + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"tag\": \"" + tag + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"paper\": \"" + paper + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"sun\": \"" + sun + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"secondBelt\": \"" + secondBelt + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"crash\": \"" + crash + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"drop\": \"" + drop + "\"\n" +
                "          }\n" +
                "        },\n" +
                "          {\n" +
                "          \"term\": {\n" +
                "            \"danger\": \"" + danger + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"ResultTime\": {\n" +
                "              \"gte\": \"" + starttime.replace("\"", "") + "\",\n" +
                "              \"lte\": \"" + endtime.replace("\"", "") + "\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }";
        //执行查询语句
        SearchResponse response = request.setQuery(QueryBuilders.wrapperQuery(que)).setSize(50)
                .setScroll(new TimeValue(5000)).execute().actionGet();
        int num = 0;
        List<Get> list = new LinkedList<Get>();
        do {
            List<String> TASKS = new ArrayList<>();
            List<String> CAMERAS = new ArrayList<>();
            List<String> DATASOURCES = new ArrayList<>();
            for (SearchHit rs : response.getHits().getHits()) {
                TASKS.add(rs.getSource().get("taskId").toString());
                CAMERAS.add(rs.getSource().get("cameraId").toString());
                DATASOURCES.add(rs.getSource().get("dataSourceId").toString());
                num++;
            }
            HashMap<String, String> task = TASKSE(TASKS);
            HashMap<String, String> camera = CAMERASE(CAMERAS);
            HashMap<String, String> dataSource = DATASOURCESE(DATASOURCES);
            LinkedList<HashMap<String, String>> res = joinResult(task, camera, dataSource);
            if (num == 50) {
                LinkedList<HashMap<String, String>> hb = searchHBase(list);
                hb.addAll(res);
                mapper.writeValue(writer, res);
                returnEsConnection(esclient);
                returnHbaseConnection(hbase);
                return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(writer.toString()).build();
            }
            response = esclient.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(5000)).execute().actionGet();
        } while (response.getHits().getHits().length != 0);
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity("").build();
    }

    //执行Redis查询
    public HashMap<String, String> searchRedis(String table, String id) {
        HashMap<String, String> map = null;
        if (redis.hexists(table, id)) {
            String result = redis.hget(table, id);
            try {
                map = mapper.readValue(result, new HashMap<String, String>().getClass());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public JedisCluster initredis() {
        HashSet jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("datanode4", 6380));
        jedisClusterNodes.add(new HostAndPort("datanode5", 6380));
        jedisClusterNodes.add(new HostAndPort("datanode6", 6380));
        JedisCluster jc = new JedisCluster(jedisClusterNodes);
        return jc;
    }

    public LinkedList<HashMap<String, String>> joinResult(HashMap<String, String> task, HashMap<String, String> camera, HashMap<String, String> dataSource) {
        LinkedList results = new LinkedList<HashMap<String, String>>();
        results.add(task);
        results.add(camera);
        results.add(dataSource);

        return results;
    }

    public LinkedList<HashMap<String, String>> searchHBase(List<Get> gets) {
        LinkedList<HashMap<String, String>> resultsFinal = new LinkedList<HashMap<String, String>>();
        try {
            Result[] res = gettable.get(gets);
            for (Result ss : res) {
                HashMap<String, String> resultHbase = new HashMap<String, String>();
                String License = Bytes.toString(ss.getValue("Result".getBytes(), "License".getBytes()));
                String PlateType = Bytes.toString(ss.getValue("Result".getBytes(), "PlateType".getBytes()));
                String PlateColor = Bytes.toString(ss.getValue("Result".getBytes(), "PlateColor".getBytes()));
                String Confidence = Bytes.toString(ss.getValue("Result".getBytes(), "Confidence".getBytes()));
                String LicenseAttribution = Bytes.toString(ss.getValue("Result".getBytes(), "LicenseAttribution".getBytes()));
                String ImageURL = Bytes.toString(ss.getValue("Result".getBytes(), "ImageURL".getBytes()));
                String CarColor = Bytes.toString(ss.getValue("Result".getBytes(), "CarColor".getBytes()));
                String ResultTime = Bytes.toString(ss.getValue("Result".getBytes(), "ResultTime".getBytes()));
                String Direction = Bytes.toString(ss.getValue("Result".getBytes(), "Direction".getBytes()));
                String frame_index = Bytes.toString(ss.getValue("Result".getBytes(), "frame_index".getBytes()));
                String vehicleKind = Bytes.toString(ss.getValue("Result".getBytes(), "vehicleKind".getBytes()));
                String vehicleBrand = Bytes.toString(ss.getValue("Result".getBytes(), "vehicleBrand".getBytes()));
                String vehicleStyle = Bytes.toString(ss.getValue("Result".getBytes(), "vehicleStyle".getBytes()));
                String LocationLeft = Bytes.toString(ss.getValue("Result".getBytes(), "LocationLeft".getBytes()));
                if (License != null) {
                    resultHbase.put("HP", License);
                }
                resultsFinal.add(resultHbase);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultsFinal;
    }
}