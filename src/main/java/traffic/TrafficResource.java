package traffic;

import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by xuefei_wang on 16-12-13.
 */
@Path("/statistics")
public class TrafficResource {

    private  final JedisCluster jc;
    private static  AtomicLong c = new AtomicLong();
    private static DecimalFormat df = new DecimalFormat("#.###");
    private static ObjectMapper objectMapper = new ObjectMapper();

    public TrafficResource(){
        System.out.println("Init resouce");
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("suna", 7000));
        jedisClusterNodes.add(new HostAndPort("sunb", 7000));
        jedisClusterNodes.add(new HostAndPort("sunc", 7000));
        jedisClusterNodes.add(new HostAndPort("suna", 7001));
        jedisClusterNodes.add(new HostAndPort("sunb", 7001));
        jedisClusterNodes.add(new HostAndPort("sunc", 7001));
        this.jc = new JedisCluster(jedisClusterNodes);
    }


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response vehicle() {
        HashMap content = new HashMap<String,Object>();

        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(stringWriter.toString()).build();
    }

    @GET
    @Path("/count")
    @Produces(MediaType.TEXT_PLAIN)
    public Response vehicleCount() {
        HashMap content = new HashMap<String,Object>();

        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(stringWriter.toString()).build();
    }


    @GET
    @Path("/sichuan")
    @Produces(MediaType.TEXT_PLAIN)
    public Response vehicleSiChuanCount() {
        HashMap content = new HashMap<String,Object>();

        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(stringWriter.toString()).build();
    }



    @GET
    @Path("/map")
    @Produces(MediaType.TEXT_PLAIN)
    public Response vehicleMap() {

        ArrayList content = new ArrayList();

        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(stringWriter.toString()).build();
    }

    @GET
    @Path("/mining")
    @Produces(MediaType.TEXT_PLAIN)
    public Response vehicleMining(){
        ArrayList content = new ArrayList();

        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(200).header("Access-Control-Allow-Origin", "*").entity(stringWriter.toString()).build();
    }

}
