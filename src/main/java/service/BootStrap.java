package service;

import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;

/**
 * Created by xuefei_wang on 16-12-13.
 */
public class BootStrap {

    public  final  static String HostName = "0.0.0.0";

    public  final  static  int Port = 8000;


    public static void main(String[] args){
        NettyJaxrsServer server = new NettyJaxrsServer();
        ResteasyDeployment deployment = new ResteasyDeployment();
        deployment.setApplication(new TrafficApplication());
        server.setDeployment(deployment);
        server.setHostname(HostName);
        server.setPort(Port);
        server.setExecutorThreadCount(4);
        server.setRootResourcePath("/solar");
        server.setSecurityDomain(null);
        server.setExecutorThreadCount(10);
        server.setKeepAlive(true);
        server.start();
    }
}
