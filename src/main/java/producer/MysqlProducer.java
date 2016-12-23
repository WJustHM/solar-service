package producer;

import common.jdbc.JdbcConnectionPool;
import org.apache.commons.cli.*;

import javax.naming.ConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.text.DecimalFormat;

import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by xuefei_wang on 16-12-22.
 */
public class MysqlProducer {



    private JdbcConnectionPool pool ;

    private String Sql = "insert into device(deviceId,longitude,latitude,state,roadId,bspeedlevel,mspeedlevel,sspeedlevel) values(?,?,?,?,?,?,?,?)";

    private String Sql_Road = "insert into road(roadId,distance,capacity,lanes) values(?,?,?,?)";

    private static DecimalFormat df = new DecimalFormat("#.##");

    private Vector<String> localtions = new Vector<String>();

    public MysqlProducer(String url,String userName ,String password){
        this("com.mysql.jdbc.Driver",url,userName,password);
    }

    public MysqlProducer(String driver ,String url,String userName ,String password){
        pool = new JdbcConnectionPool(driver,url,userName,password);
        String path = System.getProperty("user.dir") + "/src/main/resources/longAndLati.text";
        File file = new File(path);
        try {
            FileReader reader = new FileReader(file);
            BufferedReader br = new BufferedReader(reader);
            String str = null;
            while ((str = br.readLine()) != null) {
                localtions.add(str.trim());
            }
            br.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void runInsertDevice(int num) throws SQLException {
        Connection conn = pool.getConnection();
        PreparedStatement sql = conn.prepareStatement(Sql);
        ResultSet roads = conn.createStatement().executeQuery("select roadId from road");
        Vector<Integer> roadIds = new Vector<Integer>();
        while (roads.next()){
            int roadId = roads.getInt("roadId");
            roadIds.add(roadId);
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int localSize = localtions.size();
        int number  = num ;
        while (number > 0){
            int road = random.nextInt(roadIds.size());
            String[] LL = localtions.get(number%localSize).split(",");
            sql.setInt(1,10000+number);
            sql.setString(2,LL[0]);
            sql.setString(3,LL[1]);
            sql.setInt(4,1);
            sql.setInt(5,roadIds.get(road));
            sql.setFloat(6,80);
            sql.setFloat(7,80);
            sql.setFloat(8,100);
            sql.addBatch();
            sql.execute();
            System.out.println(localtions.get(random.nextInt(localSize)).split(","));
            number -- ;
        }
        pool.returnConnection(conn);
        pool.close();
    }

    private void runInsertRoad(int num) throws SQLException {
        Connection conn = pool.getConnection();
        PreparedStatement sql = conn.prepareStatement(Sql_Road);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int number  = num ;
        while (number >= 0){
             double distance = Double.valueOf(df.format(random.nextDouble(4.0,10.0)));
             int lanse = random.nextInt(3,4);
             int capacity = (int)(distance*1000*lanse/60);
            sql.setInt(1,100+number);
            sql.setDouble(2, distance);
            sql.setInt(3,capacity);
            sql.setInt(4,lanse);
            sql.addBatch();
            sql.execute();
            System.out.println(number);
            number -- ;
        }

        pool.returnConnection(conn);
        pool.close();

    }


    public static void main(String[] args) throws ConfigurationException, SQLException {
        Options options = new Options();
        Option host = new Option("h", "host", true, " mysql hostname");
        host.setRequired(true);
        options.addOption(host);

        Option port = new Option("p", "port", true, "mysql port");
        port.setRequired(true);
        options.addOption(port);


        Option database = new Option("db", "database", true, "database name");
        database.setRequired(true);
        options.addOption(database);

        Option user = new Option("u", "username", true, "database name");
        user.setRequired(true);
        options.addOption(user);


        Option passw = new Option("pwd", "password", true, "database name");
        passw.setRequired(true);
        options.addOption(passw);


        Option number = new Option("n", "number", true, "number of device should be create ");
        number.setRequired(true);
        options.addOption(number);

        Option tableId = new Option("tb", "table", true, "table id : \n  0:  device  \n 1:  road ");
        tableId.setRequired(true);
        options.addOption(tableId);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {

            String[] argsTest = {"-h","master-1","-p","3306","-db","solar","-u","root","-pwd","mysql","-n","500","-tb","0"};

            cmd = parser.parse(options, argsTest);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("参数是：", options);
            System.exit(1);
            return;
        }
        formatter.printHelp("参数是：", options);

        StringBuffer url = new StringBuffer("jdbc:mysql://");
        url.append(cmd.getOptionValue("host")).append(":");
        url.append(cmd.getOptionValue("port")).append("/");
        url.append(cmd.getOptionValue("database"));

        int num  = Integer.valueOf(cmd.getOptionValue("number"));

        String userName = cmd.getOptionValue("username");
        String password = cmd.getOptionValue("password");

        MysqlProducer mysqlProducer = new MysqlProducer(url.toString() , userName , password);

        int tbId = Integer.valueOf(cmd.getOptionValue("table"));

        switch (tbId){
            case 0 : {
                mysqlProducer.runInsertDevice(num);
                System.out.println("succss to insert device");
                break;
            }
            case 1 : {
                mysqlProducer.runInsertRoad(num);
                System.out.println("succss to insert road");
                break;
            }
        }


    }

}
