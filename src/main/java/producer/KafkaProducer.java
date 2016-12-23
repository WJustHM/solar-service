package producer;

/**
 * Created by xuefei_wang on 16-11-29.
 */


import common.PoolConfig;
import common.jdbc.JdbcConnectionPool;
import common.kafka.KafkaConnectionPool;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import protobuf.OffenceSnapDataProtos;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;



/**
 * Created by xuefei_wang on 16-10-31.
 */
public class KafkaProducer {


    public  static String[] province = {"川", "云", "贵", " 京", "津", "渝","冀","晋","苏","浙","皖","闽","赣","鲁","豫","鄂","湘","粤","桂","陕","甘","青"};

    public  static String[] city = {"A", "B", "C", "D", "E"};

    public  static String[] colors = {"红","白","银","黑","金"};

    public  static String[] brights = {"宝马","大众","奔驰","本田","丰田","路虎","保时捷","比亚迪","宝骏","凯迪拉克","法拉利"};

    public  Vector<Device> devices = new Vector<Device>();

    public  String image;
  
	public  String topic;

    public KafkaProducer(String lontiFile, String imageFile,String topic) throws Exception {
        this.image = new String(Files.readAllBytes(Paths.get(imageFile)));
        this.topic = topic;

    }


    class Device{
        public int deviceId;
        public String longitude;
        public String latitude;

        public Device(int device , String longitude, String latitude){
            this.deviceId = device;
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }

    public void initDevice(String url,String userName ,String password) throws SQLException {
        JdbcConnectionPool pool = new JdbcConnectionPool("com.mysql.jdbc.Driver",url,userName,password);
        Connection conn = pool.getConnection();
        ResultSet rs = conn.createStatement().executeQuery("select deviceId,longitude,latitude from device");
        while (rs.next()){
            int deviceId = rs.getInt("deviceId");
            String longitude = rs.getString("longitude");
            String latitude = rs.getString("latitude");
            System.out.println(deviceId + "  "+ longitude +"   "+ latitude);
            devices.add(new Device(deviceId,longitude,latitude));
        }
        pool.returnConnection(conn);
        pool.close();
    }


    public  void send(Producer<byte[], byte[]> producer) throws IOException {

        ThreadLocalRandom RandomR = ThreadLocalRandom.current();

        int parttions = producer.partitionsFor(topic).size();
        int provinceSize = province.length;
        int citySize = city.length;
        int colorSize = colors.length;
        int brightSize = brights.length;
        int deviceLen = devices.size();

        for (int i = 0; ; i++) {
            int provinceRandom = RandomR.nextInt(provinceSize);
            int cityRandom = RandomR.nextInt(citySize);
            int plateNum = RandomR.nextInt(10000, 11000);
            int picRandom = RandomR.nextInt(0, 4);
            int vehicleType= RandomR.nextInt(0,5);
            int illegalType = RandomR.nextInt(0,9);
            int speedRandm = RandomR.nextInt(0, 121);
            int chan = RandomR.nextInt(5);
            int color = RandomR.nextInt(colorSize);
            int bright = RandomR.nextInt(brightSize);

            long time = System.currentTimeMillis()/1000;

            Device device = devices.get(RandomR.nextInt(deviceLen));

            OffenceSnapDataProtos.OffenceSnapData.Builder offenceSnapData = OffenceSnapDataProtos.OffenceSnapData.newBuilder();
            offenceSnapData.setId(String.valueOf(device.deviceId));
            offenceSnapData.setDriveChan(chan);
            offenceSnapData.setVehicleType(vehicleType);
            offenceSnapData.setVehicleAttribute(0);
            offenceSnapData.setIllegalType(illegalType);
            offenceSnapData.setIllegalSubType("0");
            offenceSnapData.setPostPicNo(picRandom);
            offenceSnapData.setChanIndex(chan);
            offenceSnapData.setSpeedLimit(speedRandm);
            OffenceSnapDataProtos.PlateInfoModel.Builder plateInfo = OffenceSnapDataProtos.PlateInfoModel.newBuilder();
            plateInfo.setPlateType(vehicleType+"");
            plateInfo.setColor(colors[color]);
            plateInfo.setBright(bright);
            plateInfo.setLicenseLen(6);
            plateInfo.setCountry(provinceRandom);
            plateInfo.setLicense(province[provinceRandom] + city[cityRandom] + "." + plateNum);
            plateInfo.setBelieve("1");
            offenceSnapData.setPlateInfo(plateInfo);
            OffenceSnapDataProtos.VehicleInfoModel.Builder vehicleInfo = OffenceSnapDataProtos.VehicleInfoModel.newBuilder();
            vehicleInfo.setIndex(100);
            vehicleInfo.setVehicleType(vehicleType);
            vehicleInfo.setColorDepth(0);
            vehicleInfo.setColor(colors[color]);
            vehicleInfo.setSpeed(speedRandm);
            vehicleInfo.setLength(10);
            vehicleInfo.setIllegalType(illegalType);
            vehicleInfo.setVehicleLogoRecog(brights[bright]);
            vehicleInfo.setVehicleSubLogoRecog("0");
            vehicleInfo.setVehicleModel("0");
            offenceSnapData.setVehicleInfo(vehicleInfo);

            offenceSnapData.setMonitoringSiteID(device.longitude+"|"+device.latitude);
            offenceSnapData.setDir(1);
            offenceSnapData.setDetectType(2);
            offenceSnapData.setPilotSafebelt(0);
            offenceSnapData.setCopilotSafebelt(0);
            offenceSnapData.setPilotSubVisor(0);
            offenceSnapData.setCopilotSubVisor(0);
            offenceSnapData.setPilotCall(0);
            offenceSnapData.setAlarmDataType(0);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dd = df.format(new Date());
            offenceSnapData.setLlegalTime(dd);
            offenceSnapData.setIllegalTimer(100);
            offenceSnapData.setPicNum(picRandom);

            while (picRandom >= 0) {
                --picRandom;
                OffenceSnapDataProtos.PicInfoModel.Builder picInfo = OffenceSnapDataProtos.PicInfoModel.newBuilder();
                picInfo.setType(1);
                picInfo.setPicRecogMode(0);
                picInfo.setRedLightTime(5);
                OffenceSnapDataProtos.PlateRectModel.Builder plateRect = OffenceSnapDataProtos.PlateRectModel.newBuilder();
                plateRect.setX(0);
                plateRect.setY(0);
                plateRect.setWidth(100);
                plateRect.setHeight(100);
                picInfo.setPlateRect(plateRect);
                picInfo.setData(image);
                offenceSnapData.addPicInfo(picInfo);
            }
            OffenceSnapDataProtos.OffenceSnapData data = offenceSnapData.build();
            ProducerRecord<byte[], byte[]> message = new ProducerRecord<byte[], byte[]>(topic,device.deviceId%parttions,time, String.valueOf(device.deviceId).getBytes(), data.toByteArray());

            System.out.println(data.getMonitoringSiteID());
            sendToKafka(producer,message);
        }

    }

    private void sendToKafka(Producer<byte[], byte[]> producer,ProducerRecord<byte[], byte[]> message){
        try {
            RecordMetadata res = producer.send(message).get();
            System.out.println(res);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
       String[] command = {"-im","/home/xuefei_wang/Pictures/image.png",
               "-ks","suna:9092,sunb:9092,sunc:9092,sund:9092,sune:9092",
               "-lt","/home/xuefei_wang/workspace/solar-service/src/main/resources/longAndLati.text",
                "-tp","solar",
               "-h","master-1",
               "-p","3306",
               "-db","solar",
               "-u","root",
               "-pwd","mysql"
       };

        Options options = new Options();
        Option lonti = new Option("lt", "lonti", true, " 经纬度文件路径");
        lonti.setRequired(true);
        options.addOption(lonti);

        Option imagePath = new Option("im", "image", true, "图片路径");
        imagePath.setRequired(true);
        options.addOption(imagePath);


        Option kafkaOption = new Option("ks", "kafkaserver", true, "kafka地址");
        kafkaOption.setRequired(true);
        options.addOption(kafkaOption);

        Option topicOption = new Option("tp", "topic", true, "kafka topic");
        topicOption.setRequired(true);
        options.addOption(topicOption);


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



        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, command);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("参数是：", options);
            System.exit(1);
            return;
        }
        formatter.printHelp("参数是：", options);

        String bootstrapServers = cmd.getOptionValue("kafkaserver");
        String lontiFile = cmd.getOptionValue("lonti");
        String imageFile = cmd.getOptionValue("image");
        String topic = cmd.getOptionValue("topic");


        StringBuffer url = new StringBuffer("jdbc:mysql://");
        url.append(cmd.getOptionValue("host")).append(":");
        url.append(cmd.getOptionValue("port")).append("/");
        url.append(cmd.getOptionValue("database"));

        String userName = cmd.getOptionValue("username");
        String password = cmd.getOptionValue("password");

        int threadNum = 12;
        PoolConfig config = new PoolConfig();
        config.setMaxTotal(30);
        config.setMaxIdle(20);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("producer.type", "async");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.setProperty("request.required.acks", "0");
        props.setProperty("compression.codec", "snappy");
        props.setProperty("batch.num.messages", "2000");
        props.setProperty("max.request.size", "1000973460");

        final KafkaConnectionPool pool = new KafkaConnectionPool(config, props);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Executor executors = Executors.newFixedThreadPool(threadNum);
        final KafkaProducer man = new KafkaProducer(lontiFile,imageFile,topic);
        man.initDevice(url.toString(),userName,password);
        for (int i = 0; i < threadNum; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        man.send(pool.getConnection());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        pool.close();
    }
}
