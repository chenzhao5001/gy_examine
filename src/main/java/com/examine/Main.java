package com.examine;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.region.Region;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import org.apache.log4j.Logger;



public class Main {

    static java.sql.Connection dataCon = null;
    static PreparedStatement pstmt;

    static String articleBucketName;
    static String videoBucketName;
    static String region;
    static COSClient cosClient = null;
    static Logger logger = null;
    static {
        articleBucketName = "pic-article-1257964795";
        videoBucketName = "video-1257964795";
        region = "ap-beijing";
        COSCredentials cred = new BasicCOSCredentials("AKIDkIbfU4YZXUDgttF7MPDl36vUw9E6o7GK", "zjHchX8UbSCj9MM7ORFo8uUpwoUw9ltq");
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        cosClient = new COSClient(cred, clientConfig);
    }

    public static void main(String[] args) throws JMSException, SQLException {

        logger= Logger.getLogger(Main.class);
        logger.error("程序启动 11.26 11.04 调整");

        Session session = null;
        String mqUrl = "tcp://139.199.112.147:61616";
        String qName = "examine";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(mqUrl);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(qName);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                logger.error("回调函数触发" + message);
                TextMessage textMessage = (TextMessage) message;
                try {
                    logger.error("信息内容" + textMessage.getText());
                    dataCon = new DB().init();

                    String sql = "select * from video where id = ?";
                    pstmt = dataCon.prepareStatement(sql);
                    pstmt.setString(1, textMessage.getText());
                    ResultSet resultSet = pstmt.executeQuery();
                    if(!resultSet.next()) {
                        logger.error("没有相关视频id " + textMessage.getText() );
                        dataCon.close();
                        return;
                    }
                    String temp_url = resultSet.getString("video_up_path");

                    String tmpPath = "/tmp/temp_video/";
                    File filePath = new File(tmpPath);
                    if (!filePath.exists() && !filePath.isDirectory()) {
                        filePath.mkdir();
                    }

                    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss_");//设置日期格式
                    String strDate = df.format(new Date());// new Date()为获取当前系统时间

                    String fileName = strDate + getRandomString(8) + ".mp4";
                    String savaPath = tmpPath + fileName;
                    logger.error("准备调用转码程序： " + textMessage.getText() );
                    videoChange(temp_url,savaPath);
                    logger.error("调用转码程序完成： " + textMessage.getText() );

                    File fTemp = new File(savaPath);
                    if(!fTemp.exists()) {
                        logger.error("转码失败： " + textMessage.getText() );
                        sql = "update video set examine_status = 4 where id = ?";
                        pstmt =  dataCon.prepareStatement(sql);
                        pstmt.setString(1, textMessage.getText());
                        pstmt.execute();
                        dataCon.close();
                        return;
                    }

                    logger.error("转码完成： " + textMessage.getText() );
                    String key = fileName;
                    PutObjectRequest putObjectRequest = new PutObjectRequest(videoBucketName, key, fTemp);
                    cosClient.putObject(putObjectRequest);
                    fTemp.delete();
                    String ret_url = "http://" + videoBucketName + ".cos." + region + ".myqcloud.com" + "/" + key;


                    sql = "update video set video_show_path = ?,examine_status = 1 where id = ?";
                    pstmt = dataCon.prepareStatement(sql);
                    pstmt.setString(1, ret_url);
                    pstmt.setString(2, textMessage.getText());
                    pstmt.execute();
                    logger.error("处理完成： " + textMessage.getText() );
                    dataCon.close();
                } catch (JMSException e) {
                    logger.error("异常 JMSException：" );

                    e.printStackTrace();
                } catch (SQLException e) {
                    logger.error("异常 SQLException：" );
                    e.printStackTrace();
                }
            }
        });
    }


    /**
     *生成随机字符串
     */
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    /**
     *视频转码
     */
    public static boolean videoChange(String from,String to) {

        try {
            exec("/home/ubuntu/wu/dyFFmpeg "
                    + from + " " + to);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    //执行命令
    public static String exec(String command) throws InterruptedException {
        String returnString = "";
        Process pro = null;
        Runtime runTime = Runtime.getRuntime();
        if (runTime == null) {
            System.err.println("Create runtime false!");
        }
        try {
            pro = runTime.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            PrintWriter output = new PrintWriter(new OutputStreamWriter(pro.getOutputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                returnString = returnString + line + "\n";
            }
            input.close();
            output.close();
            pro.destroy();
        } catch (IOException ex) {
            System.out.println(ex);
        }
        return returnString;
    }
}
