package com.pingan.examine.start;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.google.common.io.Resources;

/**
 * 通用配置文件加载类
 *
 * @author Administrator
 */
public class ConfigFactory {
    private final static Logger logger = LogManager.getLogger("ConfigFactory");

    public static String kafkaip;  //kafkaip

    public static String kafkaport;  //kafka端口号

    public static String kafkazookeeper;  //kafkazookeeper集群地址

    public static String mysqlurl;  //mysql连接信息

    public static String mysqlusername; //mysql用户名

    public static String mysqlpasswd; //mysql密码

    public static String sparkmaster; //stream运行模式

    public static String sparkappname; //stream任务名称

    public static int sparkseconds; //stream时间间隔

    public static String sqlmaster; //sql运行模式

    public static String sqlappname; //stream任务名称

    public static  String localpath; //本地保存路径

    public static String hdfspath; //hdfs保存路径

    public static Configuration hadoopconf;// hadoop配置文件

    /**
     * 初始化所有通用信息
     */
    public static void initConfig() {

        readCommons();
    }

    /**
     * 读取commons.xml,加载到内存
     */
    private static void readCommons() {
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(Resources.getResource("commons.xml"));
            Element root = document.getRootElement();//获取root节点

            Element kafkapath = root.element("kafka");//获取kafka连接信息
            kafkaip = kafkapath.element("ip").getText();
            kafkaport = kafkapath.element("port").getText();
            kafkazookeeper = kafkapath.element("zookeeper").getText();

            Element mysqlpath = root.element("mysql");//获取mysql连接信息
            mysqlurl = mysqlpath.element("url").getText();
            mysqlusername = mysqlpath.elementText("username");
            mysqlpasswd = mysqlpath.elementText("password");

            Element sparkpath = root.element("spark");//获取spark连接信息
            sparkmaster = sparkpath.elementText("master");
            sparkappname = sparkpath.elementText("appname");
            sparkseconds = Integer.valueOf(sparkpath.elementText("seconds"));
            sqlmaster=sparkpath.elementText("sqlmaster");
            sqlappname=sparkpath.elementText("sqlappname");

            Element filepath=root.element("path");
            localpath=filepath.elementText("localpath");
            hdfspath=filepath.elementText("hdfspath");

            hadoopconf=new Configuration();
            hadoopconf.addResource(Resources.getResource("core-site.xml"));


        } catch (DocumentException e) {
            logger.error("找不到commons.xml文件", e);
        }

    }
}
