package cn.hanpeng;

import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Test {
    private static Logger logger = Logger.getLogger(Test.class);
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        long start=System.currentTimeMillis();
        f();
        long end=System.currentTimeMillis();
        System.out.println("total ["+(end-start)+"] ms");
    }

    public static void a() {
        String a="BUSI_NO,RPT_PROV,PERS_NO,PERS_ID,SURNAME,FIRSTNAME,ENG_NAME,VERF_NAME,CHN_NAME,GENDER,BIRTH_DATE,COUNTRY_CODE,FGN_STS,APPLY_RSN,PERS_REG_CATG,CERT_TYPE,CERT_NO,VERF_CERT_NO,CERT_VLD,FGN_TYPE,FGN_CARD_ID,ORIG_VISA_TYPE,ORIG_VISA_NO,VISA_TYPE,VISA_NO,VISA_DATE,VISA_VALID,VISA_VLD,SEQ_ID,VISA_RSTD_RSN,DATA_SRC_FLAG,INVALID_FLAG,VISA_ASSIGN_DEPT,LOAD_DT";
        String [] arr=a.split(",");
        int ix=1;
        StringBuilder sr=new StringBuilder();
        for(String i:arr){
            System.out.println("rs.getString("+ix+"));");
            ix++;
            sr.append("?,");
        }
        System.out.println(sr.toString());
    }

    public static void b() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default","default","123123");
        PreparedStatement ps = conn.prepareStatement("select count(1) from A");
        ResultSet rs = ps.executeQuery();
        if(rs.next()){
            System.out.println(rs.getInt(1));
        }
        rs.close();
        ps.close();
        conn.close();
    }

    public static void c(){
        SparkSession spark= SparkSession.builder().getOrCreate();
    }

    public static void d() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default","default","123123");
        PreparedStatement ps = conn.prepareStatement("SELECT WYBS,XM,ZJHM,ETL_TIMESTAMP,LOAD_DT,SIGN,VERSION from A");
        ResultSet rs = ps.executeQuery();
        JSONObject record=new JSONObject();
        int maxVersion=-1;
        while(rs.next()){
            int version=rs.getInt(7);
            int sign=rs.getInt(6);
            if(version>maxVersion){
                maxVersion=version;
                record.put("WYBS",rs.getString(1));
                record.put("XM",rs.getString(2));
                record.put("ZJHM",rs.getString(3));
                record.put("ETL_TIMESTAMP",rs.getString(4));
                record.put("LOAD_DT",rs.getString(5));
                record.put("SIGN",sign);
                record.put("VERSION",version);
            }else if(version==maxVersion&&record.getIntValue("SIGN")==-sign){
                record.clear();
            }
        }
        rs.close();
        ps.close();
        conn.close();
        System.out.println(record.toJSONString());
    }

    public static void e() throws ClassNotFoundException, SQLException {
        long start_c=System.currentTimeMillis();
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default","default","123123");
//        Calendar now=Calendar.getInstance();
        List<List<String>> data=new ArrayList<>();
        for(int i=0;i<100000;i++){
            List<String> row=new ArrayList<>();
            /*int i1 = now.get(Calendar.YEAR);
            int i2 = now.get(Calendar.MONTH)+1;
            String s2=String.format("%02d", i2);
            int i3 = now.get(Calendar.DATE);
            String s3=String.format("%02d", i3);
            String date=i1+"-"+s2+"-"+s3+" 12:12:12";*/
//            System.out.println(date);
            row.add(i+"");
            row.add("2019-09-09 00:00:00");
            data.add(row);
//            now.add(Calendar.DATE,1);
        }
        long end_c=System.currentTimeMillis();
        System.out.println("create ["+(end_c-start_c)+"] ms");
        long start=System.currentTimeMillis();
        SparkETL.executeBatch(conn,"insert into B (WYBS,LOAD_DT,SIGN,VERSION) values (?,?,-1,1)",data);
        long end=System.currentTimeMillis();
        System.out.println("executeBatch ["+(end-start)+"] ms");
        conn.close();
    }

    public static void f(){
        SparkConf conf=new SparkConf();
        conf.set("spark.master","local[2]");
        conf.set("spark.app.name", "test");
        conf.set("spark.executor.memory", "4g");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        List<Integer> tasks=new ArrayList<>();
        IntStream.range(1,500).forEach(tasks::add);
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(tasks);
        rdd.repartition(100).foreachPartition(i -> {
            i.forEachRemaining(t -> {
                logger.info(t+"");
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
        javaSparkContext.close();
    }

}
