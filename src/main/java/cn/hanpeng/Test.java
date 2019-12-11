package cn.hanpeng;

import com.alibaba.fastjson.JSONObject;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Log4j
public class Test {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        long start=System.currentTimeMillis();
        g();
        long end=System.currentTimeMillis();
        System.out.println("total ["+(end-start)+"] ms");
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
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        @Cleanup
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://192.168.56.103:8123/dcap");
        List<List<String>> data=new ArrayList<>();
        for(int i=1;i<=5000000;i++){
            List<String> row=new ArrayList<>();
            row.add(i+"");
            row.add("c"+i);
            row.add("1");
            row.add("3");
            data.add(row);
            if(i%20000==0){
                SparkETLNew.executeBatch(conn,"insert into A (ID,NAME,SIGN,VERSION) values (?,?,?,?)",data);
                log.info(i);
                data.clear();
            }
        }
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
                log.info(t+"");
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
        javaSparkContext.close();
    }

    public static void g() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        @Cleanup
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://192.168.56.103:8123/dcap");
        @Cleanup
        PreparedStatement ps = conn.prepareStatement("select ID,NAME,LOAD_DT,SIGN,VERSION from A where ID=?");
        ps.setString(1,"1");
        @Cleanup
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<String> columns = IntStream.range(1, columnCount + 1).mapToObj(i -> {
            try {
                return metaData.getColumnName(i);
            } catch (SQLException e) {
                e.printStackTrace();
                return "";
            }
        }).collect(Collectors.toList());

        List<Map<String,Object>> data=new ArrayList<>();
        while(rs.next()){
            Map<String,Object> row=new HashMap<>();
            columns.stream().forEach(column -> {
                try {
                    Object value = rs.getObject(column);
                    row.put(column,value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            data.add(row);
        }
        Optional<Object> first = data.stream().sorted((o1, o2) -> -o1.get("VERSION").toString().compareTo(o2.get("VERSION").toString())).map(i -> i.get("VERSION")).findFirst();
        int maxVersion=0;
        if(first.isPresent()){
            maxVersion= Integer.parseInt(first.get().toString());
        }
        int finalMaxVersion = maxVersion;
        data=data.stream().filter(i -> Integer.parseInt(i.get("VERSION").toString()) == finalMaxVersion).sorted(Comparator.comparing(o -> o.get("LOAD_DT").toString())).collect(Collectors.toList());
        String id="";
        String name="";
        int lastSign=0;
        int len=data.size();
        for (int i = 0; i < len; i++) {
            Map<String, Object> stringObjectMap = data.get(i);
            int sign = Integer.parseInt(stringObjectMap.get("SIGN").toString());
            if(lastSign+sign==0){
                lastSign=0;
                id="";
                name="";
            }else{
                lastSign=sign;
                id=stringObjectMap.get("ID").toString();
                name=stringObjectMap.get("NAME").toString();
            }
        }
        log.info("id:"+id+",name:"+name);
        /*data.forEach(i -> {
            logger.info("------------------------");
            columns.forEach(column -> {
                logger.info(column+":"+i.get(column));
            });
        });*/
    }
}
