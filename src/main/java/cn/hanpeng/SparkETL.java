package cn.hanpeng;

import lombok.extern.log4j.Log4j;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 *
 *  纯Spark版本  尚未完成
 *  -e 4g -j 20190604112235 -k 20190604083605 -n hanpeng -p 3 -g 10000 -f yyyyMMddHHmmss
 *  -e 4g -j 20160111 -k 20160101 -n hanpeng -p 3 -f yyyyMMddHHmmss
 *
 */

@Log4j
public class SparkETL {
    public static void main(String[] args) throws ParseException, IOException, java.text.ParseException {
        long start=System.currentTimeMillis();
//        TaskVo task = StringUtil.check_args(args);
        SparkSession spark = SparkSession
                .builder().master("local[1]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        String [] predicates={"name='a'","name='b'"};
        Properties sourceProp=new Properties();
        sourceProp.put("user","dcap");
        sourceProp.put("password","1qaz@WSX");
        String sourceUrl="jdbc:mysql://192.168.1.13:3306/dcap?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        Dataset<Row> ds = spark.read().jdbc(sourceUrl, "a", predicates, sourceProp).withColumn("SIGN",lit(1)).withColumn("VERSION", col("LOAD_DT"));
        Properties targetProp=new Properties();
        targetProp.put("user","");
        targetProp.put("password","");
        String targetUrl="jdbc:clickhouse://192.168.1.13:8123/default";
        ds.write().mode(SaveMode.Append).jdbc(targetUrl,"a",targetProp);
        spark.close();
        long end=System.currentTimeMillis();
        log.info(String.format("task finished,exeTime:%d ms",(end-start)));
    }
}
