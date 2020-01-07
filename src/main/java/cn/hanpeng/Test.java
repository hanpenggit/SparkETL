package cn.hanpeng;

import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

@Log4j
public class Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local[1]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        JavaSparkContext java=new JavaSparkContext(spark.sparkContext());
        List<String> a=new ArrayList<>();
        a.add("a");
        a.add("b");
        a.add("c");
        JavaRDD<String> p = java.parallelize(a);
        JavaRDD<String> map = p.map(i -> {
            log.info("-----------------------------" + i);
            return i + "1";
        });
        map.count();
        spark.close();
    }
}
