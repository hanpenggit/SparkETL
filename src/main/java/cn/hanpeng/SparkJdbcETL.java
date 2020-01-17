package cn.hanpeng;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 *
 *  并行和分区采用Spark  jdbc仍然手动实现
 *  纯用Spark实现，请参考 SparkETL ，但是仍然推荐不用纯Spark
 *  因为纯Spark的并行，每个分区(分区基于predicates集合的个数)都将新建一个数据库连接，会造成特别多的创建和关闭数据连接的动作
 *  当前类的实现是先将任务进行分区，每个分区会有多个任务，如果不使用 -g 进行重新分区，则哪怕是10万个任务，也只会建立与并行数相同的数据库连接
 *
 *  -e 4g -j 20190604112235 -k 20190604083605 -n hanpeng -p 3 -g 10000 -f yyyyMMddHHmmss
 *  -e 4g -j 20160111 -k 20160101 -n hanpeng -p 3 -f yyyyMMddHHmmss
 *
 */
@Slf4j
public class SparkJdbcETL {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        TaskVo task = StringUtil.check_args(args);
        List<BatchTaskVo> tasks= null;
        try {
            tasks = TaskUtil.createTask(task);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        if(tasks.size()>0){
            startTask(task,tasks);
        }
        long end=System.currentTimeMillis();
        log.info("task finished,exeTime:{} ms",(end-start));
    }

    public static void startTask(TaskVo task,List<BatchTaskVo> tasks) {
        SparkConf conf=new SparkConf();
        if(task.getIsLocal()){
            conf.set("spark.master","local["+task.getParallelism()+"]");
            conf.set("spark.app.name", task.getName());
            conf.set("spark.executor.memory", task.getExecutorMemory());
        }
        log.info("spark starting ");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        Broadcast<TaskVo> taskBroadcast = javaSparkContext.broadcast(task);
        JavaRDD<BatchTaskVo> rdd = javaSparkContext.parallelize(tasks);
        log.info("spark started ");
        if(task.getRepartitionNum()!=0){
            rdd=rdd.repartition(task.getRepartitionNum());
        }
        rdd.foreachPartition(i -> {
            TaskVo task_bro = taskBroadcast.getValue();
            Class.forName(task_bro.getSourceDriver());
            Class.forName(task_bro.getTargetDriver());
            Connection source= DriverManager.getConnection(task_bro.getSourceUrl(),task_bro.getSourceUser(),task_bro.getSourcePwd());
            if(!source.isClosed()){
                log.info("source connection connected");
            }
            Connection target=DriverManager.getConnection(task_bro.getTargetUrl(),task_bro.getTargetUser(),task_bro.getTargetPwd());
            if(!target.isClosed()){
                log.info("target connection connected");
            }
            i.forEachRemaining(row -> {
                TaskUtil.executeEtlTask(task_bro,row,source,target);
            });
            source.close();
            log.info("source connection closed");
            target.close();
            log.info("target connection closed");
        });
        javaSparkContext.close();
        log.info("task finished, spark closed ");
    }


}
