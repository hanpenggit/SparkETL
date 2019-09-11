package cn.hanpeng;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 *  -e 4g -j 20190604112235 -k 20190604083605 -n hanpeng -p 3
 */
public class SparkETL {
    private static Logger logger = Logger.getLogger(SparkETL.class);
    public static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
    public static void main(String[] args) throws ParseException, IOException, java.text.ParseException {
        long start=System.currentTimeMillis();
        TaskVo task = StringUtil.check_args(args);
        List<String> tasks=createTask(task);
        if(tasks.size()>0){
            startTask(task,tasks);
        }
        long end=System.currentTimeMillis();
        logger.info("task finished,exeTime:"+(end-start)+" ms");
    }

    public static void startTask(TaskVo task,List<String> tasks) throws java.text.ParseException {
        SparkConf conf=new SparkConf();
        if(task.getIsLocal()){
            conf.set("spark.master","local["+task.getParallelism()+"]");
            conf.set("spark.app.name", task.getName());
            conf.set("spark.executor.memory", task.getExecutorMemory());
        }
        logger.info("spark starting ");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        Broadcast<TaskVo> taskBroadcast = javaSparkContext.broadcast(task);
        JavaRDD<String> rdd = javaSparkContext.parallelize(tasks);
        logger.info("spark started ");
        if(task.getRepartitionNum()!=0){
            rdd=rdd.repartition(task.getRepartitionNum());
        }
        rdd.foreachPartition(i -> {
            TaskVo task_bro = taskBroadcast.getValue();
            Class.forName(task_bro.getSourceDriver());
            Class.forName(task_bro.getTargetDriver());
            Connection source= DriverManager.getConnection(task_bro.getSourceUrl(),task_bro.getSourceUser(),task_bro.getSourcePwd());
            if(!source.isClosed()){
                logger.info("source connection connected");
            }
            Connection target=DriverManager.getConnection(task_bro.getTargetUrl(),task_bro.getTargetUser(),task_bro.getTargetPwd());
            if(!target.isClosed()){
                logger.info("target connection connected");
            }
            i.forEachRemaining(row -> {
                String selectSql=task_bro.getSelectSql();
                String insertSql=task_bro.getInsertSql();
                List<List<String>> data=new ArrayList<>(task_bro.getFetchSize());
                try {
                    PreparedStatement ps = source.prepareStatement(selectSql);
                    ps.setFetchSize(task_bro.getFetchSize());
                    String [] arr=row.split(",");
                    ps.setString(1,arr[0]);
                    ps.setString(2,arr[1]);
                    ResultSet rs = ps.executeQuery();
                    int count=0;
                    while(rs.next()){
                        count++;
                        List<String> d=new ArrayList<String>(task_bro.getSelectCount());
                        for(int j=1;j<=task_bro.getSelectCount();j++){
                            d.add(rs.getString(j));
                        }
                        data.add(d);
                        if(count%task_bro.getBatchSize()==0){
                            executeBatch(target,insertSql,data);
                            logger.info("["+row+"]"+",size:"+count);
                            data.clear();
                        }
                    }
                    if(count>0){
                        executeBatch(target,insertSql,data);
                        logger.info("["+row+"]"+",last size:"+count);
                        data=null;
                    }
                    rs.close();
                    ps.close();
                } catch (SQLException e) {
                    logger.error("任务执行发生异常",e);
                }
            });
            source.close();
            logger.info("source connection closed");
            target.close();
            logger.info("target connection closed");
        });
        javaSparkContext.close();
        logger.info("task finished, spark closed ");
    }

    public static void executeBatch(Connection conn,String sqlTemplate, List<List<String>> list) {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlTemplate);
            conn.setAutoCommit(false);
            int size = list.size();
            List<String> o = null;
            for (int i = 0; i < size; i++) {
                o = list.get(i);
                for (int j = 0; j < o.size(); j++) {
                    String v=o.get(j);
                    ps.setString(j + 1, v==null?"":v);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            conn.commit();
        } catch (SQLException e) {
            logger.error("批量提交发生异常",e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    public static List<String> createTask(TaskVo task) throws java.text.ParseException, IOException {
        Set<String> logtask = null;
        if(task.getReadLog()){
            logtask = StringUtil.readTaskLog();
        }
        String startTime=task.getStartTime();
        String endTime=task.getEndTime();
        Date startTime_dt = sdf.parse(startTime);
        Date endTime_dt = sdf.parse(endTime);
        Calendar start=Calendar.getInstance();
        Calendar end=Calendar.getInstance();
        start.setTime(startTime_dt);
        end.setTime(endTime_dt);

        List<String> tasks=new ArrayList<>();
        while(end.after(start)){
            String curr=sdf.format(start.getTime());
            start.add(Calendar.MILLISECOND,task.getIntervalTime());
            String next=sdf.format(start.getTime());
            String taskTime=curr+","+next;
            if(logtask!=null&&logtask.contains(taskTime)){
                continue;
            }else{
                tasks.add(taskTime);
            }
        }
        logger.info("Task Generation Completion,a total of "+tasks.size()+" tasks");
        return tasks;
    }
}
