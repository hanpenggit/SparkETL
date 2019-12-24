package cn.hanpeng;

import lombok.extern.log4j.Log4j;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;

/**
 *  -e 4g -j 20190604112235 -k 20190604083605 -n hanpeng -p 3 -g 10000 -f yyyyMMddHHmmss
 *  -e 4g -j 20160111 -k 20160101 -n hanpeng -p 3 -f yyyyMMddHHmmss
 *
 *  此方法执行替换开始时间和结束时间仍然用?
 *
 *  此方法已经过时，可以使用SparkETLNew来代替
 */
@Log4j
@Deprecated
public class SparkETL {
    public static void main(String[] args) throws ParseException, IOException, java.text.ParseException {
        long start=System.currentTimeMillis();
        TaskVo task = StringUtil.check_args(args);
        List<String> tasks=createTask(task);
        if(tasks.size()>0){
            startTask(task,tasks);
        }
        long end=System.currentTimeMillis();
        log.info("task finished,exeTime:"+(end-start)+" ms");
    }

    public static void startTask(TaskVo task,List<String> tasks) throws java.text.ParseException {
        SparkConf conf=new SparkConf();
        if(task.getIsLocal()){
            conf.set("spark.master","local["+task.getParallelism()+"]");
            conf.set("spark.app.name", task.getName());
            conf.set("spark.executor.memory", task.getExecutorMemory());
        }
        log.info("spark starting ");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        Broadcast<TaskVo> taskBroadcast = javaSparkContext.broadcast(task);
        JavaRDD<String> rdd = javaSparkContext.parallelize(tasks);
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
                String selectSql=task_bro.getSelectSql();
                String insertSql=task_bro.getInsertSql();
                List<List<String>> data=new ArrayList<>(task_bro.getFetchSize());
                try {
                    PreparedStatement ps = source.prepareStatement(selectSql);
                    ps.setFetchSize(task_bro.getFetchSize());
                    String [] arr=row.split(",");
                    if(arr.length==2){
                        ps.setString(1,arr[0]);
                        ps.setString(2,arr[1]);
                    }else{
                        ps.setString(1,row);
                    }

                    ResultSet rs = ps.executeQuery();
                    int count=0;
                    while(rs.next()){
                        count++;
                        List<String> d=new ArrayList<>(task_bro.getSelectCount());
                        for(int j=1;j<=task_bro.getSelectCount();j++){
                            d.add(rs.getString(j));
                        }
                        d.add("1");
                        d.add(System.currentTimeMillis()+"");
                        data.add(d);
                        if(count%task_bro.getBatchSize()==0){
                            executeBatch(target,insertSql,data);
                            data.clear();
                        }
                    }
                    if(count>0){
                        executeBatch(target,insertSql,data);
                        log.info("["+row+"]"+",size:"+count);
                    }
                    rs.close();
                    ps.close();
                } catch (SQLException e) {
                    log.error("任务执行发生异常",e);
                }
            });
            source.close();
            log.info("source connection closed");
            target.close();
            log.info("target connection closed");
        });
        javaSparkContext.close();
        log.info("task finished, spark closed ");
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
            log.error("批量提交发生异常",e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    public static List<String> createTask(TaskVo task) throws java.text.ParseException, IOException {
        Set<String> logtask;
        if(task.getReadLog()){
            logtask = StringUtil.readTaskLog();
        }else{
            logtask = new HashSet<>();
        }
        String format=task.getFormat();
        String startTime=task.getStartTime();
        String endTime=task.getEndTime();

        boolean startTimeIsNotBlank=StringUtils.isNotBlank(startTime);
        boolean endTimeIsNotBlank=StringUtils.isNotBlank(endTime);
        boolean partitionsIsNotBlank=StringUtils.isNotBlank(task.getPartitions());

        List<String> tasks=new ArrayList<>();
        if(startTimeIsNotBlank&&endTimeIsNotBlank){
            Date startTime_dt = DateUtils.parseDate(startTime,format);
            Date endTime_dt = DateUtils.parseDate(endTime,format);
            Calendar start=Calendar.getInstance();
            Calendar end=Calendar.getInstance();
            start.setTime(startTime_dt);
            end.setTime(endTime_dt);

            while(end.after(start)){
                String curr=DateFormatUtils.format(start,format);
                start.add(Calendar.SECOND,task.getIntervalTime());
                String next=DateFormatUtils.format(start,format);
                String taskTime=curr+","+next;
                if(!logtask.contains(taskTime)){
                    tasks.add(taskTime);
                }
            }
        }else{
            if(startTimeIsNotBlank){
                tasks.add(startTime);
            }
            if(endTimeIsNotBlank){
                tasks.add(endTime);
            }
            if(partitionsIsNotBlank){
                String[] partitions = task.getPartitions().split(",");
                tasks.addAll(Arrays.asList(partitions));
            }
        }
        log.info("Task Generation Completion,a total of "+tasks.size()+" tasks");
        return tasks;
    }
}
