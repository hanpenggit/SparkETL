package cn.hanpeng;

import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 -e 4g -j 20190604112235 -k 20190604083605 -n hanpeng -p 3

CREATE TABLE A(WYBS String,XM String,ZJHM String,ETL_TIMESTAMP String,LOAD_DT datetime default now(),SIGN Int8,VERSION UInt8) ENGINE = VersionedCollapsingMergeTree(SIGN,VERSION) ORDER BY WYBS

insert into A(WYBS,XM,ZJHM,ETL_TIMESTAMP,SIGN) values (?,?,?,?,1)

 select COUNT(1) from (
 select WYBS,XM,ZJHM,ETL_TIMESTAMP,VERSION from A  group by WYBS,XM,ZJHM,ETL_TIMESTAMP,VERSION having SUM(SIGN)>0 order by VERSION desc
 )

 select WYBS,COUNT(1) from A GROUP BY WYBS HAVING COUNT(1)>1;

 select * from A where WYBS='WYBS666';

 select WYBS,XM,ZJHM,ETL_TIMESTAMP,VERSION from A where WYBS='WYBS666' group by WYBS,XM,ZJHM,ETL_TIMESTAMP,VERSION having SUM(SIGN)>0 ORDER by VERSION DESC
**/
@Log4j
public class SparkOracleToClickHouse {
    public static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
    public static void main(String[] args) throws ParseException, IOException, java.text.ParseException {
        TaskVo task = StringUtil.check_args(args);
        List<String> tasks=createTask(task);
        startTask(task,tasks);
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
                        insertClickHouse(target,d,task);
                    }
                    rs.close();
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
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

    public static void insertClickHouse(Connection conn,List<String> values,TaskVo task) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(task.getCheckSql());
        ps.setString(1,values.get(0));
        ResultSet rs = ps.executeQuery();
        VersionVo vo = new VersionVo();
        int data_len=task.getSelectCount();
        if(rs.next()){
            vo.setExist(true);
            List<String> data=new ArrayList<>();
            for(int i=1;i<=data_len;i++){
                data.add(rs.getString(i));
            }
            vo.setVersion(rs.getInt(data_len+1));
            vo.setData(data);
        }else{
            vo.setExist(false);
            vo.setVersion(1);
        }
        rs.close();
        ps.close();
        PreparedStatement preparedStatement = conn.prepareStatement(task.getInsertSql());
        if(vo.getExist()){
            int len=vo.getData().size();
            for(int i=0;i<len;i++){
                preparedStatement.setString(i+1,vo.getData().get(i));
            }
            preparedStatement.setInt(len+1,-1);
            preparedStatement.setInt(len+2,vo.getVersion());
            preparedStatement.addBatch();
            for(int i=0;i<len;i++){
                preparedStatement.setString(i+1,values.get(i));
            }
            preparedStatement.setInt(len+1,1);
            preparedStatement.setInt(len+2,vo.getVersion()+1);
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        }else{
            int len=values.size();
            for(int i=0;i<len;i++){
                preparedStatement.setString(i+1,values.get(i));
            }
            preparedStatement.setInt(len+1,1);
            preparedStatement.setInt(len+2,vo.getVersion());
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
    }

    public static List<String> createTask(TaskVo task) throws java.text.ParseException {
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
            tasks.add(curr+","+next);
        }
        log.info("Task Generation Completion,a total of "+tasks.size()+" tasks");
        return tasks;
    }

    @Data
    public static class VersionVo{
        private Integer version;
        private Boolean exist;
        private List<String> data;
    }
}
