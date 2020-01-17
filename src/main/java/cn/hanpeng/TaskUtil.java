package cn.hanpeng;

import com.alibaba.fastjson.JSON;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author hanpeng
 * @create 2019-12-29 9:23
 */
@Slf4j
public class TaskUtil {

    public static void executeEtlTask(TaskVo task_bro, BatchTaskVo row, Connection source,Connection target){
        String selectSql=task_bro.getSelectSql();
        String insertSql=task_bro.getInsertSql();
        List<List<String>> data=new ArrayList<>(task_bro.getFetchSize());
        try {
            Map<String,String> kvs=new HashMap<>();
            kvs.put("${start}",row.getStart());
            kvs.put("${end}",row.getEnd());
            kvs.put("${partition}",row.getPartition());
            selectSql=StringUtil.parse(selectSql,kvs);
            @Cleanup
            PreparedStatement ps = source.prepareStatement(selectSql);
            ps.setFetchSize(task_bro.getFetchSize());
            @Cleanup
            ResultSet rs = ps.executeQuery();
            int count=0;
            while(rs.next()){
                count++;
                List<String> d=new ArrayList<>(task_bro.getSelectCount());
                for(int j=1;j<=task_bro.getSelectCount();j++){
                    d.add(rs.getString(j));
                }
                data.add(d);
                if(count%task_bro.getBatchSize()==0){
                    executeBatch(target,insertSql,data);
                    data.clear();
                }
            }
            if(count>0){
                executeBatch(target,insertSql,data);
                log.info("{},size:{}",JSON.toJSONString(row),count);
            }
        } catch (SQLException e) {
            log.error("任务执行发生异常",e);
        }
    }

    public static void executeBatch(Connection conn,String sqlTemplate, List<List<String>> list) {
        try {
            @Cleanup
            PreparedStatement ps = conn.prepareStatement(sqlTemplate);
            conn.setAutoCommit(false);
            int size = list.size();
            List<String> o;
            for (int i = 0; i < size; i++) {
                o = list.get(i);
                int os = o.size();
                for (int j = 0; j < os ; j++) {
                    String v=o.get(j);
                    ps.setString(j + 1, v==null?"":v);
                }
                ps.addBatch();
            }
            ps.executeBatch();
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

    public static List<BatchTaskVo> createTask(TaskVo task) throws java.text.ParseException, IOException {
        String format=task.getFormat();
        String startTime=task.getStartTime();
        String endTime=task.getEndTime();

        boolean startTimeIsNotBlank= StringUtils.isNotBlank(startTime);
        boolean endTimeIsNotBlank=StringUtils.isNotBlank(endTime);
        boolean partitionsIsNotBlank=StringUtils.isNotBlank(task.getPartitions());

        List<BatchTaskVo> tasks=new ArrayList<>();
        if(startTimeIsNotBlank&&endTimeIsNotBlank){
            createTaskByStartEndTime(tasks,startTime,endTime,format,task.getIntervalTime(),null);
        }else{
            if(startTimeIsNotBlank){
                BatchTaskVo b = new BatchTaskVo();
                b.setStart(startTime);
                tasks.add(b);
            }
            if(endTimeIsNotBlank){
                BatchTaskVo b = new BatchTaskVo();
                b.setEnd(endTime);
                tasks.add(b);
            }
            if(partitionsIsNotBlank){
                String[] partitions = task.getPartitions().split(",");
                for (String partition : partitions) {
                    if(partition.contains(":")){
                        String[] paritionTime = partition.split(":");
                        String partitionName=paritionTime[0];
                        String time=paritionTime[1];
                        String[] timeArr = time.split("-");
                        String start=timeArr[0];
                        String end=timeArr[1];
                        createTaskByStartEndTime(tasks,start,end,format,task.getIntervalTime(),partitionName);
                    }else{
                        BatchTaskVo b = new BatchTaskVo();
                        b.setPartition(partition);
                        tasks.add(b);
                    }
                }
            }
        }
        log.info("Task Generation Completion,a total of "+tasks.size()+" tasks");
        return tasks;
    }

    public static void createTaskByStartEndTime(List<BatchTaskVo> tasks,String startTime,String endTime,String format,int intervalTime,String partition) throws java.text.ParseException {
        Date startTime_dt = DateUtils.parseDate(startTime,format);
        Date endTime_dt = DateUtils.parseDate(endTime,format);
        Calendar start=Calendar.getInstance();
        Calendar end=Calendar.getInstance();
        start.setTime(startTime_dt);
        end.setTime(endTime_dt);

        while(end.after(start)){
            String curr= DateFormatUtils.format(start,format);
            start.add(Calendar.SECOND,intervalTime);
            String next=DateFormatUtils.format(start,format);
            BatchTaskVo b = new BatchTaskVo(curr,next,partition);
            tasks.add(b);
        }
    }
}
