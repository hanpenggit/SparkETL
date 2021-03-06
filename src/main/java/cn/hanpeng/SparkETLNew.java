package cn.hanpeng;

import com.alibaba.fastjson.JSON;
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
import java.text.ParseException;
import java.util.*;
import java.util.Date;

/**
 * @author hanpeng
 * @create 2020年07月15日 10:07
 */
public class SparkETLNew {
    private static final Logger log = Logger.getLogger(SparkETLNew.class);

    public static void main(String[] args) throws ParseException {
        long start = System.currentTimeMillis();
        TaskVo task = StringUtil.check_args(args);
        List<BatchTaskVo> tasks = createTask(task);
        if (tasks.size() > 0) {
            startTask(task, tasks);
        }

        long end = System.currentTimeMillis();
        log.info("task finished,exeTime:" + (end - start) + " ms");
    }

    public static void startTask(TaskVo task, List<BatchTaskVo> tasks) {
        SparkConf conf = new SparkConf();
        if (task.getIsLocal()) {
            conf.set("spark.master", "local[" + task.getParallelism() + "]");
            conf.set("spark.app.name", task.getName());
            conf.set("spark.executor.memory", task.getExecutorMemory());
        }

        log.info("spark starting ");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        Broadcast<TaskVo> taskBroadcast = javaSparkContext.broadcast(task);
        JavaRDD<BatchTaskVo> rdd = javaSparkContext.parallelize(tasks);
        log.info("spark started ");
        if (task.getRepartitionNum() != 0) {
            rdd = rdd.repartition(task.getRepartitionNum());
        }

        rdd.foreachPartition((i) -> {
            TaskVo task_bro = taskBroadcast.getValue();
            Class.forName(task_bro.getSourceDriver());
            Class.forName(task_bro.getTargetDriver());
            Connection source = DriverManager.getConnection(task_bro.getSourceUrl(), task_bro.getSourceUser(), task_bro.getSourcePwd());
            if (!source.isClosed()) {
                log.info("source connection connected");
            }

            Connection target = DriverManager.getConnection(task_bro.getTargetUrl(), task_bro.getTargetUser(), task_bro.getTargetPwd());
            if (!target.isClosed()) {
                log.info("target connection connected");
            }

            i.forEachRemaining((row) -> {
                String selectSql = task_bro.getSelectSql();
                String insertSql = task_bro.getInsertSql();
                ArrayList data = new ArrayList(task_bro.getFetchSize());

                try {
                    Map<String, String> kvs = new HashMap();
                    kvs.put("${start}", row.getStart());
                    kvs.put("${end}", row.getEnd());
                    kvs.put("${partition}", row.getPartition());
                    selectSql = StringUtil.parse(selectSql, kvs);
                    PreparedStatement ps = source.prepareStatement(selectSql);
                    ps.setFetchSize(task_bro.getFetchSize());
                    ResultSet rs = ps.executeQuery();
                    int count = 0;

                    while(rs.next()) {
                        ++count;
                        List<String> d = new ArrayList(task_bro.getSelectCount());

                        for(int j = 1; j <= task_bro.getSelectCount(); ++j) {
                            d.add(rs.getString(j));
                        }

                        data.add(d);
                        if (count % task_bro.getBatchSize() == 0) {
                            executeBatch(target, insertSql, data);
                            data.clear();
                        }
                    }

                    if (count > 0) {
                        executeBatch(target, insertSql, data);
                        log.info("[" + JSON.toJSONString(row) + "]" + ",size:" + count);
                    }

                    rs.close();
                    ps.close();
                } catch (SQLException var13) {
                    log.error("任务执行发生异常", var13);
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

    public static void executeBatch(Connection conn, String sqlTemplate, List<List<String>> list) {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlTemplate);
            conn.setAutoCommit(false);
            int size = list.size();
            List<String> o;
            for(int i = 0; i < size; ++i) {
                o = list.get(i);
                for(int j = 0; j < o.size(); ++j) {
                    String v = o.get(j);
                    ps.setString(j + 1, v == null ? "" : v);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            conn.commit();
        } catch (SQLException var10) {
            log.error("批量提交发生异常", var10);
            try {
                conn.rollback();
            } catch (SQLException var9) {
                var9.printStackTrace();
            }
        }

    }

    public static List<BatchTaskVo> createTask(TaskVo task) throws java.text.ParseException{
        String format = task.getFormat();
        String startTime = task.getStartTime();
        String endTime = task.getEndTime();
        boolean startTimeIsNotBlank = StringUtils.isNotBlank(startTime);
        boolean endTimeIsNotBlank = StringUtils.isNotBlank(endTime);
        boolean partitionsIsNotBlank = StringUtils.isNotBlank(task.getPartitions());
        List<BatchTaskVo> tasks = new ArrayList();
        if (startTimeIsNotBlank && endTimeIsNotBlank) {
            createTaskByStartEndTime(tasks, startTime, endTime, format, task.getIntervalTime(),null);
        } else {
            BatchTaskVo b;
            if (startTimeIsNotBlank) {
                b = new BatchTaskVo();
                b.setStart(startTime);
                tasks.add(b);
            }

            if (endTimeIsNotBlank) {
                b = new BatchTaskVo();
                b.setEnd(endTime);
                tasks.add(b);
            }

            if (partitionsIsNotBlank) {
                String[] partitions = task.getPartitions().split(",");
                String[] var9 = partitions;
                int var10 = partitions.length;

                for(int var11 = 0; var11 < var10; ++var11) {
                    String partition = var9[var11];
                    if (partition.contains(":")) {
                        String[] paritionTime = partition.split(":");
                        String partitionName = paritionTime[0];
                        String time = paritionTime[1];
                        String[] timeArr = time.split("-");
                        String start = timeArr[0];
                        String end = timeArr[1];
                        createTaskByStartEndTime(tasks, start, end, format, task.getIntervalTime(), partitionName);
                    } else {
                        BatchTaskVo bs = new BatchTaskVo();
                        bs.setPartition(partition);
                        tasks.add(bs);
                    }
                }
            }
        }

        log.info("Task Generation Completion,a total of " + tasks.size() + " tasks");
        return tasks;
    }

    public static void createTaskByStartEndTime(List<BatchTaskVo> tasks, String startTime, String endTime,
                                                String format, int intervalTime, String partition) throws ParseException {
        Date startTime_dt = DateUtils.parseDate(startTime, format);
        Date endTime_dt = DateUtils.parseDate(endTime, format);
        Calendar start = Calendar.getInstance();
        Calendar end = Calendar.getInstance();
        start.setTime(startTime_dt);
        end.setTime(endTime_dt);
        while(end.after(start)) {
            String curr = DateFormatUtils.format(start, format);
            start.add(Calendar.SECOND, intervalTime);
            String next = DateFormatUtils.format(start, format);
            BatchTaskVo b = new BatchTaskVo(curr, next, partition);
            tasks.add(b);
        }
    }
}
