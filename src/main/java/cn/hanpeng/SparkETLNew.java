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

    /**
     *  --name sparkETL --isLocal true --start 0 --end 10 --format num --interval 1
     * @author hanpeng
     * @date 2021/8/23 14:26
     * @param args 输入参数
     */
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
                List<List<Object>> data = new ArrayList(task_bro.getFetchSize());

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

                    while (rs.next()) {
                        ++count;
                        List<Object> d = new ArrayList(task_bro.getSelectCount());

                        for (int j = 1; j <= task_bro.getSelectCount(); ++j) {
                            d.add(rs.getObject(j));
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
                } catch (SQLException e) {
                    log.error("任务执行发生异常", e);
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

    public static void executeBatch(Connection conn, String sqlTemplate, List<List<Object>> list) {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlTemplate);
            conn.setAutoCommit(false);
            int size = list.size();
            List<Object> o;
            for (int i = 0; i < size; ++i) {
                o = list.get(i);
                for (int j = 0; j < o.size(); ++j) {
                    Object v = o.get(j);
                    ps.setObject(j + 1, v == null ? "" : v);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            conn.commit();
        } catch (SQLException e) {
            log.error("批量提交发生异常", e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

    }

    public static List<BatchTaskVo> createTask(TaskVo task) throws java.text.ParseException {
        String format = task.getFormat();
        String start = task.getStart();
        String end = task.getEnd();
        boolean startTimeIsNotBlank = StringUtils.isNotBlank(start);
        boolean endTimeIsNotBlank = StringUtils.isNotBlank(end);
        boolean partitionsIsNotBlank = StringUtils.isNotBlank(task.getPartitions());
        List<BatchTaskVo> tasks = new ArrayList();
        if (startTimeIsNotBlank && endTimeIsNotBlank) {
            if ("num".equals(format)) {
                createTaskByStartEnd(tasks, start, end, format, task.getInterval(), null);
            } else {
                createTaskByStartEndTime(tasks, start, end, format, task.getInterval(), null);
            }
        } else {
            BatchTaskVo b;
            if (startTimeIsNotBlank) {
                b = new BatchTaskVo();
                b.setStart(start);
                tasks.add(b);
            }

            if (endTimeIsNotBlank) {
                b = new BatchTaskVo();
                b.setEnd(end);
                tasks.add(b);
            }

            if (partitionsIsNotBlank) {
                String[] partitions = task.getPartitions().split(",");
                int length = partitions.length;

                for (int i = 0; i < length; ++i) {
                    String partition = partitions[i];
                    if (partition.contains(":")) {
                        String[] paritionTime = partition.split(":");
                        String partitionName = paritionTime[0];
                        String time = paritionTime[1];
                        String[] timeArr = time.split("-");
                        String s = timeArr[0];
                        String e = timeArr[1];
                        createTaskByStartEndTime(tasks, s, e, format, task.getInterval(), partitionName);
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
                                                String format, int interval, String partition) throws ParseException {
        Date startTime_dt = DateUtils.parseDate(startTime, format);
        Date endTime_dt = DateUtils.parseDate(endTime, format);
        Calendar start = Calendar.getInstance();
        Calendar end = Calendar.getInstance();
        start.setTime(startTime_dt);
        end.setTime(endTime_dt);
        while (end.after(start)) {
            String curr = DateFormatUtils.format(start, format);
            start.add(Calendar.SECOND, interval);
            String next = DateFormatUtils.format(start, format);
            BatchTaskVo b = new BatchTaskVo(curr, next, partition);
            tasks.add(b);
        }
    }

    public static void createTaskByStartEnd(List<BatchTaskVo> tasks, String start, String end,
                                            String format, int interval, String partition) {
        long st = Long.parseLong(start);
        long ed = Long.parseLong(end);
        while (st <= ed) {
            long e = st + interval;
            BatchTaskVo b = new BatchTaskVo(String.valueOf(st), String.valueOf(e), partition);
            tasks.add(b);
            st = e;
        }
    }
}
