package cn.hanpeng;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.util.List;
import java.util.concurrent.*;

/**
 *  此实现方式采用纯java，使用连接池进行并行
 * @author hanpeng
 * @create 2019-12-29 8:40
 */
@Log4j
public class JavaETL {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        TaskVo task = StringUtil.check_args(args);
        int parallel=task.getParallelism();
        List<BatchTaskVo> tasks= null;
        try {
            tasks = TaskUtil.createTask(task);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        ThreadFactory etlThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("etlThread-%d").build();
        ExecutorService threadPool = new ThreadPoolExecutor(parallel, parallel,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024), etlThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        DruidDataSource source = DataSourceUtil.getSourceDataSource(task);
        DruidDataSource target = DataSourceUtil.getTargetDataSource(task);
        for (BatchTaskVo batchTaskVo : tasks) {
            threadPool.execute(new EtlTask(source,target,batchTaskVo,task));
        }
        try {
            threadPool.awaitTermination(2,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            threadPool.shutdown();
            source.close();
            target.close();
            long end=System.currentTimeMillis();
            log.info(String.format("task finished,exeTime:%d ms",(end-start)));
        }
    }

    @AllArgsConstructor
    public static class EtlTask implements  Runnable{
        DruidDataSource source;
        DruidDataSource target;
        BatchTaskVo batchTaskVo;
        TaskVo task;

        @SneakyThrows
        @Override
        public void run() {
            TaskUtil.executeEtlTask(task,batchTaskVo,source.getConnection(),target.getConnection());
        }
    }
}
