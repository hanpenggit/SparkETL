package cn.hanpeng;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * 任务属性VO
 *
 * @author hanpeng
 * @create 2019-08-26 15:15
 */
@Data
@Builder
public class TaskVo implements Serializable {
    private String name;
    /**
     * 是否为本地模式，默认为true，是local模式，如果为false，则由spark-submit提交，parallelism和executorMemory将失效
     */
    @Builder.Default
    private Boolean isLocal=true;
    /**
     * 并行度
     */
    @Builder.Default
    private int parallelism=1;
    /**
     * executor的内存，默认为1g
     */
    @Builder.Default
    private String executorMemory="1g";

    @Builder.Default
    private Boolean readLog=true;

    private String sourceUser;
    private String sourcePwd;
    private String sourceUrl;
    private String sourceDriver;
    private String targetUser;
    private String targetPwd;
    private String targetUrl;
    private String targetDriver;
    private String selectSql;
    //查询的sql中，需要查询多少个列
    private Integer selectCount;
    private String insertSql;
    //检查的sql，只在插入clickhouse时使用
    private String checkSql;
    /**
     * 每批次入数多少条，默认为1000
     */
    @Builder.Default
    private Integer batchSize=1000;
    /**
     * 每次fatch多少条数据，默认为1000
     */
    @Builder.Default
    private Integer fetchSize=1000;
    private String startTime;
    private String endTime;
    /**
     * 任务的间隔时间，单位ms
     */
    private Integer intervalTime;
}
