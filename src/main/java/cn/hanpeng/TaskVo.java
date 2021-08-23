package cn.hanpeng;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.sql.Connection;

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
     * 重新分区，分区的个数，0代表不重新分区，即有多少个并行就会有多少个分区
     * 该个数不能小于并行数
     */
    @Builder.Default
    private int repartitionNum=0;
    /**
     * executor的内存，默认为1g
     */
    @Builder.Default
    private String executorMemory="1g";

    @Builder.Default
    private Boolean readLog=false;

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
    private String start;
    private String end;
    /**
     * 任务的间隔，可能为时间间隔，也可能为数字的间隔
     */
    private Integer interval;

    /**
     * 日期格式化的格式，必填
     */
    private String format;
    /**
     * 也可以按照分区进行批量抽取，多个分区名称用,分开
     */
    private String partitions;
}
