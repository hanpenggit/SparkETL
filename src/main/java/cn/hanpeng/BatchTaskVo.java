package cn.hanpeng;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * 1
 *
 * @author hanpeng
 * @create 2019-12-11 11:50
 */
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BatchTaskVo implements Serializable {
    /**
     * 本批次的开始时间
     */
    private String start;
    /**
     * 本批次的结束时间
     */
    private String end;
    /**
     * 本批次查询的表分区，
     * 目前还不支持具体时间段和具体分区，只能分开，用时间段的只能用时间段，用表分区的只能用表分区
     */
    private String partition;
}
