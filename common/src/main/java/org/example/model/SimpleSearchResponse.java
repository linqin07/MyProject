package org.example.model;

import lombok.Data;

import java.util.List;

/**
 * @Description:
 * @author: LinQin
 * @date: 2020/04/16
 */
@Data
public class SimpleSearchResponse {

    /**
     * 总行数
     */
    private Long total;

    /**
     * 耗时（毫秒）
     */
    private Long took;

    /**
     * 结果
     */
    private Object result;

    /**
     * 滚动Id，只有scroll查询时，才有值
     */
    private String scrollId;

    /**
     * Field
     */
    private List<Object> fields;
}