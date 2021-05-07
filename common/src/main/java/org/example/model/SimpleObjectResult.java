package org.example.model;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @author: LinQin
 * @date: 2020/04/17
 */
@Data
public class SimpleObjectResult {

    /**
     * 滚动Id，只有scroll查询时，才有值
     */
    private String scrollId;

    /**
     * 总行数
     */
    private Long total;

    /**
     * 耗时（毫秒）
     */
    private Long took;

    /**
     * 表头
     */
    private List<String> headers = Lists.newArrayList();

    /**
     * 数据
     */
    private List<Map<String, Object>> lines = Lists.newArrayList();
}
