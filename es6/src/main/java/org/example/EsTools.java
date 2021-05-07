package org.example;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.example.model.SimpleObjectResult;
import org.example.model.SimpleSearchResponse;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractException;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * @author: Tianlun
 * @date: 2020/3/12
 * @description:
 */
@Slf4j
@Deprecated
public class EsTools {
    @Getter
    @Setter
    public static TransportClient client;

    /**
     * 将ES数据提取/封装
     *
     * @param total    总行数
     * @param took     耗时
     * @param result   ES查询结果
     * @param scrollId 滚动查询id
     * @return 处理结果
     */
    public static SimpleObjectResult extract(Long total, Long took, ObjectResult result, String scrollId) {
        SimpleObjectResult simpleObjectResult = new SimpleObjectResult();
        simpleObjectResult.setScrollId(scrollId);
        simpleObjectResult.setTotal(total);
        simpleObjectResult.setTook(took);

        if (total == null || total == 0) {
            return simpleObjectResult;
        }
        simpleObjectResult.setHeaders(result.getHeaders());

        List<String> header = result.getHeaders();

        final List<Map<String, Object>> lines = Optional.ofNullable(result)
                                                        .map(ObjectResult::getLines)
                                                        .orElse(Collections.emptyList())
                                                        .stream()
                                                        .map(line -> {
                                                            final HashMap<String, Object> tmp = Maps.newHashMap();
                                                            final Iterator<Object> iterator = line.iterator();
                                                            result.getHeaders().forEach(key -> {
                                                                if (iterator.hasNext()) {
                                                                    tmp.put(key, iterator.next());
                                                                }
                                                            });
                                                            return tmp;
                                                        }).collect(toList());


        simpleObjectResult.setLines(lines);
        return simpleObjectResult;
    }

    @SneakyThrows
    public <T> List<T> executeSql(@NonNull String sql, @NonNull Class<T> clazz) {
        List<T> result = null;
        try {
            final List<Map<String, Object>> maps = executeSql(sql);
            result = maps.stream()
                         .map(map -> BeanUtil.mapToBean(map, clazz, true))
                         .collect(toList());
        } catch (Exception e) {
            log.error("executeSql fail,sql:{}", sql, e);
            return Collections.emptyList();
        }
        return result;
    }

    public List<Map<String, Object>> executeSql(@NonNull String sql) {
        return executeSql(sql, true);
    }

    /**
     * 执行sql
     *
     * @param sql
     * @param castType 是否开启特殊字段的类型转换
     * @return
     */
    @SneakyThrows
    public List<Map<String, Object>> executeSql(@NonNull String sql, boolean castType) {
        final long startTime = System.currentTimeMillis();
        final QueryAction explain = new SearchDao(client).explain(sql);
        SimpleSearchResponse execution = QueryActionExcuter.executeAnyAction(client, explain);
        ObjectResult extractResults = new ObjectResultsExtractor(true, false, false)
                .extractResults(execution.getResult(), true);

        final List<Map<String, Object>> result = Optional.ofNullable(extractResults)
                                                         .map(ObjectResult::getLines)
                                                         .orElse(Collections.emptyList())
                                                         .stream()
                                                         .map(line -> {
                                                             final HashMap<String, Object> tmp = Maps.newHashMap();
                                                             final Iterator<Object> iterator = line.iterator();
                                                             extractResults.getHeaders().forEach(key -> {
                                                                 if (iterator.hasNext()) {
                                                                     tmp.put(key, iterator.next());
                                                                 }
                                                             });
                                                             return tmp;
                                                         }).collect(toList());
        // 进行类型转换
        // if (castType) {
        //     result.forEach(map ->
        //             NEED_TRAN_LONG_FIELDS.forEach(field -> ofNullable(map.get(field))
        //                     .filter(v -> v instanceof Double).map(v -> (Double) v).map(Double::longValue)
        //                     .ifPresent(fieldVal -> map.put(field, fieldVal))
        //             )
        //     );
        // }
        log.info("executeSql:{},size:{},spend:{} ms", sql, result.size(), System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 执行sql,滚动全量获取
     * @param sql
     * @return
     * @throws Exception
     */
    public SimpleObjectResult submitSql(String sql) throws Exception {
        final QueryAction explain = new SearchDao(client).explain(sql);
        SimpleSearchResponse execution = QueryActionExcuter.executeAnyAction(client, explain);
        ObjectResult extractResults = new ObjectResultsExtractor(true, false, false)
                .extractResults(execution.getResult(), true);
        SimpleObjectResult result = extract(execution.getTotal(), execution.getTook(), extractResults, execution
                .getScrollId());
        // 滚动获取全部数据
        if (StringUtils.isNotEmpty(result.getScrollId())) {
            SimpleObjectResult scrollResult = getScrollResult(result.getScrollId());
            result.getLines().addAll(scrollResult.getLines());
            while (scrollResult.getLines() != null && scrollResult.getLines().size() != 0) {
                scrollResult = getScrollResult(result.getScrollId());
                result.getLines().addAll(scrollResult.getLines());
            }
        }
        return result;
    }

    /**
     * 获取滚动数据
     * @param scrollId
     * @return
     * @throws Exception
     */
    public SimpleObjectResult getScrollResult(String scrollId) throws Exception {
        // 执行请求
        SearchResponse response = client.prepareSearchScroll(scrollId)
                                        .setScroll(new TimeValue(60000))
                                        .execute()
                                        .actionGet();
        SearchHits hits = response.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(hits);
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(response.getTook().getMillis());

        // 滚动查询时才有scrollId
        if (null != response.getScrollId()) {
            simpleSearchResponse.setScrollId(response.getScrollId());
        }

        // 提取结果
        ObjectResult result = null;
        try {
            result = new ObjectResultsExtractor(true, false, false)
                    .extractResults(simpleSearchResponse.getResult(), true);
        } catch (ObjectResultsExtractException e) {
            log.error(e.getMessage(), e);
            throw new Exception("滚动查询出错：" + e.getMessage());
        }
        // 封装数据
        return extract(simpleSearchResponse.getTotal(), simpleSearchResponse.getTook(), result, simpleSearchResponse.getScrollId());
    }





    public <T> List<Map<String, Object>> insertBeans(@NonNull List<T> data, @NonNull String indexName) {
        final List<Map<String, Object>> maps = data.stream().map(BeanUtil::beanToMap).collect(toList());
        return insertMap(maps, indexName);
    }

    @SneakyThrows
    public List<Map<String, Object>> insertMap(@NonNull List<Map<String, Object>> data, @NonNull String indexName) {
        if (data.isEmpty()) {
            return data;
        }
        List<Map<String, Object>> failData = Lists.newLinkedList();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        createIndexCheck(indexName);
        List<List<Map<String, Object>>> split = CollectionUtil.split(data, 5000);
        //数据进行分批均分,防止一瞬间插入数据太多
        for (List<Map<String, Object>> mapList : split) {
            invokeAndParse(indexName, failData, bulkRequest, mapList);
        }
        return failData;
    }

    private void invokeAndParse(@NonNull String indexName,
            List<Map<String, Object>> failData,
            BulkRequestBuilder bulkRequest,
            List<Map<String, Object>> data) throws IOException {
        long start = System.currentTimeMillis();
        for (Map<String, Object> lineMap : data) {
            XContentBuilder xb = XContentFactory.jsonBuilder().startObject();
            for (Map.Entry<String, Object> entry : lineMap.entrySet()) {
                try {
                    xb.field(entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    log.error("xContentBuilder field fail,kv:{}", entry, e);
                }
            }
            xb.endObject();
            IndexRequestBuilder request = client.prepareIndex(indexName, "doc").setSource(xb);
            bulkRequest.add(request);
        }
        BulkResponse rps = bulkRequest.execute().actionGet();
        int index = 0;
        for (BulkItemResponse rp : rps) {
            if (rp.isFailed()) {
                failData.add(data.get(index));
                log.error("insert fail,index:{},errorInfo:{}", indexName, rp.getFailureMessage());
            }
            index++;
        }
        log.info("insert success,index:{},size:{},failSize:{},spend:{} ms",
                indexName, data.size(), failData.size(), System.currentTimeMillis() - start);
    }

    public void createIndexCheck(@NonNull String indexName) {
        boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
        if (!exists) {
            CreateIndexResponse rp = client.admin().indices().prepareCreate(indexName).execute().actionGet();
            log.info("create index success,name:{}", rp.index());
        }
    }

}
