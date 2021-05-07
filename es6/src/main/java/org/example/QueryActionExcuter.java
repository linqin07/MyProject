package org.example;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugin.nlpcn.ElasticHitsExecutor;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.MultiRequestExecutorFactory;
import org.elasticsearch.search.SearchHits;
import org.example.model.SimpleSearchResponse;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.*;
import org.nlpcn.es4sql.query.join.ESJoinQueryAction;
import org.nlpcn.es4sql.query.multi.MultiQueryAction;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.io.IOException;

/**
 * @Description:
 * @author: LinQin
 * @date: 2020/04/16
 */
@Deprecated
public class QueryActionExcuter  {


    public static SimpleSearchResponse executeSearchAction(DefaultQueryAction searchQueryAction) throws SqlParseException {
        SqlElasticSearchRequestBuilder builder = searchQueryAction.explain();
        SearchResponse response = (SearchResponse)builder.get();
        SearchHits hits = response.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(hits);
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(response.getTook().getMillis());

        // 滚动查询时才有scrollId
        if (null != response.getScrollId()) {
            simpleSearchResponse.setScrollId(response.getScrollId());
        }
        return simpleSearchResponse;
    }

    public static SimpleSearchResponse executeJoinSearchAction(Client client, ESJoinQueryAction joinQueryAction) throws IOException, SqlParseException {
        SqlElasticRequestBuilder joinRequestBuilder = joinQueryAction.explain();
        ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, joinRequestBuilder);
        executor.run();
        SearchHits hits = executor.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(hits);
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(0L);
        return simpleSearchResponse;
    }

    public static SimpleSearchResponse executeAggCacheSearchAction(Client client, ESJoinQueryAction joinQueryAction) throws IOException, SqlParseException {
        SqlElasticRequestBuilder joinRequestBuilder = joinQueryAction.explain();
        ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, joinRequestBuilder);
        executor.run();
        SearchHits hits =executor.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(hits);
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(0L);
        return simpleSearchResponse;
    }

    public static SimpleSearchResponse executeAggregationAction(AggregationQueryAction aggregationQueryAction) throws SqlParseException {
        SqlElasticSearchRequestBuilder select = aggregationQueryAction.explain();
        SearchResponse response = (SearchResponse)select.get();

        SearchHits hits = response.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(response.getAggregations());
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(response.getTook().getMillis());
        // List<Field> fields =  aggregationQueryAction.getSelect().getFields();
        // if (fields != null) {
        //     List<String> list = new ArrayList<>(fields.size());
        //     for (Field field : fields) {
        //         list.add(field.getAlias() == null ? field.getName():field.getAlias());
        //     }
        //     simpleSearchResponse.setFields(fields);
        // }
        return simpleSearchResponse;
    }

    public static SimpleSearchResponse executeDeleteAction(DeleteQueryAction deleteQueryAction) throws SqlParseException {
        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(deleteQueryAction.explain().get());
        return simpleSearchResponse;
    }

    public static SimpleSearchResponse executeMultiQueryAction(Client client, MultiQueryAction queryAction) throws SqlParseException, IOException {
        SqlElasticRequestBuilder multiRequestBuilder = queryAction.explain();
        ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder)multiRequestBuilder);
        executor.run();

        SearchHits hits = executor.getHits();

        SimpleSearchResponse simpleSearchResponse = new SimpleSearchResponse();
        simpleSearchResponse.setResult(executor.getHits());
        simpleSearchResponse.setTotal(hits.getTotalHits());
        simpleSearchResponse.setTook(0L);
        return simpleSearchResponse;
    }


    public static SimpleSearchResponse executeAnyAction(Client client, QueryAction queryAction) throws SqlParseException, IOException {
        if (queryAction instanceof DefaultQueryAction) {
            return executeSearchAction((DefaultQueryAction) queryAction);
        } else if (queryAction instanceof AggregationQueryAction) {
            return executeAggregationAction((AggregationQueryAction)queryAction);
        } else if (queryAction instanceof ESJoinQueryAction) {
            return executeJoinSearchAction(client, (ESJoinQueryAction)queryAction);
        } else if (queryAction instanceof MultiQueryAction) {
            return executeMultiQueryAction(client, (MultiQueryAction)queryAction);
        } else {
            return queryAction instanceof DeleteQueryAction ? executeDeleteAction((DeleteQueryAction)queryAction) : null;
        }
    }

    public static SimpleSearchResponse executeScrollAction(Client client, String scrollId){
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

        return simpleSearchResponse;
    }

}
