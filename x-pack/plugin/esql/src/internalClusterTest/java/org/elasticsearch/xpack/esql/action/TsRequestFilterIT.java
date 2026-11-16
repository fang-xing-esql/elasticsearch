/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A {@code TS} command can opt out of the request time range.
 */
public class TsRequestFilterIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupMetrics() {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("metrics")
                .setSettings(Settings.builder().put("index.mode", "time_series").putList("index.routing_path", List.of("host")))
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge"
                )
        );
        long now = System.currentTimeMillis();
        String recent = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(now);
        String old = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(now - TimeUnit.HOURS.toMillis(2));
        client().prepareBulk()
            .add(prepareIndex("metrics").setSource("@timestamp", recent, "host", "a", "cpu", 2))
            .add(prepareIndex("metrics").setSource("@timestamp", old, "host", "b", "cpu", 10))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testOptOutReadsSamplesOutsideTheRequestRange() {
        long now = System.currentTimeMillis();
        var filter = QueryBuilders.rangeQuery("@timestamp").gte(now - TimeUnit.MINUTES.toMillis(30));
        assertThat(total("TS metrics | STATS total = SUM(cpu) | KEEP total", filter), equalTo(2L));
        assertThat(total("TS metrics WITH {\"apply_request_filter\": false} | STATS total = SUM(cpu) | KEEP total", filter), equalTo(12L));
    }

    private long total(String query, org.elasticsearch.index.query.QueryBuilder filter) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(filter))) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }
}
