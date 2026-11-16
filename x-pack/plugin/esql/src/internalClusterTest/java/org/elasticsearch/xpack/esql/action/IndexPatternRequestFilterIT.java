/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Field-caps {@code index_filter} when a command opts out of the request filter. One pattern keeps the filter when any
 * reference still applies it.
 */
public class IndexPatternRequestFilterIT extends AbstractEsqlIntegTestCase {

    private RangeQueryBuilder recent;

    @Before
    public void setupIndices() {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
        long now = System.currentTimeMillis();
        recent = QueryBuilders.rangeQuery("@timestamp").gte(now - TimeUnit.HOURS.toMillis(1));
        String inRange = Instant.ofEpochMilli(now).toString();
        String outOfRange = Instant.ofEpochMilli(now - TimeUnit.DAYS.toMillis(2)).toString();

        create("has_ts", true);
        create("no_ts", false);
        create("logs-a", true);
        create("logs-b", false);
        create("bare", false);

        index("has_ts", "1", inRange, 1);
        index("has_ts", "2", outOfRange, 2);
        index("no_ts", "3", null, 3);
        index("logs-a", "1", inRange, 1);
        index("logs-a", "2", outOfRange, 2);
        index("logs-b", "9", null, 9);
        index("bare", "7", null, 7);
        refresh("has_ts", "no_ts", "logs-a", "logs-b", "bare");
    }

    public void testOneCommandOptsOutOfIndexFilter() {
        assertThat(ids("FROM has_ts, no_ts | KEEP id | SORT id"), equalTo(List.of(1)));
        assertThat(ids("FROM has_ts, no_ts WITH {\"apply_request_filter\": false} | KEEP id | SORT id"), equalTo(List.of(1, 2, 3)));
    }

    public void testNestedCommandKeepsTheOuterIndexPruned() {
        assertThat(ids("FROM has_ts, (FROM no_ts WITH {\"apply_request_filter\": false}) | KEEP id | SORT id"), equalTo(List.of(1, 3)));
    }

    public void testSharedPatternKeepsIndexFilterWhenAnyReferenceAppliesIt() {
        assertThat(ids("FROM logs-* WITH {\"apply_request_filter\": false} | KEEP id | SORT id"), equalTo(List.of(1, 2, 9)));
        List<Integer> mixed = ids("""
            FROM logs-*, (FROM logs-* WITH {"apply_request_filter": true} | KEEP id) WITH {"apply_request_filter": false}
            | KEEP id
            | SORT id
            """);
        assertThat(mixed, equalTo(List.of(1, 1, 2)));
        assertThat(mixed, not(equalTo(List.of(1, 1, 2, 9))));
    }

    public void testAnalyzeWithRetryStillRunsWhenTheCommandDidNotOptOut() {
        assertThat(ids("FROM bare | KEEP id | SORT id"), equalTo(List.of()));
        assertThat(ids("FROM bare WITH {\"apply_request_filter\": false} | KEEP id | SORT id"), equalTo(List.of(7)));
    }

    private void create(String name, boolean timestamp) {
        var create = client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        if (timestamp) {
            create.setMapping("id", "type=integer", "@timestamp", "type=date");
        } else {
            create.setMapping("id", "type=integer");
        }
        assertAcked(create);
    }

    private void index(String name, String docId, String timestamp, int id) {
        IndexRequest request = new IndexRequest(name).id(docId);
        if (timestamp == null) {
            request.source("id", id);
        } else {
            request.source("id", id, "@timestamp", timestamp);
        }
        client().prepareBulk().add(request).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
    }

    private List<Integer> ids(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(recent))) {
            List<Integer> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add(((Number) row.get(0)).intValue());
            }
            return values;
        }
    }
}
