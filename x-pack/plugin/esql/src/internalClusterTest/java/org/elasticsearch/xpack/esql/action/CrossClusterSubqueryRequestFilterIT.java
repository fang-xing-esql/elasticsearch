/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * A remote command can opt out of the request filter. One main-query case and one {@code IN} case.
 */
public class CrossClusterSubqueryRequestFilterIT extends AbstractCrossClusterTestCase {

    private RangeQueryBuilder recent;

    @Before
    public void setup() throws IOException {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
        setupClusters(2);
        long now = System.currentTimeMillis();
        recent = QueryBuilders.rangeQuery("@timestamp").gte(now - TimeUnit.HOURS.toMillis(1));
        String inRange = Instant.ofEpochMilli(now).toString();
        String outOfRange = Instant.ofEpochMilli(now - TimeUnit.DAYS.toMillis(2)).toString();

        create(REMOTE_CLUSTER_1, "has_ts", true);
        create(REMOTE_CLUSTER_1, "no_ts", false);
        index(REMOTE_CLUSTER_1, "has_ts", "1", inRange, 1);
        index(REMOTE_CLUSTER_1, "has_ts", "2", outOfRange, 2);
        index(REMOTE_CLUSTER_1, "no_ts", "3", null, 3);

        create(LOCAL_CLUSTER, "test", false);
        create(REMOTE_CLUSTER_1, "test", false);
        for (int id = 1; id <= 6; id++) {
            String color = id % 2 == 0 ? "blue" : "red";
            index(LOCAL_CLUSTER, "test", Integer.toString(id), null, id, color);
            index(REMOTE_CLUSTER_1, "test", Integer.toString(id), null, id, color);
        }
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh("has_ts", "no_ts", "test").get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh("test").get();
    }

    public void testRemoteCommandOptsOutOfTheIndexTheFilterWouldDrop() {
        assertThat(ids("FROM cluster-a:has_ts, cluster-a:no_ts | KEEP id | SORT id"), equalTo(List.of(1)));
        assertThat(
            ids("FROM cluster-a:has_ts, cluster-a:no_ts WITH {\"apply_request_filter\": false} | KEEP id | SORT id"),
            equalTo(List.of(1, 2, 3))
        );
    }

    public void testRemoteInSubquery() {
        String optedOut = """
            FROM test
            | WHERE id IN (
                FROM cluster-a:test WITH {"apply_request_filter": false}
                | WHERE id > 3
                | EVAL id = id - 3
                | KEEP id
              )
            | SORT id
            | KEEP id""";
        String filtered = """
            FROM test
            | WHERE id IN (
                FROM cluster-a:test
                | WHERE id > 3
                | EVAL id = id - 3
                | KEEP id
              )
            | SORT id
            | KEEP id""";
        assertThat(ids(optedOut, new RangeQueryBuilder("id").lte(3)), equalTo(List.of(1, 2, 3)));
        assertThat(ids(filtered, new RangeQueryBuilder("id").lte(3)), empty());
    }

    private void create(String cluster, String name, boolean timestamp) {
        var create = client(cluster).admin()
            .indices()
            .prepareCreate(name)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        if (timestamp) {
            create.setMapping("id", "type=integer", "@timestamp", "type=date");
        } else {
            create.setMapping("id", "type=integer", "color", "type=keyword");
        }
        assertAcked(create);
    }

    private void index(String cluster, String name, String docId, String timestamp, int id) {
        index(cluster, name, docId, timestamp, id, null);
    }

    private void index(String cluster, String name, String docId, String timestamp, int id, String color) {
        IndexRequest request = new IndexRequest(name).id(docId);
        if (timestamp != null) {
            request.source("id", id, "@timestamp", timestamp);
        } else if (color != null) {
            request.source("id", id, "color", color);
        } else {
            request.source("id", id);
        }
        BulkRequestBuilder bulk = client(cluster).prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        bulk.add(request).get();
    }

    private List<Integer> ids(String query) {
        return ids(query, recent);
    }

    private List<Integer> ids(String query, org.elasticsearch.index.query.QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).filter(filter);
        try (EsqlQueryResponse response = runQuery(request)) {
            List<Integer> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add(((Number) row.get(0)).intValue());
            }
            return values;
        }
    }
}
