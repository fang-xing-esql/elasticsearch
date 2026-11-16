/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Request-filter opt-out on index {@code FROM} and {@code IN} subqueries. No views and no datasets.
 */
public class SubqueryRequestFilterIT extends AbstractEsqlIntegTestCase {

    /** Subquery command {@code S} from the design: ids 4, 5, 6 shifted down to 1, 2, 3 when the filter is skipped. */
    private static final String S = """
        FROM test WITH {"apply_request_filter": false}
        | WHERE id > 3
        | EVAL id = id - 3
        | KEEP id""";

    private static final String S_FILTERED = """
        FROM test
        | WHERE id > 3
        | EVAL id = id - 3
        | KEEP id""";

    @Before
    public void setupIndex() {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "color", "type=keyword")
        );
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("id", 1, "color", "red"))
            .add(new IndexRequest("test").id("2").source("id", 2, "color", "blue"))
            .add(new IndexRequest("test").id("3").source("id", 3, "color", "red"))
            .add(new IndexRequest("test").id("4").source("id", 4, "color", "blue"))
            .add(new IndexRequest("test").id("5").source("id", 5, "color", "red"))
            .add(new IndexRequest("test").id("6").source("id", 6, "color", "blue"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testWhereInOnTheFilterPathAndTheHashJoinPath() {
        String query = "FROM test | WHERE id IN (" + S + ") | SORT id | KEEP id";
        assertThat(ids(query, null), equalTo(List.of(1, 2, 3)));
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        assertThat(ids(query, hashJoin()), equalTo(List.of(1, 2, 3)));
        assertThat(ids("FROM test | WHERE id IN (" + S_FILTERED + ") | SORT id | KEEP id", null), empty());
    }

    public void testWhereNotIn() {
        assertThat(ids("FROM test | WHERE id NOT IN (" + S + ") | SORT id | KEEP id", null), empty());
        assertThat(ids("FROM test | WHERE id NOT IN (" + S_FILTERED + ") | SORT id | KEEP id", null), equalTo(List.of(1, 2, 3)));
    }

    public void testEvalIn() {
        assertThat(flags("FROM test | EVAL flag = id IN (" + S + ") | SORT id | KEEP id, flag"), equalTo(List.of(true, true, true)));
        assertThat(
            flags("FROM test | EVAL flag = id IN (" + S_FILTERED + ") | SORT id | KEEP id, flag"),
            equalTo(List.of(false, false, false))
        );
    }

    public void testStatsWhereIn() {
        assertThat(count("FROM test | STATS c = COUNT(*) WHERE id IN (" + S + ")"), equalTo(3L));
        assertThat(count("FROM test | STATS c = COUNT(*) WHERE id IN (" + S_FILTERED + ")"), equalTo(0L));
    }

    public void testInlineStatsWhereIn() {
        assertThat(
            flags("FROM test | INLINE STATS c = COUNT(*) WHERE id IN (" + S + ") | SORT id | KEEP id, c"),
            equalTo(List.of(3L, 3L, 3L))
        );
        assertThat(
            flags("FROM test | INLINE STATS c = COUNT(*) WHERE id IN (" + S_FILTERED + ") | SORT id | KEEP id, c"),
            equalTo(List.of(0L, 0L, 0L))
        );
    }

    public void testParentWithDoesNotLeakIntoIn() {
        String source = "FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id";
        assertThat(
            ids("FROM test WITH {\"apply_request_filter\": false} | WHERE id IN (" + source + ") | SORT id | KEEP id", null),
            empty()
        );
        assertThat(
            ids("FROM test WITH {\"apply_request_filter\": false} | WHERE id NOT IN (" + source + ") | SORT id | KEEP id", null),
            equalTo(List.of(1, 2, 3, 4, 5, 6))
        );
    }

    public void testNestedAndSiblingIn() {
        assertThat(
            ids("FROM test | WHERE id IN (FROM test | WHERE id IN (" + S + ") | KEEP id) | SORT id | KEEP id", null),
            equalTo(List.of(1, 2, 3))
        );
        assertThat(ids("""
            FROM test
            | WHERE id IN (
                FROM test WITH {"apply_request_filter": false}
                | WHERE id IN (FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id)
                | KEEP id
              )
            | KEEP id
            """, null), empty());
        assertThat(ids("""
            FROM test
            | WHERE id IN (
                FROM test WITH {"apply_request_filter": false}
                | WHERE id > 3
                | EVAL id = id - 3
                | KEEP id
              )
              AND id NOT IN (FROM test | WHERE id == 2 | KEEP id)
            | SORT id
            | KEEP id
            """, null), equalTo(List.of(1, 3)));
    }

    public void testFromSubqueries() {
        assertThat(ids("""
            FROM test, (FROM test WITH {"apply_request_filter": false} | WHERE id > 3)
            | KEEP id
            | SORT id
            """, null), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("""
            FROM test, (FROM test | WHERE id > 3) WITH {"apply_request_filter": false}
            | KEEP id
            | SORT id
            """, null), equalTo(List.of(1, 2, 3, 4, 4, 5, 5, 6, 6)));
        assertThat(ids("""
            FROM test, (
              FROM test
              | WHERE id IN (FROM test | WHERE id > 3 | KEEP id)
              | KEEP id
            ) WITH {"apply_request_filter": false}
            | KEEP id
            | SORT id
            """, null), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(
            ids("FROM test, test WITH {\"apply_request_filter\": false} | WHERE id > 3 | KEEP id | SORT id", null),
            equalTo(List.of(4, 5, 6))
        );
    }

    public void testLetBindings() {
        assumeTrue("requires named subqueries", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        assertThat(ids("""
            LET s = (FROM test WITH {"apply_request_filter": false} | WHERE id > 3 | EVAL id = id - 3 | KEEP id);
            FROM test | WHERE id IN (FROM s | KEEP id) | SORT id | KEEP id
            """, null), equalTo(List.of(1, 2, 3)));
        assertThat(ids("""
            LET sub = (FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id);
            FROM test | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id) | SORT id | KEEP id
            """, null), equalTo(List.of(1, 2, 3)));
        assertThat(ids("""
            LET sub = (FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id);
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM sub | KEEP id)
            | KEEP id
            """, null), empty());
    }

    public void testClauseWithoutARequestFilterIsRejected() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> run("FROM test WITH {\"apply_request_filter\": false} | KEEP id")
        );
        assertThat(e.getMessage(), containsString("cannot be set when the request has no filter"));
    }

    private List<Integer> ids(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).filter(new RangeQueryBuilder("id").lte(3));
        if (pragmas != null) {
            request.pragmas(pragmas);
        }
        try (EsqlQueryResponse response = run(request)) {
            List<Integer> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add((Integer) row.get(0));
            }
            return values;
        }
    }

    private List<Object> flags(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(new RangeQueryBuilder("id").lte(3)))) {
            List<Object> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add(row.get(1));
            }
            return values;
        }
    }

    private long count(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(new RangeQueryBuilder("id").lte(3)))) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    private static QueryPragmas hashJoin() {
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", 0).build());
    }
}
