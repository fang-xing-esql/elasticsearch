/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.GetViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ValidateViewAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Request-filter opt-out for a view over an index, used from the main query, a {@code FROM} subquery, an {@code IN}
 * subquery, or a {@code LET} body. Also the {@code PUT /_query/view} rejection.
 */
public class SubqueryViewRequestFilterIT extends AbstractEsqlIntegTestCase {

    private static final RangeQueryBuilder ID_LTE_3 = new RangeQueryBuilder("id").lte(3);

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
        putView("plain", "FROM test | KEEP id");
        putView("offset", "FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id");
        putView("shifted", "FROM test | EVAL id = id + 10 | KEEP id");
    }

    public void testMainQueryOptOut() {
        assertThat(ids("FROM plain WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("FROM plain | SORT id"), equalTo(List.of(1, 2, 3)));
        assertThat(ids("FROM offset WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(1, 2, 3)));
    }

    public void testFromSubquery() {
        assertThat(
            ids("FROM plain, test WITH {\"apply_request_filter\": false} | KEEP id | SORT id"),
            equalTo(List.of(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6))
        );
        assertThat(
            ids("FROM test, (FROM plain WITH {\"apply_request_filter\": false}) | KEEP id | SORT id"),
            equalTo(List.of(1, 1, 2, 2, 3, 3, 4, 5, 6))
        );
    }

    public void testInSubquery() {
        // The outer command opts out, so it can see ids 4, 5, and 6. The view output is filtered to 1, 2, 3 unless
        // the view command itself opts out.
        assertThat(ids("""
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM plain WITH {"apply_request_filter": false} | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("""
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM plain | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3)));
    }

    public void testComputedViewOptOutReturnsRowsTheFilterWouldRemove() {
        assertThat(ids("FROM shifted WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(11, 12, 13, 14, 15, 16)));
        assertThat(ids("FROM shifted | SORT id"), empty());
    }

    public void testViewNamedInsideLet() {
        assumeTrue("requires named subqueries", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        assertThat(ids("""
            LET sub = (FROM plain | KEEP id);
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("""
            LET sub = (FROM plain | KEEP id);
            FROM test, sub WITH {"apply_request_filter": false} | KEEP id | SORT id
            """), equalTo(List.of(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6)));
        assertThat(ids("""
            LET sub = (FROM plain | KEEP id);
            FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id) | SORT id | KEEP id
            """), equalTo(List.of(1, 2, 3)));
    }

    public void testPutViewRejectsTheClause() {
        for (String value : List.of("false", "true")) {
            for (String query : List.of(
                "FROM test WITH {\"apply_request_filter\": " + value + "}",
                "FROM test, (FROM test WITH {\"apply_request_filter\": " + value + "} | KEEP id)",
                "FROM test | WHERE id IN (FROM test WITH {\"apply_request_filter\": " + value + "} | KEEP id)"
            )) {
                String name = "rejected-" + value + "-" + Integer.toHexString(query.hashCode());
                expectThrows(
                    ElasticsearchException.class,
                    containsString("cannot be set on a source inside a view definition"),
                    () -> putView(name, query)
                );
                assertViewMissing(name);
                expectThrows(
                    ElasticsearchException.class,
                    containsString("cannot be set on a source inside a view definition"),
                    () -> client().execute(ValidateViewAction.INSTANCE, new ValidateViewAction.Request(new View(name, query))).actionGet()
                );
                assertViewMissing(name);
            }
        }
    }

    public void testPutViewWithoutTheClauseCreatesTheView() {
        putView("accepted", "FROM test | WHERE id IN (FROM test | KEEP id) | KEEP id");
        try {
            GetViewAction.Request get = new GetViewAction.Request(TEST_REQUEST_TIMEOUT);
            get.indices("accepted");
            GetViewAction.Response response = client().execute(GetViewAction.INSTANCE, get).actionGet(30, TimeUnit.SECONDS);
            assertThat(response.getViews().iterator().next().name(), equalTo("accepted"));
        } finally {
            deleteView("accepted");
        }
    }

    private List<Integer> ids(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(ID_LTE_3))) {
            List<Integer> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add(((Number) row.get(0)).intValue());
            }
            return values;
        }
    }

    private static void putView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    private static void deleteView(String name) {
        assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
            )
        );
    }

    private static void assertViewMissing(String name) {
        GetViewAction.Request get = new GetViewAction.Request(TEST_REQUEST_TIMEOUT);
        get.indices(name);
        expectThrows(
            ResourceNotFoundException.class,
            containsString(name),
            () -> client().execute(GetViewAction.INSTANCE, get).actionGet(30, TimeUnit.SECONDS)
        );
    }
}
