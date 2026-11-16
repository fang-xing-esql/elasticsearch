/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetReaderStatus;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Request-filter opt-out for a dataset in the main query, a {@code FROM} subquery, or an {@code IN} subquery, and for a
 * view whose body reads a dataset.
 */
public class FromDatasetSubqueryRequestFilterIT extends AbstractExternalDataSourceIT {

    private static final QueryBuilder ID_LTE_3 = QueryBuilders.rangeQuery("id").lte(3);
    private static final int ROWS = 500;
    private static final int TS_STEP = 10;

    private Path employees;
    private Path extra;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Before
    public void writeFixtures() throws IOException {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
        employees = csv("id:integer,color:keyword", "1,red", "2,blue", "3,red", "4,blue", "5,red", "6,blue");
        extra = csv("id:integer,color:keyword", "7,red", "8,blue");
    }

    public void testMainQueryOptOutReturnsRowsTheFilterWouldRemove() {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        assertThat(ids("FROM employees | SORT id"), equalTo(List.of(1, 2, 3)));
        assertThat(ids("FROM employees WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("FROM employees WITH {\"apply_request_filter\": false} | WHERE id > 3 | SORT id"), equalTo(List.of(4, 5, 6)));
    }

    public void testOneCommandOptsOutEveryDatasetItNames() {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        register("extra", extra);
        assertThat(ids("FROM employees, extra WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(1, 2, 3, 4, 5, 6, 7, 8)));
    }

    public void testNestedDatasetCommand() {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        register("extra", extra);
        assertThat(ids("FROM employees, (FROM extra WITH {\"apply_request_filter\": false}) | SORT id"), equalTo(List.of(1, 2, 3, 7, 8)));
    }

    public void testInSubquery() {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        createTestIndex();
        String optedOut = """
            FROM test
            | WHERE id IN (
                FROM employees WITH {"apply_request_filter": false}
                | WHERE id > 3
                | EVAL id = id - 3
                | KEEP id
              )
            | SORT id
            | KEEP id""";
        String filtered = """
            FROM test
            | WHERE id IN (
                FROM employees
                | WHERE id > 3
                | EVAL id = id - 3
                | KEEP id
              )
            | SORT id
            | KEEP id""";
        assertThat(ids(optedOut), equalTo(List.of(1, 2, 3)));
        assertThat(ids(filtered), empty());
    }

    public void testViewOverDatasetFromFromAndIn() {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        createTestIndex();
        putView("rows", "FROM employees | KEEP id");
        assertThat(
            ids("FROM test, (FROM rows WITH {\"apply_request_filter\": false}) | KEEP id | SORT id"),
            equalTo(List.of(1, 1, 2, 2, 3, 3, 4, 5, 6))
        );
        assertThat(ids("FROM test, (FROM rows) | KEEP id | SORT id"), equalTo(List.of(1, 1, 2, 2, 3, 3)));
        assertThat(ids("""
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM rows WITH {"apply_request_filter": false} | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        assertThat(ids("""
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM rows | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3)));
        putView("offset", "FROM employees | WHERE id > 3 | EVAL id = id - 3 | KEEP id");
        assertThat(ids("FROM offset WITH {\"apply_request_filter\": false} | SORT id"), equalTo(List.of(1, 2, 3)));
    }

    public void testViewOverDatasetNamedInsideLet() {
        assumeTrue("requires named subqueries", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        createTestIndex();
        putView("offset", "FROM employees | WHERE id > 3 | EVAL id = id - 3 | KEEP id");
        assertThat(ids("""
            LET sub = (FROM offset | KEEP id);
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id)
            | SORT id
            | KEEP id
            """), equalTo(List.of(1, 2, 3)));
        assertThat(ids("""
            LET sub = (FROM offset | KEEP id);
            FROM test, sub WITH {"apply_request_filter": false} | KEEP id | SORT id
            """), equalTo(List.of(1, 1, 2, 2, 3, 3, 4, 5, 6)));
        assertThat(ids("""
            LET sub = (FROM offset | KEEP id);
            FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id) | SORT id | KEEP id
            """), equalTo(List.of(1, 2, 3)));
    }

    public void testUntranslatableConjunctWarnsOnlyForACommandThatAppliesTheFilter() throws IOException {
        registerDataSource("local_ds", Map.of());
        register("employees", employees);
        Response kept = queryRest("FROM employees | KEEP id | SORT id | LIMIT 10", """
            { "wildcard": { "color": { "value": "r*" } } }""");
        assertThat(EntityUtils.toString(kept.getEntity()), containsString("\"values\""));
        assertThat(kept.getWarnings().toString(), containsString("not fully applied"));
        Response optedOut = queryRest("FROM employees WITH {\"apply_request_filter\": false} | KEEP id | SORT id | LIMIT 10", """
            { "wildcard": { "color": { "value": "r*" } } }""");
        assertThat(EntityUtils.toString(optedOut.getEntity()), containsString("1"));
        assertThat(optedOut.getWarnings().toString(), not(containsString("not fully applied")));
    }

    public void testOptedOutDatasetDoesNotRowGroupPruneOnTheRequestFilter() throws IOException {
        String dataset = registerSortedDataset("rg_opt_out");
        String read = "FROM " + dataset + " | KEEP id | SORT id";
        int unfiltered = splits(read, null, null);
        int requestFilter = splits(
            "FROM " + dataset + " WITH {\"apply_request_filter\": false} | KEEP id | SORT id",
            QueryBuilders.rangeQuery("ts").gte(0).lte(100),
            null
        );
        int where = splits(
            "FROM " + dataset + " | WHERE mv_in_range(ts, 0::long, 100::long) | KEEP id | SORT id",
            null,
            idsWithTsBetween(0, 100)
        );
        assertThat(unfiltered, greaterThan(1));
        assertThat(requestFilter, equalTo(unfiltered));
        assertThat(where, lessThan(unfiltered));
    }

    private int splits(String query, QueryBuilder filter, List<Long> expectedIds) {
        var request = syncEsqlQueryRequest(query).profile(true);
        if (filter != null) {
            request.filter(filter);
        }
        try (var response = run(request, TIMEOUT)) {
            assertThat(response.getExecutionInfo().queryProfile().splitsScanned(), greaterThan(0));
            if (expectedIds != null) {
                List<Long> ids = getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
                assertThat(ids, equalTo(expectedIds));
            }
            int readers = 0;
            for (var driver : response.profile().drivers()) {
                for (var op : driver.operators()) {
                    if (op.status() instanceof AsyncExternalSourceOperator.Status status
                        && status.formatReader() instanceof ParquetReaderStatus) {
                        readers++;
                    }
                }
            }
            assertThat(readers, greaterThan(0));
            return response.getExecutionInfo().queryProfile().splitsScanned();
        }
    }

    private String registerSortedDataset(String name) throws IOException {
        Path file = createTempDir().resolve(name + ".parquet");
        writeParquet(file, "message test { required int64 id; required int64 ts; }", ROWS, 256, (group, i) -> {
            group.add("id", (long) i);
            group.add("ts", (long) i * TS_STEP);
        });
        return registerDataset(name, StoragePath.fileUri(file), Map.of());
    }

    private List<Long> idsWithTsBetween(long lower, long upper) {
        List<Long> ids = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            long ts = id * TS_STEP;
            if (ts >= lower && ts <= upper) {
                ids.add(id);
            }
        }
        return ids;
    }

    private void register(String name, Path file) {
        registerDataset(name, "local_ds", file.toUri().toString(), Map.of("format", "csv"));
    }

    private Path csv(String... lines) throws IOException {
        Path file = createTempFile("apply-request-filter-", ".csv");
        Files.writeString(file, String.join("\n", lines) + "\n");
        return file;
    }

    private void createTestIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer")
        );
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("id", 1))
            .add(new IndexRequest("test").id("2").source("id", 2))
            .add(new IndexRequest("test").id("3").source("id", 3))
            .add(new IndexRequest("test").id("4").source("id", 4))
            .add(new IndexRequest("test").id("5").source("id", 5))
            .add(new IndexRequest("test").id("6").source("id", 6))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private static void putView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    private List<Integer> ids(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(ID_LTE_3), TIMEOUT)) {
            List<Integer> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                values.add(((Number) row.get(0)).intValue());
            }
            return values;
        }
    }

    private Response queryRest(String query, String filter) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(String.format(Locale.ROOT, """
            { "query": "%s", "filter": %s }
            """, query.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " "), filter));
        return getRestClient().performRequest(request);
    }
}
