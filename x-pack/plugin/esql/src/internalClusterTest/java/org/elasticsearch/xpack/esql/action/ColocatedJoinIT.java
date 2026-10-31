/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code FROM events | JOIN sessions ON session_id} between two indices partitioned the same way on the key.
 * <p>
 * Layout: three data nodes, three primaries per index, one shard per node, no replicas. Both indices are cloned from an
 * empty seed index, which forces shard {@code i} of each clone onto the node holding seed shard {@code i}, so the two
 * indices have the same shard-to-node assignment. Every document is indexed with {@code _routing = session_id}, so a key
 * lives in the same shard number on both sides and the join can run per shard pair on the data nodes.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class ColocatedJoinIT extends AbstractEsqlIntegTestCase {

    private static final int SHARDS = 3;

    @Before
    public void createColocatedIndices() {
        assumeTrue("co-located join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        assertAcked(
            prepareCreate("colocate_seed").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", SHARDS)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.total_shards_per_node", 1)
                    .put("index.routing.rebalance.enable", "none")
                    .put("index.blocks.write", true)
            ).setMapping("session_id", "type=keyword")
        );
        ensureGreen("colocate_seed");
        clone("colocate_seed", "events");
        clone("colocate_seed", "sessions");
        ensureGreen("events", "sessions");
        updateIndexSettings(Settings.builder().putNull("index.blocks.write"), "events", "sessions");
        assertAcked(
            indicesAdmin().preparePutMapping("events")
                .setSource(
                    "@timestamp",
                    "type=date",
                    "seq",
                    "type=long",
                    "event_type",
                    "type=keyword",
                    "page",
                    "type=keyword",
                    "duration_ms",
                    "type=long"
                )
        );
        assertAcked(
            indicesAdmin().preparePutMapping("sessions")
                .setSource(
                    "started_at",
                    "type=date",
                    "user_id",
                    "type=keyword",
                    "device",
                    "type=keyword",
                    "country",
                    "type=keyword",
                    "referrer",
                    "type=keyword",
                    "plan",
                    "type=keyword"
                )
        );

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        event(bulk, "s-100", 1, "view", "/home", 120);
        event(bulk, "s-100", 2, "click", "/pricing", 30);
        event(bulk, "s-100", 3, "purchase", "/checkout", 900);
        event(bulk, "s-200", 1, "view", "/home", 80);
        event(bulk, "s-200", 2, "click", "/docs", 45);
        event(bulk, "s-300", 1, "view", "/home", 60);
        event(bulk, "s-300", 2, "purchase", "/checkout", 700);
        event(bulk, "s-400", 1, "view", "/home", 50);
        event(bulk, "s-400", 2, "search", "/search", 210);
        event(bulk, "s-400", 3, "click", "/docs", 40);
        event(bulk, "s-500", 1, "view", "/home", 95); // no session row
        event(bulk, "s-600", 1, "view", "/pricing", 70);
        event(bulk, "s-600", 2, "click", "/checkout", 35);
        event(bulk, "s-600", 3, "purchase", "/checkout", 1200);
        session(bulk, "s-100", "u-1", "desktop", "DE", "google", "pro");
        session(bulk, "s-200", "u-2", "mobile", "US", "direct", "free");
        session(bulk, "s-300", "u-3", "mobile", "DE", "twitter", "free");
        session(bulk, "s-400", "u-1", "desktop", "DE", "google", "pro");
        session(bulk, "s-600", "u-2", "tablet", "FR", "direct", "pro");
        session(bulk, "s-700", "u-5", "desktop", "US", "google", "free"); // no events
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }

    private static void clone(String source, String target) {
        var request = ResizeIndexTestUtils.resizeRequest(
            ResizeType.CLONE,
            source,
            target,
            Settings.builder().put("index.number_of_replicas", 0)
        );
        assertAcked(client().execute(TransportResizeAction.TYPE, request).actionGet());
    }

    private void event(BulkRequestBuilder bulk, String session, int seq, String type, String page, int duration) {
        bulk.add(
            prepareIndex("events").setRouting(session)
                .setSource(
                    Map.of(
                        "session_id",
                        session,
                        "@timestamp",
                        "2026-10-30T10:00:0" + seq + "Z",
                        "seq",
                        seq,
                        "event_type",
                        type,
                        "page",
                        page,
                        "duration_ms",
                        duration
                    )
                )
        );
    }

    private void session(BulkRequestBuilder bulk, String session, String user, String device, String country, String ref, String plan) {
        bulk.add(
            prepareIndex("sessions").setId(session)
                .setRouting(session)
                .setSource(
                    Map.of(
                        "session_id",
                        session,
                        "started_at",
                        "2026-10-30T10:00:00Z",
                        "user_id",
                        user,
                        "device",
                        device,
                        "country",
                        country,
                        "referrer",
                        ref,
                        "plan",
                        plan
                    )
                )
        );
    }

    public void testJoin() {
        try (var resp = run("""
            FROM events
            | JOIN sessions ON session_id
            | KEEP session_id, seq, event_type, country, plan
            | SORT session_id, seq
            """)) {
            assertColumnNames(resp.columns(), List.of("session_id", "seq", "event_type", "country", "plan"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("s-100", 1L, "view", "DE", "pro"),
                    List.of("s-100", 2L, "click", "DE", "pro"),
                    List.of("s-100", 3L, "purchase", "DE", "pro"),
                    List.of("s-200", 1L, "view", "US", "free"),
                    List.of("s-200", 2L, "click", "US", "free"),
                    List.of("s-300", 1L, "view", "DE", "free"),
                    List.of("s-300", 2L, "purchase", "DE", "free"),
                    List.of("s-400", 1L, "view", "DE", "pro"),
                    List.of("s-400", 2L, "search", "DE", "pro"),
                    List.of("s-400", 3L, "click", "DE", "pro"),
                    List.of("s-600", 1L, "view", "FR", "pro"),
                    List.of("s-600", 2L, "click", "FR", "pro"),
                    List.of("s-600", 3L, "purchase", "FR", "pro")
                )
            );
        }
    }

    public void testJoinOutputColumns() {
        try (var resp = run("FROM events | JOIN sessions ON session_id | LIMIT 1")) {
            // left columns, then right columns minus the join key
            assertColumnNames(
                resp.columns(),
                List.of(
                    "@timestamp",
                    "duration_ms",
                    "event_type",
                    "page",
                    "seq",
                    "session_id",
                    "country",
                    "device",
                    "plan",
                    "referrer",
                    "started_at",
                    "user_id"
                )
            );
        }
    }

    public void testWhereAfterJoin() {
        try (var resp = run("""
            FROM events
            | JOIN sessions ON session_id
            | WHERE country == "DE" AND seq > 1
            | KEEP session_id, seq, country
            | SORT session_id, seq
            """)) {
            assertValues(
                resp.values(),
                List.of(
                    List.of("s-100", 2L, "DE"),
                    List.of("s-100", 3L, "DE"),
                    List.of("s-300", 2L, "DE"),
                    List.of("s-400", 2L, "DE"),
                    List.of("s-400", 3L, "DE")
                )
            );
        }
    }

    public void testWhereBeforeJoin() {
        try (var resp = run("""
            FROM events
            | WHERE event_type == "purchase"
            | JOIN sessions ON session_id
            | KEEP session_id, country, plan
            | SORT session_id
            """)) {
            assertValues(
                resp.values(),
                List.of(List.of("s-100", "DE", "pro"), List.of("s-300", "DE", "free"), List.of("s-600", "FR", "pro"))
            );
        }
    }

    public void testStatsAfterJoin() {
        try (var resp = run("""
            FROM events
            | JOIN sessions ON session_id
            | STATS n = COUNT(*), spent = SUM(duration_ms) BY country, plan
            | SORT country, plan
            """)) {
            assertColumnNames(resp.columns(), List.of("n", "spent", "country", "plan"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(2L, 760L, "DE", "free"),
                    List.of(6L, 1350L, "DE", "pro"),
                    List.of(3L, 1305L, "FR", "pro"),
                    List.of(2L, 125L, "US", "free")
                )
            );
        }
    }

    /**
     * The join runs on every data node as a local lookup against the partner shard: each data driver carries the
     * co-located lookup operator, and only aggregates leave the node.
     */
    public void testRunsOnEachDataNode() {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM events | JOIN sessions ON session_id | STATS n = COUNT(*) BY country");
        request.profile(true);
        try (var resp = run(request)) {
            List<DriverProfile> dataDrivers = resp.profile().drivers().stream().filter(d -> d.description().equals("data")).toList();
            Set<String> nodes = dataDrivers.stream().map(DriverProfile::nodeName).collect(Collectors.toSet());
            assertThat("one data driver per node, one shard pair each", nodes, hasSize(SHARDS));
            for (DriverProfile driver : dataDrivers) {
                List<String> operators = driver.operators().stream().map(OperatorStatus::operator).toList();
                String lookup = operators.stream().filter(o -> o.startsWith("LookupOperator")).findFirst().orElse(null);
                assertNotNull("no lookup operator in " + operators, lookup);
                assertThat(lookup, containsString("index=sessions"));
                assertThat(lookup, containsString("colocated=true"));
                assertThat(operators.get(operators.size() - 1), containsString("ExchangeSinkOperator"));
            }
        }
    }

    public void testRejectsDifferentShardCount() {
        assertAcked(
            prepareCreate("sessions_one").setSettings(
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            ).setMapping("session_id", "type=keyword", "plan", "type=keyword")
        );
        var e = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions_one ON session_id").close());
        assertThat(
            e.getMessage(),
            containsString("JOIN requires the same number of shards on both sides, found [3] for [events] and [1] for [sessions_one]")
        );
    }

    public void testRejectsUnknownKey() {
        var left = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions ON device").close());
        assertThat(left.getMessage(), containsString("Unknown column [device] in left side of join"));
        var right = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions ON seq").close());
        assertThat(right.getMessage(), containsString("Unknown column [seq] in right side of join"));
    }

    public void testRejectsPatterns() {
        var wildcard = expectThrows(ParsingException.class, () -> run("FROM events | JOIN sess* ON session_id").close());
        assertThat(wildcard.getMessage(), containsString("* is not allowed in JOIN"));
        var twoKeys = expectThrows(ParsingException.class, () -> run("FROM events | JOIN sessions ON session_id, seq").close());
        assertThat(twoKeys.getMessage(), containsString("supports exactly one field"));
    }
}
