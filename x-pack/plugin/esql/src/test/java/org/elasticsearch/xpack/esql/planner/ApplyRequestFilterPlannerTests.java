/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Lucene stamp and {@code can_match} for a source command that opted out of the request filter.
 */
public class ApplyRequestFilterPlannerTests extends ESTestCase {

    private static final QueryBuilder FILTER = QueryBuilders.termQuery("region", "eu");
    private static final Configuration CONFIG = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(Instant.EPOCH).build();

    public void testOptedOutRelationIsNotStamped() {
        FragmentExec optedOut = stamp(EsqlTestUtils.relation().withApplyRequestFilter(false), TransportVersion.current());
        FragmentExec optedIn = stamp(EsqlTestUtils.relation(), TransportVersion.current());
        assertThat(optedOut.esFilter(), nullValue());
        assertThat(optedIn.esFilter(), equalTo(FILTER));
    }

    public void testOptedOutViewBranchSkipsTheOldClusterFallback() {
        FragmentExec optedOut = viewFragment(false, TransportVersion.minimumCompatible());
        assertThat(optedOut.esFilter(), nullValue());
        assertTrue(optedOut.isFromViewBranch());

        FragmentExec optedIn = viewFragment(true, TransportVersion.minimumCompatible());
        assertThat("a view that did not opt out is still stamped when the output rewrite cannot run", optedIn.esFilter(), equalTo(FILTER));
    }

    public void testOptedInViewOnACurrentClusterIsFilteredAtItsOutput() {
        FragmentExec fragment = viewFragment(true, TransportVersion.current());
        assertThat(fragment.esFilter(), nullValue());
        assertTrue(fragment.isFromViewBranch());

        LogicalPlan view = EsqlTestUtils.relation();
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("computed", view);
        BranchPlan kept = new BranchPlan(Source.EMPTY, branches, Map.of("computed", BranchIdentity.view("computed", true)), List.of());
        LogicalPlan rewritten = ViewRequestFilterRewriter.rewrite(kept, FILTER, CONFIG, TransportVersion.current());
        assertThat(((BranchPlan) rewritten).namedSubqueries().get("computed"), instanceOf(Filter.class));

        BranchPlan optedOut = new BranchPlan(Source.EMPTY, branches, Map.of("computed", BranchIdentity.view("computed", false)), List.of());
        LogicalPlan skipped = ViewRequestFilterRewriter.rewrite(optedOut, FILTER, CONFIG, TransportVersion.current());
        assertThat(((BranchPlan) skipped).namedSubqueries().get("computed"), equalTo(view));
    }

    public void testCanMatchDropsTheRequestFilterWhenAnyFragmentOptedOut() {
        FragmentExec optedIn = stamp(EsqlTestUtils.relation(), TransportVersion.current());
        FragmentExec another = stamp(EsqlTestUtils.relation(), TransportVersion.current());
        FragmentExec optedOut = stamp(EsqlTestUtils.relation().withApplyRequestFilter(false), TransportVersion.current());

        assertThat(String.valueOf(canMatch(optedIn)), containsString("region"));
        assertThat(String.valueOf(canMatch(optedIn, another)), containsString("region"));
        assertThat(String.valueOf(canMatch(optedIn, optedOut)), not(containsString("region")));

        var timestamp = new org.elasticsearch.xpack.esql.core.expression.FieldAttribute(
            Source.EMPTY,
            "@timestamp",
            new org.elasticsearch.xpack.esql.core.type.EsField(
                "@timestamp",
                DataType.DATETIME,
                Map.of(),
                true,
                org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE
            )
        );
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "logs",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.of((org.elasticsearch.xpack.esql.core.expression.Attribute) timestamp),
            SliceSelection.UNSPECIFIED,
            false
        );
        Filter where = new Filter(
            Source.EMPTY,
            relation,
            new GreaterThanOrEqual(
                Source.EMPTY,
                timestamp,
                Literal.dateTime(Source.EMPTY, Instant.parse("2020-01-01T00:00:00Z")),
                ZoneOffset.UTC
            )
        );
        String mixed = String.valueOf(canMatch(optedIn, new FragmentExec(where)));
        assertThat(mixed, not(containsString("region")));
        assertThat(mixed, containsString("@timestamp"));
    }

    private static FragmentExec stamp(EsRelation relation, TransportVersion version) {
        return (FragmentExec) PlannerUtils.integrateEsFilterIntoFragment(new FragmentExec(relation), FILTER, version);
    }

    private static FragmentExec viewFragment(boolean applyRequestFilter, TransportVersion version) {
        EsRelation leaf = EsqlTestUtils.relation();
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("computed", leaf);
        BranchPlan plan = new BranchPlan(
            Source.EMPTY,
            branches,
            Map.of("computed", BranchIdentity.view("computed", applyRequestFilter)),
            List.of()
        );
        PhysicalPlan physical = new Mapper().map(new Versioned<>(plan, TransportVersion.current()));
        FragmentExec fragment = physical.collect(FragmentExec.class).getFirst();
        return (FragmentExec) PlannerUtils.integrateEsFilterIntoFragment(fragment, FILTER, version);
    }

    private static QueryBuilder canMatch(PhysicalPlan... fragments) {
        MergeExec plan = new MergeExec(Source.EMPTY, Arrays.asList(fragments), List.of(), MergeExec.Kind.UNION);
        return PlannerUtils.detectFilter(new EsqlFlags(true), CONFIG, TransportVersion.current(), plan, name -> true);
    }
}
