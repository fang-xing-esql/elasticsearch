/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern.Kind;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class SourceCommandNamedParametersTests extends ESTestCase {

    public void testSharedIndexPatternKeepsFieldCapsFilterWhenAnyReferenceAppliesIt() {
        LogicalPlan mixed = union(relation("logs-*", true), relation("logs-*", false));
        assertThat(SourceCommandNamedParameters.optedOutIndexPatterns(mixed), empty());

        LogicalPlan optedOut = union(relation("logs-*", false), relation("logs-*", false));
        assertThat(patterns(SourceCommandNamedParameters.optedOutIndexPatterns(optedOut)), contains("logs-*"));

        LogicalPlan separate = union(relation("logs-*", false), relation("metrics-*", true));
        assertThat(patterns(SourceCommandNamedParameters.optedOutIndexPatterns(separate)), contains("logs-*"));
    }

    public void testSharedShadowPatternKeepsFieldCapsFilterWhenAnyShadowAppliesIt() {
        LogicalPlan mixed = union(shadow("logs-*", true), shadow("logs-*", false));
        assertThat(SourceCommandNamedParameters.optedOutLinkedPatterns(mixed), empty());

        LogicalPlan optedOut = union(shadow("logs-*", false), datasetShadow("logs-*", false));
        assertThat(SourceCommandNamedParameters.optedOutLinkedPatterns(optedOut), contains("logs-*"));
    }

    public void testSharedExternalPathKeepsListingBoundsWhenAnyReferenceAppliesTheFilter() {
        String path = "s3://bucket/logs";
        LogicalPlan mixed = union(external(path, true), external(path, false));
        assertThat(SourceCommandNamedParameters.fullyOptedOutExternalPaths(mixed), empty());

        LogicalPlan optedOut = union(external(path, false), external(path, false));
        assertThat(SourceCommandNamedParameters.fullyOptedOutExternalPaths(optedOut), contains(path));

        LogicalPlan separate = union(external(path, false), external("s3://bucket/other", true));
        assertThat(SourceCommandNamedParameters.fullyOptedOutExternalPaths(separate), contains(path));
    }

    private static List<String> patterns(Set<IndexPattern> patterns) {
        return patterns.stream().map(IndexPattern::indexPattern).sorted().toList();
    }

    private static LogicalPlan union(LogicalPlan... children) {
        return new UnionAll(Source.EMPTY, List.of(children), List.of());
    }

    private static UnresolvedRelation relation(String pattern, boolean applyRequestFilter) {
        return new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, pattern),
            false,
            List.of(),
            IndexMode.STANDARD,
            null,
            "FROM",
            parameters(applyRequestFilter)
        );
    }

    private static ViewShadowRelation shadow(String pattern, boolean applyRequestFilter) {
        return new ViewShadowRelation(Source.EMPTY, pattern, Kind.OPTIONAL, pattern, applyRequestFilter);
    }

    private static DatasetShadowRelation datasetShadow(String pattern, boolean applyRequestFilter) {
        return new DatasetShadowRelation(Source.EMPTY, pattern, Kind.OPTIONAL, pattern, applyRequestFilter);
    }

    private static UnresolvedExternalRelation external(String path, boolean applyRequestFilter) {
        return new UnresolvedExternalRelation(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, path),
            Map.of(),
            List.of(),
            null,
            null,
            parameters(applyRequestFilter)
        );
    }

    private static CommandNamedParameters parameters(boolean applyRequestFilter) {
        return applyRequestFilter
            ? CommandNamedParameters.EMPTY
            : CommandNamedParameters.EMPTY.withInherited(SourceCommandNamedParameters.APPLY_REQUEST_FILTER, false);
    }
}
