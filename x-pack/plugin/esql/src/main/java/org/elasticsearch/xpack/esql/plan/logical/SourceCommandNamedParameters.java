/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@code WITH} options on a {@code FROM} or {@code TS} command ({@code sourceCommandNamedParameters}).
 * <p>
 * {@link #APPLY_REQUEST_FILTER} is a supported entry.
 */
public final class SourceCommandNamedParameters {

    public static final String APPLY_REQUEST_FILTER = "apply_request_filter";

    private SourceCommandNamedParameters() {}

    /**
     * Validates options. An empty map is rejected. Unknown keys are rejected.
     */
    public static CommandNamedParameters commandNamedParameters(Source source, Map<String, Object> folded) {
        if (folded.isEmpty()) {
            throw new ParsingException(source, "Empty option is not supported");
        }
        for (Map.Entry<String, Object> entry : folded.entrySet()) {
            switch (entry.getKey()) {
                case APPLY_REQUEST_FILTER -> {
                    if (entry.getValue() instanceof Boolean == false) {
                        throw new ParsingException(
                            source,
                            "The value of option [{}] must be the literal true or false",
                            APPLY_REQUEST_FILTER
                        );
                    }
                }
                default -> throw new ParsingException(source, "Option [{}] is not supported", entry.getKey());
            }
        }
        return new CommandNamedParameters(folded);
    }

    /**
     * Disables the request filter on source commands in {@code plan} that did not write {@code apply_request_filter}.
     * A command that wrote {@code true} or {@code false} is left as written.
     */
    public static LogicalPlan disableUnspecifiedRequestFilterInChildren(LogicalPlan plan) {
        LogicalPlan indexes = plan.transformDown(UnresolvedRelation.class, UnresolvedRelation::optOutOfRequestFilter);
        return indexes.transformDown(UnresolvedExternalRelation.class, UnresolvedExternalRelation::optOutOfRequestFilter);
    }

    /** Marks resolved index and dataset relations so the Lucene and dataset paths skip the request filter. */
    public static LogicalPlan optOutResolved(LogicalPlan plan) {
        LogicalPlan indexes = plan.transformDown(EsRelation.class, relation -> relation.withApplyRequestFilter(false));
        return indexes.transformDown(ExternalRelation.class, relation -> relation.withApplyRequestFilter(false));
    }

    /**
     * Rejects a query that writes {@code apply_request_filter} when the request body has no DSL {@code filter}. Both {@code true} and
     * {@code false} are rejected. {@code IN} sources are checked too. A child that only inherited the value is not a separate hit; the
     * command that wrote {@code WITH} is.
     */
    public static void rejectWithoutRequestFilter(LogicalPlan plan) {
        plan.forEachDown(node -> {
            if (node instanceof UnresolvedRelation ur && ur.applyRequestFilterExplicit()) {
                throw missingRequestFilter(ur.source());
            }
            if (node instanceof UnresolvedExternalRelation ur && ur.applyRequestFilterExplicit()) {
                throw missingRequestFilter(ur.source());
            }
            for (var expression : node.expressions()) {
                rejectWithoutRequestFilterExpressions(expression);
            }
        });
    }

    private static void rejectWithoutRequestFilterExpressions(Expression expression) {
        if (expression instanceof InSubquery inSubquery) {
            rejectWithoutRequestFilter(inSubquery.subquery());
        } else if (expression instanceof MultiColumnInSubquery multi) {
            rejectWithoutRequestFilter(multi.subquery());
        }
        for (Expression child : expression.children()) {
            rejectWithoutRequestFilterExpressions(child);
        }
    }

    private static VerificationException missingRequestFilter(Source source) {
        return new VerificationException("apply_request_filter cannot be set when the request has no filter, found at [{}]", source.text());
    }

    /**
     * Rejects a view query that writes {@code apply_request_filter}. Both {@code true} and {@code false} are rejected.
     * {@code IN} sources inside the view are checked too.
     */
    public static void rejectInView(String viewName, LogicalPlan plan) {
        plan.forEachDown(node -> {
            if (node instanceof UnresolvedRelation ur && ur.applyRequestFilterExplicit()) {
                throw rejected(viewName, ur.source());
            }
            if (node instanceof UnresolvedExternalRelation ur && ur.applyRequestFilterExplicit()) {
                throw rejected(viewName, ur.source());
            }
            for (var expression : node.expressions()) {
                rejectInViewExpressions(viewName, expression);
            }
        });
    }

    private static void rejectInViewExpressions(String viewName, Expression expression) {
        if (expression instanceof InSubquery inSubquery) {
            rejectInView(viewName, inSubquery.subquery());
        } else if (expression instanceof MultiColumnInSubquery multi) {
            rejectInView(viewName, multi.subquery());
        }
        for (Expression child : expression.children()) {
            rejectInViewExpressions(viewName, child);
        }
    }

    private static ParsingException rejected(String viewName, Source source) {
        return new ParsingException(
            source,
            "apply_request_filter cannot be set on a source inside a view definition [{}]. "
                + "The request filter applies to the view's output. Opt out from the query with FROM [{}] "
                + "WITH {\"apply_request_filter\": false}",
            viewName,
            viewName
        );
    }

    /**
     * Index patterns for which every {@link UnresolvedRelation} opted out. Field-caps has one call per pattern, so
     * {@code index_filter} is omitted only then. A pattern that any reference still applies the request filter to keeps
     * {@code index_filter}; that reference must not resolve against the unfiltered index set. Does not enter {@code IN}
     * expressions that have not yet been rewritten into plan nodes.
     */
    public static Set<IndexPattern> optedOutIndexPatterns(LogicalPlan plan) {
        Map<IndexPattern, Boolean> anyApplies = new HashMap<>();
        plan.forEachDown(UnresolvedRelation.class, ur -> anyApplies.merge(ur.indexPattern(), ur.applyRequestFilter(), Boolean::logicalOr));
        return keysWhereNoneApply(anyApplies);
    }

    /**
     * Linked shadow pattern strings for which every shadow opted out. One shadow that still applies the request filter
     * keeps {@code index_filter} for that string.
     */
    public static Set<String> optedOutLinkedPatterns(LogicalPlan plan) {
        Map<String, Boolean> anyApplies = new HashMap<>();
        plan.forEachDown(
            ViewShadowRelation.class,
            shadow -> anyApplies.merge(
                shadow.linkedIndexPattern().pattern().indexPattern(),
                shadow.applyRequestFilter(),
                Boolean::logicalOr
            )
        );
        plan.forEachDown(
            DatasetShadowRelation.class,
            shadow -> anyApplies.merge(
                shadow.linkedIndexPattern().pattern().indexPattern(),
                shadow.applyRequestFilter(),
                Boolean::logicalOr
            )
        );
        return keysWhereNoneApply(anyApplies);
    }

    /**
     * Literal external paths for which every {@link UnresolvedExternalRelation} opted out. Listing bounds are dropped
     * only for those paths. A path that any reference still applies the request filter to keeps the bounds.
     */
    public static Set<String> fullyOptedOutExternalPaths(LogicalPlan plan) {
        Map<String, Boolean> anyApplies = new HashMap<>();
        plan.forEachDown(UnresolvedExternalRelation.class, relation -> {
            if (relation.tablePath() instanceof Literal literal && literal.value() != null) {
                anyApplies.merge(BytesRefs.toString(literal.value()), relation.applyRequestFilter(), Boolean::logicalOr);
            }
        });
        return keysWhereNoneApply(anyApplies);
    }

    private static <T> Set<T> keysWhereNoneApply(Map<T, Boolean> anyApplies) {
        Set<T> optedOut = new HashSet<>();
        for (Map.Entry<T, Boolean> entry : anyApplies.entrySet()) {
            if (entry.getValue() == false) {
                optedOut.add(entry.getKey());
            }
        }
        return optedOut;
    }
}
