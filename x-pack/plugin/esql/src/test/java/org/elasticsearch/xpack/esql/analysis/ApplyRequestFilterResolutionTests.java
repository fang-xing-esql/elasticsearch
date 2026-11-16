/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommandNamedParameters;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The request-filter flag survives the copies that turn a parsed {@code FROM} into a resolved plan.
 * Plan text prints {@code [apply_request_filter=false]} only when the flag is false.
 */
public class ApplyRequestFilterResolutionTests extends org.elasticsearch.test.ESTestCase {

    public void testCopiesKeepTheOptions() {
        assumeOption();
        UnresolvedRelation parsed = (UnresolvedRelation) EsqlTestUtils.TEST_PARSER.parseQuery(
            "FROM a WITH {\"apply_request_filter\": false}"
        );
        UnresolvedRelation renamed = parsed.withIndexPattern(new IndexPattern(parsed.source(), "b"));
        assertSame(parsed.commandNamedParameters(), renamed.commandNamedParameters());
        assertFalse(renamed.applyRequestFilter());

        UnresolvedRelation withMetadata = parsed.addMetadataField(new MetadataAttribute(Source.EMPTY, "_id", DataType.KEYWORD, false));
        assertSame(parsed.commandNamedParameters(), withMetadata.commandNamedParameters());
        assertFalse(withMetadata.applyRequestFilter());
    }

    public void testPlanTextPrintsTheFlagOnlyWhenFalse() {
        assumeOption();
        TestAnalyzer analyzer = EsqlTestUtils.analyzer().addEmployees("test");
        LogicalPlan optedOut = query(analyzer, "FROM test WITH {\"apply_request_filter\": false} | LIMIT 10");
        EsRelation relation = optedOut.collect(EsRelation.class).getFirst();
        assertFalse(relation.applyRequestFilter());
        assertThat(optedOut.toString(), containsString("[apply_request_filter=false]"));

        assertThat(query(analyzer, "FROM test | LIMIT 10").toString(), not(containsString("apply_request_filter")));
        assertThat(
            query(analyzer, "FROM test WITH {\"apply_request_filter\": true} | LIMIT 10").toString(),
            not(containsString("apply_request_filter"))
        );
        UnresolvedRelation parsed = (UnresolvedRelation) EsqlTestUtils.TEST_PARSER.parseQuery(
            "FROM a, b WITH {\"apply_request_filter\": false}"
        );
        assertThat(parsed.indexPattern().indexPattern(), equalTo("a,b"));
    }

    /** The copy {@code ViewResolver} runs on a pass-through body that is still an unresolved index relation. */
    public void testPassThroughBodyInheritsTheCallSiteOptOut() {
        UnresolvedRelation body = (UnresolvedRelation) EsqlTestUtils.TEST_PARSER.parseQuery("FROM test");
        LogicalPlan optedOut = SourceCommandNamedParameters.disableUnspecifiedRequestFilterInChildren(body);
        UnresolvedRelation relation = (UnresolvedRelation) optedOut;
        assertFalse(relation.applyRequestFilter());
        assertFalse(relation.applyRequestFilterExplicit());

        UnresolvedRelation explicit = (UnresolvedRelation) EsqlTestUtils.TEST_PARSER.parseQuery(
            "FROM test WITH {\"apply_request_filter\": true}"
        );
        assertSame(explicit, SourceCommandNamedParameters.disableUnspecifiedRequestFilterInChildren(explicit));
    }

    /**
     * {@code unmapped_fields=nullify} is what keeps the computed view branch here. A request filter sets the same
     * analyzer flag. The call-site opt-out stays on the view identity and is not written into the body.
     */
    public void testComputedViewKeepsTheFlagOnTheBranchIdentity() {
        assumeOption();
        LogicalPlan plan = query(
            EsqlTestUtils.analyzer()
                .unmappedResolution(UnmappedResolution.NULLIFY)
                .addEmployees("test")
                .addView("computed", "FROM test | EVAL x = emp_no + 1 | KEEP x | LIMIT 10"),
            "FROM computed WITH {\"apply_request_filter\": false} | LIMIT 10"
        );
        BranchPlan branchPlan = plan.collect(BranchPlan.class).getFirst();
        boolean foundView = false;
        for (String key : branchPlan.namedSubqueries().keySet()) {
            if (branchPlan.isViewBranch(key)) {
                foundView = true;
                assertThat(branchPlan.branchIdentity(key).name(), equalTo("computed"));
                // The analyzer harness keeps the branch. The call-site flag on that identity is what
                // ViewResolver stores; SubqueryViewRequestFilterIT checks the rows that follow from it.
                for (EsRelation relation : branchPlan.namedSubqueries().get(key).collect(EsRelation.class)) {
                    assertTrue("the stored view body is not rewritten", relation.applyRequestFilter());
                }
            }
        }
        assertTrue(foundView);
    }

    public void testLiftedThroughCopiesTheOuterViewFlag() {
        BranchIdentity inner = BranchIdentity.INDEX.withApplyRequestFilter(true);
        BranchIdentity lifted = inner.liftedThrough(BranchIdentity.view("outer", false));
        assertFalse(lifted.applyRequestFilter());
        assertTrue(lifted.isView());
        assertThat(lifted.name(), equalTo("outer"));
        assertSame(inner, inner.liftedThrough(BranchIdentity.INDEX));
    }

    private LogicalPlan query(TestAnalyzer analyzer, String query) {
        LogicalPlan plan = analyzer.query(query);
        takeResponseWarnings();
        return plan;
    }

    private static void assumeOption() {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
    }
}
