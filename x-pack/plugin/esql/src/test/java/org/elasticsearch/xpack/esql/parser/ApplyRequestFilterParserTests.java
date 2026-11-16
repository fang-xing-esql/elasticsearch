/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommandNamedParameters;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedMetadata;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

public class ApplyRequestFilterParserTests extends AbstractStatementParserTests {

    /**
     * UnresolvedRelation[a,b]
     */
    public void testFromCommandDefault() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("FROM a, b");

        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals("a,b", relation.indexPattern().indexPattern());
        assertTrue(relation.applyRequestFilter());
        assertFalse(relation.applyRequestFilterExplicit());
    }

    /**
     * UnresolvedRelation[a,b]
     */
    public void testFromCommandOptsOut() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("FROM a, b WITH {\"apply_request_filter\": false}");

        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals("a,b", relation.indexPattern().indexPattern());
        assertFalse(relation.applyRequestFilter());
        assertTrue(relation.applyRequestFilterExplicit());
    }

    /**
     * UnresolvedRelation[a,b]
     */
    public void testFromCommandExplicitTrue() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("FROM a, b WITH {\"apply_request_filter\": true}");

        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals("a,b", relation.indexPattern().indexPattern());
        assertTrue(relation.applyRequestFilter());
        assertTrue(relation.applyRequestFilterExplicit());
    }

    /**
     * UnresolvedRelation[idx]
     */
    public void testTimeSeriesCommandOptsOut() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("TS idx WITH {\"apply_request_filter\": false}");

        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals("idx", relation.indexPattern().indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
        assertFalse(relation.applyRequestFilter());
        assertTrue(relation.applyRequestFilterExplicit());
    }

    /**
     * UnresolvedMetadata[]
     * \_UnresolvedRelation[idx]
     */
    public void testFromCommandWithMetadataOptsOut() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("FROM idx METADATA _id WITH {\"apply_request_filter\": false}");

        UnresolvedMetadata metadata = as(plan, UnresolvedMetadata.class);
        assertEquals(1, metadata.metadataFields().size());
        assertEquals("_id", as(metadata.metadataFields().get(0), MetadataAttribute.class).name());
        UnresolvedRelation relation = as(metadata.child(), UnresolvedRelation.class);
        assertEquals("idx", relation.indexPattern().indexPattern());
        assertEquals(1, relation.metadataFields().size());
        assertEquals("_id", as(relation.metadataFields().get(0), MetadataAttribute.class).name());
        assertFalse(relation.applyRequestFilter());
        assertTrue(relation.applyRequestFilterExplicit());
    }

    /**
     * Filter[?id IN (Keep[[?id]])]
     * \_UnresolvedRelation[test]
     *
     * The IN source is an expression, so the parent clause does not enter it:
     * Keep[[?id]]
     * \_UnresolvedRelation[other]
     */
    public void testParentClauseDoesNotPropagateInSubquery() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM test WITH {"apply_request_filter": false}
            | WHERE id IN (FROM other | KEEP id)
            """);

        Filter filter = as(plan, Filter.class);
        UnresolvedRelation parent = as(filter.child(), UnresolvedRelation.class);
        assertEquals("test", parent.indexPattern().indexPattern());
        assertFalse(parent.applyRequestFilter());
        assertTrue(parent.applyRequestFilterExplicit());

        InSubquery in = as(filter.condition(), InSubquery.class);
        Keep keep = as(in.subquery(), Keep.class);
        assertEquals(1, keep.projections().size());
        assertEquals("id", keep.projections().get(0).name());
        UnresolvedRelation inSource = as(keep.child(), UnresolvedRelation.class);
        assertEquals("other", inSource.indexPattern().indexPattern());
        assertTrue(inSource.applyRequestFilter());
        assertFalse(inSource.applyRequestFilterExplicit());
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[a]
     * \_Subquery[]
     *   \_Keep[[?id]]
     *     \_UnresolvedRelation[b]
     */
    public void testSourceSubqueryInheritsWithClause() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM a, (FROM b | KEEP id) WITH {"apply_request_filter": false}
            """);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation main = as(children.get(0), UnresolvedRelation.class);
        assertEquals("a", main.indexPattern().indexPattern());
        assertFalse(main.applyRequestFilter());
        assertTrue(main.applyRequestFilterExplicit());

        Subquery subquery = as(children.get(1), Subquery.class);
        Keep keep = as(subquery.plan(), Keep.class);
        assertEquals("id", keep.projections().get(0).name());
        UnresolvedRelation nested = as(keep.child(), UnresolvedRelation.class);
        assertEquals("b", nested.indexPattern().indexPattern());
        assertFalse(nested.applyRequestFilter());
        assertFalse(nested.applyRequestFilterExplicit());
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[a]
     * \_Subquery[]
     *   \_Keep[[?id]]
     *     \_UnresolvedRelation[b]
     */
    public void testExplicitInnerWithClauseWins() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM a, (FROM b WITH {"apply_request_filter": true} | KEEP id) WITH {"apply_request_filter": false}
            """);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation main = as(children.get(0), UnresolvedRelation.class);
        assertEquals("a", main.indexPattern().indexPattern());
        assertFalse(main.applyRequestFilter());
        assertTrue(main.applyRequestFilterExplicit());

        Subquery subquery = as(children.get(1), Subquery.class);
        Keep keep = as(subquery.plan(), Keep.class);
        UnresolvedRelation nested = as(keep.child(), UnresolvedRelation.class);
        assertEquals("b", nested.indexPattern().indexPattern());
        assertTrue(nested.applyRequestFilter());
        assertTrue(nested.applyRequestFilterExplicit());
    }

    public void testClauseRequiresARequestFilter() {
        requireApplyRequestFilterOption();
        for (String query : List.of(
            "FROM idx WITH {\"apply_request_filter\": false}",
            "FROM idx WITH {\"apply_request_filter\": true}",
            "TS idx WITH {\"apply_request_filter\": false}",
            "FROM a, (FROM b WITH {\"apply_request_filter\": false} | KEEP id)",
            "FROM test | WHERE id IN (FROM other WITH {\"apply_request_filter\": true} | KEEP id)"
        )) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> SourceCommandNamedParameters.rejectWithoutRequestFilter(query(query))
            );
            assertThat(e.getMessage(), containsString("cannot be set when the request has no filter"));
        }
        SourceCommandNamedParameters.rejectWithoutRequestFilter(query("FROM idx"));

        EsqlStatement unusedLet = statement("""
            LET s = (FROM logs WITH {"apply_request_filter": false} | KEEP id);
            FROM other
            """);
        SourceCommandNamedParameters.rejectWithoutRequestFilter(unusedLet.plan());
        VerificationException letError = expectThrows(
            VerificationException.class,
            () -> SourceCommandNamedParameters.rejectWithoutRequestFilter(unusedLet.letBindings().get(0).plan())
        );
        assertThat(letError.getMessage(), containsString("cannot be set when the request has no filter"));
    }

    public void testViewCreationRejectsEitherValue() {
        requireApplyRequestFilterOption();
        var parser = new EsqlParser(new org.elasticsearch.xpack.esql.parser.EsqlConfig(EsqlTestUtils.TEST_FUNCTION_REGISTRY));
        for (String value : List.of("false", "true")) {
            expectThrows(
                ParsingException.class,
                containsString("apply_request_filter cannot be set on a source inside a view definition"),
                () -> parser.parseView(
                    "FROM idx WITH {\"apply_request_filter\": " + value + "}",
                    new QueryParams(),
                    new InferenceSettings(org.elasticsearch.common.settings.Settings.EMPTY),
                    "my_view",
                    QueryPragmas.EMPTY,
                    EsqlFlags.DEFAULTS
                )
            );
        }
    }

    // negative

    public void testCommandLevelClauseRejectsInvalidOptions() {
        requireApplyRequestFilterOption();
        expectError("FROM a WITH {\"apply_request_filter\": false}, b", "mismatched input ',' expecting {<EOF>, '|'}");
        expectError("FROM idx WITH {}", "Empty option is not supported");
        expectError("FROM idx WITH {\"nope\": false}", "Option [nope] is not supported");
        expectError(
            "FROM idx WITH {\"apply_request_filter\": 1}",
            "The value of option [apply_request_filter] must be the literal true or false"
        );
        expectError(
            "FROM idx WITH {\"apply_request_filter\": false, \"apply_request_filter\": true}",
            "Duplicated named parameters with the same name [apply_request_filter] is not supported"
        );
        expectError(
            "FROM idx WITH {\"apply_request_filter\": false} METADATA _index",
            "mismatched input 'METADATA' expecting {<EOF>, '|'}"
        );
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[a]
     * \_Subquery[]
     *   \_Keep[[?id]]
     *     \_UnresolvedRelation[b]
     */
    public void testInnerWithClauseDoesNotOptOutTheOuterCommand() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM a, (FROM b WITH {"apply_request_filter": false} | KEEP id)
            """);

        UnionAll unionAll = as(plan, UnionAll.class);
        UnresolvedRelation main = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("a", main.indexPattern().indexPattern());
        assertTrue(main.applyRequestFilter());
        assertFalse(main.applyRequestFilterExplicit());

        Keep keep = as(as(unionAll.children().get(1), Subquery.class).plan(), Keep.class);
        UnresolvedRelation nested = as(keep.child(), UnresolvedRelation.class);
        assertEquals("b", nested.indexPattern().indexPattern());
        assertFalse(nested.applyRequestFilter());
        assertTrue(nested.applyRequestFilterExplicit());
    }

    /**
     * Keep[[?id]]
     * \_UnresolvedRelation[b]
     */
    public void testSoleSourceSubqueryInheritsWithClause() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM (FROM b | KEEP id) WITH {"apply_request_filter": false}
            """);

        Keep keep = as(plan, Keep.class);
        UnresolvedRelation nested = as(keep.child(), UnresolvedRelation.class);
        assertEquals("b", nested.indexPattern().indexPattern());
        assertFalse(nested.applyRequestFilter());
        assertFalse(nested.applyRequestFilterExplicit());
    }

    /**
     * Keep[[?x]]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[b]
     *   \_Subquery[]
     *     \_Keep[[?id]]
     *       \_UnresolvedRelation[c]
     */
    public void testNestedSourceListInheritsWithClause() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM (FROM b, (FROM c | KEEP id) | KEEP x) WITH {"apply_request_filter": false}
            """);

        Keep keep = as(plan, Keep.class);
        UnionAll unionAll = as(keep.child(), UnionAll.class);
        UnresolvedRelation b = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("b", b.indexPattern().indexPattern());
        assertFalse(b.applyRequestFilter());
        assertFalse(b.applyRequestFilterExplicit());

        Keep innerKeep = as(as(unionAll.children().get(1), Subquery.class).plan(), Keep.class);
        UnresolvedRelation c = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("c", c.indexPattern().indexPattern());
        assertFalse(c.applyRequestFilter());
        assertFalse(c.applyRequestFilterExplicit());
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[a]
     * \_Subquery[]
     *   \_Filter[?id IN (Keep[[?id]])]
     *     \_UnresolvedRelation[b]
     *
     * The IN source stays at the default:
     * Keep[[?id]]
     * \_UnresolvedRelation[c]
     */
    public void testSourceListWithClauseDoesNotEnterInSubquery() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("""
            FROM a, (FROM b | WHERE id IN (FROM c | KEEP id)) WITH {"apply_request_filter": false}
            """);

        UnionAll unionAll = as(plan, UnionAll.class);
        UnresolvedRelation main = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("a", main.indexPattern().indexPattern());
        assertFalse(main.applyRequestFilter());
        assertTrue(main.applyRequestFilterExplicit());

        Filter filter = as(as(unionAll.children().get(1), Subquery.class).plan(), Filter.class);
        UnresolvedRelation middle = as(filter.child(), UnresolvedRelation.class);
        assertEquals("b", middle.indexPattern().indexPattern());
        assertFalse(middle.applyRequestFilter());
        assertFalse(middle.applyRequestFilterExplicit());

        Keep keep = as(as(filter.condition(), InSubquery.class).subquery(), Keep.class);
        UnresolvedRelation inSource = as(keep.child(), UnresolvedRelation.class);
        assertEquals("c", inSource.indexPattern().indexPattern());
        assertTrue(inSource.applyRequestFilter());
        assertFalse(inSource.applyRequestFilterExplicit());
    }

    /**
     * UnresolvedRelation[a,b]
     */
    public void testTimeSeriesCommandSeveralPatterns() {
        requireApplyRequestFilterOption();
        LogicalPlan plan = query("TS a, b WITH {\"apply_request_filter\": false}");

        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals("a,b", relation.indexPattern().indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
        assertFalse(relation.applyRequestFilter());
        assertTrue(relation.applyRequestFilterExplicit());
    }

    public void testTimeSeriesCommandRejectsSubquery() {
        requireApplyRequestFilterOption();
        expectError("TS idx, (FROM b | KEEP id)", "Subqueries are not supported in TS command");
        expectError("TS idx, (FROM b | KEEP id) WITH {\"apply_request_filter\": false}", "Subqueries are not supported in TS command");
    }

    public void testOptionValueMustBeABooleanLiteral() {
        requireApplyRequestFilterOption();
        expectError(
            "FROM idx WITH {\"apply_request_filter\": null}",
            "FROM option [apply_request_filter] has null value; null is not a valid option value"
        );
        expectError(
            "FROM idx WITH {\"apply_request_filter\": \"false\"}",
            "The value of option [apply_request_filter] must be the literal true or false"
        );
        expectError("FROM idx WITH {\"apply_request_filter\": ?p}", "has null value; null is not a valid option value");
        expectError("FROM idx WITH {\"apply_request_filter\": {\"nested\": false}}", "must be literal values");
    }

    public void testParseViewRejectsTheClauseDeeperInTheQuery() {
        requireApplyRequestFilterOption();
        var parser = new EsqlParser(new EsqlConfig(EsqlTestUtils.TEST_FUNCTION_REGISTRY));
        for (String query : List.of(
            "TS idx WITH {\"apply_request_filter\": false}",
            "TS idx WITH {\"apply_request_filter\": true}",
            "FROM a, (FROM b WITH {\"apply_request_filter\": false} | KEEP id)",
            "FROM a | WHERE id IN (FROM b WITH {\"apply_request_filter\": true} | KEEP id)"
        )) {
            expectThrows(
                ParsingException.class,
                containsString("apply_request_filter cannot be set on a source inside a view definition"),
                () -> parseView(parser, query)
            );
        }
        parseView(parser, "FROM test | WHERE id IN (FROM test | KEEP id)");
    }

    public void testExternalCommandDoesNotReadApplyRequestFilter() {
        requireApplyRequestFilterOption();
        assumeTrue("requires the external command", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        LogicalPlan plan = query("EXTERNAL \"p\" WITH {\"format\": \"parquet\", \"apply_request_filter\": false}");

        UnresolvedExternalRelation relation = as(plan, UnresolvedExternalRelation.class);
        assertTrue(relation.applyRequestFilter());
        assertFalse(relation.applyRequestFilterExplicit());
        assertEquals("parquet", relation.config().get("format"));
        assertEquals(false, relation.config().get("apply_request_filter"));
    }

    public void testLookupJoinRejectsTheClause() {
        requireApplyRequestFilterOption();
        expectError("FROM t | LOOKUP JOIN idx WITH {\"apply_request_filter\": false} ON id", "expecting 'on'");
    }

    public void testReleaseBuildRejectsWithClause() {
        assumeFalse("release builds do not parse the with clause", Build.current().isSnapshot());
        expectError("FROM idx WITH {\"apply_request_filter\": false}", "no viable alternative at input 'WITH'");
        expectError("FROM idx METADATA _id WITH {\"apply_request_filter\": false}", "no viable alternative at input 'WITH'");
        expectError("TS idx WITH {\"apply_request_filter\": true}", "no viable alternative at input 'WITH'");
        var parser = new EsqlParser(new EsqlConfig(EsqlTestUtils.TEST_FUNCTION_REGISTRY));
        expectThrows(
            ParsingException.class,
            containsString("no viable alternative at input 'WITH'"),
            () -> parseView(parser, "FROM idx WITH {\"apply_request_filter\": false}")
        );
        var from = query("FROM idx");
        UnresolvedRelation relation = as(from, UnresolvedRelation.class);
        assertEquals("idx", relation.indexPattern().indexPattern());
        assertTrue(relation.applyRequestFilter());
    }

    private static void requireApplyRequestFilterOption() {
        assumeTrue("apply_request_filter syntax is snapshot-only", EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled());
    }

    private static void parseView(EsqlParser parser, String query) {
        parser.parseView(
            query,
            new QueryParams(),
            new InferenceSettings(org.elasticsearch.common.settings.Settings.EMPTY),
            "my_view",
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS
        );
    }
}
