/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.CommandNamedParameters;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommandNamedParameters;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;
import java.util.Map;

public class ApplyRequestFilterViewCompactionTests extends ESTestCase {

    public void testEqualOptionsMerge() {
        CommandNamedParameters optedOut = explicitFalse();
        UnresolvedRelation merged = ViewCompaction.mergeIfPossible(relation("a", optedOut), relation("b", optedOut));
        assertNotNull(merged);
        assertEquals("a,b", merged.indexPattern().indexPattern());
        assertFalse(merged.applyRequestFilter());
        assertSame(optedOut, merged.commandNamedParameters());
    }

    public void testDifferentOptionsDoNotMerge() {
        assertNull(ViewCompaction.mergeIfPossible(relation("a", explicitFalse()), relation("b", CommandNamedParameters.EMPTY)));
        assertNull(
            ViewCompaction.mergeIfPossible(
                relation("a", explicitFalse()),
                relation("b", CommandNamedParameters.EMPTY.withInherited(SourceCommandNamedParameters.APPLY_REQUEST_FILTER, false))
            )
        );
    }

    private static CommandNamedParameters explicitFalse() {
        return new CommandNamedParameters(Map.of(SourceCommandNamedParameters.APPLY_REQUEST_FILTER, false));
    }

    private static UnresolvedRelation relation(String pattern, CommandNamedParameters parameters) {
        return new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, pattern),
            false,
            List.of(),
            IndexMode.STANDARD,
            null,
            "FROM",
            parameters
        );
    }
}
