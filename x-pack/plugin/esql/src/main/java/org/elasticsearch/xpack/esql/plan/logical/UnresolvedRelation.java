/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Unresolved index-side relation produced by the parser for {@code FROM <index>} (and other
 * index-pattern source commands). Pre-analysis machinery and optimizer rules typically pattern-match
 * on this class.
 *
 * @see UnresolvedExternalRelation external-side counterpart for {@code FROM <dataset>} and inline
 * {@code EXTERNAL}; if you traverse one and care about FROM-style leaves, consider whether you need
 * the other too.
 * @see UnresolvedSourceRelation marker implemented by both shapes; match on it to traverse any
 * FROM-style leaf.
 */
public final class UnresolvedRelation extends LeafPlan implements Unresolvable, TelemetryAware, UnresolvedSourceRelation {

    private final IndexPattern indexPattern;
    private final boolean frozen;
    private final List<NamedExpression> metadataFields;
    /*
     * Expected indexMode based on the declaration - used later for verification
     * at resolution time.
     */
    private final IndexMode indexMode;
    private final String unresolvedMsg;

    /**
     * Used by telemetry to say if this is the result of a FROM command
     * or a TS command (or maybe something else in the future)
     */
    private final String commandName;
    /**
     * Options from the command's {@code WITH} map. Coordinator-only: this node is not serialized.
     */
    private final CommandNamedParameters commandNamedParameters;

    public UnresolvedRelation(
        Source source,
        IndexPattern indexPattern,
        boolean frozen,
        List<NamedExpression> metadataFields,
        String unresolvedMessage,
        SourceCommand sourceCommand
    ) {
        this(
            source,
            indexPattern,
            frozen,
            metadataFields,
            sourceCommand.indexMode(),
            unresolvedMessage,
            sourceCommand.name(),
            CommandNamedParameters.EMPTY
        );
    }

    public UnresolvedRelation(
        Source source,
        IndexPattern indexPattern,
        boolean frozen,
        List<NamedExpression> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage,
        @Nullable String commandName,
        CommandNamedParameters commandNamedParameters
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.frozen = frozen;
        this.metadataFields = metadataFields;
        this.indexMode = indexMode;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown index [" + indexPattern.indexPattern() + "]" : unresolvedMessage;
        this.commandName = commandName;
        this.commandNamedParameters = commandNamedParameters == null ? CommandNamedParameters.EMPTY : commandNamedParameters;
    }

    public UnresolvedRelation(
        Source source,
        IndexPattern table,
        boolean frozen,
        List<NamedExpression> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage
    ) {
        this(source, table, frozen, metadataFields, indexMode, unresolvedMessage, null, CommandNamedParameters.EMPTY);
    }

    public UnresolvedRelation(
        Source source,
        IndexPattern indexPattern,
        boolean frozen,
        List<NamedExpression> metadataFields,
        IndexMode indexMode,
        String unresolvedMessage,
        @Nullable String commandName
    ) {
        this(source, indexPattern, frozen, metadataFields, indexMode, unresolvedMessage, commandName, CommandNamedParameters.EMPTY);
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(
            this,
            UnresolvedRelation::new,
            indexPattern,
            frozen,
            metadataFields,
            indexMode,
            unresolvedMsg,
            commandName,
            commandNamedParameters
        );
    }

    public IndexPattern indexPattern() {
        return indexPattern;
    }

    public UnresolvedRelation withIndexPattern(IndexPattern newIndexPattern) {
        return newIndexPattern.equals(indexPattern)
            ? this
            : new UnresolvedRelation(
                source(),
                newIndexPattern,
                frozen,
                metadataFields,
                indexMode,
                unresolvedMsg,
                commandName,
                commandNamedParameters
            );
    }

    /** Options from this command's {@code WITH} map. Empty when the clause was absent. */
    public CommandNamedParameters commandNamedParameters() {
        return commandNamedParameters;
    }

    /** Whether the request DSL filter applies to this command. Absent means yes. */
    public boolean applyRequestFilter() {
        Object value = commandNamedParameters.get(SourceCommandNamedParameters.APPLY_REQUEST_FILTER);
        return value == null || Boolean.TRUE.equals(value);
    }

    /** Whether this command wrote {@code apply_request_filter}, so an enclosing command does not overwrite it. */
    public boolean applyRequestFilterExplicit() {
        return commandNamedParameters.explicit(SourceCommandNamedParameters.APPLY_REQUEST_FILTER);
    }

    /** Records that this command does not apply the request filter, unless it already wrote the option. */
    public UnresolvedRelation optOutOfRequestFilter() {
        CommandNamedParameters updated = commandNamedParameters.withInherited(SourceCommandNamedParameters.APPLY_REQUEST_FILTER, false);
        if (updated == commandNamedParameters) {
            return this;
        }
        return new UnresolvedRelation(source(), indexPattern, frozen, metadataFields, indexMode, unresolvedMsg, commandName, updated);
    }

    public UnresolvedRelation withMetadata(List<NamedExpression> metadata, String unresolvedMessage) {
        return new UnresolvedRelation(
            source(),
            indexPattern,
            frozen,
            metadata,
            indexMode,
            unresolvedMessage,
            commandName,
            commandNamedParameters
        );
    }

    public boolean frozen() {
        return frozen;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    /**
     *
     * This is used by {@link PlanTelemetry} to collect query statistics
     * It can return
     * <ul>
     *     <li>"FROM" if this a <code>|FROM idx</code> command</li>
     *     <li>"METRICS" if it is the result of a <code>| METRICS idx some_aggs() BY fields</code> command</li>
     * </ul>
     */
    @Override
    public String telemetryLabel() {
        return commandName;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    public List<NamedExpression> metadataFields() {
        return metadataFields;
    }

    public UnresolvedRelation addMetadataField(MetadataAttribute newField) {
        ArrayList<NamedExpression> newFields = new ArrayList<>(metadataFields);
        newFields.add(newField);
        return new UnresolvedRelation(
            source(),
            indexPattern,
            frozen,
            newFields,
            indexMode,
            unresolvedMsg,
            commandName,
            commandNamedParameters
        );
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), indexPattern, metadataFields, indexMode, unresolvedMsg, commandNamedParameters);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedRelation other = (UnresolvedRelation) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(frozen, other.frozen)
            && Objects.equals(metadataFields, other.metadataFields)
            && indexMode == other.indexMode
            && Objects.equals(unresolvedMsg, other.unresolvedMsg)
            && Objects.equals(commandNamedParameters, other.commandNamedParameters);
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(indexPattern);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + indexPattern.indexPattern();
    }

    /**
     * @return true if and only if this relation is being loaded in "time series mode",
     *         which changes a number of behaviors in the planner.
     */
    public boolean isTimeSeriesMode() {
        return indexMode.isTsdb();
    }
}
