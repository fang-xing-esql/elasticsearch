/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;

/**
 * An INNER equi-join between two regular indices that are partitioned the same way on the join key: same number of
 * primaries, documents routed by the key, and shard {@code i} of both indices hosted on the same node. Under that
 * layout a key can only match within the same shard number, so the join executes on the data nodes, each node joining
 * its local left shards against the matching local right shards. Nothing is exchanged between data nodes.
 * <p>
 * Syntax: {@code FROM left | JOIN right ON key}. The output is the left output followed by the right output minus the
 * key, with the right side shadowing the left on name clashes, as for {@code LOOKUP JOIN}.
 * <p>
 * The join always executes remotely and survives optimization as its own node so the mappers can plan it. It is
 * serialized because the whole join ships to the data nodes inside a {@code FragmentExec}.
 */
public class ColocatedJoin extends Join implements TelemetryAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ColocatedJoin",
        ColocatedJoin::new
    );

    public ColocatedJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> joinFields) {
        this(source, left, right, JoinTypes.INNER, joinFields, joinFields, null);
    }

    public ColocatedJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions
    ) {
        this(source, left, right, new JoinConfig(type, leftFields, rightFields, joinOnConditions));
    }

    public ColocatedJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config, ExecuteLocation.REMOTE);
    }

    private ColocatedJoin(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(LogicalPlan.class),
            new JoinConfig(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
        config().writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Join> info() {
        // Same as Join: list the config's parts so expressions and references are registered.
        return NodeInfo.create(
            this,
            ColocatedJoin::new,
            left(),
            right(),
            config().type(),
            config().leftFields(),
            config().rightFields(),
            config().joinOnConditions()
        );
    }

    @Override
    public Join withConfig(JoinConfig config) {
        return new ColocatedJoin(source(), left(), right(), config);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new ColocatedJoin(source(), left, right, config());
    }

    /**
     * USING semantics: the right side contributes everything but the join keys, which are kept from the left.
     * The right side overrides the left on name clashes (same as the field-based {@code LOOKUP JOIN}).
     */
    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        AttributeSet rightKeys = AttributeSet.of(config().rightFields());
        List<? extends NamedExpression> rightWithoutKeys = right.stream()
            .filter(ne -> rightKeys.contains(ne.toAttribute()) == false)
            .toList();
        return mergeOutputExpressions(rightWithoutKeys, left);
    }

    @Override
    public String telemetryLabel() {
        return "COLOCATED JOIN";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        EsRelation leftRelation = findRelation(left());
        EsRelation rightRelation = findRelation(right());
        if (leftRelation == null || rightRelation == null) {
            failures.add(fail(this, "JOIN requires a plain index on both sides"));
            return;
        }
        IndexProperties leftProps = singleLocalIndex(leftRelation, "left", failures);
        IndexProperties rightProps = singleLocalIndex(rightRelation, "right", failures);
        if (leftProps == null || rightProps == null) {
            return;
        }
        if (leftProps.numberOfShards() <= 0 || rightProps.numberOfShards() <= 0) {
            failures.add(fail(this, "JOIN requires the shard count of both indices to be known"));
        } else if (leftProps.numberOfShards() != rightProps.numberOfShards()) {
            failures.add(
                fail(
                    this,
                    "JOIN requires the same number of shards on both sides, found [{}] for [{}] and [{}] for [{}]",
                    leftProps.numberOfShards(),
                    leftRelation.indexPattern(),
                    rightProps.numberOfShards(),
                    rightRelation.indexPattern()
                )
            );
        }
        for (int i = 0; i < config().leftFields().size(); i++) {
            Attribute leftField = config().leftFields().get(i);
            Attribute rightField = config().rightFields().get(i);
            if (leftField.dataType() != DataType.KEYWORD || rightField.dataType() != DataType.KEYWORD) {
                failures.add(
                    fail(
                        leftField,
                        "JOIN key must be a keyword field on both sides, found [{}] and [{}]",
                        leftField.dataType(),
                        rightField.dataType()
                    )
                );
            }
        }
    }

    /**
     * Walks down unary nodes (filters, projections, evals) to the relation feeding one side of the join, or
     * {@code null} if the side is not a plain index scan.
     */
    public static EsRelation findRelation(LogicalPlan plan) {
        while (plan instanceof UnaryPlan unary) {
            plan = unary.child();
        }
        return plan instanceof EsRelation relation ? relation : null;
    }

    private IndexProperties singleLocalIndex(EsRelation relation, String side, Failures failures) {
        Map<String, List<String>> concrete = relation.concreteIndices();
        List<String> local = concrete.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        if (concrete.size() != 1 || local == null || local.size() != 1) {
            failures.add(
                fail(
                    this,
                    "JOIN requires the {} side [{}] to resolve to a single local index, found {}",
                    side,
                    relation.indexPattern(),
                    concrete
                )
            );
            return null;
        }
        IndexProperties properties = relation.indexProperties().get(local.get(0));
        if (properties == null) {
            failures.add(fail(this, "JOIN could not find index properties for [{}]", local.get(0)));
        }
        return properties;
    }
}
