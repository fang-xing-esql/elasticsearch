/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.ColocatedJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Chooses how each {@code JOIN} command executes (see {@link Join#isCandicateColocatedJoin()}). The parser and the optimizer only know
 * that it is an inner join of two indices; whether the two indices can be joined shard by shard depends on index metadata
 * and on where the shards are right now, which only the coordinator's cluster state tells. When all of the following hold,
 * the join becomes a {@link ColocatedJoin}:
 * <ul>
 *     <li>each side is a single local index, read through streaming commands only ({@code WHERE}, {@code EVAL},
 *     {@code KEEP} and the like on the left, {@code WHERE} on the right): the join runs inside the data-node fragment, so a
 *     {@code LIMIT} or {@code STATS} below it would run per node;</li>
 *     <li>both indices route documents the same way: same shard count and routing function, no routing partitions,
 *     no {@code routing_path}, no resharding, and {@code _routing} required;</li>
 *     <li>each side's join key is that index's {@code index.routing_field}, so the server guarantees that a key value is
 *     on the shard it hashes to, in both indices;</li>
 *     <li>for every shard number, an active copy of the left and of the right shard share a node.</li>
 * </ul>
 * There is no other way to execute the command yet, so any failed condition fails the query, with every reason listed.
 * Shards can still move between this check and the execution; the data-node dispatch and the local lookup check again.
 */
final class SelectJoinStrategy {

    private SelectJoinStrategy() {}

    static LogicalPlan apply(LogicalPlan plan, ProjectState project) {
        Failures failures = new Failures();
        LogicalPlan result = plan.transformUp(
            Join.class,
            join -> join.isCandicateColocatedJoin() && join instanceof ColocatedJoin == false ? select(join, project, failures) : join
        );
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return result;
    }

    private static Join select(Join join, ProjectState project, Failures allFailures) {
        // Failures dedupes by plan node, so the reasons are collected here and reported as one failure of the join.
        List<Failure> failures = new ArrayList<>();
        EsRelation left = scan(join, join.left(), "left", p -> p instanceof Filter || p instanceof Eval || p instanceof Project, failures);
        EsRelation right = scan(join, join.right(), "right", p -> p instanceof Filter || p instanceof Project, failures);
        IndexMetadata leftIndex = left == null ? null : singleLocalIndex(join, left, project, failures);
        IndexMetadata rightIndex = right == null ? null : singleLocalIndex(join, right, project, failures);
        if (leftIndex != null && rightIndex != null) {
            checkRouting(join, leftIndex, failures);
            checkRouting(join, rightIndex, failures);
            checkRoutingField(join, join.config().leftFields().get(0), leftIndex, failures);
            checkRoutingField(join, join.config().rightFields().get(0), rightIndex, failures);
            if (checkSameRoutingFunction(join, leftIndex, rightIndex, failures)) {
                checkPlacement(join, leftIndex, rightIndex, project, failures);
            }
        }
        if (failures.isEmpty() == false) {
            allFailures.add(fail(join, "{}", failures.stream().map(Failure::message).collect(Collectors.joining("; "))));
            return join;
        }
        return new ColocatedJoin(join);
    }

    /**
     * The relation feeding one side, if every command between it and the join is allowed there.
     */
    private static EsRelation scan(Join join, LogicalPlan side, String which, Predicate<LogicalPlan> allowed, List<Failure> failures) {
        LogicalPlan plan = side;
        while (plan instanceof UnaryPlan unary && allowed.test(unary)) {
            plan = unary.child();
        }
        if (plan instanceof EsRelation relation) {
            return relation;
        }
        failures.add(
            fail(join, "JOIN can't be executed after [{}] on the {} side, only after a scan of an index", plan.source().text(), which)
        );
        return null;
    }

    private static IndexMetadata singleLocalIndex(Join join, EsRelation relation, ProjectState project, List<Failure> failures) {
        List<String> local = relation.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        if (relation.concreteIndices().size() != 1 || local == null || local.size() != 1) {
            failures.add(
                fail(
                    join,
                    "JOIN requires [{}] to resolve to a single local index, found {}",
                    relation.indexPattern(),
                    relation.concreteIndices()
                )
            );
            return null;
        }
        IndexMetadata index = project.metadata().index(local.get(0));
        if (index == null) {
            failures.add(fail(join, "JOIN could not find index [{}]", local.get(0)));
        }
        return index;
    }

    /**
     * Rejects routing schemes that do not send all documents with the same {@code _routing} to one shard by its hash alone.
     */
    private static void checkRouting(Join join, IndexMetadata index, List<Failure> failures) {
        String name = index.getIndex().getName();
        if (index.isRoutingPartitionedIndex()) {
            failures.add(fail(join, "JOIN does not support [{}]: it uses [index.routing_partition_size]", name));
        }
        if (index.getRoutingPaths().isEmpty() == false || index.getTimeSeriesDimensions().isEmpty() == false) {
            failures.add(fail(join, "JOIN does not support [{}]: it routes on document fields, not on [_routing]", name));
        }
        if (index.getReshardingMetadata() != null) {
            failures.add(fail(join, "JOIN does not support [{}]: it is being resharded", name));
        }
        if (index.mapping() == null || index.mapping().routingRequired() == false) {
            failures.add(fail(join, "JOIN requires [{}] to have [_routing] required in its mapping", name));
        }
    }

    /**
     * The join key must be the field the index routes by. Only then does the server guarantee where each key value lives;
     * the routing function checks alone would accept a join on any field.
     */
    private static void checkRoutingField(Join join, Attribute key, IndexMetadata index, List<Failure> failures) {
        String name = index.getIndex().getName();
        String routingField = IndexSettings.ROUTING_FIELD.get(index.getSettings());
        String keyField = key instanceof FieldAttribute field ? field.fieldName().string() : null;
        if (routingField.isEmpty()) {
            failures.add(
                fail(join, "JOIN requires [{}] to set [{}] to the join key [{}]", name, IndexSettings.ROUTING_FIELD.getKey(), key.name())
            );
        } else if (routingField.equals(keyField) == false) {
            failures.add(
                fail(
                    join,
                    "JOIN requires the join key [{}] to be the routing field of [{}], found [{}: {}]",
                    key.name(),
                    name,
                    IndexSettings.ROUTING_FIELD.getKey(),
                    routingField
                )
            );
        }
    }

    /**
     * Both indices must map a routing value to the same shard number. On indices that route by the hash modulo the shard
     * count, an equal shard count is enough; older indices route through {@code routing_num_shards}, which also has to match.
     */
    private static boolean checkSameRoutingFunction(Join join, IndexMetadata left, IndexMetadata right, List<Failure> failures) {
        if (left.getNumberOfShards() != right.getNumberOfShards()) {
            failures.add(
                fail(
                    join,
                    "JOIN requires the same number of shards on both sides, found [{}] for [{}] and [{}] for [{}]",
                    left.getNumberOfShards(),
                    left.getIndex().getName(),
                    right.getNumberOfShards(),
                    right.getIndex().getName()
                )
            );
            return false;
        }
        boolean leftModulo = IndexRouting.shouldUseShardCountModRouting(left.getCreationVersion());
        boolean rightModulo = IndexRouting.shouldUseShardCountModRouting(right.getCreationVersion());
        if (leftModulo != rightModulo
            || (leftModulo == false
                && (left.getRoutingNumShards() != right.getRoutingNumShards() || left.getRoutingFactor() != right.getRoutingFactor()))) {
            failures.add(
                fail(
                    join,
                    "JOIN requires [{}] and [{}] to route documents to shards the same way",
                    left.getIndex().getName(),
                    right.getIndex().getName()
                )
            );
            return false;
        }
        return true;
    }

    /**
     * For every shard number, an active copy of the left shard and of the right shard must be on a common node.
     */
    private static void checkPlacement(Join join, IndexMetadata left, IndexMetadata right, ProjectState project, List<Failure> failures) {
        IndexRoutingTable leftTable = project.routingTable().index(left.getIndex());
        IndexRoutingTable rightTable = project.routingTable().index(right.getIndex());
        if (leftTable == null || rightTable == null) {
            failures.add(
                fail(join, "JOIN could not find the shards of [{}] and [{}]", left.getIndex().getName(), right.getIndex().getName())
            );
            return;
        }
        List<String> misplaced = new ArrayList<>();
        for (int shard = 0; shard < left.getNumberOfShards(); shard++) {
            Set<String> leftNodes = activeNodes(leftTable, shard);
            Set<String> rightNodes = activeNodes(rightTable, shard);
            if (leftNodes.stream().noneMatch(rightNodes::contains)) {
                misplaced.add("shard [" + shard + "]: " + nodeNames(project, leftNodes) + " and " + nodeNames(project, rightNodes));
            }
        }
        if (misplaced.isEmpty() == false) {
            failures.add(
                fail(
                    join,
                    "JOIN requires shard i of [{}] and [{}] on the same node, but {}",
                    left.getIndex().getName(),
                    right.getIndex().getName(),
                    String.join(", ", misplaced)
                )
            );
        }
    }

    private static Set<String> activeNodes(IndexRoutingTable table, int shard) {
        Set<String> nodes = new TreeSet<>();
        table.shard(shard).allShards().filter(s -> s.active() && s.isSearchable()).map(ShardRouting::currentNodeId).forEach(nodes::add);
        return nodes;
    }

    private static Set<String> nodeNames(ProjectState project, Set<String> nodeIds) {
        Set<String> names = new TreeSet<>();
        for (String id : nodeIds) {
            DiscoveryNode node = project.cluster().nodes().get(id);
            names.add(node == null ? id : node.getName());
        }
        return names;
    }
}
