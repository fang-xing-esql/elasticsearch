# Co-located join: making it flexible and robust

Status: improvement 1 (routed lookup) implemented on branch `colocated-join-route-rhs`
(2026-11-05), see section 9; the others are proposals. Companion to `colocated_join_design.md`
and `routing_field_design.md`.

## 1. Summary

Today the co-located join treats data layout as a correctness requirement. `SelectJoinStrategy`
rejects the query unless both indices:

- route by the join key (`index.routing_field`);
- have the same shard count and routing function, with no resharding in progress;
- have shard i of both indices on the same node at planning time.

The first condition is cheap to meet and the server now enforces it. The others are not: shard
counts are chosen per index, the balancer moves shards one at a time and does not keep shards of
different indices together, and resharding one index breaks the pair. The query also depends on
a placement snapshot taken on the coordinator, which a relocation can invalidate before the data
nodes run it.

This doc proposes moving the requirement down to the cheap part and treating everything else as
a speed-up:

| # | Improvement | Requirement it removes | Effort |
|---|---|---|---|
| 1 | Routed lookup | left routing, equal shard counts, placement | medium |
| 2 | Divisible shard counts | equal shard counts (keeps full locality) | small |
| 3 | Allocator co-location groups | placement left to luck | large |
| 4 | Join strategy selection | "reject when not co-located" | medium, grows with strategies |
| 5 | Execution-time hardening | trusting the coordinator's snapshot | small |

Recommended order: 1, 2, 5, then 3 only if profiles show remote lookups dominate. A shuffle join
(section 6.4) is the general long-term answer and a separate project.

Scope: `LOOKUP JOIN` is out of scope and does not change. Where these improvements reuse code
shared with it (the lookup service and its transport action), they add co-located-join-only
paths and leave `LOOKUP JOIN`'s behaviour, validation and requirements as they are.

## 2. Background

A co-located join runs on each data node that holds a left shard. The left shard's pages go
through `LookupFromIndexOperator` with `innerJoin=true`, which looks up the right side through
`lookupAsyncOnLocalShard` against the partner shard on the same node and assembles the result
with `RightChunkedInnerJoin`.

Shard selection (`IndexRouting`, `RoutingFunction`):

- indices created on or after `IndexVersions.MOD_ROUTING_FUNCTION`:
  `shard = floorMod(hash, number_of_shards)`;
- older indices: `shard = floorMod(hash, routing_num_shards) / routing_factor`, where
  `routing_factor = routing_num_shards / number_of_shards`.

`hash` is `murmur3(_routing)`, and with `index.routing_field` `_routing` is the canonical join
key value. So any node can compute, from cluster state alone, which shard of an index holds a
given key.

## 3. Improvement 1: routed lookup

### 3.1 Idea

Only the right side has to be routed by the join key. For each left row, the shard that holds
its matches is `IndexRouting.fromIndexMetadata(right).getShard(_, key)`. The left side can be
any relation: any routing, any shard count, any placement, even the output of `STATS`.

Execution on each data node:

1. Compute the canonical routing value of each left key and its target right shard.
2. Split the page into one sub-page per target shard (keeping the original positions).
3. For a target shard with a copy on this node, look up locally (`lookupAsyncOnLocalShard`, as
   today). Otherwise send the sub-page to a node holding a copy, over the transport action that
   `LOOKUP JOIN` already uses.
4. Assemble each sub-page's result with `RightChunkedInnerJoin` and emit it.

When every target is local this is the current co-located join plus one hash per row.

### 3.2 What it removes

- **Shard count and routing function equality** between the two sides.
- **Placement**: a misplaced pair becomes a remote lookup instead of a rejected query.
- **Left side shape**: the left side no longer has to be a scan of one routed index, so
  `FROM logs-*`, data streams whose backing indices have different shard counts, and pipeline
  breakers before the join all work.
- **Relocation during the query**: a partner shard that moves after planning becomes a remote
  lookup (or a retry against another copy) instead of a failure.

### 3.3 Design points

- **Canonical key on the lookup side.** The right index's routing value comes from
  `RoutingField` canonicalization of the key type (keyword text, decimal integer, formatted IP).
  The left key must be canonicalized the same way, from the ES|QL block value rather than from
  `_source`. Add a block-value entry point next to the `XContentParser` one so both share the
  code.
- **Ordering.** The inner join does not promise left order across sub-pages, so each sub-page is
  assembled independently and `RightChunkedInnerJoin`'s non-decreasing positions rule holds
  within a sub-page. A left join variant would have to merge sub-page results back into left
  order, or emit unmatched rows per sub-page.
- **Batching.** Small sub-pages to many remote shards are expensive. Buffer per target shard up
  to a size or latency bound before sending; fall back to the current page-at-a-time path when
  everything is local.
- **Copy selection.** Prefer a local copy, then the same copy for all requests to a shard during
  the query (stable reads), as search does with preference.
- **Right-side filters.** Pushed-down right filters and projections travel with the lookup
  request exactly as they do today.
- **Security and CCS.** Remote lookups run under the same index privileges as `LOOKUP JOIN`. The
  right index stays local-cluster only, as now.

### 3.4 Cost

Network traffic for every row whose partner shard is not local, and a hash per left row.
`PROFILE` should report local and remote lookup counts per driver so users can see when
co-location is lost.

### 3.5 Planner change

`SelectJoinStrategy` keeps only the right-side checks: one local index, routing field equal to
the right join key, no routing partitions, no `routing_path`. The left-side and pairwise checks
move into the choice between "all local" and "routed" (section 6), not into rejections.

## 4. Improvement 2: divisible shard counts

### 4.1 Idea

Two indices whose shard counts divide one another still line up shard for shard, so the join can
stay fully local without equal shard counts. Let the left index have N shards and the right
index M, with M dividing N:

- **Modulo routing** (new indices): `hash mod M = (hash mod N) mod M`, so left shard j holds only
  keys of right shard `j mod M`.
- **Legacy routing** with the same `routing_num_shards` R: `(hash mod R) / (R/M) =
  ((hash mod R) / (R/N)) / (N/M)`, so left shard j holds only keys of right shard `j / (N/M)`.

If instead N divides M, each left shard needs the right shards that map back to it: M/N partner
shards, all reachable with improvement 1's routing, or all local if placed together.

Both sides must use the same routing family (both modulo, or both legacy with equal
`routing_num_shards`). For legacy indices the default `routing_num_shards` is `numShards * 2^x`
up to 1024 (`MetadataCreateIndexService.calculateNumRoutingShards`), so 3, 6, 12 and 24 shards
all default to 768 and 1, 2, 4 and 8 shards to 1024: counts differing by a power of two already
match without configuration.

### 4.2 What it gives

- A large fact index and a small dimension index with different shard counts, both fully local.
  For example a 12-shard `events` with a 3-shard `sessions`.
- **Resharding one index at a time.** Split and in-place resharding multiply the shard count, so
  divisibility survives. The pair never has to be resharded together.

### 4.3 Change

- `SelectJoinStrategy.checkSameRoutingFunction` accepts "same family, one count divides the
  other" (plus equal `routing_num_shards` for legacy).
- The placement check pairs left shard j with its partner shard by the formula above.
- The runtime receives the partner shard id per left shard instead of assuming the same number.

### 4.4 Resharding in progress

Today the planner rejects an index with `IndexReshardingMetadata`. With improvement 1, a lookup
can route by the resharding state the way search does (source or target shard, depending on the
split's progress), so resharding the right index no longer blocks the join.

## 5. Improvement 3: allocator co-location groups

### 5.1 Idea

Make "shard i of these indices on one node" an allocation constraint instead of a result of
cloning from the same seed.

- A setting such as `index.colocate_with: <leader index>` (Citus calls this `colocate_with`).
- An `AllocationDecider` that allows shard i of a member only on nodes holding a copy of the
  leader's partner shard (shard i, or the improvement 2 partner).
- The balancer and the desired-balance computer treat a group's partner shards as one weighted
  unit, so rebalancing moves them together.

### 5.2 Replicas

The join needs, for each left shard, *some* node holding a copy of both it and its partner, not
the primaries together. Planning against that weaker condition is much more robust, and copy
selection at search time picks that node. Replicas also give the allocator more ways to satisfy
the group.

### 5.3 Cost and risk

This is the expensive item: grouped movement changes balancing, disk-threshold handling (a group
needs room for all members on one node), recovery throttling and shutdown and drain logic. With
improvement 1 in place, losing co-location costs only network, so this can come late or never.

## 6. Improvement 4: join strategy selection

### 6.1 Idea

Keep the logical `Join[INNER]` and let `SelectJoinStrategy` choose how to execute it instead of
choosing between co-located and rejected. In order of preference:

| Strategy | Requires | Cost |
|---|---|---|
| Co-located | right routed by key, layouts aligned (section 4), partner shards on the same nodes | no network |
| Routed lookup (section 3) | right routed by key | network for rows whose partner is not local |
| Broadcast | small right side, e.g. `auto_expand_replicas: 0-all` | every node holds every right shard, so trivially local |
| Shuffle (section 6.4) | nothing | repartitions data across nodes |

### 6.2 Choosing

- Co-located and routed lookup run the same operator; the difference is per shard and can be
  decided at execution time (section 7), so the planner does not need to choose between them.
- Broadcast versus routed lookup depends on the right side's size, which index stats provide.
- Rejection remains only when the user asks for strict co-location, for example a query setting
  that forbids remote lookups, so a slow plan never surprises them.

### 6.3 Explainability

`EXPLAIN` or `PROFILE` should name the chosen strategy and, for a non-local one, the reason (for
example "shard [1]: [n1] and [n2]"), reusing today's rejection messages.

### 6.4 Shuffle join

Repartition both sides by hash of the key across data nodes and join each partition locally.
This needs data-node-to-data-node exchanges in the ES|QL compute engine, which today only
exchanges between data nodes and the coordinator. It is the general answer for unrouted data and
a separate project.

## 7. Improvement 5: execution-time hardening

- **Verify locality on the data node.** The coordinator's placement check is a snapshot. The data
  node should confirm that the partner shard is actually local, and if not, take the routed
  lookup path rather than trusting the plan.
- **Right side as a data stream.** Routed lookup needs every backing index of a right-side data
  stream routed by the same field, which `index.routing_field` in the index template provides;
  the lookup then targets the backing index and shard that hold the key, or all backing indices
  when the stream has no time filter.

## 8. Open questions

- How much buffering per target shard is worth the added latency?
- Is a left outer variant of routed lookup needed, given its ordering cost (section 3.3)?
- Does improvement 2's partner mapping need a transport version for the plan change, or can the
  data node compute it from cluster state?

## 9. Implementation status: improvement 1 (2026-11-05)

Only the right index has to be routed by the join key. `FROM events_unrouted | JOIN sessions ON
session_id` runs whatever the left index's routing, shard count or placement.

What changed:

- **`SelectJoinStrategy`** keeps the shape checks, a single local right index routed by `_routing`
  alone with `index.routing_field` equal to the right key, and adds "left side reads local
  indices only". Equal shard counts, the routing function check, the left routing field and the
  placement check are gone. The left side can read several indices.
- **`PartnerShards`** (new, `esql/enrich`) splits each left page by the right shard that holds
  each row's matches. Aligned path: the page comes from a left shard whose index is routed by the
  left key the same way as the right index (`ColocatedRouting.aligned`), so the whole page goes to
  the shard with the same number, with no hashing. Routed path: each key is made canonical with the
  same `RoutingField` helpers the server uses for documents (`canonicalIntegral`, `canonicalIp`,
  keyword text) and hashed with the right index's `IndexRouting`. Null keys are dropped (an inner
  join can't match them); multi-valued keys join the first group, so the lookup reports them as for
  a lookup join.
- **`LookupFromIndexOperator`**: the co-located hook is now a `PartnerShards` instead of a left
  shard number. A page with one target shard is looked up as before; a split page is looked up
  per group in parallel (`RefCountingListener`) and the groups' joins are chained
  (`OngoingJoin.next`). `LOOKUP JOIN` never sets the hook and runs as before.
- **`AbstractLookupService.lookupAsyncOnShard`** replaces `lookupAsyncOnLocalShard`: it targets one
  shard by number, prefers the copy on this node and otherwise goes to another copy through the same
  copy-retry loop as `lookupAsync`. The loop now takes the shard resolution as a function; for
  `LOOKUP JOIN` that is still `getShardIterator`, single-shard check included.
- **`DataNodeRequestSender`**: the partner index is a preference, not a restriction. For a left
  shard aligned with the right index (`ColocatedRouting.sameRouting`), nodes holding the partner
  shard come first; no node is ruled out, so a misplaced pair is joined with remote lookups.
- **`LocalExecutionPlanner.planColocatedJoin`** builds the `PartnerShards` from the data node's
  cluster state; the `_doc` column is only used for the aligned path.

Tests: `PartnerShardsTests` (keyword, integral and ip keys route like documents of the right
index, null and multi-valued keys, single-shard pages, aligned pages); `SelectJoinStrategyTests`
(misplaced shards, other shard counts, an unrouted left index, a left index routed by another
field and several left indices are accepted; remote left indices and a right index routed by
another field are rejected); `ColocatedJoinIT` (`events_unrouted`: two shards, routed by `_id`;
plain join, filters and stats, a pattern mixing aligned and unrouted indices, the profile, a
one-shard right index, misplaced shards now joined, and an unrouted right index rejected).

Not done: per-target-shard buffering across pages, local/remote lookup counters in the profile
(the operator status is shared with `LOOKUP JOIN`), pipeline breakers on the left, resharding of
the right index.
