# Co-located JOIN in ES|QL: POC design

Status: draft, POC scope. Branch: `on-week`.

## 1. Goal

Make this query execute as a co-located join on the data nodes:

```
FROM events | JOIN sessions ON session_id
```

and then, in that order:

```
FROM events | JOIN sessions ON session_id | WHERE country == "DE"
FROM events | JOIN sessions ON session_id | STATS n = COUNT(*), spent = SUM(duration_ms) BY country, plan
```

"Co-located" means: both indices have the same number of primaries, every document
is routed by the join key (`_routing = session_id`, set by the `session-routing`
final pipeline), and shard `i` of `events` and shard `i` of `sessions` live on the
same node. Under that layout a key can only match inside the same shard number, so
each data node can join its local `events` shards against its local `sessions`
shards and ship only join results (or partial aggregates) to the coordinator. No
data moves between data nodes.

The schema and data are produced by `selfjoin_setup.sh` (seed index cloned into
`events` and `sessions`, 3 primaries, 0 replicas, `total_shards_per_node: 1`,
`rebalance.enable: none`).

Non-goals for the POC: LEFT/RIGHT/FULL variants, `ON` boolean expressions, more
than one join key, remote clusters, replicas, resharding, any fallback to a
non-co-located execution, and comprehensive tests.

## 2. Semantics

- `JOIN <index> ON <field>` is an INNER equi-join. `ON` takes exactly one
  unqualified field name that must exist on both sides with a compatible type
  (POC: `keyword` on both sides, so the routing hash of the string form is the
  same on both sides).
- Output columns: all left columns, then all right columns except the join key.
  On a non-key name clash the right column shadows the left one, which is the
  existing `LOOKUP JOIN` rule from `Join.computeOutputExpressions` and
  `NamedExpressions.mergeOutputExpressions`.
- Rows: one output row per matching (left row, right row) pair. One-to-many and
  many-to-many both work, because the runtime is the `LOOKUP JOIN` operator chain
  whose `RightChunkedLeftJoin` emits a left row once per matching right row.
- Commands after the join behave as for any other command. `WHERE` and `STATS BY`
  are the two targets. An implicit `LIMIT 1000` is always present.

## 3. Preconditions and how they are checked

| precondition | where it is known | check |
|---|---|---|
| both sides are one concrete local index each (no wildcard, no remote, no selector) | parser + analyzer | `LogicalPlanBuilder` reuses the LOOKUP JOIN pattern checks; `ColocatedJoin.postAnalysisVerification` requires each side to resolve to one `EsRelation` with one concrete index |
| same number of primary shards | `EsRelation.indexProperties()` carries `IndexProperties.numberOfShards` from `_field_caps` (PR #156022) | `ColocatedJoin.postAnalysisVerification` |
| join key types compatible | analyzer | existing `Join.postAnalysisVerification` key type check |
| both indices routed by the join key | not available to the planner | **trusted** in the POC. Follow-up: expose a declared routing field through `_field_caps` or read `IndexMetadata` in `ComputeService` and verify |
| shard `i` of both indices has a common node | coordinator, `DataNodeRequestSender` | for every left shard, pick a node that also holds the right partner shard, fail the query if none |
| the partner shard is started on the data node | data node, at operator planning | guard before planning: `sessions[i]` is an active local `IndexShard` for every `events[i]` in the batch, else fail |

Failing loudly is the POC behavior. There is no fallback to a coordinator join.

## 4. Pipeline overview

```
parser      FROM events | JOIN sessions ON session_id
            -> UnresolvedRelation(events)  ColocatedJoin  UnresolvedRelation(sessions, STANDARD)

pre-analyze both relations are STANDARD, so both are resolved as main indices
            (PreAnalyzer.indexes, preAnalyzeMainIndices); no lookup-index path

analyzer    ResolveTable -> EsRelation(events), EsRelation(sessions), fresh attribute ids
            ResolveRefs  -> resolveColocatedJoin: leftKeys vs left output, rightKeys vs right output
            Verifier     -> ColocatedJoin.postAnalysisVerification (types, shard count, single index)

optimizer   ColocatedJoin is kept as is (no surrogate); right-only filters are pushed into the right side

Mapper      executesOn() == REMOTE  =>  FragmentExec( Limit( ColocatedJoin(events, sessions) ) )
(coord)     LimitExec <- ExchangeExec <- FragmentExec
            STATS: AggregateExec(FINAL) <- ExchangeExec <- FragmentExec( Aggregate( ColocatedJoin ) )

ComputeService.getIndices  -> {events} only: the right relation is treated like a lookup relation
DataNodeRequestSender      -> events shards, each sent to a node that also holds sessions[i]

LocalMapper EsSourceExec(events)  +  FragmentExec(right logical plan)  ->  ColocatedJoinExec
(data node) local physical optimizer: EsQueryExec(events), left-side filter pushdown,
            field extraction of the left key and left columns only; the right fragment stays logical

runtime     ColocatedJoinExec -> LookupFromIndexOperator, one lookup per left page,
(data node) target shard = sessions[i] for the page's events shard i, executed on the local node:
            EnrichQuerySourceOperator (term queries on session_id, plus pushed right filter)
            -> field extraction of right columns -> MergePositionsOperator -> RightChunkedLeftJoin
            -> FilterExec(IsNotNull(right key)) -> ProjectExec(join output)
```

The runtime is the existing `LOOKUP JOIN` chain pointed at a local, multi-shard
index. The only new runtime machinery is "choose the partner shard from the left
page and run the lookup locally". The right index is never scanned by the main
pipeline.

## 5. Changes by layer

All paths are relative to `x-pack/plugin/esql/`.

### 5.1 Grammar

Today `JOIN` is only a token inside lexer mode `JOIN_MODE`, which is entered by
`lookup` (and the dev-only `left`/`right`/`full`). A bare `| JOIN` lexes as
`UNKNOWN_CMD`.

`src/main/antlr/lexer/Join.g4`, default mode section, next to `JOIN_LOOKUP`:

```antlr
DEV_JOIN : 'join' {this.isDevVersion()}? -> pushMode(JOIN_MODE);
```

This enters `JOIN_MODE` directly, so `joinTarget` (an `UNQUOTED_SOURCE`) and
`ON` (which pops to `EXPRESSION_MODE`) work unchanged. The existing
`JOIN : 'join'` inside `JOIN_MODE` is a different mode and does not conflict.

`src/main/antlr/parser/Join.g4`:

```antlr
colocatedJoinCommand
    : DEV_JOIN joinTarget joinCondition
    ;
```

`src/main/antlr/EsqlBaseParser.g4`, in `processingCommand`:

```antlr
    | {this.isDevVersion()}? colocatedJoinCommand
```

Regenerate with `./gradlew :x-pack:plugin:esql:regen`. Do not hand-edit the
generated parser.

Capability: `EsqlCapabilities.Cap.COLOCATED_JOIN_V1(Build.current().isSnapshot())`,
checked in the plan builder like `JOIN_LOOKUP_V12` is today.

Syntax alternatives considered and rejected for the POC: `INNER JOIN` (needs one
more token and buys nothing yet) and `JOIN ... USING` (the `USING` token exists but
no rule uses it).

### 5.2 LogicalPlanBuilder

`src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`, new
`visitColocatedJoinCommand`, modeled on `visitJoinCommand` (lines 1217-1270):

- gate on `COLOCATED_JOIN_V1`;
- `visitIndexPattern` on the target, reject wildcards, remote clusters (including
  the `_coordinator:` prefix) and selectors, exactly as the lookup path does;
- right side: `new UnresolvedRelation(source, pattern, false, emptyList(), IndexMode.STANDARD, null)`.
  STANDARD, not LOOKUP, so the pre-analyzer resolves it as a main index;
- condition: `visitJoinCondition`, field-based path only. Reject the
  expression-based path (`joinExpressions` non-empty) and more than one field with
  a parsing error;
- result: `p -> new ColocatedJoin(source, p, right, joinInfo.joinFields())`.

### 5.3 Logical plan: `ColocatedJoin`

New `plan/logical/join/ColocatedJoin.java`, `extends Join`:

- constructor `(Source, LogicalPlan left, LogicalPlan right, List<Attribute> joinFields)`
  builds `JoinConfig(JoinTypes.INNER, joinFields, joinFields, null)` (same
  unresolved list on both sides, as `LookupJoin` does) and mode
  `ExecuteLocation.REMOTE`;
- it is **not** a `SurrogateLogicalPlan`. It must survive optimization so the
  mappers can recognize it;
- `computeOutputExpressions`: `Join.computeOutputExpressions` throws for anything
  but LEFT. Either extend that switch to accept INNER with the same body, or
  override in the subclass with the field-based LEFT logic (right output minus
  right keys, merged after the left output). `rightOutputFields()` then works
  unchanged and is what the physical join needs as its added columns;
- serialization: `ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "ColocatedJoin", ColocatedJoin::new)`,
  `writeTo` as `Join.writeTo`, constructor from stream restores mode REMOTE (in
  `Join` the mode is transient and reads back as ANY, which would be wrong here).
  Register in `plan/PlanWritables.logical()` next to `Join.ENTRY`. This is required
  because the whole join ships inside a `FragmentExec` to the data nodes;
- `postAnalysisVerification`: call `super` for the key type check, then:
  - each side must be an `EsRelation`, optionally under `Filter`/`Eval`/`Project`
    (walk down unary nodes), with `concreteIndices` of size one on the local
    cluster;
  - `numberOfShards` equal on both `IndexProperties`, and greater than zero (zero
    means an old node that did not send shard counts);
  - key type is `keyword` on both sides (POC);
- `postOptimizationVerification`: inherited `checkForRemoteJoinBlockers` already
  rejects pipeline breakers below a REMOTE join except `Limit`. Keep it.
- `telemetryLabel()`: "COLOCATED JOIN".

### 5.4 Pre-analysis and field names

- `PreAnalyzer.doPreAnalyze`: no change. The right relation is STANDARD, so it is
  added to `indexes` and resolved by `preAnalyzeMainIndices`, the same way a
  second subquery branch is.
- `session/FieldNameUtils`: today it special-cases `LookupJoin` (adds the keys to
  `joinRefs`, and the right pattern to `wildcardJoinIndices` when no KEEP follows).
  POC: if the plan contains a `ColocatedJoin`, return `IndexResolver.ALL_FIELDS`.
  Column pruning across the join is a follow-up.
- `EsqlSession.receiveLookupIndexResolution` and the lookup-index checks are not
  on this path at all.

### 5.5 Analyzer

`analysis/Analyzer.java`, `ResolveRefs.rule` (line 1515 area): add
`case ColocatedJoin j -> resolveColocatedJoin(j)` before the generic default.

`resolveColocatedJoin`:

```java
List<Attribute> leftKeys  = resolved(config.leftFields())  ? config.leftFields()
                           : resolveUsingColumns(config.leftFields(),  join.left().output(),  "left");
List<Attribute> rightKeys = resolved(config.rightFields()) ? config.rightFields()
                           : resolveUsingColumns(config.rightFields(), join.right().output(), "right");
return join.withConfig(new JoinConfig(JoinTypes.INNER, leftKeys, rightKeys, null));
```

`resolveUsingColumns` (line 2578) already produces the "column [x] in left/right
side of join" errors. Because `ResolveTable` creates fresh attribute ids per
relation, `session_id` on the left and on the right are distinct attributes even
though the name is the same, which is what `rightOutputFields()` relies on.

Rules to review for the right child being a STANDARD relation instead of LOOKUP:

- `analysis/rules/ResolveUnmapped.java` lines 320 and 432 treat the right child
  of a `Join` specially; make sure it does not assume lookup semantics for
  `ColocatedJoin` (POC: exclude `ColocatedJoin` from that special case).
- `DetermineUnmappedFieldsToKeep` recurses into the left side only for `Join`;
  acceptable for the POC.
- `canSurfaceFromSource` recurses left only for LEFT joins; `ColocatedJoin` is
  INNER so it falls through to the default. Acceptable.

### 5.6 Logical optimizer

- `SubstituteSurrogatePlans`: nothing to do, `ColocatedJoin` is not a surrogate.
- `PushDownAndCombineFilters.pushDownPastJoin` (line 179): extend to
  `ColocatedJoin`. For an inner join a conjunct that references only left
  attributes goes to the left child, only right attributes goes to the right
  child, mixed conjuncts stay above the join. This is the stage 2 change that
  turns `| WHERE country == "DE"` into a term query inside the per-page lookup
  (see 5.10). Without it the filter still executes correctly above the join on
  the data node, so it is an optimization, not a requirement.
- `PushDownAndCombineLimits`: it duplicates a `Limit` into the left side of LEFT
  lookup joins. That is wrong for an inner join. Exclude `ColocatedJoin`.
- `PushDownJoinPastProject`, `PruneLeftJoinOnNullMatchingField`: LEFT-specific,
  verify they skip INNER.

### 5.7 Coordinator physical planning (`Mapper`)

`planner/mapper/Mapper.java`, `mapBinary` (line 265 onward): the LEFT-only check
at line 267 runs before the `REMOTE` check at line 271. Add an explicit
`ColocatedJoin` case first that returns `new FragmentExec(bp)`. The rest of the
machinery is unchanged:

- a following `WHERE` is a non-pipeline-breaker and is folded into the fragment;
- a following `STATS` goes through `addExchangeForFragment`, producing
  `AggregateExec(FINAL) <- ExchangeExec <- FragmentExec(Aggregate(ColocatedJoin))`;
  the data node runs the INITIAL aggregate over the join output;
- the implicit `Limit` produces `LimitExec <- ExchangeExec <- FragmentExec(Limit(ColocatedJoin))`;
- `ProjectAwayColumns` wraps the fragment's logical plan in a `Project` of the
  columns the coordinator needs. That projection sits at the top of the data-node
  plan, so `_doc` is still in the page layout at the join. 5.10 relies on that.

`PlannerUtils.reductionPlan` picks the first pipeline breaker in the fragment for
node-level reduction (Limit or intermediate Aggregate). It maps only that node with
`LocalMapper`, never the join, so it works unchanged.

`PhysicalVerifier` fails on a REMOTE node left in the coordinator plan; the join is
inside the fragment, so this passes.

### 5.8 Coordinator execution: indices, can_match, shard pairing

The right relation must be invisible to the request machinery, the same way a
LOOKUP relation is today. The data-node request carries only `events` shards; the
`sessions` shard is opened by the lookup service on the data node.

- `PlannerUtils.forEachRelation` (line 323) skips `EsRelation`s with
  `IndexMode.LOOKUP`. Extend it to skip the right subtree of a `ColocatedJoin`.
  `ComputeService.getIndices` (line 2339) then yields `{events}` and
  `startComputeOnDataNodes` resolves only `events` shards. (The `holder.set`
  last-relation bug in `getIndices` is left alone; it does not bite once the
  right relation is skipped.)
- `PlannerUtils.detectFilter` (used by `canMatchFilter`) ANDs every `Filter`
  directly above an `EsRelation` in the fragment into the can_match filter for
  all shards. After stage 2 pushes `country == "DE"` into the right side, that
  filter would be applied to `events` shards and skip them all. Skip the right
  subtree of a `ColocatedJoin` in `detectFilter`. Left-side filters still
  contribute, which is correct.

`plugin/DataNodeRequestSender`:

- `selectNodeRequests` (line 444) picks any remaining copy per shard. When the
  plan contains a `ColocatedJoin` (detect once from the `FragmentExec`), restrict
  each `TargetShard.remainingNodes` of `events[i]` to the nodes that hold an
  active copy of `sessions[i]`, read from `clusterService.state().routingTable()`
  the way `resolveShards` (line 584) already does. If the intersection is empty,
  fail the query with
  `co-located join: shard [N] of [events] and [sessions] have no common node`.
- `resolveShards` retries a failed shard on another copy. With the restriction
  applied there too, a retry can only land on a node that holds the partner, and
  the data-node guard (5.10) catches a copy that moved in between.
- The POC layout has zero replicas, so the choice is forced. The pairing code
  still has to exist so the failure mode is explicit.

Batching is a non-issue in this design: a batch holds only `events` shards, and
each lookup opens its own partner shard, so pairs cannot straddle batches.

### 5.9 Data-node planning (`LocalMapper`, physical node, optimizer rules)

`planner/mapper/LocalMapper.mapBinary` (line 149): add a `ColocatedJoin` case
before the LEFT-only lookup logic. It mirrors the lookup case: the left side is
mapped, the right side stays logical inside a `FragmentExec` so the lookup service
can compile its filter into the per-page query:

```java
if (binary instanceof ColocatedJoin cj) {
    return new ColocatedJoinExec(cj.source(), map(cj.left()), new FragmentExec(cj.right()),
        cj.config().leftFields(), cj.config().rightFields(), cj.rightOutputFields());
}
```

New `plan/physical/ColocatedJoinExec.java`, modeled on `LookupJoinExec`:

- fields: `left`, `right` (a `FragmentExec` holding the right logical plan),
  `leftFields`, `rightFields`, `addedFields` (right output minus key);
- `output()` is the logical join output;
- `references()` returns `leftFields` only, as `LookupJoinExec` does. Field
  extraction then happens only on the left side; the right columns are
  materialized by the lookup (`InsertFieldExtraction.missingAttributes`, comment
  at line 92, already describes this contract);
- not serialized (`writeTo` throws, like `EsQueryExec`). It only ever exists on
  the data node between `LocalMapper` and `LocalExecutionPlanner`.

`PlannerUtils.localPlan` (line 608) collects the right children of every
`LookupJoinExec` and leaves those `FragmentExec`s logical. Add `ColocatedJoinExec`
to that collection; otherwise the right fragment would be expanded into an
`EsSourceExec` and the physical verifier would reject a second source.

Local physical optimizer: no rule changes. `ReplaceSourceAttributes`,
`PushFiltersToSource` and `InsertFieldExtraction` act on the left `EsQueryExec`
exactly as they do under a `LookupJoinExec`.

### 5.10 Data-node runtime (`LocalExecutionPlanner`, lookup service)

`LocalExecutionPlanner.planLookupJoin` (line 1788) already builds the operator we
need; it only refuses non-LOOKUP indices. Add `planColocatedJoin`, a copy with
these differences:

- no `IndexMode.LOOKUP` checks; the index name comes from the single
  `indexProperties` entry of the right `EsRelation` found through
  `findEsRelation(join.right())`;
- `loadFields` is `addedFields` **plus the right key attribute**, so a non-match
  can be detected after the left join;
- `useStreamingOperator = false` (the streaming variant opens a bidirectional
  exchange to a remote node, which is pointless locally);
- a `PartnerShardResolver` is passed to the operator factory: given the shard
  index of a left page, return `new ShardId(sessionsIndex, shardNumber)`. The
  shard index to `ShardId` mapping is the planner's `shardContexts` list
  (`EsPhysicalOperationProviders`), so the resolver is built at planning time;
- above the operator, `planColocatedJoin` appends an `IsNotNull(rightKey)` filter
  and a projection to `join.output()`. Equivalent: let `LocalMapper` emit
  `ProjectExec(FilterExec(IsNotNull(rightKey), ColocatedJoinExec))` and keep the
  planner change smaller. Either is fine; the mapper variant keeps the inner
  semantics visible in the plan.

**Guard.** While building the resolver, check for every left shard in the batch
that `indicesService.indexServiceSafe(sessionsIndex).getShard(i)` exists and its
routing entry is started. Fail planning with a message naming the missing
partner. This is the runtime check of the co-location invariant.

`LookupFromIndexOperator` (`enrich/LookupFromIndexOperator.java`):

- the factory gains an optional `PartnerShardResolver`;
- in `performAsync` (line 212), when the resolver is set, read the shard index
  from the page's `_doc` block (`DocVector.shards()`, the `_doc` column is still
  in the layout per 5.7), resolve the partner `ShardId`, and call the new
  `lookupAsync(request, ShardId, parentTask, listener)` instead of the index-name
  overload. Pages from `LuceneSourceOperator` and `LuceneTopNSourceOperator` come
  from one slice and therefore one shard; assert that and, if it ever fails, split
  the page by shard before looking up.

`AbstractLookupService` (`enrich/AbstractLookupService.java`):

- new `lookupAsync(R request, ShardId targetShard, CancellableTask, ActionListener)`:
  `T transportRequest = transportRequest(request, targetShard);
  sendChildRequest(parentTask, listener, clusterService.localNode(), transportRequest);`.
  It bypasses `getShardIterator` (line 296), which is the only place that refuses
  a multi-shard index. Sending to the local node reuses the child task, the
  transport handler, warnings and profile plumbing unchanged; a direct `doLookup`
  call is possible later if the hop measurably matters;
- `doLookup` (line 445) needs nothing: `LookupShardContextFactory.fromSearchService`
  (line 957) builds a search context from any `ShardId`, and nothing in the
  `enrich` package checks `IndexMode.LOOKUP`. The `SplitShardCountSummary.IRRELEVANT`
  it passes is acceptable for the POC (no resharding).
- `LookupFromIndexService.queryList` (line 191) maps `rightPreJoinPlan` with
  `LocalMapper` to extract a right-only filter and builds term queries on the key
  through `termQueryList`. With the stage 2 pushdown the right `FragmentExec` holds
  `Filter(country == "DE", EsRelation(sessions))`, so the German-session filter
  becomes part of the per-page query against `sessions[i]`. This is the same
  mechanism `LOOKUP JOIN` uses for right-side filters today.

Per left page the work is: one term-query batch on the partner shard,
`EnrichQuerySourceOperator` reading the matches, extraction of the right columns,
`MergePositionsOperator`, then `RightChunkedLeftJoin` replicating left rows per
match. Memory is bounded per page by the lookup service's local circuit breaker.
Nothing is materialized across pages.

### 5.11 `STATS BY` after the join (stage 3)

No join-specific work is expected. The coordinator `Mapper` splits the
`Aggregate` into INITIAL (inside the fragment, above the join) and FINAL (above the
exchange). `LocalMapper` maps the fragment's `Aggregate` to `AggregateExec(INITIAL)`
over `ColocatedJoinExec`. The local plan is
`AggregateExec(INITIAL) <- ProjectExec <- FilterExec <- ColocatedJoinExec <- FieldExtractExec <- EsQueryExec(events)`,
an ordinary single-source pipeline, which is also exactly the shape of
`LOOKUP JOIN ... | STATS` today. Node-level reduction sees the `Aggregate` as the
first pipeline breaker and reduces as usual. Things to verify rather than build:
grouping on a right-side column (`country`, `plan`) and aggregating a left-side
column (`duration_ms`) both resolve to attributes that the join emits.

## 6. Worked example

```
FROM events | JOIN sessions ON session_id | WHERE country == "DE" | STATS n = COUNT(*) BY plan
```

Logical, after analysis:

```
Limit(1000)
  Aggregate(n = COUNT(*) BY plan)
    Filter(country == "DE")
      ColocatedJoin(INNER, [events.session_id] = [sessions.session_id], mode REMOTE)
        EsRelation(events, STANDARD, shards 3)
        EsRelation(sessions, STANDARD, shards 3)
```

After the logical optimizer (filter pushed into the right side):

```
Limit(1000)
  Aggregate(... BY plan)
    ColocatedJoin
      EsRelation(events)
      Filter(country == "DE")
        EsRelation(sessions)
```

Coordinator physical plan:

```
LimitExec(1000)
  AggregateExec(FINAL, n BY plan)
    ExchangeExec
      FragmentExec( Project(Aggregate(ColocatedJoin(EsRelation(events), Filter(EsRelation(sessions))))) )
```

Data-node request for runTask-0: shard `[events][1]` only, chosen because
runTask-0 also holds `[sessions][1]`. Local plan after `LocalMapper` and the local
optimizer:

```
AggregateExec(INITIAL, n BY plan)
  ProjectExec(join output)
    FilterExec(IsNotNull(session_id#7))
      ColocatedJoinExec(leftFields=[session_id#1], rightFields=[session_id#7],
                        added=[started_at, user_id, device, country, referrer, plan],
                        right=FragmentExec(Filter(country == "DE", EsRelation(sessions))))
        FieldExtractExec([session_id#1])
          EsQueryExec(events, contexts {events[1]})
```

Runtime on runTask-0: the `events[1]` scan emits a page with the rows of `s-100`
and `s-400`. The operator reads shard index 0 from `_doc`, resolves
`[sessions][1]`, and runs a lookup on the local node with the query
`session_id IN (s-100, s-400) AND country == "DE"` against that shard. Both
sessions are German, so two right rows come back; `RightChunkedLeftJoin` attaches
them to the six left rows, the `IsNotNull` filter drops nothing, and the INITIAL
aggregate counts by `plan`. A session row with another country would simply not
be returned and its events would be dropped by the filter. Each node sends
intermediate aggregates to the coordinator, which finalizes `n BY plan`.

## 7. Implementation order

1. Grammar + capability + `LogicalPlanBuilder` + `ColocatedJoin` + `PlanWritables`.
   Checkpoint: the bare join parses and the analyzer resolves it (an
   `AnalyzerTests`-style unit test is enough).
2. `ResolveRefs` case, `postAnalysisVerification`, `FieldNameUtils` ALL_FIELDS.
3. `Mapper` case, `forEachRelation` and `detectFilter` skipping the right subtree.
   Checkpoint: the data node receives a `FragmentExec` with the join and only
   `events` shards. It will fail in `LocalMapper`.
4. `LocalMapper` case, `ColocatedJoinExec`, `localPlan` skip of the right
   fragment, `planColocatedJoin`, `PartnerShardResolver`, `lookupAsync(ShardId)`.
   Checkpoint: bare join returns rows against the 3-node cluster from
   `selfjoin_setup.sh`. Expected: 13 rows (every event whose session has a row in
   `sessions`; `s-500` has none). Check `profile` output: one
   `LuceneSourceOperator` over `events` per node and `LookupOperator` child tasks
   on the same node, none remote.
5. `DataNodeRequestSender` pairing and failure path; the data-node guard.
6. Stage 2: `PushDownAndCombineFilters` extension; check `PushDownAndCombineLimits`.
   Expected for `WHERE country == "DE"`: 8 rows (`s-100`, `s-300`, `s-400`).
7. Stage 3: run the `STATS BY` query; fix whatever surfaces.

Verification is manual against the cluster for the POC: the three queries from
section 1, compared with the equivalent `IN` subquery results already shown to be
correct on this data set. The negative case `JOIN sessions ON user_id` cannot be
rejected in the POC (see 8) and will return only the pairs that happen to share a
shard; record its output as the motivating example for the routing-field check.

## 8. Known gaps and risks

- **Routing field is not verified.** The planner cannot see that `session_id` is
  the routing field. `JOIN sessions ON user_id` runs and returns partial results.
  Needs a declared routing field surfaced through `_field_caps` (next to
  `numberOfShards`) or an `IndexMetadata` check in `ComputeService`. First
  follow-up after the POC works.
- **Per-page lookup cost.** Each left page issues a term-query batch against the
  partner shard, which is the `LOOKUP JOIN` cost profile. Fine for a
  dimension-sized right side; a large right side with high match rates would
  want a build-once hash join (not available today: there is no streaming
  `HashJoinOperator`, and `HashJoinExec` with `BlockHashRowInTableLookup` throws
  `found a duplicate row` on duplicate keys, so it cannot serve one-to-many).
- **All right columns loaded** per lookup (`FieldNameUtils` returns all fields).
  Cost only. Follow-up: push the coordinator's projection into `addedFields`.
- **`_doc` dependency.** The partner shard is derived from the page's `_doc`
  block. If a future rule projects `_doc` away before the join, the operator needs
  an explicit shard-id column instead. An assertion at planning time should check
  `_doc` is in the layout.
- **Replicas** make pairing a real choice and expose the relocation race (a copy
  moving between the sender's decision and the lookup). The POC layout has none;
  the data-node guard turns a stale decision into a hard failure.
- **`LIMIT` pushdown** into join sides is invalid for an inner join; must be
  excluded explicitly.
- **Union types and unmapped fields** rules were written with LOOKUP right sides
  in mind; keep `ColocatedJoin` out of their special cases until exercised.
- **No fallback.** Any unmet precondition fails the query.

## 9. Follow-ups after the POC

1. Declared routing field and planner verification (closes the correctness hole).
2. Column pruning through the join.
3. Build-once hash join for large right sides, keyed per shard pair.
4. Pairing-aware copy selection with replicas, plus retry semantics.
5. A node-level "partition group" concept so the allocator keeps pairs together,
   replacing the clone-from-seed trick in `selfjoin_setup.sh`.
6. LEFT variant (drop the `IsNotNull` filter and make `addedFields` nullable),
   `ON` expressions (the lookup path already supports them via
   `ExpressionQueryList`), multiple keys.

## 10. Implementation status (2026-10-31)

Implemented on `on-week` per sections 5.1 to 5.10 with the lookup-operator runtime; verified on a
3-node `./gradlew run --nodes=3` cluster loaded by `selfjoin_setup.sh`.

Working:

- `FROM events | JOIN sessions ON session_id` returns the 13 matched rows (`s-500` has no session row,
  `s-700` has no events).
- `| WHERE country == "DE"` (8 rows), mixed left/right conjuncts, a filter before the join, `LIMIT`,
  `STATS ... BY country, plan`, `COUNT_DISTINCT` over both sides.
- Profile shows, per data node: `LuceneSourceOperator(events)` -> `LookupOperator(sessions, colocated)`
  -> `FilterOperator` -> partial `HashAggregationOperator` -> exchange. Only aggregates cross the wire.
- Rejections: wildcard or remote right pattern (parser), unknown key on either side (analyzer),
  different shard counts (analyzer: `JOIN requires the same number of shards on both sides`).

Deviations from the design text:

- `FeatureMetric` needed a `COLOCATED_JOIN` entry: every logical command must be mapped for telemetry.
- `DataNodeRequestSender.startComputeOnDataNodes` now catches exceptions from the dispatch step and fails
  the compute through the listener. Before, an exception there completed the listener twice and tripped an
  assertion that killed the coordinator node. Found while debugging an immutable-list bug in the partner
  node restriction, kept because the failure mode is otherwise fatal.
- `EsqlNodeSubclassTests.joinTypeFor` maps `ColocatedJoin` to INNER.
- `ReplaceFieldWithConstantOrNull` retains the right-side fields of a `ColocatedJoin`, as it does for lookup
  indices; otherwise the data node nullifies them because its own shard stats do not know them.

Still open, unchanged from section 8: routing-field verification, column pruning, replicas, batching.
