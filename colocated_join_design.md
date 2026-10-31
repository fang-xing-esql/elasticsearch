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
is routed by the join key (`index.routing_field: session_id`, see
`routing_field_design.md`), and shard `i` of `events` and shard `i` of `sessions` live on the
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
  (`keyword`, `long`, `integer` or `ip`, the types a routing field can have; the server
  routes a canonical form of the value, so equal keys share a shard number).
- Output columns: all left columns, then all right columns except the join key.
  On a non-key name clash the right column shadows the left one, which is the
  existing `LOOKUP JOIN` rule from `Join.computeOutputExpressions` and
  `NamedExpressions.mergeOutputExpressions`.
- Rows: one output row per matching (left row, right row) pair. One-to-many and
  many-to-many both work, because the runtime is the `LOOKUP JOIN` operator chain
  with an inner-join assembly step (`RightChunkedInnerJoin`) that emits a left row
  once per matching right row and drops left rows without a match.
- Commands after the join behave as for any other command. `WHERE` and `STATS BY`
  are the two targets. An implicit `LIMIT 1000` is always present.

## 3. Preconditions and how they are checked

The parser only builds an INNER `Join`; whether it can run co-located is decided on the
coordinator after logical optimization, where the cluster state is available
(`SelectJoinStrategy`, section 5.7a). Checks by stage:

| precondition | stage | check |
|---|---|---|
| one field, no wildcard, remote or selector in the target | parser | `LogicalPlanBuilder.visitInnerJoinCommand` |
| key resolves on both sides; types compatible | analyzer | `resolveUsingColumns`; `Join.postAnalysisVerification` key type check |
| right side is one local index; key is `keyword`, `long`, `integer` or `ip` | analyzer | `Join.verifyIndexJoin` (only for `Join.isIndexJoin()`) |
| each side is a scan with streaming commands only (no `LIMIT`, `STATS`, `SORT` below the join) | PreMapper | `SelectJoinStrategy.scan`: left `Filter`/`Eval`/`Project`, right `Filter`/`Project` over an `EsRelation` |
| each side resolves to one local index | PreMapper | `SelectJoinStrategy.singleLocalIndex` |
| both indices route by `_routing` hash alone | PreMapper | no `routing_partition_size`, no `routing_path` / dimensions, no resharding, `_routing` required in the mapping |
| same routing function | PreMapper | equal shard counts; for legacy (non-modulo) indices also equal `routing_num_shards` and routing factor |
| both indices routed by the join key | PreMapper | `SelectJoinStrategy.checkRoutingField`: `index.routing_field` of each index is the join key; the server enforces the setting on every write |
| shard `i` of both indices has a common node | PreMapper, then dispatch | `SelectJoinStrategy.checkPlacement` lists every misplaced pair; `DataNodeRequestSender.restrictToPartnerNodes` re-checks at dispatch |
| the partner shard is started on the data node | data node | `AbstractLookupService.lookupAsyncOnLocalShard` |

Every failed condition fails the query with all reasons listed. There is no other
runtime for joining two regular indices yet; the PreMapper step is where a
broadcast or shuffle join would become the fallback.

## 4. Pipeline overview

```
parser      FROM events | JOIN sessions ON session_id
            -> UnresolvedRelation(events)  Join[INNER]  UnresolvedRelation(sessions, STANDARD)

pre-analyze both relations are STANDARD, so both are resolved as main indices
            (PreAnalyzer.indexes, preAnalyzeMainIndices); no lookup-index path

analyzer    ResolveTable -> EsRelation(events), EsRelation(sessions), fresh attribute ids
            ResolveRefs  -> resolveIndexJoin: leftKeys vs left output, rightKeys vs right output
            Verifier     -> Join.postAnalysisVerification (types, keyword keys, single right index)

optimizer   Join[INNER] is kept as is; side-only filters are pushed into that side, limits are not

PreMapper   SelectJoinStrategy (coordinator, cluster state): Join[INNER] -> ColocatedJoin (REMOTE)
            when shape, routing and shard placement allow it, else the query fails

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
            -> field extraction of right columns
            -> RightChunkedInnerJoin (left rows filtered by match positions + right columns)
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
innerJoinCommand
    : DEV_JOIN joinTarget joinCondition
    ;
```

`src/main/antlr/EsqlBaseParser.g4`, in `processingCommand`:

```antlr
    | {this.isDevVersion()}? innerJoinCommand
```

Regenerate with `./gradlew :x-pack:plugin:esql:regen`. Do not hand-edit the
generated parser.

Capability: `EsqlCapabilities.Cap.COLOCATED_JOIN_V1(Build.current().isSnapshot())`,
checked in the plan builder like `JOIN_LOOKUP_V12` is today.

Syntax alternatives considered and rejected for the POC: `INNER JOIN` (needs one
more token and buys nothing yet) and `JOIN ... USING` (the `USING` token exists but
no rule uses it).

### 5.2 LogicalPlanBuilder

`parser/LogicalPlanBuilder.visitInnerJoinCommand`:

- gate on `COLOCATED_JOIN_V1`;
- `visitIndexPattern` on the target, reject wildcards, remote clusters and
  selectors, exactly as the lookup path does;
- right side: `new UnresolvedRelation(source, pattern, false, emptyList(), IndexMode.STANDARD, null)`.
  STANDARD, not LOOKUP, so the pre-analyzer resolves it as a main index;
- condition: field-based path only, exactly one field;
- result: a plain `Join` with `JoinConfig(INNER, [key], [key], null)` and
  `ExecuteLocation.ANY`. The parser states the semantics only; it does not know
  whether the indices are co-located, so it does not pick a strategy.

`Join.isIndexJoin()` identifies this node everywhere later: a plain `Join` (not a
subclass) of type INNER without ON expressions. No other command produces one.
`Join` is `TelemetryAware` and reports `JOIN` only for it; `FeatureMetric.JOIN`
matches it.

### 5.3 Logical plan: `ColocatedJoin`

`plan/logical/join/ColocatedJoin.java`, `extends Join`, is the execution strategy,
not the command:

- created only by `SelectJoinStrategy` through `new ColocatedJoin(Join)`, with mode
  `ExecuteLocation.REMOTE`;
- `isIndexJoin()` returns true, so rules keyed on the command keep applying to it
  (for example on the data node);
- output comes from `Join.computeOutputExpressions`, which now has an INNER branch
  with the USING semantics of the field-based LEFT branch (right output minus right
  keys, merged over the left output);
- serialized (`ENTRY` in `PlanWritables`), because the whole join ships inside a
  `FragmentExec` to the data nodes; the stream constructor restores mode REMOTE;
- no verification of its own: the semantic checks live on `Join`, the physical ones
  in `SelectJoinStrategy`.

### 5.4 Pre-analysis and field names

- `PreAnalyzer.doPreAnalyze`: no change. The right relation is STANDARD, so it is
  added to `indexes` and resolved by `preAnalyzeMainIndices`, the same way a
  second subquery branch is.
- `session/FieldNameUtils`: today it special-cases `LookupJoin` (adds the keys to
  `joinRefs`, and the right pattern to `wildcardJoinIndices` when no KEEP follows).
  POC: if the plan contains a `JOIN` command (`Join.isIndexJoin()`), return
  `IndexResolver.ALL_FIELDS`.
  Column pruning across the join is a follow-up.
- `EsqlSession.receiveLookupIndexResolution` and the lookup-index checks are not
  on this path at all.

### 5.5 Analyzer

`analysis/Analyzer.java`, `ResolveRefs.rule`: `case Join j when j.isIndexJoin() ->
resolveIndexJoin(j)`, after the `LookupJoin` and subquery-join cases.

`resolveIndexJoin`:

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

The optimizer sees the INNER `Join`, never `ColocatedJoin`, so rules are written for
the join type and the command, not for the strategy:

- `PushDownAndCombineFilters.pushDownPastJoin`: applies to LEFT joins and to
  `isIndexJoin()`. For an inner join a conjunct that references only left
  attributes moves to the left child; one that references only right attributes is
  also applied to the right child (and stays above the join, as for LEFT); mixed
  conjuncts stay above. This turns `| WHERE country == "DE"` into a term query
  inside the per-page lookup (see 5.10). The PromQL `InnerJoin` (also INNER) is
  not affected.
- `PushDownAndCombineLimits` and `PushDownAndCombineLimitBy` only push through LEFT
  joins, so a limit is never copied below the inner join.
- `PropagateEmptyRelation` already treats INNER: empty left gives empty output.
- `PushDownJoinPastProject`, `PruneLeftJoinOnNullMatchingField`: LEFT-only.

### 5.7a Strategy selection (`SelectJoinStrategy`, PreMapper)

`planner/premapper/SelectJoinStrategy` runs first in `PreMapper.preMapper`, on the
coordinator, after logical optimization and before `Mapper`. `PreMapper` already
holds `TransportActionServices`, so it reads the current project state
(`projectResolver.getProjectState(clusterService.state())`). For every
`Join.isIndexJoin()` that is not yet a `ColocatedJoin` it checks, collecting all
failures:

1. shape: the left side is `Filter`/`Eval`/`Project` over an `EsRelation`, the right
   side `Filter`/`Project` over an `EsRelation`. Everything below the join runs
   inside the data-node fragment, so a `LIMIT` or `STATS` there would run per node
   (the first POC cut accepted `FROM events | LIMIT 5 | JOIN ...` and returned up
   to five rows per node);
2. each side is one local index, found in the project metadata;
3. routing scheme of each index: no `routing_partition_size`, no `routing_path` or
   time-series dimensions, no resharding, `_routing` required;
4. same routing function: equal shard counts, and for indices created before
   modulo routing, equal `routing_num_shards` and routing factor;
5. placement: for every shard number an active, searchable copy of both shards on
   a common node, from the routing table.

If all hold, the join becomes `new ColocatedJoin(join)`. Otherwise the step throws a
`VerificationException` listing every reason, for example
`JOIN requires shard i of [events] and [sessions] on the same node, but shard [1]: [node-1] and [node-2]`.
The step is skipped when the plan has no `JOIN` command.

### 5.7 Coordinator physical planning (`Mapper`)

`planner/mapper/Mapper.java`, `mapBinary`: the LEFT-only check runs before the
`REMOTE` check. An explicit `ColocatedJoin` case comes first and returns
`new FragmentExec(bp)`. An INNER `Join` that `SelectJoinStrategy` did not convert
never reaches the mapper, because the selection fails the query. The rest of the
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
- `loadFields` is `addedFields`; the right key is not loaded, since the operator
  only ever sees matching right rows (see "Inner join assembly" below);
- `useStreamingOperator = false` (the streaming variant opens a bidirectional
  exchange to a remote node, which is pointless locally);
- a `PartnerShardResolver` is passed to the operator factory: given the shard
  index of a left page, return `new ShardId(sessionsIndex, shardNumber)`. The
  shard index to `ShardId` mapping is the planner's `shardContexts` list
  (`EsPhysicalOperationProviders`), so the resolver is built at planning time;
- `innerJoin = true` on the operator factory: the operator assembles pages as an
  inner join, so neither a filter nor a projection is planned above it.

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

**Inner join assembly.** The lookup service runs with `mergePages == false` for
lookup joins, so its response is already inner: pages of
`[positions, right columns...]` holding only the matches, sorted by left position,
one row per matching right document. `LOOKUP JOIN` turns that back into a left join
in `RightChunkedLeftJoin` (fill the gaps with `insertNulls`, pad the unmatched tail
with constant nulls). The first POC cut then undid that again: it loaded the right
key a second time as a `$$session_id$match` marker, filtered `IS NOT NULL` on it and
projected it away, so every left page was copied twice and padded only to be thrown
away.

The co-located join skips the left join altogether:

- `compute/operator/lookup/RightChunkedInnerJoin`: per right page, the output is the
  left blocks filtered by `positions` (duplicates allowed, so one-to-many works)
  next to the right blocks, which are reused with `mustIncRef()`. No null padding,
  no trailing output. When the positions are exactly `0..n-1` (each left row
  matched once, in order) the left blocks are reused as well, so the page costs no
  copy at all. Positions must not decrease across pages; a left row whose matches
  span two right pages appears in both outputs.
- `RightChunkedJoin` is the small interface both assemblers implement, so
  `LookupFromIndexOperator.OngoingJoin` can hold either.
- `LookupFromIndexOperator.Factory` has `boolean innerJoin` (only
  `planColocatedJoin` sets it; the streaming operator rejects it). `describe()` and
  `toString()` print `join=inner`.
- `ColocatedJoinExec` has no marker; its output is the left output minus shadowed
  names plus `addedFields`, which equals `ColocatedJoin.output()`. `LocalMapper`
  returns it directly and only adds a `ProjectExec` if the two ever diverge.

Per left page the work is: one term-query batch on the partner shard,
`EnrichQuerySourceOperator` reading the matches, extraction of the right columns,
then `RightChunkedInnerJoin` replicating matched left rows per match. A left page
with no match produces no output page. Memory is bounded per page by the lookup service's local circuit breaker.
Nothing is materialized across pages.

### 5.11 `STATS BY` after the join (stage 3)

No join-specific work is expected. The coordinator `Mapper` splits the
`Aggregate` into INITIAL (inside the fragment, above the join) and FINAL (above the
exchange). `LocalMapper` maps the fragment's `Aggregate` to `AggregateExec(INITIAL)`
over `ColocatedJoinExec`. The local plan is
`AggregateExec(INITIAL) <- ColocatedJoinExec <- FieldExtractExec <- EsQueryExec(events)`,
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
sessions are German, so two right rows come back; `RightChunkedInnerJoin` attaches
them to the six left rows, and the INITIAL aggregate counts by `plan`. A session
row with another country would simply not be returned, so its events never get a
position in the right pages and are not emitted. Each node sends
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
6. LEFT variant (`innerJoin = false`: the operator then assembles with
   `RightChunkedLeftJoin`, as `LOOKUP JOIN` does, and `addedFields` become nullable),
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
  -> partial `HashAggregationOperator` -> exchange. Only aggregates cross the wire. (Before the inner
  join assembly of 2026-11-03 a `FilterOperator` on a match marker followed the lookup.)
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

### Update (2026-11-03): inner join assembly

The lookup runtime no longer produces a left join that is filtered afterwards; see
"Inner join assembly" in section 5.10. Verified by `RightChunkedInnerJoinTests`
(examples, overlap across right pages, the identity fast path, decreasing positions,
random pages against a nested loop, cranky breaker variants) and `ColocatedJoinIT`
(unchanged results, a new all-unmatched case, and a profile check for `join=inner`
with no `FilterOperator`).

### Update (2026-11-03): strategy chosen on the coordinator

The parser builds a plain INNER `Join`; `SelectJoinStrategy` (section 5.7a) turns it
into a `ColocatedJoin` in the PreMapper, from the cluster state. Changes:

- `Join`: INNER output (USING semantics), `isIndexJoin()`, `TelemetryAware` with
  label `JOIN` for the command only, and the analysis checks of the command
  (single right index, keyword keys).
- `ColocatedJoin`: copy constructor from `Join`; its own output and verification
  are gone. `FeatureMetric.COLOCATED_JOIN` is now `FeatureMetric.JOIN`.
- Analyzer, `FieldNameUtils` and `PushDownAndCombineFilters` match
  `Join.isIndexJoin()` instead of `ColocatedJoin`.
- Newly rejected: pipeline breakers below the join (they ran per node before),
  routing partitions, `routing_path`, resharding, `_routing` not required, and
  different routing functions. Misplaced shards are now reported on the coordinator
  with every misplaced pair, before anything is sent to the data nodes.

Tests: `SelectJoinStrategyTests` (hand-built cluster state: aligned, streaming
commands below the join, `LIMIT` below, misplaced shard, shard count, `_routing` not
required, routing partitions, other joins untouched); `ColocatedJoinParserTests`
(plain INNER `Join`); `PushDownAndCombineFiltersTests.testPushDownFilterPastIndexJoin`;
`PushDownAndCombineLimitsTests.testLimitNotPushedPastIndexJoin`; `ColocatedJoinIT`
(`_routing` now required in the seed mapping; new `testRejectsPipelineBreakerBeforeJoin`
and `testRejectsMisplacedShards`, which moves `sessions[1]` with a reroute).
