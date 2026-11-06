# Co-located JOIN: streaming lookups

Status: option A and the `colocated_join_streaming` pragma are **implemented** on `colocated-join-route-rhs` (2026-11-06), not
committed; see [Implementation](#implementation). LOOKUP JOIN stays unchanged.
Any change to shared lookup code needs a co-located-only path. The co-located join is switched by its own query pragma,
`colocated_join_streaming` (see [Query pragma](#query-pragma-colocated_join_streaming)), not by `LOOKUP_JOIN_STREAMING`.

Related: `colocated_join_improvements.md`, `colocated_join_column_pruning.md`, and the Rally track plan
`rally-tracks/colocated_joins_plan.md`.

## How streaming LOOKUP JOIN works today

`StreamingLookupFromIndexOperator` (`x-pack/plugin/esql/.../enrich/`) keeps one session per worker open for the whole query,
instead of sending one request per page:

1. **Setup.** For each worker, one request (`AbstractLookupService.lookupAsync(request, serverNode, ...)`) carries the lookup plan.
   The server (`LookupFromIndexService.doLookupStreaming`) plans it once and opens a `BidirectionalBatchExchangeServer` on the
   lookup shard.
2. **Batches.** Each input page is a batch with an id, sent through a client-to-server exchange, one per worker. Results come back
   through a server-to-client exchange shared by all workers. The operator matches them to their page by batch id, with up to
   `maxOutstandingRequests` batches in flight.
3. **Workers.** `BidirectionalBatchExchangeClient` (compute module) creates up to `maxWorkers` workers and gives each batch to
   the least loaded one. `determineServerNode()` supplies the node.
4. **Gating.** `LocalExecutionPlanner.shouldUseStreamingOperator` uses it only when the `LOOKUP_JOIN_STREAMING` setting is on, and
   only if every node that holds a copy of the index supports the `ESQL_LOOKUP_PLANNING` transport version.

The non-streaming path (`LookupFromIndexOperator`, which the co-located join uses) pays several costs on **every page**: a transport
request, a new shard context (`lookupShardContextFactory.create`), and planning the lookup again. Streaming pays them once per worker.

## Why the co-located join cannot use it today

1. **It assumes a single shard.** `determineServerNode()` throws unless the lookup index has exactly one shard. A server session
   is bound to one `ShardId`. The co-located right index has N shards, and one page's rows can belong to several of them.
2. **It only does left joins.** `PendingJoin` always uses `RightChunkedLeftJoin`. The `LookupFromIndexOperator.Factory` rejects
   `useStreamingOperator && innerJoin`, and the co-located join is inner.
3. **Page splitting.** On the routed path, `PartnerShards.split` turns a page into sub-pages, one per right shard. Streaming expects
   one batch per input page and joins it back with a `RightChunkedLeftJoin` over that page.
4. **Explicit rejection.** The Factory throws for `useStreamingOperator && partnerShards != null`.

## Options

### A. One streaming client per right shard (smallest change)

- The operator keeps a `Map<shard, BidirectionalBatchExchangeClient>` and creates each client the first time a page needs that
  shard.
- A client's node supplier picks an active copy of its shard, local first. That is the logic of `shardCopies()` behind
  `lookupAsyncOnShard`.
- The client's setup request carries that shard's `ShardId`.
- `PartnerShards.split` gives the sub-pages. Each sub-page is its own batch with its own join: `RightChunkedInnerJoin` or
  `RightChunkedLeftJoin`, as `LookupFromIndexOperator.newJoin` picks today. Outputs are emitted in completion order.
- `BidirectionalBatchExchangeClient` is unchanged.
- **Cost:** up to drivers × right shards × workers server sessions per query. Needs a cap, for example one worker per shard per
  driver.

### B. One client with workers per shard

- Teach `BidirectionalBatchExchangeClient` to pick a worker by shard instead of by load.
- Fewer exchanges than A, but it changes shared compute code that LOOKUP JOIN streaming also uses. That is riskier under the
  "LOOKUP JOIN unchanged" constraint.

### C. Aligned path first

- An aligned page (`ColocatedRouting.aligned`) goes to exactly one right shard, which is on the same node. This is A with one
  client per left shard: no splitting, no remote sessions.
- A cheap first step that measures what streaming alone is worth, separate from routing.

### D. For aligned pages, skip the exchange altogether

- When the right shard is on this node, the operator could acquire its searcher once per driver and run the lookup pipeline in
  process, with no transport, no exchange and no batch markers. The pipeline is `EnrichQuerySourceOperator` →
  `ValuesSourceReaderOperator` → merge.
- This is the truly co-located join the layout promises. Streaming is then only needed for remote shards on the routed path.
- **Cost:** a new operator, and the right shard has to stay acquired for the driver's lifetime.

## Query pragma: `colocated_join_streaming`

Implemented as specified below, except that there is no partial fallback: option A streams both the aligned and the routed path.

| | |
|---|---|
| Name | `colocated_join_streaming` |
| Declaration | `QueryPragmas.COLOCATED_JOIN_STREAMING = Setting.boolSetting("colocated_join_streaming", true)` with an accessor `colocatedJoinStreaming()`, like `NODE_LEVEL_REDUCTION` |
| Default | `true`: the co-located join streams its lookups wherever it can |
| `false` | every co-located lookup goes through the per-page `LookupFromIndexOperator` path, as on the branch today |
| Scope | the co-located `JOIN` only. LOOKUP JOIN ignores it and keeps following the `LOOKUP_JOIN_STREAMING` cluster setting |

How it is used:

- `LocalExecutionPlanner.planColocatedJoin` reads `context.queryPragmas().colocatedJoinStreaming()` on the data node and picks the
  operator. The pragmas travel to the data nodes with the query, so every node decides the same way.
- `LOOKUP_JOIN_STREAMING` is not consulted for the co-located join, and the new pragma is not consulted for LOOKUP JOIN. Neither
  affects the other.
- `true` means "stream where it is supported". The join falls back to the per-page path, without failing the query, when:
  - a node that holds a copy of a right shard does not support the streaming transport version. Check it for each right shard, as
    `shouldUseStreamingOperator` does for the lookup index, or with a new version if the co-located setup request changes;
  - the operator has no `Configuration`, which the streaming operator requires;
  - a path is not streamed yet. During step C (aligned path only), routed pages keep using the per-page path while aligned pages
    stream.
- The profile shows which path ran: `StreamingLookupOperator[... colocated=true ...]` or `LookupOperator[... colocated=true ...]`.
  Once streaming is per shard, add counts of streamed and per-page batches, and of local and remote ones.
- Like every pragma, setting it on a release build needs `accept_pragma_risks`. The `JOIN` command is snapshot-only anyway.

Why default `true`: streaming is expected to be the faster path, mainly on the routed path, which splits pages into many small
lookups. The pragma is then an escape hatch, to compare both paths in benchmarks or to get around a streaming problem in one query
without a cluster-wide setting.

Tests:

- `ColocatedJoinIT`: randomize the pragma, so every test runs both paths with the same expected rows.
- A profile test for each value checks the operator name.
- A unit test for the fallback: with the pragma `true` and a right-shard node on an old transport version, the plan uses the
  per-page operator.
- The Rally track (`rally-tracks/colocated_joins_plan.md`) can add `colocated_join_streaming: false` variants of the co-located
  joins to measure what streaming brings.

## What streaming would and would not fix

- **It would help** most on the routed path, where pages break into many small per-shard lookups that each pay the full cost of a
  request. In the track, `rhs-sharded-on-join-key` runs about 460-row pages across 3 shards. Planning the lookup once per worker
  removes the repeated planning cost.
- **It would not fix** the unused columns (`colocated_join_column_pruning.md`). The streaming setup still sends
  `loadFields = addedFields`. The left page size comes from the same row-size estimate.
- **Retries:** the non-streaming path retries another copy of the shard (#161683, `lookupOnNextShardCopy`). A streaming session
  is pinned to one node. Check how streaming LOOKUP JOIN handles shard relocation or node loss before relying on it.
- **Profiling:** per-shard clients make it easy to report local and remote lookups and batch counts per shard. The track plan
  lists that as missing (section 7, item 4).

## Suggested order

1. The two column-pruning fixes in `colocated_join_column_pruning.md`. They are independent of streaming and cheaper.
2. **C**: streaming on the aligned path only, with the `colocated_join_streaming` pragma (default `true`). Measure with the track's
   `stats-count-right-columns` shape, which needs the same right columns in every variant.
3. **A** for the routed path.
4. **D** if C shows that the exchange still costs a lot for local lookups.

## Open questions

- How does streaming LOOKUP JOIN react to a shard relocation or a node leaving in the middle of a query?
- Per-driver clients vs one client shared by all drivers on a node: how many server sessions are acceptable?
- Does the server-to-client exchange shared by all workers keep up when N shard clients send results at once?

## Implementation

Option A, without changes to `BidirectionalBatchExchangeClient`:

- **`ColocatedStreamingLookupOperator`** (new, `enrich`). It keeps one `StreamingLookupFromIndexOperator` per right shard,
  opened the first time a row needs that shard, and splits each page with `PartnerShards` (aligned pages go whole to their
  partner shard). Sub-pages are separate batches. Outputs are polled round-robin over the shards. It accepts input while fewer
  than `enrich_max_workers` batches are in flight over all shards; it is blocked only while every shard lookup that still has
  work is blocked. Its status sums the per-shard `StreamingLookupStatus`. Profile name:
  `StreamingLookupOperator[index=... load_fields=[...] colocated=true join=inner shards=[...]]`.
- **`StreamingLookupFromIndexOperator`**: a package-private constructor adds `shardNumber` (`-1` for a lookup index), `innerJoin`
  (`RightChunkedInnerJoin` instead of `RightChunkedLeftJoin`) and `maxWorkers`. The public constructor passes `-1, false` and
  `maxOutstandingRequests`, so LOOKUP JOIN behaves as before. With a shard number, the server node comes from
  `AbstractLookupService.shardCopyNode` (local copy first) and the session is set up with `lookupAsyncOnShard(request, shard,
  node, ...)`. The co-located operator opens one worker (session) per right shard and driver.
- **`AbstractLookupService`**: `shardCopyNode(index, shard)` and `lookupAsyncOnShard(request, shard, targetNode, ...)`, which
  checks that the node has a copy of that shard. `shardCopies` is shared with the per-page `lookupAsyncOnShard`.
- **`LookupFromIndexOperator.Factory`** builds the co-located streaming operator when `useStreamingOperator` and `partnerShards`
  are both set. The streaming + inner join check now applies only to LOOKUP JOIN.
- **`QueryPragmas.COLOCATED_JOIN_STREAMING`** (`colocated_join_streaming`, default `true`), in `VALID_PRAGMA_NAMES`.
- **`LocalExecutionPlanner.planColocatedJoin`** streams when the pragma is on, a `Configuration` is present, and
  `allCopiesSupportStreaming` (extracted from `shouldUseStreamingOperator`, which LOOKUP JOIN still calls) holds for every copy of
  every right shard. Otherwise it uses the per-page operator.

Tests:

- `ColocatedJoinIT` randomizes the pragma for every query (`getPragmas`), and the profile tests run both values: the aligned
  left index uses one shard per driver (`shards=[i]`), the unrouted one all three.
- Regression runs: ES|QL unit tests for `enrich`, `optimizer`, `planner`, `plugin` and `EsqlNodeSubclassTests` (7,865 tests);
  `ColocatedJoinIT`, `LookupJoinIT`, `LookupFromIndexIT`, `CoordinatorLookupJoinIT`, `LookupJoinTypesIT`,
  `EsqlReductionLateMaterialization*IT` and `RemoteLookupDemoIT` with `-Dtests.iters=2`. All pass.

Not done:

- Option D (in-process lookup on the local shard, no exchange).
- No unit test of `ColocatedStreamingLookupOperator` alone; the multi-node operator test harness covers a single-shard lookup
  index only. The ITs exercise it on three nodes, aligned and routed.
- The open questions above still stand, in particular relocation during a query: a streaming session is pinned to one node and
  does not retry another copy.
- Benchmarks: run the track with `colocated_join_streaming: false` and `true`.
