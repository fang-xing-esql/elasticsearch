# Declared routing field: design

Status: option A implemented (2026-11-04), with `keyword`, integer and `ip` routing fields; see
section 11. Companion to `colocated_join_design.md`.

## 1. Summary

Add an index setting, `index.routing_field: <field>`, that makes a document field the
source of truth for `_routing`. The server derives `_routing` from that field on the
coordinating node, rejects documents whose `_routing` disagrees with it, and re-checks
on the data node, so no write path (index, bulk, update, reindex, old coordinators) can
store a document on a shard its field value does not hash to.

**Feasible.** The recommended design (option A, section 4) keeps `_routing` as the
carrier: shard = `murmur3(field value)`, exactly what `?routing=<value>` produces today.
Reads by `_id` therefore keep working as they do for any index with required routing
(`GET /idx/_doc/1?routing=s-100`). The "hard part" in the earlier one-line idea, embedding
the routing hash in `_id` the way TSDB and LogsDB do, is only needed if routing is taken
off the request entirely (option B). Option B is evaluated and deferred.

For ES|QL it closes the correctness hole of the co-located join: the planner can verify
that the join key *is* the routing field of both indices, instead of trusting an ingest
pipeline.

## 2. Problem

### 2.1 What the co-located join POC trusts today

`selfjoin_setup.sh` routes documents with a `final_pipeline` that copies `session_id`
into `_routing`, plus `_routing.required: true`. `SelectJoinStrategy` (ES|QL PreMapper)
verifies shard counts, the routing function and placement, but not that `_routing`
equals the join key, because nothing records it. Known holes:

- **Updates bypass ingest.** `TransportUpdateAction` / `UpdateHelper` build the new
  document on the primary from the stored one and keep the stored routing
  (`UpdateHelper.calculateRouting`); no pipeline runs. An update that changes
  `session_id` leaves the document on the shard of the old value. A co-located join then
  silently misses it.
- **The pipeline is a convention.** Removing or editing it, or indexing into an index
  created without it, produces arbitrary routing. Nothing in the metadata says the index
  is "routed by `session_id`".
- **No planner-visible fact.** `JOIN sessions ON user_id` passes every check that exists
  today and returns partial results.

### 2.2 Why a server feature rather than an ES|QL check

Field-based routing is useful on its own: users already emulate it with pipelines or
client code (tenant id, session id, customer id). A declared field gives them
enforcement, and gives every reader (ES|QL, search shard pruning, tooling) a fact to
rely on.

## 3. How routing works today (relevant parts)

`cluster/routing/IndexRouting.fromIndexMetadata` picks one strategy per index:

| strategy | used for | shard from | `_id` | update API | split |
|---|---|---|---|---|---|
| `Unpartitioned` | regular indices | `murmur3(_routing ?: _id)` via the routing function | any | yes | yes |
| `Partitioned` | `routing_partition_size > 1` | routing hash + id hash offset | any | yes | yes |
| `ExtractFromSource.ForRoutingPath` | `index.routing_path`: old TSDB, LogsDB `route_on_sort_fields` | hash of (field name, value) pairs from `_source` (`RoutingHashBuilder`) | generated; LogsDB embeds the routing hash 9 bytes before the end (`IndexRequest.autoGenerateTimeBasedId(OptionalInt)`) | **no** (`updateShard` throws) | **no** |
| `ExtractFromSource.ForIndexDimensions` | TSDB on recent versions | hash of the tsid | synthetic, routing hash as prefix or suffix (`TsidExtractingIdFieldMapper`) | **no** | **no** |

`ExtractFromSource` takes routing off the request (`checkNoRouting`), so a later read by
`_id` has nowhere to get the routing from, which is why it recovers the hash from the
`_id` (`idToHash`). That is the "TSDB trick".

Related: `index.slice.enabled` (`SliceIndexing`) treats a client-provided `_slice` value
as routing and surfaces it as a metadata field. It is request-side, not derived from the
document.

## 4. Options

**A. Derived and enforced `_routing` (recommended).** The routing value is still
`_routing`, but the server fills it from the field and enforces equality.

**B. Routing from source, hash in `_id`.** A new `ExtractFromSource` variant over one
field: no `_routing` at all, the routing hash embedded in a server-generated `_id`.

| | A: derived `_routing` | B: hash in `_id` |
|---|---|---|
| shard of value `v` | `murmur3(v)`, same as `?routing=v` | `murmur3(name) ^ murmur3(v)` with `RoutingHashBuilder`, or a new value-only hash |
| two indices with different key names (`session_id` vs `id`) co-locate | yes, the hash ignores the name | only with a new value-only hash |
| client-provided `_id` | allowed | must be rejected or rewritten (the hash has to be in it) |
| `GET`/`DELETE` by `_id` | needs `?routing=v`, as for any index with required routing | works without routing |
| update API | supported; changing the field is rejected | unsupported (as in TSDB/LogsDB) |
| split | supported (routing-based) | unsupported |
| existing clients that already send `?routing=v` | keep working | must stop sending routing |
| change size | one `IndexRouting` strategy, one mapper check, settings validation | new strategy, id format, get/delete/update paths, docs |

B is the better end-user experience for append-only data (no routing parameter ever),
but it inherits every TSDB/LogsDB restriction and needs a new id format. A delivers the
guarantee ES|QL needs with a small, local change. B can be added later as a second mode
of the same setting (section 10).

## 5. Option A in detail

### 5.1 Setting

```
index.routing_field: session_id      # IndexScope, Final
```

- Allowed in `standard` mode only for the first version. Rejected together with
  `index.routing_path`, `index.routing_partition_size > 1`, time-series mode,
  `index.logsdb.route_on_sort_fields`, and `index.slice.enabled` (both would claim
  `_routing`). Validation sits next to the existing ones in
  `IndexMode.validateWithOtherSettings` / `VALIDATE_WITH_SETTINGS`.
- Implies required routing. Either set `_routing.required: true` in the mapping
  automatically, or reject an index whose mapping does not require it. Recommendation:
  reject, so the mapping says what the index does.
- Registered in `IndexScopedSettings`; gated by a new `IndexVersion` so the index can only
  be allocated on nodes that enforce it (section 5.8).

### 5.2 Mapping constraints

Checked when the index is created and on every mapping update (`MapperService` merge, like
the `routing_path` checks in `DocumentMapper` / `MappedFieldType`):

- the field is mapped explicitly as `keyword`, `long`, `integer`, `short`, `byte` or `ip`
  (dotted paths allowed), not a runtime field, not a multi-field sub-field;
- a `keyword` has no `normalizer` (the routing value is the source string, a normalized
  indexed term would differ).

The data node checks the value in the source, not the indexed term, so `ignore_above` does
not weaken the check.

**Canonical routing values.** `_routing` is a string, and equal values must give equal
strings in every index and in every spelling the source allows, so the value is
canonicalized before it is used (`RoutingField`):

| field type | accepted source values | routing value |
|---|---|---|
| `keyword` | any scalar | the text as written (`"s-100"`, `1e3` stays `1e3`, as a keyword indexes it) |
| `long`, `integer`, `short`, `byte` | numbers and numeric strings with an exact integer value | decimal form: `42`, `"42"`, `42.0` all give `42`; `42.5` is rejected rather than truncated |
| `ip` | strings | Elasticsearch's `ip` format after the indexed round trip: `::ffff:10.0.0.1` gives `10.0.0.1`, IPv6 is compressed |

All integer types share one form, so a `long` key and an `integer` key with the same value
route to the same shard number. Floating point types (`double`, `float`, `half_float`,
`scaled_float`) and `unsigned_long` are not supported: equality on rounded values is a poor
routing key, and `unsigned_long` would need its own form. Clients that pass `?routing` (for
`GET` or `DELETE`) must use the canonical form, which the rejection message shows.

### 5.3 Write path

**Coordinating node: new strategy `FieldRouted`** (a subclass of `IdAndRoutingOnly`, same
shard function as `Unpartitioned`):

- `preProcess(IndexRequest)`: extract the field from `_source` with a filtered parser on
  that one path (the same technique `ExtractFromSource` uses with `parserConfig`). Then:
  - missing, `null`, an array, or a non-string value: reject
    (`routing field [session_id] must have exactly one string value`);
  - `routing` absent: set it to the value;
  - `routing` present and different: reject
    (`routing [x] does not match routing field [session_id] value [y]`).
- shard: `routingFunction.shardNum(murmur3(routing))`, unchanged from `Unpartitioned`.
- Ingest runs before routing, so a pipeline may still compute the field; routing follows
  the final document. The `session-routing` pipeline in the POC becomes unnecessary.
- The batch path (`indexShard(IndexRequest[], SourceBatch)`) needs a `RoutingExtractor`
  for one field, or batch indexing is disabled for such indices in the first version.

**Data node: enforcement in `RoutingFieldMapper`.** At parse time compare
`context.routing()` with the value of the routing field in the parsed document and fail
the document on mismatch. This is the guarantee; the coordinator step is the convenience.
It covers:

- the update API: `UpdateHelper` builds the new document on the primary with the stored
  routing. Changing `session_id` in an update is rejected
  (`cannot change routing field [session_id] in an update, delete and re-index the
  document`), mirroring that `_routing` itself cannot change;
- writes through old coordinating nodes in a mixed cluster, and any internal path that
  builds an `IndexRequest` without going through `IndexRouting.preProcess`;
- the batch columnar path, if it bypasses `preProcess`.

Translog replay and recovery re-parse documents that already passed the check, so they do
not fail.

**Other write APIs:**

- delete: unchanged, `routing` required as today;
- reindex into such an index: `routing=keep` copies the source document's `_routing`, which
  is then validated; `routing=discard` lets the coordinator fill it. Document `discard` as
  the recommended option;
- aliases with `index_routing`: an alias routing value that differs from the field is
  rejected by the same checks. Reject `index_routing` on aliases pointing at such an index
  at alias creation for a clearer error.

### 5.4 Hash and co-location

The routing value is the field value, so the shard is `murmur3(value)` mapped through the
index's routing function: identical to `?routing=value`. Two indices with the same routing
function (equal shard counts on modulo-routed indices, as `SelectJoinStrategy` already
checks) put equal key values on equal shard numbers, regardless of the field names. That
is exactly the co-location invariant, now enforced by the server.

### 5.5 Read path

- `GET`, `mget`, `explain`, `termvectors` by `_id`: unchanged, `routing` required. A later
  convenience could accept `routing_field` values under that name; not needed.
- search with `routing=v`: unchanged (`collectSearchShards`).
- Follow-up: shard pruning from queries. A `term` on the routing field (`session_id ==
  "s-100"` in ES|QL, a `term` query in `_search`) can target one shard, the way `_routing`
  queries already can. The metadata makes this safe.

### 5.6 Exposure

- **Cluster state** (`IndexMetadata` settings): enough for ES|QL. `SelectJoinStrategy`
  already reads `IndexMetadata` on the coordinator.
- **Field caps** (optional, later): add `routingField` next to `numberOfShards` in
  `FieldCapabilitiesIndexResponse` behind a new transport version, if the analyzer (before
  the PreMapper) or cross-cluster planning needs it. Same pattern as
  `field_caps_number_of_shards`.
- `GET _settings`, index templates and data streams carry the setting like any other.

### 5.7 Resize and other index operations

- **Clone, shrink**: the setting is copied with the index settings; routing does not change.
- **Split**: supported. `IdAndRoutingOnly.shardMatcherForSplit` uses the routing, which equals
  the field.
- **Mapping updates**: the field cannot be removed or change type (already true for
  mappings); the constraints of 5.2 are re-checked.
- **Reindex out of such an index**: no impact.

### 5.8 Compatibility

- The setting is `Final`, so it is set at creation only; existing indices are not affected.
- Index creation with the setting requires all nodes to understand it (node feature check
  in `MetadataCreateIndexService`), and a new `IndexVersion` keeps the index off old data
  nodes, so the data-node check always runs.
- Old coordinating nodes in a mixed cluster do not fill `_routing`: the request fails with
  the existing `RoutingMissingException` (routing is required). A mismatched routing from
  an old coordinator is rejected by the data-node check. Both are safe failures.
- No `_id` format change, no transport changes for writes (`IndexRequest.routing` already
  exists).

## 6. ES|QL integration

- `SelectJoinStrategy` adds the check that is a TODO today: both indices have
  `index.routing_field` set and the join key equals it on each side. Error:
  `JOIN requires [sessions] to be routed by [session_id], found [index.routing_field: user_id]`
  or `... found no routing field`.
- The `_routing.required` check stays (implied by the setting).
- `selfjoin_setup.sh`: drop the `session-routing` pipeline, set
  `index.routing_field: session_id` on the seed (clones inherit it). The clone-from-seed
  trick for placement stays until the allocation follow-up.
- Follow-up: since the key is the routing field, `WHERE session_id == "s-100"` before a
  join can prune to one shard pair.

## 7. Option B (routing from source, hash in `_id`), for later

What it would take, using LogsDB `route_on_sort_fields` as the template:

- an `ExtractFromSource` variant over one field. Use a value-only hash (`murmur3(value)`)
  instead of `RoutingHashBuilder`, so that indices with different key names still
  co-locate and the shard matches option A for the same value;
- server-generated ids that embed the hash (`autoGenerateTimeBasedId(OptionalInt.of(hash))`);
  client-provided ids must be rejected, because `getShard` recovers the shard from the id
  (`idToHash`);
- the update API and split unsupported, as in TSDB/LogsDB;
- requests carrying `routing` rejected (`checkNoRouting`).

It fits append-only data (events, logs) and removes the routing parameter from clients. It
does not fit entity indices like `sessions`, which are updated and addressed by natural ids.
A second mode of the same setting (`index.routing_field_mode: derived | in_id`) could offer
both.

## 8. Implementation plan

1. **Setting and validation** (`server`):
   - `IndexSettings` / `IndexMetadata`: `INDEX_ROUTING_FIELD`, registered in
     `IndexScopedSettings`, validated in `IndexMode.validateWithOtherSettings`;
   - mapping constraints (5.2) in the mapping merge path;
   - new `IndexVersion` and node feature.
2. **Coordinator routing**: `IndexRouting.FieldRouted` selected in `IndexRouting.create`
   when the setting is present; single-field source extraction shared with the data node.
3. **Data-node enforcement**: `RoutingFieldMapper` check, with the update-specific message.
4. **APIs**: alias `index_routing` rejection; reindex documentation; batch path support or
   opt-out.
5. **ES|QL**: `SelectJoinStrategy` routing-field check; `ColocatedJoinIT` and
   `SelectJoinStrategyTests` cases; `selfjoin_setup.sh` without the pipeline.
6. **Docs**: setting reference, routing guide section "Routing by a document field".
7. **Later**: field caps exposure; shard pruning on term queries on the routing field;
   option B mode.

## 9. Tests

- **Unit**: `IndexRoutingTests` for `FieldRouted` (fills routing, rejects mismatch, missing,
  array, non-string; same shard as `?routing=v`); settings validation (incompatible
  settings, mapping constraints); `RoutingFieldMapperTests` for the mismatch check.
- **YAML REST** (preferred for API behavior): create with the setting; index without routing
  and `GET` with `routing`; mismatched `routing` rejected; update changing the field rejected;
  update of another field accepted; bulk with mixed valid/invalid items; reindex with
  `routing=discard`.
- **Internal cluster**: mixed write paths land on the expected shard for random values
  (`_search` with `preference=_shards:i` per shard, check every document's field hashes to
  `i`); split keeps the invariant.
- **Mixed cluster / BWC**: index creation refused until all nodes support the feature;
  writes through an old coordinator fail safely.
- **ES|QL**: join on a non-routing field rejected; join on the routing field accepted;
  `ColocatedJoinIT` without the pipeline.

## 10. Open questions

1. Name: `index.routing_field` (next to `routing_path`, `routing_partition_size`) or
   `index.routing.field`. The `index.routing.*` namespace is used by allocation settings,
   which argues for the former.
2. Should the setting set `_routing.required: true` itself, or require it in the mapping
   (recommended: require)?
3. Types beyond `keyword`: resolved for integers and `ip` (5.2). `double`, `date` and
   `unsigned_long` remain open.
4. Composite keys (several fields) for multi-key joins: a list value with an ordered,
   value-only hash; same enforcement.
5. Interaction with `_slice`: both turn a value into routing. A slice could be declared as
   "the routing field" of a slice-enabled index, or the two stay mutually exclusive
   (recommended for the first version).
6. Should `GET` by `_id` accept the routing field's value under its own name (sugar), or is
   `routing` enough?

## 11. Implementation status (2026-11-04)

Implemented (option A):

- `IndexSettings.ROUTING_FIELD` (`index.routing_field`, `IndexScope`, `Final`), registered in
  `IndexScopedSettings`. Its validator rejects it with a non-standard `index.mode`,
  `index.routing_path`, `index.routing_partition_size > 1`, `index.slice.enabled` and
  `index.batch_indexing` (the columnar batch path neither routes nor parses documents one by
  one, so it would skip both checks).
- `cluster/routing/RoutingField`: reads the field from the source through a parser filtered
  to its path (nested objects and dotted names), requires exactly one non-null value, and
  canonicalizes it per type (5.2). `typeFromMapping` finds the field's type in the mapping
  source, which is all the coordinating node has.
- `IndexRouting.FieldRouted`: selected in `IndexRouting.create` when the setting is set;
  `preProcess` fills `_routing` from the field or rejects a different one; shard selection
  as `Unpartitioned`. Writes fail while the field is not mapped.
- `RoutingFieldMapper.postParse`: the data-node check against the source, which covers
  updates (an update that changes the field is rejected), upserts, and writes that bypass
  the coordinator's routing.
- `DocumentMapper.validate`: `_routing` must be required, the field mapped with a supported
  type, not a multi-field, and a keyword without normalizer.
- ES|QL: `SelectJoinStrategy` requires `index.routing_field` on both indices and the join key
  to be that field; `Join` accepts `keyword`, `long`, `integer` and `ip` keys. The
  `session-routing` pipeline is gone from `selfjoin_setup.sh` and `ColocatedJoinIT`.

Tests: `RoutingFieldTests` (extraction, canonical forms, coordinator routing, `long` and
`integer` routing alike), `RoutingFieldEnforcementTests` (mapping validation, data-node
check), `RoutingFieldSingleNodeTests` (index, bulk, update, get with keyword, long and ip
fields; incompatible settings; unsupported mapping), `SelectJoinStrategyTests` (missing and
different routing field, every reason reported), `ColocatedJoinIT` (`long` key join, index
without routing field). Existing server suites for settings, mapping, routing, bulk and
update pass (2117 tests).

Not done yet:

- `IndexVersion` and node feature gating (5.8); today an index with the setting could be
  created in a mixed cluster.
- rejecting alias `index_routing` on such indices at alias creation (writes through such an
  alias are already rejected by the checks);
- field caps exposure, shard pruning on term queries, reference docs, YAML REST tests;
- reindex guidance (`routing=discard`) in the docs.
