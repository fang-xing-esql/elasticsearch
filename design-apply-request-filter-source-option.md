# Apply the request DSL filter per source command

Status: the engine is implemented on this branch. This file describes that code. User docs are out of scope here. The remaining work is the test plan at the bottom. Do not add tests from this file until that plan is reviewed.

Source: [Make Request Filter Optional for IN Subqueries](https://docs.google.com/document/d/1WTYkKdo0mfPvFvTnu625y_EEV5DIt8GoYh1pj4ecjU0/edit?tab=t.0#heading=h.azubovwpks5), option 3 (`WITH` on `FROM` / `TS`).

The clause applies to a whole `FROM` or `TS` command. Every index pattern, view, and dataset that command names shares the decision. An earlier draft put the clause on each pattern. That needs a rewrite that splits one `FROM` into several relations whenever the flags differ, and that split is not needed for the `IN` subquery case this feature is for.

The grammar is snapshot-only. `EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION` is `Build.current().isSnapshot()`, and it is the last capability constant. The `FROM` and `TS` parser rules read it. A release build rejects the clause as a syntax error. Queries that do not use it are unchanged on both builds.

Inline `EXTERNAL` does not accept `apply_request_filter`. A `FROM` or `TS` that names a dataset does. The flag is copied onto the dataset branch when that command is rewritten.

## Decision

Add an optional `WITH` map at the end of a `FROM` or `TS` command. The first key is `apply_request_filter`, a boolean literal, default `true`.

```esql
FROM logs-*, metrics-* WITH {"apply_request_filter": false}
FROM my_view WITH {"apply_request_filter": false}
TS metrics WITH {"apply_request_filter": false}
FROM my_dataset WITH {"apply_request_filter": false}
FROM logs-* METADATA _id WITH {"apply_request_filter": false}
```

`FROM a, b WITH {"apply_request_filter": false}` opts out `a` and `b`. The command stays one `UnresolvedRelation` with the comma-joined pattern it has today. Field-caps, concrete-index dedup, and schema merging do not change shape.

A command that sets the key to `false` is read without the request-body Query DSL `filter`. Every other command keeps today's implicit filter. A query that never writes the clause is unchanged: same plan shape, same field-caps call, same results.

The request body does not grow a setting. There is no cluster setting and no `SET` command for this. The clause is the whole feature.

### Why this option

The request `filter` is how Kibana sends the time picker and pinned filters. ES|QL applies it to every index, view output, and dataset the query reads, including sources inside `IN` subqueries. A range on `@timestamp` applied to an index that has no such field matches nothing, the subquery returns no rows, and the outer query silently returns nothing. The reports called out in the design doc are [elasticsearch#154849](https://github.com/elastic/elasticsearch/issues/154849), [elasticsearch#159347](https://github.com/elastic/elasticsearch/issues/159347), and [elasticsearch#161657](https://github.com/elastic/elasticsearch/issues/161657).

Option 1 (a query setting `apply_request_filter_to_in_subquery`) is whole-query and `IN`-only. It cannot opt one subquery out and leave another in, and a later join type needs another setting. Option 2 (`REQUEST_FILTER()`) is per predicate position, turns off the implicit filter everywhere as soon as the function appears once, and has three evaluation paths whose answers disagree after `EVAL` or `STATS`. Option 3 keeps the filter at the source command, so Lucene, field-caps pruning, and `can_match` stay valid for every command that did not opt out. The same clause covers `IN`, `FROM` subqueries, `LET`, `TS`, views, and datasets.

### What one command covers

| Query | Meaning |
| --- | --- |
| `FROM a, b` | Both filtered. One relation, as today. |
| `FROM a, b WITH {"apply_request_filter": false}` | `a` and `b` are not filtered. Still one relation. |
| `FROM a WITH {"apply_request_filter": false}, b` | Parse error. `WITH` is not allowed between patterns. |
| `FROM a, (FROM b WITH {"apply_request_filter": false} \| KEEP id)` | `a` is filtered. The nested `FROM` opted out on its own. |
| `FROM a, (FROM b \| KEEP id) WITH {"apply_request_filter": false}` | `a` and `FROM b` are both not filtered. The subquery is part of this command's source list, so the clause applies to its source command too. |
| `FROM a, (FROM b WITH {"apply_request_filter": true} \| KEEP id) WITH {"apply_request_filter": false}` | `a` is not filtered. `FROM b` stays filtered, because an explicit `WITH` on that source command wins. |
| `FROM my_view, logs-* WITH {"apply_request_filter": false}` | The view output and `logs-*` are both unfiltered. A view used in place of a source-list subquery is covered the same way. |
| `LET sub = (FROM b \| KEEP id); FROM a, sub WITH {"apply_request_filter": false}` | `a` is not filtered, and the source command inside `sub` is not filtered either. A named subquery used in place of a source-list subquery is covered at the use site. |

`METADATA` stays where it is, before `WITH`:

```esql
FROM logs-* METADATA _id WITH {"apply_request_filter": false}
```

To opt out one side and not the other, put the clause on the nested source command only. An outer clause would cover that nested command as well:

```esql
FROM has_ts, (FROM no_ts WITH {"apply_request_filter": false})
```

## Behavior

The request `filter` is unchanged. Omitting the clause and writing `{"apply_request_filter": true}` are the same effective value. Explicit `true` does not change the plan string. `EsRelation` prints `[apply_request_filter=false]` only when the flag is false. `ExternalRelation` plan text does not print the flag.

`false` removes only the out-of-band request filter from that command. It does not remove:

- `WHERE`, `STATS WHERE`, or any other predicate in the query text
- document-level or field-level security
- the view's own commands
- the `METRICS_INFO` / `TS_INFO` safety cap (`esql.metrics_info.max_time_range`). For an opted-out command that cap is placed at `[now - cap, now]`, the same window used when the request filter has no range. The Kibana range does not position the window.

The clause also covers the source command of every subquery written in that command's source list, and it keeps going through source-list subqueries nested inside those. `FROM a, (FROM b, (FROM c | KEEP id) | KEEP id) WITH {"apply_request_filter": false}` opts out `a`, `b`, and `c`.

A `WITH` on the parent query does not apply to the source command of an `IN` subquery. The `IN` source is opted out only by a `WITH` on its own `FROM` or `TS`. In particular:

```esql
FROM test WITH {"apply_request_filter": false}
| WHERE id IN (FROM test
               | WHERE id > 3
               | EVAL id = id - 3
               | KEEP id)
```

The outer `FROM test` is unfiltered. The `IN` source stays filtered, returns no rows, and the `IN` matches nothing. The same holds when the `IN` source is a view or a named subquery that did not write its own clause, and when the `IN` is written in `WHERE`, `EVAL`, `STATS`, `INLINE STATS`, `CASE`, `COALESCE`, `IS NULL`, or `IS NOT NULL`. Replacing the parent source with a view or a named subquery does not change this: the parent's opt-out still stops at the parent.

A view or a named subquery standing in a source-list position is covered too.

- A view name is a pattern of the command. `FROM a, my_view WITH {"apply_request_filter": false}` opts out `a` and skips the request filter on `my_view`'s output. A computed view body is not rewritten. The call-site flag is stored on `BranchIdentity.view`. A pass-through view, whose body is a bare standard `UnresolvedRelation`, is different: filtering the index and filtering the view output are the same operation, so the call-site `false` is copied onto that index relation and the branch identity is `INDEX`, not a view. See [Views](#3-views).
- A named subquery is substituted by `LetResolver` for a pattern token. `LET sub = (FROM b | KEEP id); FROM a, sub WITH {"apply_request_filter": false}` opts out `a` and, at this use only, the source commands inside `sub` that did not write their own `WITH`. The stored binding is copied first. Another use of `sub` that did not opt out stays filtered.

An explicit `WITH` on an inner source command wins, including a `WITH` written inside the named-subquery body or on the inline source. Propagation only fills in a source command that did not write its own clause. A view definition cannot contain the clause, so there is no explicit `WITH` inside a stored view body.

Propagation walks plan nodes. It does not walk expressions. An `InSubquery` or `MultiColumnInSubquery` plan is therefore never reached from the parent. `FROM a, (FROM b | WHERE id IN (FROM c | KEEP id)) WITH {"apply_request_filter": false}` opts out `a` and `b`, not `c`.

A `LET` body is not changed at the definition. It changes only when the use that names it opted out. A use inside an `IN` did not.

The top-level command may opt out. `FROM logs-* WITH {"apply_request_filter": false}` runs the main query with no request filter. Kibana time-picker filters will not apply to that command. User docs for that sentence are not part of this branch.

### Running example

Index `test` has rows `id` 1 through 6. The request filter is `{"range": {"id": {"lte": 3}}}`.

Subquery command opted out. The subquery reads ids 4, 5, and 6 and returns 1, 2, and 3. The outer query is still filtered to `id <= 3` and returns rows 1, 2, and 3.

```esql
FROM test
| WHERE id IN (FROM test WITH {"apply_request_filter": false}
               | WHERE id > 3
               | EVAL id = id - 3
               | KEEP id)
| KEEP id
```

Two subqueries, one opted out. The first returns 1, 2, and 3. The second keeps the filter and its `WHERE id == 2`, so it returns 2. The query returns rows 1 and 3.

```esql
FROM test
| WHERE id IN (FROM test WITH {"apply_request_filter": false}
               | WHERE id > 3
               | EVAL id = id - 3
               | KEEP id)
  AND id NOT IN (FROM test | WHERE id == 2 | KEEP id)
| KEEP id
```

Without the clause, the first query returns no rows, because the subquery sees both `id <= 3` and `id > 3`.

## Out of scope

- Option 1's query setting, and option 2's `REQUEST_FILTER()` function.
- A `WITH` map on an individual pattern inside one command. Patterns in one `FROM` or `TS` share the clause. Opting out one side only is an explicit `WITH` on a nested source command, with no clause on the outer command.
- `SET` inside a subquery. The grammar only allows `SET` as a statement prefix.
- Parameters, `null`, or non-boolean values for the key. The value is a `true` or `false` literal so the decision is visible in the query text. Kibana rewrites the text; it does not send a settings key.
- `WITH {"apply_request_filter": false}` or `WITH {"apply_request_filter": true}` inside a view definition. View creation rejects either value. The request filter applies to the view output when the view is queried. Callers write `FROM view_name WITH {"apply_request_filter": false}`.
- `LOOKUP JOIN`. Its right side is not a `FROM` and does not receive the request filter today. The clause is not added to it.
- Inline `EXTERNAL`. It keeps its own `WITH` map and does not recognise `apply_request_filter`. The key stays in the config map and fails later as an unknown external option.
- `EQL`. A query that contains an `EQL` source and a request filter already fails in `EsqlSession` before either rewriter runs. This change does not add `WITH` to `EQL` and does not lift that rejection.
- A transport version. The decision is consumed on the coordinator. Data nodes receive a `FragmentExec` whose `esFilter` is already set or already absent.
- Changing `preserveViewBoundaries` for commands that did not opt out, and changing view compaction for queries that do not use the clause.
- Turning view-boundary preservation off when every view command opted out. The code over-preserves. See [Views](#3-views).
- User docs (`from.md`, the `TS` snippet, `esql-rest.md`, `view_limitations.md`, the federated querying page). Not in this branch.
- A second field-caps call when the same pattern is referenced with different flags.
- Per-fragment `can_match` on a mixed union.

## How the filter is applied today

Four independent paths, all driven by `EsqlQueryRequest.filter()`:

1. **Lucene push-in.** `EsqlSession.logicalPlanToPhysicalPlan` calls `PlannerUtils.integrateEsFilterIntoFragment`, which stamps `request.filter()` onto every `FragmentExec` except view-branch fragments when `ViewRequestFilterRewriter.supportsRewrite` is true. `PlannerUtils.detectFilter` reads that same field to build the data-node `can_match` filter (`DataNodeComputeHandler`). `IN` subqueries, `INLINE STATS`, and subquery joins each run through `executeSubPlan`, which calls the same method, so the stamp is per physical plan rather than per join type.
2. **Field-caps pruning.** `EsqlSession.preAnalyzeMainIndices` sends the filter as `index_filter` for each `IndexPattern`, on the local path and the cross-project path. Patterns reachable only inside a view branch are already exempt (`viewInternalIndexPatterns`). A pattern used both inside a view and directly by the query is treated as view-internal and loses pruning, which is the safe direction. `IndexPattern` equality is `(pattern string, view scope)`.
3. **View outputs.** `ViewRequestFilterRewriter` inserts a logical `Filter` above each view branch of a `BranchPlan`, translated against that branch's output. The walk stops at a view branch, so a view nested inside another view is not filtered on its own. A view nested in a user-written subquery is filtered, because the walk continues through non-view branches. `FragmentExec.isFromViewBranch()` stops the Lucene stamp from also pushing the raw DSL into the view's sources. Below `esql_request_filter_on_dataset`, the rewriter does not run and the Lucene stamp is the fallback.
4. **Datasets.** `RequestFilterRewriter` inserts a logical `Filter` above each `ExternalRelation` that is not inside a view. Untranslatable conjuncts are dropped with a warning. Listing also takes timestamp bounds from the request filter (`PartitionSpec.addTimestampBounds` in `preAnalyzeExternalSources`) and uses them for every external path in the query, except a path that every reference opted out of.

`FROM a, b` is one `UnresolvedRelation` whose `IndexPattern` is the comma-joined string `a,b` (`LogicalPlanBuilder.visitRelation`). A pattern in that list may later resolve to a view (`ViewResolver`) or a dataset (`DatasetRewriter`) and become a `BranchPlan` branch. `BranchIdentity` records whether a branch is an index, a view, a dataset, or a subquery. `BranchIdentity.liftedThrough` promotes a piece lifted out of a view to that view's identity so the request-filter rewriter still targets the view output.

Inline `EXTERNAL path WITH { ... }` already parses `commandNamedParameters`. Registered datasets are `FROM dataset_name` and start life as an `UnresolvedRelation`.

## Design

### Syntax

In `EsqlBaseParser.g4`, `indexPatternAndMetadataFields` always ends in `sourceCommandNamedParameters`. The capability gate is a sibling alternative, not a predicate on the only alternative of the parent rule. A predicate on the only alternative would reject plain `FROM idx` on a release build. An optional `WITH` that can match empty trips ANTLR warning 154, which is an error under `-Werror`. The empty alternative is what makes `WITH` optional on a snapshot build and absent on a release build.

```
indexPatternAndMetadataFields
    : indexPatternOrSubquery (COMMA indexPatternOrSubquery)* metadata? sourceCommandNamedParameters
    ;

sourceCommandNamedParameters
    : {EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled()}? WITH mapExpression
    |
    ;
```

`fromCommand` and `timeSeriesCommand` stay `FROM` or `TS` plus `indexPatternAndMetadataFields`. Nothing else references that rule. `METADATA` stays before `WITH`. `WITH` is already a keyword, so the lexer rule stays unconditional. When the capability is off, the `WITH` alternative is disabled, the empty alternative matches `FROM idx` and `TS idx`, and a written `WITH` is a syntax error (`no viable alternative at input 'WITH'`). `mapExpression` is `{ string : constant }`, the same map `EXTERNAL`, `RERANK`, and `COMPLETION` use.

The capability is declared as `APPLY_REQUEST_FILTER_SOURCE_OPTION(Build.current().isSnapshot())`. It is a global `EsqlCapabilities.Cap`, not a function capability. Promoting the syntax later means removing the predicate and constructing the capability with `true`.

`LogicalPlanBuilder.visitExternalCommand` is unchanged. It does not read `apply_request_filter`.

`LOOKUP JOIN` and `EQL` do not grow the clause.

ANTLR regeneration is the esql subproject `regen` task (`:x-pack:plugin:esql:regen`). `regenParser` alone deletes the PromQL parser sources. Generated parser files are not hand-edited.

### Parsing and validation

`LogicalPlanBuilder.visitRelation` reads one map for the command, folds literals with `foldOptionLiterals`, and stores a `CommandNamedParameters` on the `UnresolvedRelation` it already builds. It does not split the pattern list and does not insert a `UnionAll` because of the clause. A source-list subquery still becomes a `UnionAll` the way it does today.

`SourceCommandNamedParameters.commandNamedParameters` validates the folded map. An empty map is `Empty option is not supported`. An unknown key is `Option [key] is not supported`. A non-boolean value is `The value of option [apply_request_filter] must be the literal true or false`. There is no list of supported keys in the message.

`foldOptionLiterals` runs first. `null` fails there as `FROM option [apply_request_filter] has null value; null is not a valid option value` (`TS` uses the command name `TS`). A parameter or a nested map that is not a literal fails as `FROM options must be literal values; option [apply_request_filter] has expression [...]`. A string `"false"` folds to a string and then fails the boolean check. A duplicate key fails earlier, in map parsing: `Duplicated named parameters with the same name [apply_request_filter] is not supported`.

When this command's effective value is `false`, the builder calls `SourceCommandNamedParameters.disableUnspecifiedRequestFilterInChildren` on the source-list plan. That copies a non-explicit `false` onto each `UnresolvedRelation` in that plan that did not write the key. An omitted `WITH` and an explicit `true` both have `applyRequestFilter() == true` and do not propagate. The walk is `transformDown` over plan nodes. It does not visit `InSubquery` or `MultiColumnInSubquery` expressions. The plans stay the plans the parser already built.

A `LET` body is not walked here. `LetResolver` applies the same copy at each use site, using the flag on the relation that named the binding. A parent relation's flag is not consulted while substituting a name inside an `IN`.

`FROM (FROM b | KEEP id) WITH {"apply_request_filter": false}` is legal. The outer command has no pattern of its own; the clause applies to `FROM b`.

The option is only meaningful when the request body has a DSL `filter`. After parse, and before `LET` substitution, `EsqlSession` checks `request.filter()`. When that is null, `SourceCommandNamedParameters.rejectWithoutRequestFilter` walks the unwrapped main plan and every `LET` binding, including a binding the main query never uses. The walk visits plan children and `IN` subquery expressions. A command that wrote `WITH {"apply_request_filter": true}` or `false` throws `VerificationException`: `apply_request_filter cannot be set when the request has no filter, found at [<source text>]`. A child that only inherited `false` is not a separate failure, because the check uses `applyRequestFilterExplicit()`. A `match_all` or empty `bool` is still a filter in the body, so the check allows it. View definitions are unchanged: creation already rejects the option, and `FROM view_name WITH {...}` is the query this check sees.

Validation, at the `WITH` source location:

| Input | Result |
| --- | --- |
| no `WITH` | `CommandNamedParameters.EMPTY`. `applyRequestFilter()` is true. The key is not explicit. |
| `{"apply_request_filter": false}` | value false, explicit |
| `{"apply_request_filter": true}` | value true, explicit |
| unknown key | `Option [key] is not supported` |
| duplicate key | `Duplicated named parameters with the same name [apply_request_filter] is not supported` |
| non-boolean literal, including `"false"` | `The value of option [apply_request_filter] must be the literal true or false` |
| `null` | `FROM option [apply_request_filter] has null value; null is not a valid option value` |
| a parameter or a nested map | `FROM options must be literal values; option [apply_request_filter] has expression [...]` |
| `WITH {}` | `Empty option is not supported` |

`TS` keeps its existing rejection of subqueries and of concrete view names. The clause does not make a view legal under `TS`. The subquery rejection happens in `visitRelation` before the flag is copied.

### Where the flag lives

The flag is coordinator-only. It is not written by `writeTo`. Data nodes never read it. No new `TransportVersion`, and `generateTransportVersion` is not run. A data node that reconstructs `EsRelation` or `ExternalRelation` sets the flag to true.

`CommandNamedParameters` is the options object on one unresolved source command. Each entry is `Option(value, explicit)`. The public constructor marks every written key explicit. `withInherited(key, value)` adds a non-explicit value only when the command did not write that key, and it copies the map because several relations can share one instance (`withIndexPattern`, `addMetadataField`) and `EMPTY` is `Map.of()`. `SourceCommandNamedParameters` is the static helper: the key name, validation, the child walk, the view and no-filter rejections, and the pre-analysis sets. The two classes stay separate. The constant `APPLY_REQUEST_FILTER` lives on the helper.

One `FROM` or `TS` produces one `UnresolvedRelation`. Later resolution may turn that relation into several branches (a view, a dataset, and a leftover index pattern). Every branch inherits the same decision. That is a field copy inside the resolvers that already split the relation.

| Node | What it stores | Consumed by |
| --- | --- | --- |
| `UnresolvedRelation` | `CommandNamedParameters`. Absent key means apply true. | field-caps, and every resolver that copies the relation |
| `EsRelation` | `boolean applyRequestFilter`, not serialized. Plan text appends `[apply_request_filter=false]` only when false. | `integrateEsFilterIntoFragment`, `MetricsInfoTimeRangeRewriter` |
| `UnresolvedExternalRelation` | `CommandNamedParameters`, same rules. Inline `EXTERNAL` is built with `EMPTY`. A dataset branch is built with the `FROM` command's map. | listing-path opt-out, then `ExternalRelation` |
| `ExternalRelation` | `boolean applyRequestFilter`, not serialized, not printed in plan text | `RequestFilterRewriter` |
| `BranchIdentity` | `boolean applyRequestFilter`. `INDEX` and `SUBQUERY` constants are true. `view(name)` and `dataset(name)` default true. `withApplyRequestFilter` is what resolvers use. | `ViewRequestFilterRewriter` and `Mapper`, and only for a branch those readers treat as a view |
| `ViewShadowRelation`, `DatasetShadowRelation` | the same boolean | linked field-caps |

`UnresolvedRelation` already throws from `writeTo`. `EsRelation.writeTo` / `readFrom` stay byte-compatible. `ExternalRelation` already omits coordinator-only fields from the wire. `BranchIdentity` is not a `Writeable`.

`ViewCompaction.toBranchPlan` still assigns the raw `BranchIdentity.INDEX`, `SUBQUERY`, and `view(name)` constants, all with the flag true, when it wraps a plain `UnionAll`. That does not re-enable the filter. `ViewRequestFilterRewriter` and `Mapper` read `identity.applyRequestFilter()` for view branches, and `ViewResolver` is what tags those. Leaves keep their own flags. Tests must not expect those constants to become false.

Plan text prints the flag only on `EsRelation`, and only when it is false, so existing golden plans do not move. `PlanAnonymizer` does not need a new index token. The value is a boolean.

### Copy sites

A missed copy silently applies the filter again. Each of these carries the flag:

- `UnresolvedRelation` constructors, `withIndexPattern`, `addMetadataField`, and `NodeInfo`. The options object is shared until `withInherited` replaces it.
- `Analyzer`, when it builds `EsRelation` from `UnresolvedRelation`, and when it copies a shadow's flag onto the resolved relation.
- `ViewResolver.buildPlanFromBranches`, from the call-site `UnresolvedRelation`:
  - A computed body (`NamedSubquery`, or any plan that is not a bare standard `UnresolvedRelation` or a `ViewShadowRelation`) becomes `BranchIdentity.view(name, callSiteFlag)`. The body is not rewritten.
  - A pass-through body (bare standard `UnresolvedRelation`) gets `disableUnspecifiedRequestFilterInChildren` when the call site opted out. The identity is `INDEX.withApplyRequestFilter(callSiteFlag)`. Filtering that index is filtering the view output, so the branch can collapse.
  - A `ViewShadowRelation` stays bare, with `INDEX.withApplyRequestFilter(callSiteFlag)`. `ViewCompaction` strips it.
  - The single-entry short-circuit returns the raw body only when `boundaryPreservation.keepsViewBranches()` is false. A request filter sets that, so `buildPlanFromBranches` runs and the flag is copied there, not at parse. A view definition cannot contain an explicit `WITH`, so the pass-through copy is filling in a command that did not write the key.
- `LetResolver`, when a binding token is replaced. The leftover non-binding patterns keep the use-site options through `withIndexPattern`. Each binding inserted for a token of an opted-out relation is the stored plan, then `disableUnspecifiedRequestFilterInChildren` on that copy. The stored binding is not mutated. Substitution inside an `InSubquery` does not take the flag from a parent command outside the `IN`.
- `BranchIdentity.liftedThrough`: a piece lifted through a view takes the outer view's flag. Lifting through a non-view returns `this`, flag included.
- `DatasetRewriter`. `buildDatasetBranch` receives the command's `CommandNamedParameters` and stores them on the `UnresolvedExternalRelation`. `BranchIdentity.dataset(name, flag)` and a leftover index branch use `INDEX.withApplyRequestFilter(flag)`.
- `ViewCompaction.mergeIfPossible`. Concatenate only when `commandNamedParameters` are equal, including which keys were written. Two commands that opted out differently stay separate. Queries that do not use the clause are unaffected, because every map is empty.
- `EsqlSession.scopeIndexPatterns`, which rebuilds an `UnresolvedRelation` through `withIndexPattern`.

View-internal patterns stay on the existing `viewInternalIndexPatterns` exemption. The call-site flag is not copied onto them.

### The four skip points

#### 1. Lucene stamp and `can_match`

`PlannerUtils.integrateEsFilterIntoFragment` skips a `FragmentExec` when `fragmentOptsOut` is true: any `EsRelation` or `ExternalRelation` in the fragment has `applyRequestFilter == false`.

`Mapper`, after it marks a view-branch fragment with `asFromViewBranch`, also calls `SourceCommandNamedParameters.optOutResolved` on that fragment when the branch identity's flag is false. That sets the leaf relations to false on the physical copy. The view body's logical plan is left alone until then. This is what skips the Lucene fallback on a cluster too old for the output rewrite. View branches from a command that did not opt out keep today's rule: skip the stamp only when the output rewrite exists, and stamp as the fallback when it does not.

`executeSubPlan` does not grow a null-filter path. The subquery's own `FROM` carries the flag, and the same stamp method sees it. `SemiJoin`, `AntiJoin`, `MarkJoin`, and a PromQL `InnerJoin` need no special case. A PromQL plan has no clause, so its fragments stay stamped.

`can_match` (`detectFilter`) unions fragment `esFilter`s with a `FILTER` conjunction and the data node applies that one query to every shard of the plan. It is shard skipping, not a second `WHERE`. A fragment that opted out has a null request `esFilter`. Another command's request filter in the same physical plan would still be ANDed into the node-level `can_match` and could drop shards of the opted-out index.

Rule: the request-filter portion of `can_match` is included only when every `FragmentExec` in that physical plan still applies the request filter. Query-text filters pushed onto a single relation still contribute. Consequences:

- One command, clause absent or `true`: `can_match` unchanged.
- An `IN` subquery whose `FROM` opted out: that subplan's fragments have no request filter, so its `can_match` does not contain it. The outer plan still prunes. This is the original bug, and the two plans are already separate.
- A union of two commands in one physical plan, only one of which opted out: `can_match` drops the request-filter conjunct. Per-fragment Lucene filters still apply to the opted-in command, so results stay correct. Shard skipping is weaker on that plan only.

Per-fragment `can_match` would preserve skipping on the filtered side of a mixed union. That is a follow-up, not part of this change. The doc's "kept for filtered sources" holds for the separate-subplan case, which is the `IN` case.

`can_match` does not follow the field-caps true-wins rule. Fragments can share a shard, and adding the opted-in fragment's filter there would skip a shard the opted-out fragment still needs.

#### 2. Field-caps

`preAnalyzeMainIndices` passes `null` instead of `request.filter()` when the `IndexPattern` is in `SourceCommandNamedParameters.optedOutIndexPatterns`, or when it is already view-internal. Both local call sites use that. Linked-project resolution uses `optedOutLinkedPatterns` on the pattern string, for `ViewShadowRelation` and `DatasetShadowRelation`.

Do not encode the flag in `IndexPattern.equals`. The same pattern string can still appear in two commands (`FROM logs-*` and `FROM logs-* WITH {"apply_request_filter": false}` inside an `IN`). Those `IndexPattern`s compare equal, so pre-analysis has one field-caps entry and cannot both prune and not prune. A second field-caps call is not added.

Send `index_filter` when any relation with that `IndexPattern` still applies the request filter. Omit it only when every relation with that key opted out. The opted-in reference then resolves against the filtered index set, so its mapping is not widened by indices the filter would have dropped. The opted-out reference searches those same concrete indices without the request filter, and still sees documents the filter would remove inside an index field-caps kept. An index the filter excludes entirely is not returned, so the opted-out reference cannot read it either.

`optedOutIndexPatterns`, `optedOutLinkedPatterns`, and `fullyOptedOutExternalPaths` implement that rule with `Map.merge(..., Boolean::logicalOr)` and then keep the keys whose merged value is false. The index-pattern walk does not enter `IN` expressions that have not yet been rewritten into plan nodes. By the time `EsqlSession` calls it, those expressions are plan nodes.

`analyzeWithRetry` retries without the filter when pruning removes every index. An opted-out command never entered that path, because it was not sent an `index_filter`. A pattern that kept `index_filter` because another command still applies the filter uses the existing retry. No new retry condition.

#### 3. Views

`FROM my_view WITH {"apply_request_filter": false}` is legal in a query. `ViewRequestFilterRewriter` does not insert a `Filter` on a view branch whose identity opted out, and it does not warn. The skip is what the command wrote. A view named by a different command is still filtered. The walk rule is unchanged: do not enter a view branch.

`FROM my_view, other_view WITH {"apply_request_filter": false}` opts out both views, because they are one command. `FROM test, (FROM my_view | KEEP id) WITH {"apply_request_filter": false}` opts out `test` and `my_view`. To filter `test` and not the view, put the clause only on the inner command: `FROM test, (FROM my_view WITH {"apply_request_filter": false} | KEEP id)`.

`preserveViewBoundaries` is `ViewRequestFilterRewriter.appliesToViewOutputs(requestFilter)` or the existing unmapped-fields / view-settings requirement. It does not look at the per-command flags. A request filter that applies to view outputs keeps the boundaries even when every view command opted out. That is deliberate over-preserving. Under-preserving would push the DSL into a view's sources. The "turn preservation off when every view opted out" optimization is not implemented.

View creation rejects the clause. `PUT /_query/view/{name}` stores only the ES|QL query text, and that text must not contain `WITH {"apply_request_filter": false}` or `WITH {"apply_request_filter": true}`. A view has no place to store a per-request DSL decision, and a flag on a source inside the body would not mean the same thing as the caller's request filter, which is applied to the view output.

`ViewService` parses the query with `EsqlParser.parseView` before the view is written to cluster state. `parseView` calls `SourceCommandNamedParameters.rejectInView` after a successful parse, so the put fails and nothing is stored. `PUT` with `validate=true` uses the same parse and fails the same way. Message:

```text
apply_request_filter cannot be set on a source inside a view definition [name].
The request filter applies to the view's output. Opt out from the query with
FROM [name] WITH {"apply_request_filter": false}
```

Both values are rejected, on every source command the walk can see: the top-level `FROM` or `TS`, a source-list subquery, an `IN` source, and an `UnresolvedExternalRelation` that wrote the key. A nested view name inside the definition is fine. The clause on the query that later reads this view is the opt-out. On a release build the clause fails earlier, as a syntax error, before this message.

An `IN` subquery that lives inside a view body cannot opt out, because the body cannot contain the clause. The query that reads the view can opt the whole view out, or it can write the `IN` itself and opt that `IN`'s `FROM` out.

#### 4. Datasets

`RequestFilterRewriter`'s target predicate requires `relation.applyRequestFilter()` and requires that the relation is not inside a view. An opted-out `ExternalRelation` is not wrapped. On a cluster that supports `esql_request_filter_on_dataset`, it is also absent from the "not fully applied" warning, because it is not a rewrite target.

On a cluster below `esql_request_filter_on_dataset`, `warnNotApplied` splits the warning:

- relations that still want the filter: `Request filter not applied to external datasets [{}], a node is too old to evaluate it; use WHERE instead`
- relations with the flag false: `Request filter not applied to external datasets [{}] because apply_request_filter is false`

A `FROM` that names several datasets and opts out skips the filter for every dataset that command resolves to. `PartitionSpec.addTimestampBounds` attaches the request filter's time bounds to an external path unless every reference to that path opted out (`fullyOptedOutExternalPaths`). One reference that still applies the filter keeps the bounds, so its file list and schema stay inside the filter window. `WHERE` bounds extracted from the query text still apply. `folderRanges` derived only from the request filter follow the same split: the bounds are removed from `boundedPathConfigs` before `addTimestampBounds`, and paths that were removed do not get request-filter bounds.

Row-group pruning that uses the translated request filter does not see a filter on an opted-out dataset, because the logical `Filter` was not inserted.

A dataset inside an opted-out view is already excluded by `datasetsInsideViews`. The dataset leaf's own flag is not a second condition for that case. `Mapper.optOutResolved` still clears the leaf flag so the Lucene fallback does not stamp the view's sources.

### Inline `EXTERNAL`

```esql
EXTERNAL "file:///data.parquet" WITH {"format": "parquet", "apply_request_filter": false}
```

This does not opt out. `visitExternalCommand` folds the whole map into the external config. `apply_request_filter` is not a storage or format key, so `ConfigKeyValidator` rejects it as an unknown option (`unknown option [apply_request_filter] ...`). The parsed `UnresolvedExternalRelation` keeps `CommandNamedParameters.EMPTY`, so `applyRequestFilter()` stays true. Existing `EXTERNAL` queries that do not use the key are unchanged.

`FROM dataset` is the path that carries the flag. `DatasetRewriter` copies the command's `CommandNamedParameters` onto the `UnresolvedExternalRelation` and the dataset branch identity. After resolution, that relation shares `RequestFilterRewriter` and the listing-bounds rule with any other external relation.

### `TS`

`TS metrics WITH {"apply_request_filter": false}` is the same options object on a time-series `UnresolvedRelation`. Several patterns in one `TS` share it. Time-series index-mode checks are unchanged. Concrete views under `TS` are still rejected. `TS` still rejects a source-list subquery before propagation runs.

`MetricsInfoTimeRangeRewriter.capTimeRange` caps `TS_INFO` and `METRICS_INFO`. For an `EsRelation` that opted out, the lower bound is `now - cap`. For a relation that did not, it is `requestedEnd - cap`, or `now - cap` when the request filter has no range. The early return (the requested window is already inside the cap) is skipped when any `EsRelation` in the plan opted out, so an opted-out relation is not left uncapped. The warning still fires when a bound is inserted. A non-tsdb relation, or one without `@timestamp`, is skipped.

### Remote and cross-project

The field-caps exemption is the null `index_filter` above, on the cross-project call site too. `FROM remote:logs-* WITH {"apply_request_filter": false}` opts out that command. Linked patterns from a command that did not opt out keep the filter. View shadows and dataset shadows are index reads of a remote namesake. They follow the command flag that produced them, and `index_filter` is omitted for a pattern string only when every shadow with that string opted out.

### Security, serverless, mixed versions

Document-level and field-level security are applied by the search layer, not by this filter. Opting out does not grant access to documents the role cannot read. A role that forbids DLS on a view is unchanged.

Serverless uses the same coordinator path. No new project setting.

Mixed clusters:

- Old coordinator: the syntax is a parse error.
- New coordinator, old data node: the fragment is shipped with `esFilter` already omitted. The data node does not re-stamp. No transport version.
- View fallback on a cluster whose minimum version is below `esql_request_filter_on_dataset`: commands that did not opt out keep the current Lucene fallback. Opted-out view commands skip that fallback too, because `Mapper` cleared the leaf flag before the stamp.

### Capability

The capability name is `apply_request_filter_source_option`. It is present only when `Build.current().isSnapshot()` is true, which is what the nodes-capabilities API reports. Kibana uses that to hide autocomplete for the clause on release builds. Do not add a cluster setting or `withPreview()`.

User docs are not updated on this branch.

Kibana does not sync this grammar into a release build. On a snapshot build it can offer the key inside a `FROM` or `TS` `WITH` map when the capability is reported. No request or response schema change.

## Implementation sequence

Done on this branch:

1. Capability `APPLY_REQUEST_FILTER_SOURCE_OPTION(Build.current().isSnapshot())`, last in `EsqlCapabilities`. Grammar as in [Syntax](#syntax). Parser regenerated with `:x-pack:plugin:esql:regen`.
2. `CommandNamedParameters` on `UnresolvedRelation` and `UnresolvedExternalRelation`. Boolean on `EsRelation`, `ExternalRelation`, `BranchIdentity`, and the two shadow relations. `writeTo` / `readFrom` stay byte-compatible.
3. `LogicalPlanBuilder.visitRelation` parses the one map, validates it, and propagates `false` only when this command wrote `false`. `visitExternalCommand` is not modified.
4. `LetResolver` copies `false` onto a copied binding when that use opted out.
5. Field-caps omits `index_filter` only when every reference opted out. Listing bounds follow the same rule per path. `can_match` stays permissive when flags differ.
6. `integrateEsFilterIntoFragment` and the `can_match` uniformity rule. `Mapper` clears leaf flags on an opted-out view branch.
7. `ViewRequestFilterRewriter` skip, and `parseView` rejection. `preserveViewBoundaries` is unchanged and over-preserves.
8. `RequestFilterRewriter` skip, the split old-cluster warning, listing-bounds split, and the `TS_INFO` / `METRICS_INFO` cap.
9. `VerificationException` when the request body has no DSL `filter` and the query wrote the option, before view resolution.

Not done, and not part of the engine work: user docs. The tests still to write are the next section.

## Test plan

Tests that parse or execute the clause start with `assumeTrue(EsqlCapabilities.Cap.APPLY_REQUEST_FILTER_SOURCE_OPTION.isEnabled())`, except the release-build parser test, which assumes the opposite. There is no YAML REST file for this change.

The release-build parser test is skipped unless the test JVM is a release build. `Build.current().isSnapshot()` reads `build.snapshot`. Run:

```
./gradlew :x-pack:plugin:esql:test \
  --tests org.elasticsearch.xpack.esql.parser.ApplyRequestFilterParserTests.testReleaseBuildRejectsWithClause \
  -Dbuild.snapshot=false \
  -Dtests.jvm.argline=-Dbuild.snapshot=false
```

Default and explicit `true` must match today's results. `EsRelation` plan text gains `[apply_request_filter=false]` only when the flag is false, so existing goldens stay put without a new "clause absent" golden. Every positive execution case uses a filter whose opt-in and opt-out results differ.

Fixture unless a case says otherwise: index `test` with `id` 1..6 and a `color` of `red` for odd ids and `blue` for even ids. Request filter `{"range": {"id": {"lte": 3}}}`. Subquery text:

```esql
FROM test WITH {"apply_request_filter": false}
| WHERE id > 3
| EVAL id = id - 3
| KEEP id
```

Call that command `S`. It returns no rows when the clause is removed, and ids 1, 2, 3 when the clause is present.

Where an `IN` has two execution paths, force the hash join with the `in_subquery_hash_join_threshold` pragma, the way `InSubqueryIT` splits `FilterAndHashJoinPathsAgree`. Do that for the one `WHERE id IN (S)` case. The other `IN` forms need one path. The flag is on the subquery source before `SemiJoin`, `AntiJoin`, or `MarkJoin` is chosen, and `executeSubPlan` stamps every subplan with the same method.

### Already covered

Do not re-write these.

`ApplyRequestFilterParserTests`:

- `FROM a, b` is one relation, pattern `a,b`, flag true, not explicit.
- `FROM a, b WITH {"apply_request_filter": false}` is one relation, flag false, explicit.
- The same with explicit `true`: flag true, explicit.
- `TS idx WITH {"apply_request_filter": false}` keeps time-series mode and sets the flag.
- `FROM idx METADATA _id WITH {"apply_request_filter": false}` accepts `METADATA` before the map.
- `FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM other | KEEP id)` opts out `test` and leaves `other` at the default.
- `FROM a, (FROM b | KEEP id) WITH {"apply_request_filter": false}` opts out `a` (explicit) and `b` (inherited).
- `FROM a, (FROM b WITH {"apply_request_filter": true} | KEEP id) WITH {"apply_request_filter": false}` leaves `b` explicit true.
- `rejectWithoutRequestFilter` throws for `FROM` and `TS` writing `true` or `false`, for a source-list subquery that wrote the key, for an `IN` source that wrote the key, and for an unused `LET` binding. `FROM idx` does not throw. This calls the static method. It does not go through `EsqlSession`.
- `parseView` rejects top-level `FROM idx` with `true` and with `false`.
- Errors: `WITH` between patterns, `WITH {}`, unknown key, non-boolean `1`, duplicate key, `METADATA` after `WITH`.
- Release build: `FROM idx WITH {...}`, `FROM idx METADATA _id WITH {...}`, and `TS idx WITH {...}` fail at `WITH`. `FROM idx` still parses.

`SourceCommandNamedParametersTests`: mixed flags on one index pattern, one linked shadow pattern, or one external path do not opt that key out. All references opted out, and a separate pattern that opted out, do.

`RequestFilterRewriterTests.testVersionGateNamesDatasetsThatDisabledTheRequestFilter`: below `esql_request_filter_on_dataset`, an opted-out dataset gets the `apply_request_filter is false` warning and a dataset that still wants the filter gets the "node is too old" warning.

`MetricsInfoTimeRangeRewriterTests` covers the cap for relations that did not opt out. It does not cover an opted-out relation.

### Parser gaps

Add to `ApplyRequestFilterParserTests`. Same plan-walk style as the tests already there.

- `FROM a, (FROM b WITH {"apply_request_filter": false} | KEEP id)` sets the flag only on `b`.
- `FROM (FROM b | KEEP id) WITH {"apply_request_filter": false}` sets the flag on `b`, inherited.
- `FROM (FROM b, (FROM c | KEEP id) | KEEP x) WITH {"apply_request_filter": false}` sets the flag on `b` and on `c`.
- `FROM a, (FROM b | WHERE id IN (FROM c | KEEP id)) WITH {"apply_request_filter": false}` sets the flag on `a` and `b`, and leaves `c` at the default.
- `TS a, b WITH {"apply_request_filter": false}` is one relation, pattern `a,b`, time-series mode, flag false.
- `TS` still rejects a subquery, with or without the clause.
- `null`, a parameter, a nested map, and the string `"false"`. Assert the messages in the validation table, not a single "must be boolean" string.
- `parseView` also rejects the clause on `TS`, on a source-list subquery, and on the `FROM` of an `IN` inside the view body. A body with no clause is accepted, including a body that is only an `IN` over `FROM test`.
- On a release build, `parseView` of a body that contains the clause fails as a syntax error (`WITH`), before the view-definition message. The view-definition message is asserted only when the capability is enabled.
- `EXTERNAL "p" WITH {"format": "parquet", "apply_request_filter": false}` parses with `applyRequestFilter()` true and the key still in the config map. It is not a successful opt-out. Do not assert that the request filter was skipped. Unknown-key rejection stays with the existing external-option checks (`unknown option [apply_request_filter]`).
- `LOOKUP JOIN idx WITH {"apply_request_filter": false}` fails to parse.

`LetResolver` is not a parser test. Cover it next to the other `LET` unit tests:

- `LET sub = (FROM b | KEEP id); FROM a, sub WITH {"apply_request_filter": false}` leaves `a` opted out and the copied body of `sub` opted out on its source command. The stored binding is not that plan node.
- `LET sub = (FROM b WITH {"apply_request_filter": true} | KEEP id); FROM sub WITH {"apply_request_filter": false}` leaves `b` explicit true.
- `LET sub = (FROM b | KEEP id); FROM sub WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id)` opts out the first use and leaves the `IN` use filtered.

The no-filter check's walk is already unit-tested. `SubqueryRequestFilterIT` adds one request that writes the clause and has no `filter` in the body, and expects `VerificationException` with `cannot be set when the request has no filter`. That is the `EsqlSession` wiring. There is no REST file for it.

### Plan flag survives resolution

Unit tests. Build the parsed plan and run the resolver or the copy helper. These are not cluster tests.

- `withIndexPattern` and `addMetadataField` keep the same `CommandNamedParameters`.
- `ViewResolver` on a computed view, `FROM my_view WITH {"apply_request_filter": false}`, stores the flag on `BranchIdentity.view` and does not rewrite relations inside the body.
- The same call on a pass-through view copies inherited `false` onto the index relation and uses `INDEX.withApplyRequestFilter(false)`. The branch is not a view branch.
- A request filter is present and the only view command opted out: the computed view branch is still preserved. This locks the current over-preserve. Do not assert that the boundary was dropped.
- `liftedThrough` copies the outer view's flag.
- `DatasetRewriter` copies the command's `CommandNamedParameters` onto the dataset's `UnresolvedExternalRelation`, and the same boolean onto the dataset identity and a leftover index branch.
- `ViewCompaction.mergeIfPossible` concatenates two relations whose options are equal, and returns null when one wrote `false` and the other did not. Do not assert that `BranchIdentity.INDEX` or `SUBQUERY` become false for a plain `UnionAll`.
- `Analyzer` copies the flag onto `EsRelation`.
- `EsRelation` and `ExternalRelation` stream round-trip without the field and without a new transport version: write on a current node, read back, flag is true. Assert the named writeable still reads with the current `TransportVersion` and with `esql_es_relation_slices` and `dataset_declared_schema`.

`RequestFilterGoldenTests`: one opted-out plan whose `EsRelation` line contains `[apply_request_filter=false]`, and whose unresolved shape is still one relation for `FROM a, b WITH {"apply_request_filter": false}`. No new golden for a query that omits the clause. `ExternalRelation` text is not expected to contain the flag.

### Lucene stamp and `can_match`

One planner unit test, one physical plan per case:

- Opted-out `EsRelation`: `FragmentExec.esFilter()` is null. The fragment of a command that did not opt out carries the request filter.
- Opted-out view branch: `Mapper` has cleared the leaf flags, so fragments under it have a null request `esFilter` even when `minimumVersion` does not support `esql_request_filter_on_dataset`. A view command that did not opt out, on that old version, is still stamped.
- A view command that did not opt out, on a new version: still not stamped, and `ViewRequestFilterRewriter` has inserted the logical `Filter`.
- `can_match` for a plan whose every fragment still applies the request filter contains it.
- `can_match` for a plan with one stamped fragment and one opted-out fragment does not contain the request filter. A `WHERE` on the opted-out relation that constrains `@timestamp` still contributes.

Skip a PromQL `InnerJoin` case. PromQL has no `WITH`, and the stamp method has no PromQL branch. Existing PromQL filter tests cover a plan that never opts out.

Integration tests are new classes under `x-pack/plugin/esql/src/internalClusterTest/java/org/elasticsearch/xpack/esql/action/`. Do not add these cases to `InSubqueryIT`, `ViewRequestFilterIT`, `RequestFilterIT`, `RequestIndexFilteringIT`, `AnalyzeRetryRequestFilterIT`, `CrossClusterInSubqueryIT`, `FromDatasetSubqueryIT`, `ExternalDatasetRequestFilterConformanceIT`, `ExternalRequestFilterRowGroupPruningIT`, or `MetricsAndTSInfoIT`. Those suites already carry their own matrices.

### `SubqueryRequestFilterIT`

Index sources only. `FROM` subqueries and `IN` subqueries. No view, no dataset. One assertion per form below. The clause-absent result is the pair of the clause-present result, in the same test, so a default regression fails here.

The earlier matrix walked every `IN` spelling (`CASE`, `COALESCE`, `IS NULL`, `IS NOT NULL`, multi-column, `BY color`, a second aggregate). Those spellings change how `IN` handles nulls or grouping. They do not change where the request filter is applied. `S` already returns either no rows or ids 1, 2, 3 before that logic runs. Keep the forms that are different subplans or the isolation case.

- `WHERE id IN (S)`, on the filter path and the hash-join path. No rows without the clause. Ids 1, 2, 3 with it.
- `WHERE id NOT IN (S)`. Ids 1, 2, 3 without the clause. No rows with it.
- `EVAL flag = id IN (S) | KEEP id, flag`. The outer `FROM` stays filtered, so the outer rows are 1, 2, 3 either way. `flag` is false without the clause and true with it. This is the `MarkJoin` path.
- `STATS c = COUNT(*) WHERE id IN (S)`. 0 without the clause, 3 with it.
- The same aggregate as `INLINE STATS`.

Parent `WITH` must not leak. The request filter is `id <= 3`. The `IN` source has no clause of its own, so it returns no rows. The parent command is unfiltered, so its rows are ids 1 through 6.

- `FROM test WITH {"apply_request_filter": false} | WHERE id IN (...)` returns no rows. A leak would return 1, 2, and 3.
- The same query with `NOT IN` returns ids 1 through 6. A leak would return ids 4, 5, and 6.
- `EVAL flag = id IN (...)` is false on every parent row. `STATS c = COUNT(*) WHERE id IN (...)` is 0. One of those two is enough if the `WHERE` pair above is already in the class. Prefer the `WHERE` pair. Add `EVAL` only if the `WHERE` pair is awkward to assert.

Nested and sibling:

- `WHERE id IN (FROM test | WHERE id IN (S) | KEEP id)`. The inner command opts out. The middle `FROM test` stays filtered and returns ids 1, 2, 3.
- `WHERE id IN (FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id) | KEEP id)`. Only the middle command opts out. The inner source stays filtered and returns no rows, so the outer `IN` matches nothing.
- The two-sibling example in the behavior section returns ids 1 and 3.

`FROM` subqueries, in the same class:

- `FROM test, (FROM test WITH {"apply_request_filter": false} | WHERE id > 3)` returns the filtered side's ids 1, 2, 3 and the opted-out side's ids 4, 5, 6.
- `FROM test, (FROM test | WHERE id > 3) WITH {"apply_request_filter": false}` opts out both source commands. The outer side returns ids 1 through 6. The inner side still has `WHERE id > 3`, so it returns ids 4, 5, and 6.
- `FROM test, (FROM test | WHERE id IN (FROM test | WHERE id > 3 | KEEP id) | KEEP id) WITH {"apply_request_filter": false}` opts out the outer `test` and the middle `FROM test`. The `IN` source stays filtered and returns no rows.
- `FROM test, test WITH {"apply_request_filter": false} | WHERE id > 3` opts out the whole command and returns ids 4, 5, 6. The command stays one relation. This is the case that must not become a filtered union plus an unfiltered union.

One request writes the clause and sends no DSL `filter`. It throws `VerificationException` with `cannot be set when the request has no filter`. The walk itself is already unit-tested. This request is the `EsqlSession` wiring.

`LET` at execution, still index sources only:

- `LET s = (S); FROM test | WHERE id IN (FROM s | KEEP id)` returns ids 1, 2, 3.
- `LET sub = (FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id); FROM test | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id)` returns ids 1, 2, and 3. `FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id)` does not opt `sub` out, so the query returns no rows.

The plan-identity cases stay in the `LetResolver` unit test. `FORK` needs no cluster test. A parser assertion that two `FORK` branches keep independent flags is enough, and only if `FORK` syntax is easy to add beside the other parser cases. Skip it if the grammar makes that assertion noisy. The flag is per `FROM`.

### `SubqueryViewRequestFilterIT`

Views over indices. A view in the main query, a view inside a `FROM` subquery, a view inside an `IN` subquery, and a view named inside a `LET` body. No dataset. Result-level only. The pass-through versus computed split stays the `ViewResolver` unit test.

- A view over `test` that keeps `id`, queried as `FROM view WITH {"apply_request_filter": false}`, returns ids 1..6. `FROM view` with the same request filter returns ids 1, 2, 3. The view's own `WHERE`, if it has one, still applies in both.
- `FROM view, test WITH {"apply_request_filter": false}` leaves both unfiltered. `FROM test, (FROM view WITH {"apply_request_filter": false})` filters `test` and does not filter the view.
- A view defined as `FROM test | WHERE id > 3 | EVAL id = id - 3 | KEEP id`, queried as `FROM test | WHERE id IN (FROM view WITH {"apply_request_filter": false} | KEEP id)`, returns ids 1, 2, and 3. `FROM test | WHERE id IN (FROM view | KEEP id)` returns no rows.
- A view that computes `id` with `EVAL`, queried with `FROM view WITH {"apply_request_filter": false}`, returns the computed rows the filter would have removed. This is the case that fails if either the output skip or the Lucene skip is missing. The existing "caller did not opt out" rewriter tests stay as they are. Do not add a duplicate of them.
- `LET sub = (FROM view | KEEP id); FROM test | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id)` opts the view out at that use and returns ids 1, 2, and 3 when `view` is the `id > 3` view above. The same `IN` without the clause on `FROM sub` returns no rows. `FROM test, sub WITH {"apply_request_filter": false}` opts the view out on the source-list use. `FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id)` does not: the parent's clause does not enter the `IN`, so the view stays filtered and the query returns no rows.

`PUT /_query/view` is an integration case in this class, through `PutViewAction` and `ValidateViewAction`. It is not added to `ViewIT`. The parser tests still cover where in the view text the clause is rejected. The cluster test covers that the put fails before the view is stored.

- `PUT` of `FROM idx WITH {"apply_request_filter": false}` fails with the view-definition message, and a following get says the view is missing. Repeat with `true`.
- The same pair on a source-list subquery inside the view query, and on the `FROM` of an `IN` inside the view query.
- `ValidateViewAction` (`PUT` with `validate=true`) fails the same way and does not create the view.
- A put whose query has no clause creates the view. An `IN` inside that body cannot carry the clause either; that rejection is the parser case. The caller opts the view out later with `FROM view WITH {"apply_request_filter": false}`.

### `IndexPatternRequestFilterIT`

Field-caps and `analyzeWithRetry`, on indices only. The shared-pattern set rule is already unit-tested. These cases are the field-caps call and the rows.

- Two indices, `has_ts` with `@timestamp` and `no_ts` without it. Filter is a range on `@timestamp`. `FROM has_ts, no_ts` drops `no_ts` via `index_filter`, as today.
- `FROM has_ts, no_ts WITH {"apply_request_filter": false}` reads both. One field-caps call, no `index_filter`.
- `FROM has_ts, (FROM no_ts WITH {"apply_request_filter": false})` keeps `has_ts` pruned and reads `no_ts`.
- The same pattern string in two commands (`FROM logs-* | WHERE id IN (FROM logs-* WITH {"apply_request_filter": false} | KEEP id)`) keeps `index_filter`, because the outer command still applies the filter. The opted-out side still returns rows the filter would remove inside an index field-caps kept. An index the filter excludes entirely is not visible to either side.
- `analyzeWithRetry`: a filter that matches no index still retries when the command did not opt out. An opted-out command that would have matched nothing is not an analysis failure.

### `CrossClusterSubqueryRequestFilterIT`

One remote main command opted out reads the remote index the filter would have dropped. One `IN` case is the same shape as `WHERE id IN (S)` with the subquery index remote. Do not repeat the local `IN` matrix across clusters.

### `FromDatasetSubqueryRequestFilterIT`

Datasets on a current cluster. A dataset in the main query, a dataset inside a `FROM` subquery, and a dataset inside an `IN` subquery. Also a view whose body reads a dataset, when that view is used from a `FROM` subquery, an `IN` subquery, or a `LET` body. The old-cluster warning split stays in `RequestFilterRewriterTests`. Do not add a mixed-cluster IT for that warning. No inline `EXTERNAL` success case.

- `FROM dataset` with a request filter keeps today's translated filter and today's listing bounds.
- `FROM dataset WITH {"apply_request_filter": false}` returns rows the filter would have removed, does not emit the "not fully applied" warning, and lists files outside the filter's time bounds. A `WHERE` on the same column still filters and still prunes listing.
- `FROM opted_in, opted_out WITH {"apply_request_filter": false}` skips the filter for both datasets, because they are one command.
- `FROM opted_in, (FROM opted_out WITH {"apply_request_filter": false})` applies listing bounds and a translated `Filter` only on `opted_in`.
- `FROM test | WHERE id IN (FROM dataset WITH {"apply_request_filter": false} | KEEP id)` reads the dataset without the request filter. The same `IN` without the clause keeps it.
- A view defined over a dataset, queried as `FROM test, (FROM view WITH {"apply_request_filter": false})` and as `FROM test | WHERE id IN (FROM view WITH {"apply_request_filter": false} | KEEP id)`. The view output is unfiltered. The same references without the clause stay filtered. The raw DSL is not pushed into the dataset inside the view when the caller does not opt out.
- The same view named inside a `LET`: `LET sub = (FROM view | KEEP id); FROM test | WHERE id IN (FROM sub WITH {"apply_request_filter": false} | KEEP id)` and `FROM test, sub WITH {"apply_request_filter": false}` leave the view output unfiltered. Without the clause on `FROM sub`, the view stays filtered. `FROM test WITH {"apply_request_filter": false} | WHERE id IN (FROM sub | KEEP id)` does not opt the view out.
- An untranslatable conjunct (`match_phrase` on text) still warns on a command that did not opt out, and does not warn about a command that did.
- An opted-out dataset does not row-group-prune on the request filter, and still prunes on a `WHERE`.

### `TsRequestFilterIT`

- `TS metrics WITH {"apply_request_filter": false}` returns samples outside the request range. `TS metrics` with the same filter does not.
- `TS` of a concrete view name still fails whether or not the clause is present. Parser-level if that rejection already is. Do not add a cluster test for it.

The `TS_INFO` / `METRICS_INFO` cap stays a unit test in `MetricsInfoTimeRangeRewriterTests`: an opted-out `EsRelation` is capped at `now - cap` even when the requested window is already inside the cap, and an opted-in relation in the same plan stays on the request-anchored window. That is one method on the rewriter's own test, not a new case in `MetricsAndTSInfoIT`.

### What this change will not test

- A new function or a query setting.
- Per-pattern flags inside one `FROM`.
- Inline `EXTERNAL` opting out. The key is an unknown external option.
- `preserveViewBoundaries` turning off when every view command opted out. The code keeps the boundary. The unit test locks that.
- The full `IN` spelling matrix. One filter path, one hash-join path, one `NOT IN`, one `EVAL`, one `STATS WHERE`, and one `INLINE STATS`.
- A `FORK` or `LOOKUP JOIN` execution test. `LOOKUP JOIN` is a parse error. `FORK` is per `FROM`.
- A cluster test of the old-dataset warning. `RequestFilterRewriterTests` already names both warnings.
- `EQL` combined with a request filter, beyond the existing rejection.
- Per-fragment `can_match` on a mixed union.
- User docs.
- A YAML REST file.
- Kibana grammar sync. A downstream dependency, not an Elasticsearch test.

## Rollout

Ship in one Elasticsearch PR. No index upgrade, no reindex, no new cluster setting. No doc changes in this PR.

Snapshot builds accept the clause. Release builds do not. Discover and Lens keep sending the request `filter` and keep today's behavior. They do not append the clause until the grammar predicate is removed and the capability is enabled on release builds.

The behavior change on a snapshot build is limited to queries that contain the new clause. Queries that do not are the compatibility suite on both builds: existing `RequestFilterIT`, `ViewRequestFilterIT`, `InSubqueryIT`, and the external-dataset filter suites stay green without new inputs.
