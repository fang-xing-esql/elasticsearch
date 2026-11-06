# Co-located JOIN: columns loaded but not used

Status: investigated and **fixed** on `colocated-join-route-rhs` (2026-11-06), not committed. LOOKUP JOIN is not affected and
stays unchanged. See [Implementation](#implementation) at the end.

Context: the `colocated_joins` Rally track (`rally-tracks/colocated_joins_plan.md`, section 8.6) showed that the co-located `JOIN`
is 3-6 times slower than `LOOKUP JOIN` on `stats-count`. It also showed that a `KEEP` after the join did not reduce the values loaded.
The root cause of that second issue was not known then. Both have the causes below.

## How it was checked

A throwaway internal cluster test (subclass of `ColocatedJoinIT`, since deleted) ran each query with `profile: true` and printed the
operators of every driver and the data-node physical plans (`resp.profile().plans()`). For LOOKUP JOIN it used `sessions_lookup`, a
one-shard lookup-mode copy of `sessions`. To reproduce, rebuild that probe and compare the `LookupOperator` `load_fields` and the
`ValuesSourceReaderOperator` fields per driver.

## Finding 1: the right side always loads every column

`FROM events | JOIN sessions ON session_id | STATS n = COUNT(*)`:

| | Right fields loaded | Left page size |
|---|---|---|
| `JOIN` (co-located) | `load_fields=[country, device, plan, referrer, started_at, user_id]`, all six | 840 rows |
| `LOOKUP JOIN` | none | 4,854 rows |

The same holds for `STATS ... BY country` and for `KEEP session_id, plan`: the co-located join loads all six fields every time.

### Cause

- `PruneColumns.pruneColumnsInEsRelation` (`x-pack/plugin/esql/.../optimizer/rules/logical/PruneColumns.java`) trims an `EsRelation`'s
  attributes only for `IndexMode.LOOKUP` and `IndexMode.TIME_SERIES`.
- A standard index normally relies on `InsertFieldExtraction` to load only the fields the plan references.
- The right side of the co-located join is a standard index, but it never goes through field extraction. It stays a logical
  `FragmentExec` and is handed to the lookup service as it is.
- `LocalMapper.mapColocatedJoin` passes `join.rightOutputFields()` as `addedFields`: every right attribute except the key.
- `LocalExecutionPlanner.planColocatedJoin` turns `addedFields` into the lookup's `load_fields`.
- The unused columns also count in `ColocatedJoinExec.estimateRowSize`. A larger estimated row size gives smaller left pages:
  `TARGET_PAGE_SIZE / estimatedRowSize`, which here is 840 rows instead of 4,854.

### Effect in the track

On `stats-count` with 4096-row pages forced for all variants, the co-located joins took 79 and 158 ms, against 26 and 30 ms for
LOOKUP JOIN. With the same ten right columns needed by every variant (`stats-count-right-columns`), the routed co-located join was
the fastest. So the gap on `stats-count` is the unused right columns.

### Fix idea

- In `PruneColumns`, also trim the right relation of a `JOIN` command (`Join.isCandicateColocatedJoin()`).
- Keep only the attributes used above the join, plus the join key.
- This runs before `SelectJoinStrategy` picks the co-located strategy, so key it on the `JOIN` command, not on `ColocatedJoin`.
- LOOKUP JOIN's right side is already in lookup mode, so its path does not change.
- To verify: with `COUNT(*)`, nothing on the right is used, so the lookup has no fields to load. The inner join still has to drop
  rows without a match. The lookup loads the right key under a synthetic name for that, so it should still work; add a test.

## Finding 2: with a SORT, all left columns are loaded too early

| Shape | Co-located `JOIN` | `LOOKUP JOIN` |
|---|---|---|
| `KEEP @timestamp, plan \| LIMIT 10000` | `session_id` before the lookup, `@timestamp` after the limit (fine) | the same |
| `SORT plan \| LIMIT 10000` | the data driver loads **every** left field before its TopN, then node_reduce loads them **again** after its TopN | the data driver keeps only `_doc`; node_reduce loads the fields once |

The track's `KEEP key, lookup_keyword_0 | SORT ... | LIMIT` (20.9M values loaded) and the full `sort-limit10000` shape both hit this.

### Cause

Data-node plan of the co-located `SORT plan | LIMIT 10000`:

```
TopNExec[plan]
\_ProjectExec[@timestamp, duration_ms, event_type, page, seq, session_id, _doc, country, ...]
  \_FieldExtractExec[@timestamp, duration_ms, event_type, ...]   <- every left field, loaded early
    \_ColocatedJoinExec[...]
      |_FieldExtractExec[session_id] \_EsQueryExec[events]
      \_FragmentExec[EsRelation[sessions][_doc{f}#44, country, ...]]   <- right side has the left's _doc
```

1. For a TopN, `LateMaterializationPlanner.buildSetupContext` (`x-pack/plugin/esql/.../plugin/LateMaterializationPlanner.java`)
   prepends `_doc` to every `EsRelation` that is not in `LOOKUP` mode. The co-located join's right index is in standard mode, so it
   gets the *same* `_doc` attribute as the left index.
2. The logical join now lists `_doc` among its right-side columns, so its output order differs from `ColocatedJoinExec.output()`.
3. `LocalMapper.mapColocatedJoin` handles any difference by adding `ProjectExec(join.output())`.
4. That projection references every left attribute. The output of a standard relation is not trimmed logically, so this is every
   field of the left index. `InsertFieldExtraction` then loads all of them below the TopN, before the late materialization can help.

### Fix idea

- In `buildSetupContext`, do not add `_doc` to the right side of a co-located `JOIN`, the same way `LOOKUP` relations are skipped.
  The join's output then matches `ColocatedJoinExec.output()`, and the `ProjectExec` goes away.
- As a safety net, `mapColocatedJoin` could project only the attributes the parent needs, instead of all of `join.output()`.
- Keep the LOOKUP JOIN path unchanged.

## Expected effect after both fixes

- `stats-count`: the co-located joins load no right columns and get pages as large as LOOKUP JOIN's.
- `sort-limit10000`, `keep-after-join-limit10000`: the left fields are loaded once, on node_reduce, for the surviving rows only.
- The track's `stats-count` and `keep-after-join-limit10000` shapes are the baselines for these fixes. Each fix needs a test that
  checks the profiled `load_fields` and the left `ValuesSourceReaderOperator` fields, like the probe did.

## Implementation

Both fixes are in; the safety net in `mapColocatedJoin` was not needed.

- **Finding 1:** `PruneColumns.pruneColumnsInJoinRight` handles a `Join` with `isCandicateColocatedJoin()` (the `JOIN` command, before
  and after `SelectJoinStrategy`). It keeps the right relation's attributes that are used above the join, the right join keys, and
  whatever unary nodes on the right side reference. A right side that is not a chain of unary nodes over an `EsRelation` is left
  alone. `addedFields`, the lookup's `load_fields` and the row-size estimate follow from the trimmed relation.
  `COUNT(*)` leaves `load_fields=[]`; the inner join still drops rows without a match.
- **Finding 2:** `LateMaterializationPlanner.buildSetupContext` collects the relations on the right side of `JOIN` commands and
  does not add `_doc` to them, as for lookup-mode relations. The join's output then matches `ColocatedJoinExec.output()` and no
  `ProjectExec` over every left field is planned.

Tests:

- `PruneColumnsTests.testJoinCommandKeepsOnlyUsedRightFields`, `testJoinCommandWithNoUsedRightField`.
- `ColocatedJoinIT.testRunsOnEachDataNode`: `STATS ... BY country` loads `load_fields=[country]` only.
- `ColocatedJoinIT.testUnroutedLeftProfile`: `STATS COUNT(*)` loads `load_fields=[]`.
- `ColocatedJoinIT.testSortReadsLeftFieldsAfterTopN`: with `SORT plan | LIMIT 3` the data drivers read only `session_id`; the
  other left fields are read on the node_reduce driver after its top-n.
- Each of the three runs with both the per-page and the streaming lookup (`colocated_join_streaming.md`).

To check in the Rally track: `stats-count`, `sort-limit10000` and `keep-after-join-limit10000` should move toward the
`LOOKUP JOIN` numbers.
