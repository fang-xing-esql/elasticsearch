# ES|QL stream GET API: design and test plan (iteration 1)

Scope: `GET /_query/stream/{id}` (resume/read) from `esql_streaming_api.md`.
Storage contract: `StreamStorage`, `StreamMetadata`, `InMemoryStreamStore`, `NoopStreamStore` from PR #165893
(package `org.elasticsearch.xpack.esql.streaming`), used **as provided**. The in-memory store is a test-only stand-in for the
future system-index store.

Status: draft for review. Nothing is implemented yet.

## 1. Goal of iteration 1

Make the security checks behave **the same as the async GET** (`GET /_query/async/{id}`), make `wait_timeout` follow the same rules as `wait_for_completion_timeout` (type, default, total deadline, timeout is not an error), and return a result shaped like
the async result, but sliced by the user's `offset`, `max_rows` and `max_bytes`.

One deliberate difference: a stream can stay `RUNNING` for a long time while it keeps adding pages (one page is one document in the system index). So **GET never waits for the stream to complete**. It returns whatever `getPages` returns. `wait_timeout` only bounds how long GET waits when `getPages` has no rows yet. Everything beyond that is a follow-up (section 8).

Differences from async that remain by design:

- The result is read from many stored page entries, not one document, so only the requested range is read.
- The result is written as NDJSON, one row per line, as in the stream spec.
- GET reads only `StreamStorage`. It never reads task or node memory.

## 2. How async GET behaves (what we copy)

Taken from `RestEsqlGetAsyncResultAction`, `AsyncResultsService`, `AsyncTaskManagementService.addCompletionListener`, `AsyncSearchSecurity`.

| Aspect | Async GET behaviour | Stream GET in iteration 1 |
|---|---|---|
| Wait parameter | `wait_for_completion_timeout`, a `TimeValue`. Unset means no wait. | `wait_timeout`, same type, **same default: unset means no wait**. |
| Zero or negative timeout | Returns the current result immediately | Same |
| Positive timeout | Waits **up to the timeout in total** for the query to complete. Returns the final result as soon as it completes, otherwise the current (running) result when the timeout hits. | **Differs by design.** If `getPages` already returns rows, return them at once, never wait for completion. Only when there are no rows yet and the stream is `RUNNING`, wait up to `wait_timeout` in total for rows (or a terminal status). Then return what is there. |
| Timeout is an error? | No. A timeout returns the still-running result with 200. | Same |
| Query failed while waiting | The failure is returned as the result | Rows stored so far are served, then the error goes in the last line (see 4.5) |
| Expiration | `expirationTime < now` gives 404 | Same, from `StreamMetadata.ttl()` |
| Security | Stored headers checked with `securityContext.canIAccessResourcesCreatedWithHeaders(headers)`. Mismatch throws `ResourceNotFoundException` (404). | Same call, same 404, using `StreamMetadata.headers()` |
| Unknown id | 404 | 404 |
| Response status of running query | `is_running: true` | `query_status: running` in the first line |

## 3. Request

```
GET /_query/stream/{id}?offset=0&max_rows=..&max_bytes=..&wait_timeout=..
```

| Parameter | Default | Meaning |
|---|---|---|
| `offset` | `0` | First row to return |
| `max_rows` | none | Stop after exactly this many rows. `0` means status only. **When not provided there is no limit on the number of rows.** |
| `max_bytes` | none | Stop after about this many bytes, checked between rows. `0` means status only, like `max_rows=0`. **When not provided there is no limit on the size of the response or of any row.** |

Limit rules: a limit that is provided is always honoured. A limit that is not provided is never replaced by a hidden default or cap. A GET reads the pages once (4.3), for `max_rows` rows or for every row from `offset`, so there is no internal page size or cap that could change what is returned.
| `wait_timeout` | none (no wait) | Same type, default and total-deadline rule as async `wait_for_completion_timeout`. It bounds the wait for rows when none are stored yet at `offset`. It never delays rows that are already stored. |

Validation, all before any byte is sent, as 400 `illegal_argument_exception`: `offset >= 0`, `max_rows >= 0`, `max_bytes >= 0` (`ByteSizeValue`), `wait_timeout` a valid `TimeValue`. Response format is always NDJSON in this iteration.

## 4. Design

### 4.1 Components (all new, named after their async counterparts)

The packages of the classes are listed in section 9. The storage interface is used as provided, except that `StreamMetadata.Status` and `StreamCompletionInfo` were made public so the GET classes can live in the same packages as their async counterparts.

| Stream component (new) | Async counterpart (existing) | Role and difference |
|---|---|---|
| `RestEsqlGetStreamResultAction` | `RestEsqlGetAsyncResultAction` (`GET /_query/async/{id}`) | Route and parameter parsing. Takes `offset`, `max_rows`, `max_bytes`, `wait_timeout` instead of `wait_for_completion_timeout` and `keep_alive`. Writes a chunked NDJSON response instead of one JSON body. |
| `EsqlStreamGetResultAction` (`ActionType`) | `EsqlAsyncGetResultAction` | Action name and request/response types. |
| `TransportEsqlStreamGetResultsAction` | `TransportEsqlAsyncGetResultsAction`, built on `AbstractTransportQlAsyncGetResultsAction` | Runs on the receiving node: lookup, security, wait, then the read. Async may forward the request to the node that owns the running task. Stream GET never forwards, because it reads only the store. |
| `StreamResultsService` | `AsyncResultsService.retrieveResult` | The flow in 4.2 to 4.4. Async looks at the in-memory task first and falls back to the index. Stream GET only uses `StreamStorage`. |
| `StreamTaskManagementService` (small, separate class) with static `addCompletionListener(threadPool, store, id, listener, timeout, ...)` and `getCurrentResult(...)` | `AsyncTaskManagementService.addCompletionListener` and `getCurrentResult` | Same method names and shape: `timeout <= 0` returns the current result at once, otherwise wait up to `timeout` in total, and on timeout return the current result. Two differences: async registers a completion listener on the task and waits for completion, while stream GET polls `getMetadata` and `getPages` (there is no task) and stops waiting as soon as rows exist or the status is terminal. |
| `StreamSearchSecurity` (small, separate class) with `currentUserHasAccessToStreamWithHeaders(headers)` and `getStreamAndCheckAuthentication(...)` | `AsyncSearchSecurity.currentUserHasAccessToTaskWithHeaders` and `AsyncTaskIndexService.getTaskAndCheckAuthentication` | Same method shape. The check is the same call, `securityContext.canIAccessResourcesCreatedWithHeaders(headers)`, with the headers from `StreamMetadata.headers()`, and the same `ResourceNotFoundException` (404) on a mismatch. `AsyncSearchSecurity` itself is not reused, because its constructor needs an index name, a client and an origin that stream GET does not have. |
| `StreamStorage` / `StreamMetadata` (from PR #165893) | `AsyncTaskIndexService` and the stored `StoredAsyncResponse` document | Storage. Async keeps one document with all pages. Stream keeps metadata plus one entry per page. |
| The NDJSON writer (`StreamNdjsonBody`) | `EsqlQueryResponse` (whole-result serialisation) | Stream GET writes the pages it read one after the other, honouring `max_rows` and `max_bytes`. |

### 4.2 Flow

```
validate params                                              -> 400
meta = store.getMetadata(id)
  meta == null or ResourceNotFoundException                  -> 404
  meta.ttl() < now                                           -> 404
  !securityContext.canIAccessResourcesCreatedWithHeaders(meta.headers())
                                                             -> 404 (same body as unknown id)
if max_rows == 0 or max_bytes == 0: write first and last lines from meta, done (no getPages, no wait)
pages = read the pages (4.3)   // getMetadata, then one getPages; waits only if no rows yet
write phase (4.4), with the pages that were read
```

Security is checked once, up front, because the stored headers never change. Expiry and not-found are checked on the metadata of every poll in 4.3, so a delete or expiry during a wait gives 404 rather than rows.

### 4.3 Read the pages once, waiting for rows only when there are none

GET does **not** wait for the stream to complete. A running stream may keep adding pages for a long time, so GET returns whatever `getPages` returns.

Each attempt is: `getMetadata`, then `getPages(id, offset, rows)`, where `rows` is `max_rows` when it is set and otherwise every row from `offset` (clamped so that `offset + rows` cannot overflow in the storage). `getPages` returns a list of pages, and each page is one document of the system index. `getMetadata` is relatively fast and tells us the status.

Once an attempt returns pages, **that is the only `getPages` call of the GET**. There is no second read to ask for more rows: the response is made of the pages of that one read.

| Attempt result | Action |
|---|---|
| `getPages` returns rows | Stop. Hand the pages to the write phase. Return them without waiting, whether the stream is running or terminal, and whether or not fewer than `max_rows` rows came back. |
| No rows, status terminal | Stop. This is the end of the result (offset rules in 4.4) |
| No rows, status `RUNNING`, `wait_timeout` unset or `<= 0` | Stop. Return a running result with no rows, `complete=false`, `next_offset=offset` |
| No rows, status `RUNNING`, `wait_timeout > 0`, time left | Schedule the next attempt (below) |
| No rows, status `RUNNING`, deadline reached | Stop. Same result as the unset case. A timeout is not an error. |

Details:

- Total deadline `start + wait_timeout`. It is not an idle timer, and it does not reset.
- There is no change notification in `StreamStorage`, so waiting is polling, scheduled on the thread pool (`threadPool.schedule`), never blocking a thread. Backoff starts at about 50 ms and is capped at about 250 ms, and the last delay is shortened so it ends at the deadline.
- Every attempt re-checks not-found and ttl on the metadata it just read. The security check is not repeated.
- Attempts that return no rows hold no pages, so nothing is held while waiting.
- The wait is cancelled if the client disconnects.
- Rows already stored are never delayed by `wait_timeout`. With no `wait_timeout` the behaviour is the same as one attempt.

### 4.4 Write phase

Inputs: the metadata and the pages of the read in 4.3, `offset`, `max_rows`, `max_bytes`.

1. Metadata is read **before** pages (done by each attempt in 4.3). If the status was terminal at that point, a page read that returns nothing really means "end of result". If it was `RUNNING`, the rows after the ones returned are for the next call: `complete` is false and `next_offset` says where to continue.
2. Offset checks:

   | Situation | Result |
   |---|---|
   | Terminal, `offset == rows` | 200, no rows, `complete=true`, `next_offset=rows` |
   | Terminal, `offset > rows` | 400 `offset [x] is beyond the end of the result [rows]` |
   | `RUNNING`, no rows at `offset` after the wait in 4.3 | 200, no rows, `complete=false`, `next_offset=offset` |
   | Otherwise | Rows from `offset` |

3. `max_rows == 0` or `max_bytes == 0`: write the first and last lines only, with no page reads and no wait. This is the one case where `max_bytes` produces no row at all. For any `max_bytes > 0`, the at-least-one-row rule in step 5 applies.
4. Otherwise write the pages of the read, one page after the other, rows one per line. The HTTP layer asks for the next part when it has written the previous one, so a slow client slows the writing. No storage read happens while writing.
5. The limits are counted while writing:
   - **`max_rows` provided:** stop after exactly `max_rows` rows, or earlier if the result has fewer rows from `offset`.
   - **`max_bytes` provided:** stop at the first row boundary where the bytes written reach `max_bytes`. If a single row is larger than `max_bytes`, it is still written, so a request with `max_rows != 0` and `max_bytes != 0` always makes progress when a row exists.
   - **Both provided:** whichever is reached first ends the response.
   - **Neither provided:** no limit. Every row the read returned is written, whatever their number or size, and no single row is cut or rejected for being large. For a still-running query these are the rows stored when the read happened.
   - **Only one provided:** the other is unlimited.
6. Every page returned by `getPages` is a shallow copy owned by the caller. All the pages of the read are held until they are written, so the memory of a GET is the size of what it returns. A page is released as soon as it is written, and the pages not written are released when a limit is reached, on error and on client disconnect.
7. End of response:
   - `complete = true` only when the status is terminal and `next == completionInfo.rows`.
   - `next_offset` is the first row not written.
   - A terminal status with fewer stored rows than `completionInfo.rows`, when no limit stopped the response, is an internal error (the storage lost pages).

### 4.5 Result shape (modelled on async)

First line: `{"id": ..., "columns": [...], "query_status": "running|completed|failed|cancelled"}` from `StreamMetadata.status()`, as the async result reports `is_running`.

Rows: one array per line.

Last line: `status`, `is_partial`, `warnings`, `complete`, `next_offset`, plus `error` when the status is `FAILED`.

| Metadata status | `query_status` | Rows | Last line |
|---|---|---|---|
| `RUNNING` | `running` | Whatever is stored in the range | `complete=false` |
| `COMPLETE` | `completed` | Range up to the end | `complete=true` when the end was reached, `is_partial` from `completionInfo.partial()` |
| `FAILED` | `failed` | Stored rows in the range | `complete=false`, `is_partial=true`, `error` from `completionInfo.exception()` |
| `CANCELED` | `cancelled` | Stored rows in the range | `complete=false` |

`warnings` comes from `StreamMetadata.warnings()`. `is_partial` for a `RUNNING` query should mirror what the async running response reports, to be confirmed while implementing. `took` is omitted from the GET last line in this iteration.

If the failure happens before the first row is written, return a normal HTTP error. After rows are written, the status is already 200 and the error goes in the last line. To make that possible, the first line is written only once the first page read has succeeded, or once it is known that no rows will be written.

### 4.6 Errors

| Case | Response |
|---|---|
| Bad parameters | 400 |
| Unknown id, expired, or not owned by the caller | 404, same body in all three |
| Offset beyond the end of a terminal result | 400 |
| Store failure during the read | HTTP error mapped from the exception. The read is finished before the response starts. |
| A `FAILED` stream with rows stored at the offset | The rows, then the stored error in the last line |
| Client disconnect | Stop, release pages, no error log |

### 4.7 Concurrency and lifecycle

Several GETs on one id run together. They only read, and the store serialises its own state. Because pages are shallow copies, a delete during a GET cannot free memory that is in use. A GET reads the storage once, before the response starts, so a delete after that point does not affect the response being written. The pages it holds are released as they are written, and the rest when the response ends or the client disconnects.

## 5. Interfaces used, unchanged

| Used | For |
|---|---|
| `getMetadata` | Existence, ttl, headers, status, columns, warnings, completion info (rows, partial, exception). Called on every attempt in 4.3, before `getPages`. |
| `getPages(id, offset, rows)` | Row ranges, already trimmed to the range by the store. This is what the wait polls, and its result is what GET returns. |

Notes on the provided behaviour that GET must handle itself:

- `getMetadata` returns `null` for an unknown id, but `getPages` throws `ResourceNotFoundException`. Treat both as 404.
- `completionInfo` is `null` while `RUNNING`.
- `Stream.getPages` returns fewer rows when the stream does not have them yet. That alone never means the end.
- The in-memory `ttl` is not enforced by the store. GET checks it.
- Only `RUNNING`, `COMPLETE` and `FAILED` can currently be produced by the in-memory store. `CANCELED` is handled in the code and tested with a small test subclass of the store.

## 6. Test plan

### 6.1 How async GET is tested today, and the stream counterpart

| Layer | Async test (existing) | What it does | Stream counterpart (new) |
|---|---|---|---|
| Request wire format | `GetAsyncResultRequestTests` (`AbstractWireSerializingTestCase`) | Serialisation round trip of the GET request | `GetStreamResultRequestTests`: round trip of id, `offset`, `max_rows`, `max_bytes`, `wait_timeout` |
| Results service | `AsyncResultsServiceTests` (`ESSingleNodeTestCase`, test task, real index): `testRecordNotFound`, `testRetrieveFromMemoryWithExpiration` (random expired or not, random success or failure), `testAssertExpirationPropagation`, `testRetrieveFromDisk`, `testFailWithIncompatibleResults` | Not found, expiry gives `ResourceNotFoundException`, success and failure results | `StreamResultsServiceTests` (`ESSingleNodeTestCase`, real `InMemoryStreamStore`): not found, expired, running, completed, failed (6.3) |
| Wait logic | `AsyncTaskManagementServiceTests` (`ESSingleNodeTestCase`): `testReturnBeforeTimeout`, `testReturnAfterTimeout`, `testGetWaitForCompletionWhenSearchPoolBusy` (occupies the SEARCH pool and checks the timeout still fires), `testOnResponseAfterTimeoutCallbackIsInvoked` | Timeout rules, with a latch-controlled operation | `StreamTaskManagementServiceTests` (`ESSingleNodeTestCase`): same test names and shape, with a latch-controlled test writer thread (6.4) |
| Security helper | `AsyncSearchSecurityTests` (`ESSingleNodeTestCase`, `testEnsuredAuthenticatedUserIsSame`) | Same user, unauthenticated original, unauthenticated current, run-as | `StreamSearchSecurityTests`: same cases (6.5) |
| Action level | `AsyncEsqlQueryActionIT` (`AbstractPausableIntegTestCase`, script permits pause the query): `testBasicAsyncExecution` (GET with a short timeout while running, GET with a long wait, GET again, delete, not found), `testUpdateKeepAlive`, cancel and delete tests | End to end through the transport action with a really running query | `StreamEsqlGetResultActionIT` (`ESIntegTestCase`, single node): same story with a writer thread pausing and releasing pages (6.7) |
| Security QA | `EsqlAsyncSecurityIT extends EsqlSecurityIT` in `qa/security`: reruns the security tests through the async API, and `testUnauthorizedIndices` checks that user2 gets 404 on user1's id for GET and DELETE | Real users and roles | `EsqlStreamSecurityIT` in `qa/security`: owner reads, other user gets 404 (6.8) |
| REST YAML | No hand-written GET YAML. `EsqlClientYamlAsyncIT` and `EsqlClientYamlAsyncSubmitAndFetchIT` rewrite the existing `esql.query` YAML tests to `esql.async_query` and `esql.async_query_get` (with `wait_for_completion_timeout: 30m`) | Whole ES\|QL YAML suite runs through async | Hand-written YAML for the stream GET (6.9). A `EsqlClientYamlStream...IT` harness that reruns the ES\| **Out of scope for iteration 1**, see follow-up 15 |
| REST API spec | `esql.async_query_get.json` | API definition used by the YAML runner | **Out of scope for iteration 1** (needed only by the YAML runner), see follow-up 15 |

Conventions, following async: service-level tests extend `ESSingleNodeTestCase` so real thread pools exist (needed for the SEARCH-pool-busy test), with real classes and no mocks. Pure logic tests (limits, slicing, parameter parsing) use `ESTestCase`. Real pages come from a breaker-enabled `BlockFactory` and every test asserts zero breaker usage at the end. Waiting is driven by latches and the writer thread, as the async tests do, and tests use `assertBusy` and `safeGet` rather than sleeps. Use random values where async does (random keep-alive, random success or failure), and keep the seed deterministic for reproduction.

### 6.2 `GetStreamResultRequestTests` (wire and parameters)

- Wire round trip with random `offset`, `max_rows`, `max_bytes`, `wait_timeout`, including unset values.
- Mutation test: every field takes part in `equals` and `hashCode`.

REST parameter parsing (also covered in `RestEsqlGetStreamResultActionTests`):

| Test | Checks |
|---|---|
| Defaults | `offset=0`, no limits, no wait |
| Valid values | Parsed correctly, `max_bytes` units |
| Invalid | Negative `offset`/`max_rows`/`max_bytes`, bad `max_bytes`, bad `wait_timeout` give 400 |

### 6.3 `StreamResultsServiceTests` (counterpart of `AsyncResultsServiceTests`)

Test names mirror the async ones. Lookup and expiry:

| Test | Async test it mirrors | Expected |
|---|---|---|
| `testRecordNotFound` | `testRecordNotFound` | Random unknown id gives `ResourceNotFoundException` |
| `testRetrieveWithExpiration` | `testRetrieveFromMemoryWithExpiration` | Random expired or not, random success or failure. Expired gives 404, not expired returns the result, failure returns the stored error |
| `testRetrieveCompleted` | `testRetrieveFromDisk` | A stream completed earlier is read in full, twice, with the same result |
| `testRetrieveRunning` | (new, async has no equivalent) | A running stream returns the pages stored so far |
| `testFailWithIncompatibleResults` | `testFailWithIncompatibleResults` | Not applicable to stream in iteration 1 (pages are written by one path). Add only if a version check is introduced |

| Test | Expected |
|---|---|
| Unknown id via `getMetadata` returning `null` | 404 |
| Store throwing `ResourceNotFoundException` from `getPages` | 404 or last-line error depending on whether rows were written |
| `ttl` in the past while the entry still exists | 404 |

### 6.4 `StreamTaskManagementServiceTests` (counterpart of `AsyncTaskManagementServiceTests`)

GET never waits for completion. It returns what `getPages` returns, and `wait_timeout` only bounds the wait when there are no rows yet. Async waits for completion, so these tests are modelled on the async ones but assert the stream rule.

| Async test | Stream test | Expected |
|---|---|---|
| `testReturnBeforeTimeout` | `testReturnBeforeTimeout` | Rows arrive before `wait_timeout`: returns as soon as a poll sees them |
| `testReturnAfterTimeout` | `testReturnAfterTimeout` | Nothing arrives within `wait_timeout`: 200, no rows, `complete=false`, `next_offset=offset`. The writer is released afterwards and a later GET gets the rows |
| `testGetWaitForCompletionWhenSearchPoolBusy` | `testGetWaitForRowsWhenSearchPoolBusy` | With the SEARCH pool fully occupied (`withSearchPoolOccupied`), `wait_timeout` still fires on time because polling runs on a thread pool that is not SEARCH |
| `testOnResponseAfterTimeoutCallbackIsInvoked` | `testRowsAfterTimeout` | Rows appended after a timed-out GET do not touch that GET (listener completed once), and the next GET returns them |
| `testOnFailureAfterTimeoutCallbackIsInvoked` | `testFailureAfterTimeout` | Stream failing after a timed-out GET: that GET is unaffected, the next GET shows `failed` |

Detailed cases:

GET never waits for completion. It returns what `getPages` returns, and `wait_timeout` only bounds the wait when there are no rows yet.

| Test | Expected |
|---|---|
| Running, rows stored at `offset`, `wait_timeout` unset | Rows returned at once, `query_status=running`, `complete=false` |
| Running, rows stored at `offset`, `wait_timeout=30s` | Rows returned at once, **no wait**, even though the stream is not complete |
| Running, fewer rows stored than `max_rows`, `wait_timeout=30s` | Returns the stored rows at once, does not wait to fill `max_rows` |
| Running, stream keeps adding pages for longer than `wait_timeout` | Returns the stored rows at once, never waits for completion |
| Running, no rows at `offset`, `wait_timeout` unset, `0` or negative | Returns at once, no rows, `complete=false`, `next_offset=offset` |
| Running, no rows at `offset`, rows appended during the wait | Returns as soon as a poll sees rows, not at the deadline |
| Running, no rows at `offset`, nothing arrives | Returns at the deadline with 200, no rows, `complete=false`, no wait beyond the deadline |
| No rows at `offset`, stream completes during the wait | Returns promptly, `completed`, end of result per the offset rules |
| No rows at `offset`, stream fails during the wait | Returns promptly, `failed`, error in the last line |
| No rows at `offset`, stream cancelled during the wait | Returns promptly, `cancelled` |
| Already terminal at request time | No wait, whatever the timeout |
| `max_rows=0` or `max_bytes=0` | No wait and no `getPages` call |
| Stream deleted during the wait | 404 |
| ttl expires during the wait | 404 |
| Client cancels during the wait | Polling stops, nothing leaks |
| Many concurrent waiters | No thread is blocked (a single-threaded scheduler still serves other requests) |
| Each attempt order | `getMetadata` is called before `getPages` on every attempt (store wrapper records call order) |
| Attempts with no rows | Hold no pages, breaker stays at 0 during the wait |
| Attempt with rows | Pages go to the read phase and are not read twice for the same range |
| `getPages` throws not-found during the wait | 404 |

### 6.5 `StreamSearchSecurityTests` (counterpart of `AsyncSearchSecurityTests`)

Copy the structure of `testEnsuredAuthenticatedUserIsSame`, using `AuthenticationTestHelper` and `SecurityContext` on the node's thread context:

| Case | Expected |
|---|---|
| Same authentication as the stored headers | Allowed |
| Equal but separately built authentication of the same user and realm | Allowed |
| Stored headers empty (stream created with security off) | Allowed |
| Current request not authenticated, stored headers present | Denied |
| Current request not authenticated, stored headers empty | Allowed |
| Original user was run-as, current is the effective user, or the authenticating user | Same outcome as `testEnsuredAuthenticatedUserIsSame` |
| API key creator reads with the same API key | Allowed |
| API key creator reads with a different API key, or with the creating user's basic auth | Denied |
| Denied through the service | `ResourceNotFoundException` with the same message as an unknown id |

| Test | Expected |
|---|---|
| Same user | Allowed |
| Different user | 404, same body as an unknown id |
| API key creator, same key | Allowed |
| API key creator, different key, or same user with basic auth | 404 |
| Security disabled | Allowed |
| Denied caller with a long `wait_timeout` | 404 immediately, no wait, no page read |
| Stored headers | Never present in any response |

### 6.6 Slicing and limits (stream-specific, `ESTestCase`)

These have no async counterpart, because async returns the whole result.

| Test | Expected |
|---|---|
| Full read of a completed stream | All rows in order, `complete=true`, `next_offset=rows` |
| Offset at a page start, mid-page, last row, equal to rows, beyond rows | Per the table in 4.4 |
| `max_rows` below, equal to, above the total, across several pages | Exact row count and `next_offset` |
| `max_rows=0` or `max_bytes=0` | First and last line only, no `getPages` call, `next_offset=offset` |
| `max_bytes=0` on a running stream with `wait_timeout` set | No wait |
| `max_bytes` small and large; a single row larger than `max_bytes` | Row-boundary stop, at least one row |
| Both limits set | The first one reached wins |
| Resume loop (`offset=next_offset` until `complete`) | Concatenated rows equal the full result, no gap or duplicate |
| Running stream with rows stored, offset in range | Rows returned, `complete=false` |
| Running stream, offset not yet reached | No rows, `next_offset=offset` |
| Failed stream | Stored rows, then error in the last line, `is_partial=true` |
| Failure with no rows written | Normal HTTP error |
| Cancelled stream (test subclass of the store) | `cancelled` |
| Order check: the query completes between the metadata read and the page read | Never reports the end before the last page was read |
| Store failure or delete during the read | A normal HTTP error, because the read is finished before the response starts |
| Writing the response | Reads nothing from the storage: the `getPages` call count does not change while the parts are written |
| `max_rows` unset with a large offset | No overflow, correct rows (GET never passes an unbounded row count) |
| Neither limit provided, result of many pages | Every row from `offset` to the end is returned, `complete=true`, nothing truncated, and `getPages` was called once |
| Neither limit provided, one very large row | Returned in full |
| Neither limit provided, running query | All rows stored at that moment are returned, `complete=false`, `next_offset` right after the last row |
| Only `max_rows` provided, rows are large | Exactly `max_rows` rows, no byte cap applied |
| Only `max_bytes` provided, many small rows | Stops at the first row boundary at or past `max_bytes`, no row cap applied |
| `max_rows` provided and fewer rows exist | All remaining rows, `complete=true` when terminal |
| Exact boundary: `max_rows` equals the rows remaining | All rows returned, `complete=true`, `next_offset=rows` |
| One read per GET | A request that returns rows makes exactly one `getPages` call, and writing the response reads nothing more |
| Rows asked from the storage | `max_rows` when set, otherwise `Long.MAX_VALUE - offset`, clamped so `offset + rows` cannot overflow |
| Page release | Breaker back to 0 after success, failure, disconnect and delete |

### 6.7 `StreamEsqlGetResultActionIT` (counterpart of `AsyncEsqlQueryActionIT`)

Single node, in-memory store (it is per node). A writer thread appends pages to the store and is paused and released with permits, as `AbstractPausableIntegTestCase` does with script permits.

- `testBasicStreamGet` (mirrors `testBasicAsyncExecution`): create a running stream, GET with a short `wait_timeout` and no rows stored gives running with no rows (like the async "timeout first" step). Release some pages, GET with a long `wait_timeout` returns the pages at once, still `running`, with `next_offset`. Release the rest and complete. GET with `offset=next_offset` returns the end with `complete=true`. GET again returns the same rows. Delete the stream, then GET gives 404.
- `testResumeLoop`: multi-page result, read with random `max_rows`/`max_bytes`, concatenation equals the full result.
- `testGetWhileStreamFails` and `testGetWhileStreamCancelled` (mirrors `testGetAsyncWhileQueryTaskIsBeingCancelled`): rows stored so far plus failure or cancel state.
- `testConcurrentGets`: several threads read different offsets of one id.
- `testDeleteDuringGet`: last-line error, all pages released.
- Expiry: a stream with a short ttl returns 404 once expired (async has `testCancelOnExpiry`).
- No thread is blocked while many GETs wait.

### 6.8 `EsqlStreamSecurityIT` (counterpart of `EsqlAsyncSecurityIT`, `qa/security`)

Needs stream POST (or a test-only way to create a stream under a given user) and the in-memory store on a single-node cluster.

- Owner creates a stream and reads it with GET: 200.
- Other user: GET gives 404 with the same body as an unknown id (mirrors `testUnauthorizedIndices`, user2 on user1's id and the reverse).
- API key owner reads with the same key: 200. Different key: 404.
- Denied user with a long `wait_timeout` gets 404 at once.
- Optionally rerun the `EsqlSecurityIT` queries through stream POST and GET, as `EsqlAsyncSecurityIT` reruns them through async.

### 6.8.1 `RBACEngineTests` (authorization of the action name)

Existing async tests do not cover `RBACEngine` for the get result actions. One test is added in the security module, written like the other `authorizeIndicesAction` tests of the class (a spied `Role`, an `ActionListener` with `assertTrue(isGranted())` and `Assert.fail` on failure, `Mockito.verify` on the role):

| Test | Checks |
|---|---|
| `testStreamGetResultActionNeedsNoIndexPrivileges` | With a role that grants nothing and a request that is not an indices request, `authorizeIndexAction` grants the stream get result action, through the `isStreamRelatedAction` branch. The role is never asked to check the action (`checkIndicesAction` and `authorize` are never called). Without the branch the test fails. |

The existing `authorizeIndicesAction` helper always built a `SearchRequest`, which is an indices request and never reaches this branch. It now has an overload that takes the request, and the old signature calls it with a `SearchRequest`, so the other tests are unchanged.

### 6.9 YAML REST tests

Out of scope for iteration 1. The planned cases are kept in follow-up 15 (section 8).

### 6.10 Later

- Rerun against the real system-index store, including multi-node reads on a node that did not create the stream.
- Expiry by the real cleanup, node loss while `RUNNING`, rolling upgrade and mixed cluster reads, large results and deep offsets, and a performance baseline against async GET.

## 7. Implementation steps (after approval)

1. Add parameter parsing, the REST handler and the transport action.
2. Add `StreamResultsService` with security, expiry, the single read (with wait for rows) and the write phase.
3. Reuse or add the NDJSON renderer pieces shared with POST.
4. Unit tests (6.2 to 6.6), then the integration test (6.7) and security QA (6.8). No YAML tests in this iteration.

## 8. Follow-ups (out of scope for iteration 1)

Numbers refer to the earlier review findings.

| # | Follow-up | Effect in iteration 1 |
|---|---|---|
| 1 | Add `firstOffset` to `StreamMetadata` and `create`, and let the store accept a non-zero first offset, for `overflow` | Only results that start at row 0 (the `all` mode) are supported. The "offset before the first stored row" error is not implemented. |
| 2 | Storage-side support for `max_bytes`, and fix the `offset + rows` overflow in `Stream.getPages` | `max_bytes` is enforced by the renderer. GET never passes an unbounded row count. |
| 3 | **3a done:** `StreamMetadata.Status` and `StreamCompletionInfo` are public. **3b and 3c remain:** a `markCancelled` method, and one convention for unknown ids (`getMetadata` returns `null`, the other methods throw `ResourceNotFoundException`) | The GET classes are placed like the async ones (section 9). `CANCELED` is handled but not producible by the in-memory store. A missing stream is handled both ways. |
| 5 | A last-updated time in the metadata, to detect a stale `RUNNING` after the writer node is lost | Such a stream shows `running` until its `ttl` expires. |
| 6 | `keep_alive` on GET, using `extendTtl`, and a `format` parameter | Not accepted. NDJSON only. |
| 7 | The in-memory store is per node | Left as is. Integration tests run on a single node. |
| 11 to 14 | `took` in the GET last line, `drop_null_columns` and `column_metadata` with streaming, `documents_found`/`_clusters`/`profile` in the last line, the `Allow_partial_results` typo in the spec | Not handled. Open questions for the spec owners. |
| 15 | YAML REST tests and the REST API spec (`esql.stream_query_get.json`). Planned cases: GET with `offset`, `max_rows`, `max_bytes` checking the first and last lines and `next_offset`; GET with neither limit returns all rows with `complete: true`; `max_rows=0` and `max_bytes=0` return status only; unknown id gives 404; bad parameters give 400; offset beyond the end gives 400; `wait_timeout` on a running seeded stream with stored rows returns those rows at once. Explicit mappings, with stream POST or a test-only way to seed a stream. Also a harness like `EsqlClientYamlAsyncIT` that reruns the ES\|QL YAML suite through stream POST and GET, once the NDJSON result can be mapped back to the YAML assertions. | No YAML tests. Coverage comes from the unit, integration and security QA tests in section 6. |
| 16 | Documentation for the new endpoint (read `docs/AGENTS.md` first). | No docs are added in this iteration. |
| 17 | Store the query time zone, `drop_null_columns` and the other POST-time rendering options with the stream | GET writes dates in UTC and does not drop null columns. |
| 18 | `EsqlStreamSecurityIT` in `qa/security` (needs stream POST or a way to create a stream as a user on a real cluster), and unit cases for API keys in `StreamSearchSecurityTests` (the esql unit test classpath has no `AuthenticationTestHelper`) | Security is covered by `StreamSearchSecurityTests` (realm users, run-as, no authentication), `StreamResultsServiceTests` and `RBACEngineTests`. API keys are covered only by the shared `SecurityContext` check. |
| 19 | The in-memory store shares blocks between readers. Block reference counts are not thread safe until a page is allowed to pass to another thread, so concurrent GETs on one stream need that call. The real storage deserializes a page per read. | The tests call `Page.allowPassingToDifferentDriver()` on the pages they store. |
| 20 | Changelog entry (`docs/changelog`) | Added with the PR, which has no number yet. |
| 21 | The stream POST must store the security headers of the request that started the query when it creates the stream (`StreamStorage#create`), and a DELETE needs the same check. Empty stored headers mean "security was off" and allow every caller, so a writer that passes an empty map on a secured cluster would expose the stream. Add a writer-side check or assertion. | GET checks the caller against the stored headers (`StreamSearchSecurity`), but nothing stores them yet. Marked with `TODO` in `StreamResultsService` and `StreamSearchSecurity`. |
| 22 | (Implemented, see section 14.9. Design in section 12, plan in section 14, benchmark switch in 14.8.) Read the pages in bounded chunks, instead of one `getPages` call for the whole range. `getPages` returns a list of pages, and each page is one document of the system index, so one call for a large result reads every document of the range and holds all the pages in memory until they are written. The real storage may also limit how many documents one read can return. Chunking would read `chunkRows` rows (or a number of pages) at a time, only when the HTTP layer asks for the next part, and release each page after it is written. Decide later whether it is needed, and whether the bound should be rows, pages or bytes. A page-by-page read in the storage would make the row count unnecessary. | A GET reads once and holds all the pages it returns. The memory of a GET is the size of what it returns. A huge result with no `max_rows` is read whole. |
| 23 | Replace the polling of `wait_timeout` by a change notification in `StreamStorage` (`addProgressListener`), first with polling inside the storage that is shared by all the waiters of a stream on a node, later possibly with a wake-up from the writer node. See section 13. | The GET polls the storage itself: each waiting GET reads the metadata and the pages every 50 to 250 ms. |
| 24 | Benchmark `getPages` against `getPageBatch` with the hidden `read_mode` switch (section 14.8), once the index-backed store exists. Then delete the producer that loses and the switch. | Both modes are implemented and switchable (section 14.9). `batch` is the default. |
| 25 | Run the GET as an ES|QL driver: a source operator that reads the storage, and the sink of the streaming POST (`StreamingPageOperator`). First as a third `read_mode=driver` over `getPageBatch`, later possibly in two stages (find the pages, then load them). Design and test plan in section 15. | The one-stage version is implemented as `read_mode=driver` (section 15.12). `batch` is still the default. |
| 26 | Improve the read side of `StreamStorage` for the driver mode: the status and an end flag with each batch, a `BlockFactory` parameter, and later a reader object, a change notification and page references. See section 15.11. | A reader calls `getMetadata` and then `getPageBatch` for every batch, and the storage decides which breaker its pages are charged to. |
| 27 | Reading from the system index `.esql-stream-results` (PR #166343): decide the page documents, their visibility and where the reads run, then build the index-backed `StreamStorage` and, only if needed, shard-side read operators. Brainstorm and proposal in section 16. | Proposal only, not implemented. |

Possible later improvement: when fewer rows than `max_rows` are stored and the stream is running, optionally wait a short time to fill the response. Not in iteration 1.

## 9. Implementation status

Implemented as designed. The classes are placed like the async ones: the REST handler, action type and response writer in `org.elasticsearch.xpack.esql.action`, the transport action in `org.elasticsearch.xpack.esql.plugin`, and the service layer next to the storage in `org.elasticsearch.xpack.esql.streaming`, as `AsyncResultsService`, `AsyncTaskManagementService` and `AsyncSearchSecurity` sit next to the async storage.

| Class | Package | Async counterpart | Role |
|---|---|---|---|
| `RestEsqlGetStreamResultAction` | `action` | `RestEsqlGetAsyncResultAction` | `GET /_query/stream/{id}`, parameter parsing, chunked NDJSON response. Registered only when `EsqlCapabilities.Cap.STREAMING_V2` is enabled (snapshot builds today), the capability of `POST /_query/stream`. Also parses the hidden `read_mode` (14.8). |
| `EsqlStreamGetResultAction` | `action` | `EsqlAsyncGetResultAction` | Action type |
| `StreamNdjsonBody` | `action` | (`EsqlQueryResponse` serialisation) | Pull-based NDJSON writer and subscriber of the pages (section 14): first line, rows with `max_rows` and `max_bytes`, last line. Public so tests outside the package can drive it, like `EsqlStreamResponseListener` for the POST. |
| `TransportEsqlStreamGetResultsAction` | `plugin` | `TransportEsqlAsyncGetResultsAction` | Runs on the receiving node, never forwards. Builds its storage in `createStreamStorage`. |
| `GetStreamResultRequest`, `GetStreamResultResponse` | `streaming` | `GetAsyncResultRequest` (in core, next to `AsyncResultsService`) | Local-only request (cancellable task, read mode) and response (the first metadata and the producer of the pages) |
| `StreamResultsService` | `streaming` | `AsyncResultsService` | Metadata lookup, security and expiry, the first read of the pages (with the wait), offset and failure checks, and the producer of the read mode |
| `StreamTaskManagementService` | `streaming` | `AsyncTaskManagementService` | `getCurrentResult` and `addCompletionListener`: the `wait_timeout` polling |
| `StreamSearchSecurity` | `streaming` | `AsyncSearchSecurity` | The check against the stored headers, the same 404 for unknown, expired and not owned |
| `StreamPages` | `streaming` | none | The result of one read: the metadata, the pages and where the next read starts |
| `StreamPageProducer`, `StreamStorageSingleReadProducer`, `StreamStoragePageProducer` | `streaming` | none (`StreamingPageOperator` for the POST) | Feed the pages to a `PageStreamPublisher`, see section 14.9 |

The request and response stay in `streaming`, as `GetAsyncResultRequest` stays with its service, so that `action` depends on `streaming` and not the other way round.

### 9.1 Where the implementation differs from the plan above

- **The request is local-only**, as the existing stream query request is. It carries no wire format, so there is no wire round-trip test. `GetStreamResultRequestTests` covers the parameters, `validate()` and that it is not serializable.
- **Where the tests are:** `RestEsqlGetStreamResultActionTests` is in `action` and `StreamEsqlGetResultActionIT` and its plugin `StreamTestEsqlPlugin` are in `action` (`internalClusterTest`), next to the classes and tests of the async API. The other tests stay in `streaming`, with the shared test base class. `StreamNdjsonBodyTests` stays there because it uses that base class.
- **No chunking (superseded by section 14.9, where the default read mode reads in batches):** a GET made one `getPages` call and wrote all the pages it returned. The row-count chunking with a chunk size of 4096 that this design first had was removed to keep the first iteration simple, and is follow-up 22. `StreamChunk` is now `StreamPages`, and `StreamChunkSource` is gone.
- **Class names of tests:** the service tests (`StreamTaskManagementServiceTests`, `StreamResultsServiceTests`, `StreamSearchSecurityTests`) extend `ESTestCase` with a `TestThreadPool` instead of `ESSingleNodeTestCase`, because the in-memory store needs no node. The SEARCH-pool-busy test occupies the SEARCH pool of the `TestThreadPool`.
- **Added tests:** `StreamNdjsonBodyTests` (slicing and limits), `RestEsqlGetStreamResultActionTests` (parameters), `StreamEsqlGetResultActionIT` (REST on a real node, in `internalClusterTest`), and `RBACEngineTests#testStreamGetResultActionNeedsNoIndexPrivileges`.
- **Not implemented, moved to follow-ups 18 and 19:** `EsqlStreamSecurityIT`, API key unit cases.
- **`max_bytes` takes a byte size with a unit:** `10b`, `1mb`. A plain number other than `0` is a 400, like every byte size parameter. `0` means status only, and a negative value is a 400. A negative `max_rows` is a 400 too.
- **`format` is accepted and ignored** because every REST handler accepts it. The response is always NDJSON. `keep_alive` is rejected with a 400, as an unsupported parameter.

- **After the rebase on PR #166343 (system index and `StreamMetadata` for the index):** `StreamMetadata` now has the field order `streamId, taskId, expiration, columns, headers, warnings, status, completionInfo` (`ttl` is `expiration`). `StreamCompletionInfo.rows` and `pages` are `Long` and null for a failed stream, and the failure is a `StreamFailure(type, reason, status)` and no longer an `Exception`. The GET builds its error from it with `StreamFailure.toException()`, an `ElasticsearchStatusException` with the stored reason and status, so the original exception class (for example `IllegalStateException`) is not restored. The type name is only in `StreamFailure.type()`. `Status`, `StreamCompletionInfo` and `StreamFailure` were made public for the `action` package. The new feature flag `esql_stream_results_stored` only registers the index and is separate from the `STREAMING` capability that gates the endpoints.

### 9.2 Changes outside the esql `streaming` package

- `x-pack/plugin/core`: `EsqlStreamActionNames` holds the action name `indices:data/read/esql/stream/get`, like `EsqlAsyncActionNames`.
- `x-pack/plugin/security`: `RBACEngine` has its own branch for the stream GET, with a new `isStreamRelatedAction(action)` that checks `ESQL_STREAM_GET_RESULT_ACTION_NAME`. The branch sits next to the async one and returns `ALLOW_NO_INDICES`. The action is not an indices request, so without it the action would need index privileges. The check that the caller owns the stream is done by the GET itself (`StreamSearchSecurity`), as for the async actions. The action is deliberately not added to `isAsyncRelatedAction`: it is not an async action, and a separate method names what it is and lets later stream actions that read a result join it.
- `StreamMetadata` (the PR #165893 interface): `Status` and `StreamCompletionInfo` are public, so classes outside the package can use `metadata.status()` and the completion info. Nothing else in the interface changed.
- `EsqlPlugin`: registers the action and the REST handler. It registers no stream component. The REST handler is gated with `EsqlCapabilities.Cap.STREAMING_V2.isEnabled()`, the capability of `POST /_query/stream`. It was `Cap.STREAMING` before, but main disabled that capability when streaming moved from `POST /_query?streaming=true` to `POST /_query/stream`, which left the GET unregistered.

## 10. Plan: remove `StreamStorageHolder` and build the storage the way async does

Status: implemented. The unit tests (5 iterations), the integration test (3 iterations), spotless, checkstyle and forbidden-apis pass.

### 10.1 Why

`StreamStorageHolder` has no async counterpart. Async never registers its storage with `EsqlPlugin`. The transport action builds it in its own constructor: `AbstractTransportQlAsyncGetResultsAction.createResultsService(...)` creates the `AsyncTaskIndexService` (index name, cluster service, thread context, client, origin, reader, registry, big arrays), wraps it in an `AsyncResultsService`, and `TransportEsqlQueryAction` does the same for `AsyncTaskManagementService`. The holder was added only because the real storage does not exist yet and the IT needs to swap the storage. Both needs can be met without a component that the plugin registers and a test mutates.

### 10.2 Target shape

| Async | Stream after this change |
|---|---|
| `AbstractTransportQlAsyncGetResultsAction` constructor calls `createResultsService(...)`, which builds the storage, its security and the results service | `TransportEsqlStreamGetResultsAction` constructor calls an overridable `createResultsService(...)` that builds the `StreamStorage`, a `StreamSearchSecurity` and the `StreamResultsService` |
| The storage is the system index through `AsyncTaskIndexService` | The storage is created by an overridable factory `createStreamStorage(...)`. It returns `NoopStreamStore.INSTANCE` until the system-index backed storage exists, and then builds that one from the same inputs async uses (client, cluster service, thread context, origin) |
| `EsqlPlugin` registers only the action and REST handlers | Same. No stream component is registered in `createComponents` |
| `AsyncResultsService` receives the storage | `StreamResultsService` receives a `StreamStorage` directly, not a holder |

### 10.3 Steps

1. **`StreamResultsService`:** replace the `StreamStorageHolder` parameter by a `StreamStorage`. `retrieveResult` uses the field instead of `holder.get()`.
2. **`TransportEsqlStreamGetResultsAction`:**
   - Drop the `StreamStorageHolder` constructor parameter.
   - Add `protected StreamStorage createStreamStorage(ClusterService, ThreadPool, Client)`, returning `NoopStreamStore.INSTANCE` for now. This is the single place that changes when the real storage lands.
   - Build the `StreamSearchSecurity` and the `StreamResultsService` in `createResultsService(...)`, like the async action does. Keep the this-escape suppression the async action uses.
3. **`EsqlPlugin`:** remove the `new StreamStorageHolder(...)` component and the imports of `StreamStorageHolder` and `NoopStreamStore`. Remove the `Build` import that is no longer used after the switch to `Cap.STREAMING.isEnabled()`.
4. **Delete `StreamStorageHolder`.**
5. **Unit tests:** `AbstractStreamTestCase` loses its `holder` field. `service(chunkRows, storage)` takes a `StreamStorage`. The tests that pass `new StreamStorageHolder(recording)` pass `recording` directly. No test needs a plugin.
6. **`StreamEsqlGetResultActionIT`** needs the in-memory store inside the real node, so it gets a test seam through a plugin, the way `EsqlPluginWithEnterpriseOrTrialLicense` already subclasses `EsqlPlugin` in `internalClusterTest`:
   - A test transport action `TestTransportEsqlStreamGetResultsAction extends TransportEsqlStreamGetResultsAction` overrides `createStreamStorage` to return a store shared with the test (a static field of the test, set in `@Before`).
   - A test plugin `StreamTestEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense` overrides `getActions()` and replaces the handler of `EsqlStreamGetResultAction` with the test action.
   - The IT lists `StreamTestEsqlPlugin` in `nodePlugins()` instead of the base plugin. The cluster has one node, so a static store is enough.
7. **Design doc:** update the rows and text of sections 5 and 9 that mention the holder (done in 9, this section is the target for the rest).
8. **Checks:** `spotlessApply`, `checkstyle`, `forbiddenApis`, the `streaming` unit tests with several iterations, and the IT.

### 10.4 Further alignment with async, optional

| Item | Async | Stream today | Proposal |
|---|---|---|---|
| Package of the classes | REST handler and action type in `org.elasticsearch.xpack.esql.action`; transport action in `org.elasticsearch.xpack.esql.plugin` | **Done:** same layout, with the request, response and service layer in `streaming` next to the storage | Done after `StreamMetadata.Status` became public (follow-up 3a). |
| Request | Serializable `GetAsyncResultRequest`, `HandledTransportAction`, may forward to another node | Local-only request, `TransportAction`, never forwarded | Keep. The response is a local streaming body, and stream GET reads only the storage, so there is nothing to forward to. |
| Security inside the storage service | `AsyncSearchSecurity` is created by `AsyncTaskIndexService` | `StreamSearchSecurity` is created by the transport action | Keep. It is the same `SecurityContext` rule, built from the same inputs. |
| REST gating | Registered unconditionally | `Cap.STREAMING.isEnabled()` | Keep until the feature is released. |

### 10.5 Risks

- **Test seam:** the IT depends on a plugin subclass that replaces an action handler. If `getActions()` changes shape, the subclass must follow. This is no worse than the existing `EsqlPluginWithEnterpriseOrTrialLicense`.
- **Real storage:** the factory method has the cluster service, thread pool and client available, which is what a system-index store needs. If it needs more (for example a registry for page serialization), the constructor parameters grow like the async one did.
- **Nothing public changes:** the endpoint, its parameters and its behavior stay the same. In production every stream is still unknown until the real storage exists.

### 10.6 What was implemented

- `StreamStorageHolder` is deleted and `EsqlPlugin` no longer registers a stream component or imports `NoopStreamStore`.
- `StreamResultsService` takes a `StreamStorage`.
- `TransportEsqlStreamGetResultsAction` is injected with the `Client` instead of the holder. Its constructor calls `createResultsService(...)`, which calls the protected `createStreamStorage(clusterService, threadPool, client)`. That returns `NoopStreamStore.INSTANCE` for now.
- The unit tests pass the store, or a recording or forwarding wrapper of it, straight to the service.
- The integration test uses `StreamTestEsqlPlugin` (in `internalClusterTest`), a subclass of `EsqlPluginWithEnterpriseOrTrialLicense` that replaces the handler of `EsqlStreamGetResultAction` with a test action. That action overrides `createStreamStorage` to return a shared `InMemoryStreamStore`. The test lists the new plugin in `nodePlugins()` in place of the base one.
- The package alignment in 10.4 was done afterwards, see section 9.

## 11. Code path walk-through

Paths are relative to `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/`.

### 11.1 Entry point and call chain

The entry point is the REST handler `action/RestEsqlGetStreamResultAction.java`, route `GET /_query/stream/{id}`. It exists only where `EsqlCapabilities.Cap.STREAMING_V2` is enabled.

This walk-through describes the `read_mode=pages` path (one `getPages` read). The steps up to the first read are the same in the default `batch` mode. What differs after it is in section 14.9.

| Step | Where | What happens |
|---|---|---|
| 1 | `action/RestEsqlGetStreamResultAction` `prepareRequest` | The HTTP request arrives. `parseRequest` turns `offset`, `max_rows`, `max_bytes` and `wait_timeout` into a `GetStreamResultRequest`. A bad value is a 400. The handler then runs the transport action through a `RestCancellableNodeClient`, so a client disconnect cancels the task. |
| 2 | Security filter (`x-pack/plugin/security`, `RBACEngine.isStreamRelatedAction`) | The caller must be authenticated. The action `indices:data/read/esql/stream/get` needs no index privilege, because the ownership check happens in step 5. |
| 3 | `plugin/TransportEsqlStreamGetResultsAction` `doExecute` | Calls `resultsService.retrieveResult(...)`. The service, its storage (`createStreamStorage`, a `NoopStreamStore` for now) and its security check were built in the constructor, like the async action does. |
| 4 | `streaming/StreamResultsService` `retrieveResult` | Reads the metadata, runs the 404 checks, reads the pages once (waiting for rows if asked), and builds a `GetStreamResultResponse`. |
| 5 | `streaming/StreamSearchSecurity` `getStreamAndCheckAuthentication` | Unknown, expired and not-owned streams all give the same `ResourceNotFoundException` (404). |
| 6 | `streaming/StreamTaskManagementService` `addCompletionListener`, `getCurrentResult`, `attempt` | One read is `getMetadata` then `getPages`. With a positive `wait_timeout` and no rows in a running stream, it polls again on the scheduler. The first read that returns rows is the only one that is used. |
| 7 | `action/RestEsqlGetStreamResultAction` `processResponse` | Builds the `StreamNdjsonBody` and sends a chunked 200 response with `body.firstPart()`. |
| 8 | `action/StreamNdjsonBody` `firstPart`, `next` | The HTTP layer pulls one part at a time: the first line, one part per page, and the last line. Writing reads nothing from the storage. |
| 9 | `StreamNdjsonBody.close()` | When the response ends, fails or the client disconnects, the pages that were not written are released. |

### 11.2 Flow chart

```
 Client
   |  GET /_query/stream/{id}?offset=&max_rows=&max_bytes=&wait_timeout=
   v
+---------------------------------------------+
| RestEsqlGetStreamResultAction.prepareRequest |
|   parse the 4 parameters                     |
+---------------------------------------------+
   |                              \
   | ok                            \ bad value
   v                                v
 execute action                   400 error response
   |
   v
+---------------------------------------------+
| Security filter (RBACEngine)                 |
|   authenticated? action allowed without      |
|   index privileges                           |
+---------------------------------------------+
   |
   v
+---------------------------------------------+
| TransportEsqlStreamGetResultsAction.doExecute|
+---------------------------------------------+
   |
   v
+---------------------------------------------+
| StreamResultsService.retrieveResult          |
|   metadata = store.getMetadata(id)           |
+---------------------------------------------+
   |
   v
 metadata missing, expired, or the caller
 is not the one who started the stream?  --- yes --->  404 (same body for all three)
   |
   no
   v
 max_rows = 0 or max_bytes = 0?  --- yes --->  respond with the status only (no page read, no wait)
   |
   no
   v
+---------------------------------------------+
| Read the pages once (see 11.3)               |
|   getMetadata, then getPages(offset, rows)   |
|   waits only when there are no rows yet      |
+---------------------------------------------+
   |
   v
 offset beyond the end of a COMPLETE stream?  --- yes --->  400
 FAILED stream and no row at the offset?      --- yes --->  HTTP error with the stored exception
   |
   no
   v
 GetStreamResultResponse (the metadata and the pages)
   |
   v
+---------------------------------------------+
| RestEsqlGetStreamResultAction.processResponse|
|   new StreamNdjsonBody, send chunked 200     |
+---------------------------------------------+
   |
   v
 Response body, written part by part (see 11.4)
   first line -> rows ... -> last line
```

### 11.3 The read and the wait

```
 wait_timeout unset, 0 or negative?  --- yes --->  one read, return whatever it gives
   |
   no (positive)
   v
 deadline = now + wait_timeout          (a total deadline, it never resets)
   |
   v
+--->  task cancelled (client left)?  --- yes --->  stop with a cancellation
|        |
|        no
|        v
|     read once:  getMetadata  ->  404 if gone or expired
|                 getPages(id, offset, rows)
|        |
|        v
|     rows returned?  ------------------------ yes --->  return them now. This is the only getPages
|        |                                                of the GET, whether or not the stream is
|        no                                                still running and whatever the deadline
|        v
|     status is COMPLETE, FAILED or CANCELED?  --- yes --->  return, nothing more will come
|        |
|        no (still RUNNING)
|        v
|     deadline reached?  -------------------- yes --->  return, with no rows (not an error)
|        |
|        no
|        v
|     schedule the next read after 50 ms, then 100, 200, then every 250 ms
|     (never later than the deadline, never blocks a thread)
+--------+
```

The wait never waits for the query to finish, and it never reads again once a read returned rows. `rows` in `getPages(id, offset, rows)` is `max_rows` when it is set, otherwise every row from `offset`. `getPages` returns a list of pages, and each page is one document of the system index.

### 11.4 Writing the response

```
 first part:  {"id": ..., "columns": [...], "query_status": "..."}
   |
   v
 HTTP layer asks for the next part --->  next()
   |
   v
 status only, or max_rows / max_bytes reached? --- yes --->  release unwritten pages, write the last line
   |
   no
   v
 a page of the read is left?
   |                       \
   yes                      no
   v                         v
 write that page row by row     write the last line
   - one JSON array per line
   - count rows and bytes after each row
   - stop at a row boundary when a limit is reached
     (a row larger than max_bytes is still written)
   |
   v
 next()

 last line:  {"status": 200 | error status,
              "is_partial": ...,
              "warnings": [...],
              "complete": true only when the stream is COMPLETE and every row was written,
              "next_offset": offset + rows written,
              ["error": {...}]}
```

#### Why the HTTP layer asks for the next part

The arrow "HTTP layer asks for the next part ---> `next()`" is how Elasticsearch writes a chunked response: it **pulls** the body, one `ChunkedRestResponseBodyPart` at a time, and our code does not push.

1. We give `RestResponse.chunked(...)` the first part, `body.firstPart()`.
2. The HTTP layer calls `encodeChunk` on the part until `isPartComplete()` is true, and writes those bytes to the connection.
3. When the part is complete, and only when the connection can take more data, it calls `getNextPart(listener)`.
4. In `StreamNdjsonBody`, `ColumnsPart.getNextPart` and `PagePart.getNextPart` call `next(listener)`. The last-line part has `isLastPart() == true` and no continuation, which ends the response.

This is used for three reasons:

| Reason | Effect |
|---|---|
| Backpressure | A page is turned into bytes only when the previous part was written, so a slow client slows the writing and no encoded bytes pile up ahead of the socket |
| Unknown length | The size of the body is not known up front, so the response is sent in parts, ending with the last line |
| Early stop | When a limit is reached, the pages not yet written are released and the last line follows |

Writing does not read from the storage. All the pages were read before the response started, so a failure of the read is a normal HTTP error.

### 11.5 Worked example

A query is still running. Its storage holds two pages (two documents): page 1 has the rows `0..3` and page 2 has the rows `4..6`. Each row is one `long` column `n` whose value is the row number.

**Call 1: `GET /_query/stream/abc?offset=2&max_rows=4&wait_timeout=5s`**

1. `parseRequest` builds the request: id `abc`, offset 2, `max_rows` 4, no `max_bytes`, wait 5 s.
2. Security allows the action. `retrieveResult` reads the metadata: status `RUNNING`, not expired, and the stored headers match the caller, so no 404. The request is not status only.
3. The read asks the storage for 4 rows from offset 2. The storage trims its pages to the range: page 1 gives rows `2, 3` and page 2 gives rows `4, 5`. Rows exist, so the read returns at once. The 5 s wait is not used. This is the only `getPages` call.
4. `processResponse` builds the body and sends a 200 with these lines, one per part:

```
{"id":"abc","columns":[{"name":"n","type":"long"}],"query_status":"running"}
[2]
[3]
[4]
[5]
{"status":200,"is_partial":false,"warnings":[],"complete":false,"next_offset":6}
```

5. While writing, the body counted 4 rows. After the fourth, the limit is reached, so `next()` writes the last line. `next_offset` is `2 + 4 = 6`.

**Call 2: `GET /_query/stream/abc?offset=6&wait_timeout=5s`** (no limits)

1. The read asks for every row from offset 6 and gets row `6` (page 2, trimmed). Rows exist, so it returns at once, with no further `getPages` call.
2. The body writes `[6]`, then the last line. The status read before the pages was `RUNNING`, so `complete` is false:

```
{"id":"abc","columns":[{"name":"n","type":"long"}],"query_status":"running"}
[6]
{"status":200,"is_partial":false,"warnings":[],"complete":false,"next_offset":7}
```

**Call 3: `GET /_query/stream/abc?offset=7&wait_timeout=5s`**

1. The first read finds no rows and the status is `RUNNING`, so it waits: reads again after 50 ms, 100 ms, 200 ms, then every 250 ms.
2. After 300 ms the query finishes. The next read sees status `COMPLETE` with 7 rows in the completion info. It returns, with no rows.
3. The response is:

```
{"id":"abc","columns":[{"name":"n","type":"long"}],"query_status":"completed"}
{"status":200,"is_partial":false,"warnings":[],"complete":true,"next_offset":7}
```

`complete` is true because the status is `COMPLETE` and `next_offset` (7) reached the total rows. The client stops.

### 11.6 Other outcomes of the same code path

| Request or state | Outcome |
|---|---|
| Unknown id, expired stream, or a stream started by another user | 404, same body for all three, before any wait or page read |
| `offset` beyond the rows of a `COMPLETE` stream | 400 `offset [x] is beyond the end of the result [n]` |
| `max_rows=0` or `max_bytes=0` | The first and last lines only, no page read, no wait |
| `max_bytes` smaller than one row | One row is still written, then the last line, so a client always makes progress |
| `FAILED` stream, rows stored at `offset` | The rows, then a last line with `"status"` from the stored exception, `"complete":false`, `"is_partial":true` and an `error` |
| `FAILED` stream, no row at `offset` | A normal HTTP error with the stored exception (the response had not started) |
| The storage read fails | A normal HTTP error, because the read happens before the response starts |
| Client disconnects during a wait or while rows are written | Polling stops, the unwritten pages are released, nothing is logged as an error |
| No `wait_timeout` and no rows in a running stream | At once: no rows, `complete=false`, `next_offset=offset` |

## 12. Design: a page-granular read in `StreamStorage` (follow-up 22)

Status: implemented, see section 14.9 for the differences. It is the storage side of follow-up 22 (bounded reads), and it replaces the one `getPages` call of the first iteration by reads of a few whole pages at a time.

### 12.1 Goal

A read that returns **whole stored pages**, in order, a few at a time, so that a caller never re-reads a document or asks for rows it will discard. `getPages(streamId, offset, rows)` stays as it is.

Why `getPages` is not enough:

- It returns pages trimmed to a row range, so the caller does not know where pages start or how many rows each one has, and it cannot ask for "the next page".
- If it is called repeatedly with a row count, a chunk boundary that falls inside a page makes every call read and slice the same document again.
- A request with `max_bytes` and no `max_rows` asks for every row from the offset, reads the rest of the result, and discards most of it.

### 12.2 Interface

```java
/**
 * Reads whole pages in order, starting with the page that contains row {@code fromOffset}.
 * Pages are not trimmed: the first page may start before {@code fromOffset} (the caller trims it) and
 * each page carries its start offset. The next call starts at {@link PageBatch#nextOffset()}, which
 * is the end of the last page, so no document is read twice.
 */
void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener);

record StoredPage(long startOffset, Page page, long storedBytes) {
    long endOffset() { return startOffset + page.getPositionCount(); }
}

record PageBatch(List<StoredPage> pages) implements Releasable {
    long nextOffset() { /* endOffset of the last page, or the requested fromOffset when empty */ }
    // close() releases every page
}
```

### 12.3 Semantics

| Aspect | Rule |
|---|---|
| Start | The page with `startOffset <= fromOffset < endOffset`. If `fromOffset` is at a page boundary, that page is the first one. |
| Size of a batch | At most `maxPages` pages. If `maxBytes` is positive, it stops after the page that brings the stored bytes to `maxBytes` or more. At least one page is returned when one is available. |
| Empty result | No stored page covers `fromOffset` yet. The caller decides from the metadata status (read before the call, as now) whether that is the end of the result or only not written yet. |
| Ownership | The caller owns the pages and closes the batch. The in-memory store returns shallow copies. |
| Unknown stream | `ResourceNotFoundException`, as for the other methods. The convention for unknown ids is follow-up 3c. |
| No trimming | The caller trims the first page to `fromOffset` and the last page to `max_rows`, with `Page.slice`. |
| No waiting | The storage never waits. `wait_timeout` stays in the GET. |

### 12.4 Why these choices

- **Whole pages with start offsets:** the caller knows where it is, and the next call starts on a page boundary, so no document is read twice.
- **`maxPages` and `maxBytes`:** one sizes a batch by count and the other by the stored size of the documents. Row counts say nothing about memory. `maxBytes` can follow the `max_bytes` of the request.
- **`storedBytes` per page:** lets the GET stop reading when it estimates it has enough for `max_bytes`. It is the stored size, so it is only a hint for the rendered JSON size.
- **Stateless:** every call is independent, so there is no cursor, scroll or point in time. A deep offset costs the same as offset 0.

### 12.5 How the GET would use it

```
nextOffset = offset
loop:
  batch = getPageBatch(id, nextOffset, maxPages, maxBytes)   // the status is read just before
  empty                  -> the end, or "nothing yet" (for the first read: wait, as now)
  trim the first page to the requested offset
  write the pages; stop at max_rows / max_bytes and trim the last page
  nextOffset = batch.nextOffset()
  (optionally start the next batch read while the previous batch is written)
```

- The memory of a GET is one batch, or two with a prefetch, whatever the size of the result.
- With `max_bytes` alone it stops after about that many bytes instead of reading to the end of the result.
- The wait for rows stays in front of the first read, exactly as in 4.3.

### 12.6 Implementations

**`InMemoryStreamStore`**

- It keeps an ordered list of entries with an offset and a page.
- It finds the first page by a scan or a binary search on the offset, then takes up to `maxPages` shallow copies.
- `storedBytes` can come from the page's size in memory.
- `NoopStreamStore` returns an empty batch.

**The real system-index store**

- One document per page, with the fields `stream_id`, `offset`, `end_offset` and the serialized page.
- One search per batch: `stream_id = X AND end_offset > fromOffset`, sorted by `offset` ascending, size `maxPages`. It returns the page that contains `fromOffset` and the pages after it, with no cursor.
- The writer already passes `offset` to `appendPage`. `end_offset` is `offset + page.getPositionCount()`.

### 12.7 Open questions

1. **Visibility of new pages:** a search sees pages only after an index refresh. A get by document id is real-time, but the ids of later pages are not known. Does the writer refresh after each page (or wait for refresh), or does the GET accept up to a refresh interval of delay?
2. **Metadata in the same call:** should the batch also return the status and the row counts? It saves a call and removes the "metadata before pages" ordering from the caller, and a real store can read both together.
3. **Rows stored so far in the metadata while running:** it would tell a GET at once that an offset is not written yet.
4. **Prefetch:** one batch ahead, or strictly one batch at a time?
5. **Page size:** can a single stored page exceed a sensible memory bound? That limits how small `maxPages` can make a batch.
6. **Name:** `getPageBatch` or `readPages`. `getPageBatch` sits next to `getPages`.

The driver mode adds more questions and proposals for this interface (status with the batch, an end flag, a `BlockFactory`, a reader object, page references). They are in section 15.11.

### 12.8 Compatibility

- Keep `getPages` for the row-range read.
- A new abstract method needs implementations in `InMemoryStreamStore`, `NoopStreamStore` and the test class `DelegatingStreamStorage`.
- The method is additive, so the POST writer is not affected.
- Reusing the stream POST machinery (`PageStreamPublisher` and `EsqlStreamResponseListener`) for this loop is a separate choice, see the comparison of the two writers.

## 13. Design: a change notification in `StreamStorage` (instead of polling)

Status: design only. Nothing in this section is implemented. It is a possible replacement for the polling in `StreamTaskManagementService.attempt` (section 4.3). The first iteration keeps `wait_timeout` with polling, see follow-up 23.

### 13.1 What the GET needs

To learn that there is a row at `offset` yet, or that the stream is no longer running. After that one wake-up the GET reads with `getPages`, as now. A wake-up is only a hint. The data always comes from the storage.

Today the GET polls because `StreamStorage` has no way to say that something changed. Async does not poll: the GET registers a listener on the in-memory task (`addCompletionListener`), wraps it with a timeout (`ListenerTimeouts.wrapWithTimeout`), and the task calls the listener when the query finishes. A stream GET has no task to listen to, so the notification has to come from the storage.

### 13.2 Interface: a listener that can be added and removed

It mirrors `StoredAsyncTask.addCompletionListener` and `removeCompletionListener`.

```java
/**
 * Completes the listener with the current metadata as soon as the stream has more than
 * {@code knownRows} rows stored, or is no longer running, or no longer exists (failure).
 * If that is already true, it completes at once. Closing the returned handle removes the listener.
 */
Releasable addProgressListener(String streamId, long knownRows, ActionListener<StreamMetadata> listener);
```

- The storage does not know about timeouts. The GET adds them as async does, with `ListenerTimeouts.wrapWithTimeout`: on a timeout it closes the handle and returns the empty running result. A task cancellation closes the handle too.
- The metadata needs a number of rows stored while the stream is running (open item 3 of 12.7, `rowsStored`), so `knownRows` can be compared. A version number that increases with every append would do as well.

### 13.3 Properties

| Property | Rule |
|---|---|
| No lost wake-up | The condition is checked when the listener is added, under the same lock as the writes |
| Hints, not data | After every wake-up the GET reads the metadata and then the pages again. An early or spurious wake-up costs one read. |
| Terminal events | Completion, failure, cancellation and delete wake every waiting listener |
| Expiry | A ttl passing makes no event. The GET's timeout covers it. |
| Many waiters on one stream | The storage can keep one registration per stream per node and fan out locally |
| Delivery | Listeners are completed on a generic executor, so a slow listener cannot stall the writer |

### 13.4 Implementations

**`InMemoryStreamStore` (easy):** a list of listeners per stream, guarded by the stream lock. `appendPage`, `markComplete`, `markFailed` and `delete` complete the matching listeners after the change. This is the technique of `StoredAsyncTask.addCompletionListener`.

**The real system-index store (the hard part):** an index gives no push. A document written on node A does not tell a GET on node B, and Elasticsearch has no change feed or watch for new documents in an index. Refresh listeners make a write visible, they do not tell a reader about it. Two options:

| Option | How it works | Notes |
|---|---|---|
| **1. Polling inside the storage, with coalescing** | The storage polls itself behind `addProgressListener`. There is one poll loop per stream per node, with backoff and jitter, and it stops when nobody is waiting. Every GET waiting on that stream on that node registers a listener, and the loop wakes them all. | 100 clients waiting on one stream cost one loop (two storage reads per tick) instead of 100 loops. The GET code is the same as with a real notification, so the storage can change its mechanism later without touching the GET. |
| **2. A local fast path plus a remote wake-up** | The metadata holds `taskId`, which names the node that writes the stream. On that node, the storage keeps an in-memory list of listeners and wakes them right after it indexes a page. A GET on another node registers a watch with the writer node through a transport action, and the writer sends a ping after indexing a page. | Real push. The ping only says "look again", so the data still comes from the index, which keeps the rule that GET reads nothing from node memory. If the writer node dies, no ping arrives and the timeout ends the wait. It needs a new transport action, with its own versioning, security and failure handling. |

**Recommendation:** start with option 1, then move to option 2 if polling load shows up.

### 13.5 How the GET would change

```
read once (metadata, then pages)
rows found, or the status is terminal      -> done
otherwise:
  handle = addProgressListener(id, knownRows = offset, wake-up listener)
  wrap it with the remaining deadline; close the handle on a timeout or a cancellation
  on a wake-up: read again (in a loop, since a wake-up can come from rows after the offset only)
```

- `StreamTaskManagementService.attempt` loses its scheduling and backoff, and keeps the loop and the total deadline.
- The rule that the metadata is read before the pages stays.

### 13.6 Open questions

1. **The condition:** `knownRows` (needs `rowsStored` in the metadata), a version number, or "pages after the offset"?
2. **The handle:** a `Releasable` as above, or a `Cancellable`?
3. **The delivery thread:** a generic executor, as in 13.3, or the writer's thread?
4. **The cross-node protocol of option 2:** the name of its transport action, its versioning and its security.
5. **Is it needed before polling hurts?** The two storage reads of a poll are cheap, so wait for load numbers from the real store.

## 14. Plan: stream the GET result through the publisher/subscriber model of the streaming POST

Status: implemented, with both read modes switchable. Section 14.9 lists what was built and where it differs from this plan. It replaces the single `getPages` read of the first iteration (the GET holds every page it returns) by the model the streaming POST uses: pages flow one at a time from a producer to the HTTP response, and the next pages are read only when the client is ready for them. It builds on the page-granular read of section 12. Both storage reads (`getPages` and `getPageBatch`) stay available behind a hidden switch, so they can be benchmarked against each other, see 14.8.

### 14.1 How the streaming POST does it

| Piece | Role |
|---|---|
| `PageStreamPublisher` (compute module) | A `Flow.Publisher<Page>` with a buffer. Producers call `registerProducer()`, then `addPage(page)` and `finish()`. The owner ends the stream with `completeWithFooter(...)` or `failStream(e)`. |
| Demand | The subscriber calls `subscription.request(1)`. Each unit of demand delivers one page, unchanged. |
| Backpressure | `waitForWriting()` returns a listener that completes when `demand > buffer.size()`, so a producer adds a page only when the client asked for one that is not already buffered. |
| Stop | `subscription.cancel()` releases the buffered pages and makes `addPage` return false. `addCloseListener` and `isClosed()` tell the producer. |
| `StreamingPageOperator` | The producer of the POST: the query's driver stalls on `waitForWriting()` and adds its pages. |
| `EsqlStreamResponseListener` | The subscriber of the POST: it turns each page into one `ChunkedRestResponseBodyPart`, and calls `request(1)` when the HTTP layer asks for the next part. The response is not sent before the first page, the footer or an error arrives. |

Findings from reading the code:

- `PageStreamPublisher` has nothing specific to a query. Everything the GET needs is public: `registerProducer`, `waitForWriting`, `Producer.addPage`, `Producer.finish`, `completeWithFooter`, `failStream`, `addCloseListener`, `isClosed`, `subscribe`. **It can be reused without any change.**
- The completion signal needs a footer (`pagesFinished && footer != null`) and outstanding demand, so the producer must call `completeWithFooter`, and the subscriber must keep requesting until it gets `onComplete`.
- `waitForWriting()` completes its listener on the thread that raised the demand, which is an HTTP thread. A producer that reads from the storage must fork before reading.
- `EsqlStreamResponseListener` would almost work for the GET as it is, but it lacks four things: `id` and `query_status` in the first line, `next_offset` counted from the request's `offset`, `max_rows`/`max_bytes`, and the GET's rule for `complete`.

### 14.2 Target design

```
 TransportEsqlStreamGetResultsAction                       HTTP layer
   |  metadata, 404 checks, wait for the first rows           ^
   |  (unchanged, sections 4.2 and 4.3)                       | one part at a time
   v                                                          |
 StreamStoragePageProducer  --addPage-->  PageStreamPublisher  --onNext-->  StreamNdjsonBody
   reads the next batch of pages            buffer, demand,                 (the subscriber)
   only when waitForWriting() says          backpressure                    first line, rows,
   the client wants a page                  (reused as it is)               limits, last line
        ^                                                                        |
        +---------------- request(1) when the HTTP layer asks for the next part -+
```

**The producer: `StreamStoragePageProducer`** (new, `streaming` package)

This is the `getPageBatch` producer. A second producer that keeps the single `getPages` read feeds the same publisher, and a hidden switch picks one of the two, see 14.8.

- It is the single producer of a `PageStreamPublisher` created for the GET.
- Loop: wait for `waitForWriting()`, fork to an executor, read the status and then the next batch with `getPageBatch(id, nextOffset, maxPages, maxBytes)` (section 12), trim the first page to the requested `offset` and the last page to `max_rows` with `Page.slice`, `addPage` each page, and repeat from `batch.nextOffset()`.
- It ends with `finish()` and `completeWithFooter(...)` when a batch is empty (the end of a terminal stream, or nothing more stored in a running stream) or when `max_rows` rows were added.
- A storage failure, a deleted stream or an expired stream ends it with `failStream(e)`.
- It stops when `addPage` returns false or the publisher is closed (the client left or a byte limit was reached).
- It keeps the last metadata it read, and the reason it stopped, for the subscriber.

**The subscriber: `StreamNdjsonBody`** (changed)

- It becomes a `Flow.Subscriber<Page>`. The queue of pages it holds today goes away: `next()` calls `subscription.request(1)`, and `onNext` answers the HTTP layer with a page part.
- `max_bytes` stays here, because it counts rendered bytes. When a limit is reached it cancels the subscription and writes the last line itself.
- `onComplete` writes the last line from the producer's final state. `onError` writes an error last line.
- The handling of "the HTTP layer asked before the page arrived" and "the page arrived before the HTTP layer asked" is the pattern of `EsqlStreamResponseListener` (`nextBodyPartListener`, a pending part, `request(1)` outside the lock).

**The transport action and the response**

- The metadata lookup, the 404 checks, the status-only path, the offset check and the wait for the first rows stay where they are. A failure there is still a normal HTTP error.
- The first batch is read in the action, as today, so the response only starts when there is something to say. Its pages are the first ones added to the publisher.
- `GetStreamResultResponse` carries the publisher and the producer's state instead of all the pages.

### 14.3 Behaviour changes

| Aspect | Today | With this plan |
|---|---|---|
| Storage reads per GET | One `getPages` for the whole range | One batch of whole pages at a time, read only when the client is ready |
| Memory of a GET | Every page it returns | One batch plus the page being written |
| `max_bytes` without `max_rows` | Reads the whole rest of the result, writes a little | Stops reading soon after the limit is reached |
| A slow client | Slows the writing only | Slows the storage reads too |
| A failure, delete or expiry after rows were written | Cannot happen, the read is finished before the response | Error in the last line, with the right `next_offset` (HTTP status is already 200) |
| `query_status` in the first line | The status when the pages were read | The status when the first batch was read. The last line still says whether the result is complete. |
| A running stream | Returns the rows stored at the time of the read | Returns rows until a batch comes back empty, so it can include rows stored while the response was being written |
| Parameters, limits, 404 and 400 rules, `wait_timeout` | | Unchanged |

### 14.4 Decisions to make

| # | Decision | Options | Recommendation |
|---|---|---|---|
| 1 | The subscriber | **A.** The GET's own subscriber in `StreamNdjsonBody`. **B.** Generalise `EsqlStreamResponseListener` with four hooks (first-line fields, offset base, row limits, completeness rule) and use it for both. | **A** now. It leaves the POST code, which others are changing, untouched. B is the better end state and fits the moment `POST /_query/stream` is built. The cost of A is a second copy of the continuation logic. |
| 2 | The storage read | **A.** `getPageBatch` from section 12 (whole pages, no document read twice). **B.** `getPages` with a row count per batch (no interface change, but a page that spans two batches is read twice). | **Both, behind the switch of 14.8.** Add `getPageBatch` to `StreamStorage` first, keep `getPages` as the second producer, benchmark, then delete the one that loses. A is the expected winner. |
| 3 | The batch size | Number of pages, stored bytes, or both | Both, as in section 12. Start with a small constant number of pages and a byte bound, and tune with the real storage. |
| 4 | The executor of the storage reads | `GENERIC`, `SEARCH`, or the ES|QL worker pool | To decide with the real storage. Never the HTTP thread. |
| 5 | Prefetch | Read one batch ahead while the previous one is written, or strictly on demand | Strictly on demand first. The publisher's rule (`demand > buffer.size()`) gives that for free. |
| 6 | The final state for the last line | Extend `PageStreamPublisher.StreamFooter`, or let the subscriber read it from the producer | From the producer. It keeps the compute module unchanged. |
| 7 | The benchmark switch | A pragma, a hidden URL parameter, or a dynamic cluster setting | A hidden URL parameter that only snapshot builds accept, see 14.8. |

### 14.5 Steps

1. **Storage:** add `getPageBatch`, `StoredPage` and `PageBatch` to `StreamStorage` (section 12), with implementations in `InMemoryStreamStore`, `NoopStreamStore` and the test class `DelegatingStreamStorage`. Unit tests for the in-memory implementation: the page that contains an offset, the page and byte bounds, an empty batch, ownership and release.
2. **Producers:** add `StreamStoragePageProducer`, with the loop of 14.2, and `StreamStorageSingleReadProducer`, which feeds the pages of one `getPages` call (14.8). Both implement one small interface, so the rest of the code does not know which one runs.
3. **Service and response:** `GetStreamResultRequest` carries the read mode. `StreamResultsService` creates the publisher and the producer of that mode after the first read. `GetStreamResultResponse` carries them. `StreamTaskManagementService` makes the first read with `getPageBatch` or `getPages`, depending on the mode.
4. **Subscriber:** turn `StreamNdjsonBody` into a `Flow.Subscriber<Page>`. Bring back the "failure after rows were written" last line.
5. **REST handler:** `processResponse` subscribes the body and sends the response. Closing the response cancels the subscription. `parseRequest` reads `read_mode` and rejects it outside snapshot builds.
6. **Tests:** see 14.6.
7. **Design doc:** update sections 4.3, 4.4, 9 and 11 (the flow charts and the worked example), and close follow-up 22.
8. **After the benchmark:** delete the producer that lost, the `read_mode` parameter and the request field (follow-up 24).

Steps 1 and 2 can be reviewed on their own. Steps 3 to 5 change the behaviour together.

### 14.6 Test plan

| Area | Tests |
|---|---|
| Producer | No storage read without demand. One batch per unit of demand at most. The first page is trimmed to `offset`, the last one to `max_rows`. It stops on cancel, on a closed publisher and when `addPage` returns false. An empty batch ends a terminal stream as complete and a running stream as not complete. A storage failure, a delete and an expiry end with `failStream`. Reads never run on the calling thread of `waitForWriting`. |
| Memory | With many pages and a client that reads slowly, the breaker never holds more than one batch plus one page. Everything is released after success, failure, cancel and a reached limit. |
| Subscriber | The existing `StreamNdjsonBodyTests` cases for offsets, limits, the status line and the last line. Both orders of "page arrives" and "HTTP layer asks". A limit reached in the middle of a page cancels the subscription and still writes the last line. An error after rows goes in the last line, an error before rows is an HTTP error. |
| Resume | A resume loop over a large result with random `max_rows` and `max_bytes` gives every row once. |
| Integration | `StreamEsqlGetResultActionIT`: a result of many pages read with small batches, a client that stops reading in the middle (the pages are released and the reads stop), a stream deleted while it is read, a stream that keeps growing while it is read, several GETs on one stream. |
| Both modes | The producer tests that do not depend on the number of reads (trimming, limits, cancel, failures, release) run once per read mode, with a randomly chosen or parameterized mode. The subscriber and REST tests do not depend on the mode. A resume loop gives the same rows in both modes. |
| Switch | `read_mode` is parsed (`pages`, `batch`), an unknown value is a 400, and any value is a 400 outside snapshot builds. The default is `batch`. |
| Kept for the `pages` mode | `testOneReadPerGetAndNothingIsReadWhileWriting`, `testNoLimitsReturnsEverythingInOneRead` and `testTheSingleReadAsksForTheRestOfTheResultWithoutOverflowing` assert the single read. They move to the tests of the single-read producer, and go away with it. |

### 14.7 Risks

- **Concurrency:** the subscriber is called from the producer's thread and from HTTP threads. The lock ordering rule of `EsqlStreamResponseListener` applies: never call `request` while holding the body's lock.
- **A second copy of the continuation logic** (decision 1, option A), until the two writers are merged.
- **More storage round trips** than one read. The batch size is the lever.
- **A response can now end with an error after a 200.** Clients must read the last line, as they must for the streaming POST.
- **The in-memory store shares blocks between readers** (follow-up 19). The tests keep calling `Page.allowPassingToDifferentDriver()`, and more threads touch the pages with this plan.
- **Two read paths to keep working** while the switch exists (14.8). It is limited to the two producers and their tests, and it has an end date: the benchmark.

### 14.8 Benchmark switch: `getPages` and `getPageBatch` side by side

Status: implemented. The goal is to compare the two storage reads with numbers before one of them is removed.

**Where the switch sits**

The switch does not choose between today's code path and the publisher path. That would measure two things at once (the storage call and the way the response is delivered), and it would keep two response writers in `StreamNdjsonBody`. Instead, every GET goes through `PageStreamPublisher`, and the switch only picks the producer:

```
                         read_mode=pages                      read_mode=batch (default)
                  StreamStorageSingleReadProducer          StreamStoragePageProducer
                  one getPages(offset, rows) call          one getPageBatch per round of demand
                              \                                   /
                               +-----> PageStreamPublisher <-----+
                                               |
                                        StreamNdjsonBody  (one subscriber, one writer, one set of limit rules)
```

| Producer | Storage call | Memory held | Reads after a limit or a cancel |
|---|---|---|---|
| `StreamStorageSingleReadProducer` | One `getPages(offset, rows)` call, as in the first iteration. The pages are then added to the publisher one at a time, as the client asks. | Every page the GET returns | None, but everything was already read |
| `StreamStoragePageProducer` | One `getPageBatch` per round of demand (14.2) | One batch plus the page being written | None |

- Both producers implement one small interface (start with the result of the first read, stop, final metadata and stop reason), so `StreamResultsService`, the response and the subscriber are the same in both modes.
- The single-read producer is small: it holds the list that `getPages` returned and adds the next page each time `waitForWriting()` completes. It replaces the queue of pending pages that `StreamNdjsonBody` has today.
- The wait for the first rows (`wait_timeout`) is the same in both modes. Only the call it polls differs.
- **Optional third producer:** `getPages` called in row windows on demand. It shows what `getPageBatch` buys over the existing interface, including the page that is read twice when it spans two windows (decision 2, option B). Add it only if the benchmark needs that answer.

**How the switch is set**

| Option | Fit |
|---|---|
| Pragma | Does not fit. Pragmas are a field of the POST body (`EsqlQueryRequest`, parsed into `QueryPragmas`, rejected outside snapshot builds). The GET has no body and no `EsqlQueryRequest`. Storing the POST's pragma in `StreamMetadata` would fix the mode per stream, so the two modes could not be compared on the same stored result. |
| **Hidden URL parameter (chosen)** | `GET /_query/stream/{id}?read_mode=pages\|batch`. Accepted only in snapshot builds, otherwise a 400, the same rule as for pragmas. Not documented and not in the REST spec. It is set per request, so a benchmark can read the same stored stream in both modes. |
| Dynamic cluster setting | Easier for a benchmark tool that cannot change URLs, but it is a registered setting that must be removed later, and it switches every request at once. Only if the URL parameter turns out not to be enough. |

- The default is `batch`. `pages` is the baseline.
- The parameter name is not part of the API in `esql_streaming_api.md` and must not appear in the docs.

**What to measure**

- Peak heap and circuit breaker use of one GET, and of many GETs at once.
- Time to the first row, and the total time for a full read.
- The number of storage round trips, and the cost of a GET that stops early (`max_bytes` without `max_rows`, or a client that disconnects).
- The dimensions: result size, page size, `max_rows` and `max_bytes`, a slow client, a running stream and a completed one.

**Caveats**

- **No real numbers before the index-backed store exists.** With `InMemoryStreamStore` both calls only slice a list, and production uses `NoopStreamStore`. The switch is worth building when that store is close.
- **`getPageBatch` on every store:** `StreamStorage` gets a default implementation built on `getPages` (a row window), so a store that has not implemented it yet works in both modes. A benchmark of such a store compares `getPages` with itself.
- **Removal:** the producer that loses, the parameter and the request field are deleted after the benchmark (step 8, follow-up 24).

### 14.9 Implementation status

Implemented. `GET /_query/stream/{id}` reads in batches by default, and `?read_mode=pages` (snapshot builds only) switches one request back to the single `getPages` read. Both modes go through the same publisher and the same response writer.

**What was built**

| Piece | Where | Notes |
|---|---|---|
| `StreamStorage#getPageBatch`, `StoredPage`, `PageBatch` | `streaming/StreamStorage.java` | A `default` method built on `getPages`, with a window of `maxPages * 1000` rows. `InMemoryStreamStore` overrides it and returns whole pages. `NoopStreamStore` uses the default and returns an empty batch. |
| `GetStreamResultRequest.ReadMode` | `streaming` | `PAGES` or `BATCH`, default `BATCH`. Part of `equals` and `hashCode`. |
| `read_mode` | `RestEsqlGetStreamResultAction.parseRequest` | `pages` or `batch`. A 400 for another value, and for any value outside snapshot builds. |
| `StreamPageProducer` | `streaming` | The shared part: owns the `PageStreamPublisher`, adds one page each time the publisher is writable, trims to `max_rows`, ends with `completeWithFooter` or `failStream`, and releases its pages when it is closed or the subscriber cancels. |
| `StreamStorageSingleReadProducer` | `streaming` | `PAGES` mode, and every status-only request. No read after the first. |
| `StreamStoragePageProducer` | `streaming` | `BATCH` mode. Reads the metadata and then the next batch from where the previous one ended. |
| `StreamTaskManagementService.PageReader` | `streaming` | The one read that differs between the modes (`rowRangeReader`, `pageBatchReader`). `getCurrentResult` and the `wait_timeout` polling take a reader, so the wait is the same code in both modes. |
| `StreamNdjsonBody` | `action` | A `Flow.Subscriber<Page>`. It asks for one page when the HTTP layer asks for the next part. The queue of pending pages is gone. |
| `GetStreamResultResponse` | `streaming` | Carries the first metadata and the producer. `publisher()` gives the pages, `latestMetadata()` the metadata of the last read. |

**Where it differs from the plan**

- **`PageBatch` has the requested offset:** `PageBatch(fromOffset, pages)`, so that `nextOffset()` of an empty batch is the requested offset.
- **Batch size:** 16 pages and 4 MB of stored bytes (`StreamStoragePageProducer.DEFAULT_BATCH_PAGES` and `DEFAULT_BATCH_BYTES`). They are constructor parameters of `StreamResultsService`, so tests use small batches. They are not settings.
- **Executor:** the generic pool, the one the `wait_timeout` polling uses. There is a `TODO` to decide it with the real storage.
- **No read that comes back empty for a completed stream:** when the status is complete and the batch ended at the total number of rows, the producer ends without another read. A running stream ends with the first empty batch.
- **The last line uses the metadata of the last read:** a stream that completes or fails while the response is written is reported as such in the last line, while the first line still says `running`.
- **A failure after the response started** (storage failure, stream deleted or expired) is an error in the last line with its own status, `is_partial: true`, `complete: false` and the right `next_offset`.
- **The optional third producer** (`getPages` in row windows) was not built as a mode. The default `getPageBatch` is exactly that read, so a storage without its own `getPageBatch` shows its cost.
- **Sections 4 and 11** still describe the single read. They are accurate for `read_mode=pages` and for everything up to the first read.

**Tests**

| Test | What it covers |
|---|---|
| `StreamStoragePageBatchTests` (new) | `getPageBatch` in the in-memory store and the default implementation: the page that contains an offset, `maxPages`, `maxBytes`, an empty batch, an unknown stream, the no-op store, reading a whole result once in batches. |
| `StreamPageProducerTests` (new) | The batch mode: a batch is read only when the client asks, later reads never run on the asking thread, `max_bytes` and `max_rows` stop the reads, closing the body in the middle, a running stream, rows stored while the response is written, a stream that fails or is deleted while it is read, a storage failure, both modes give the same response, a storage with only `getPages`. |
| The existing tests in `streaming` | They pick the read mode and the batch size randomly. The three tests that assert the single read, and the one about a completion between the metadata and the read, set the `pages` mode. |
| `GetStreamResultRequestTests`, `RestEsqlGetStreamResultActionTests` | The read mode and the `read_mode` parameter. |
| `StreamEsqlGetResultActionIT` | Every GET picks a random mode. `testBothReadModesOverSeveralBatches` reads 40 to 80 pages in both modes over HTTP. |

**Not done**

- No benchmark was run. With the in-memory store both reads only slice a list (follow-up 24).
- `StreamNdjsonBody` has its own copy of the "who asked first" logic of `EsqlStreamResponseListener` (decision 1, option A).

## 15. Design: a driver mode for the stream GET (follow-up 25)

Status: the one-stage version is implemented as a third read mode, `read_mode=driver`. Section 15.12 lists what was built and where it differs from this design, including two claims of 15.3 that turned out not to hold (the tasks API and task cancellation). The two-stage variant (15.6) and the storage improvements (15.11) are not implemented.

**Direction: the GET lines up with the POST as much as possible.** The driver mode is therefore the intended end state of the GET, not only a third mode to benchmark. Section 15.10 lists, piece by piece, what the GET shares with the POST and what cannot be shared.

**One limit on that direction, for now: `EsqlStreamResponseListener` is not changed or generalised.** The one-stage version reuses the POST's pieces as they are (driver, `StreamingPageOperator`, `PageStreamPublisher`) and keeps the GET's own subscriber, `StreamNdjsonBody`, as decided in 14.4. Sharing the listener is a possible later step, recorded in 15.10 and not part of this plan.

The idea is to read a stored result the way a query reads an index: a driver moves pages from a source operator to a sink, and the sink is the one the streaming POST already uses. `StreamStorage` plays the role of the index behind `LuceneSourceOperator`, and the loading of the stored pages plays the role of `ValuesSourceReaderOperator`.

### 15.1 What the streaming POST does today

The POST is a normal ES|QL query whose last operator feeds the publisher:

1. `TransportEsqlStreamQueryAction` creates a `PageStreamPublisher`.
2. The physical plan gets a `StreamingOutputExec` node on top, which holds the publisher.
3. `LocalExecutionPlanner` turns that node into a `StreamingPageOperator` sink.
4. The coordinator's driver runs the pipeline. The sink is blocked on `publisher.waitForWriting()` until the client asks for a page, and the driver parks while it is blocked.
5. The transport action ends the stream with `completeWithFooter` or `failStream`.

The GET of section 14 uses the same publisher but no driver. `StreamPageProducer.pump()` does by hand what the driver does: wait until the publisher is writable, leave the HTTP thread, add a page or read more, repeat.

### 15.2 The two variants

```
One stage (first step, read_mode=driver):

  Driver on the esql_worker pool
    StreamStorageSourceOperator  -->  StreamingPageOperator  -->  PageStreamPublisher  -->  StreamNdjsonBody
    calls getPageBatch, trims to       the sink of the POST,        unchanged                 unchanged
    offset and max_rows, emits         unchanged
    the stored pages

Two stages (later, with the index-backed store):

  Driver on the esql_worker pool
    StreamStorageSourceOperator  -->  StoredPageReaderOperator  -->  StreamingPageOperator  -->  publisher  -->  body
    emits references to pages:         loads the pages of the
    page number, offsets, size         references, trims
    (like LuceneSourceOperator         (like ValuesSourceReaderOperator
    emits doc ids)                     loads the values)
```

| | One stage | Two stages |
|---|---|---|
| Storage calls | `getPageBatch`, as the batch mode | A call that lists page references, and a call that loads pages by reference |
| `StreamStorage` change | None | New calls, or the page offsets and sizes in `StreamMetadata` |
| What the source emits | Result pages | Small blocks that say which pages to load |
| What is new compared with the batch mode | Who drives the reads: the driver, not `pump()` | Also how pages are found and loaded |
| Storage reads for the same request | The same as the batch mode | Fewer and exact, see 15.6 |

The one-stage variant is the batch mode of section 14 with the producer's loop replaced by a driver. It makes the same `getPageBatch` calls and returns the same response, so it isolates one question: is a driver worth its overhead for a read with no computation.

### 15.3 One-stage design

**`StreamStorageSourceOperator`** (new, a `SourceOperator`)

| Method | Behaviour |
|---|---|
| Constructor | Takes the storage, the stream id, the request offset, `max_rows`, the batch bounds and the first read (`StreamPages`), which the transport action made before the response started. |
| `getOutput()` | Returns the next page it holds, trimmed to the offset and to the rows left for `max_rows`. When it holds none and more may be stored, it starts the next read (metadata, then `getPageBatch` from where the last batch ended) and returns `null`. |
| `isBlocked()` | Returns the listener of the read in flight, or not blocked. The read completes on a storage thread, and the driver resumes on its own executor. |
| `isFinished()` | True when `max_rows` rows were emitted, when a batch came back empty, or when the stream is complete and the last batch ended at its total number of rows. These are the rules of `StreamStoragePageProducer`. |
| `finish()`, `close()` | Stop reading and release the pages it holds. A read in flight releases its pages when it returns. |
| `status()` | Batches read, pages and rows emitted, the offset of the next read. It shows in the tasks API. |
| Failure | A failed read (storage failure, stream deleted or expired) is rethrown from `getOutput()`, which fails the driver. |
| `metadata()` | The metadata of the latest read, for the last line. |

**Backpressure comes from the driver loop.** The driver only calls `getOutput()` on an operator when the next operator needs input (`Driver.java`, `op.isFinished() == false && nextOp.needsInput()`). `StreamingPageOperator.needsInput()` is true only when the publisher is writable. So the source reads from the storage only when the client asked for a page, with no code for it in the source.

**Wiring in `StreamResultsService`**

1. Unchanged: metadata lookup, security and expiry (404), the status-only path, the first read with the `wait_timeout` polling, the offset and failure checks. A failure there is still a normal HTTP error.
2. Create the `PageStreamPublisher`, a `DriverContext` and a `Driver` with the source, no intermediate operator and a `StreamingPageOperator` (with an identity alignment) as the sink.
3. `Driver.start(threadContext, executor, driver, maxIterations, listener)` on the `esql_worker` pool. The listener calls `publisher.completeWithFooter(...)` on success and `publisher.failStream(e)` on failure, as `TransportEsqlStreamQueryAction` does.
4. `GetStreamResultResponse` carries the publisher and the source (for `latestMetadata()`). Closing the response cancels the driver.

**What stays as it is**

- `StreamNdjsonBody`: it subscribes to the publisher and counts `max_bytes`, in every mode.
- `PageStreamPublisher` and `StreamingPageOperator`: reused without a change.
- The response: the same lines in every mode.

**What the driver replaces**

| Concern | Batch mode (`StreamPageProducer`) | Driver mode |
|---|---|---|
| Waiting for the client | `pump()` and `waitForWriting()` | The sink's `isBlocked()`, handled by the driver |
| Leaving the HTTP thread | `fork()` to the generic pool | The driver's executor, the `esql_worker` pool |
| The client leaves | The close listener of the publisher stops the producer | `StreamingPageOperator` throws `DriverEarlyTerminationException`, the driver closes its operators |
| A read fails | `fail()` calls `failStream` | The driver fails, its listener calls `failStream` |
| Cancelling the task | Not connected after the first read | Connected, but it cannot happen after the first read either, see 15.12 |
| Visibility | None | None in the tasks API with this design, see 15.12 |

### 15.4 The switch

`read_mode=driver` joins `pages` and `batch` (section 14.8): `GetStreamResultRequest.ReadMode.DRIVER`, accepted only in snapshot builds, not documented. `batch` stays the default while the driver mode is built and compared. Once it holds up, `driver` becomes the default and the only mode, and the two producers of section 14 and the switch are removed, unless the benchmark of follow-up 24 shows a cost that outweighs lining up with the POST. The three modes share the first read, the subscriber and the publisher, so a comparison of `batch` and `driver` measures only the driver.

### 15.5 Decisions to make

| # | Decision | Options | Recommendation |
|---|---|---|---|
| 1 | Where the first read is made | **A.** In the transport action, as today, and handed to the source. **B.** In the source, with the body waiting for the first page, the end or an error before it sends the response, as `EsqlStreamResponseListener` does. | **A.** The 404 and 400 rules and `wait_timeout` do not move, and `StreamNdjsonBody` needs no change. B only pays off together with a shared listener, which is deferred (15.10). |
| 2 | `max_rows` | In the source, or an existing `LimitOperator` after it | In the source. It also stops the reads, and the first step then has no intermediate operator. |
| 3 | `wait_timeout` | Keep the polling before the driver starts, or a source that is blocked until rows arrive | Keep the polling. A blocked source fits the change notification of section 13 and can come with it. |
| 4 | Executor | `esql_worker`, or the generic pool | `esql_worker`, as every other driver. It answers the open executor question of 14.4. |
| 5 | The task | Run the driver under the task of the GET, or under a child task | The task of the GET, which is already cancellable. |
| 6 | Circuit breaker and block factory | The node's block factory through a `DriverContext` | As the query does. The stored pages are deserialized with it. |
| 7 | The subscriber | Keep `StreamNdjsonBody`, or generalise `EsqlStreamResponseListener` and use it for both | Keep `StreamNdjsonBody`. `EsqlStreamResponseListener` is not touched for now. |
| 8 | The last line | From the metadata of the source, as today, or from a `PageStreamPublisher.StreamFooter` passed to `completeWithFooter` | From the metadata of the source, as today. `StreamFooter` has no field for the stored status or the total number of rows, which the GET needs for `complete`, and `StreamNdjsonBody` already works this way. The driver's listener still calls `completeWithFooter` to end the stream, with an empty footer. |

### 15.6 The two-stage variant

It only pays off with the index-backed store, and it should be designed together with its documents:

- **Deterministic document ids:** one document per page with the id `streamId:pageNumber`, and the page offsets and stored sizes in the metadata (or in a small listing document).
- **The source needs no search:** it emits references from the metadata. The reader loads the pages with a get by id.
- **No refresh delay:** a get by id is real-time, so the visibility question of section 12.7 goes away for the pages. It remains for the metadata if that is read by search.
- **Exact batches:** the stored size is known before a page is loaded, so a batch can be sized in bytes exactly, and `max_rows` and `offset` choose the pages without reading any.
- **Parallel loads:** the reader can load the pages of one batch in parallel and still emit them in order.

It needs a storage interface for "the page references of a stream" and "load these pages", which is a change to the interface of PR #165893. That is the reason to start with one stage.

### 15.7 Costs and risks

- **A driver per GET:** a `DriverContext`, a `Driver` and its status reporting, for a read that computes nothing. This is the overhead the benchmark has to weigh against the hand-written loop.
- **Worker pool:** a GET now competes with queries for `esql_worker` threads. A driver that is blocked holds no thread, so a slow client costs none.
- **Pages made outside a driver:** stored pages are built on a storage thread and passed to the driver. They need `Page.allowPassingToDifferentDriver()`, and the in-memory store shares blocks between readers (follow-up 19).
- **Two ways to end the stream:** the driver's listener and the subscriber's cancel can race. The publisher already handles that for the POST, and the tests of 15.8 cover it for the GET.
- **A third mode to keep working** until the benchmark removes the ones that lose.

### 15.8 Test plan

Nothing here exists yet. The tests follow the ones of section 14.9, so that the three modes are held to the same behaviour.

**Source operator: `StreamStorageSourceOperatorTests`** (new, extends the compute module's `SourceOperatorTestCase` or `OperatorTestCase`, which run an operator in a real driver and check that every block is released)

| Case | Expectation |
|---|---|
| A completed stream | The operator emits every row from the offset, in order, and finishes. |
| Offset inside a page | The first page is trimmed, nothing before the offset is emitted. |
| `max_rows` inside a page and at a page boundary | The last page is trimmed, the operator finishes, and no batch is read after it. |
| Batches | With one page per batch and N pages, N reads. No read comes back empty for a completed stream. |
| A running stream | It finishes with the first empty batch, and `metadata()` still says running. |
| The stream completes or fails between two batches | `metadata()` has the status of the last read. |
| A read fails, the stream is deleted, the stream expires | `getOutput()` throws, the driver fails with that exception, and the pages held are released. |
| `isBlocked()` | Not blocked while it holds pages, blocked while a read is in flight, and the read is only started by `getOutput()`. |
| `close()` during a read in flight | The pages of that read are released when it returns. |
| `status()` | Counts of batches, pages and rows, and its serialization (`AbstractWireSerializingTestCase` for the status class, as for the other operators). |

**On-demand reads and the driver: additions to `StreamPageProducerTests`**, run for both `batch` and `driver` (rename the class to cover both, or parameterize it)

| Case | Expectation |
|---|---|
| A batch is read only when the client asks | The same counts of reads as the batch mode, part by part. |
| Reads never run on the thread that asks | The reads after the first run on an `esql_worker` thread. |
| `max_bytes` and `max_rows` stop the reads | No read after the limit. |
| The body is closed in the middle | The driver ends with an early termination, not a failure, and everything is released. |
| Rows stored while the response is written | They are returned, and the last line has the latest status. |
| A stream that fails, is deleted or expires while it is read, and a storage failure | An error in the last line with its status, `is_partial: true`, `complete: false` and the right `next_offset`. |
| The task of the GET is cancelled while the response is written | The driver stops, the response ends with an error in the last line, and everything is released. This is new: the batch mode does not connect the task after the first read. |
| A storage with only `getPages` | Works through the default `getPageBatch`. |

**Same response in every mode**

- `testBothReadModesGiveTheSameResponse` becomes a test of all three modes: random streams, offsets and limits give equal first lines, rows and last lines.
- The shared tests of the package (`StreamNdjsonBodyTests`, `StreamResultsServiceTests`, `StreamTaskManagementServiceTests`) pick the mode randomly from the three. The tests that assert the single read keep the `pages` mode.

**Request and REST**

- `GetStreamResultRequestTests` and `RestEsqlGetStreamResultActionTests`: `read_mode=driver` is parsed, and it is a 400 outside snapshot builds, as the two other values.

**Integration: `StreamEsqlGetResultActionIT`**

| Case | Expectation |
|---|---|
| The random mode of every GET | Includes `driver`. |
| `testBothReadModesOverSeveralBatches` | Covers the three modes. |
| A GET in the driver mode that is waiting for a slow client | It is visible in the tasks API with a driver status, and it holds no `esql_worker` thread: other queries still run with the pool at its smallest size. |
| The client disconnects in the middle | The task and the driver are gone shortly after, and the breaker is back to its level. |
| Many GETs at once on one stream | All return the whole result. |

**Not in this test plan**

- The two-stage variant (15.6). It gets its own plan with the index-backed store.
- The benchmark itself (follow-up 24): peak heap and breaker use, time to the first row, total time, and the cost of a driver per GET for small results, where the fixed overhead matters most.

### 15.9 Steps

1. `StreamStorageSourceOperator` and its tests. It can be reviewed on its own.
2. `ReadMode.DRIVER`, the parsing of `read_mode=driver`, and the wiring in `StreamResultsService` (driver, listener, response).
3. The shared tests pick from three modes, and the driver-specific cases of 15.8.
4. The integration cases.
5. Update sections 9 and 14.9 of this document, and follow-up 25.
6. Make `driver` the only mode and remove `StreamPageProducer`, its two subclasses and `read_mode`. `StreamNdjsonBody` stays.

Not a step of this plan: sharing `EsqlStreamResponseListener` between the POST and the GET (15.10, "Deferred").

### 15.10 Lining up with the POST

The target is that a GET is a POST whose pages come from the storage and not from a query. In the one-stage version everything between the source and the subscriber is the same code as the POST. The subscriber stays the GET's own.

| Piece | POST | GET today (section 14) | GET in the one-stage driver mode |
|---|---|---|---|
| Who moves the pages | A driver | `StreamPageProducer.pump()` | A driver |
| Where the pages come from | The query pipeline (exchange source and the operators after it) | `getPages` or `getPageBatch`, called by the producer | `StreamStorageSourceOperator` |
| Sink | `StreamingPageOperator` | None, the producer adds to the publisher itself | `StreamingPageOperator` |
| Publisher | `PageStreamPublisher` | The same | The same |
| End of the stream | The transport action calls `completeWithFooter(StreamFooter)` or `failStream` | The producer does, with an empty footer. The body reads the final state from the producer. | The driver's listener calls `completeWithFooter` (empty footer) or `failStream`. The body reads the final state from the source. |
| Hand-off to the REST layer | `EsqlStreamQueryAction.ResultStream` (columns, publisher, null columns, time zone) | `GetStreamResultResponse` (metadata, producer) | `GetStreamResultResponse` (metadata, publisher, source). Unchanged shape. |
| Subscriber and writer | `EsqlStreamResponseListener` | `StreamNdjsonBody` | `StreamNdjsonBody`, unchanged. Not lined up, on purpose. |
| When the response starts | With the first page, the footer or the error, so a failure before the first row is an HTTP error | After the first read, which the transport action makes | As the GET today |
| Rows, `complete`, `next_offset` | One line per row. `next_offset` is the number of rows written. | The same lines. `next_offset` starts at the request's `offset`. | As the GET today |
| Executor | `esql_worker` | Generic pool | `esql_worker` |
| Cancel and disconnect | Task cancel and early termination of the driver | The publisher's close listener | As the POST |
| Status in the tasks API | Driver and operator status | None | As the POST |

**Deferred: one listener for both**

Not part of the one-stage version and not planned for now. `EsqlStreamResponseListener` stays as it is. This is kept as a record of what sharing it would take, for when `POST /_query/stream` gets its `store` modes and limits.

The listener lacks four things for the GET (14.1). Each one is something the POST of `esql_streaming_api.md` needs too, so they are features of the listener and not GET-only hooks:

| Need | For the GET | For the POST |
|---|---|---|
| Extra fields in the first line | `id`, `query_status` | `id`, when the result is stored (`store: overflow` or `all`) |
| An offset base for `next_offset` | The request's `offset` | 0 |
| `max_rows` and `max_bytes`, counted while rows are written | Yes | In the API spec, not implemented yet |
| Who decides `complete` | The stored status and the total number of rows | The normal end of the query |

If it is done, the limits are written once, in the listener, and the POST gets them from the same change. Until then the cost is the one already named in 14.7: `StreamNdjsonBody` has its own copy of the "who asked first" logic.

**What cannot line up**

| Piece | Why |
|---|---|
| The plan and the planner | A GET has no query, so there is no `StreamingOutputExec` and no `LocalExecutionPlanner`. The GET builds its driver directly: one source and the sink. |
| The checks before the stream starts | The stream lookup, the owner check, the expiry and the offset check are only for a stored result. They stay in `StreamResultsService`, before the driver starts. |
| `wait_timeout` | The POST has no such wait. It stays in front of the driver (polling), or becomes a blocked source with the change notification of section 13. |
| The footer's query fields | `took`, `documents_found`, `_clusters` and `profile` describe a running query. A GET's last line has the stored `is_partial` and warnings, and leaves the others out unless they are stored with the stream. |
| The time zone | The POST uses the query's zone. The GET writes UTC until the zone is stored with the stream (follow-up 17). |
| The transport action | `TransportEsqlStreamQueryAction` plans and runs a query. The GET keeps its own small action. |
| The subscriber (for now) | `EsqlStreamResponseListener` is not changed, so the GET keeps `StreamNdjsonBody`. |

**Effect on the test plan of 15.8**

- None for the one-stage version: `StreamNdjsonBody` and its tests are unchanged, and `EsqlStreamResponseListenerTests` is not touched.
- One comparison worth adding anyway: a query run with the POST, and the same rows stored and read with a GET, give the same row lines. It pins the two writers to each other while they are separate.
- If the listener is shared later, the limit and error cases move to `EsqlStreamResponseListenerTests`, which must first pass unchanged with the default values.

### 15.11 `StreamStorage` and the driver mode

**Is `StreamStorage` like `LuceneSourceOperator`?**

Not quite. It is closer to the shard that `LuceneSourceOperator` reads than to the operator itself. The operator is the piece still to be written.

| Query side | Stream GET side |
|---|---|
| The Lucene index of a shard (`IndexReader`, `ShardContext`) | `StreamStorage` |
| `LuceneSourceOperator`: owned by one driver, keeps a position, emits pages when the driver asks | `StreamStorageSourceOperator` (15.3) |
| `ValuesSourceReaderOperator`: loads the values of the doc ids it receives | `StoredPageReaderOperator` (two stages only, 15.6) |

The differences that matter for the design:

| Aspect | `LuceneSourceOperator` | `StreamStorage` |
|---|---|---|
| State | Holds a cursor: which slice, which document | Stateless. Every call names the stream and an offset. |
| Scope | Reads only | Also writes (`create`, `appendPage`, `markComplete`, `markFailed`) and deletes |
| Sync or async | Reads synchronously on the driver thread | Asynchronous. The source operator blocks the driver with `isBlocked()`. |
| Memory | Builds its blocks with the `BlockFactory` of the driver | Has no such parameter. Which breaker its pages are charged to is not defined. |

**Proposed improvements to the read side**

Ordered by how much they help the one-stage version. None is implemented.

| # | Improvement | What it changes | Needed for |
|---|---|---|---|
| 1 | Status with the batch | `PageBatch` carries the status and the total number of rows as of the read. A reader makes one call per batch and not two, and the rule "metadata before pages" moves into the storage, which can make it atomic. | One stage |
| 2 | An end flag | The batch says "this is the end of a complete stream". It replaces the guess made today from `nextOffset` and the total number of rows (`StreamStoragePageProducer.mayHaveMore`). | One stage |
| 3 | A `BlockFactory` parameter | `getPageBatch(..., blockFactory, listener)`. The real store deserializes the pages on the breaker of the driver, as the Lucene readers do. It also settles ownership: the pages belong to the caller alone and are not shared with other readers, which is what the in-memory store does today (follow-up 19). | One stage |
| 4 | A reader object | `openReader(streamId, fromOffset)` returns an object with `readNext(maxPages, maxBytes, listener)` and `close()`. The source operator owns it for its lifetime, as `LuceneSourceOperator` owns its slice. A real store can keep a point in time or a search-after cursor in it. The cost: the storage holds state per reader, which must be released when a client disconnects. | Later |
| 5 | A change notification | `addProgressListener` of section 13. The source is blocked until rows arrive, which replaces the `wait_timeout` polling (15.5, decision 3). | Later |
| 6 | Page references | A call that lists pages (number, offsets, stored size) and a call that loads pages by reference. | Two stages (15.6) |

**A sketch of 1 to 3**

```java
void getPageBatch(
    String streamId,
    long fromOffset,
    int maxPages,
    long maxBytes,
    BlockFactory blockFactory,          // 3: where the pages are allocated
    ActionListener<PageBatch> listener
);

record PageBatch(
    long fromOffset,
    List<StoredPage> pages,
    StreamMetadata.Status status,       // 1: the status as of this read
    long totalRows,                     // 1: only meaningful when the status is COMPLETE
    boolean endOfStream                 // 2: no page will ever follow these
) implements Releasable { ... }
```

- `endOfStream` is true only when the status is terminal and the batch ends at the last stored row. An empty batch of a running stream has `endOfStream == false`: the caller decides whether to stop or to wait.
- The last line of the response needs more than the status (the warnings, `is_partial`, the failure), so the batch carries the whole `StreamMetadata` or the reader makes one `getMetadata` call at the end. To decide with the real store.
- A failed stream: the failure is in the metadata, as today. The batch does not fail because the stream failed, since the rows stored before the failure are still readable.

**Recommendation**

- **With the one-stage driver mode:** 1, 2 and 3. They are small additions to `getPageBatch`, they halve the storage calls of a batch, and they make the memory accounting of the driver correct. `getPages` is not changed, and the `pages` and `batch` read modes keep working, since they can ignore the new fields.
- **With the index-backed store:** 4 to 6. They depend on how its documents and ids are designed, so they should be decided with it.
- **Compatibility:** `getPageBatch` was added by this work and has no caller outside the GET, so changing its signature is cheap now. `InMemoryStreamStore`, the default implementation on top of `getPages`, and the test storages (`DelegatingStreamStorage`, `GetPagesOnlyStreamStorage`) follow.

**Tests to add with 1 to 3** (to `StreamStoragePageBatchTests`)

| Case | Expectation |
|---|---|
| The status of a running, a complete and a failed stream | The batch has the status and, when complete, the total number of rows. |
| `endOfStream` | True for the last batch of a complete stream and for an empty batch past its end. False for every batch of a running stream, and for a batch of a complete stream that is cut by `maxPages` or `maxBytes`. |
| The stream completes between two batches | The first batch says running, the next one says complete and ends the stream. No row is lost or returned twice. |
| `BlockFactory` | The pages are charged to the breaker that was passed, and closing the batch brings it back to zero. Two readers of one stream do not share blocks. |
| The default implementation on top of `getPages` | It fills the status from `getMetadata`, read before the pages. |

### 15.12 Implementation status of the one-stage version

Implemented. `GET /_query/stream/{id}?read_mode=driver` (snapshot builds only) runs the GET as a driver. `batch` is still the default, and `pages` still works. The three modes return the same response.

**What was built**

| Piece | Where | Notes |
|---|---|---|
| `StreamStorageSourceOperator` | `streaming` | A `SourceOperator`. Holds the pages of one batch, starts the next `getPageBatch` read when the driver asks for a page and it has none, and is blocked while the read runs. Trims to the offset and to `max_rows`. A failed read is thrown from `getOutput()`. It registers each read as an async action of the `DriverContext`, so the driver does not report that it is done before a late read released its pages. |
| `StreamStorageDriver` | `streaming` | Builds the driver (the source, no intermediate operator, a `StreamingPageOperator` with an identity alignment) and starts it. The driver's listener calls `completeWithFooter` or `failStream`. Closing it cancels the driver. |
| `StreamStorageDriver.Settings` | `streaming` | What a driver needs from the node: `BigArrays`, `BlockFactory`, the executor, the cluster and node names. |
| `StreamResultPages` | `streaming` | The small interface the response and the body use: `publisher()`, `metadata()`, `close()`. `StreamPageProducer` (modes `pages` and `batch`) and `StreamStorageDriver` (mode `driver`) implement it. |
| `ReadMode.DRIVER`, `read_mode=driver` | `GetStreamResultRequest`, `RestEsqlGetStreamResultAction` | Next to `pages` and `batch`. |
| `StreamResultsService` | `streaming` | Makes the first read with `getPageBatch` for both `batch` and `driver`, then starts a producer or a driver. |
| `TransportEsqlStreamGetResultsAction` | `plugin` | Injects `BigArrays` and `BlockFactoryProvider`, and uses the `esql_worker` pool for the driver. |

Not changed: `StreamNdjsonBody`, `EsqlStreamResponseListener`, `PageStreamPublisher`, `StreamingPageOperator` and `StreamStorage`.

**Where it differs from the design**

- **No status in the tasks API, and no task cancellation while the response is written.** Section 15.3 claimed both, and both are wrong for this design. The transport action completes when it hands the response to the REST layer, and its task is unregistered at that moment, before the first page is written. So there is no task to look at or to cancel while the driver runs. The driver is still cancelled when the client disconnects, through the body's `close()`. The task is connected to the driver (`task.addListener`), which only matters between the first read and the hand-off. The streaming POST has both because its transport action stays open until the query ends and passes the stream to the REST layer on the side (`resultStreamListener`). Getting them for the GET means doing the same, which goes with decision 1, option B, and is deferred with it.
- **No operator status.** `StreamStorageSourceOperator` has a `toString()` with its stream, next offset and number of batches, but no `Operator.Status`. A status is a named writeable and needs a transport version, and nothing can display it while there is no task (previous point).
- **The first read stays in the transport action** (decision 1, option A), and the last line is still written from the metadata of the source (decision 8).
- **The storage is unchanged.** The improvements 1 to 3 of section 15.11 (status and end flag with the batch, a `BlockFactory` parameter) are not implemented. The source still reads the metadata before every batch, and the pages are allocated by the storage, not on the breaker of the driver (follow-up 26).
- **`max_rows` is in the source**, with no `LimitOperator` (decision 2).
- **The switch stays.** `batch` is the default until the modes are compared (follow-up 24). Removing the two producers and `read_mode` (step 6 of 15.9) is not done.

**Tests**

| Test | What it covers |
|---|---|
| `StreamStorageSourceOperatorTests` (new) | The operator called as a driver calls it: pages in order, an offset inside a page, `max_rows` inside a page and at a page boundary, reads only when asked for a page, a running stream, a completion between two batches, blocked while a read runs, closed while a read runs, a failed read, a stream deleted between two batches, `finish()`. It calls the operator directly and does not extend the compute module's `OperatorTestCase`. |
| `StreamPageProducerTests` | Every test picks `batch` or `driver` randomly: reads only when the client asks, reads off the asking thread, `max_bytes` and `max_rows` stop the reads, the body closed in the middle, rows stored while the response is written, a stream that fails or is deleted while it is read, a storage failure, a storage with only `getPages`. `testEveryReadModeGivesTheSameResponse` compares the three modes on random streams, offsets and limits. |
| The other tests in `streaming` | They pick the mode randomly from the three. |
| `GetStreamResultRequestTests`, `RestEsqlGetStreamResultActionTests` | `driver` is parsed, and is a 400 outside snapshot builds. |
| `StreamEsqlGetResultActionIT` | Every GET picks a random mode from the three. `testEveryReadModeOverSeveralBatches` reads 40 to 80 pages in each mode over HTTP, on the real `esql_worker` pool. |

**From the test plan of 15.8, not done**

- The tasks API case and the task cancellation case: they cannot happen with this design (first point above).
- The integration case for a client that disconnects in the middle. The unit test that closes the body in the middle covers the release of the pages, not the HTTP path.
- The serialization test of the operator status: there is no status.
- The comparison of the row lines of a POST and of a GET (15.10).
- In the unit tests the generic pool stands in for `esql_worker`. Only the integration test runs on the real pool.

## 16. Brainstorm: reading from the system index, and whether to build read operators now

Status: proposal for review. Nothing here is implemented. PR #166343 added `.esql-stream-results` and the metadata document, but not the storage that writes and reads pages, so most of the read side is still open.

### 16.1 What the index gives us, and what it does not say yet

From `EsqlStreamResultsIndexDescriptor`:

| Fact | Consequence for a reader |
|---|---|
| One shard per stream: documents are routed by `stream_id`, 6 shards in total, replicas `0-1` | A GET reads from one shard copy. With 6 shards, a GET usually lands on a node other than the one that got the HTTP request, so a remote hop is the normal case, not the exception. |
| Index sorted by `(stream_id, seq)` | The pages of a stream are a contiguous, ordered run of documents in a segment. Reading "from page n on" is a range, not a sort. |
| The metadata is document `seq=0`, id `streamId`, read today with a real-time get | Already works with `GET /.esql-stream-results/_doc/{id}?routing={id}` (see `EsqlStreamResultsIndexIT`). |
| A page is one document: `seq`, `offset` (first row), `size` (stored bytes), `transport_version`, `page` (binary doc values), all single-valued doc values, `index: false` except `page` | The page bytes are read from doc values, not from `_source`. The wire version is stored per page, so the reader decodes with that version. |
| Refresh interval `-1` | **Pages are not searchable until something refreshes the shard.** This is the largest open question, see 16.3. |
| Columnar index mode, pages indexed as one columnar batch | The writer is a bulk of pages. Its design is not in the repository yet. |

Not decided anywhere yet, and needed by any reader: the **document id of a page** (`streamId:seq` is the natural one, it would allow get by id), **how `offset` is found** (a GET asks for a row offset, a document is a page), and **how a running stream's pages become visible**.

### 16.2 Where the reads could run

```
A. Storage on the coordinator, reads through a client          B. Shard-side source operator, coordinator-side decode

   driver (esql_worker, coordinator node)                         shard node                              coordinator node
     StreamStorageSourceOperator   (as today)                       StreamPageSourceOperator  --exchange-->  StoredPageDecodeOperator
       -> StreamStorage.getPageBatch                                  (Lucene doc values,                      -> StreamingPageOperator
          -> index-backed impl: get/mget or search                     emits the raw page bytes)                -> PageStreamPublisher
          -> transport hop to the shard
```

| | A. `StreamStorage` over the index, no new operator | B. Two stages: shard-side source, coordinator-side decode |
|---|---|---|
| New compute code | None. The index-backed `StreamStorage` implements `getPageBatch`. | A shard-side source operator and a decode operator, plus the plumbing to run a data-node driver outside a query. |
| Use of the driver design | Section 15 as built: one driver, one source, the POST's sink | Section 15.6 realised: the first stage finds and reads pages, the second turns them into pages. |
| Transport | The client call (get, mget, search) or, better, a small dedicated transport action that returns the page bytes | The exchange of ES\|QL (backpressure, cancellation, status in the tasks API) |
| Decode | Once, on the coordinator | Once, on the coordinator, **if** the exchange carries the stored bytes untouched. If the shard node decoded, the exchange would encode again: a second serialization of the same data. |
| Visibility of unrefreshed pages | Possible with get by id (real-time) | Needs a searcher, so needs a refresh (16.3) |
| Offset to page | One extra lookup | The Lucene query does it (a range on `offset`) |
| Risk | Low. Reversible. The storage interface hides it. | High. System-index access from compute, a plan or driver without the planner, security and tasks on the shard node. |

The point that decides between them is the second row of 16.1: **the hop is unavoidable, so the real question is what travels and what it costs, not where the operator sits.** What travels should be the stored bytes, decoded once at the end. Both A (with a dedicated transport action, not JSON `docvalue_fields`, which would base64 the page) and B can do that. B adds the exchange machinery on top, which is worth it only if it is wanted for itself.

### 16.3 The blocking question: visibility of pages of a running stream

The GET has to serve rows while the query is still running (iteration 1 requirement). With a refresh interval of `-1`:

| Way to read | Sees unrefreshed pages? | Notes |
|---|---|---|
| Get or mget by id (`streamId:seq`) | Yes, real-time. Whether it forces a refresh internally depends on the engine path for this index and has to be checked. | Needs deterministic page ids. A reader finds the end by probing the next `seq` (not found means nothing more yet). |
| Search / Lucene source operator | No, only after a refresh | Refresh per read is too expensive when many streams run. Refresh by the writer every n pages or n bytes costs the writer and still delays rows. A refresh at `markComplete` helps completed streams only. |

So the shape of the read depends on a decision of the writer's author, and **the read operators of B cannot be finished before it**. Proposed rule to agree: page documents have id `streamId:seq`; running streams are read by id; the writer or the metadata update at completion refreshes, so a completed stream may use search if that turns out faster.

### 16.4 Offset to page

A GET asks for a row offset. Options: (1) a lookup of the last page with `offset <= x` (a range on a doc-values-only long, on a sorted index, one document), then reads by id from there; (2) the client passes back `next_offset`, which is a row offset again, so (1) is still needed for the first read of each request; (3) keep a small page index (`seq`, `offset`, `size`) in the metadata document at completion, which also gives exact batch sizes in bytes, as 15.6 wanted. Proposal: (1) first, (3) if the lookup shows up in measurements.

### 16.5 Is this the time to build the operators?

Short answer: **not the index-reading operators yet, but yes to the work that does not depend on them.**

What can start now, because it holds whatever the index looks like:

1. **15.11 items 1 to 3** on `StreamStorage.getPageBatch`: status and an end flag with the batch, and a `BlockFactory` parameter. The index-backed store needs exactly this contract (one call per batch, pages allocated on the driver's breaker), and it is cheap to change while there is one caller.
2. **Settle the page document with the writer's author** (16.1 and 16.3): ids, visibility, whether `offset` lookup is by search, whether the metadata carries a page index. This is a conversation, not code, and it gates everything below.
3. **A decode path with the stored format**, test-first against bytes written with `Page.writeTo` at a recorded `transport_version`, still behind `StreamStorage` (for example inside the in-memory store, which could then store bytes instead of shared blocks). That also removes follow-up 19 (shared blocks between readers) for good, as a side effect.

What should wait:

4. **The index-backed `StreamStorage`** (option A, with a dedicated transport action to the shard that reads the `page` doc values and returns bytes). Next after 1 to 3, once the writer exists. Measure it with the `batch` and `driver` modes (follow-up 24).
5. **The shard-side source operator and the exchange (option B).** Build it only if the numbers of step 4 show that the hop or the pool is the cost, or if we want the GET visible in the tasks API through the exchange. Until then it is a large cost (system-index access from compute, security origin, a driver outside the planner, refresh) for a gain we cannot yet measure.

**What stays as it is.** `StreamStorageSourceOperator` is already the right shape: it only knows `StreamStorage`, so an index-backed storage drops in without changing the driver, the sink or `StreamNdjsonBody`. If B is ever built, this operator becomes the second stage's input side (or is replaced by the exchange source), and nothing above it changes.

### 16.6 Open questions for review

| # | Question | My lean |
|---|---|---|
| 1 | Page document id | `streamId:seq`, so a page is read with a real-time get |
| 2 | How rows of a running stream become visible | Read by id while running. Refresh at completion. Confirm that a real-time get does not force a refresh of the shard on every read |
| 3 | Where the metadata read happens | Keep it as a get by id, in the same call as the pages if the batch call allows it (15.11 item 1) |
| 4 | Offset to page | A lookup first, a page index in the metadata later if needed |
| 5 | Dedicated transport action versus the client API for reading pages | Dedicated, to return raw bytes and not base64 in JSON. Needs a system-index read with the `ESQL_ORIGIN` and the security check of the stream headers made by the caller before it |
| 6 | Option B at all | Not now. Revisit after the measurements of follow-up 24 on the index-backed store |
| 7 | Who owns the writer and its page layout | To be confirmed before step 4. The reader should not guess it |
