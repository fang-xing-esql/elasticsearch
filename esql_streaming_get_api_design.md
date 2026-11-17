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
| 22 | Read the pages in bounded chunks, instead of one `getPages` call for the whole range. `getPages` returns a list of pages, and each page is one document of the system index, so one call for a large result reads every document of the range and holds all the pages in memory until they are written. The real storage may also limit how many documents one read can return. Chunking would read `chunkRows` rows (or a number of pages) at a time, only when the HTTP layer asks for the next part, and release each page after it is written. Decide later whether it is needed, and whether the bound should be rows, pages or bytes. A page-by-page read in the storage would make the row count unnecessary. | A GET reads once and holds all the pages it returns. The memory of a GET is the size of what it returns. A huge result with no `max_rows` is read whole. |
| 23 | Replace the polling of `wait_timeout` by a change notification in `StreamStorage` (`addProgressListener`), first with polling inside the storage that is shared by all the waiters of a stream on a node, later possibly with a wake-up from the writer node. See section 13. | The GET polls the storage itself: each waiting GET reads the metadata and the pages every 50 to 250 ms. |

Possible later improvement: when fewer rows than `max_rows` are stored and the stream is running, optionally wait a short time to fill the response. Not in iteration 1.

## 9. Implementation status

Implemented as designed. The classes are placed like the async ones: the REST handler, action type and response writer in `org.elasticsearch.xpack.esql.action`, the transport action in `org.elasticsearch.xpack.esql.plugin`, and the service layer next to the storage in `org.elasticsearch.xpack.esql.streaming`, as `AsyncResultsService`, `AsyncTaskManagementService` and `AsyncSearchSecurity` sit next to the async storage.

| Class | Package | Async counterpart | Role |
|---|---|---|---|
| `RestEsqlGetStreamResultAction` | `action` | `RestEsqlGetAsyncResultAction` | `GET /_query/stream/{id}`, parameter parsing, chunked NDJSON response. Registered only when `EsqlCapabilities.Cap.STREAMING` is enabled (snapshot builds today), like streaming on `POST /_query`. |
| `EsqlStreamGetResultAction` | `action` | `EsqlAsyncGetResultAction` | Action type |
| `StreamNdjsonBody` | `action` | (`EsqlQueryResponse` serialisation) | Pull-based NDJSON writer: first line, rows with `max_rows` and `max_bytes`, last line. Public so tests outside the package can drive it, like `EsqlStreamResponseListener` for the POST. |
| `TransportEsqlStreamGetResultsAction` | `plugin` | `TransportEsqlAsyncGetResultsAction` | Runs on the receiving node, never forwards. Builds its storage in `createStreamStorage`. |
| `GetStreamResultRequest`, `GetStreamResultResponse` | `streaming` | `GetAsyncResultRequest` (in core, next to `AsyncResultsService`) | Local-only request (cancellable task) and response (the pages that were read) |
| `StreamResultsService` | `streaming` | `AsyncResultsService` | Metadata lookup, security and expiry, the single read of the pages (with the wait), offset and failure checks |
| `StreamTaskManagementService` | `streaming` | `AsyncTaskManagementService` | `getCurrentResult` and `addCompletionListener`: the `wait_timeout` polling |
| `StreamSearchSecurity` | `streaming` | `AsyncSearchSecurity` | The check against the stored headers, the same 404 for unknown, expired and not owned |
| `StreamPages` | `streaming` | none | The result of the read: the metadata and the pages |

The request and response stay in `streaming`, as `GetAsyncResultRequest` stays with its service, so that `action` depends on `streaming` and not the other way round.

### 9.1 Where the implementation differs from the plan above

- **The request is local-only**, as the existing stream query request is. It carries no wire format, so there is no wire round-trip test. `GetStreamResultRequestTests` covers the parameters, `validate()` and that it is not serializable.
- **Where the tests are:** `RestEsqlGetStreamResultActionTests` is in `action` and `StreamEsqlGetResultActionIT` and its plugin `StreamTestEsqlPlugin` are in `action` (`internalClusterTest`), next to the classes and tests of the async API. The other tests stay in `streaming`, with the shared test base class. `StreamNdjsonBodyTests` stays there because it uses that base class.
- **No chunking:** a GET makes one `getPages` call and writes all the pages it returns. The row-count chunking with a chunk size of 4096 that this design first had was removed to keep the first iteration simple, and is follow-up 22. `StreamChunk` is now `StreamPages`, and `StreamChunkSource` is gone.
- **Class names of tests:** the service tests (`StreamTaskManagementServiceTests`, `StreamResultsServiceTests`, `StreamSearchSecurityTests`) extend `ESTestCase` with a `TestThreadPool` instead of `ESSingleNodeTestCase`, because the in-memory store needs no node. The SEARCH-pool-busy test occupies the SEARCH pool of the `TestThreadPool`.
- **Added tests:** `StreamNdjsonBodyTests` (slicing and limits), `RestEsqlGetStreamResultActionTests` (parameters), `StreamEsqlGetResultActionIT` (REST on a real node, in `internalClusterTest`), and `RBACEngineTests#testStreamGetResultActionNeedsNoIndexPrivileges`.
- **Not implemented, moved to follow-ups 18 and 19:** `EsqlStreamSecurityIT`, API key unit cases.
- **`max_bytes` takes a byte size with a unit:** `10b`, `1mb`. A plain number other than `0` is a 400, like every byte size parameter. `0` means status only, and a negative value is a 400. A negative `max_rows` is a 400 too.
- **`format` is accepted and ignored** because every REST handler accepts it. The response is always NDJSON. `keep_alive` is rejected with a 400, as an unsupported parameter.

### 9.2 Changes outside the esql `streaming` package

- `x-pack/plugin/core`: `EsqlStreamActionNames` holds the action name `indices:data/read/esql/stream/get`, like `EsqlAsyncActionNames`.
- `x-pack/plugin/security`: `RBACEngine` has its own branch for the stream GET, with a new `isStreamRelatedAction(action)` that checks `ESQL_STREAM_GET_RESULT_ACTION_NAME`. The branch sits next to the async one and returns `ALLOW_NO_INDICES`. The action is not an indices request, so without it the action would need index privileges. The check that the caller owns the stream is done by the GET itself (`StreamSearchSecurity`), as for the async actions. The action is deliberately not added to `isAsyncRelatedAction`: it is not an async action, and a separate method names what it is and lets later stream actions that read a result join it.
- `StreamMetadata` (the PR #165893 interface): `Status` and `StreamCompletionInfo` are public, so classes outside the package can use `metadata.status()` and the completion info. Nothing else in the interface changed.
- `EsqlPlugin`: registers the action and the REST handler. It registers no stream component. The REST handler is gated with `EsqlCapabilities.Cap.STREAMING.isEnabled()`, the same switch as streaming on `POST /_query`, instead of a direct `Build.current().isSnapshot()` check. The two are equal today.

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

The entry point is the REST handler `action/RestEsqlGetStreamResultAction.java`, route `GET /_query/stream/{id}`. It exists only where `EsqlCapabilities.Cap.STREAMING` is enabled.

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

Status: design only. Nothing in this section is implemented. It is the storage side of follow-up 22 (bounded reads), and it replaces the one `getPages` call of the first iteration by reads of a few whole pages at a time.

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

