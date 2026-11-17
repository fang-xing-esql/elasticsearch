# ES\|QL stream API

Every result is streamed, from the query or from storage, and is never held whole in memory. A query can also store its result so a client can resume reading it later, from any row.

## Start a query: `POST /_query/stream`

```
POST /_query/stream
{
  "query": "FROM logs-* | WHERE status >= 500 | KEEP @timestamp, host, bytes",
  "store": "overflow",
  "keep_alive": "1h",
  "max_rows": 1000,
  "max_bytes": "1mb"
}
```

| Parameter | Default | Description |
| :---- | :---- | :---- |
| `store` | `none` | `none`, `overflow` or `all`. See the store modes below. |
| `keep_alive` | `5d` | How long stored rows are kept. Only with `overflow` or `all`. |
| `max_rows` | none | Ends the response after this many rows. |
| `max_bytes` | none | Ends the response after about this many bytes, checked between rows. |
| `format` | `ndjson` | Response format, also set with the `Accept` header. Only `ndjson` for now. |

- **Limits:** if both are set, the first one reached ends the response.  
- **`max_rows` is not `LIMIT`:** `LIMIT` changes the result, while `max_rows` only splits how the result is read.  
- **Parameters from `_query` that also apply here:**   
  - `params`, `filter`  
  - `locale`, `time_zone`  
  - `Allow_partial_results`  
  - `drop_null_columns`, `profile`  
  - `include_execution_metadata`  
  - `project_routing`   
  - `approximation`  
  - `column_metadata`  
- **Parameters that do not apply:** `columnar` and `delimiter`.  
- **Compression:** responses are compressed when the client sends `Accept-Encoding: gzip`.

The response is NDJSON (`application/x-ndjson`), one row per line:

```
{"id":"FmNJRUZ1YWZ...","columns":[{"name":"@timestamp","type":"date"},{"name":"host","type":"keyword"},{"name":"bytes","type":"long"}]}
["2026-10-29T10:00:01.000Z","web-1",5120]
["2026-10-29T10:00:01.250Z","web-3",null]
...
{"status":200,"took":84,"is_partial":false,"warnings":[],"complete":false,"next_offset":1000}
```

- **First line:** the columns and their types, plus the `id` when the result is stored.  
- **Rows:** one array per row, values in column order, with `null` when there is no value.  
- **Last line:** how the response ended.  
  - `complete` is true when the whole result has been sent.  
  - `next_offset` is the first row that was not sent.  
  - It also includes today's other fields when they apply, such as `documents_found`, `_clusters` and `profile`.

### Store modes

| `store` | What is stored | Query lifetime | `id` | When a limit is reached |
| :---- | :---- | :---- | :---- | :---- |
| `none` | Nothing | Ends with the connection | No | The query is cancelled, with `complete: false` |
| `overflow` | Only the rows not delivered, from `next_offset` on | Runs to completion | Yes | The rest is stored from `next_offset` on |
| `all` | Every row, from row 0 | Runs to completion | Yes | Every row stays readable |

With `overflow`, a result that fits in the response stores nothing.

A response that ends cleanly, at a limit or at the end of the result, can always be resumed from `next_offset`. After a disconnect, the server cannot know which rows reached the client, so only `all` guarantees that the client can resume from the last row it received.

## Resume: `GET /_query/stream/{id}`

`GET` reads the stored result. The query is not run again, so rows come back in the same order and an offset always points to the same row.

```
GET /_query/stream/FmNJRUZ1YWZ...?offset=1000&max_rows=1000&max_bytes=1mb
```

| Parameter | Default | Description |
| :---- | :---- | :---- |
| `offset` | `0` | First row to return, as a row number. |
| `max_rows` | none | Same as on the `POST`. |
| `max_bytes` | none | Same as on the `POST`. |
| `wait_timeout` | to decide | How long to wait for rows when the query is still running. |

The response has the same shape as the `POST`: first line, rows, last line.

```
{"id":"FmNJRUZ1YWZ...","columns":[...],"query_status":"running"}
["2026-10-29T10:00:09.800Z","web-2",740]
...
{"status":200,"is_partial":false,"warnings":[],"complete":false,"next_offset":2000}
```

- **A query that is still running:** rows stream as they are stored, until a limit, the end of the result or `wait_timeout`.  
  - `query_status` is `running`, `completed`, `failed` or `cancelled`.  
  - If no rows arrive before `wait_timeout`, the response has only the first and last lines, with `complete:false` and the same `next_offset`.  
- **Which offsets work:**  
  - With `all`, any offset.  
  - With `overflow`, only offsets from the first stored row on. An earlier offset returns an error that names the first available row.  
- **Reading the rest:** the client calls again with `offset = next_offset` until `complete` is true. It can also jump to any offset, or come back later within `keep_alive`.  
- **Status only:** `max_rows=0` returns the first and last lines without rows.  
- **Several reads at once:** several `GET`s on the same `id` can run together, since they only read.

## Delete: `DELETE /_query/stream/{id}`

Cancels the query if it is still running and deletes the stored rows. Otherwise they expire after `keep_alive`.

## Errors

- **Before the first row:** a regular HTTP error status with the standard error body.  
- **After rows were sent:** the status is already 200, so the error is in the last line.

```
{"status":500,"took":1203,"is_partial":true,"complete":false,"next_offset":1534,"error":{"type":"...","reason":"..."}}
```

## Examples

```
# Show the first screen fast, keep the rest only if it doesn't fit
POST /_query/stream   {"query": "...", "store": "overflow", "max_bytes": "1mb"}
GET  /_query/stream/{id}?offset=<next_offset>&max_bytes=1mb

# Save a result to come back to later
POST /_query/stream   {"query": "...", "store": "all", "keep_alive": "7d"}
GET  /_query/stream/{id}?offset=0

# Check progress without reading rows
GET  /_query/stream/{id}?max_rows=0
```

## Always streamed

No step holds the whole result in memory. Each one keeps only a few pages at a time and lets the next step's speed set the pace.

- **From the query to the client:** pages leave the exchange, go through the format, and are written to the connection as the client asks for more. A slow client slows the query; it never makes the server buffer.  
- **From the query to the index (`overflow`, `all`):** each page is serialized once and indexed in the background, and only the bytes not yet acknowledged are kept. When there are too many, the query waits for the index.  
- **From the index to the client (`GET`):** stored pages are read in order, a few at a time, rendered, then released. Reading row 10 million costs the same as reading row 0\.  
- **Stopping at a limit:** `max_rows` and `max_bytes` are counted while rows are written. In `overflow` mode, nothing is held back to decide what to store: rows sent before the limit are not stored, and rows after it go to the index.  
- **Pages:** results are produced and stored as ES\|QL pages, which are self-contained columnar blocks sized by the engine. Each format renders pages without regrouping them.  
- **What the client controls:** only `offset`, `max_rows` and `max_bytes`, and `next_offset` comes back.

So memory per query stays the same whether the result has a thousand rows or a billion, and whether it is read live or from the index.

## Formats

NDJSON is the first format, not the only one. Adding a format means writing a renderer for pages. The API, the storage and the execution stay the same.

A renderer has three steps:

1. **Start:** write the columns, their types and the `id`.  
2. **Page:** write one page, as it arrives, without regrouping it.  
3. **End:** write how the response ended: status, `is_partial`, warnings, `complete`, `next_offset`, or the error.

Each format stops at its own boundary when it reaches `max_rows` or `max_bytes`: a row for row formats, a page for block formats. `next_offset` always points to the next row, so clients resume the same way in every format.

| Format | Kind | Start | Each page | End | Stops at |
| :---- | :---- | :---- | :---- | :---- | :---- |
| NDJSON | rows | columns line | one line per row | status line | a row |
| CSV / TSV | rows | header line | one line per row | none, an error cuts the stream | a row |
| Arrow IPC stream | columnar blocks | schema message | one record batch | metadata on a final message | a page |
| Parquet | columnar file | file header | one row group | file footer, with status in its metadata | a page |

**Columnar formats fit just as well as row formats.**   
A page is columnar inside and complete on its own: every column for its range of rows. Our own binary encoding, used for the exchange and for storage, is already columnar in this way. So a result is a stream of columnar blocks, which maps to Arrow record batches and Parquet row groups. A columnar format never needs the whole result: it writes one block per page as pages arrive. A row format rebuilds rows inside each page at the moment it writes them. `offset` and `max_bytes` stop at a page boundary for columnar formats and at a row for row formats, and both resume at `next_offset`.

- **Every response is a complete unit of its format.** A Parquet response is a valid file, and the next `GET` returns the next file from `next_offset`.  
- **The format is chosen per request,** with `format` or the `Accept` header. A result stored by one request can be read back in any format, because the index holds pages, not formatted bytes.  
- **Public formats stay standard.** A binary format exposed to clients should be a standard one like Arrow, not our internal page encoding, which is tied to the transport version.

**Multi-valued fields need care in fixed-schema formats.**   
Arrow and Parquet fix each column's type before the first block, but a column read from a field can turn out to be multi-valued in any page. NDJSON is not affected, since each value carries its own shape.

- **Types are decided once, when the query starts.** Columns from fields are declared as lists. Columns known to be single-valued, such as expressions and aggregation results, keep their plain type.  
- **Every response for the result uses the same types,** whether it is read live or later from storage, so resuming never changes the schema.

## Security and running time

- **Who can read or delete:** only the user who started the query, as with `_query/async`. The security headers are stored with the result and checked on every `GET` and `DELETE`.  
- **API keys:** a result started with an API key can be read or deleted only with that same key.  
- **Unknown id:** for anyone else, the result does not exist (`404`).  
- **Running time:** stored queries outlive the connection, so they have a maximum running time. When it is reached, the query is cancelled and the rows already stored stay readable.  
- **`keep_alive`:** if it expires while the query is still running, the query is cancelled and its rows are deleted.  
- **Upgrades:** each page is serialized once, when it is stored, at the cluster's minimum transport version, as `_query/async` does. Every node can read it during a rolling upgrade, at no extra cost.

## Conclusion

The NDJSON format keeps today's first and last lines and writes one row per line, in place of up to 1000 rows per `{"values":[…]}` line.

- **Simpler for clients:** reading a line gives one row, with no `values` loop and no large line to buffer.  
- **Simpler for the server:** `batch_size` is gone, along with the code that regroups pages to match it. Rows keep today's encoding: an array of values in column order.  
- **Page size belongs to the query, not the stream API.** It is set by the engine for each query (about 256KB by default, or the `page_size` pragma). The stream API never sees it.  
- **The API does not depend on the format.** NDJSON is the first format. Arrow, Parquet or CSV only need a page renderer, and `offset`, `max_rows`, `max_bytes`, `next_offset` and `store` mean the same thing in every one (see Formats).  
- **Stored results do not depend on the format either.** They are stored once as ES\|QL pages, and any format can read them back.

## Open questions

- **Disconnects with `overflow`:** should the server keep the current response's rows until it ends cleanly? That makes resuming after a disconnect safe, and costs at most `max_bytes` of memory, but it is unbounded without a limit.  
- **`_query/async`:** does this API replace it for Kibana, and how does Kibana move over?  
- **Default values** for the maximum running time and `wait_timeout`.

