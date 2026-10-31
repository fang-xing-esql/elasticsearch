set -x

# Co-located join: `events` (many rows per session) and `sessions`
# (one row per session) are both routed by session_id, and both are cloned from
# an empty seed index so shard i of each index lands on the same node.
# Run against `./gradlew run --nodes=3`.

main() {
#cleanup
#pipeline
#seed
#create_events
#create_sessions
#unblock
#shards
#routing
#colocation
join
join_where
join_stats
#join_profile
#join_reference
#join_rejected
}

# -----------------------------------------------------------------------------
# cleanup
# -----------------------------------------------------------------------------
cleanup() {
curl -u elastic:password -X DELETE "localhost:9200/events/"
curl -u elastic:password -X DELETE "localhost:9200/sessions/"
curl -u elastic:password -X DELETE "localhost:9200/colocate_seed/"
curl -u elastic:password -X DELETE "localhost:9200/_ingest/pipeline/session-routing"
}

# -----------------------------------------------------------------------------
# ingest pipeline: copy session_id into _routing so a session lives in one shard
# -----------------------------------------------------------------------------
pipeline() {
curl -u elastic:password -X PUT "localhost:9200/_ingest/pipeline/session-routing?pretty" -H 'Content-Type: application/json' -d'
{
  "description": "Route every document by session_id so a session lives in one shard",
  "processors": [
    {
      "fail": {
        "if": "ctx.session_id == null || ctx.session_id == \"\"",
        "message": "session_id is required for routing"
      }
    },
    {
      "set": {
        "field": "_routing",
        "copy_from": "session_id",
        "override": true
      }
    }
  ]
}
'
}

# -----------------------------------------------------------------------------
# seed: an empty, write-blocked index that carries the shared layout.
# 3 primaries, no replicas, one shard per node (3-node cluster), routing
# required, routing pipeline as final_pipeline so it cannot be bypassed.
# Both real indices are cloned from it; ResizeAllocationDecider forces clone
# shard i onto the node holding seed shard i, which gives the alignment.
# number_of_replicas is not copied by clone, so each clone sets it explicitly.
# -----------------------------------------------------------------------------
seed() {
curl -u elastic:password -X PUT "localhost:9200/colocate_seed?pretty" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "index.routing.allocation.total_shards_per_node": 1,
    "index.routing.rebalance.enable": "none",
    "index.final_pipeline": "session-routing",
    "index.blocks.write": true
  },
  "mappings": {
    "dynamic": "strict",
    "_routing": { "required": true },
    "properties": {
      "session_id": { "type": "keyword" }
    }
  }
}
'
curl -u elastic:password -X GET "localhost:9200/_cluster/health/colocate_seed?wait_for_status=green&timeout=30s&pretty"
}

# -----------------------------------------------------------------------------
# events: many rows per session
# -----------------------------------------------------------------------------
create_events() {
curl -u elastic:password -X POST "localhost:9200/colocate_seed/_clone/events?wait_for_active_shards=1&pretty" -H 'Content-Type: application/json' -d'
{ "settings": { "index.number_of_replicas": 0 } }
'
curl -u elastic:password -X GET "localhost:9200/_cluster/health/events?wait_for_status=green&timeout=30s&pretty"
curl -u elastic:password -X PUT "localhost:9200/events/_mapping?pretty" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "@timestamp":  { "type": "date" },
    "seq":         { "type": "long" },
    "event_type":  { "type": "keyword" },
    "page":        { "type": "keyword" },
    "duration_ms": { "type": "long" }
  }
}
'
}

# -----------------------------------------------------------------------------
# sessions: one row per session (the dimension side of the join).
# s-500 has events but no session row; s-700 has a session row but no events.
# -----------------------------------------------------------------------------
create_sessions() {
curl -u elastic:password -X POST "localhost:9200/colocate_seed/_clone/sessions?wait_for_active_shards=1&pretty" -H 'Content-Type: application/json' -d'
{ "settings": { "index.number_of_replicas": 0 } }
'
curl -u elastic:password -X GET "localhost:9200/_cluster/health/sessions?wait_for_status=green&timeout=30s&pretty"
curl -u elastic:password -X PUT "localhost:9200/sessions/_mapping?pretty" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "started_at": { "type": "date" },
    "user_id":    { "type": "keyword" },
    "device":     { "type": "keyword" },
    "country":    { "type": "keyword" },
    "referrer":   { "type": "keyword" },
    "plan":       { "type": "keyword" }
  }
}
'
}

# -----------------------------------------------------------------------------
# clones inherit the write block from the seed; lift it, then load data
# -----------------------------------------------------------------------------
unblock() {
curl -u elastic:password -X PUT "localhost:9200/events,sessions/_settings?pretty" -H 'Content-Type: application/json' -d'
{ "index.blocks.write": null }
'
load_events
load_sessions
}

load_events() {
curl -u elastic:password -X PUT "localhost:9200/events/_bulk?refresh&pretty" -H 'Content-Type: application/json' -d'
{"index": {"_id": "1"}}
{"session_id": "s-100", "@timestamp": "2026-10-30T10:00:00Z", "seq": 1, "event_type": "view",     "page": "/home",     "duration_ms": 120}
{"index": {"_id": "2"}}
{"session_id": "s-100", "@timestamp": "2026-10-30T10:00:05Z", "seq": 2, "event_type": "click",    "page": "/pricing",  "duration_ms": 30}
{"index": {"_id": "3"}}
{"session_id": "s-100", "@timestamp": "2026-10-30T10:01:00Z", "seq": 3, "event_type": "purchase", "page": "/checkout", "duration_ms": 900}
{"index": {"_id": "4"}}
{"session_id": "s-200", "@timestamp": "2026-10-30T11:00:00Z", "seq": 1, "event_type": "view",     "page": "/home",     "duration_ms": 80}
{"index": {"_id": "5"}}
{"session_id": "s-200", "@timestamp": "2026-10-30T11:00:09Z", "seq": 2, "event_type": "click",    "page": "/docs",     "duration_ms": 45}
{"index": {"_id": "6"}}
{"session_id": "s-300", "@timestamp": "2026-10-30T12:00:00Z", "seq": 1, "event_type": "view",     "page": "/home",     "duration_ms": 60}
{"index": {"_id": "7"}}
{"session_id": "s-300", "@timestamp": "2026-10-30T12:00:20Z", "seq": 2, "event_type": "purchase", "page": "/checkout", "duration_ms": 700}
{"index": {"_id": "8"}}
{"session_id": "s-400", "@timestamp": "2026-10-30T13:00:00Z", "seq": 1, "event_type": "view",     "page": "/home",     "duration_ms": 50}
{"index": {"_id": "9"}}
{"session_id": "s-400", "@timestamp": "2026-10-30T13:00:30Z", "seq": 2, "event_type": "search",   "page": "/search",   "duration_ms": 210}
{"index": {"_id": "10"}}
{"session_id": "s-400", "@timestamp": "2026-10-30T13:01:00Z", "seq": 3, "event_type": "click",    "page": "/docs",     "duration_ms": 40}
{"index": {"_id": "11"}}
{"session_id": "s-500", "@timestamp": "2026-10-30T14:00:00Z", "seq": 1, "event_type": "view",     "page": "/home",     "duration_ms": 95}
{"index": {"_id": "12"}}
{"session_id": "s-600", "@timestamp": "2026-10-30T15:00:00Z", "seq": 1, "event_type": "view",     "page": "/pricing",  "duration_ms": 70}
{"index": {"_id": "13"}}
{"session_id": "s-600", "@timestamp": "2026-10-30T15:00:12Z", "seq": 2, "event_type": "click",    "page": "/checkout", "duration_ms": 35}
{"index": {"_id": "14"}}
{"session_id": "s-600", "@timestamp": "2026-10-30T15:00:50Z", "seq": 3, "event_type": "purchase", "page": "/checkout", "duration_ms": 1200}
'
}

load_sessions() {
curl -u elastic:password -X PUT "localhost:9200/sessions/_bulk?refresh&pretty" -H 'Content-Type: application/json' -d'
{"index": {"_id": "s-100"}}
{"session_id": "s-100", "started_at": "2026-10-30T10:00:00Z", "user_id": "u-1", "device": "desktop", "country": "DE", "referrer": "google",  "plan": "pro"}
{"index": {"_id": "s-200"}}
{"session_id": "s-200", "started_at": "2026-10-30T11:00:00Z", "user_id": "u-2", "device": "mobile",  "country": "US", "referrer": "direct",  "plan": "free"}
{"index": {"_id": "s-300"}}
{"session_id": "s-300", "started_at": "2026-10-30T12:00:00Z", "user_id": "u-3", "device": "mobile",  "country": "DE", "referrer": "twitter", "plan": "free"}
{"index": {"_id": "s-400"}}
{"session_id": "s-400", "started_at": "2026-10-30T13:00:00Z", "user_id": "u-1", "device": "desktop", "country": "DE", "referrer": "google",  "plan": "pro"}
{"index": {"_id": "s-600"}}
{"session_id": "s-600", "started_at": "2026-10-30T15:00:00Z", "user_id": "u-2", "device": "tablet",  "country": "FR", "referrer": "direct",  "plan": "pro"}
{"index": {"_id": "s-700"}}
{"session_id": "s-700", "started_at": "2026-10-30T16:00:00Z", "user_id": "u-5", "device": "desktop", "country": "US", "referrer": "google",  "plan": "free"}
'
}

# -----------------------------------------------------------------------------
# which node holds each shard copy: the node column must match per shard number
# -----------------------------------------------------------------------------
shards() {
curl -u elastic:password -X GET "localhost:9200/_cat/nodes?v&h=name,http,node.role,master"
curl -u elastic:password -X GET "localhost:9200/_cat/shards/events,sessions?v&s=shard,index&h=index,shard,prirep,state,docs,node"
}

# -----------------------------------------------------------------------------
# which shard a session_id maps to: same shard number and node in both indices
# -----------------------------------------------------------------------------
routing() {
curl -u elastic:password -X GET "localhost:9200/events/_search_shards?routing=s-100&pretty&filter_path=shards.shard,shards.index,shards.node"
curl -u elastic:password -X GET "localhost:9200/sessions/_search_shards?routing=s-100&pretty&filter_path=shards.shard,shards.index,shards.node"
curl -u elastic:password -X GET "localhost:9200/events/_search_shards?routing=s-300&pretty&filter_path=shards.shard,shards.index,shards.node"
curl -u elastic:password -X GET "localhost:9200/sessions/_search_shards?routing=s-300&pretty&filter_path=shards.shard,shards.index,shards.node"
}

# -----------------------------------------------------------------------------
# co-location invariant: for each shard number, the session_id set in events
# and the one in sessions must be subsets of the same partition, and a session
# must not appear in two shard numbers.
# -----------------------------------------------------------------------------
colocation() {
for shard in 0 1 2; do
curl -u elastic:password -X POST "localhost:9200/events,sessions/_search?preference=_shards:$shard&pretty&filter_path=aggregations" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "by_index": {
      "terms": { "field": "_index" },
      "aggs": { "sessions": { "terms": { "field": "session_id", "size": 1000 } } }
    }
  }
}
'
done
}

# -----------------------------------------------------------------------------
# co-located join: `FROM events | JOIN sessions ON session_id` (snapshot only).
# Each data node joins its local events shard against the sessions shard with the
# same number; only results (or partial aggregates) go back to the coordinator.
# -----------------------------------------------------------------------------
join() {
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id | KEEP session_id, seq, event_type, country, plan | SORT session_id, seq"
}
'
}

# filters after the join: the right-side conjunct is pushed into the lookup query, the left one into the events scan
join_where() {
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id | WHERE country == \"DE\" AND seq > 1 | KEEP session_id, seq, event_type, country, plan | SORT session_id, seq"
}
'
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | WHERE event_type == \"purchase\" | JOIN sessions ON session_id | KEEP session_id, seq, country, plan | SORT session_id"
}
'
}

# aggregation after the join: partial aggregates per data node, final on the coordinator
join_stats() {
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id | STATS n = COUNT(*), spent = SUM(duration_ms) BY country, plan | SORT country, plan"
}
'

curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id | STATS n = COUNT(*), spent = SUM(duration_ms) BY session_id | SORT session_id"
}
'
}

# where it ran: one data driver per node, each with LookupOperator[index=sessions ... colocated=true]
join_profile() {
curl -u elastic:password -X POST "localhost:9200/_query?format=json&filter_path=profile.drivers.node_name,profile.drivers.description,profile.drivers.operators.operator&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id | STATS n = COUNT(*) BY country",
  "profile": true
}
'
}

# same answers through IN subqueries (coordinator-side semi / anti join), for cross-checking
join_reference() {
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | WHERE session_id IN (FROM sessions | WHERE country == \"DE\" | KEEP session_id) | STATS events = COUNT(*), purchases = COUNT(event_type == \"purchase\" OR NULL) BY session_id | SORT session_id"
}
'
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM sessions | WHERE session_id NOT IN (FROM events | WHERE event_type == \"purchase\" | KEEP session_id) | KEEP session_id, user_id, plan | SORT session_id"
}
'
}

# -----------------------------------------------------------------------------
# rejected queries: different shard count, unknown key, wildcard, two keys.
# NOT rejected (known POC gap): a key that is not the routing field, e.g.
#   FROM sessions | JOIN sessions ON user_id   -- runs, matches only within a shard
# -----------------------------------------------------------------------------
join_rejected() {
curl -u elastic:password -X PUT "localhost:9200/sessions_one" -H 'Content-Type: application/json' -d'
{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
  "mappings": { "properties": { "session_id": { "type": "keyword" }, "plan": { "type": "keyword" } } }
}
'
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions_one ON session_id"
}
'
curl -u elastic:password -X DELETE "localhost:9200/sessions_one"
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON device"
}
'
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sess* ON session_id"
}
'
curl -u elastic:password -X POST "localhost:9200/_query?format=txt&pretty" -H 'Content-Type: application/json' -d'
{
  "query": "FROM events | JOIN sessions ON session_id, seq"
}
'
}

main "@"
