Valkey Cluster shards data across multiple nodes by hash slot. That's great for scale, but it pushes real complexity onto every client.

## The Problem

The client must know (and keep up to date) the slot → node mapping.
Every reply can come back with a MOVED/ASK redirect that the client must handle, or the operation fails silently.
Multi-key operations (MGET, DEL, ...) either fail with CROSSSLOT, or the client has to manually fan them out per-shard and merge the results.
Every client process ends up opening and managing its own pool of connections per shard — multiplied across every application instance.
Not every language/framework has a mature, well-maintained cluster client, and even when one exists, it's another dependency and another thing to keep in sync with the actual cluster topology. A transparent proxy lets applications keep using an ordinary, single-node Valkey client while getting cluster-mode data placement for free.

Valkey-proxy is a multi-threaded, non-blocking standalone application, which is written by C, that sits between
application clients and a Valkey (or Redis) Cluster deployment. It speaks the plain RESP protocol clients already use, transparently figures out which cluster shard owns a given key, and forwards the request — so applications can talk to the proxy exactly as if it were a single alkey server, without embedding a cluster-aware client library or handling MOVED/ASK redirects, slot maps, or per-shard connection pools themselves.

This proposal summarizes what the proxy already does today, and what's on the roadmap next.
We're sharing it publicly because we believe this fills a real gap for teams who want cluster-mode Valkey without rewriting their client code or vendoring a heavyweight client-side cluster implementation.

## This is a stub repo.

This repo will containt the work-in-progress that was previously held in a private repo. 
Here is the _estimated_ timeline:

- **Mid Oct 2026**: Prep repo, migrate commits from private repo to this public repo under BSD-3-Clause license.  
- **Nov-Dec 2026**: Public feedback, testing, documentation
- **Dec 2026-Jan 2027**: Automation, release candidates, general avaliability.

## Why This Matters

No cluster-aware client needed. Any Redis/Valkey client library — even a bare-bones single-node one — works unmodified against the proxy.

Centralizes cluster complexity. Slot routing, MOVED handling (at the topology level), multi-key fan-out, and CROSSSLOT enforcement live in one place, operated and upgraded independently of application code.

Fewer backend connections. Application instances multiplex through the proxy's pooled connections instead of each opening its own connection per shard, which matters at scale (many short-lived app processes, serverless, etc.).

Operational continuity across resharding/failover. The background topology refresh means a cluster resize or failover doesn't require every application to reconnect or be redeployed.

Small, auditable C codebase. No heavyweight runtime or dependency tree — straightforward to build, deploy, and reason about in security-sensitive environments.

## What Valkey-proxy Does Today

Single endpoint. Applications connect to valkey-proxy over TCP (or a Unix domain socket) exactly like a single Valkey instance, with optional TLS between client and proxy.

Automatic slot-aware routing. Single-key commands are routed by CRC16(key) to the correct backend node — no client-side cluster logic required.

Multi-key command fan-out. MGET, MSET, MSETEX, DEL, UNLINK, EXISTS, TOUCH, LCS, and even some module commands JSON.MGET, JSON.MSET are automatically split per shard, dispatched in parallel, and the replies are reassembled into a single, correctly-ordered response — the kind of logic every hand-rolled cluster client has to reinvent.

No-key command broadcast. Cluster-wide commands such as DBSIZE, KEYS, SCAN (with cursor reassembly), FUNCTION STATS, and the FT.* search-index family are broadcast to every primary node and merged into one reply.

Safe handling of same-slot-only commands. Commands like MSETNX, RENAME, and set/zset store-ops are rejected with a clear CROSSSLOT error when their keys don't share a slot, rather than silently producing wrong results.

Dynamic topology tracking. A configurable thread periodically re-discovers cluster topology (CLUSTER NODES), detects resharding and failovers, and republishes the new topology to every worker thread without dropping in-flight connections or requiring a proxy restart. A short grace period rides out transient blips before evicting a node.

Multi-threaded, connection-pooled architecture. Each worker thread runs its own event loop and maintains independent, per-command-class connection pools (normal traffic, blocking commands, pub/sub, transactions, ACL) to each backend node — so one slow blocking command (BLPOP, WAIT, ...) can't starve ordinary traffic, and connection counts to the backend cluster stay bounded and configurable instead of growing with client count.

ACL-aware routing. A client that authenticates as a non-default ACL user gets its own dedicated, already-authenticated backend connection per node for its business traffic, so per-user permissions are enforced correctly without every request re-authenticating

RESP3 support.  HELLO-based RESP3 protocol negotiation is supported end-to-end.

Simple, single-file configuration. One proxy.conf controls seed node, bind address, worker thread count, per-pool-type connection limits, TLS, Unix socket, and topology-refresh tuning — no external coordination service required.

## Current Limitations and Roadmap

Multi-database support — allow SELECT to a non-zero database end-to-end, not just at the proxy's own backend-connection config level.

Limited support for MULTI/EXEC/DISCARD/WATCH End-to-end transaction commands.

Not implemented: CLUSTER, CONFIG, DEBUG etc commands related to administration and configuration.

Read/write splitting architecture : By enabling read-write separation feature in the Proxy, we can let the primary node focus on processing write operations and let the replica handle read operations. It is very suitable for scenarios with relatively large read operations and can reduce the pressure on the primary.

Audit Log: Record the operations of clients accessing Proxy and provide storage, query, and analysis functions. With the audit log function, Proxy will automatically log the read and write requests through the proxy and standardize various information such as system security events, user access records, system operation logs, and system operation status in the information system.

