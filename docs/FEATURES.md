# Supported Features and Commands

## 1. Six ways a command can be handled

| Handling | Description |
|---|---|
| **Single-key routing** | Compute `CRC16(key)` to find the slot, forward to that node, streaming the raw bytes through unparsed |
| **Multi-key (same-slot check)** | Extract every key in the command; if they don't all hash to the same slot, return `-CROSSSLOT` immediately, otherwise forward like a single-key command |
| **Multi-key fan-out** | Split the command per key into several sub-commands sent to their respective nodes, then merge the replies |
| **No-key broadcast** | Broadcast to every primary node and merge the replies |
| **Single-node forward** | Pick one master node, forward the command, and relay its reply verbatim (no broadcast, no merging) |
| **Local reply** | The proxy builds the reply itself, with no backend request at all |

## 2. Fan-out command details

These commands are split per key into several sub-commands sent to each key's owning node, then
merged into one reply (`FANOUT_CMDS` in `src/fanout.c`):

| Command | Merge strategy |
|---|---|
| `MGET` `JSON.MGET` | Results reassembled in original key order |
| `MSET` `JSON.MSET` | Broadcast write; returns `+OK` only if every node succeeds |
| `MSETEX key seconds value [...]` | **A proxy-specific extension** (not a native Valkey command); each triplet is translated into its own `SETEX` and dispatched individually |
| `DEL` `EXISTS` `TOUCH` `UNLINK` | Integer results summed across nodes |
| `LCS` | Dispatched per key pair; the proxy recomputes the LCS result itself (not a simple forward) |

> `MSETNX` is **not** a fan-out command — it's a multi-key, same-slot command (all keys must
> share a slot, otherwise `-CROSSSLOT`).

## 3. No-key broadcast / single-node forward command details

### 3.1 Local direct reply (no backend request)
`PING` (replies `+PONG` with no argument, echoes a bulk string with one), `ECHO`, `TIME`, `CLIENT
ID/GETNAME/SETNAME/INFO/HELP`, `SCRIPT HELP`, `FUNCTION HELP`, and the help-text portion of
`ACL HELP`.

### 3.2 Single-node forward (pick one master, relay its reply verbatim)
`RANDOMKEY` `LOLWUT` `FT._LIST` `FT.INFO` `FT.CREATE` `FT.DROPINDEX` `FT.SEARCH` `FT.AGGREGATE` `OBJECT HELP` `XGROUP HELP` `XINFO HELP`
`SCRIPT EXISTS/SHOW` `FUNCTION LIST` `MODULE LIST` `COMMAND HELP/GETKEYS/GETKEYSANDFLAGS`
`SLOWLOG HELP` `COMMANDLOG HELP` `PUBSUB HELP` `ACL LIST/USERS/WHOAMI/GETUSER/DRYRUN` (uses the
shared connection while unauthenticated / authenticated as the default user; once a client has
`AUTH`'d as a non-default user, it switches to that client's own dedicated authenticated
connection — see §8)

`COMMAND` (the bare command is equivalent to `COMMAND INFO` with no arguments); the reply to
`COMMAND INFO/DOCS/LIST` is forwarded and then **filtered** down to only the subset of commands
this proxy itself supports before being relayed to the client; `COMMAND COUNT` forwards
`COMMAND LIST` and counts the filtered result; `COMMAND GETKEYS/GETKEYSANDFLAGS` first checks
whether the target command is on the supported list and rejects it outright (without forwarding
anything) if it isn't.

### 3.3 Broadcast to all masters, results merged
`DBSIZE` (summed), `KEYS` (concatenated), `SCAN` (each node's cursor is encoded into the returned
cursor value; scanning proceeds per node and results are reassembled), `FLUSHALL`/`FLUSHDB`
(`+OK` only if every node succeeds), `PUBSUB CHANNELS`/`SHARDCHANNELS` (deduplicated and
merged), `PUBSUB NUMSUB`/`SHARDNUMSUB` (per-channel counts summed), `PUBSUB NUMPAT` (summed),
`SLOWLOG GET [count]` (each node's entries merged newest first by timestamp and truncated to
`count` — default 10, `-1` = all; entry ids are per-node and so not unique across the merged
reply), `SLOWLOG LEN` (summed), `SLOWLOG RESET` (`+OK` only if every node succeeds), `COMMANDLOG GET <count> <type>`
(merged newest first and truncated to `count`, same as `SLOWLOG GET`; `-1` = all), `COMMANDLOG LEN`
(summed), `COMMANDLOG RESET` (`+OK` only if every node succeeds), `SCRIPT KILL` (succeeds if
**any** node succeeds), `SCRIPT FLUSH [ASYNC|SYNC]` (**the argument is ignored** — always
rewritten as `SCRIPT FLUSH ASYNC` and broadcast, always replies `+OK`), `SCRIPT LOAD` (broadcast
to every node so any shard's subsequent `EVALSHA` can find it; only one sha1 is relayed back),
`FUNCTION KILL` (succeeds if **any** node succeeds), `FUNCTION FLUSH [ASYNC|SYNC]` (same
treatment as `SCRIPT FLUSH` — always `ASYNC`, always `+OK`), `FUNCTION DELETE` (succeeds only if
every node succeeds), `FUNCTION LOAD` (succeeds only if every node succeeds), `FUNCTION STATS`
(each node's execution state is reported separately, not merged).

### 3.4 INFO and PROXY introspection

`INFO` describes the proxy as one standalone server instead of relaying a single shard's reply.
Each section comes from one of three sources:

| Section | Source |
|---|---|
| `server` | Proxy. `redis_version`/`valkey_version` are the lowest seen across the shards (probed at startup, after each topology change, and every 60s with periodic refresh on; with refresh off, only at startup. A master that doesn't answer a probe is left out until the next one); `server_mode:standalone`; uptime, `process_id`, `run_id`, `tcp_port`, `executable`, `config_file` and listeners are the proxy's own; adds `proxy_version` |
| `tls` | Proxy. Serial and seconds to expiry of the proxy's server certificate; the client and CA fields stay `none`/`0` |
| `clients` | Proxy. `connected_clients` across all workers, `maxclients` = `max_connection` |
| `replication` | Proxy. `role` is the one `HELLO` reports, `connected_slaves:0`. The backends' own replication is in `PROXY INFO replication` |
| `cluster` | Proxy. Always `cluster_enabled:0` |
| `keyspace` | Every master, summed per db (`avg_ttl` weighted by `expires`), so it agrees with `DBSIZE` |
| `memory` `persistence` `stats` `commandstats` `errorstats` `latencystats` | Every master, listed per shard without merging |
| `proxy` | Proxy. Workers, topology version, shard count, unmapped slots, and how many shards answered this reply (`info_shards_queried`/`_ok`/`_failed`) |
| `pools` | Proxy. Same as `PROXY POOLS` |

In a per-shard section each shard starts with `shard<idx>:addr=<ip>:<port>,node_id=<id>,status=ok|error`
and its own lines follow prefixed with `shard<idx>_` (e.g. `shard0_used_memory:...`), so the reply
stays a flat `key:value` list. `<idx>` is the shard's index in the proxy's topology, the same one
`PROXY POOLS` and `PROXY INFO ... SHARD` use.

`INFO` / `INFO default`: server, clients, cluster, keyspace, proxy. `INFO all`: adds tls, memory,
persistence, stats, replication, commandstats, errorstats, latencystats. `INFO everything`: adds pools. Section
names are case-insensitive and can be combined (`INFO server keyspace`); output follows the order
above. Unknown names are ignored, and a reply with no known section is empty, as in Valkey. Requests
that only name proxy-answered sections never contact a backend. A RESP3 client gets a verbatim
string, as from Valkey.

`HELLO` reports `server` `valkey-proxy` and `mode` `standalone`. `version`, `role` and `modules`
follow the backends: the lowest version across the masters (the same one `INFO server` shows),
`master`, and the modules loaded on every master. Each one can be overridden in the config with
`reported_version`, `reported_role` and `reported_modules`. `reported_version` also applies to
`INFO server`, and `reported_role` to `INFO replication`. On Valkey, `reported_version` changes
`valkey_version`; `redis_version` stays as the backends report it (7.2.4 on Valkey), as on a direct
connection.

`HELLO` says `server` `valkey-proxy`, while `INFO server` keeps `server_name:valkey`. Clients and
tools read `server_name` to tell Valkey from Redis, so it stays as on a direct connection: present
for a Valkey backend, left out for a Redis one.
`proxy_version` in `INFO server` shows that the server is the proxy.

`PROXY INFO [section ...] [SHARD <idx>|<ip:port>]` sends `INFO [section ...]` to every master (or
just the given one) and returns each reply unmerged: an array of label/reply pairs (a map for a
RESP3 client), labelled `<idx>:<ip>:<port>`. A shard that can't be reached gets an error entry.
`SHARD` can come before, between or after the sections.

`PROXY POOLS` returns a bulk string in INFO's `key:value` format: a `# Pools` header, `workers:<n>`,
then one `pool_db<db>_n<node>_<type>` line per backend node and pool type (`normal`, `blocking`,
`subscription`, `transaction`, `acl`), summed across worker threads. Each line carries the node's
`addr`, `gen` and `has_slots` (from the answering worker's topology snapshot), `max` (summed over the
workers that have this pool), the gauges `open`, `in_use` and `waiting`, and the counters
`acquires` (every connection handed out, including after a wait), `waits`, `handoffs`,
`wait_us_sum`, `wait_us_max`, `wait_abandoned`, `wait_failed`, `connects`, `connect_failures`,
`reconnects` and `destroys`. Counters are never reset; they keep counting when a node index is
reassigned after a failover, which `gen` makes visible.

## 4. Blocking commands

Blocking commands use a separate `POOL_BLOCKING` connection pool (long-lived, kept in blocking
mode) so they don't stall the non-blocking event loop.

| Command | Notes |
|---|---|
| `BLPOP` `BRPOP` | Block until an element is available in any of the listed lists |
| `BLMOVE` `BRPOPLPUSH` | Block until a source list is non-empty, then pop and move one element |
| `BLMPOP` | Block until any of the listed lists is non-empty, then pop one or more elements |
| `BZPOPMIN` `BZPOPMAX` | Block until a member is available in any of the listed sorted sets |
| `BZMPOP` | Block until any of the listed sorted sets is non-empty, then pop one or more members |
| `XREAD ... BLOCK ...` | Block until new entries arrive on the listed streams (only uses the blocking pool when `BLOCK` is present, otherwise takes the ordinary multi-key path) |
| `XREADGROUP ... BLOCK ...` | Same, for a consumer-group read |

## 5. Pub/Sub

`(P|S)SUBSCRIBE` pins the client connection to a dedicated `POOL_SUBSCRIPTION` backend connection
until it unsubscribes; subsequent subscribe/unsubscribe calls on the same client connection reuse
that same pinned connection. `PUBLISH`/`SPUBLISH` need no dedicated connection — they're routed by
channel like an ordinary single-key command.

## 6. Key configuration knobs (`proxy.conf`) at a glance

See the comments in `proxy.conf` at the repo root for the full picture; only the settings that
directly affect command behavior are listed here:

| Setting | Default | Effect |
|---|---|---|
| `min_normal_connection` / `max_normal_connection` | 2 / 20 | Pool used by the vast majority of read/write commands |
| `min_blocking_connection` / `max_blocking_connection` | 2 / 10 | `BLPOP` and other blocking commands |
| `min_subscription_connection` / `max_subscription_connection` | 0 / 10 | Pub/Sub |
| `min_transaction_connection` / `max_transaction_connection` | 0 / 10 | Capacity settings exist for the transaction pool |
| `min_acl_connection` / `max_acl_connection` | 0 / 10 | `ACL`/`AUTH`, plus each authenticated client's dedicated per-node connections |
| `cluster_refresh_interval_ms` | 0 (disabled) | Whether/how often the cluster topology is refreshed in the background |
| `max_connection` | 4000 | Per-worker-thread fd budget (client sockets + backend connections + listener fd(s)) |
| `backend_multiplexing` | yes | Single-key and fan-out commands share a fixed set of connections per node and run concurrently within a pipeline (see §7) |
| `shared_connection_per_node` | 4 | Size of that shared pool, per node (per db, per worker thread); fixed, a restart is needed to change it |
| `max_pipeline_inflight` | 256 | Most commands one client may have in flight before the proxy stops reading from it |

## 7. Pipelining and backend multiplexing

With `backend_multiplexing yes` (the default):

- **Which commands are multiplexed**: single-key commands, and the fan-out commands in §2 (`MGET`, `MSET`, `DEL`, `EXISTS`, ...), whenever the whole command fits in 16KB. Instead of checking a connection out of the normal pool for one request, they are written to one of `shared_connection_per_node` connections to the target node, many requests at a time. Replies come back in write order on each connection.
- **Pipelines run concurrently**: a client's pipelined commands are all sent at once, across nodes, rather than one after the other. Replies are still returned to the client in command order.
- **Per-key order**: the shared connection is chosen by `slot % shared_connection_per_node`, so every command on one key, from any client, goes down the same connection and runs in the order the proxy received it.
- **Everything else is a barrier**: any other command in a pipeline (keyless commands, blocking commands, `SUBSCRIBE`, `AUTH`/`ACL`/`HELLO`/`SELECT`, `EVAL`, commands larger than 16KB, ...) waits until every reply before it has been written to the client, then runs exactly as with multiplexing off. The same applies to clients in a transaction or subscribed, and clients that `AUTH`ed as a non-default user.
- **Backpressure**: the proxy stops reading a client's commands while it has `max_pipeline_inflight` in flight, or more than 4MB of replies waiting to be written to it.
- **Failures**: if a shared connection breaks, or its node is replaced in a failover, every request in flight on it gets an `-ERR` reply (the client stays connected) and the connection is reopened on next use.
- **RESP3**: once a client has switched to RESP3 with `HELLO 3`, shared connections switch too (a `HELLO 3` queued ahead of the next request on each); RESP2 clients' replies are converted back as before.

`PROXY POOLS` reports the shared pool as `pool_db<N>_n<N>_shared`, where `open` is the number of connected shared connections and `in_use` the number of requests currently in flight on them.

With `backend_multiplexing no`, every command is handled one at a time per client on a connection checked out of the normal pool, as before.
