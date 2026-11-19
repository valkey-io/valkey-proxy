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
| `server` | Proxy. `redis_version`/`valkey_version` are the lowest seen across the shards (probed at startup and after each topology change, so an in-place upgrade that leaves the topology unchanged shows up only after the next change or a restart. A master that doesn't answer a probe is left out until the next one); `server_mode:standalone`; uptime, `process_id`, `run_id`, `tcp_port`, `executable`, `config_file` and listeners are the proxy's own; adds `proxy_version` |
| `tls` | Proxy. Serial and seconds to expiry of the proxy's server certificate; the client and CA fields stay `none`/`0` |
| `clients` | Proxy. `connected_clients` across all workers, `maxclients` = `max-connection` |
| `replication` | Proxy. `role` is the one `HELLO` reports, `connected_slaves:0`. The backends' own replication is in `PROXY INFO replication` |
| `cluster` | Proxy. Always `cluster_enabled:0` |
| `keyspace` | Every master, summed per db (`avg_ttl` weighted by `expires`), so it agrees with `DBSIZE` |
| `memory` `persistence` `stats` `commandstats` `errorstats` `latencystats` | Every master, listed per shard without merging |
| `proxy` | Proxy. Workers, topology version, shard count, unmapped slots, and how many shards answered this reply (`info_shards_queried`/`_ok`/`_failed`), and one `worker<i>:connected_clients=<n>,peak_connected_clients=<n>` line per worker (current clients and the most it has held since startup). The main thread accepts every client and hands it to the worker with the fewest, so these stay within one of each other unless clients disconnect unevenly |
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
`reported-version`, `reported-role` and `reported-modules`. `reported-version` also applies to
`INFO server`, and `reported-role` to `INFO replication`. On Valkey, `reported-version` changes
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

`PROXY CONFIG GET <pattern> [<pattern> ...]` returns the proxy's own `proxy.conf` settings whose
names match any of the glob patterns (case-insensitive, `*` `?` `\`), as name/value pairs (a map
for a RESP3 client); `tls-key` is never shown. `PROXY CONFIG SET <parameter> <value> [<parameter>
<value> ...]` changes them at runtime, in memory only until saved. Every pair is
checked before anything changes, so one bad pair leaves all of them as they were. Errors use
Valkey's `CONFIG SET` wording (`can't set immutable config`, `Unknown option ...`). Settable:

| Setting | Accepts | Takes effect |
|---|---|---|
| `max-pipeline-inflight`, `max-multikey-payload`, `backend-connect-timeout-ms` | ≥ 1 | On the next command / connect |
| `transaction-replay-buffer-size`, `cluster-node-fail-grace-ms` | ≥ 0 | On the next transaction / refresh cycle |
| `cluster-refresh-interval-ms` | ≥ 0 | From the next cycle; 0 pauses refresh, and a positive value starts it if it was off at startup |
| `min-<pool>-connection` / `max-<pool>-connection` (`normal`, `blocking`, `subscription`, `transaction`, `acl`) | min ≥ 0, 1 ≤ max ≤ `max-connection`, min ≤ max | Each worker resizes its own pools as soon as it wakes: raising `min` opens connections up to it, raising `max` hands the new room to clients waiting for a connection, lowering `max` closes idle connections past it at once and busy ones as they're released |

The rest (listen address, `worker-thread-no`, `max-connection`, `db`, `backend-multiplexing`,
`shared-connection-per-node`, TLS, `reported-*`, the `proxy-config-*-users` lists) is fixed at
startup. `PROXY CONFIG HELP` prints
the syntax and the settable parameters with the values each accepts.

`PROXY CONFIG SAVE [<file>]` writes the config file the proxy was started with again, with the
settable parameters' current values: over the startup file itself, or to `<file>` (relative to the
proxy's working directory). The startup file is the template either way, so the result is a whole
config the proxy can start from: every other line, comments included, is kept, and a settable
parameter keeps its line and only gets its value replaced. A settable parameter the file doesn't
mention is appended under `# Added by PROXY CONFIG SAVE`, but only if its value changed since
startup. The file is written to a temporary file beside it and renamed over it, keeping an existing
file's permissions, so a failed save leaves the old file intact.

Who may run `PROXY CONFIG` is set in `proxy.conf`, not by the users' ACL rules on the backends:

```
proxy-config-admin-users     ops,lucas    # GET, SET, SAVE and HELP
proxy-config-readonly-users  monitor      # GET and HELP only
```

A client must first `AUTH <user> <password>` (or `HELLO ... AUTH`) as an ACL user that exists on
the backends, which check the password; the proxy then lets it in by name. Names are
comma-separated without spaces, and either line may be repeated to extend its list. Neither set
means nobody may run `PROXY CONFIG`. The default identity -- a client that never sent `AUTH`, or
went back to it with `AUTH default` -- is always refused with `-NOPERM the default user can't run
the 'proxy|config|<sub>' command`; a user in neither list (or a read-only one running `SET` or
`SAVE`) gets `-NOPERM User <name> has no permissions to run the 'proxy|config|<sub>' command (not
in ...)`. Both lists are fixed at startup: `GET` shows them, `SET` refuses them. `PROXY HELP`
itself isn't restricted.

`CLIENT LIST` is in Valkey's `@admin` category, so the proxy checks it as Valkey would. For a client
logged in as a non-default user, the proxy asks Valkey: it sends a `CLIENT LIST ID` that matches no
client on the user's own connection. Any error refuses the command, and the client gets that error.
A `-NOPERM` is kept per user on each worker for 5 seconds, as is an answer that allows it. Other
errors (e.g. `-BUSY`) aren't kept. If Valkey can't be asked (the connection was closed, the ACL pool
for that node is full, or logging in to it fails), the command gets an error such as
`-ERR backend connection closed`. Clients using the default identity aren't checked. `INFO` isn't
an `@admin` command, and `PROXY POOLS` and `PROXY INFO` have no Valkey equivalent, so none of them
is checked.

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
| `min-normal-connection` / `max-normal-connection` | 2 / 20 | Pool used by the vast majority of read/write commands |
| `min-blocking-connection` / `max-blocking-connection` | 2 / 10 | `BLPOP` and other blocking commands |
| `min-subscription-connection` / `max-subscription-connection` | 0 / 10 | Pub/Sub |
| `min-transaction-connection` / `max-transaction-connection` | 0 / 10 | Connections pinned by transactions (see §8); when all are in use, further transactions wait for one |
| `transaction-replay-buffer-size` | 1048576 | Most bytes of queued commands kept per transaction for replaying it on the new node after `-MOVED` (see §8); 0 disables replay |
| `min-acl-connection` / `max-acl-connection` | 0 / 10 | `ACL`/`AUTH`, plus each authenticated client's dedicated per-node connections |
| `cluster-refresh-interval-ms` | 500 | How often (ms) the cluster topology is refreshed in the background; 0 disables it |
| `proxy-config-admin-users` / `proxy-config-readonly-users` | unset | ACL users (after `AUTH`) who may run every `PROXY CONFIG` subcommand / only `GET` and `HELP`; unset means nobody (see §3.4) |
| `max-connection` | 4000 | Per-worker-thread fd budget (client sockets + backend connections + listener fd(s)) |
| `max-multikey-payload` | 67108864 (64MB) | Largest command held in full before forwarding (multi-key fan-out such as `MGET`/`MSET`/`DEL`, blocking commands, `AUTH`); a larger one gets `-ERR command too large` |
| `backend-multiplexing` | yes | Single-key and fan-out commands share a fixed set of connections per node and run concurrently within a pipeline (see §7) |
| `shared-connection-per-node` | 4 | Size of that shared pool, per node (per db, per worker thread); fixed, a restart is needed to change it |
| `max-pipeline-inflight` | 256 | Most commands one client may have in flight before the proxy stops reading from it |

## 7. Pipelining and backend multiplexing

With `backend-multiplexing yes` (the default):

- **Which commands are multiplexed**: single-key commands, and the fan-out commands in §2 (`MGET`, `MSET`, `DEL`, `EXISTS`, ...), whenever the whole command fits in 16KB. Instead of checking a connection out of the normal pool for one request, they are written to one of `shared-connection-per-node` connections to the target node, many requests at a time. Replies come back in write order on each connection.
- **Pipelines run concurrently**: a client's pipelined commands are all sent at once, across nodes, rather than one after the other. Replies are still returned to the client in command order.
- **Per-key order**: the shared connection is chosen by `slot % shared-connection-per-node`, so every command on one key, from any client, goes down the same connection and runs in the order the proxy received it.
- **Everything else is a barrier**: any other command in a pipeline (keyless commands, blocking commands, `SUBSCRIBE`, `AUTH`/`ACL`/`HELLO`/`SELECT`, `EVAL`, commands larger than 16KB, ...) waits until every reply before it has been written to the client, then runs exactly as with multiplexing off. The same applies to clients in a transaction or subscribed, and clients that `AUTH`ed as a non-default user.
- **Backpressure**: the proxy stops reading a client's commands while it has `max-pipeline-inflight` in flight, or more than 4MB of replies waiting to be written to it.
- **Failures**: if a shared connection breaks, or its node is replaced in a failover, every request in flight on it gets an `-ERR` reply (the client stays connected) and the connection is reopened on next use.
- **RESP3**: once a client has switched to RESP3 with `HELLO 3`, shared connections switch too (a `HELLO 3` queued ahead of the next request on each); RESP2 clients' replies are converted back as before.

`PROXY POOLS` reports the shared pool as `pool_db<N>_n<N>_shared`, where `open` is the number of connected shared connections and `in_use` the number of requests currently in flight on them.

With `backend-multiplexing no`, every command is handled one at a time per client on a connection checked out of the normal pool, as before.

## 8. Transactions

`MULTI` / `EXEC` / `DISCARD` / `WATCH` / `UNWATCH` work as on a Valkey cluster: a transaction runs
on one backend connection, pinned to the client from its first `WATCH` or first queued command until
`EXEC`, `DISCARD` or `UNWATCH`.

- **One slot**: every key in the transaction, including `WATCH`ed keys, must hash to the same slot.
  The first key used picks the slot and its node. A command with a key in another slot gets
  `-CROSSSLOT`.
- **What can be queued**: commands with keys (single-key, same-slot multi-key such as `MGET`/`MSET`/`DEL`,
  blocking commands, `EVAL`/`FCALL` with keys). Once the node is known, `PING`, `ECHO`, `TIME`,
  `UNWATCH` and `EVAL`/`FCALL` with no keys can be queued too. Anything else (`SCAN`, `KEYS`, `INFO`,
  `CLIENT`, `SELECT`, `HELLO`, `AUTH`, `ACL`, Pub/Sub, ...) gets an error.
- **Rejected commands**: a command rejected inside `MULTI`, by the proxy or by the backend, fails the
  transaction, and `EXEC` returns `-EXECABORT Transaction discarded because of previous errors.`.
  Errors raised while `EXEC` runs (for example `-WRONGTYPE`) come back as elements of `EXEC`'s array,
  as on Valkey.
- **`MULTI` is sent late**: the proxy answers `MULTI` itself and sends it to the backend just before
  the first queued command (or `EXEC`), as a separate request.
- **Lost connection**: if the pinned connection breaks, a `WATCH` on it is lost and `EXEC` returns nil
  (the client retries, as after any `WATCH` abort); a `MULTI` on it fails and `EXEC` returns
  `-EXECABORT`.
- **Slot moved (`-MOVED`)**: commands keep going to the node picked at the start. If the slot has
  moved since, the backend answers `-MOVED`, having run nothing. A transaction without `WATCH` is
  then replayed on the new node from the queued commands, and the client sees the normal replies.
  Replay is skipped, and the `-MOVED` returned, when the transaction has more than
  `transaction-replay-buffer-size` bytes of queued commands, the new node isn't in the proxy's
  topology yet, or it has already been redirected twice. With `WATCH`, `EXEC` returns nil instead.
  `-ASK` and `-TRYAGAIN` (a slot migration in progress) are returned as-is.
- **RESP3**: a RESP2 client gets each element of `EXEC`'s reply converted like the reply to the
  command queued in its position.
- **ACL users** run their transactions on their own per-node connections instead of the transaction
  pool.
- `CLIENT INFO` / `CLIENT LIST` show `flags=x` and `multi=<queued count>` inside `MULTI`, and
  `watch=<key count>` after `WATCH`.

Known limitations:

- A client that `WATCH`es and never runs `EXEC`/`DISCARD`/`UNWATCH` keeps a transaction-pool
  connection until it disconnects.
- For a multi-key command with more than 512 keys inside `MULTI`, the proxy checks only the first
  512 keys' slots; a later key in another slot is caught by the backend's `-CROSSSLOT`.
