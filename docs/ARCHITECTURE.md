# Architecture

valkey-proxy sits between your clients and a Valkey Cluster. To a client, the whole cluster looks
like one ordinary Valkey server: it connects to the proxy, sends commands, and never has to know
which node holds which key.

This document explains how the code under `src/` is organized and what happens to a command from the
moment it arrives until its reply goes back. For the list of supported commands, see
[FEATURES.md](FEATURES.md) and [COMMANDS.md](COMMANDS.md).

## The big picture

![valkey-proxy architecture: clients reach a worker thread through the main thread; the worker routes each command by slot over the multiplexed shared pool or an exclusive pool to the primary that owns it; a refresh thread polls CLUSTER NODES](architecture.svg)

The thick line is multiplexed traffic (many requests per connection), thin lines are exclusive
traffic (one request per connection), and dashed lines are control traffic. The three-shard slot
layout is only an example.

In short:

1. The **main thread** answers new connections and passes each one to a worker.
2. A **worker thread** does all the real work for its clients: it reads their commands, works out
   which cluster node each command belongs to, sends it there, and returns the reply.
3. The **topology refresh thread** keeps an eye on the cluster layout (which node owns which slots)
   and tells the workers when it changes.

## Threads

| Thread | Source | What it does |
|---|---|---|
| Main | `valkey-proxy.c` | Reads the config, discovers the cluster, starts the other threads, then accepts new clients for as long as the proxy runs (`accept_loop()`). |
| Worker (`worker-thread-no` of them) | `valkey-proxy.c` (`worker_thread()`) | Serves a group of clients. Each worker has its own event loop, its own backend connections and its own copy of the cluster layout. |
| Topology refresh | `cluster.c` (`topology_refresh_loop()`) | Re-reads the cluster layout every `cluster-refresh-interval-ms` (default 500ms) and tells the workers when it has changed. |

### Main thread: reading the config

`parse_conf()` in `config.c` reads `proxy.conf` before anything else starts. Each line is an option
name and a value; option names always use `-` (`max-connection`, `cluster-refresh-interval-ms`).
The proxy prints an error naming the file (and the line, for a bad option) and exits with code 1
when:

- `server-port` or `port` is missing or not between 1 and 65535;
- an option name isn't one the proxy knows, including the old `_` spellings such as
  `max_connection`;
- an option is given without a value;
- `server-ip` is longer than a hostname can be.

Numeric settings that must be positive (`max-connection`, `max-multikey-payload`,
`backend-connect-timeout-ms`, `shared-connection-per-node`, `max-pipeline-inflight`) fall back to
their default with a warning instead. Everything ends up in the global `g_config`, plus the pool
sizes in `g_pool_min[]`/`g_pool_max[]`, and doesn't change while the proxy runs.

### Main thread: taking in new clients

The main thread's only job after startup is to accept new connections, on the TCP port and on the
Unix socket if one is configured.

For each new client:

1. `pick_worker()` chooses the worker that currently has the fewest clients. If several are tied, it
   takes turns between them.
2. The main thread passes the connection to that worker. Every connection is identified by a small
   number, its file descriptor (fd), so "passing the connection" just means sending that number.
   Each worker has a **handoff pipe** for this: a one-way channel that the main thread writes fds
   into and the worker reads them from.

Two details make this hand-off safe and self-regulating:

- **A number is never split.** Each fd is written as one 4-byte `int`. The operating system
  guarantees that a write this small goes into the pipe in one piece, so the worker never reads half
  a number or a mix of two.
- **A busy worker slows down new connections.** A pipe can only hold a limited amount of data. If a
  worker falls far behind and stops reading its pipe, the pipe fills up, and the main thread waits
  on its next write. While it waits, it doesn't accept anyone new. New connections queue up in the
  operating system until the worker catches up. This is called **back-pressure**: when the next step
  can't keep up, the step before it slows down instead of piling up more work.

The main thread also adds 1 to the worker's client count right away, before the worker has even
seen the new client. Otherwise, during a burst of new connections, every one of them would go to
whichever worker happened to be least busy when the burst started.

### Workers: each one works on its own

Each worker runs an **event loop**: it waits until one of its sockets has something to do (data to
read, room to write), handles it, and goes back to waiting. The `el_*.c` files implement this on top
of whatever the operating system offers (epoll on Linux, kqueue on macOS/BSD, event ports on
Solaris, or plain select); `el_backend_name()` reports which one is in use.

Each worker keeps its own private copy of everything it needs:

- `client_map` / `backend_map`: lookup tables from an fd to the client (`ClientConn`) or backend
  connection (`PoolConn`) it belongs to, with room for `max-connection` entries;
- its backend connection pools, grouped by database (`DbGroup`) and by node (`NodePools`);
- a snapshot of the cluster layout (`TopoSnapshot`).

Because workers share nothing while serving commands, they never wait on each other and need no
locks. A client stays on the same worker until it disconnects.

One pass of the worker loop (`worker_thread()`):

1. `el_poll()` waits for events. It returns straight away if a client already has more pipelined
   commands sitting in memory (`input_ready`), so those are never left waiting.
2. If the cluster layout has changed, the worker updates its own copy first
   (`worker_sync_topology()`). It knows by comparing `g_topology_version` with the version it last
   copied (`topology_version_seen`). That check is a single atomic read, so while the layout stays
   the same it costs no lock and no copying.
3. It handles each socket that has something to do:
   - the handoff pipe: new clients from the main thread;
   - the wake pipe: a nudge about a layout change, or a `CLIENT LIST` request from another worker;
   - a client socket;
   - a backend socket.
4. `client_run_buffered_input()` runs any commands already waiting in client buffers.

### Topology refresh: keeping up with the cluster

At startup, `fetch_cluster_nodes()` sends `CLUSTER NODES` to the configured address (`server-ip`,
which can be a hostname) and records the result in the globals `g_nodes`, `g_slot_map`,
`g_node_gen` and `g_node_has_slots`, and sets `g_topology_version` to 1.

Unless `cluster-refresh-interval-ms` is 0, the refresh thread repeats this on a timer (every 500ms
by default), each time over a new short-lived connection:

- It first asks the node that last answered with a usable layout. That starts out as the configured
  address.
- If that node doesn't answer, it tries every other node it already knows. The first one that
  answers with a usable layout is asked first from then on, so a node that has gone down costs one
  connect timeout, not one per refresh.
- If no node answers, or the answer lists no primaries, it keeps using the last good layout.
- Otherwise `reconcile_and_publish()` works out the new layout on a private copy:
  - a node it already knows (same node ID) keeps its index;
  - a new node that took over the slots of a node that has gone (a promoted replica) reuses that
    node's index, and `gen[i]` goes up by 1;
  - a node that is missing starts a timer (`fail_since[i]`); once it has been gone longer than
    `cluster-node-fail-grace-ms`, `gen[i]` goes up by 1 and its slots are left unowned;
  - a node never seen before gets a new index.

  It then publishes the result **only if something changed**: the number of nodes, any `gen[i]`, or
  the slot map. When nothing has, which is the case on almost every refresh, it stops there: no
  global is written and no worker is woken. Changes to a node's other fields alone, such as its
  `flags`, don't count, and reach `g_nodes` only with the next real change.

  When something did change, it:
  - copies the layout into `g_nodes`, `g_node_gen`, `g_node_has_slots`, `g_slot_map` and
    `g_node_count` under `g_topology_lock`;
  - adds 1 to `g_topology_version`. This is the only place it changes after startup, so its value
    (shown as `proxy_topology_version` in `INFO proxy`) counts the layout changes the proxy has seen;
  - writes to every worker's **wake pipe**, so idle workers notice right away instead of on their
    next event;
  - asks the primaries again for their version and modules (see
    [Making the cluster look like one server](#making-the-cluster-look-like-one-server)).

The refresh thread keeps a little state of its own between rounds (`fail_since`, `grace_released`,
`owner_idx`, and which node to ask first). Only it uses that state, so it isn't published.

Each worker then copies the new layout into its own snapshot (`worker_sync_topology()`) and checks
its connections: connections to a node that changed are reconnected or closed. A connection that is
busy with a request at that moment is marked `discard_on_release` and closed only after that request
finishes.

A command only ever looks at its worker's snapshot, so even if the layout changes mid-command, all
of that command's routing decisions are based on the same layout.

## Life of a command

### 1. Reading the command (`client.c`, `resp.c`)

`client_process_input()` tracks where each client is in handling its current command, using
`ClientConn.state`:

| State | Meaning |
|---|---|
| `CS_READING_HDR` | Reading the start of the next command and working out its name, key and length (`resp_parse_header()`). |
| `CS_STREAMING_CMD` | A large single-key command: its remaining bytes are passed on to the backend as they arrive, instead of being held in memory first. |
| `CS_BUFFERING_CMD` | Collecting a whole multi-key or blocking command (or `AUTH`) before handling it. A command larger than `max-multikey-payload` (default 64MB) is rejected with `-ERR command too large`. |
| `CS_WAITING_CONN` | All backend connections are busy; waiting in line (`wait_queue`) for one to free up. |
| `CS_WAITING_RESP` / `CS_WRITING_RESP` | Waiting for the backend's reply, then sending it to the client. |
| `CS_DISCARDING_CMD` | Throwing away the rest of a rejected command (for example, one that is too large). |

Commands typed as plain text (inline commands, like `PING` in telnet) are converted to the normal
RESP format first. Any command can be sent this way; a line is at most 16KB.

With pipelining, one read from the socket can contain several commands. Only the current command
is kept in `hbuf`/`pbuf`; any bytes after it belong to later commands and wait in `qbuf` until their
turn.

### 2. Deciding where the command goes (`cmd.c`)

Transaction commands (`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`) and every command sent inside
`MULTI` are handed straight to `txn_dispatch()` in `txn.c` (see [Transactions](#transactions-txnc)
below). For everything else, `route_command()` picks which kind of backend connection the command
needs. A client that has subscribed to channels uses its subscription connection, except for
`PUBLISH`/`SPUBLISH`.

`classify_command()` then sorts the command into one of six classes:

| Class | How it's handled | Main source |
|---|---|---|
| `CMD_CLASS_SINGLE_KEY` | Hash the key to a slot (`key_slot()`: CRC16, respecting `{hash tags}`), look up the node that owns that slot, send it there. | `client.c` |
| `CMD_CLASS_MULTI_KEY` | **Fan-out** commands (`MGET`, `MSET`, `DEL`, `EXISTS`, `TOUCH`, `UNLINK`, `LCS`, ...) are split into one piece per node, sent in parallel, and the replies are combined into one. Other multi-key commands must have all their keys in the same slot; otherwise the client gets `-CROSSSLOT`. | `fanout.c` |
| `CMD_CLASS_NO_SLOT` | Commands without a key. Some are answered by the proxy itself (`INFO`, `CLIENT`, `HELLO`, `PROXY`, `CLUSTER KEYSLOT`, ...), some are sent to every primary and the results combined (`DBSIZE`, `SCAN`, `FLUSHALL`, ...), and the rest go to one node (`CLUSTER SLOTS`, for example, is relayed unchanged from one primary). | `fanout.c`, `info.c`, `hello.c`, `metrics.c` |
| `CMD_CLASS_BLOCKING` | Commands that can wait for data (`BLPOP`, `BRPOP`, `XREAD ... BLOCK`, ...) get a connection of their own, so they don't hold up anyone else. | `client.c` |
| `CMD_CLASS_PUBSUB` | Subscriptions use a connection reserved for that client (`sub_conn`). | `pubsub.c` |
| `CMD_CLASS_ACL` | `AUTH`/`ACL`. A client that logs in as a non-default user gets its own logged-in connection to each node. | `acl.c` |

### 3. Talking to the backend (`pool.c`, `mux.c`)

Each worker keeps a set of ready-made connections, called a **pool**, for every combination of
database × node × pool type:

| Pool | Used for |
|---|---|
| `POOL_NORMAL` | Single-key and fan-out commands, when multiplexing is off. |
| `POOL_BLOCKING` | Blocking commands. |
| `POOL_SUBSCRIPTION` | Pub/Sub. |
| `POOL_TRANSACTION` | Transactions. A connection stays with one client for the whole transaction. |
| `POOL_ACL` | Connections logged in as a client's own ACL user, including for that user's transactions. |
| `POOL_SHARED` | Connections shared by many requests at once, when `backend-multiplexing` is on. |

The first five pools give each connection to one user at a time. At startup each pool opens its
`min` connections. When a connection is needed, the pool hands out an idle one, or opens a new one
if it's below `max`. A connection that is given back goes to the next waiting client, or becomes
idle. Idle connections are never closed, so a pool doesn't shrink once it has grown.

`POOL_NORMAL` serves one command at a time: a single-key command, or one node's part of a fan-out
command. The connection is given back as soon as the reply has been read. When the pool is full, a
single-key command waits for a free connection, while a fan-out command fails. With
`backend-multiplexing` on, these commands use `POOL_SHARED` instead, and `POOL_NORMAL` is only used
for the rest (keyless commands, `HELP` subcommands, commands too large to buffer, ...).

`POOL_BLOCKING` works the same way for blocking commands: the connection is held until the command
returns, then given back. When the pool is full, the client waits.

`POOL_SUBSCRIPTION` gives a client its own connection on its first `SUBSCRIBE`. The client keeps it
until it has unsubscribed from everything or disconnects. The connection is then closed, not given
back. When the pool is full, the client gets an error.

`POOL_TRANSACTION` holds a transaction. `MULTI` itself is answered by the proxy, so no connection
is taken yet. The first `WATCH` or the first queued command with a key decides the slot, and so the
node, of the whole transaction. The client then takes a connection to that node and keeps it until
`EXEC`, `DISCARD` or `UNWATCH`, after which the connection goes back to the pool. If the
transaction breaks off or the client disconnects, the connection is closed instead, since it may
still be inside a `MULTI` or hold a `WATCH`. If the connection is lost in the middle, a `WATCH` on it
is lost too (`EXEC` then answers nil), and a `MULTI` on it fails (`EXEC` then answers
`-EXECABORT`). When the pool is full, the client waits.

`POOL_ACL` is for clients logged in as a non-default user. On `AUTH user pass`, the proxy takes a
connection to the first primary and sends the `AUTH` there. If it succeeds, that connection belongs
to the client. The first time the client's commands go to another node, it takes one more
connection, logged in the same way, to that node. The client keeps these connections between
commands and never gives them back. They are closed when the client logs in again (including
`AUTH default`) or disconnects. Single-key commands, multi-key commands in one slot, blocking
commands and transactions of such a client run on these connections. Fan-out commands, keyless
commands and Pub/Sub still use the other pools, as the default user. When the pool is full, `AUTH`
or the command gets an error.

When a node is replaced (for example after a failover), idle connections to it are closed, and
connections in use reconnect to the new node when they are given back.

Each worker creates the pools for the configured `db` at startup (`db_group_get()`); all clients use
that one database. If `db` isn't 0, each new connection sends `SELECT <db>` before it's used. A client
`SELECT` is answered locally: `+OK` for the configured index, `-ERR DB index is out of range` otherwise.

#### Multiplexing (`backend-multiplexing yes`)

Lending a connection to one request at a time means a client's pipelined commands still go out one
by one. With multiplexing on, single-key commands and the pieces of fan-out commands instead share a
small, fixed set of connections per node (`shared-connection-per-node`), and many requests can be on
the same connection at once:

- **Which connection:** a request uses connection number `slot % shared-connection-per-node`. All
  requests for the same key always use the same connection, so they reach Valkey in the order the
  proxy received them, even when they come from different clients.
- **Matching replies on the backend side:** Valkey always answers the requests on a connection in
  the order they were sent. So each connection keeps a first-in, first-out list of the requests
  written to it (`PoolConn.mq`), and every complete reply belongs to the oldest request on that list.
- **Keeping the client's replies in order:** each multiplexed command gets a `Req` entry, added to the
  client's list in the order the commands arrived. Replies from different connections can come back
  in any order, but they are sent to the client strictly in the order of that list.
- **Commands that can't share a connection:** these wait until every reply before them has been sent
  to the client, then run the one-at-a-time way described above. This applies to keyless, blocking,
  Pub/Sub, ACL, `SELECT` and very large (> 16 KB) commands, and to every command from a client that
  is in a transaction or subscribed.
- **Not letting one client run ahead:** the proxy stops reading from a client while it has
  `max-pipeline-inflight` requests in progress, or 4 MB of replies it hasn't read yet.
- **When a connection drops:** reconnecting is retried with a pause in between, so a dead node costs
  one connect timeout per pause instead of one per command.

All shared connections are opened at startup: `shared-connection-per-node × nodes × workers`, plus
the same again for each extra database in use. The proxy prints a warning if that adds up to more
than half of `max-connection`.

#### Transactions (`txn.c`)

`MULTI`, `EXEC`, `DISCARD`, `WATCH` and `UNWATCH` behave as they do on a Valkey cluster. The details,
including exactly which commands can be queued, are in [FEATURES.md](FEATURES.md) §8.

- **One connection:** a transaction runs on one backend connection, which stays with the client
  (`client->txn_conn`) from its first `WATCH` or first queued command until `EXEC`, `DISCARD` or
  `UNWATCH`. Clients using the default user take one from `POOL_TRANSACTION`. If all of them are in
  use, the transaction waits for one. A client logged in as its own ACL user uses its own connection
  to that node instead.
- **One slot:** the first key the transaction uses, in `WATCH` or in a queued command, picks the slot
  and the node. A key in any other slot gets `-CROSSSLOT`. Keyless commands that only make sense on
  one node (`PING`, `ECHO`, `TIME`, `UNWATCH`, and `EVAL`/`FCALL` with no keys) can be queued once
  that node is known. Other keyless commands are rejected.
- **`MULTI` goes out late:** the proxy answers `MULTI` itself and sends it to the backend only just
  before the first command that needs it, as a request of its own. That way, if the backend refuses
  `MULTI` (for example with `NOPERM`), the next command can't run outside the transaction by
  mistake.
- **Rejected commands:** if the proxy or the backend rejects a command inside `MULTI`, the
  transaction is marked as failed (`txn_dirty`), and `EXEC` answers `-EXECABORT`.
- **Slot moved:** commands keep going to the node picked at the start. If the slot has moved since,
  the backend answers `-MOVED` without running anything. A transaction without `WATCH` is then
  replayed on the new node from a copy of its queued commands (`txn_buf`, up to
  `transaction-replay-buffer-size` bytes, at most twice). A transaction with `WATCH` can't be
  replayed, because the `WATCH` was on the old node, so `EXEC` answers nil and the client retries,
  as after any `WATCH` abort.
- **Lost connection:** every way a backend connection can close ends in `pool_destroy_conn()`, which
  tells the transaction (`txn_conn_gone()`). A lost `WATCH` makes `EXEC` answer nil, and a lost
  `MULTI` makes it answer `-EXECABORT`.
- **RESP2 clients:** each element of `EXEC`'s reply is converted the same way as the reply to the
  command queued in that position.

### 4. Sending the reply: RESP2 and RESP3 (`hello.c`, `resp.c`)

Clients can use either version of the protocol, RESP2 or RESP3, and switch with `HELLO`:

1. The first time any client sends `HELLO 3`, the proxy checks whether the backends support RESP3,
   and records the answer in `g_backend_resp3_active`.
2. After that, each backend connection switches itself to RESP3 the next time it's used.
3. When a reply from a RESP3 connection goes to a RESP2 client, `deliver_backend_reply()` converts it
   back with `resp3_downgrade()`. A few commands need a little more reshaping: flattening or nesting
   pairs (`cmd_resp3_pair_transform()`), or returning a nil array instead of a nil string
   (`cmd_resp2_nil_array_depth()`).

## Making the cluster look like one server

- **`INFO`** (`info.c`) describes the proxy as a single standalone server:
  - the `server` fields are the proxy's own;
  - the version shown is the lowest version among the primaries, and the modules listed are the
    ones loaded on every primary. `probe_backend_versions()` asks each primary with
    `INFO server modules` at startup and again after every published layout change, and at no
    other time;
  - `keyspace` is added up across all primaries, while `memory`, `stats` and similar sections are
    shown per shard (see [FEATURES.md](FEATURES.md) for where each section comes from);
  - `reported-version`, `reported-role` and `reported-modules` in `proxy.conf` override what `HELLO`
    and `INFO` report.
- **`CLIENT LIST` / `CLIENT INFO`** (`client_list.c`, `fanout.c`) list the clients connected to the
  proxy, not the proxy's own connections to the backends. Since each worker only knows its own
  clients, the worker serving `CLIENT LIST` asks every other worker through its wake pipe and
  combines their answers.
- **`PROXY POOLS` / `PROXY INFO`** (`metrics.c`) are proxy-only commands that show connection pool
  usage (open, in use, waiting, connects, waits and wait times), added up across workers.

## Where to find things

| File | What's in it |
|---|---|
| `valkey-proxy.c` | `main()`, listeners, the accept loop, worker threads, handing clients to workers |
| `config.c` | Reading `proxy.conf`, socket helpers (`tcp_connect_timeout()`, listeners), TLS setup |
| `event_loop.c`, `el_*.c` | The event loop, on top of epoll / kqueue / event ports / select |
| `cluster.c` | Discovering the cluster with `CLUSTER NODES`, periodic refresh, publishing changes, `key_slot()` |
| `client.c` | The client state machine, command dispatch, single-key commands, delivering backend replies |
| `resp.c` | RESP parsing, extracting arguments and keys, RESP3 → RESP2 conversion |
| `cmd.c` | Command tables; picking the pool type and command class |
| `pool.c` | One-at-a-time connection pools, the wait queue, updating pools after a layout change |
| `mux.c` | Multiplexing over `POOL_SHARED` connections |
| `txn.c` | Transactions: `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH`, the pinned connection, replay after `-MOVED` |
| `fanout.c` | Fan-out, sent-to-every-node and proxy-answered commands, combining replies |
| `pubsub.c` | Pub/Sub sessions |
| `acl.c` | `AUTH`/`ACL` and per-user logged-in connections |
| `acl_cat.c`, `acl_cat_data.h` | `ACL CAT`, answered from a built-in table generated by `tools/gen_acl_cat.py` |
| `hello.c` | `HELLO`, the RESP3 support check and per-connection switch to RESP3 |
| `info.c` | `INFO` and `PROXY INFO` |
| `client_list.c` | `CLIENT LIST` across workers |
| `metrics.c` | `PROXY POOLS` and other proxy-only commands |
| `proxy.h` | Shared types (`ClientConn`, `PoolConn`, `ConnPool`, `Worker`, `Config`) and declarations |

## Limits and known gaps

- `MAX_NODES` (64) and `MAX_WORKERS` (64) are fixed when the proxy is compiled, so clusters with more
  than 64 nodes aren't supported.
- `max-connection` (default 4000) is a single limit on open connections for the whole process:
  client connections, backend connections and listeners all count. At startup the proxy tries to
  raise the operating system's open-file limit (`RLIMIT_NOFILE`) to match.
- Periodic topology refresh is on by default, every 500ms (`cluster-refresh-interval-ms 500`). With
  it set to 0, the proxy doesn't notice failovers or resharding until it's restarted.
- The version and modules reported by `HELLO` and `INFO` are read again only when the layout
  changes. An upgrade done in place (same addresses) or a `MODULE LOAD` that leaves the layout as it
  was shows up only after the next layout change or a restart. `reported-version` and
  `reported-modules` can set them explicitly.
- Outside transactions, the proxy doesn't follow `-MOVED` or `-ASK` replies. A slot that has moved
  is picked up only by the next topology refresh. Inside a transaction, `-ASK` and `-TRYAGAIN` are
  returned to the client as they are.
- Once the refresh thread has switched to another node, it doesn't go back to the configured
  `server-ip`. If that is a DNS name that later points to new nodes, the proxy only notices if one
  of the nodes it already knows still answers.
- A client that `WATCH`es and never sends `EXEC`, `DISCARD` or `UNWATCH` keeps its transaction
  connection until it disconnects.
