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
| Worker (`worker_thread_no` of them) | `valkey-proxy.c` (`worker_thread()`) | Serves a group of clients. Each worker has its own event loop, its own backend connections and its own copy of the cluster layout. |
| Topology refresh | `cluster.c` (`topology_refresh_loop()`) | Re-reads the cluster layout every `cluster_refresh_interval_ms` and tells the workers about changes. |

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
  connection (`PoolConn`) it belongs to, with room for `max_connection` entries;
- its backend connection pools, grouped by database (`DbGroup`) and by node (`NodePools`);
- a snapshot of the cluster layout (`TopoSnapshot`).

Because workers share nothing while serving commands, they never wait on each other and need no
locks. A client stays on the same worker until it disconnects.

One pass of the worker loop (`worker_thread()`):

1. `el_poll()` waits for events. It returns straight away if a client already has more pipelined
   commands sitting in memory (`input_ready`), so those are never left waiting.
2. If the cluster layout has changed (`g_topology_version` moved), the worker updates its own copy
   first (`worker_sync_topology()`).
3. It handles each socket that has something to do:
   - the handoff pipe: new clients from the main thread;
   - the wake pipe: a nudge about a layout change, or a `CLIENT LIST` request from another worker;
   - a client socket;
   - a backend socket.
4. `client_run_buffered_input()` runs any commands already waiting in client buffers.

### Topology refresh: keeping up with the cluster

At startup, `fetch_cluster_nodes()` sends `CLUSTER NODES` to the configured address (`server_ip`,
which can be a hostname) and records the result in the globals `g_nodes`, `g_slot_map` and
`g_node_gen`.

If `cluster_refresh_interval_ms` is set, the refresh thread repeats this on a timer:

- If the configured address doesn't answer, it tries every node it already knows.
- If no node answers, or the answer lists no primaries, it keeps using the last good layout.
- Otherwise `reconcile_and_publish()` compares the new layout with the current one, and:
  - bumps `g_node_gen[i]` whenever node `i` is now a different server (after a failover, a new shard,
    or a node that stayed down longer than `cluster_node_fail_grace_ms`);
  - increments `g_topology_version` to announce that a new layout is available;
  - writes to every worker's **wake pipe**, so idle workers notice right away instead of on their
    next event.

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
| `CS_BUFFERING_CMD` | Collecting a whole multi-key or blocking command before handling it. |
| `CS_WAITING_CONN` | All backend connections are busy; waiting in line (`wait_queue`) for one to free up. |
| `CS_WAITING_RESP` / `CS_WRITING_RESP` | Waiting for the backend's reply, then sending it to the client. |
| `CS_DISCARDING_CMD` | Throwing away the rest of a rejected command (for example, one that is too large). |

Commands typed as plain text (inline commands, like `PING` in telnet) are converted to the normal
RESP format first.

With pipelining, one read from the socket can contain several commands. Only the current command
is kept in `hbuf`/`pbuf`; any bytes after it belong to later commands and wait in `qbuf` until their
turn.

### 2. Deciding where the command goes (`cmd.c`)

`route_command()` picks which kind of backend connection the command needs, taking the client's
session into account:

- a client inside `MULTI` uses a transaction connection;
- a client that has subscribed to channels uses its subscription connection (except for
  `PUBLISH`/`SPUBLISH`).

`classify_command()` then sorts the command into one of six classes:

| Class | How it's handled | Main source |
|---|---|---|
| `CMD_CLASS_SINGLE_KEY` | Hash the key to a slot (`key_slot()`: CRC16, respecting `{hash tags}`), look up the node that owns that slot, send it there. | `client.c` |
| `CMD_CLASS_MULTI_KEY` | **Fan-out** commands (`MGET`, `MSET`, `DEL`, `EXISTS`, `TOUCH`, `UNLINK`, `LCS`, ...) are split into one piece per node, sent in parallel, and the replies are combined into one. Other multi-key commands must have all their keys in the same slot; otherwise the client gets `-CROSSSLOT`. | `fanout.c` |
| `CMD_CLASS_NO_SLOT` | Commands without a key. Some are answered by the proxy itself (`INFO`, `CLIENT`, `HELLO`, `PROXY`, `CLUSTER KEYSLOT`, ...), some are sent to every primary and the results combined (`DBSIZE`, `SCAN`, `FLUSHALL`, ...), and the rest go to one node. | `fanout.c`, `info.c`, `hello.c`, `metrics.c` |
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
| `POOL_TRANSACTION` | `MULTI`/`EXEC`/`WATCH`. |
| `POOL_ACL` | Connections logged in as a client's own ACL user. |
| `POOL_SHARED` | Connections shared by many requests at once, when `backend_multiplexing` is on. |

The first five pools lend out connections **one request at a time**. A command borrows a connection
(`pool_acquire_nb()`) and gives it back when it's done (`pool_release()`); if someone is already
waiting, the connection goes straight to them. If no connection is free, the client waits in the
pool's `wait_queue`.

Pools for a database are created the first time a client `SELECT`s it (`db_group_get()`). For any
database other than 0, each new connection sends `SELECT <db>` before it's used.

#### Multiplexing (`backend_multiplexing yes`)

Lending a connection to one request at a time means a client's pipelined commands still go out one
by one. With multiplexing on, single-key commands and the pieces of fan-out commands instead share a
small, fixed set of connections per node (`shared_connection_per_node`), and many requests can be on
the same connection at once:

- **Which connection:** a request uses connection number `slot % shared_connection_per_node`. All
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
  `max_pipeline_inflight` requests in progress, or 4 MB of replies it hasn't read yet.
- **When a connection drops:** reconnecting is retried with a pause in between, so a dead node costs
  one connect timeout per pause instead of one per command.

All shared connections are opened at startup: `shared_connection_per_node × nodes × workers`, plus
the same again for each extra database in use. The proxy prints a warning if that adds up to more
than half of `max_connection`.

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
  - the version shown is the lowest version among the shards;
  - `keyspace` is added up across all primaries, while `memory`, `stats` and similar sections are
    shown per shard (see [FEATURES.md](FEATURES.md) for where each section comes from);
  - `reported_version`, `reported_role` and `reported_modules` in `proxy.conf` override what `HELLO`
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
| `fanout.c` | Fan-out, sent-to-every-node and proxy-answered commands, combining replies |
| `pubsub.c` | Pub/Sub sessions |
| `acl.c` | `AUTH`/`ACL` and per-user logged-in connections |
| `hello.c` | `HELLO`, the RESP3 support check and per-connection switch to RESP3 |
| `info.c` | `INFO` and `PROXY INFO` |
| `client_list.c` | `CLIENT LIST` across workers |
| `metrics.c` | `PROXY POOLS` and other proxy-only commands |
| `proxy.h` | Shared types (`ClientConn`, `PoolConn`, `ConnPool`, `Worker`, `Config`) and declarations |

## Limits and known gaps

- `MAX_NODES` (64) and `MAX_WORKERS` (64) are fixed when the proxy is compiled, so clusters with more
  than 64 nodes aren't supported.
- `max_connection` (default 4000) is a single limit on open connections for the whole process:
  client connections, backend connections and listeners all count. At startup the proxy tries to
  raise the operating system's open-file limit (`RLIMIT_NOFILE`) to match.
- Periodic topology refresh is off by default (`cluster_refresh_interval_ms 0`). Without it, the
  proxy doesn't notice failovers or resharding until it's restarted.
- `POOL_TRANSACTION` exists and its size can be configured, but `classify_command()` doesn't send
  anything to it yet. See the note above `RESP3_NIL_ARRAY_DEPTH0_CMDS` in `cmd.c`.
