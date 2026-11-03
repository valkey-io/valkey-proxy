#ifndef PROXY_H
#define PROXY_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>
#include "event_loop.h"

#ifdef USE_TLS
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

/* ── constants ──────────────────────────────────────────────────────── */
#define MAX_NODES     64
#define MAX_WORKERS   64
#define PROXY_VERSION "0.1.0"
/* Fallback for the "max_connection" proxy.conf setting (g_config.max_connection)
 * when the config file doesn't set it. This is the shared fd budget for the
 * whole process: client sockets + backend pool connections + listener(s). */
#define DEFAULT_MAX_CONNECTIONS 4000
#define CLUSTER_SLOTS           16384
#define HDR_BUF_SIZE            16384
#define MAX_RESP_SIZE           (64 * 1024 * 1024)

#define CLUSTER_NODES_CMD "*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n"
#define MAX_FANOUT_KEYS   256
#define MAX_FANOUT_SUBS   MAX_FANOUT_KEYS

/* ── pool types ─────────────────────────────────────────────────────── */
typedef enum {
    POOL_NORMAL = 0,
    POOL_BLOCKING,
    POOL_SUBSCRIPTION,
    POOL_TRANSACTION,
    POOL_ACL,
    /* Multiplexed: a fixed set of connections per node, each carrying many
     * in-flight requests at once (see src/mux.c). */
    POOL_SHARED,
    POOL_TYPE_COUNT
} PoolType;

extern const char *POOL_NAME[POOL_TYPE_COUNT];
extern int g_pool_min[POOL_TYPE_COUNT];
extern int g_pool_max[POOL_TYPE_COUNT];

/* ── client / command enums ─────────────────────────────────────────── */
typedef enum {
    CS_READING_HDR,
    CS_WAITING_CONN,
    CS_STREAMING_CMD,
    CS_WAITING_RESP,
    CS_WRITING_RESP,
    CS_BUFFERING_CMD,
    CS_DISCARDING_CMD, /* skipping the rest of a rejected command */
} ClientState;

typedef enum {
    CMD_CLASS_SINGLE_KEY = 0,
    CMD_CLASS_MULTI_KEY,
    CMD_CLASS_NO_KEY,
    CMD_CLASS_BLOCKING,
    CMD_CLASS_PUBSUB,
    CMD_CLASS_ACL,
} CmdClass;

extern const char *CMD_CLASS_NAME[];

/* ── forward declarations ───────────────────────────────────────────── */
typedef struct ConnPool ConnPool;
typedef struct ClientConn ClientConn;
typedef struct FanoutSub FanoutSub;
typedef struct FanoutCtx FanoutCtx;
typedef struct Req Req;

/* One request in flight on a POOL_SHARED connection, in the order it was
 * written to that connection (see src/mux.c): replies come back in the same
 * order, so the head entry always owns the next complete reply. Exactly one
 * of req/sub is set, or neither for internal==1 (the connection's own
 * HELLO 3 handshake). */
typedef struct {
    Req *req;
    FanoutSub *sub;
    int internal;
    int backend_resp3; /* reply to this entry comes back as RESP3 */
} MuxEntry;

/* ── pool connection ────────────────────────────────────────────────── */
typedef enum { CONN_AVAILABLE = 0, CONN_IN_USE = 1 } ConnState;

typedef struct {
    int fd;
    ConnState state;
    int db;
    ConnPool *pool;
    ClientConn *waiting_client;
    FanoutSub *fanout_sub;
    /* Only meaningful when pool->type==POOL_ACL: set while this connection is
     * waiting on the reply to an internal AUTH handshake (either the top-level
     * AUTH command's own credential probe, or a transparent per-node
     * reconnect-and-reauth done on behalf of a queued business command). */
    int acl_authenticating;
    int acl_is_login_probe;
    /* Login probe issued by "HELLO <ver> AUTH user pass" rather than by the
     * AUTH command (see hello.c): owned via ClientConn.hello_auth_conn, not
     * acl_conns[], until it succeeds. acl_hello_resp3 = the probe sent
     * "HELLO 3 AUTH ..." (so success also means this conn speaks RESP3)
     * instead of a plain "AUTH ...". */
    int acl_hello_login;
    int acl_hello_resp3;
    /* RESP3 handshake state (see src/hello.c). resp3=1 once this connection
     * has completed its own "HELLO 3" with the backend and therefore speaks
     * RESP3 from now on; resp3_handshake=1 while a HELLO 3 sent on this
     * connection (either the one-time global capability probe, or an
     * ordinary lazy per-connection upgrade) is in flight. resp3_probe
     * distinguishes the two: only meaningful while resp3_handshake is set. */
    int resp3;
    int resp3_handshake;
    int resp3_probe;
    /* While resp3_handshake is set, wbuf is occupied by the HELLO 3 bytes,
     * so the real command this connection was about to forward (for the
     * call sites that have no client-side hbuf/pbuf to fall back on -- fan-
     * out sub-dispatch and the pub/sub sub_conn) is held here instead, and
     * flushed into wbuf once the handshake completes. Supports repeated
     * append calls (see resp3_defer_append() in hello.c): pubsub in
     * particular may see more than one client command land on the same
     * sub_conn before its in-flight handshake replies. */
    char *resp3_deferred_cmd;
    int resp3_deferred_len, resp3_deferred_cap;
    char *wbuf;
    int wbuf_len, wbuf_off, wbuf_cap;
    char *rbuf;
    int rbuf_len, rbuf_cap;
    /* Bumped every time this slot is handed to a new logical owner (via
     * pool_acquire_nb or pool_release's waiter hand-off). A single epoll
     * batch can report EPOLLHUP/EPOLLERR as both readable and writable on
     * one event; if the writable handler reassigns this connection to a
     * new owner, the dispatch loop uses this to detect that and skip the
     * now-stale readable dispatch instead of misapplying it to the new
     * owner. */
    uint32_t epoch;
    /* POOL_SHARED only: requests written to this connection still waiting
     * for their reply, as a ring mq[(mq_head + i) % mq_cap], i < mq_len. */
    MuxEntry *mq;
    int mq_head, mq_len, mq_cap;
    /* POOL_SHARED only: fd is -1 while disconnected; no reconnect attempt
     * before mono_us() reaches this, so a dead node costs one connect
     * timeout per backoff period rather than one per command. */
    uint64_t mux_retry_us;
    /* Set by worker_sync_topology() when this index's node identity has
     * moved on (failover / grace-period eviction) while this connection was
     * CONN_IN_USE. pool_release() checks this instead of tearing the
     * in-flight request down immediately: the connection is reconnected (or
     * destroyed on failure) only once the caller is done with it. */
    int discard_on_release;
} PoolConn;

/* Usage counters for one ConnPool, reported by PROXY POOLS (see
 * src/metrics.c). Only the owning worker thread ever writes them, so pool.c
 * updates them with plain relaxed load/store pairs rather than atomic
 * read-modify-writes; the atomics only exist so another worker can read
 * them mid-update without a data race. open/in_use/waiting are gauges,
 * re-published after every change to the pool; the rest only ever grow. */
typedef struct {
    atomic_int live; /* 1 once pool_init() has run for this pool */
    atomic_int open, in_use, waiting;
    atomic_ullong acquires;         /* connection handed out, immediately or after a wait */
    atomic_ullong waits;            /* client parked in wait_queue */
    atomic_ullong handoffs;         /* parked client handed a connection (also in acquires) */
    atomic_ullong wait_us_sum;      /* total time parked clients waited */
    atomic_ullong wait_us_max;      /* longest single wait */
    atomic_ullong wait_abandoned;   /* parked client disconnected */
    atomic_ullong wait_failed;      /* parked client failed: node gone */
    atomic_ullong connects;         /* backend connections opened */
    atomic_ullong connect_failures; /* backend connect (or SELECT) failed */
    atomic_ullong reconnects;       /* reconnected after a topology change */
    atomic_ullong destroys;         /* backend connections closed, incl. a reconnect's old one */
} PoolStats;

struct ConnPool {
    PoolType type;
    PoolConn *conns;
    int count, max;
    int in_use; /* conns in CONN_IN_USE, kept exact by conn_set_state() */
    int node_idx, db;
    EventLoop *el;
    pthread_mutex_t lock;
    ClientConn **wait_queue;
    int wait_len, wait_cap;
    PoolStats stats;
};

/* gen mirrors the owning Worker's topo.node_gen[node_idx] as of the last time
 * this NodePools was (re)synced -- see worker_sync_topology(). -1 means this
 * index has never been backed by a live shard yet (no pools initialized). */
typedef struct {
    int node_idx;
    int gen;
    ConnPool pools[POOL_TYPE_COUNT];
} NodePools;
typedef struct {
    int db;
    NodePools *node_pools;
} DbGroup;

/* ── client connection ──────────────────────────────────────────────── */
struct ClientConn {
    int fd, db;
    int in_transaction, in_subscribe;
    ClientState state;
    char hbuf[HDR_BUF_SIZE];
    int hbuf_len;
    /* Pipelined input already read off the socket but belonging to commands
     * after the current one: qbuf[qbuf_off..qbuf_len). client_read() drains
     * it before touching the socket again, client_unread() pushes bytes back
     * onto its front -- so hbuf/pbuf only ever hold the current command. */
    char *qbuf;
    int qbuf_off, qbuf_len, qbuf_cap;
    int input_queued; /* 1 = listed in the owning Worker's input_ready[] */
    /* Multiplexed commands (see src/mux.c), oldest first: their replies are
     * appended to wbuf strictly in this order as the head completes.
     * req_count counts every entry, done or not. */
    Req *req_head, *req_tail;
    int req_count;
    /* 1 = the command being read goes out over a POOL_SHARED connection. */
    int mux_cmd;
    /* 1 = a non-multiplexed command is waiting (pushed back to qbuf) for
     * every multiplexed reply before it to be written out. */
    int mux_barrier;
    /* 1 = input paused at max_pipeline_inflight or with too much reply
     * output queued; resumed once the backlog drains. */
    int mux_throttled;
    /* 1 = the last read() of the socket hit EAGAIN, so nothing more is
     * waiting there until the next readable event. */
    int read_eagain;
    int node_idx;
    int total_cmd_len;
    long long cmd_remaining;
    PoolConn *pending_conn;
    ConnPool *waiting_pool;
    uint64_t wait_since_us; /* mono_us() when last parked in a wait_queue */
    char *wbuf;
    int wbuf_len, wbuf_off, wbuf_cap;
    char *pbuf;
    int pbuf_len, pbuf_cap;
    int using_pbuf;
    FanoutCtx *fanout_ctx;
    PoolConn *sub_conn;
    /* 1 = an unsubscribe's fence PING is in flight on sub_conn (see
     * pubsub.c): client input is held until it comes back. */
    int sub_fence_pending;
    /* ── CLIENT ID / GETNAME / SETNAME (see handle_client_cmd in fanout.c) ── */
    long long id;
    char name[64];
    int namelen;
    /* HELLO 3 SETNAME's name, held back while the RESP3 probe is in flight
     * so a failed probe leaves client->name untouched (see hello.c). */
    int hello_pending_setname;
    char hello_pending_name[64];
    int hello_pending_namelen;
    /* HELLO ... AUTH, applied only once the whole HELLO succeeds (see
     * hello.c). hello_pending_auth_default: switch back to the default
     * identity on commit. hello_auth_conn: in-flight POOL_ACL login probe
     * for a non-default user -- kept out of acl_conns[] so a failed login
     * leaves the client's current identity and pinned conns untouched. */
    int hello_pending_auth_default;
    PoolConn *hello_auth_conn;
    char hello_auth_user[64];
    char hello_auth_pass[512];
    int hello_auth_userlen, hello_auth_passlen;
    int hello_auth_protover;
    /* ── CLIENT INFO metadata (see format_client_info_line in fanout.c) ── */
    char addr[280];  /* peer ip:port, or "<unix_socket_path>:0" */
    char laddr[280]; /* proxy-side local ip:port for this connection */
    time_t connect_time;
    time_t last_interaction;
    char last_cmd[40]; /* lowercase "cmd" or "cmd|subcommand" */
    int sub_count, psub_count, ssub_count;
    int multi_count; /* # cmds queued since MULTI; -1 when not in a transaction */
    int watch_count;
    /* ── ACL identity (see src/acl.c) ──────────────────────────────────── */
    int acl_authenticated; /* 1 = using a non-default ACL identity */
    char acl_user[64];
    char acl_pass[512];
    int acl_userlen, acl_passlen;
    PoolConn *acl_conns[MAX_NODES]; /* per-node pinned, authenticated conn;
                                            NULL = not yet connected to that node */
    int waiting_acl_help;           /* 1 = pending reply is an ACL HELP probe */
    /* ── RESP protocol version (see src/hello.c) ────────────────────────── */
    int resp; /* 2 or 3; set at accept() time, changed by HELLO */
#ifdef USE_TLS
    SSL *ssl;
#endif
};

/* ── fan-out sub-request and context ───────────────────────────────── */
struct FanoutSub {
    FanoutCtx *ctx;
    int sub_idx;
    int node_idx;
    int slot; /* picks the POOL_SHARED connection when ctx->req is set */
    PoolConn *conn;
    char *rbuf;
    int rbuf_len, rbuf_cap;
    int done, error;
};

struct FanoutCtx {
    ClientConn *client; /* NULL once a multiplexed request is orphaned */
    /* Set when this fan-out is a multiplexed request: subs go out over
     * POOL_SHARED connections and the aggregated reply completes req instead
     * of being written to client->wbuf. client->fanout_ctx stays NULL. */
    Req *req;
    char cmd[16];
    int nsubs, ndone, had_error;
    FanoutSub subs[MAX_FANOUT_SUBS];
    int total_keys;
    int mget_sub[MAX_FANOUT_KEYS];
    int mget_pos[MAX_FANOUT_KEYS];
    uint64_t scan_node_cursors[MAX_NODES];
    int scan_ntargets;
    int scan_target_nodes[MAX_NODES];
    int sub_for_node[MAX_NODES];
    int lcs_mode;
    long long slowlog_count; /* SLOWLOG/COMMANDLOG GET: max merged entries, -1 = all */
    uint32_t info_sections;  /* INFO: requested sections, a bit per section (see info.c) */
};

/* ── cluster / config ───────────────────────────────────────────────── */
typedef struct {
    char node_id[64], ip[64];
    int port;
    char flags[128];
    int is_master;
} ClusterNode;

/* Per-worker frozen view of the cluster topology, refreshed wholesale from
 * the global g_nodes/g_node_gen/g_slot_map/g_node_count/g_topology_version
 * (see worker_sync_topology() in pool.c) whenever g_topology_version moves.
 * A worker reads only its own copy while processing a command, so a single
 * command's routing decisions stay self-consistent even if the background
 * refresh thread publishes a new topology mid-command. */
typedef struct {
    ClusterNode nodes[MAX_NODES];
    int node_gen[MAX_NODES];
    int node_has_slots[MAX_NODES];
    int node_count;
    int slot_map[CLUSTER_SLOTS];
    int version;
} TopoSnapshot;

/* ── worker ─────────────────────────────────────────────────────────── */
typedef struct {
    int thread_id;
    pthread_t tid;
    EventLoop *el;
    DbGroup *db_groups;
    int db_group_count, db_group_cap;
    pthread_mutex_t db_lock;
    /* Sized at startup to g_config.max_connection (see worker_thread()),
     * since that value is only known once proxy.conf has been parsed. */
    ClientConn **client_map;
    PoolConn **backend_map;
    uint8_t *fd_mask;
    TopoSnapshot topo;
    int topology_version_seen;
    /* Self-pipe: read end sits in this worker's own event loop like any
     * other fd, write end is used by the topology refresh thread (see
     * reconcile_and_publish()/wake_all_workers() in cluster.c) to kick this
     * worker out of a blocking el_poll(-1) as soon as a new topology is
     * published, instead of waiting for it to next wake up on its own via
     * client/backend traffic. -1 until worker_thread() sets it up. */
    int wake_r, wake_w;
    /* Clients that went back to waiting for input while qbuf still holds
     * pipelined commands -- no epoll event will ever fire for bytes that are
     * already in user space, so the worker loop runs these itself (see
     * client_run_buffered_input()). Sized to g_config.max_connection. */
    ClientConn **input_ready;
    int input_ready_len;
    /* Set once worker_thread() has initialized db_lock and db_groups, so
     * PROXY POOLS on another worker knows it's safe to walk this one's
     * pools (see src/metrics.c). */
    atomic_int stats_ready;
    /* Clients currently connected to this worker, for INFO clients. Only
     * this worker writes it; INFO on any worker reads it. */
    atomic_int client_count;
} Worker;

typedef struct {
    char server_ip[64];
    int server_port;
    char bind_ip[64];
    int proxy_port;
    int worker_thread_no, db;
    char unix_socket_path[256];
    int tls_enabled;
    char tls_cert[256], tls_key[256];
    int max_multikey_payload;
    int max_connection;
    /* Multiplexing (see src/mux.c): single-key and fan-out commands share
     * shared_connection_per_node connections per node instead of checking
     * one out per command, and a client may have up to
     * max_pipeline_inflight of them in flight at once. */
    int backend_multiplexing;
    int shared_connection_per_node;
    int max_pipeline_inflight;
    int cluster_refresh_interval_ms; /* 0 = periodic refresh disabled */
    int cluster_node_fail_grace_ms;
    /* Connect timeout for backend connections made *after* startup: pool
     * (re)connects from a worker thread and the periodic topology refresh's
     * own probes. Kept short by default so a momentarily-unreachable node
     * (e.g. a replica still finishing promotion, or a confirmed-dead seed)
     * doesn't tie up a worker thread or stall a whole refresh cycle for
     * seconds -- unlike the one-shot startup discovery, which uses a fixed,
     * more generous timeout since it blocks nothing but its own boot. */
    int backend_connect_timeout_ms;
    /* Overrides for what HELLO and INFO report about the backend; empty
     * means report what the backends say (see src/info.c). */
    char reported_version[32];
    char reported_role[16];
    char reported_modules[448];
} Config;

/* ── globals ────────────────────────────────────────────────────────── */
extern Config g_config;
extern ClusterNode g_nodes[MAX_NODES];
extern int g_node_count;
extern int g_slot_map[CLUSTER_SLOTS];
/* Parallel to g_nodes: bumped whenever index i's identity is reassigned
 * (new shard taking over a free index, failover to a different physical
 * node, or grace-period eviction after an unpromoted master fail). Workers
 * compare their cached NodePools.gen against this to know which pools need
 * to be flushed/reconnected -- see worker_sync_topology() in pool.c. */
extern int g_node_gen[MAX_NODES];
/* Whether index i owns at least one slot as of the last published topology.
 * False for an index that's been grace-released with no new claimant
 * (scenario 2) -- workers use this to tell that case apart from a failover
 * (scenario 3, gen also bumped but the index *does* still own slots, just
 * under a new address) so they know whether reconnecting is worthwhile. */
extern int g_node_has_slots[MAX_NODES];
extern atomic_int g_topology_version;
/* Sticky, process-wide: 0 until the first client's "HELLO 3" successfully
 * probes backend RESP3 support (see resp3_start_probe() in hello.c), then
 * stays 1 for the rest of the process's life -- never reset back to 0. Once
 * set, every PoolConn lazily upgrades itself to RESP3 the next time it's
 * used to forward a command (see resp3_conn_needs_upgrade()/
 * resp3_start_conn_upgrade() in hello.c), regardless of which client's
 * command triggers that. Deliberately much simpler than g_topology_version:
 * there is no "downgrade" to detect, so no per-worker snapshot/diff is
 * needed -- a plain atomic load at the point of use suffices. */
extern atomic_int g_backend_resp3_active;
extern atomic_llong g_next_client_id;
extern pthread_rwlock_t g_topology_lock;
extern Worker g_workers[MAX_WORKERS];
extern int g_tcp_fd, g_unix_fd;
#ifdef USE_TLS
extern SSL_CTX *g_ssl_ctx;
#endif

/* ── event mask helpers ─────────────────────────────────────────────── */
void client_queue_buffered_input(Worker *w, ClientConn *client);
static inline void fd_watch(Worker *w, int fd, int events) {
    if (fd < 0 || fd >= g_config.max_connection) return;
    w->fd_mask[fd] |= (uint8_t)events;
    el_add(w->el, fd, w->fd_mask[fd]);
    /* Every path that finishes a client command hands the client back to
     * input via fd_watch(EL_READABLE) -- the one choke point where pipelined
     * commands still sitting in qbuf can be picked up again. */
    if ((events & EL_READABLE) && w->client_map[fd] && w->client_map[fd]->qbuf_len > 0)
        client_queue_buffered_input(w, w->client_map[fd]);
}
static inline void fd_unwatch(Worker *w, int fd, int events) {
    if (fd < 0 || fd >= g_config.max_connection) return;
    w->fd_mask[fd] &= (uint8_t)~events;
    if (w->fd_mask[fd])
        el_add(w->el, fd, w->fd_mask[fd]);
    else
        el_del(w->el, fd);
}

/* ── config / network ───────────────────────────────────────────────── */
void parse_conf(const char *path);
void set_nonblocking(int fd);
int tcp_connect(const char *ip, int port);
int tcp_connect_timeout(const char *ip, int port, int timeout_ms);
int create_tcp_listener(const char *ip, int port);
int create_unix_listener(const char *path);
#ifdef USE_TLS
int init_tls(void);
#endif

/* Monotonic microseconds, for measuring how long clients wait on a pool. */
static inline uint64_t mono_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ── cluster ────────────────────────────────────────────────────────── */
void fetch_cluster_nodes(void);
int key_slot(const char *key, int klen);
void topology_start_refresh_thread(void);
static inline int topology_version_load(void) {
    return atomic_load_explicit(&g_topology_version, memory_order_acquire);
}
static inline int backend_resp3_active_load(void) {
    return atomic_load_explicit(&g_backend_resp3_active, memory_order_acquire);
}

/* ── RESP protocol ──────────────────────────────────────────────────── */
int resp_parse_header(const char *buf, int len, char *cmd, int *cmdlen_out, char *key, int *keylen_out, int *argc_out,
                      int *total_cmd_len_out);
int resp_response_len(const char *buf, int len);
int resp_get_nth_arg(const char *buf, int len, int n, const char **out, int *out_len);
int resp_extract_keys(const char *buf, int len, const char *cmd, const char **key_ptrs, int *key_lens, int max_keys);
int resp_extract_all_args(const char *buf, int len, const char **args, int *arglens, int max);
int resp_extract_blocking_keys(const char *buf, int len, const char *cmd, const char **kp, int *kl, int max_keys);
int resp_xread_has_block(const char *buf, int len);
char *build_resp_cmd(const char *cmd, int cmdlen, const char **args, const int *arglens, int nargs, int *out_len);
/* ── RESP3 ─────────────────────────────────────────────────────────────
 * Convert a complete RESP3-shaped reply (buf[0..len)) into an equivalent
 * RESP2 byte sequence, written to a newly malloc'd *out (caller frees).
 * nil_array_depth selects which recursion depth (0 = the reply's own
 * outermost element, 1 = an immediate child of it, -1 = never) a bare `_`
 * becomes `*-1\r\n` (nil array) instead of the default `$-1\r\n` (nil bulk)
 * -- see cmd_resp2_nil_array_depth(). Returns 0 on success, -1 if buf isn't
 * valid RESP (treat like any other malformed backend reply). */
int resp3_downgrade(const char *buf, int len, int nil_array_depth, char **out, int *out_len);
/* Same, but for a pub/sub push message: only ever called on a buffer whose
 * outermost type is `>` (push), which becomes `*` (array); no nil case
 * applies to push messages. */
int resp3_downgrade_push(const char *buf, int len, char **out, int *out_len);
/* Structural (non-marker) RESP3→RESP2 fixups for the handful of commands
 * whose RESP3 reply changes nesting depth with no distinguishing type byte
 * at all -- resp3_downgrade() above can't detect these on its own. Call
 * *after* resp3_downgrade() has already run; see cmd_resp3_pair_transform()
 * in cmd.c for which commands need which one. Both return 0 (transformed,
 * out/out_len set, caller frees), 1 (buf isn't in the expected shape --
 * caller keeps using its original, already-downgraded buffer unchanged),
 * or -1 (malformed input -- caller should likewise just fall back to the
 * original buffer rather than treat this as a hard error). */
int resp3_flatten_pairs(const char *buf, int len, char **out, int *out_len);
int resp3_nest_pairs(const char *buf, int len, char **out, int *out_len);

/* Growable byte buffer (see resp.c). Both appenders return 0 on success,
 * -1 on allocation failure; the caller owns and frees buf. */
typedef struct {
    char *buf;
    int len, cap;
} GrowBuf;
int gb_append(GrowBuf *g, const char *data, int len);
int gb_append_fmt(GrowBuf *g, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/* ── connection pool ────────────────────────────────────────────────── */
void pool_init(Worker *w, ConnPool *p, PoolType type, int node_idx, int db);
int pool_new_fd(Worker *w, ConnPool *p);
PoolConn *pool_acquire_nb(Worker *w, ConnPool *p);
void pool_enqueue_waiter(ConnPool *p, ClientConn *client);
void pool_remove_waiter(ConnPool *p, ClientConn *client);
void pool_release(Worker *w, PoolConn *c);
void pool_destroy_conn(Worker *w, PoolConn *c);
DbGroup *db_group_get(Worker *w, int db);
void worker_sync_topology(Worker *w);

/* ── multiplexed requests (see src/mux.c) ───────────────────────────── */
struct Req {
    ClientConn *client; /* NULL = client gone; the reply is dropped on arrival */
    Req *next;
    int done;
    char *reply;
    int reply_len;
    FanoutCtx *fanout; /* set while a fan-out request's subs are out */
    /* Reply shaping, captured when the command was sent rather than read
     * from the client later, since more commands may have run since. */
    int client_resp, nil_array_depth, pair_transform;
};

void mux_pool_init(Worker *w, ConnPool *p);
void mux_pool_reset(Worker *w, ConnPool *p, int reconnect, const char *msg);
int client_mux_busy(const ClientConn *client);
int client_can_mux(const ClientConn *client, const char *cmd, int total_cmd_len);
int mux_client_output_full(const ClientConn *client);
Req *mux_req_new(ClientConn *client);
void mux_req_complete(Worker *w, Req *req, const char *buf, int len);
void mux_queue_error(ClientConn *client, const char *line, int len);
void mux_input_done(Worker *w, ClientConn *client);
int mux_send_cmd(Worker *w, ClientConn *client, int node_idx, int slot, const char *buf, int len);
int mux_send_sub(Worker *w, FanoutSub *sub, int db, const char *buf, int len);
void mux_client_flush(Worker *w, ClientConn *client);
void mux_client_release(ClientConn *client);
void mux_conn_writable(Worker *w, PoolConn *conn);
void mux_conn_readable(Worker *w, PoolConn *conn);

/* ── command routing ────────────────────────────────────────────────── */
PoolType cmd_get_pool_type(const char *cmd);
int cmd_in_list(const char *cmd, const char **list);
int cmd_is_supported(const char *cmd);
void str_toupper(char *s);
PoolType route_command(ClientConn *client, const char *cmd);
CmdClass classify_command(const char *cmd, PoolType pt);
int cmd_is_fanout(const char *cmd);
void record_last_cmd(ClientConn *client, const char *cmd, const char *key, int keylen);
/* Which recursion depth (if any) of cmd's reply is semantically a nil
 * *array* rather than a nil bulk string -- feeds resp3_downgrade()'s
 * nil_array_depth argument directly. -1 = never (the common case). Most
 * affected commands are 0 (their own reply, e.g. a blocking command's
 * timeout); GEOPOS is the one exception needing 1 (see resp.c). Case-
 * insensitive. */
int cmd_resp2_nil_array_depth(const char *cmd);
/* 0 = no structural fixup needed, 1 = resp3_flatten_pairs(), 2 =
 * resp3_nest_pairs() -- see cmd.c for the command lists and resp.c for
 * what each transform does. Case-insensitive. */
int cmd_resp3_pair_transform(const char *cmd);

/* ── client helpers ─────────────────────────────────────────────────── */
void client_send_error(Worker *w, ClientConn *client, const char *msg);
void client_send_crossslot(ClientConn *client);
void close_client(Worker *w, ClientConn *client);
void resume_client_after_conn(Worker *w, ClientConn *client, PoolConn *conn);
void client_recover_after_backend_error(Worker *w, ClientConn *client, PoolConn *conn, const char *msg);
/* Deliver a complete backend reply (buf[0..len)) to cl: runs it through
 * resp3_downgrade() first when cl is RESP2 but conn has been upgraded to
 * RESP3, otherwise copies it verbatim. Grows cl->wbuf as needed. Returns 0
 * on success (caller proceeds as normal), -1 if resp3_downgrade() rejects
 * buf as malformed (cl/conn still alive -- caller should treat this like
 * any other bad backend response), or -2 if an OOM already forced this
 * function to call client_send_error()+close_client() itself (caller must
 * return immediately without touching cl or conn again). */
int deliver_backend_reply(Worker *w, ClientConn *cl, PoolConn *conn, const char *buf, int len);
/* The RESP3->RESP2 conversion deliver_backend_reply() applies, for a reply to
 * a command with the given cmd_resp2_nil_array_depth() and
 * cmd_resp3_pair_transform(). 0 on success (*out malloc'd, caller frees),
 * -1 if buf is malformed or on allocation failure. */
int resp3_downgrade_reply(const char *buf, int len, int nil_array_depth, int pair_transform, char **out, int *out_len);

/* ── backend / client event handlers ───────────────────────────────── */
void handle_backend_writable(Worker *w, PoolConn *conn);
void handle_backend_readable(Worker *w, PoolConn *conn);
void handle_client_writable(Worker *w, ClientConn *client);
void handle_client_readable(Worker *w, ClientConn *client);
void client_run_buffered_input(Worker *w);

/* ── fan-out handlers ───────────────────────────────────────────────── */
int topo_first_master(Worker *w);
int topo_all_masters(Worker *w, int *out);
void fanout_ctx_free(FanoutCtx *ctx);
void handle_fanout_sub_readable(Worker *w, PoolConn *conn);
void fanout_aggregate_and_reply(Worker *w, FanoutCtx *ctx);
/* A multiplexed fan-out sub's reply (buf == NULL: it failed). */
void fanout_sub_complete(Worker *w, FanoutSub *sub, const char *buf, int len, int backend_resp3);
void handle_fanout_cmd(Worker *w, ClientConn *client, const char *cmd);
void handle_nokey_cmd(Worker *w, ClientConn *client, const char *cmd, int cmdlen);
void handle_scan_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_multikey_complete(Worker *w, ClientConn *client, const char *cmd);

/* ── pub/sub and blocking ───────────────────────────────────────────── */
void handle_sub_conn_readable(Worker *w, PoolConn *conn);
void handle_blocking_complete(Worker *w, ClientConn *client, const char *cmd);
void handle_pubsub_cmd(Worker *w, ClientConn *client, int cmdlen, const char *cmd, const char *key, int keylen);
void handle_pubsub_inspect_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_slowlog_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_commandlog_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_module_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_script_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_function_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_command_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_client_cmd(Worker *w, ClientConn *client, int cmdlen);
int client_name_check(Worker *w, ClientConn *client, const char *name, int namelen);
int client_set_name(Worker *w, ClientConn *client, const char *name, int namelen);
void send_multibulk_reply(Worker *w, ClientConn *client, const char **lines, int n);
void send_bulk_reply(Worker *w, ClientConn *client, const char *data, int len);
/* Sends resp[0..resp_len) to client as its reply and takes ownership of resp. */
void fanout_send_response(Worker *w, ClientConn *client, char *resp, int resp_len);
/* Sends cmd_buf to each node in targets on its POOL_NORMAL pool and
 * aggregates by ctx->cmd once every node has replied. ctx must already be
 * attached to client (client->fanout_ctx). */
void fanout_to_nodes(Worker *w, ClientConn *client, FanoutCtx *ctx, const char *cmd_buf, int cmd_len,
                     const int *targets, int ntargets);

/* ── proxy-local commands (see src/metrics.c) ───────────────────────── */
void handle_proxy_cmd(Worker *w, ClientConn *client, int cmdlen);
/* Appends the "# Pools" section that PROXY POOLS returns. 0 on success,
 * -1 on allocation failure. */
int proxy_pools_append(Worker *w, GrowBuf *g);

/* ── INFO (see src/info.c) ──────────────────────────────────────────── */
void info_init(const char *argv0, const char *conf_path);
/* A module the backends have loaded, as HELLO reports it. */
#define MAX_BACKEND_MODULES 32
typedef struct {
    char name[64];
    long long ver;
} BackendModule;
void info_set_backend_info(const char *valkey_version, const char *redis_version, const BackendModule *mods, int nmods);
/* What HELLO and INFO report, with the reported_* config overrides
 * applied. */
void info_get_reported_versions(char *valkey_version, size_t vcap, char *redis_version, size_t rcap);
const char *info_reported_role(void);
int info_get_reported_modules(BackendModule *out, int cap);
void handle_info_cmd(Worker *w, ClientConn *client, int cmdlen);
char *info_aggregate(Worker *w, FanoutCtx *ctx, int *out_len);
void handle_proxy_info_cmd(Worker *w, ClientConn *client, int cmdlen);
char *proxy_info_aggregate(Worker *w, FanoutCtx *ctx, int *out_len);

/* ── ACL / AUTH ──────────────────────────────────────────────────────── */
void handle_auth_cmd(Worker *w, ClientConn *client);
void handle_acl_conn_readable(Worker *w, PoolConn *conn);
int acl_route_business_cmd(Worker *w, ClientConn *client, int node_idx);
void acl_teardown_all(Worker *w, ClientConn *client);
void handle_acl_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_acl_help_readable(Worker *w, PoolConn *conn);

/* ── RESP3 (HELLO command + per-connection upgrade handshake) ──────────
 * See src/hello.c. */
int resp3_conn_needs_upgrade(PoolConn *conn);
/* Both return 0 on success, -1 on allocation failure (treat exactly like any
 * other OOM at that call site -- see hello.c for why this can't block). */
int resp3_start_conn_upgrade(Worker *w, PoolConn *conn);
int resp3_start_probe(Worker *w, PoolConn *conn);
int resp3_defer_append(PoolConn *conn, const char *data, int len);
void handle_resp3_handshake_readable(Worker *w, PoolConn *conn);
void handle_hello_cmd(Worker *w, ClientConn *client, int cmdlen);
void handle_hello_auth_reply(Worker *w, PoolConn *conn, ClientConn *cl, int rlen);
void hello_auth_conn_forget(ClientConn *client);

#endif /* PROXY_H */
