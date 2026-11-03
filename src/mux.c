#include "proxy.h"

/* ── backend multiplexing ────────────────────────────────────────────────
 *
 * With backend_multiplexing on, single-key commands and fan-out commands
 * (MGET/MSET/DEL/...) don't check a connection out of a pool for the length
 * of one request. Instead each (db, node) has a fixed set of POOL_SHARED
 * connections, and every request -- from any client, any number at a time --
 * is appended to one of them. Valkey answers the requests on a connection in
 * the order they were written, so each connection keeps a FIFO of what it is
 * waiting for (PoolConn.mq) and hands each complete reply to the head entry.
 *
 * The connection is picked by slot % pool size, never by load: every request
 * for a given key goes down the same connection, so commands on one key run
 * in the order the proxy received them even across clients and across a
 * single-key command and a fan-out sub-request.
 *
 * On the client side, each multiplexed command gets a Req, queued on the
 * client in command order (ClientConn.req_head). Replies may arrive out of
 * order across connections; they are written to the client strictly in queue
 * order as the head completes. Any command that can't be multiplexed waits
 * as a barrier (see client_process_input() in client.c) until everything
 * before it has been written out, then runs through the original one-at-a-
 * time code path unchanged.
 */

/* Stop reading a client's commands once this much reply data is waiting to
 * be written to it -- it isn't reading as fast as it is sending. */
#define MUX_MAX_PENDING_OUTPUT (4 * 1024 * 1024)

static inline void mux_stat_add(atomic_ullong *s, unsigned long long n) {
    atomic_store_explicit(s, atomic_load_explicit(s, memory_order_relaxed) + n, memory_order_relaxed);
}

/* open = connections currently connected; in_use = requests in flight. */
static void mux_stats_publish(ConnPool *p) {
    int open = 0;
    for (int i = 0; i < p->count; i++)
        if (p->conns[i].fd >= 0) open++;
    atomic_store_explicit(&p->stats.open, open, memory_order_relaxed);
    atomic_store_explicit(&p->stats.in_use, p->in_use, memory_order_relaxed);
}

/* fd_watch() costs a syscall every time; with many requests or replies
 * per event-loop pass, only the first one needs to arm EL_WRITABLE. */
static void mux_watch_writable(Worker *w, int fd) {
    if (fd >= 0 && fd < g_config.max_connection && !(w->fd_mask[fd] & EL_WRITABLE)) fd_watch(w, fd, EL_WRITABLE);
}

/* ── shared connections ─────────────────────────────────────────────── */
static int mux_conn_connect(Worker *w, PoolConn *c) {
    uint64_t now = mono_us();
    if (now < c->mux_retry_us) return -1;
    int fd = pool_new_fd(w, c->pool);
    if (fd < 0) {
        c->mux_retry_us = now + (uint64_t)g_config.backend_connect_timeout_ms * 1000ULL;
        return -1;
    }
    c->fd = fd;
    c->db = c->pool->db;
    c->resp3 = 0;
    c->resp3_handshake = 0;
    if (fd < g_config.max_connection) w->backend_map[fd] = c;
    mux_stats_publish(c->pool);
    return 0;
}

void mux_pool_init(Worker *w, ConnPool *p) {
    p->max = g_pool_max[POOL_SHARED];
    if (p->max <= 0) return; /* multiplexing off */
    p->conns = calloc((size_t)p->max, sizeof(PoolConn));
    if (!p->conns) {
        p->max = 0;
        return;
    }
    /* Every slot exists from the start, connected or not, so slot % count
     * always names the same connection. */
    p->count = p->max;
    for (int i = 0; i < p->count; i++) {
        p->conns[i].fd = -1;
        p->conns[i].pool = p;
        p->conns[i].db = p->db;
        p->conns[i].state = CONN_IN_USE;
    }
    atomic_store_explicit(&p->stats.live, 1, memory_order_release);
    int ok = 0;
    for (int i = 0; i < p->count; i++)
        if (mux_conn_connect(w, &p->conns[i]) == 0) ok++;
    printf("      %-14s pool: %d/%d connections ready\n", POOL_NAME[POOL_SHARED], ok, p->max);
}

/* NULL if the connection is down and can't be reconnected right now. */
static PoolConn *mux_conn_for(Worker *w, int db, int node_idx, int slot) {
    DbGroup *grp = db_group_get(w, db);
    if (!grp) return NULL;
    ConnPool *p = &grp->node_pools[node_idx].pools[POOL_SHARED];
    if (p->count <= 0) return NULL;
    PoolConn *c = &p->conns[slot % p->count];
    if (c->fd < 0 && mux_conn_connect(w, c) < 0) return NULL;
    return c;
}

static int mux_wbuf_append(PoolConn *c, const char *buf, int len) {
    if (c->wbuf_off > 0 && c->wbuf_off == c->wbuf_len) c->wbuf_off = c->wbuf_len = 0;
    if (c->wbuf_cap - c->wbuf_len < len) {
        int nc = c->wbuf_cap ? c->wbuf_cap : 4096;
        while (nc - c->wbuf_len < len) nc *= 2;
        char *tmp = realloc(c->wbuf, (size_t)nc);
        if (!tmp) return -1;
        c->wbuf = tmp;
        c->wbuf_cap = nc;
    }
    memcpy(c->wbuf + c->wbuf_len, buf, (size_t)len);
    c->wbuf_len += len;
    return 0;
}

static int mux_mq_push(PoolConn *c, MuxEntry e) {
    if (c->mq_len == c->mq_cap) {
        int nc = c->mq_cap ? c->mq_cap * 2 : 16;
        MuxEntry *q = malloc((size_t)nc * sizeof(MuxEntry));
        if (!q) return -1;
        for (int i = 0; i < c->mq_len; i++) q[i] = c->mq[(c->mq_head + i) % c->mq_cap];
        free(c->mq);
        c->mq = q;
        c->mq_cap = nc;
        c->mq_head = 0;
    }
    c->mq[(c->mq_head + c->mq_len) % c->mq_cap] = e;
    c->mq_len++;
    c->pool->in_use++;
    return 0;
}

static MuxEntry mux_mq_pop(PoolConn *c) {
    MuxEntry e = c->mq[c->mq_head];
    c->mq_head = (c->mq_head + 1) % c->mq_cap;
    c->mq_len--;
    c->pool->in_use--;
    return e;
}

/* Queues buf as the next request on c, preceded by a HELLO 3 handshake the
 * first time c is used after backends went RESP3 (see hello.c) -- written
 * ahead of the request on the same connection, so everything queued from
 * then on is answered in RESP3. Fills e->backend_resp3. -1 on allocation
 * failure, with nothing queued. */
static int mux_write(Worker *w, PoolConn *c, MuxEntry *e, const char *buf, int len) {
    static const char HELLO3[] = "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
    if (c->wbuf_off > 0 && c->wbuf_off == c->wbuf_len) c->wbuf_off = c->wbuf_len = 0;
    int queued_hello = 0;
    if (resp3_conn_needs_upgrade(c)) {
        MuxEntry hello = {.internal = 1};
        int mark = c->wbuf_len;
        if (mux_wbuf_append(c, HELLO3, (int)sizeof(HELLO3) - 1) < 0) return -1;
        if (mux_mq_push(c, hello) < 0) {
            c->wbuf_len = mark;
            return -1;
        }
        c->resp3_handshake = 1;
        queued_hello = 1;
    }
    e->backend_resp3 = c->resp3 || c->resp3_handshake;
    int mark = c->wbuf_len;
    int rc = 0;
    if (mux_wbuf_append(c, buf, len) < 0 || mux_mq_push(c, *e) < 0) {
        c->wbuf_len = mark; /* a HELLO 3 queued above stays: its reply is due */
        rc = -1;
    } else {
        mux_stat_add(&c->pool->stats.acquires, 1);
        atomic_store_explicit(&c->pool->stats.in_use, c->pool->in_use, memory_order_relaxed);
    }
    if (rc == 0 || queued_hello) mux_watch_writable(w, c->fd);
    return rc;
}

static void mux_entry_fail(Worker *w, MuxEntry *e, const char *msg) {
    if (e->req) {
        char line[128];
        int n = snprintf(line, sizeof(line), "-ERR %s\r\n", msg);
        mux_req_complete(w, e->req, line, n);
    } else if (e->sub) {
        fanout_sub_complete(w, e->sub, NULL, 0, 0);
    }
}

/* c's protocol state is gone (I/O error, EOF, unparsable reply): close it
 * and fail everything in flight on it. The slot reconnects on next use. */
static void mux_conn_fail(Worker *w, PoolConn *c, const char *msg) {
    ConnPool *p = c->pool;
    const ClusterNode *nd = &w->topo.nodes[p->node_idx];
    if (c->fd >= 0) {
        printf("[worker %d] [pool:%s] destroy  fd=%-4d  node=%s:%d  db=%d  in-flight=%d  (%s)\n", w->thread_id,
               POOL_NAME[p->type], c->fd, nd->ip, nd->port, c->db, c->mq_len, msg);
        fd_unwatch(w, c->fd, EL_READABLE | EL_WRITABLE);
        if (c->fd < g_config.max_connection) w->backend_map[c->fd] = NULL;
        close(c->fd);
        c->fd = -1;
        mux_stat_add(&p->stats.destroys, 1);
    }
    c->wbuf_len = c->wbuf_off = 0;
    c->rbuf_len = 0;
    c->resp3 = 0;
    c->resp3_handshake = 0;
    /* Reconnect on next use straight away -- only a failed reconnect backs
     * off (see mux_conn_connect()), so an idle connection the backend
     * closed doesn't fail the next requests to a healthy node. */
    c->mux_retry_us = 0;

    /* Detach the queue first: failing an entry can complete a fan-out, whose
     * reply can make its client queue more -- onto a fresh queue. */
    MuxEntry *q = c->mq;
    int head = c->mq_head, len = c->mq_len, cap = c->mq_cap;
    c->mq = NULL;
    c->mq_head = c->mq_len = c->mq_cap = 0;
    p->in_use -= len;
    mux_stats_publish(p);
    for (int i = 0; i < len; i++) mux_entry_fail(w, &q[(head + i) % cap], msg);
    free(q);
}

void mux_pool_reset(Worker *w, ConnPool *p, int reconnect, const char *msg) {
    for (int i = 0; i < p->count; i++) {
        PoolConn *c = &p->conns[i];
        if (c->fd >= 0 || c->mq_len > 0) mux_conn_fail(w, c, msg);
        c->mux_retry_us = 0;
        if (reconnect) mux_conn_connect(w, c);
    }
}

void mux_conn_writable(Worker *w, PoolConn *c) {
    while (c->wbuf_off < c->wbuf_len) {
        ssize_t n = write(c->fd, c->wbuf + c->wbuf_off, (size_t)(c->wbuf_len - c->wbuf_off));
        if (n < 0) {
            if (errno == EAGAIN) return;
            mux_conn_fail(w, c, "backend connection error");
            return;
        }
        c->wbuf_off += (int)n;
    }
    c->wbuf_off = c->wbuf_len = 0;
    fd_unwatch(w, c->fd, EL_WRITABLE);
}

/* Hands one complete reply to the entry it answers. Returns -1 if the reply
 * means c itself must be dropped (its HELLO 3 was refused). */
static int mux_entry_deliver(Worker *w, PoolConn *c, MuxEntry *e, const char *buf, int len) {
    if (e->internal) {
        if (buf[0] == '-') return -1;
        c->resp3 = 1;
        c->resp3_handshake = 0;
        return 0;
    }
    if (e->sub) {
        fanout_sub_complete(w, e->sub, buf, len, e->backend_resp3);
        return 0;
    }
    Req *req = e->req;
    if (req->client && req->client_resp == 2 && e->backend_resp3) {
        char *out;
        int out_len;
        if (resp3_downgrade_reply(buf, len, req->nil_array_depth, req->pair_transform, &out, &out_len) < 0) {
            static const char BAD[] = "-ERR bad backend response\r\n";
            mux_req_complete(w, req, BAD, (int)sizeof(BAD) - 1);
            return 0;
        }
        mux_req_complete(w, req, out, out_len);
        free(out);
        return 0;
    }
    mux_req_complete(w, req, buf, len);
    return 0;
}

void mux_conn_readable(Worker *w, PoolConn *c) {
    /* Edge-triggered: drain the socket, matching each complete reply to the
     * head of the queue as soon as it is in. */
    for (;;) {
        if (c->rbuf_cap - c->rbuf_len < 4096) {
            int nc = c->rbuf_cap ? c->rbuf_cap * 2 : 16384;
            if (nc > MAX_RESP_SIZE) {
                mux_conn_fail(w, c, "response too large");
                return;
            }
            char *tmp = realloc(c->rbuf, (size_t)nc);
            if (!tmp) {
                mux_conn_fail(w, c, "out of memory");
                return;
            }
            c->rbuf = tmp;
            c->rbuf_cap = nc;
        }
        ssize_t n = read(c->fd, c->rbuf + c->rbuf_len, (size_t)(c->rbuf_cap - c->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            mux_conn_fail(w, c, "backend connection error");
            return;
        }
        if (n == 0) {
            mux_conn_fail(w, c, "backend connection closed");
            return;
        }
        c->rbuf_len += (int)n;

        int off = 0;
        while (off < c->rbuf_len) {
            int rlen = resp_response_len(c->rbuf + off, c->rbuf_len - off);
            if (rlen == 0) break;
            if (rlen < 0) {
                mux_conn_fail(w, c, "bad backend response");
                return;
            }
            if (c->mq_len == 0) {
                mux_conn_fail(w, c, "unexpected backend reply");
                return;
            }
            MuxEntry e = mux_mq_pop(c);
            if (mux_entry_deliver(w, c, &e, c->rbuf + off, rlen) < 0) {
                mux_conn_fail(w, c, "backend refused HELLO 3");
                return;
            }
            off += rlen;
        }
        if (off > 0) {
            memmove(c->rbuf, c->rbuf + off, (size_t)(c->rbuf_len - off));
            c->rbuf_len -= off;
            atomic_store_explicit(&c->pool->stats.in_use, c->pool->in_use, memory_order_relaxed);
        }
    }
}

/* ── client side ────────────────────────────────────────────────────── */
static const char *MUX_SPECIAL_KEY_CMDS[] = {"MEMORY", "OBJECT", "XGROUP", "XINFO", NULL};

/* Whether cmd can go out over a shared connection. Side-effect free: called
 * before route_command(), since a barrier re-parses the command later. */
int client_can_mux(const ClientConn *client, const char *cmd, int total_cmd_len) {
    if (!g_config.backend_multiplexing) return 0;
    if (client->acl_authenticated || client->in_transaction || client->in_subscribe || client->fanout_ctx ||
        client->sub_fence_pending)
        return 0;
    if (!cmd_is_supported(cmd)) return 0;
    PoolType pt = cmd_get_pool_type(cmd);
    if (pt != POOL_NORMAL) return 0;
    CmdClass cls = classify_command(cmd, pt);
    /* Whole command must fit in hbuf: the streaming path for bigger ones
     * hands the client's socket straight through to one connection. */
    if (cls == CMD_CLASS_SINGLE_KEY)
        return total_cmd_len <= HDR_BUF_SIZE && !cmd_in_list(cmd, MUX_SPECIAL_KEY_CMDS);
    if (cls == CMD_CLASS_MULTI_KEY) return cmd_is_fanout(cmd);
    return 0;
}

static int mux_pending_output(const ClientConn *client) {
    return client->state == CS_WRITING_RESP ? 0 : client->wbuf_len - client->wbuf_off;
}

/* Some multiplexed reply is still owed to client or not yet written out --
 * anything sent to it now must be queued behind that. */
int client_mux_busy(const ClientConn *client) {
    return client->req_head != NULL || mux_pending_output(client) > 0;
}

int mux_client_output_full(const ClientConn *client) {
    return client->req_count >= g_config.max_pipeline_inflight || mux_pending_output(client) > MUX_MAX_PENDING_OUTPUT;
}

Req *mux_req_new(ClientConn *client) {
    Req *req = calloc(1, sizeof(Req));
    if (!req) return NULL;
    req->client = client;
    req->client_resp = client->resp;
    req->nil_array_depth = cmd_resp2_nil_array_depth(client->last_cmd);
    req->pair_transform = cmd_resp3_pair_transform(client->last_cmd);
    if (client->req_tail)
        client->req_tail->next = req;
    else
        client->req_head = req;
    client->req_tail = req;
    client->req_count++;
    return req;
}

/* A reply-less error for a client with replies still owed: queued in order
 * rather than written straight to the socket. */
void mux_queue_error(ClientConn *client, const char *line, int len) {
    Req *req = mux_req_new(client);
    if (!req) return;
    req->reply = malloc((size_t)len);
    if (req->reply) {
        memcpy(req->reply, line, (size_t)len);
        req->reply_len = len;
    }
    req->done = 1;
}

static int client_wbuf_append(ClientConn *cl, const char *buf, int len) {
    if (cl->wbuf_off > 0 && cl->wbuf_off == cl->wbuf_len) cl->wbuf_off = cl->wbuf_len = 0;
    if (cl->wbuf_cap - cl->wbuf_len < len) {
        if (cl->wbuf_off > 0) {
            memmove(cl->wbuf, cl->wbuf + cl->wbuf_off, (size_t)(cl->wbuf_len - cl->wbuf_off));
            cl->wbuf_len -= cl->wbuf_off;
            cl->wbuf_off = 0;
        }
        if (cl->wbuf_cap - cl->wbuf_len < len) {
            int nc = cl->wbuf_cap ? cl->wbuf_cap : 4096;
            while (nc - cl->wbuf_len < len) nc *= 2;
            char *tmp = realloc(cl->wbuf, (size_t)nc);
            if (!tmp) return -1;
            cl->wbuf = tmp;
            cl->wbuf_cap = nc;
        }
    }
    memcpy(cl->wbuf + cl->wbuf_len, buf, (size_t)len);
    cl->wbuf_len += len;
    return 0;
}

static void mux_req_free(Req *req) {
    free(req->reply);
    free(req);
}

/* After the queue or the output backlog shrank: lift a barrier or throttle
 * that was waiting for exactly that. */
static void mux_client_resume(Worker *w, ClientConn *cl) {
    if (cl->mux_barrier && !client_mux_busy(cl)) {
        cl->mux_barrier = 0;
        fd_watch(w, cl->fd, EL_READABLE);
    }
    if (cl->mux_throttled && !mux_client_output_full(cl)) {
        cl->mux_throttled = 0;
        fd_watch(w, cl->fd, EL_READABLE);
    }
}

/* Moves every completed reply at the head of cl's queue into wbuf. Returns
 * -1 if cl was closed. */
static int mux_client_advance(Worker *w, ClientConn *cl) {
    int moved = 0;
    while (cl->req_head && cl->req_head->done) {
        Req *req = cl->req_head;
        cl->req_head = req->next;
        if (!cl->req_head) cl->req_tail = NULL;
        cl->req_count--;
        static const char OOM[] = "-ERR out of memory\r\n";
        int rc = req->reply ? client_wbuf_append(cl, req->reply, req->reply_len)
                            : client_wbuf_append(cl, OOM, (int)sizeof(OOM) - 1);
        mux_req_free(req);
        if (rc < 0) {
            close_client(w, cl);
            return -1;
        }
        moved = 1;
    }
    if (moved) mux_watch_writable(w, cl->fd);
    mux_client_resume(w, cl);
    return 0;
}

void mux_req_complete(Worker *w, Req *req, const char *buf, int len) {
    ClientConn *cl = req->client;
    if (!cl) {
        /* Orphaned by close_client(): nobody to answer. */
        mux_req_free(req);
        return;
    }
    if (req == cl->req_head) {
        /* The common case: straight into wbuf, no copy. */
        cl->req_head = req->next;
        if (!cl->req_head) cl->req_tail = NULL;
        cl->req_count--;
        mux_req_free(req);
        if (client_wbuf_append(cl, buf, len) < 0) {
            close_client(w, cl);
            return;
        }
        mux_watch_writable(w, cl->fd);
        mux_client_advance(w, cl);
        return;
    }
    req->reply = malloc((size_t)len);
    if (req->reply) {
        memcpy(req->reply, buf, (size_t)len);
        req->reply_len = len;
    }
    req->done = 1;
}

/* The command just dispatched is out of the client's input buffers. */
void mux_input_done(Worker *w, ClientConn *client) {
    (void)w;
    client->hbuf_len = 0;
    client->pbuf_len = 0;
    client->using_pbuf = 0;
    client->mux_cmd = 0;
    client->state = CS_READING_HDR;
}

/* Sends buf (a whole command) for client over the shared connection slot
 * maps to on node_idx. Failures become an error reply in the command's
 * place. Returns -1 if client was closed. */
int mux_send_cmd(Worker *w, ClientConn *client, int node_idx, int slot, const char *buf, int len) {
    Req *req = mux_req_new(client);
    if (!req) {
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return -1;
    }
    PoolConn *c = mux_conn_for(w, client->db, node_idx, slot);
    MuxEntry e = {.req = req};
    const char *err = !c ? "-ERR backend connection error\r\n"
                      : mux_write(w, c, &e, buf, len) < 0 ? "-ERR out of memory\r\n"
                                                          : NULL;
    if (err) {
        int fd = client->fd;
        mux_req_complete(w, req, err, (int)strlen(err));
        if (w->client_map[fd] != client) return -1;
    }
    return 0;
}

/* Sends a fan-out sub-request over the shared connection for sub->slot.
 * -1 if it couldn't be sent (the caller fails the sub). */
int mux_send_sub(Worker *w, FanoutSub *sub, int db, const char *buf, int len) {
    PoolConn *c = mux_conn_for(w, db, sub->node_idx, sub->slot);
    if (!c) return -1;
    MuxEntry e = {.sub = sub};
    return mux_write(w, c, &e, buf, len);
}

/* handle_client_writable() for a client whose wbuf holds multiplexed
 * replies: unlike the one-command-at-a-time path, the client keeps reading
 * meanwhile, so this must leave state and hbuf alone. */
void mux_client_flush(Worker *w, ClientConn *client) {
    while (client->wbuf_off < client->wbuf_len) {
        ssize_t n = write(client->fd, client->wbuf + client->wbuf_off, (size_t)(client->wbuf_len - client->wbuf_off));
        if (n < 0) {
            if (errno == EAGAIN) return;
            close_client(w, client);
            return;
        }
        client->wbuf_off += (int)n;
    }
    client->wbuf_off = client->wbuf_len = 0;
    fd_unwatch(w, client->fd, EL_WRITABLE);
    mux_client_resume(w, client);
}

/* close_client(): requests still in flight stay owned by their connection
 * (or fan-out) and are freed when their reply arrives. */
void mux_client_release(ClientConn *client) {
    Req *req = client->req_head;
    while (req) {
        Req *next = req->next;
        if (req->done) {
            mux_req_free(req);
        } else {
            req->client = NULL;
            req->next = NULL;
            if (req->fanout) req->fanout->client = NULL;
        }
        req = next;
    }
    client->req_head = client->req_tail = NULL;
    client->req_count = 0;
}
