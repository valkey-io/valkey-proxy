#include "proxy.h"

/* ── Transactions: MULTI / EXEC / DISCARD / WATCH / UNWATCH ─────────────
 *
 * A transaction lives on one backend connection, pinned to the client in
 * client->txn_conn (conn->txn_client points back) from its first WATCH or
 * first forwarded queued command until EXEC / DISCARD / UNWATCH ends it.
 * Default-identity clients pin a POOL_TRANSACTION connection; ACL users pin
 * their own acl_conns[node] (see acl.c), which stays theirs afterwards.
 *
 * As on a Valkey cluster, the whole transaction is bound to one slot: the
 * first key used (by WATCH or by a queued command) picks the slot and its
 * node, and any other slot is answered with CROSSSLOT. Keyless commands
 * that only make sense on one node (PING / ECHO / TIME / UNWATCH, EVAL and
 * FCALL with no keys) are queued once that node is known; everything else
 * keyless is rejected. A command the proxy rejects inside MULTI fails the
 * transaction (txn_dirty): EXEC then answers -EXECABORT.
 *
 * MULTI itself is answered here and sent to the backend only just before
 * the first command that needs it, as a request of its own: were it
 * pipelined with that command and refused (e.g. NOPERM), the command would
 * run outside any transaction. Once the node is picked, commands go to it
 * whatever the proxy's own slot map says later; a slot that has moved
 * meanwhile comes back as -MOVED, and a transaction without WATCH is then
 * replayed on the new node from the queued commands' bytes kept in txn_buf
 * (up to transaction_replay_buffer_size). With WATCH it can't be: the
 * WATCH is gone with the old node, so EXEC answers nil and the client
 * retries, as it would after any WATCH abort.
 *
 * Losing the pinned connection (backend error, topology change) is noticed
 * in pool_destroy_conn() via txn_conn_gone(): a WATCH on it is lost
 * (txn_watch_lost, EXEC answers nil), a MULTI on it is failed (txn_dirty).
 */

static const char RE_OK[] = "+OK\r\n";
static const char RE_QUEUED[] = "+QUEUED\r\n";
static const char RE_EMPTY[] = "*0\r\n";
static const char RE_EXECABORT[] = "-EXECABORT Transaction discarded because of previous errors.\r\n";
static const char RE_NOT_ALLOWED[] = "-ERR Command not allowed inside a transaction\r\n";
static const char RE_NO_NODE[] =
    "-ERR Command without a key can't come before the first keyed command of a transaction\r\n";
static const char RE_CROSSSLOT[] = "-CROSSSLOT Keys in request don't hash to the same slot\r\n";
static const char RE_UNMAPPED[] = "-ERR slot not mapped\r\n";

static const char CMD_MULTI[] = "*1\r\n$5\r\nMULTI\r\n";
static const char CMD_EXEC[] = "*1\r\n$4\r\nEXEC\r\n";
static const char CMD_DISCARD[] = "*1\r\n$7\r\nDISCARD\r\n";
static const char CMD_UNWATCH[] = "*1\r\n$7\r\nUNWATCH\r\n";

#define TXN_MAX_REDIRECTS 2

/* Keyless commands queued on the transaction's node (see the top). */
static const char *TXN_KEYLESS_CMDS[] = {"ECHO", "PING", "TIME", NULL};
/* Single-key commands whose key isn't their first argument. */
static const char *TXN_SUBCMD_KEY_CMDS[] = {"MEMORY", "OBJECT", "XGROUP", "XINFO", NULL};

static int is_moved(const char *r, int len) {
    return len > 7 && !memcmp(r, "-MOVED ", 7);
}

static int is_queued(const char *r, int len) {
    return len >= 7 && !memcmp(r, "+QUEUED", 7);
}

static void nil_reply(const ClientConn *cl, const char **r, int *len) {
    *r = cl->resp == 3 ? "_\r\n" : "*-1\r\n";
    *len = (int)strlen(*r);
}

/* ── state ──────────────────────────────────────────────────────────── */

/* EXEC / DISCARD: the transaction is over as far as the client can tell. */
static void txn_end_client_state(ClientConn *cl) {
    cl->in_transaction = 0;
    cl->multi_count = -1;
    cl->watch_count = 0;
    cl->txn_dirty = 0;
    cl->txn_watch_lost = 0;
}

/* Once nothing is pinned, queued or watched any more, forget the slot and
 * what was kept for EXEC's reply and for replays. */
static void txn_maybe_finish(ClientConn *cl) {
    if (cl->txn_conn || cl->in_transaction || cl->watch_count > 0 || cl->txn_watch_lost || cl->txn_op != TXN_OP_NONE)
        return;
    cl->txn_slot = cl->txn_node = -1;
    cl->txn_ncmds = 0;
    cl->txn_buf_len = cl->txn_cur_start = 0;
    cl->txn_replay_off = 0;
    cl->txn_redirects = 0;
    cl->txn_had_watch = 0;
}

/* A new MULTI starts with an empty queue. */
static void txn_reset_queue(ClientConn *cl) {
    cl->txn_ncmds = 0;
    cl->txn_buf_len = cl->txn_cur_start = 0;
    cl->txn_replay_off = 0;
    cl->txn_redirects = 0;
}

static void txn_push_name(ClientConn *cl) {
    if (cl->txn_ncmds == cl->txn_cmds_cap) {
        int nc = cl->txn_cmds_cap ? cl->txn_cmds_cap * 2 : 16;
        char(*tmp)[24] = realloc(cl->txn_cmds, (size_t)nc * sizeof(*tmp));
        if (!tmp) return; /* only costs the per-element RESP2 downgrade */
        cl->txn_cmds = tmp;
        cl->txn_cmds_cap = nc;
    }
    memcpy(cl->txn_cmds[cl->txn_ncmds++], cl->txn_cur_name, sizeof(cl->txn_cur_name));
}

/* Lets go of the pinned connection: back to its pool (clean = no MULTI or
 * WATCH left on it), or closed. An ACL user's connection stays in its
 * acl_conns[] when clean. */
static void txn_unbind(Worker *w, ClientConn *cl, int clean) {
    PoolConn *c = cl->txn_conn;
    if (!c) return;
    cl->txn_conn = NULL;
    c->txn_client = NULL;
    cl->txn_multi_sent = 0;
    if (cl->pending_conn == c) cl->pending_conn = NULL;
    if (c->pool->type == POOL_ACL) {
        if (clean) {
            fd_unwatch(w, c->fd, EL_READABLE);
            return;
        }
        int ni = c->pool->node_idx;
        if (cl->acl_conns[ni] == c) cl->acl_conns[ni] = NULL;
        c->waiting_client = NULL;
        pool_destroy_conn(w, c);
        return;
    }
    c->waiting_client = NULL;
    fd_unwatch(w, c->fd, EL_READABLE);
    if (clean)
        pool_release(w, c);
    else
        pool_destroy_conn(w, c);
}

/* After a reply: the pinned connection stays, idle. */
static void txn_conn_idle(Worker *w, ClientConn *cl, PoolConn *conn) {
    conn->rbuf_len = 0;
    cl->pending_conn = NULL;
    cl->txn_op = TXN_OP_NONE;
    if (conn->pool->type == POOL_ACL) {
        /* ACL convention (see acl.c): waiting_client stays, reads are off. */
        fd_unwatch(w, conn->fd, EL_READABLE);
    } else {
        /* Keep reading: an EOF on the idle connection then destroys it (see
         * handle_backend_readable()), and txn_conn_gone() records the loss. */
        conn->waiting_client = NULL;
    }
}

/* ── hooks called from the rest of the proxy ─────────────────────────── */

void txn_on_conn(ClientConn *cl, PoolConn *conn) {
    if (cl->txn_op == TXN_OP_NONE || cl->txn_conn == conn) return;
    if (conn->pool->type != POOL_TRANSACTION && conn->pool->type != POOL_ACL) return;
    if (cl->txn_conn) cl->txn_conn->txn_client = NULL;
    cl->txn_conn = conn;
    conn->txn_client = cl;
}

/* Puts buf[0..len) on conn as the next request, the client waiting on it. */
static int txn_write(Worker *w, ClientConn *cl, PoolConn *conn, const char *buf, int len) {
    if (conn->wbuf_cap < len) {
        char *tmp = realloc(conn->wbuf, (size_t)len);
        if (!tmp) {
            client_send_error(w, cl, "out of memory");
            close_client(w, cl);
            return -1;
        }
        conn->wbuf = tmp;
        conn->wbuf_cap = len;
    }
    memcpy(conn->wbuf, buf, (size_t)len);
    conn->wbuf_len = len;
    conn->wbuf_off = 0;
    conn->waiting_client = cl;
    cl->pending_conn = conn;
    cl->state = CS_WAITING_RESP;
    fd_unwatch(w, cl->fd, EL_READABLE);
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
    fd_watch(w, conn->fd, EL_WRITABLE);
    return 0;
}

int txn_before_send(Worker *w, ClientConn *cl, PoolConn *conn) {
    if (cl->txn_conn != conn) return 0;
    if (cl->txn_op == TXN_OP_REPLAY_MULTI) {
        txn_write(w, cl, conn, CMD_MULTI, (int)sizeof(CMD_MULTI) - 1);
        return 1;
    }
    if ((cl->txn_op == TXN_OP_QUEUE || cl->txn_op == TXN_OP_EXEC) && !cl->txn_multi_sent) {
        cl->txn_saved_op = cl->txn_op;
        cl->txn_op = TXN_OP_MULTI_PRE;
        txn_write(w, cl, conn, CMD_MULTI, (int)sizeof(CMD_MULTI) - 1);
        return 1;
    }
    return 0;
}

void txn_record(ClientConn *cl, const char *buf, int len) {
    if (cl->txn_op != TXN_OP_QUEUE || cl->txn_replay_off || !cl->txn_conn || cl->pending_conn != cl->txn_conn) return;
    if ((long long)cl->txn_buf_len + len > g_config.transaction_replay_buffer_size) {
        cl->txn_replay_off = 1;
        cl->txn_buf_len = cl->txn_cur_start = 0;
        return;
    }
    if (cl->txn_buf_cap < cl->txn_buf_len + len) {
        int nc = cl->txn_buf_cap ? cl->txn_buf_cap : 4096;
        while (nc < cl->txn_buf_len + len) nc *= 2;
        char *tmp = realloc(cl->txn_buf, (size_t)nc);
        if (!tmp) {
            cl->txn_replay_off = 1;
            return;
        }
        cl->txn_buf = tmp;
        cl->txn_buf_cap = nc;
    }
    memcpy(cl->txn_buf + cl->txn_buf_len, buf, (size_t)len);
    cl->txn_buf_len += len;
}

/* The op whose outcome the client is waiting for: a deferred or replayed
 * MULTI is only on its way to it. */
static int txn_effective_op(const ClientConn *cl) {
    if (cl->txn_op == TXN_OP_MULTI_PRE || cl->txn_op == TXN_OP_REPLAY_MULTI || cl->txn_op == TXN_OP_REPLAY)
        return cl->txn_saved_op;
    return cl->txn_op;
}

void txn_conn_gone(ClientConn *cl, PoolConn *conn) {
    cl->txn_conn = NULL;
    if (conn->pool->type == POOL_ACL && cl->acl_conns[conn->pool->node_idx] == conn)
        cl->acl_conns[conn->pool->node_idx] = NULL;
    if (cl->watch_count > 0) {
        cl->txn_watch_lost = 1;
        cl->watch_count = 0;
    }
    int op = txn_effective_op(cl);
    if (cl->in_transaction && (cl->txn_multi_sent || op == TXN_OP_QUEUE)) cl->txn_dirty = 1;
    if (op == TXN_OP_QUEUE) cl->txn_buf_len = cl->txn_cur_start;
    cl->txn_multi_sent = 0;
    cl->txn_op = TXN_OP_NONE;
    txn_maybe_finish(cl);
}

void txn_cmd_failed(Worker *w, ClientConn *cl) {
    int op = txn_effective_op(cl);
    if (cl->txn_op == TXN_OP_NONE) return;
    cl->txn_op = TXN_OP_NONE;
    if (op == TXN_OP_QUEUE) {
        if (cl->in_transaction) cl->txn_dirty = 1;
        cl->txn_buf_len = cl->txn_cur_start;
    }
    PoolConn *c = cl->txn_conn;
    if (c) {
        if (op == TXN_OP_EXEC || op == TXN_OP_DISCARD || op == TXN_OP_UNWATCH || op == TXN_OP_CLEANUP) {
            /* What's left on it is unknown: don't hand it to anyone else. */
            txn_unbind(w, cl, 0);
        } else {
            if (c->pool->type != POOL_ACL && c->waiting_client == cl) c->waiting_client = NULL;
            if (cl->pending_conn == c) cl->pending_conn = NULL;
        }
    }
    txn_maybe_finish(cl);
}

void txn_reconcile(Worker *w, ClientConn *cl) {
    if (cl->txn_op == TXN_OP_NONE) return;
    printf("[worker %d] client-fd=%d: transaction request left over (op=%d), failing it\n", w->thread_id, cl->fd,
           cl->txn_op);
    txn_cmd_failed(w, cl);
}

void txn_identity_changed(Worker *w, ClientConn *cl) {
    PoolConn *c = cl->txn_conn;
    if (!c || c->pool->type != POOL_TRANSACTION) return;
    if (cl->pending_conn == c) cl->pending_conn = NULL;
    c->waiting_client = NULL;
    pool_destroy_conn(w, c); /* txn_conn_gone() records the lost WATCH / MULTI */
}

void txn_client_free(Worker *w, ClientConn *cl) {
    PoolConn *c = cl->txn_conn;
    if (c && c->pool->type == POOL_TRANSACTION) {
        if (cl->pending_conn == c) cl->pending_conn = NULL;
        c->waiting_client = NULL;
        pool_destroy_conn(w, c);
    }
    /* An ACL user's pinned connection goes with acl_teardown_all(). */
    free(cl->txn_cmds);
    cl->txn_cmds = NULL;
    cl->txn_ncmds = cl->txn_cmds_cap = 0;
    free(cl->txn_buf);
    cl->txn_buf = NULL;
    cl->txn_buf_len = cl->txn_buf_cap = 0;
}

/* A topology change moved the pinned connection's node identity on (see
 * worker_sync_topology()): it can't carry this transaction any more. */
static void txn_check_conn(Worker *w, ClientConn *cl) {
    PoolConn *c = cl->txn_conn;
    if (!c || !c->discard_on_release) return;
    printf("[worker %d] client-fd=%d: transaction connection fd=%d lost to a topology change\n", w->thread_id, cl->fd,
           c->fd);
    if (cl->pending_conn == c) cl->pending_conn = NULL;
    if (c->pool->type == POOL_ACL) {
        if (cl->acl_conns[c->pool->node_idx] == c) cl->acl_conns[c->pool->node_idx] = NULL;
    }
    c->waiting_client = NULL;
    pool_destroy_conn(w, c);
}

/* ── sending ────────────────────────────────────────────────────────── */

/* Sends the client's current command (hbuf/pbuf) for cl->txn_op on the
 * pinned connection, or pins one on cl->txn_node first. */
static void txn_send_cmd(Worker *w, ClientConn *cl) {
    cl->node_idx = cl->txn_node;
    PoolConn *conn = cl->txn_conn;
    if (conn) {
        conn->waiting_client = cl;
        cl->pending_conn = conn;
        resume_client_after_conn(w, cl, conn);
        return;
    }
    if (cl->acl_authenticated) {
        /* Lazily AUTHs a connection to that node if there's none yet; once
         * it's ready, resume_client_after_conn() pins it (txn_on_conn()). */
        int fd = cl->fd;
        if (!acl_route_business_cmd(w, cl, cl->txn_node) && w->client_map[fd] == cl) txn_cmd_failed(w, cl);
        return;
    }
    DbGroup *grp = db_group_get(w, cl->db);
    if (!grp) {
        client_send_error(w, cl, "db group error");
        close_client(w, cl);
        return;
    }
    ConnPool *pool = &grp->node_pools[cl->txn_node].pools[POOL_TRANSACTION];
    conn = pool_acquire_nb(w, pool);
    if (!conn) {
        cl->state = CS_WAITING_CONN;
        fd_unwatch(w, cl->fd, EL_READABLE);
        pool_enqueue_waiter(pool, cl);
        return;
    }
    conn->waiting_client = cl;
    cl->pending_conn = conn;
    resume_client_after_conn(w, cl, conn);
}

/* Sends a proxy-made DISCARD / UNWATCH on the pinned connection to clean it
 * up; the client gets local[] once it's done. */
static void txn_send_cleanup(Worker *w, ClientConn *cl, const char *cmd, int cmdlen, const char *local, int local_len) {
    cl->txn_op = TXN_OP_CLEANUP;
    cl->txn_local_reply = local;
    cl->txn_local_reply_len = local_len;
    cl->hbuf_len = 0;
    txn_write(w, cl, cl->txn_conn, cmd, cmdlen);
}

/* Binds the transaction to slot. 0, or -1 for another slot than the one
 * already bound, -2 for an unmapped slot. */
static int txn_bind_slot(Worker *w, ClientConn *cl, int slot) {
    if (cl->txn_slot >= 0) return slot == cl->txn_slot ? 0 : -1;
    int ni = w->topo.slot_map[slot];
    if (ni < 0 || ni >= w->topo.node_count) return -2;
    cl->txn_slot = slot;
    cl->txn_node = ni;
    return 0;
}

/* Inside MULTI: answers the current command with reply without queueing it,
 * which fails the transaction. */
static void txn_reject_dirty(Worker *w, ClientConn *cl, const char *reply, int len) {
    cl->txn_dirty = 1;
    client_reject_cmd(w, cl, reply, len);
}
#define REJECT_DIRTY(w, cl, lit) txn_reject_dirty((w), (cl), (lit), (int)sizeof(lit) - 1)

/* Forwards the current command as one queued inside MULTI. */
static void txn_forward_queued(Worker *w, ClientConn *cl, const char *cmd) {
    if (cl->txn_dirty || cl->txn_watch_lost) {
        /* EXEC can only answer EXECABORT / nil now: nothing more needs to
         * reach the backend. */
        cl->multi_count++;
        client_reject_cmd(w, cl, RE_QUEUED, (int)sizeof(RE_QUEUED) - 1);
        return;
    }
    if (!cl->txn_multi_sent && !cl->using_pbuf && cl->hbuf_len < cl->total_cmd_len) {
        /* MULTI still has to go out (and be acknowledged) first: take the
         * whole command in now, so none of it is left unread on the client
         * socket while that happens. Comes back via txn_buffered_complete(). */
        int r = client_buffer_cmd(w, cl);
        if (r > 0) txn_buffered_complete(w, cl, cmd);
        return;
    }
    snprintf(cl->txn_cur_name, sizeof(cl->txn_cur_name), "%s", cmd);
    cl->txn_op = TXN_OP_QUEUE;
    cl->txn_cur_start = cl->txn_buf_len;
    txn_send_cmd(w, cl);
}

static void txn_queue_keyless(Worker *w, ClientConn *cl, const char *cmd) {
    if (cl->txn_node < 0) {
        REJECT_DIRTY(w, cl, RE_NO_NODE);
        return;
    }
    txn_forward_queued(w, cl, cmd);
}

static void txn_queue_slot(Worker *w, ClientConn *cl, const char *cmd, int slot) {
    int r = txn_bind_slot(w, cl, slot);
    if (r == -1) {
        REJECT_DIRTY(w, cl, RE_CROSSSLOT);
        return;
    }
    if (r == -2) {
        REJECT_DIRTY(w, cl, RE_UNMAPPED);
        return;
    }
    txn_forward_queued(w, cl, cmd);
}

enum { Q_REJECT, Q_KEYLESS, Q_FIRST_ARG_KEY, Q_BUFFER };

/* How a command inside MULTI is routed (see the top of this file). */
static int txn_queue_kind(const char *cmd) {
    if (!strcmp(cmd, "UNWATCH")) return Q_KEYLESS;
    PoolType rpt = cmd_get_pool_type(cmd);
    if (rpt == POOL_SUBSCRIPTION || rpt == POOL_ACL || rpt == POOL_TRANSACTION) return Q_REJECT;
    if (!strcmp(cmd, "WAIT") || !strcmp(cmd, "WAITAOF")) return Q_REJECT;
    CmdClass cls = classify_command(cmd, rpt);
    if (cls == CMD_CLASS_NO_SLOT) return cmd_in_list(cmd, TXN_KEYLESS_CMDS) ? Q_KEYLESS : Q_REJECT;
    if (cls == CMD_CLASS_SINGLE_KEY && !cmd_in_list(cmd, TXN_SUBCMD_KEY_CMDS)) return Q_FIRST_ARG_KEY;
    return Q_BUFFER;
}

/* A command inside MULTI, header in hbuf (the rest may still be unread). */
static void txn_queue(Worker *w, ClientConn *cl, const char *cmd, const char *key, int keylen) {
    switch (txn_queue_kind(cmd)) {
        case Q_REJECT: REJECT_DIRTY(w, cl, RE_NOT_ALLOWED); return;
        case Q_KEYLESS: txn_queue_keyless(w, cl, cmd); return;
        case Q_FIRST_ARG_KEY:
            if (keylen == 0)
                txn_queue_keyless(w, cl, cmd); /* e.g. "GET" alone: the backend answers the arity error */
            else
                txn_queue_slot(w, cl, cmd, key_slot(key, keylen));
            return;
        default: {
            int r = client_buffer_cmd(w, cl);
            if (r > 0) txn_buffered_complete(w, cl, cmd);
            return;
        }
    }
}

/* WATCH, whole in pbuf. */
static void txn_watch_complete(Worker *w, ClientConn *cl) {
    int argc = atoi(cl->pbuf + 1);
    int nkeys = argc - 1;
    const char **kp = malloc((size_t)(nkeys > 0 ? nkeys : 1) * sizeof(*kp));
    int *kl = malloc((size_t)(nkeys > 0 ? nkeys : 1) * sizeof(*kl));
    if (!kp || !kl) {
        free(kp);
        free(kl);
        client_send_error(w, cl, "out of memory");
        close_client(w, cl);
        return;
    }
    int n = resp_extract_all_args(cl->pbuf, cl->pbuf_len, kp, kl, nkeys);
    int slot = -1, cross = 0;
    for (int i = 0; i < n; i++) {
        int s = key_slot(kp[i], kl[i]);
        if (slot < 0)
            slot = s;
        else if (s != slot)
            cross = 1;
    }
    free(kp);
    free(kl);
    if (n <= 0) {
        client_reject_error(w, cl, "wrong number of arguments for 'watch' command");
        return;
    }
    int r = cross ? -1 : txn_bind_slot(w, cl, slot);
    if (r == -1) {
        client_reject_cmd(w, cl, RE_CROSSSLOT, (int)sizeof(RE_CROSSSLOT) - 1);
        return;
    }
    if (r == -2) {
        client_reject_cmd(w, cl, RE_UNMAPPED, (int)sizeof(RE_UNMAPPED) - 1);
        return;
    }
    cl->txn_op_nkeys = n;
    cl->txn_op = TXN_OP_WATCH;
    txn_send_cmd(w, cl);
}

void txn_buffered_complete(Worker *w, ClientConn *cl, const char *cmd) {
    cl->using_pbuf = 1;
    if (!cl->in_transaction) {
        txn_watch_complete(w, cl); /* the only one buffered outside MULTI */
        return;
    }
    int kind = txn_queue_kind(cmd);
    if (kind == Q_REJECT) {
        REJECT_DIRTY(w, cl, RE_NOT_ALLOWED);
        return;
    }
    if (kind == Q_KEYLESS) {
        txn_queue_keyless(w, cl, cmd);
        return;
    }
    const char *kp[512];
    int kl[512];
    int nk;
    if (kind == Q_FIRST_ARG_KEY) {
        nk = resp_get_nth_arg(cl->pbuf, cl->pbuf_len, 1, &kp[0], &kl[0]) ? 1 : 0;
    } else if (cmd_get_pool_type(cmd) == POOL_BLOCKING) {
        nk = resp_extract_blocking_keys(cl->pbuf, cl->pbuf_len, cmd, kp, kl, 512);
    } else if (cmd_in_list(cmd, TXN_SUBCMD_KEY_CMDS)) {
        /* OBJECT ENCODING key, XINFO STREAM key, XGROUP CREATE key ...,
         * MEMORY USAGE key; HELP and MEMORY's other subcommands are keyless. */
        const char *sub;
        int sublen;
        nk = 0;
        if (resp_get_nth_arg(cl->pbuf, cl->pbuf_len, 1, &sub, &sublen) &&
            !(sublen == 4 && !strncasecmp(sub, "HELP", 4)) &&
            (strcmp(cmd, "MEMORY") || (sublen == 5 && !strncasecmp(sub, "USAGE", 5))))
            nk = resp_get_nth_arg(cl->pbuf, cl->pbuf_len, 2, &kp[0], &kl[0]) ? 1 : 0;
    } else {
        nk = resp_extract_keys(cl->pbuf, cl->pbuf_len, cmd, kp, kl, 512);
    }
    if (nk <= 0) {
        txn_queue_keyless(w, cl, cmd); /* e.g. EVAL with numkeys 0 */
        return;
    }
    int slot = key_slot(kp[0], kl[0]);
    for (int i = 1; i < nk; i++) {
        if (key_slot(kp[i], kl[i]) != slot) {
            REJECT_DIRTY(w, cl, RE_CROSSSLOT);
            return;
        }
    }
    txn_queue_slot(w, cl, cmd, slot);
}

/* ── MULTI / EXEC / DISCARD / UNWATCH ───────────────────────────────── */

static void txn_exec(Worker *w, ClientConn *cl) {
    int dirty = cl->txn_dirty, lost = cl->txn_watch_lost;
    cl->txn_had_watch = cl->watch_count > 0 || lost;
    txn_end_client_state(cl);
    if (dirty || lost) {
        const char *rep = RE_EXECABORT;
        int rl = (int)sizeof(RE_EXECABORT) - 1;
        if (!dirty) nil_reply(cl, &rep, &rl);
        if (cl->txn_conn) {
            if (cl->txn_multi_sent)
                txn_send_cleanup(w, cl, CMD_DISCARD, (int)sizeof(CMD_DISCARD) - 1, rep, rl);
            else
                txn_send_cleanup(w, cl, CMD_UNWATCH, (int)sizeof(CMD_UNWATCH) - 1, rep, rl);
            return;
        }
        client_reject_cmd(w, cl, rep, rl);
        txn_maybe_finish(cl);
        return;
    }
    if (cl->txn_conn) {
        /* Sends MULTI first if nothing was queued (WATCH k / MULTI / EXEC
         * must still answer nil if k changed). */
        cl->txn_op = TXN_OP_EXEC;
        txn_send_cmd(w, cl);
        return;
    }
    client_reject_cmd(w, cl, RE_EMPTY, (int)sizeof(RE_EMPTY) - 1);
    txn_maybe_finish(cl);
}

static void txn_discard(Worker *w, ClientConn *cl) {
    txn_end_client_state(cl);
    if (cl->txn_conn) {
        if (cl->txn_multi_sent) {
            cl->txn_op = TXN_OP_DISCARD;
            txn_send_cmd(w, cl);
        } else {
            txn_send_cleanup(w, cl, CMD_UNWATCH, (int)sizeof(CMD_UNWATCH) - 1, RE_OK, (int)sizeof(RE_OK) - 1);
        }
        return;
    }
    client_reject_cmd(w, cl, RE_OK, (int)sizeof(RE_OK) - 1);
    txn_maybe_finish(cl);
}

static void txn_unwatch(Worker *w, ClientConn *cl) {
    cl->watch_count = 0;
    cl->txn_watch_lost = 0;
    if (cl->txn_conn) {
        cl->txn_op = TXN_OP_UNWATCH;
        txn_send_cmd(w, cl);
        return;
    }
    client_reject_cmd(w, cl, RE_OK, (int)sizeof(RE_OK) - 1);
    txn_maybe_finish(cl);
}

static void txn_arity_error(Worker *w, ClientConn *cl, const char *cmd) {
    char name[16], msg[80];
    int i = 0;
    for (; cmd[i] && i < (int)sizeof(name) - 1; i++) name[i] = (char)tolower((unsigned char)cmd[i]);
    name[i] = '\0';
    snprintf(msg, sizeof(msg), "wrong number of arguments for '%s' command", name);
    /* Inside MULTI an invalid command fails the transaction, as on Valkey. */
    if (cl->in_transaction) cl->txn_dirty = 1;
    client_reject_error(w, cl, msg);
}

void txn_dispatch(Worker *w, ClientConn *cl, const char *cmd, const char *key, int keylen, int argc) {
    txn_check_conn(w, cl);

    if (!strcmp(cmd, "MULTI")) {
        if (argc != 1) {
            txn_arity_error(w, cl, cmd);
        } else if (cl->in_transaction) {
            client_reject_error(w, cl, "MULTI calls can not be nested");
        } else {
            cl->in_transaction = 1;
            cl->multi_count = 0;
            cl->txn_dirty = 0;
            txn_reset_queue(cl);
            client_reject_cmd(w, cl, RE_OK, (int)sizeof(RE_OK) - 1);
        }
        return;
    }
    if (!strcmp(cmd, "EXEC") || !strcmp(cmd, "DISCARD")) {
        if (argc != 1) {
            txn_arity_error(w, cl, cmd);
        } else if (!cl->in_transaction) {
            client_reject_error(w, cl, !strcmp(cmd, "EXEC") ? "EXEC without MULTI" : "DISCARD without MULTI");
        } else if (!strcmp(cmd, "EXEC")) {
            txn_exec(w, cl);
        } else {
            txn_discard(w, cl);
        }
        return;
    }
    if (!strcmp(cmd, "WATCH")) {
        if (argc < 2) {
            txn_arity_error(w, cl, cmd);
        } else if (cl->in_transaction) {
            client_reject_error(w, cl, "WATCH inside MULTI is not allowed");
        } else {
            int r = client_buffer_cmd(w, cl);
            if (r > 0) txn_buffered_complete(w, cl, cmd);
        }
        return;
    }
    if (!strcmp(cmd, "UNWATCH") && !cl->in_transaction) {
        if (argc != 1)
            txn_arity_error(w, cl, cmd);
        else
            txn_unwatch(w, cl);
        return;
    }
    txn_queue(w, cl, cmd, key, keylen);
}

/* ── replies from the pinned connection ─────────────────────────────── */

/* Puts reply[0..len) in the client's wbuf as the answer to the command
 * just forwarded. -1 if the client had to be closed. */
static int txn_reply_local(Worker *w, ClientConn *cl, const char *reply, int len) {
    if (cl->wbuf_cap < len) {
        char *tmp = realloc(cl->wbuf, (size_t)len);
        if (!tmp) {
            client_send_error(w, cl, "out of memory");
            close_client(w, cl);
            return -1;
        }
        cl->wbuf = tmp;
        cl->wbuf_cap = len;
    }
    memcpy(cl->wbuf, reply, (size_t)len);
    cl->wbuf_len = len;
    cl->wbuf_off = 0;
    cl->state = CS_WRITING_RESP;
    fd_watch(w, cl->fd, EL_WRITABLE);
    return 0;
}

/* The pinned connection failed with a request in flight. */
static void txn_backend_error(Worker *w, ClientConn *cl, PoolConn *conn, const char *msg) {
    int op = cl->txn_op;
    const char *local = cl->txn_local_reply;
    int local_len = cl->txn_local_reply_len;
    if (cl->pending_conn == conn) cl->pending_conn = NULL;
    conn->waiting_client = NULL;
    pool_destroy_conn(w, conn); /* txn_conn_gone(): WATCH lost / MULTI failed */
    if (cl->state == CS_STREAMING_CMD) {
        /* Part of the command is still unread: no way to resync. */
        close_client(w, cl);
        return;
    }
    if (op == TXN_OP_CLEANUP) {
        /* The client's EXEC / DISCARD already has its answer. */
        txn_reply_local(w, cl, local, local_len);
        return;
    }
    char buf[256];
    int n = snprintf(buf, sizeof(buf), "-ERR %s\r\n", msg);
    txn_reply_local(w, cl, buf, n);
}

/* RESP3 -> RESP2 for EXEC's reply: each element as the reply to the command
 * queued in its position. 0, or -1 if malformed / out of memory. */
static int txn_downgrade_exec(const ClientConn *cl, const char *buf, int len, char **out, int *out_len) {
    if (buf[0] != '*') return resp3_downgrade_reply(buf, len, 0, 0, out, out_len);
    const char *eol = memchr(buf, '\n', (size_t)len);
    if (!eol) return -1;
    int n = atoi(buf + 1);
    if (n <= 0) return resp3_downgrade_reply(buf, len, 0, 0, out, out_len);
    int hdr = (int)(eol - buf) + 1;
    int cap = len + 64, olen = 0;
    char *o = malloc((size_t)cap);
    if (!o) return -1;
    memcpy(o, buf, (size_t)hdr);
    olen = hdr;
    int off = hdr;
    for (int i = 0; i < n; i++) {
        int el = resp_response_len(buf + off, len - off);
        if (el <= 0) {
            free(o);
            return -1;
        }
        const char *name = i < cl->txn_ncmds ? cl->txn_cmds[i] : "";
        char *d;
        int dl;
        if (resp3_downgrade_reply(buf + off, el, cmd_resp2_nil_array_depth(name), cmd_resp3_pair_transform(name), &d,
                                  &dl) < 0) {
            free(o);
            return -1;
        }
        if (olen + dl > cap) {
            while (olen + dl > cap) cap *= 2;
            char *tmp = realloc(o, (size_t)cap);
            if (!tmp) {
                free(d);
                free(o);
                return -1;
            }
            o = tmp;
        }
        memcpy(o + olen, d, (size_t)dl);
        olen += dl;
        free(d);
        off += el;
    }
    *out = o;
    *out_len = olen;
    return 0;
}

/* Hands a backend reply to the client. 0, or -1 if the client or conn is
 * gone (nothing more may be done with either). */
static int txn_deliver(Worker *w, ClientConn *cl, PoolConn *conn, const char *buf, int len, int is_exec) {
    if (is_exec && cl->resp == 2 && conn->resp3) {
        char *out;
        int out_len;
        if (txn_downgrade_exec(cl, buf, len, &out, &out_len) < 0) {
            txn_backend_error(w, cl, conn, "bad backend response");
            return -1;
        }
        int r = txn_reply_local(w, cl, out, out_len);
        free(out);
        return r;
    }
    int dr = deliver_backend_reply(w, cl, conn, buf, len);
    if (dr == -2) return -1;
    if (dr < 0) {
        txn_backend_error(w, cl, conn, "bad backend response");
        return -1;
    }
    cl->state = CS_WRITING_RESP;
    fd_watch(w, cl->fd, EL_WRITABLE);
    return 0;
}

/* Index of the node a -MOVED reply points at, -1 if it isn't a known one. */
static int txn_moved_target(Worker *w, const char *r, int len) {
    char line[320];
    int n = 0;
    while (n < len && n < (int)sizeof(line) - 1 && r[n] != '\r' && r[n] != '\n') {
        line[n] = r[n];
        n++;
    }
    line[n] = '\0';
    char *sp = strchr(line + 7, ' '); /* past "-MOVED <slot>" */
    if (!sp) return -1;
    char *host = sp + 1;
    char *colon = strrchr(host, ':');
    if (!colon) return -1;
    *colon = '\0';
    int port = atoi(colon + 1);
    for (int i = 0; i < w->topo.node_count; i++)
        if (w->topo.nodes[i].port == port && !strcmp(w->topo.nodes[i].ip, host)) return i;
    return -1;
}

/* Replays the transaction on node target: a fresh MULTI there (op
 * TXN_OP_REPLAY_MULTI), then every queued command (and EXEC, when phase
 * is TXN_OP_EXEC) in one go. The old connection is closed: it may still
 * hold a failed MULTI. */
static void txn_redirect(Worker *w, ClientConn *cl, int target, int phase) {
    printf("[worker %d] client-fd=%d: transaction slot %d moved, replaying %d command(s)%s on node [%d] %s:%d\n",
           w->thread_id, cl->fd, cl->txn_slot, cl->txn_ncmds + (phase == TXN_OP_QUEUE ? 1 : 0),
           phase == TXN_OP_EXEC ? " and EXEC" : "", target, w->topo.nodes[target].ip, w->topo.nodes[target].port);
    txn_unbind(w, cl, 0);
    cl->txn_node = target;
    cl->txn_redirects++;
    cl->txn_saved_op = phase;
    cl->txn_op = TXN_OP_REPLAY_MULTI;
    txn_send_cmd(w, cl);
}

/* -MOVED for a queued command (phase TXN_OP_QUEUE) or for EXEC. */
static void txn_on_moved(Worker *w, ClientConn *cl, PoolConn *conn, int rlen, int phase) {
    int watched = phase == TXN_OP_EXEC ? cl->txn_had_watch : (cl->watch_count > 0 || cl->txn_watch_lost);
    if (watched) {
        /* The WATCH stayed behind on the old node: the transaction can only
         * be retried by the client, so answer as a WATCH abort. */
        if (phase == TXN_OP_EXEC) {
            const char *nil;
            int nl;
            nil_reply(cl, &nil, &nl);
            txn_conn_idle(w, cl, conn);
            txn_unbind(w, cl, 1); /* EXEC refused: nothing left on it */
            if (txn_reply_local(w, cl, nil, nl) < 0) return;
            txn_maybe_finish(cl);
        } else {
            cl->txn_watch_lost = 1;
            cl->watch_count = 0;
            cl->txn_buf_len = cl->txn_cur_start;
            cl->multi_count++;
            txn_conn_idle(w, cl, conn);
            txn_reply_local(w, cl, RE_QUEUED, (int)sizeof(RE_QUEUED) - 1);
        }
        return;
    }
    int target = txn_moved_target(w, conn->rbuf, rlen);
    if (target < 0 || cl->txn_replay_off || cl->txn_redirects >= TXN_MAX_REDIRECTS ||
        g_config.transaction_replay_buffer_size <= 0) {
        if (phase == TXN_OP_QUEUE) {
            cl->txn_dirty = 1;
            cl->txn_buf_len = cl->txn_cur_start;
            if (txn_deliver(w, cl, conn, conn->rbuf, rlen, 0) < 0) return;
            txn_conn_idle(w, cl, conn);
        } else {
            if (txn_deliver(w, cl, conn, conn->rbuf, rlen, 0) < 0) return;
            txn_conn_idle(w, cl, conn);
            txn_unbind(w, cl, 1);
            txn_maybe_finish(cl);
        }
        return;
    }
    conn->rbuf_len = 0;
    txn_redirect(w, cl, target, phase);
}

/* QUEUE's reply (also the last reply of a QUEUE-phase replay). */
static void txn_on_queued_reply(Worker *w, ClientConn *cl, PoolConn *conn, int rlen) {
    if (is_queued(conn->rbuf, rlen)) {
        cl->multi_count++;
        txn_push_name(cl);
        cl->txn_cur_start = cl->txn_buf_len;
    } else {
        cl->txn_dirty = 1; /* the backend has failed its MULTI too */
        cl->txn_buf_len = cl->txn_cur_start;
    }
    if (txn_deliver(w, cl, conn, conn->rbuf, rlen, 0) < 0) return;
    txn_conn_idle(w, cl, conn);
}

/* EXEC's reply (also the last reply of an EXEC-phase replay). */
static void txn_on_exec_reply(Worker *w, ClientConn *cl, PoolConn *conn, int rlen) {
    if (txn_deliver(w, cl, conn, conn->rbuf, rlen, 1) < 0) return;
    txn_conn_idle(w, cl, conn);
    txn_unbind(w, cl, 1);
    txn_maybe_finish(cl);
}

/* One complete reply in conn->rbuf[0..rlen). Returns 1 to go on with the
 * next reply on conn (only while a replay's replies are coming in), 0 when
 * done -- conn or client may be gone then. */
static int txn_reply(Worker *w, ClientConn *cl, PoolConn *conn, int rlen) {
    const char *r = conn->rbuf;
    int ok = r[0] == '+';
    switch (cl->txn_op) {
        case TXN_OP_MULTI_PRE: {
            if (ok) {
                conn->rbuf_len = 0;
                cl->txn_multi_sent = 1;
                cl->txn_op = cl->txn_saved_op;
                resume_client_after_conn(w, cl, conn); /* now the command itself */
                return 0;
            }
            /* MULTI refused: the command never went out. */
            int op = cl->txn_saved_op;
            char *copy = malloc((size_t)rlen);
            if (!copy) {
                client_send_error(w, cl, "out of memory");
                close_client(w, cl);
                return 0;
            }
            memcpy(copy, r, (size_t)rlen);
            txn_conn_idle(w, cl, conn);
            if (op == TXN_OP_QUEUE) {
                cl->txn_dirty = 1;
                cl->txn_buf_len = cl->txn_cur_start;
            } else {
                txn_unbind(w, cl, 0); /* EXEC: may still hold a WATCH */
            }
            client_reject_cmd(w, cl, copy, rlen);
            free(copy);
            txn_maybe_finish(cl);
            return 0;
        }
        case TXN_OP_QUEUE:
            if (is_moved(r, rlen)) {
                txn_on_moved(w, cl, conn, rlen, TXN_OP_QUEUE);
                return 0;
            }
            txn_on_queued_reply(w, cl, conn, rlen);
            return 0;
        case TXN_OP_WATCH:
            if (ok) cl->watch_count += cl->txn_op_nkeys;
            if (txn_deliver(w, cl, conn, r, rlen, 0) < 0) return 0;
            txn_conn_idle(w, cl, conn);
            if (!ok && cl->watch_count == 0 && !cl->txn_watch_lost) {
                txn_unbind(w, cl, 1);
                txn_maybe_finish(cl);
            }
            return 0;
        case TXN_OP_EXEC:
            if (is_moved(r, rlen)) {
                txn_on_moved(w, cl, conn, rlen, TXN_OP_EXEC);
                return 0;
            }
            txn_on_exec_reply(w, cl, conn, rlen);
            return 0;
        case TXN_OP_DISCARD:
        case TXN_OP_UNWATCH:
            if (txn_deliver(w, cl, conn, r, rlen, 0) < 0) return 0;
            txn_conn_idle(w, cl, conn);
            txn_unbind(w, cl, ok);
            txn_maybe_finish(cl);
            return 0;
        case TXN_OP_CLEANUP: {
            const char *local = cl->txn_local_reply;
            int local_len = cl->txn_local_reply_len;
            txn_conn_idle(w, cl, conn);
            txn_unbind(w, cl, ok);
            if (txn_reply_local(w, cl, local, local_len) < 0) return 0;
            txn_maybe_finish(cl);
            return 0;
        }
        case TXN_OP_REPLAY_MULTI: {
            int phase = cl->txn_saved_op;
            if (ok) {
                conn->rbuf_len = 0;
                cl->txn_multi_sent = 1;
                int extra = phase == TXN_OP_EXEC ? (int)sizeof(CMD_EXEC) - 1 : 0;
                int total = cl->txn_buf_len + extra;
                char *payload = malloc((size_t)(total > 0 ? total : 1));
                if (!payload) {
                    client_send_error(w, cl, "out of memory");
                    close_client(w, cl);
                    return 0;
                }
                if (cl->txn_buf_len) memcpy(payload, cl->txn_buf, (size_t)cl->txn_buf_len);
                if (extra) memcpy(payload + cl->txn_buf_len, CMD_EXEC, (size_t)extra);
                cl->txn_replay_expect = cl->txn_ncmds + 1; /* + the current command, or EXEC */
                cl->txn_replay_bad = 0;
                cl->txn_replay_moved = -1;
                cl->txn_op = TXN_OP_REPLAY;
                txn_write(w, cl, conn, payload, total);
                free(payload);
                return 0;
            }
            /* MULTI refused on the new node: nothing went out there. */
            if (phase == TXN_OP_QUEUE) {
                cl->txn_dirty = 1;
                cl->txn_buf_len = cl->txn_cur_start;
                if (txn_deliver(w, cl, conn, r, rlen, 0) < 0) return 0;
                txn_conn_idle(w, cl, conn);
            } else {
                if (txn_deliver(w, cl, conn, r, rlen, 0) < 0) return 0;
                txn_conn_idle(w, cl, conn);
                txn_unbind(w, cl, 1);
                txn_maybe_finish(cl);
            }
            return 0;
        }
        case TXN_OP_REPLAY: {
            if (--cl->txn_replay_expect > 0) {
                /* One of the earlier commands, queued again. */
                if (!is_queued(r, rlen)) {
                    cl->txn_replay_bad = 1;
                    if (is_moved(r, rlen) && cl->txn_replay_moved < 0)
                        cl->txn_replay_moved = txn_moved_target(w, r, rlen);
                }
                memmove(conn->rbuf, conn->rbuf + rlen, (size_t)(conn->rbuf_len - rlen));
                conn->rbuf_len -= rlen;
                return 1;
            }
            int phase = cl->txn_saved_op;
            int moved_target = is_moved(r, rlen) ? txn_moved_target(w, r, rlen) : cl->txn_replay_moved;
            int moved = is_moved(r, rlen) || cl->txn_replay_moved >= 0;
            cl->txn_op = phase;
            if (moved && moved_target >= 0 && !cl->txn_replay_off && cl->txn_redirects < TXN_MAX_REDIRECTS) {
                /* Moved again while replaying. */
                conn->rbuf_len = 0;
                txn_redirect(w, cl, moved_target, phase);
                return 0;
            }
            if (phase == TXN_OP_QUEUE) {
                if (cl->txn_replay_bad) cl->txn_dirty = 1; /* the backend fails EXEC as well */
                txn_on_queued_reply(w, cl, conn, rlen);
            } else {
                txn_on_exec_reply(w, cl, conn, rlen);
            }
            return 0;
        }
        default:
            /* Nothing expected: drop it. */
            conn->rbuf_len = 0;
            return 0;
    }
}

void txn_conn_readable(Worker *w, PoolConn *conn) {
    ClientConn *cl = conn->txn_client;
    for (;;) {
        if (conn->rbuf_len > 0) {
            int rlen = resp_response_len(conn->rbuf, conn->rbuf_len);
            if (rlen < 0) {
                txn_backend_error(w, cl, conn, "bad backend response");
                return;
            }
            if (rlen > 0) {
                if (!txn_reply(w, cl, conn, rlen)) return;
                continue;
            }
        }
        /* Edge-triggered: drain the socket until a reply is complete or
         * nothing more is there (see handle_backend_readable()). */
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                txn_backend_error(w, cl, conn, "response too large");
                return;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                txn_backend_error(w, cl, conn, "out of memory");
                return;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }
        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            txn_backend_error(w, cl, conn, "backend connection error");
            return;
        }
        if (n == 0) {
            txn_backend_error(w, cl, conn, "backend connection closed");
            return;
        }
        conn->rbuf_len += (int)n;
    }
}
