#include "proxy.h"

/* Parse subscribe/unsubscribe confirmation and return the subscription count.
 * Format: *3\r\n$N\r\n(un)subscribe\r\n$M\r\nchannel\r\n:count\r\n
 * Returns count (>=0) or -1 if the message is not a confirmation. */
static int parse_sub_count_from_msg(const char *buf, int len) {
    if (len < 10 || buf[0] != '*') return -1;
    const char *nl = memchr(buf + 1, '\n', len - 1);
    if (!nl || atoi(buf + 1) != 3) return -1;
    int off = (int)(nl - buf) + 1;

    if (off >= len || buf[off] != '$') return -1;
    const char *p2 = buf + off + 1;
    const char *nl2 = memchr(p2, '\n', len - off - 1);
    if (!nl2) return -1;
    int namelen = atoi(p2);
    if (namelen < 9 || namelen > 12) return -1;
    int ds = (int)(nl2 - buf) + 1;
    if (ds + namelen + 2 > len) return -1;
    const char *name = buf + ds;

    int is_ctrl = (namelen == 9 && strncasecmp(name, "subscribe", 9) == 0) ||
                  (namelen == 11 && strncasecmp(name, "unsubscribe", 11) == 0) ||
                  (namelen == 10 && strncasecmp(name, "psubscribe", 10) == 0) ||
                  (namelen == 12 && strncasecmp(name, "punsubscribe", 12) == 0) ||
                  (namelen == 10 && strncasecmp(name, "ssubscribe", 10) == 0) ||
                  (namelen == 12 && strncasecmp(name, "sunsubscribe", 12) == 0);
    if (!is_ctrl) return -1;

    off = ds + namelen + 2;
    if (off >= len || buf[off] != '$') return -1;
    const char *p3 = buf + off + 1;
    const char *nl3 = memchr(p3, '\n', len - off - 1);
    if (!nl3) return -1;
    int chanlen = atoi(p3);
    int ds2 = (int)(nl3 - buf) + 1;
    off = ds2 + (chanlen >= 0 ? chanlen + 2 : 0);

    if (off >= len || buf[off] != ':') return -1;
    return atoi(buf + off + 1);
}

/* Queue data[0..len) behind whatever is still unsent on this sub_conn and
 * arm it for writing. Returns -1 on allocation failure. */
static int sub_conn_append(Worker *w, PoolConn *conn, const char *data, int len) {
    if (conn->wbuf_off > 0) {
        int remaining = conn->wbuf_len - conn->wbuf_off;
        if (remaining > 0) memmove(conn->wbuf, conn->wbuf + conn->wbuf_off, (size_t)remaining);
        conn->wbuf_len = remaining > 0 ? remaining : 0;
        conn->wbuf_off = 0;
    }

    if (conn->wbuf_cap < conn->wbuf_len + len) {
        int nc = conn->wbuf_len + len + 4096;
        char *tmp = realloc(conn->wbuf, (size_t)nc);
        if (!tmp) return -1;
        conn->wbuf = tmp;
        conn->wbuf_cap = nc;
    }
    memcpy(conn->wbuf + conn->wbuf_len, data, (size_t)len);
    conn->wbuf_len += len;

    fd_watch(w, conn->fd, EL_WRITABLE);
    return 0;
}

/* ── unsubscribe fence ──────────────────────────────────────────────────
 * An (P|S)UNSUBSCRIBE may or may not take the client out of subscribe mode,
 * and that decides where the next command must go -- so, like predixy, the
 * client's next pipelined command isn't read until it has completed. Its
 * confirmations can't be counted up front (one per channel, or per current
 * subscription when given none), so a PING of our own is sent right behind
 * it instead: by FIFO, once its echo comes back every confirmation has been
 * relayed and in_subscribe is final. The echo is swallowed, the sub_conn is
 * released if the count reached 0, and client input resumes. */
#define SUB_FENCE_TOKEN     "valkey-proxy-unsubscribe-fence"
#define SUB_FENCE_TOKEN_LEN ((int)sizeof(SUB_FENCE_TOKEN) - 1)

static int sub_conn_send_fence(Worker *w, PoolConn *conn) {
    char cmd[96];
    int len = snprintf(cmd, sizeof(cmd), "*2\r\n$4\r\nPING\r\n$%d\r\n%s\r\n", SUB_FENCE_TOKEN_LEN, SUB_FENCE_TOKEN);
    /* An in-flight RESP3 upgrade owns wbuf; queue behind it instead (see
     * handle_pubsub_cmd()). */
    if (conn->resp3_handshake) return resp3_defer_append(conn, cmd, len);
    return sub_conn_append(w, conn, cmd, len);
}

/* The fence's reply: a bulk string once the backend has left subscribe
 * mode, or ["pong", token] (array / RESP3 push) while still subscribed. */
static int is_sub_fence_reply(const char *buf, int len) {
    char exp[96];
    int n = snprintf(exp, sizeof(exp), "$%d\r\n%s\r\n", SUB_FENCE_TOKEN_LEN, SUB_FENCE_TOKEN);
    if (len == n && !memcmp(buf, exp, (size_t)n)) return 1;
    n = snprintf(exp, sizeof(exp), "*2\r\n$4\r\npong\r\n$%d\r\n%s\r\n", SUB_FENCE_TOKEN_LEN, SUB_FENCE_TOKEN);
    return len == n && (buf[0] == '*' || buf[0] == '>') && !memcmp(buf + 1, exp + 1, (size_t)n - 1);
}

void handle_sub_conn_readable(Worker *w, PoolConn *conn) {
    ClientConn *cl = conn->waiting_client;
    if (!cl) {
        fd_unwatch(w, conn->fd, EL_READABLE);
        return;
    }

    /* Drain everything already buffered on this backend fd in one go
     * (edge-triggered epoll won't re-notify for a burst that already
     * arrived just because our per-call cap didn't fully consume it). */
    for (;;) {
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                close_client(w, cl);
                return;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                close_client(w, cl);
                return;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }

        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) break;
            close_client(w, cl);
            return;
        }
        if (n == 0) {
            close_client(w, cl);
            return;
        }
        conn->rbuf_len += (int)n;
    }

    int off = 0, resume_input = 0;
    while (off < conn->rbuf_len) {
        int rlen = resp_response_len(conn->rbuf + off, conn->rbuf_len - off);
        if (rlen == 0) break;
        if (rlen < 0) {
            close_client(w, cl);
            return;
        }

        if (cl->sub_fence_pending && is_sub_fence_reply(conn->rbuf + off, rlen)) {
            off += rlen;
            cl->sub_fence_pending = 0;
            if (cl->in_subscribe) {
                resume_input = 1;
                continue;
            }

            /* Nothing of the client's can be queued behind the fence. */
            conn->rbuf_len = 0;
            conn->waiting_client = NULL;
            cl->sub_conn = NULL;
            pool_destroy_conn(w, conn);

            /* Input was held while draining (see client_wants_input() in
             * client.c); either path below resumes it. */
            if (cl->wbuf_len > 0) {
                cl->state = CS_WRITING_RESP;
                fd_watch(w, cl->fd, EL_WRITABLE);
            } else {
                cl->state = CS_READING_HDR;
                fd_watch(w, cl->fd, EL_READABLE);
            }
            return;
        }

        int pending = cl->wbuf_len - cl->wbuf_off;
        if (cl->wbuf_off > 0) {
            if (pending > 0) memmove(cl->wbuf, cl->wbuf + cl->wbuf_off, (size_t)pending);
            cl->wbuf_len = pending > 0 ? pending : 0;
            cl->wbuf_off = 0;
        }

        /* Once this sub_conn has been (lazily) upgraded to RESP3 -- which
         * can happen independently of what this particular client asked
         * for, see resp3_conn_needs_upgrade() in hello.c -- everything the
         * backend sends on it (message/pmessage/smessage pushes, and, it
         * turns out, the subscribe/unsubscribe/etc confirmations too --
         * verified against a real Valkey server) arrives as RESP3 push type
         * ('>') instead of a plain array, and needs downgrading before
         * relaying to a RESP2 subscriber. Deliberately keyed off the type
         * marker alone rather than the message name, so it's correct
         * regardless of exactly which of these real Valkey does or doesn't
         * push-encode. A RESP3 subscriber (cl->resp==3) gets it straight
         * through either way. */
        const char *out = conn->rbuf + off;
        int out_len = rlen;
        char *downgraded = NULL;
        if (conn->resp3 && cl->resp == 2 && conn->rbuf[off] == '>') {
            if (resp3_downgrade_push(conn->rbuf + off, rlen, &downgraded, &out_len) < 0) {
                close_client(w, cl);
                return;
            }
            out = downgraded;
        }

        if (cl->wbuf_cap < cl->wbuf_len + out_len) {
            int nc = cl->wbuf_len + out_len + 4096;
            char *tmp = realloc(cl->wbuf, (size_t)nc);
            if (!tmp) {
                free(downgraded);
                close_client(w, cl);
                return;
            }
            cl->wbuf = tmp;
            cl->wbuf_cap = nc;
        }
        memcpy(cl->wbuf + cl->wbuf_len, out, (size_t)out_len);
        cl->wbuf_len += out_len;
        free(downgraded);

        int sub_count = parse_sub_count_from_msg(conn->rbuf + off, rlen);
        off += rlen;

        /* The sub_conn itself is released once the fence that always
         * follows an unsubscribe comes back (see above). */
        if (sub_count == 0) cl->in_subscribe = 0;
    }

    if (off > 0) {
        int remaining = conn->rbuf_len - off;
        if (remaining > 0) memmove(conn->rbuf, conn->rbuf + off, (size_t)remaining);
        conn->rbuf_len = remaining;
    }

    if (cl->wbuf_len > 0) {
        cl->state = CS_WRITING_RESP;
        fd_watch(w, cl->fd, EL_WRITABLE);
    } else if (resume_input) {
        fd_watch(w, cl->fd, EL_READABLE);
    }
}

void handle_blocking_complete(Worker *w, ClientConn *client, const char *cmd) {
    const char *kp[256];
    int kl[256];
    int nkeys = resp_extract_blocking_keys(client->pbuf, client->pbuf_len, cmd, kp, kl, 256);
    if (nkeys <= 0) {
        client_send_error(w, client, "missing key or unsupported blocking command");
        client->pbuf_len = 0;
        client->hbuf_len = 0;
        client->using_pbuf = 0;
        client->state = CS_READING_HDR;
        fd_watch(w, client->fd, EL_READABLE);
        return;
    }

    int slot = key_slot(kp[0], kl[0]);
    for (int i = 1; i < nkeys; i++) {
        if (key_slot(kp[i], kl[i]) != slot) {
            client_send_crossslot(client);
            client->pbuf_len = 0;
            client->hbuf_len = 0;
            client->using_pbuf = 0;
            client->state = CS_READING_HDR;
            fd_watch(w, client->fd, EL_READABLE);
            return;
        }
    }

    int node_idx = w->topo.slot_map[slot];
    if (node_idx < 0 || node_idx >= w->topo.node_count) {
        client_send_error(w, client, "slot not mapped");
        client->pbuf_len = 0;
        client->hbuf_len = 0;
        client->using_pbuf = 0;
        client->state = CS_READING_HDR;
        fd_watch(w, client->fd, EL_READABLE);
        return;
    }

    client->using_pbuf = 1;
    client->node_idx = node_idx;

    if (client->acl_authenticated) {
        acl_route_business_cmd(w, client, node_idx);
        return;
    }

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }
    ConnPool *pool = &grp->node_pools[node_idx].pools[POOL_BLOCKING];
    PoolConn *conn = pool_acquire_nb(w, pool);
    if (!conn) {
        client->state = CS_WAITING_CONN;
        fd_unwatch(w, client->fd, EL_READABLE);
        pool_enqueue_waiter(pool, client);
        return;
    }
    conn->waiting_client = client;
    client->pending_conn = conn;
    resume_client_after_conn(w, client, conn);
}

void handle_pubsub_cmd(Worker *w, ClientConn *client, int cmdlen, const char *cmd, const char *key, int keylen) {
    if (!client->sub_conn) {
        int ni;
        if (!strcmp(cmd, "SSUBSCRIBE") || !strcmp(cmd, "SUNSUBSCRIBE")) {
            if (keylen > 0) {
                int slot = key_slot(key, keylen);
                ni = w->topo.slot_map[slot];
                if (ni < 0 || ni >= w->topo.node_count) ni = 0;
            } else {
                ni = topo_first_master(w);
            }
        } else {
            ni = topo_first_master(w);
        }

        DbGroup *grp = db_group_get(w, client->db);
        if (!grp) {
            client_send_error(w, client, "db group error");
            client->hbuf_len = 0;
            return;
        }
        ConnPool *pool = &grp->node_pools[ni].pools[POOL_SUBSCRIPTION];
        PoolConn *conn = pool_acquire_nb(w, pool);
        if (!conn) {
            client_send_error(w, client, "subscription pool full");
            client->hbuf_len = 0;
            return;
        }
        conn->waiting_client = client;
        client->sub_conn = conn;
        if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
    }

    PoolConn *conn = client->sub_conn;

    /* A RESP3 handshake on this sub_conn (either just kicked off below, or
     * still in flight from an earlier command on this same connection --
     * the client may pipeline a second SUBSCRIBE/UNSUBSCRIBE before the
     * first one's handshake reply has even arrived) means wbuf is occupied
     * by "HELLO 3" bytes right now. Append behind whatever's already
     * deferred instead -- resp3_defer_append() supports repeated calls for
     * exactly this reason -- and let handle_resp3_handshake_readable()
     * flush the whole thing, in order, once the handshake completes. */
    if (conn->resp3_handshake || resp3_conn_needs_upgrade(conn)) {
        int need_start = !conn->resp3_handshake;
        if (resp3_defer_append(conn, client->hbuf, cmdlen) < 0 ||
            (need_start && resp3_start_conn_upgrade(w, conn) < 0)) {
            close_client(w, client);
            return;
        }
    } else if (sub_conn_append(w, conn, client->hbuf, cmdlen) < 0) {
        close_client(w, client);
        return;
    }

    if (!strcmp(cmd, "UNSUBSCRIBE") || !strcmp(cmd, "PUNSUBSCRIBE") || !strcmp(cmd, "SUNSUBSCRIBE")) {
        if (sub_conn_send_fence(w, conn) < 0) {
            close_client(w, client);
            return;
        }
        client->sub_fence_pending = 1;
    }
    fd_watch(w, client->fd, EL_READABLE);

    client->hbuf_len = 0;
    client->state = CS_READING_HDR;
}
