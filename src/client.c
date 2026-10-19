#include "proxy.h"

/* ── client helpers ─────────────────────────────────────────────────── */
void client_send_error(Worker *w, ClientConn *client, const char *msg) {
    char buf[256];
    int n = snprintf(buf, sizeof(buf), "-ERR %s\r\n", msg);
    ssize_t wr = write(client->fd, buf, (size_t)n);
    (void)wr;
    (void)w;
}

void client_send_crossslot(ClientConn *client) {
    const char *err = "-CROSSSLOT Keys in request don't hash to the same slot\r\n";
    ssize_t wr = write(client->fd, err, strlen(err));
    (void)wr;
}

void close_client(Worker *w, ClientConn *client) {
    int fd = client->fd;
    fd_unwatch(w, fd, EL_READABLE | EL_WRITABLE);
    if (fd >= 0 && fd < g_config.max_connection) w->client_map[fd] = NULL;
    if (client->state == CS_WAITING_CONN && client->waiting_pool) {
        pool_remove_waiter(client->waiting_pool, client);
        client->waiting_pool = NULL;
    }
    if (client->hello_auth_conn) {
        /* In-flight HELLO ... AUTH login probe (see hello.c); never aliased
         * by pending_conn or acl_conns[], so it's destroyed here only. */
        PoolConn *hc = client->hello_auth_conn;
        hello_auth_conn_forget(client);
        pool_destroy_conn(w, hc);
    }
    if (client->acl_authenticated && client->pending_conn) {
        /* pending_conn is transiently aliasing one of client->acl_conns[]
         * while a request is in flight on it -- let acl_teardown_all() below
         * destroy it exactly once instead of double-destroying it here. */
        client->pending_conn = NULL;
    }
    if (client->pending_conn) {
        /* Protocol state on this connection is unknown (request may be
         * half-sent, reply may be half-read) -- destroy it rather than
         * releasing it, so it can't be handed to an unrelated client. */
        pool_destroy_conn(w, client->pending_conn);
        client->pending_conn = NULL;
    }
    if (client->fanout_ctx) {
        FanoutCtx *ctx = client->fanout_ctx;
        for (int i = 0; i < ctx->nsubs; i++) {
            FanoutSub *sub = &ctx->subs[i];
            if (sub->conn) {
                sub->conn->fanout_sub = NULL;
                pool_destroy_conn(w, sub->conn);
                sub->conn = NULL;
            }
        }
        fanout_ctx_free(ctx);
        client->fanout_ctx = NULL;
    }
    if (client->sub_conn) {
        PoolConn *sc = client->sub_conn;
        sc->waiting_client = NULL;
        client->sub_conn = NULL;
        pool_destroy_conn(w, sc);
    }
    acl_teardown_all(w, client);
    free(client->wbuf);
    free(client->pbuf);
#ifdef USE_TLS
    if (client->ssl) {
        SSL_free(client->ssl);
        client->ssl = NULL;
    }
#endif
    close(fd);
    free(client);
}

/* A backend connection just failed (write error, read error/EOF, garbled
 * reply, etc.) while `client` was waiting on it for a plain business
 * command's reply. Rather than tearing down the client's own connection
 * over what is, from the client's point of view, just one failed request
 * (killing a long-lived connection -- a benchmark tool, a persistent
 * application connection -- on every backend hiccup or failover is far
 * more disruptive than the one command failing), send it a RESP error and
 * put it back in CS_READING_HDR so it can keep sending commands on the
 * same connection; only the broken backend connection itself is destroyed.
 *
 * CS_STREAMING_CMD is the one exception: part of this command's body may
 * still be in flight from the client and hasn't been read off its socket
 * yet, so there's no safe way to resync framing here -- closing the client
 * is the only option that can't desync the next command's parsing. */
void client_recover_after_backend_error(Worker *w, ClientConn *client, PoolConn *conn, const char *msg) {
    if (client->state == CS_STREAMING_CMD) {
        close_client(w, client);
        return;
    }
    /* A failed HELLO ... AUTH login probe just fails the whole HELLO. */
    if (conn == client->hello_auth_conn) hello_auth_conn_forget(client);
    client_send_error(w, client, msg);
    client->pending_conn = NULL;
    client->hbuf_len = 0;
    client->pbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_READING_HDR;
    fd_watch(w, client->fd, EL_READABLE);
    pool_destroy_conn(w, conn);
}

/* Copies buf[0..len) into cl->wbuf, downgrading it from RESP3 to RESP2 wire
 * format first if cl itself is RESP2 but conn has been upgraded to RESP3
 * (see resp3_conn_needs_upgrade()/resp3_start_conn_upgrade() in hello.c) --
 * otherwise a plain byte-for-byte copy, identical to what every call site
 * used to do inline. Grows cl->wbuf as needed like those call sites did.
 *
 * Returns 0 on success (cl->wbuf now holds the reply; caller proceeds as
 * normal -- release conn, set cl->state, etc). Returns -1 if
 * resp3_downgrade() rejected buf as malformed RESP3: cl and conn are both
 * still alive, and the caller should react exactly like any other bad
 * backend response (e.g. client_recover_after_backend_error()). Returns -2
 * if an allocation failure forced this function to already call
 * client_send_error()+close_client() itself: the caller must return
 * immediately without touching cl or conn again (both may already be
 * freed) -- this mirrors what every one of these call sites already did
 * inline before this helper existed. */
int deliver_backend_reply(Worker *w, ClientConn *cl, PoolConn *conn, const char *buf, int len) {
    if (cl->resp == 2 && conn->resp3) {
        char *out;
        int out_len;
        int nil_array_depth = cmd_resp2_nil_array_depth(cl->last_cmd);
        if (resp3_downgrade(buf, len, nil_array_depth, &out, &out_len) < 0) return -1;

        /* Marker-level downgrade is done; some commands additionally need a
         * pure nesting-depth fixup that no type byte can reveal (see
         * cmd_resp3_pair_transform() in cmd.c) -- both transforms verify
         * the shape first and report "not applicable" (1) rather than force
         * anything, so a malformed-input (-1) result here is treated the
         * same conservative way: just keep using the already-correct
         * marker-level output instead of failing the whole reply over it. */
        int pt = cmd_resp3_pair_transform(cl->last_cmd);
        if (pt != 0) {
            char *out2;
            int out2_len;
            int rc = (pt == 1) ? resp3_flatten_pairs(out, out_len, &out2, &out2_len)
                               : resp3_nest_pairs(out, out_len, &out2, &out2_len);
            if (rc == 0) {
                free(out);
                out = out2;
                out_len = out2_len;
            }
        }

        if (cl->wbuf_cap < out_len) {
            char *tmp = realloc(cl->wbuf, (size_t)out_len);
            if (!tmp) {
                free(out);
                client_send_error(w, cl, "out of memory");
                close_client(w, cl);
                return -2;
            }
            cl->wbuf = tmp;
            cl->wbuf_cap = out_len;
        }
        memcpy(cl->wbuf, out, (size_t)out_len);
        cl->wbuf_len = out_len;
        cl->wbuf_off = 0;
        free(out);
        return 0;
    }
    if (cl->wbuf_cap < len) {
        char *tmp = realloc(cl->wbuf, (size_t)len);
        if (!tmp) {
            client_send_error(w, cl, "out of memory");
            close_client(w, cl);
            return -2;
        }
        cl->wbuf = tmp;
        cl->wbuf_cap = len;
    }
    memcpy(cl->wbuf, buf, (size_t)len);
    cl->wbuf_len = len;
    cl->wbuf_off = 0;
    return 0;
}

/* ── resume client after getting a pool connection ──────────────────── */
void resume_client_after_conn(Worker *w, ClientConn *client, PoolConn *conn) {
    if (resp3_conn_needs_upgrade(conn)) {
        if (resp3_start_conn_upgrade(w, conn) < 0) {
            /* client->pending_conn is already conn at every call site (set
             * right before calling this function) -- close_client()'s
             * existing pending_conn cleanup destroys it correctly. */
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        client->state = CS_WAITING_RESP;
        fd_unwatch(w, client->fd, EL_READABLE);
        return;
    }
    const ClusterNode *nd = &w->topo.nodes[conn->pool->node_idx];
    printf("[worker %d] [pool:%s] acquire  fd=%-4d  node=%s:%d  db=%d  in-use=%d/%d  client-fd=%d\n", w->thread_id,
           POOL_NAME[conn->pool->type], conn->fd, nd->ip, nd->port, conn->db, conn->pool->count, conn->pool->max,
           client->fd);

    char *src_buf = client->using_pbuf ? client->pbuf : client->hbuf;
    int src_len = client->using_pbuf ? client->pbuf_len : client->hbuf_len;

    if (conn->wbuf_cap < src_len) {
        char *tmp = realloc(conn->wbuf, (size_t)src_len);
        if (!tmp) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        conn->wbuf = tmp;
        conn->wbuf_cap = src_len;
    }
    memcpy(conn->wbuf, src_buf, src_len);
    conn->wbuf_len = src_len;
    conn->wbuf_off = 0;

    if (client->using_pbuf) {
        client->cmd_remaining = 0;
    } else {
        client->cmd_remaining = client->total_cmd_len - src_len;
        if (client->cmd_remaining < 0) client->cmd_remaining = 0;
    }

    client->state = (client->cmd_remaining > 0) ? CS_STREAMING_CMD : CS_WAITING_RESP;

    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
    fd_watch(w, conn->fd, EL_WRITABLE);

    if (client->state == CS_STREAMING_CMD)
        fd_watch(w, client->fd, EL_READABLE);
    else
        fd_unwatch(w, client->fd, EL_READABLE);
}

/* ── backend event handlers ─────────────────────────────────────────── */
void handle_backend_writable(Worker *w, PoolConn *conn) {
    if (!conn->wbuf || conn->wbuf_len == 0) {
        fd_unwatch(w, conn->fd, EL_WRITABLE);
        return;
    }
    ssize_t n = write(conn->fd, conn->wbuf + conn->wbuf_off, (size_t)(conn->wbuf_len - conn->wbuf_off));
    if (n < 0) {
        if (errno == EAGAIN) return;
        if (conn->fanout_sub) {
            FanoutSub *sub = conn->fanout_sub;
            FanoutCtx *ctx = sub->ctx;
            sub->error = 1;
            sub->done = 1;
            conn->fanout_sub = NULL;
            /* conn just failed to write -- destroy it, don't recycle a dead fd. */
            conn->waiting_client = NULL;
            pool_destroy_conn(w, conn);
            sub->conn = NULL;
            ctx->had_error = 1;
            ctx->ndone++;
            if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
            return;
        }
        ClientConn *cl = conn->waiting_client;
        if (cl)
            client_recover_after_backend_error(w, cl, conn, "backend connection error");
        else
            pool_destroy_conn(w, conn);
        return;
    }
    conn->wbuf_off += (int)n;
    if (conn->wbuf_off >= conn->wbuf_len) {
        conn->wbuf_len = 0;
        conn->wbuf_off = 0;
        fd_unwatch(w, conn->fd, EL_WRITABLE);

        if (conn->fanout_sub) {
            fd_watch(w, conn->fd, EL_READABLE);
            return;
        }

        if (conn->pool->type == POOL_SUBSCRIPTION) {
            fd_watch(w, conn->fd, EL_READABLE);
            ClientConn *cl2 = conn->waiting_client;
            if (cl2) fd_watch(w, cl2->fd, EL_READABLE);
            return;
        }

        ClientConn *cl = conn->waiting_client;
        if (!cl) return;

        if (cl->state == CS_STREAMING_CMD && cl->cmd_remaining > 0) {
            fd_watch(w, cl->fd, EL_READABLE);
        } else {
            cl->state = CS_WAITING_RESP;
            fd_watch(w, conn->fd, EL_READABLE);
        }
    }
}

void handle_backend_readable(Worker *w, PoolConn *conn) {
    /* Checked first, regardless of pool type or conn->fanout_sub: a RESP3
     * handshake (lazy per-connection upgrade or the one-time global probe --
     * see hello.c) can be in flight on any kind of connection, including one
     * that already has conn->fanout_sub set (fan-out sub-dispatch defers its
     * real command and starts the upgrade without ever clearing fanout_sub --
     * see resp3_conn_needs_upgrade()'s callers in fanout.c). Putting this
     * check first means those calls don't need any "assign fanout_sub late"
     * trick: the handshake's own reply is always intercepted here before it
     * could ever reach handle_fanout_sub_readable(). */
    if (conn->resp3_handshake) {
        handle_resp3_handshake_readable(w, conn);
        return;
    }
    if (conn->fanout_sub) {
        handle_fanout_sub_readable(w, conn);
        return;
    }
    if (conn->pool->type == POOL_SUBSCRIPTION) {
        handle_sub_conn_readable(w, conn);
        return;
    }
    if (conn->pool->type == POOL_ACL) {
        handle_acl_conn_readable(w, conn);
        return;
    }
    ClientConn *cl = conn->waiting_client;
    /* Idle CONN_AVAILABLE connection (nobody's waiting on a reply) just got
     * EOF/error from the backend -- destroy it instead of merely unwatching:
     * leaving it in p->conns[] still marked CONN_AVAILABLE would make the
     * next pool_acquire_nb() hand out a connection that's already dead. */
    if (!cl) {
        pool_destroy_conn(w, conn);
        return;
    }
    if (cl->waiting_acl_help) {
        handle_acl_help_readable(w, conn);
        return;
    }

    /* epoll is edge-triggered (see el_epoll.c's EPOLLET), which only
     * re-notifies on *new* arrivals. A reply that lands in one network
     * burst larger than a single read() must be drained in a loop here --
     * returning after one partial read could leave the rest sitting in
     * the kernel buffer with no further event ever firing for it. */
    int rlen;
    for (;;) {
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                client_recover_after_backend_error(w, cl, conn, "response too large");
                return;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                client_recover_after_backend_error(w, cl, conn, "out of memory");
                return;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }

        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            client_recover_after_backend_error(w, cl, conn, "backend connection error");
            return;
        }
        if (n == 0) {
            client_recover_after_backend_error(w, cl, conn, "backend connection closed");
            return;
        }
        conn->rbuf_len += (int)n;

        rlen = resp_response_len(conn->rbuf, conn->rbuf_len);
        if (rlen < 0) {
            client_recover_after_backend_error(w, cl, conn, "bad backend response");
            return;
        }
        if (rlen > 0) break;
        /* rlen==0: incomplete, keep draining the socket */
    }

    {
        int dr = deliver_backend_reply(w, cl, conn, conn->rbuf, rlen);
        if (dr == -2) return; /* already client_send_error()+close_client()'d */
        if (dr < 0) {
            client_recover_after_backend_error(w, cl, conn, "bad backend response");
            return;
        }
    }

    conn->rbuf_len = 0;

    fd_unwatch(w, conn->fd, EL_READABLE);
    conn->waiting_client = NULL;
    pool_release(w, conn);

    cl->pending_conn = NULL;
    cl->state = CS_WRITING_RESP;
    fd_watch(w, cl->fd, EL_WRITABLE);
}

/* ── client writable: flush response buffer ─────────────────────────── */
void handle_client_writable(Worker *w, ClientConn *client) {
    if (!client->wbuf || client->wbuf_len == 0) {
        fd_unwatch(w, client->fd, EL_WRITABLE);
        fd_watch(w, client->fd, EL_READABLE);
        client->state = CS_READING_HDR;
        return;
    }
    ssize_t n = write(client->fd, client->wbuf + client->wbuf_off, (size_t)(client->wbuf_len - client->wbuf_off));
    if (n < 0) {
        if (errno == EAGAIN) return;
        close_client(w, client);
        return;
    }
    client->wbuf_off += (int)n;
    if (client->wbuf_off >= client->wbuf_len) {
        client->wbuf_len = 0;
        client->wbuf_off = 0;
        fd_unwatch(w, client->fd, EL_WRITABLE);
        fd_watch(w, client->fd, EL_READABLE);
        client->state = CS_READING_HDR;
        client->hbuf_len = 0;
        client->using_pbuf = 0;
        client->pbuf_len = 0;
    }
}

/* ── client readable: parse and dispatch ────────────────────────────── */
void handle_client_readable(Worker *w, ClientConn *client) {
    /* ── CS_STREAMING_CMD: forward cmd body bytes to backend ── */
    if (client->state == CS_STREAMING_CMD) {
        if (!client->pending_conn) {
            close_client(w, client);
            return;
        }
        PoolConn *conn = client->pending_conn;

        /* Drain everything already buffered for this client fd in one go
         * (edge-triggered epoll won't re-notify for a burst that already
         * arrived just because our per-call cap didn't fully consume it). */
        while (client->cmd_remaining > 0) {
            int want = (int)(client->cmd_remaining < 65536 ? client->cmd_remaining : 65536);
            if (conn->wbuf_cap < conn->wbuf_len + want) {
                int nc = conn->wbuf_len + want + 4096;
                char *tmp = realloc(conn->wbuf, (size_t)nc);
                if (!tmp) {
                    close_client(w, client);
                    return;
                }
                conn->wbuf = tmp;
                conn->wbuf_cap = nc;
            }
            ssize_t n = read(client->fd, conn->wbuf + conn->wbuf_len, (size_t)want);
            if (n < 0) {
                if (errno == EAGAIN) {
                    fd_watch(w, conn->fd, EL_WRITABLE);
                    return;
                }
                close_client(w, client);
                return;
            }
            if (n == 0) {
                close_client(w, client);
                return;
            }
            conn->wbuf_len += (int)n;
            client->cmd_remaining -= n;
        }

        fd_watch(w, conn->fd, EL_WRITABLE);
        client->state = CS_WAITING_RESP;
        fd_unwatch(w, client->fd, EL_READABLE);
        return;
    }

    /* ── CS_BUFFERING_CMD: accumulate multi-key command ── */
    if (client->state == CS_BUFFERING_CMD) {
        /* Drain everything already buffered for this client fd in one go
         * (edge-triggered epoll won't re-notify for a burst that already
         * arrived just because our per-call cap didn't fully consume it). */
        int rlen;
        for (;;) {
            if (client->pbuf_cap - client->pbuf_len < 4096) {
                int nc = client->pbuf_cap ? client->pbuf_cap * 2 : 4096;
                if (nc > g_config.max_multikey_payload) {
                    client_send_error(w, client, "command too large");
                    client->pbuf_len = 0;
                    client->state = CS_READING_HDR;
                    fd_watch(w, client->fd, EL_READABLE);
                    return;
                }
                char *tmp = realloc(client->pbuf, (size_t)nc);
                if (!tmp) {
                    close_client(w, client);
                    return;
                }
                client->pbuf = tmp;
                client->pbuf_cap = nc;
            }
            ssize_t n =
                read(client->fd, client->pbuf + client->pbuf_len, (size_t)(client->pbuf_cap - client->pbuf_len));
            if (n < 0) {
                if (errno == EAGAIN) return;
                close_client(w, client);
                return;
            }
            if (n == 0) {
                close_client(w, client);
                return;
            }
            client->pbuf_len += (int)n;

            rlen = resp_response_len(client->pbuf, client->pbuf_len);
            if (rlen != 0) break;
            /* rlen==0: incomplete, keep draining the socket */
        }

        if (rlen > 0) {
            char cmd2[64] = "", key2[512] = "";
            int cl2 = 0, kl2 = 0, ac2 = 0, tc2 = 0;
            resp_parse_header(client->pbuf, client->pbuf_len, cmd2, &cl2, key2, &kl2, &ac2, &tc2);
            PoolType pt2 = cmd_get_pool_type(cmd2);
            if (pt2 == POOL_BLOCKING)
                handle_blocking_complete(w, client, cmd2);
            else if (pt2 == POOL_ACL)
                handle_auth_cmd(w, client);
            else
                handle_multikey_complete(w, client, cmd2);
        }
        return;
    }

    /* ── CS_READING_HDR: read and parse command header ── */
    /* Drain everything already buffered for this client fd in one go
     * (edge-triggered epoll won't re-notify for a burst that already
     * arrived just because our per-call cap didn't fully consume it). */
    char cmd[64] = "", key[512] = "";
    int cmdlen = 0, keylen = 0, argc = 0, total_cmd_len = 0;
    int consumed;
    for (;;) {
        int avail = HDR_BUF_SIZE - client->hbuf_len;
        if (avail <= 0) {
            client_send_error(w, client, "command header too large");
            close_client(w, client);
            return;
        }

        ssize_t n = read(client->fd, client->hbuf + client->hbuf_len, (size_t)avail);
        if (n < 0) {
            if (errno == EAGAIN) return;
            close_client(w, client);
            return;
        }
        if (n == 0) {
            close_client(w, client);
            return;
        }
        client->hbuf_len += (int)n;

        consumed = resp_parse_header(client->hbuf, client->hbuf_len, cmd, &cmdlen, key, &keylen, &argc, &total_cmd_len);
        if (consumed != 0) break;
        /* consumed==0: header incomplete, keep draining the socket */
    }
    if (consumed < 0) {
        client_send_error(w, client, "protocol error");
        close_client(w, client);
        return;
    }

    str_toupper(cmd);
    PoolType pt = route_command(client, cmd);
    CmdClass cls = classify_command(cmd, pt);
    client->total_cmd_len = total_cmd_len;

    if (!cmd_is_supported(cmd)) {
        client_send_error(w, client, "unsupport command");
        client->hbuf_len = 0;
        return;
    }

    /* HELLO's AUTH/SETNAME clauses end the command, and resp_parse_header()
     * doesn't wait for the last argument's bytes -- read until the whole
     * (always small) command is in hbuf before dispatching it. PROXY is
     * answered locally from hbuf too, so it needs the same. */
    if ((!strcmp(cmd, "HELLO") || !strcmp(cmd, "PROXY")) && client->hbuf_len < total_cmd_len) {
        if (total_cmd_len > HDR_BUF_SIZE) {
            client_send_error(w, client, "command header too large");
            close_client(w, client);
            return;
        }
        while (client->hbuf_len < total_cmd_len) {
            ssize_t n = read(client->fd, client->hbuf + client->hbuf_len, (size_t)(HDR_BUF_SIZE - client->hbuf_len));
            if (n < 0) {
                if (errno == EAGAIN) return;
                close_client(w, client);
                return;
            }
            if (n == 0) {
                close_client(w, client);
                return;
            }
            client->hbuf_len += (int)n;
        }
    }

    /* ── CLIENT INFO bookkeeping: this is the single dispatch point shared by
     * every command class, so it's the right place to record the client's
     * last command/last-interaction time and the WATCH/SUBSCRIBE-family
     * counters that have no other tracking anywhere in the codebase. ── */
    client->last_interaction = time(NULL);
    record_last_cmd(client, cmd, key, keylen);

    if (!strcmp(cmd, "WATCH")) {
        client->watch_count += (argc > 1 ? argc - 1 : 0);
    } else if (!strcmp(cmd, "UNWATCH") || !strcmp(cmd, "EXEC") || !strcmp(cmd, "DISCARD")) {
        client->watch_count = 0;
    } else if (!strcmp(cmd, "SUBSCRIBE") || !strcmp(cmd, "PSUBSCRIBE") || !strcmp(cmd, "SSUBSCRIBE")) {
        int *n = !strcmp(cmd, "SUBSCRIBE")    ? &client->sub_count
                 : !strcmp(cmd, "PSUBSCRIBE") ? &client->psub_count
                                              : &client->ssub_count;
        *n += argc - 1;
    } else if (!strcmp(cmd, "UNSUBSCRIBE") || !strcmp(cmd, "PUNSUBSCRIBE") || !strcmp(cmd, "SUNSUBSCRIBE")) {
        int *n = !strcmp(cmd, "UNSUBSCRIBE")    ? &client->sub_count
                 : !strcmp(cmd, "PUNSUBSCRIBE") ? &client->psub_count
                                                : &client->ssub_count;
        *n = (argc > 1 && *n > argc - 1) ? *n - (argc - 1) : 0;
    }

    if (!strcmp(cmd, "AUTH")) {
        if (client->in_subscribe) {
            client_send_error(w, client, "can't AUTH while subscribed to channels");
            client->hbuf_len = 0;
            return;
        }
        /* password may be the 3rd RESP arg, which resp_parse_header() does
         * not guarantee is fully buffered yet -- fully buffer like
         * MULTI_KEY/BLOCKING commands before handling it. */
        client->state = CS_BUFFERING_CMD;
        if (client->pbuf_cap < client->hbuf_len) {
            char *tmp = realloc(client->pbuf, (size_t)client->hbuf_len + 4096);
            if (!tmp) {
                close_client(w, client);
                return;
            }
            client->pbuf = tmp;
            client->pbuf_cap = client->hbuf_len + 4096;
        }
        memcpy(client->pbuf, client->hbuf, client->hbuf_len);
        client->pbuf_len = client->hbuf_len;
        int rlen = resp_response_len(client->pbuf, client->pbuf_len);
        if (rlen > 0) handle_auth_cmd(w, client);
        return;
    }

    if (cls == CMD_CLASS_SINGLE_KEY) {
        if (client->acl_authenticated) {
            if (keylen == 0) {
                client_send_error(w, client, "missing key");
                client->hbuf_len = 0;
                return;
            }
            acl_route_business_cmd(w, client, w->topo.slot_map[key_slot(key, keylen)]);
            return;
        }
        if (!strcmp(cmd, "OBJECT")) {
            if (!strcmp(key, "HELP")) {
                int ni = topo_first_master(w);
                DbGroup *grp = db_group_get(w, client->db);
                if (!grp) {
                    client_send_error(w, client, "db group error");
                    client->hbuf_len = 0;
                    return;
                }
                ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
                PoolConn *conn = pool_acquire_nb(w, pool);
                if (!conn) {
                    client->node_idx = ni;
                    client->state = CS_WAITING_CONN;
                    fd_unwatch(w, client->fd, EL_READABLE);
                    pool_enqueue_waiter(pool, client);
                    return;
                }
                conn->waiting_client = client;
                client->pending_conn = conn;
                resume_client_after_conn(w, client, conn);
                return;
            }
            const char *obj_key;
            int obj_keylen;
            if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &obj_key, &obj_keylen)) {
                return;
            }
            keylen = obj_keylen < 511 ? obj_keylen : 511;
            memcpy(key, obj_key, keylen);
            key[keylen] = '\0';
        }

        if (!strcmp(cmd, "XGROUP") || !strcmp(cmd, "XINFO")) {
            if (!strcmp(key, "HELP")) {
                int ni = topo_first_master(w);
                DbGroup *grp = db_group_get(w, client->db);
                if (!grp) {
                    client_send_error(w, client, "db group error");
                    client->hbuf_len = 0;
                    return;
                }
                ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
                PoolConn *conn = pool_acquire_nb(w, pool);
                if (!conn) {
                    client->node_idx = ni;
                    client->state = CS_WAITING_CONN;
                    fd_unwatch(w, client->fd, EL_READABLE);
                    pool_enqueue_waiter(pool, client);
                    return;
                }
                conn->waiting_client = client;
                client->pending_conn = conn;
                resume_client_after_conn(w, client, conn);
                return;
            }
            const char *xg_key;
            int xg_keylen;
            if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &xg_key, &xg_keylen)) {
                return;
            }
            keylen = xg_keylen < 511 ? xg_keylen : 511;
            memcpy(key, xg_key, keylen);
            key[keylen] = '\0';
        }

        if (!strcmp(cmd, "MEMORY")) {
            if (!strcmp(key, "USAGE")) {
                const char *mem_key;
                int mem_keylen;
                if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &mem_key, &mem_keylen)) {
                    return;
                }
                keylen = mem_keylen < 511 ? mem_keylen : 511;
                memcpy(key, mem_key, keylen);
                key[keylen] = '\0';
            } else {
                client_send_error(w, client, "unknown MEMORY subcommand");
                client->hbuf_len = 0;
                return;
            }
        }

        if (keylen == 0) {
            client_send_error(w, client, "missing key");
            client->hbuf_len = 0;
            return;
        }
        int slot = key_slot(key, keylen);
        int node_idx = w->topo.slot_map[slot];
        if (node_idx < 0 || node_idx >= w->topo.node_count) {
            /* Transient during a live topology change (slot briefly
             * unmapped between an old master dropping out and its
             * replacement being discovered) as well as a genuinely
             * unassigned slot -- either way this is just this one
             * command's problem, not the connection's; killing the whole
             * client here would turn an ordinary "try again in a moment"
             * into every long-lived client (e.g. a benchmark tool holding
             * one connection open) getting disconnected on every failover. */
            client_send_error(w, client, "slot not mapped");
            client->hbuf_len = 0;
            return;
        }
        client->node_idx = node_idx;

        DbGroup *grp = db_group_get(w, client->db);
        if (!grp) {
            client_send_error(w, client, "db group error");
            client->hbuf_len = 0;
            return;
        }
        ConnPool *pool = &grp->node_pools[node_idx].pools[POOL_NORMAL];
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
    } else if (cls == CMD_CLASS_MULTI_KEY) {
        client->state = CS_BUFFERING_CMD;
        if (client->pbuf_cap < client->hbuf_len) {
            char *tmp = realloc(client->pbuf, (size_t)client->hbuf_len + 4096);
            if (!tmp) {
                close_client(w, client);
                return;
            }
            client->pbuf = tmp;
            client->pbuf_cap = client->hbuf_len + 4096;
        }
        memcpy(client->pbuf, client->hbuf, client->hbuf_len);
        client->pbuf_len = client->hbuf_len;
        int rlen = resp_response_len(client->pbuf, client->pbuf_len);
        if (rlen > 0) {
            handle_multikey_complete(w, client, cmd);
        }
    } else if (cls == CMD_CLASS_BLOCKING && strcmp(cmd, "WAIT") != 0 && strcmp(cmd, "WAITAOF") != 0) {
        client->state = CS_BUFFERING_CMD;
        if (client->pbuf_cap < client->hbuf_len) {
            char *tmp = realloc(client->pbuf, (size_t)client->hbuf_len + 4096);
            if (!tmp) {
                close_client(w, client);
                return;
            }
            client->pbuf = tmp;
            client->pbuf_cap = client->hbuf_len + 4096;
        }
        memcpy(client->pbuf, client->hbuf, client->hbuf_len);
        client->pbuf_len = client->hbuf_len;
        int rlen = resp_response_len(client->pbuf, client->pbuf_len);
        if (rlen > 0) {
            handle_blocking_complete(w, client, cmd);
        }
    } else if (cls == CMD_CLASS_NO_KEY) {
        if (!strcmp(cmd, "SCAN"))
            handle_scan_cmd(w, client, total_cmd_len);
        else
            handle_nokey_cmd(w, client, cmd, total_cmd_len);
    } else if (cls == CMD_CLASS_PUBSUB) {
        handle_pubsub_cmd(w, client, total_cmd_len, cmd, key, keylen);
    } else if (cls == CMD_CLASS_ACL) {
        handle_acl_cmd(w, client, total_cmd_len);
    } else {
        printf("[worker %d] [%s] cmd=%s  pool=%s  (not yet implemented)\n", w->thread_id, CMD_CLASS_NAME[cls], cmd,
               POOL_NAME[pt]);
        client_send_error(w, client, "command class not yet supported");
        client->hbuf_len = 0;
    }
}
