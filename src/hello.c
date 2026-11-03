#include "proxy.h"

/* ── HELLO / RESP3 protocol negotiation ──────────────────────────────────
 *
 * Backend connections speak RESP2 by default (pool_new_fd() never sends
 * HELLO). The first time any client anywhere sends "HELLO 3", this worker
 * acquires an ordinary POOL_NORMAL connection and probes the backend with a
 * real "HELLO 3" (resp3_start_probe()). If the backend accepts it, every
 * *other* PoolConn in the process -- across every worker, db, node and pool
 * type -- is considered upgradable from then on: g_backend_resp3_active
 * (proxy.h) flips to 1 (sticky, never reset), and each individual
 * connection catches up lazily the next time it's actually used to forward
 * a command (resp3_conn_needs_upgrade()/resp3_start_conn_upgrade()), for
 * *any* client regardless of that client's own RESP version -- this is what
 * lets "every connection eventually speaks RESP3" hold without ever having
 * to reach across worker threads to touch another thread's pools.
 *
 * A RESP2 client whose command lands on an already-upgraded connection gets
 * its reply downgraded back to RESP2 wire format by deliver_backend_reply()
 * (client.c) / the fan-out ingestion point (fanout.c) / the pub/sub push
 * path (pubsub.c) -- all of which call resp3_downgrade()/
 * resp3_downgrade_push() (resp.c). This file only owns the handshake
 * itself and the client-facing HELLO command.
 */

/* Writes "HELLO 3" into conn->wbuf and marks the handshake in flight.
 * Returns 0 on success, -1 on allocation failure (caller must treat this
 * exactly like any other OOM at whatever call site triggered the upgrade --
 * see resp3_start_conn_upgrade()/resp3_start_probe() callers). */
static int backend_send_hello3(Worker *w, PoolConn *conn) {
    static const char HELLO3[] = "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
    int len = (int)sizeof(HELLO3) - 1;
    if (conn->wbuf_cap < len) {
        char *tmp = realloc(conn->wbuf, (size_t)len);
        if (!tmp) return -1;
        conn->wbuf = tmp;
        conn->wbuf_cap = len;
    }
    memcpy(conn->wbuf, HELLO3, (size_t)len);
    conn->wbuf_len = len;
    conn->wbuf_off = 0;
    conn->resp3_handshake = 1;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
    fd_watch(w, conn->fd, EL_WRITABLE);
    return 0;
}

int resp3_conn_needs_upgrade(PoolConn *conn) {
    return backend_resp3_active_load() && !conn->resp3 && !conn->resp3_handshake;
}

int resp3_start_conn_upgrade(Worker *w, PoolConn *conn) {
    conn->resp3_probe = 0;
    return backend_send_hello3(w, conn);
}

int resp3_start_probe(Worker *w, PoolConn *conn) {
    conn->resp3_probe = 1;
    return backend_send_hello3(w, conn);
}

int resp3_defer_append(PoolConn *conn, const char *data, int len) {
    if (conn->resp3_deferred_cap - conn->resp3_deferred_len < len) {
        int nc = conn->resp3_deferred_len + len + 256;
        char *tmp = realloc(conn->resp3_deferred_cmd, (size_t)nc);
        if (!tmp) return -1;
        conn->resp3_deferred_cmd = tmp;
        conn->resp3_deferred_cap = nc;
    }
    memcpy(conn->resp3_deferred_cmd + conn->resp3_deferred_len, data, (size_t)len);
    conn->resp3_deferred_len += len;
    return 0;
}

/* Common "this connection's handshake failed/errored" cleanup, shared by the
 * read-error and backend-rejected-HELLO branches below. Mirrors exactly how
 * each of these three call-site shapes already handles an ordinary backend
 * error elsewhere (handle_fanout_sub_readable() in fanout.c,
 * client_recover_after_backend_error() in client.c, handle_sub_conn_readable()'s
 * close_client() in pubsub.c). */
static void resp3_handshake_fail(Worker *w, PoolConn *conn, ClientConn *cl, int is_fanout, const char *msg) {
    if (is_fanout) {
        FanoutSub *sub = conn->fanout_sub;
        FanoutCtx *ctx = sub->ctx;
        sub->error = 1;
        sub->done = 1;
        conn->fanout_sub = NULL;
        pool_destroy_conn(w, conn);
        sub->conn = NULL;
        ctx->had_error = 1;
        ctx->ndone++;
        if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
    } else if (cl) {
        if (conn->pool->type == POOL_SUBSCRIPTION)
            close_client(w, cl);
        else
            client_recover_after_backend_error(w, cl, conn, msg);
    } else {
        pool_destroy_conn(w, conn);
    }
}

/* Appends a RESP2 bulk-string field ("$len\r\n<s>\r\n") to buf at off,
 * returning the new offset. Caller guarantees buf is large enough --
 * build_and_send_hello_reply()'s fixed field set makes that easy to size
 * up front. */
static int hello_put_bulk(char *buf, int off, int cap, const char *s, int slen) {
    return off + snprintf(buf + off, (size_t)(cap - off), "$%d\r\n%.*s\r\n", slen, slen, s);
}

/* Builds and sends this client's HELLO reply entirely locally -- no backend
 * round-trip. server and mode are fixed. version, role and modules are what
 * the backends report (version and modules probed at topology discovery),
 * unless the reported_* config overrides them (see src/info.c). Shape
 * depends on client->resp: a RESP3 client gets a 7-pair map (%7), a RESP2
 * client gets the same 14 elements flattened into a plain array (*14) --
 * matching how real Valkey answers HELLO on however that connection ends
 * up speaking after the switch. */
static void build_and_send_hello_reply(Worker *w, ClientConn *client) {
    static const char *SERVER = "valkey-proxy";
    static const char *MODE = "standalone";
    const char *ROLE = info_reported_role();
    char valkey_version[32], redis_version[32];
    info_get_reported_versions(valkey_version, sizeof(valkey_version), redis_version, sizeof(redis_version));
    const char *VERSION = valkey_version[0] ? valkey_version : redis_version[0] ? redis_version : "7.4.0";
    BackendModule mods[MAX_BACKEND_MODULES];
    int nmods = info_get_reported_modules(mods, MAX_BACKEND_MODULES);

    /* Sized for MAX_BACKEND_MODULES entries with the longest names. */
    char buf[8192];
    int off = snprintf(buf, sizeof(buf), client->resp == 3 ? "%%7\r\n" : "*14\r\n");

    off = hello_put_bulk(buf, off, (int)sizeof(buf), "server", 6);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), SERVER, (int)strlen(SERVER));
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "version", 7);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), VERSION, (int)strlen(VERSION));
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "proto", 5);
    off += snprintf(buf + off, sizeof(buf) - (size_t)off, ":%d\r\n", client->resp);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "id", 2);
    off += snprintf(buf + off, sizeof(buf) - (size_t)off, ":%lld\r\n", client->id);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "mode", 4);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), MODE, (int)strlen(MODE));
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "role", 4);
    off = hello_put_bulk(buf, off, (int)sizeof(buf), ROLE, (int)strlen(ROLE));
    off = hello_put_bulk(buf, off, (int)sizeof(buf), "modules", 7);
    /* Each module is a name/ver/path/args map, as Valkey sends it. The
     * backends' module paths mean nothing to a proxy client, so path is
     * empty and args is an empty array. */
    off += snprintf(buf + off, sizeof(buf) - (size_t)off, "*%d\r\n", nmods);
    for (int i = 0; i < nmods; i++) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off, client->resp == 3 ? "%%4\r\n" : "*8\r\n");
        off = hello_put_bulk(buf, off, (int)sizeof(buf), "name", 4);
        off = hello_put_bulk(buf, off, (int)sizeof(buf), mods[i].name, (int)strlen(mods[i].name));
        off = hello_put_bulk(buf, off, (int)sizeof(buf), "ver", 3);
        off += snprintf(buf + off, sizeof(buf) - (size_t)off, ":%lld\r\n", mods[i].ver);
        off = hello_put_bulk(buf, off, (int)sizeof(buf), "path", 4);
        off = hello_put_bulk(buf, off, (int)sizeof(buf), "", 0);
        off = hello_put_bulk(buf, off, (int)sizeof(buf), "args", 4);
        off += snprintf(buf + off, sizeof(buf) - (size_t)off, "*0\r\n");
    }

    if (client->wbuf_cap < off) {
        char *tmp = realloc(client->wbuf, (size_t)off);
        if (!tmp) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        client->wbuf = tmp;
        client->wbuf_cap = off;
    }
    memcpy(client->wbuf, buf, (size_t)off);
    client->wbuf_len = off;
    client->wbuf_off = 0;
    client->pbuf_len = 0;
    client->hbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_WRITING_RESP;
    fd_watch(w, client->fd, EL_WRITABLE);
}

/* Drops everything a HELLO held back until it's known to succeed (see
 * the ClientConn.hello_* fields in proxy.h). */
static void hello_pending_clear(ClientConn *cl) {
    cl->hello_pending_setname = 0;
    cl->hello_pending_auth_default = 0;
    memset(cl->hello_auth_pass, 0, sizeof(cl->hello_auth_pass));
    cl->hello_auth_userlen = cl->hello_auth_passlen = 0;
    cl->hello_auth_protover = 0;
}

/* Detaches client from its in-flight HELLO AUTH login probe (the caller
 * destroys the conn itself) and drops the HELLO's held-back state -- i.e.
 * the whole HELLO fails and changes nothing. */
void hello_auth_conn_forget(ClientConn *client) {
    PoolConn *c = client->hello_auth_conn;
    if (c) {
        c->waiting_client = NULL;
        c->acl_authenticating = 0;
        c->acl_is_login_probe = 0;
        c->acl_hello_login = 0;
        c->acl_hello_resp3 = 0;
    }
    client->hello_auth_conn = NULL;
    hello_pending_clear(client);
}

/* Commit point every successful HELLO ends at: applies the held-back
 * "AUTH default" identity switch and SETNAME, switches to `resp` (0 = keep
 * the current version, i.e. bare HELLO) and replies. */
static void hello_commit(Worker *w, ClientConn *cl, int resp) {
    if (cl->hello_pending_auth_default) {
        if (cl->acl_authenticated) acl_teardown_all(w, cl);
        cl->acl_authenticated = 0;
        cl->acl_userlen = 0;
        cl->acl_passlen = 0;
    }
    /* Already validated by handle_hello_cmd(), so this can't fail. */
    if (cl->hello_pending_setname) client_set_name(w, cl, cl->hello_pending_name, cl->hello_pending_namelen);
    hello_pending_clear(cl);
    if (resp) cl->resp = resp;
    build_and_send_hello_reply(w, cl);
}

void handle_resp3_handshake_readable(Worker *w, PoolConn *conn) {
    int rlen;
    for (;;) {
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                rlen = -1;
                break;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                rlen = -1;
                break;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }
        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            rlen = -1;
            break;
        }
        if (n == 0) {
            rlen = -1;
            break;
        }
        conn->rbuf_len += (int)n;
        rlen = resp_response_len(conn->rbuf, conn->rbuf_len);
        if (rlen != 0) break;
        /* rlen==0: incomplete, keep draining the socket */
    }

    int is_probe = conn->resp3_probe;
    int is_fanout = (conn->fanout_sub != NULL);
    ClientConn *cl = conn->waiting_client;
    conn->resp3_handshake = 0;
    conn->resp3_probe = 0;

    if (rlen < 0) {
        conn->rbuf_len = 0;
        if (is_probe && cl) hello_pending_clear(cl);
        /* A probe that never even got a parseable reply is exactly the
         * "backend doesn't support this" case from the client's point of
         * view too, but there's no clean error to relay -- just fail the
         * HELLO like any other broken connection would. g_backend_resp3_active
         * stays untouched either way. */
        resp3_handshake_fail(w, conn, cl, is_fanout, "backend connection error");
        return;
    }

    int ok = (conn->rbuf[0] != '-');

    if (!ok) {
        if (is_probe) {
            hello_pending_clear(cl);
            /* 3.1: relay the backend's real error (e.g. an old Valkey/Redis
             * rejecting HELLO's protover argument) to the client that asked
             * for RESP3, verbatim. The connection itself is healthy -- this
             * was a normal RESP error reply, not a broken socket -- so
             * release it instead of destroying it. Nothing global changes:
             * a later client's "HELLO 3" is free to probe again. */
            if (cl->wbuf_cap < rlen) {
                char *tmp = realloc(cl->wbuf, (size_t)rlen);
                if (!tmp) {
                    conn->rbuf_len = 0;
                    client_recover_after_backend_error(w, cl, conn, "out of memory");
                    return;
                }
                cl->wbuf = tmp;
                cl->wbuf_cap = rlen;
            }
            memcpy(cl->wbuf, conn->rbuf, (size_t)rlen);
            cl->wbuf_len = rlen;
            cl->wbuf_off = 0;
            conn->rbuf_len = 0;

            fd_unwatch(w, conn->fd, EL_READABLE);
            conn->waiting_client = NULL;
            pool_release(w, conn);
            cl->pending_conn = NULL;
            cl->state = CS_WRITING_RESP;
            fd_watch(w, cl->fd, EL_WRITABLE);
            return;
        }
        /* An ordinary lazy upgrade being rejected by the backend after the
         * cluster-wide probe already succeeded shouldn't happen (see the
         * plan's "assume a homogeneous cluster version" simplification) --
         * handle it exactly like any other backend connection error. */
        conn->rbuf_len = 0;
        resp3_handshake_fail(w, conn, cl, is_fanout, "backend connection error");
        return;
    }

    /* Success: this connection speaks RESP3 from now on. */
    conn->rbuf_len = 0;
    conn->resp3 = 1;

    if (is_probe) {
        atomic_store_explicit(&g_backend_resp3_active, 1, memory_order_release);
        fd_unwatch(w, conn->fd, EL_READABLE);
        conn->waiting_client = NULL;
        pool_release(w, conn);
        cl->pending_conn = NULL;
        hello_commit(w, cl, 3);
        return;
    }

    /* Ordinary lazy upgrade succeeded. Two shapes, distinguished by whether
     * anything was deferred (see resp3_defer_append()'s callers in fanout.c/
     * pubsub.c): fan-out sub-dispatch and pub/sub sub_conn always defer
     * before starting the upgrade (they have no client-side hbuf/pbuf to
     * fall back on), so a nonzero resp3_deferred_len unambiguously means one
     * of those; the resume_client_after_conn() hook (client.c) never defers
     * at all -- the real command is simply still sitting untouched in
     * cl->hbuf/pbuf, exactly like the ACL AUTH-before-forward handshake
     * leaves it (see acl_route_business_cmd() in acl.c). */
    if (conn->resp3_deferred_len > 0) {
        if (conn->wbuf_cap < conn->resp3_deferred_len) {
            char *tmp = realloc(conn->wbuf, (size_t)conn->resp3_deferred_len);
            if (!tmp) {
                resp3_handshake_fail(w, conn, cl, is_fanout, "out of memory");
                return;
            }
            conn->wbuf = tmp;
            conn->wbuf_cap = conn->resp3_deferred_len;
        }
        memcpy(conn->wbuf, conn->resp3_deferred_cmd, (size_t)conn->resp3_deferred_len);
        conn->wbuf_len = conn->resp3_deferred_len;
        conn->wbuf_off = 0;
        free(conn->resp3_deferred_cmd);
        conn->resp3_deferred_cmd = NULL;
        conn->resp3_deferred_len = conn->resp3_deferred_cap = 0;
        fd_watch(w, conn->fd, EL_WRITABLE);
    } else if (cl) {
        resume_client_after_conn(w, cl, conn);
    }
}

static void hello_send_noproto(ClientConn *client) {
    static const char msg[] = "-NOPROTO unsupported protocol version\r\n";
    ssize_t wr = write(client->fd, msg, sizeof(msg) - 1);
    (void)wr;
}

/* Same acceptance rules as Valkey's string2ll(): optional '-', no leading
 * zeros (except "0" itself), no whitespace, must fit in a long long. */
static int hello_protover_is_int(const char *s, int len) {
    char tmp[32];
    if (len <= 0 || len >= (int)sizeof(tmp)) return 0;
    memcpy(tmp, s, (size_t)len);
    tmp[len] = '\0';
    if (len == 1 && tmp[0] == '0') return 1;
    const char *p = tmp[0] == '-' ? tmp + 1 : tmp;
    if (*p < '1' || *p > '9') return 0;
    for (const char *q = p + 1; *q; q++)
        if (*q < '0' || *q > '9') return 0;
    errno = 0;
    (void)strtoll(tmp, NULL, 10);
    return errno != ERANGE;
}

/* "HELLO <ver> AUTH user pass" for a non-default user: log in on a fresh
 * POOL_ACL conn, the same way handle_auth_cmd() (acl.c) does, except the
 * conn is held in client->hello_auth_conn rather than acl_conns[] until the
 * backend says yes -- so on failure the client's current identity (and its
 * pinned per-node conns) are left exactly as they were. ver 3 sends
 * "HELLO 3 AUTH user pass", folding login and RESP3 negotiation into one
 * round-trip (and doubling as the process-wide RESP3 probe if none has
 * succeeded yet); ver 2 sends a plain "AUTH user pass". The held-back
 * user/pass/name are already in client->hello_*. See
 * handle_hello_auth_reply() for the other half. */
static void hello_start_auth_login(Worker *w, ClientConn *client) {
    int ni = topo_first_master(w);
    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }
    ConnPool *pool = &grp->node_pools[ni].pools[POOL_ACL];
    PoolConn *conn = pool_acquire_nb(w, pool);
    if (!conn) {
        hello_pending_clear(client);
        client_send_error(w, client, "ACL connection pool exhausted");
        client->hbuf_len = 0;
        return;
    }

    int ver = client->hello_auth_protover;
    int len;
    char *cmd;
    if (ver == 3) {
        const char *args[4] = {"3", "AUTH", client->hello_auth_user, client->hello_auth_pass};
        int arglens[4] = {1, 4, client->hello_auth_userlen, client->hello_auth_passlen};
        cmd = build_resp_cmd("HELLO", 5, args, arglens, 4, &len);
    } else {
        const char *args[2] = {client->hello_auth_user, client->hello_auth_pass};
        int arglens[2] = {client->hello_auth_userlen, client->hello_auth_passlen};
        cmd = build_resp_cmd("AUTH", 4, args, arglens, 2, &len);
    }
    if (cmd && conn->wbuf_cap < len) {
        char *tmp = realloc(conn->wbuf, (size_t)len);
        if (!tmp) {
            free(cmd);
            cmd = NULL;
        } else {
            conn->wbuf = tmp;
            conn->wbuf_cap = len;
        }
    }
    if (!cmd) {
        pool_destroy_conn(w, conn);
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return;
    }
    memcpy(conn->wbuf, cmd, (size_t)len);
    conn->wbuf_len = len;
    conn->wbuf_off = 0;
    free(cmd);

    conn->waiting_client = client;
    conn->acl_authenticating = 1;
    conn->acl_is_login_probe = 1;
    conn->acl_hello_login = 1;
    conn->acl_hello_resp3 = (ver == 3);
    client->hello_auth_conn = conn;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;

    client->hbuf_len = 0;
    client->state = CS_WAITING_RESP;
    fd_unwatch(w, client->fd, EL_READABLE);
    fd_watch(w, conn->fd, EL_WRITABLE);
}

/* handle_acl_conn_readable()'s (acl.c) branch for a HELLO AUTH login probe:
 * conn->rbuf[0..rlen) holds the backend's reply to it. */
void handle_hello_auth_reply(Worker *w, PoolConn *conn, ClientConn *cl, int rlen) {
    fd_unwatch(w, conn->fd, EL_READABLE);

    if (conn->rbuf[0] == '-') {
        /* Relay the real reason (WRONGPASS, NOPROTO, ...) verbatim, like a
         * failed AUTH does; nothing about the client changes. */
        if (cl->wbuf_cap < rlen) {
            char *tmp = realloc(cl->wbuf, (size_t)rlen);
            if (!tmp) {
                conn->rbuf_len = 0;
                client_send_error(w, cl, "out of memory");
                close_client(w, cl);
                return;
            }
            cl->wbuf = tmp;
            cl->wbuf_cap = rlen;
        }
        memcpy(cl->wbuf, conn->rbuf, (size_t)rlen);
        cl->wbuf_len = rlen;
        cl->wbuf_off = 0;
        conn->rbuf_len = 0;
        hello_auth_conn_forget(cl);
        pool_destroy_conn(w, conn);
        cl->pbuf_len = 0;
        cl->hbuf_len = 0;
        cl->using_pbuf = 0;
        cl->state = CS_WRITING_RESP;
        fd_watch(w, cl->fd, EL_WRITABLE);
        return;
    }

    conn->rbuf_len = 0;
    int sent_hello3 = conn->acl_hello_resp3;
    conn->acl_authenticating = 0;
    conn->acl_is_login_probe = 0;
    conn->acl_hello_login = 0;
    conn->acl_hello_resp3 = 0;
    if (sent_hello3) {
        conn->resp3 = 1;
        atomic_store_explicit(&g_backend_resp3_active, 1, memory_order_release);
    }

    /* Commit point. Drop the previous identity's pinned conns first -- that
     * may compact the POOL_ACL array this conn lives in and relocate it
     * (pool_destroy_conn() keeps hello_auth_conn pointing at it), so re-read
     * it afterwards rather than trusting `conn`. It stays pinned to cl
     * (waiting_client) from now on, exactly like an AUTH login probe. */
    int ni = conn->pool->node_idx;
    acl_teardown_all(w, cl);
    conn = cl->hello_auth_conn;
    cl->hello_auth_conn = NULL;
    cl->acl_conns[ni] = conn;
    memcpy(cl->acl_user, cl->hello_auth_user, (size_t)cl->hello_auth_userlen);
    cl->acl_user[cl->hello_auth_userlen] = '\0';
    cl->acl_userlen = cl->hello_auth_userlen;
    memcpy(cl->acl_pass, cl->hello_auth_pass, (size_t)cl->hello_auth_passlen);
    cl->acl_passlen = cl->hello_auth_passlen;
    cl->acl_authenticated = 1;
    hello_commit(w, cl, cl->hello_auth_protover);
}

/* client->hbuf holds the whole "HELLO [protover [AUTH user pass]
 * [SETNAME name]]" command -- handle_client_readable() (client.c) keeps
 * reading until all of it has arrived before dispatching, since
 * resp_parse_header() alone doesn't wait for a trailing SETNAME/AUTH
 * argument's bytes. Validation order mirrors real Valkey: protover first
 * (non-integer, then -NOPROTO), then option syntax and name, then AUTH,
 * and only then SETNAME and the protocol switch. The whole HELLO is
 * atomic: every effect is held back in client->hello_* and applied only by
 * hello_commit(), so a rejected HELLO -- including a failed AUTH -- changes
 * nothing, not even a previously authenticated identity. */
void handle_hello_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;

    const char *protover;
    int protover_len;
    int has_protover = resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &protover, &protover_len);
    if (has_protover && !hello_protover_is_int(protover, protover_len)) {
        client_send_error(w, client, "Protocol version is not an integer or out of range");
        client->hbuf_len = 0;
        return;
    }
    if (has_protover && (protover_len != 1 || (protover[0] != '2' && protover[0] != '3'))) {
        hello_send_noproto(client);
        client->hbuf_len = 0;
        return;
    }

    const char *name = NULL;
    int namelen = 0;
    const char *user = NULL, *pass = NULL;
    int userlen = 0, passlen = 0;
    const char *opt, *a1;
    int optlen, a1len;
    for (int j = 2; resp_get_nth_arg(client->hbuf, client->hbuf_len, j, &opt, &optlen); j++) {
        if (optlen == 4 && !strncasecmp(opt, "AUTH", 4) &&
            resp_get_nth_arg(client->hbuf, client->hbuf_len, j + 1, &user, &userlen) &&
            resp_get_nth_arg(client->hbuf, client->hbuf_len, j + 2, &pass, &passlen)) {
            j += 2;
        } else if (optlen == 7 && !strncasecmp(opt, "SETNAME", 7) &&
                   resp_get_nth_arg(client->hbuf, client->hbuf_len, j + 1, &a1, &a1len)) {
            name = a1;
            namelen = a1len;
            j += 1;
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "Syntax error in HELLO option '%.*s'", optlen < 64 ? optlen : 64, opt);
            client_send_error(w, client, msg);
            client->hbuf_len = 0;
            return;
        }
    }

    if (name && client_name_check(w, client, name, namelen) < 0) return;
    if (user && (userlen >= (int)sizeof(client->acl_user) || passlen >= (int)sizeof(client->acl_pass))) {
        client_send_error(w, client, "invalid username/password length");
        client->hbuf_len = 0;
        return;
    }

    /* Everything is valid: hold back its effects for hello_commit(). hbuf
     * is reused once hbuf_len is reset, so copy what's needed out of it. */
    client->hello_pending_setname = (name != NULL);
    if (name) {
        memcpy(client->hello_pending_name, name, (size_t)namelen);
        client->hello_pending_namelen = namelen;
    }

    if (!has_protover) {
        /* Bare HELLO: report current state, no change. */
        hello_commit(w, client, 0);
        return;
    }
    int ver = protover[0] - '0';

    if (user) {
        if (userlen == 7 && strncasecmp(user, "default", 7) == 0) {
            /* Like AUTH default: never checked against the backend (see
             * acl.c's header comment), just a held-back identity switch. */
            client->hello_pending_auth_default = 1;
        } else {
            memcpy(client->hello_auth_user, user, (size_t)userlen);
            client->hello_auth_userlen = userlen;
            memcpy(client->hello_auth_pass, pass, (size_t)passlen);
            client->hello_auth_passlen = passlen;
            client->hello_auth_protover = ver;
            hello_start_auth_login(w, client);
            return;
        }
    }

    if (ver == 2 || backend_resp3_active_load()) {
        hello_commit(w, client, ver);
        return;
    }

    /* First "HELLO 3" anywhere in the process (or the previous probe
     * failed and this is a retry): probe one backend node for real. */
    int ni = topo_first_master(w);
    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }
    ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
    PoolConn *conn = pool_acquire_nb(w, pool);
    if (!conn) {
        /* Deliberately not queued on pool->wait_queue: a wait-queue hand-off
         * always resumes through resume_client_after_conn(), which would
         * misinterpret this as an ordinary business command (forwarding
         * whatever's in hbuf, i.e. this same "HELLO 3") instead of a probe --
         * the backend's HELLO reply would then be relayed straight back to
         * this client without ever flipping g_backend_resp3_active. Fail
         * fast instead; the client can just resend HELLO 3. */
        hello_pending_clear(client);
        client_send_error(w, client, "RESP3 probe: connection pool exhausted, try again");
        client->hbuf_len = 0;
        return;
    }
    conn->waiting_client = client;
    client->pending_conn = conn;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
    /* The held-back state is committed by handle_resp3_handshake_readable()
     * only if the probe succeeds. */
    if (resp3_start_probe(w, conn) < 0) {
        hello_pending_clear(client);
        conn->waiting_client = NULL;
        client->pending_conn = NULL;
        pool_release(w, conn);
        client_send_error(w, client, "out of memory");
        client->hbuf_len = 0;
        return;
    }
    client->hbuf_len = 0;
    client->state = CS_WAITING_RESP;
    fd_unwatch(w, client->fd, EL_READABLE);
}
