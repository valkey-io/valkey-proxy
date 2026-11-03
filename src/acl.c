#include "proxy.h"

/* ── ACL / AUTH ──────────────────────────────────────────────────────────
 * Manages per-client, per-node dedicated POOL_ACL connections. Once a client
 * authenticates as a non-default user, its single-key / same-slot multi-key /
 * key-based blocking commands stop using the shared per-node pools and
 * instead use a connection that's exclusively theirs and already AUTH'd as
 * that user (lazily opened the first time that client's traffic touches a
 * given node -- see acl_route_business_cmd()). Fan-out/no-key commands,
 * pub/sub, and transactions are out of scope and keep using the default
 * identity via the shared pools even for an ACL-authenticated client.
 *
 * "HELLO <ver> AUTH user pass" logs in the same way (see hello.c), but
 * atomically: its probe conn stays out of acl_conns[] until it succeeds,
 * so a failed HELLO AUTH -- unlike a failed AUTH -- keeps whatever identity
 * the client already had.
 *
 * AUTH default's success is never checked against the backend: there is no
 * "default identity" probe connection to validate it against, and the shared
 * pools never AUTH at all (pool_new_fd() only sends SELECT), so this feature
 * already assumes the backend's default user is nopass.
 */

static void acl_reply_ok(Worker *w, ClientConn *client) {
    static const char OK[] = "+OK\r\n";
    if (client->wbuf_cap < (int)sizeof(OK) - 1) {
        char *tmp = realloc(client->wbuf, sizeof(OK));
        if (!tmp) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        client->wbuf = tmp;
        client->wbuf_cap = (int)sizeof(OK);
    }
    memcpy(client->wbuf, OK, sizeof(OK) - 1);
    client->wbuf_len = (int)sizeof(OK) - 1;
    client->wbuf_off = 0;
    client->pbuf_len = 0;
    client->hbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_WRITING_RESP;
    fd_watch(w, client->fd, EL_WRITABLE);
}

static void acl_soft_fail(Worker *w, ClientConn *client, const char *msg) {
    client_send_error(w, client, msg);
    client->pbuf_len = 0;
    client->hbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_READING_HDR;
    fd_watch(w, client->fd, EL_READABLE);
}

void acl_teardown_all(Worker *w, ClientConn *client) {
    for (int i = 0; i < w->topo.node_count; i++) {
        if (client->acl_conns[i]) {
            PoolConn *c = client->acl_conns[i];
            c->waiting_client = NULL;
            c->acl_authenticating = 0;
            c->acl_is_login_probe = 0;
            client->acl_conns[i] = NULL;
            pool_destroy_conn(w, c);
        }
    }
}

/* Top-level AUTH command: client->pbuf/pbuf_len holds the fully-buffered
 * "AUTH [user] pass" command. */
void handle_auth_cmd(Worker *w, ClientConn *client) {
    const char *args[8];
    int arglens[8];
    int nargs = resp_extract_all_args(client->pbuf, client->pbuf_len, args, arglens, 8);

    const char *user, *pass;
    int userlen, passlen;
    if (nargs == 1) {
        user = "default";
        userlen = 7;
        pass = args[0];
        passlen = arglens[0];
    } else if (nargs == 2) {
        user = args[0];
        userlen = arglens[0];
        pass = args[1];
        passlen = arglens[1];
    } else {
        acl_soft_fail(w, client, "wrong number of arguments for 'auth' command");
        return;
    }

    if (userlen < 0 || passlen < 0 || userlen >= (int)sizeof(client->acl_user) ||
        passlen >= (int)sizeof(client->acl_pass)) {
        acl_soft_fail(w, client, "invalid username/password length");
        return;
    }

    int is_default = (userlen == 7 && strncasecmp(user, "default", 7) == 0);

    if (is_default) {
        if (client->acl_authenticated) acl_teardown_all(w, client);
        client->acl_authenticated = 0;
        client->acl_userlen = 0;
        client->acl_passlen = 0;
        acl_reply_ok(w, client);
        return;
    }

    if (client->in_subscribe) {
        acl_soft_fail(w, client, "can't AUTH while subscribed to channels");
        return;
    }

    if (client->acl_authenticated) acl_teardown_all(w, client);

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
        acl_soft_fail(w, client, "ACL connection pool exhausted");
        return;
    }

    /* NUL-terminated: CLIENT INFO prints acl_user with %s (fanout.c). */
    memcpy(client->acl_user, user, (size_t)userlen);
    client->acl_user[userlen] = '\0';
    client->acl_userlen = userlen;
    memcpy(client->acl_pass, pass, (size_t)passlen);
    client->acl_passlen = passlen;

    const char *aargs[2] = {client->acl_user, client->acl_pass};
    int aarglens[2] = {userlen, passlen};
    int authlen;
    char *authcmd = build_resp_cmd("AUTH", 4, aargs, aarglens, 2, &authlen);
    if (!authcmd) {
        pool_destroy_conn(w, conn);
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return;
    }
    if (conn->wbuf_cap < authlen) {
        char *tmp = realloc(conn->wbuf, (size_t)authlen);
        if (!tmp) {
            free(authcmd);
            pool_destroy_conn(w, conn);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        conn->wbuf = tmp;
        conn->wbuf_cap = authlen;
    }
    memcpy(conn->wbuf, authcmd, (size_t)authlen);
    conn->wbuf_len = authlen;
    conn->wbuf_off = 0;
    free(authcmd);

    conn->waiting_client = client;
    conn->acl_authenticating = 1;
    conn->acl_is_login_probe = 1;
    client->acl_conns[ni] = conn;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;

    client->pbuf_len = 0;
    client->hbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_WAITING_RESP;
    fd_unwatch(w, client->fd, EL_READABLE);
    fd_watch(w, conn->fd, EL_WRITABLE);
}

/* Shared entry point for single-key / same-slot multi-key / key-based
 * blocking commands once client->acl_authenticated is set. Routes through
 * (lazily establishing, if needed) client->acl_conns[node_idx]. */
int acl_route_business_cmd(Worker *w, ClientConn *client, int node_idx) {
    if (node_idx < 0 || node_idx >= w->topo.node_count) {
        acl_soft_fail(w, client, "slot not mapped");
        return 0;
    }
    client->node_idx = node_idx;

    PoolConn *conn = client->acl_conns[node_idx];
    if (conn) {
        conn->waiting_client = client;
        client->pending_conn = conn;
        resume_client_after_conn(w, client, conn);
        return 1;
    }

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return 0;
    }
    ConnPool *pool = &grp->node_pools[node_idx].pools[POOL_ACL];
    conn = pool_acquire_nb(w, pool);
    if (!conn) {
        acl_soft_fail(w, client, "ACL connection pool exhausted for this node");
        return 0;
    }

    const char *args[2] = {client->acl_user, client->acl_pass};
    int arglens[2] = {client->acl_userlen, client->acl_passlen};
    int authlen;
    char *authcmd = build_resp_cmd("AUTH", 4, args, arglens, 2, &authlen);
    if (!authcmd) {
        pool_destroy_conn(w, conn);
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return 0;
    }
    if (conn->wbuf_cap < authlen) {
        char *tmp = realloc(conn->wbuf, (size_t)authlen);
        if (!tmp) {
            free(authcmd);
            pool_destroy_conn(w, conn);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return 0;
        }
        conn->wbuf = tmp;
        conn->wbuf_cap = authlen;
    }
    memcpy(conn->wbuf, authcmd, (size_t)authlen);
    conn->wbuf_len = authlen;
    conn->wbuf_off = 0;
    free(authcmd);

    conn->waiting_client = client;
    conn->acl_authenticating = 1;
    conn->acl_is_login_probe = 0;
    client->acl_conns[node_idx] = conn;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;

    /* client's real business command stays buffered in hbuf/pbuf untouched;
     * handle_acl_conn_readable() forwards it once this handshake succeeds. */
    client->state = CS_WAITING_RESP;
    fd_unwatch(w, client->fd, EL_READABLE);
    fd_watch(w, conn->fd, EL_WRITABLE);
    return 1;
}

/* ── ACL <subcommand> (top-level command, not to be confused with a
 * client's per-node authenticated business-command connection above) ──
 * All six subcommands are forwarded verbatim to a single node and the reply
 * relayed as-is (same "any one node, relay reply as-is" idiom used elsewhere
 * for RANDOMKEY/INFO/COMMAND HELP/etc.), via this shared, default-identity
 * connection. HELP always uses it regardless of the client's own AUTH state
 * (its reply is proxy-substituted anyway). LIST/USERS/WHOAMI/GETUSER/DRYRUN
 * instead prefer the client's pinned, already-authenticated connection (see
 * acl_route_business_cmd() above) once it has AUTH'd as a non-default user,
 * so WHOAMI/permission-gated replies reflect that identity, not "default". */
static void acl_forward_to_one_node(Worker *w, ClientConn *client) {
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
        client->node_idx = ni;
        client->state = CS_WAITING_CONN;
        fd_unwatch(w, client->fd, EL_READABLE);
        pool_enqueue_waiter(pool, client);
        return;
    }
    conn->waiting_client = client;
    client->pending_conn = conn;
    resume_client_after_conn(w, client, conn);
}

void handle_acl_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "wrong number of arguments for 'acl' command");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    if (!strcmp(subcmd, "HELP")) {
        client->waiting_acl_help = 1;
        acl_forward_to_one_node(w, client);
        return;
    }
    if (!strcmp(subcmd, "LIST") || !strcmp(subcmd, "USERS") || !strcmp(subcmd, "WHOAMI") ||
        !strcmp(subcmd, "GETUSER") || !strcmp(subcmd, "DRYRUN")) {
        /* A client that has AUTH'd as a non-default user must see its own
         * identity/permissions reflected (WHOAMI in particular is otherwise
         * meaningless), so route through its pinned, already-authenticated
         * per-node connection instead of the shared default-identity pool. */
        if (client->acl_authenticated) {
            int ni = topo_first_master(w);
            acl_route_business_cmd(w, client, ni);
            return;
        }
        acl_forward_to_one_node(w, client);
        return;
    }

    client_send_error(w, client, "unknown ACL subcommand");
    client->hbuf_len = 0;
}

/* handle_backend_readable()'s "this reply is for an ACL HELP probe" branch:
 * relay a backend error verbatim, otherwise discard the backend's own HELP
 * text and answer with the proxy's fixed ACL HELP array instead. */
void handle_acl_help_readable(Worker *w, PoolConn *conn) {
    ClientConn *cl = conn->waiting_client;
    if (!cl) {
        fd_unwatch(w, conn->fd, EL_READABLE);
        return;
    }

    int rlen;
    for (;;) {
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                client_send_error(w, cl, "response too large");
                close_client(w, cl);
                return;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                client_send_error(w, cl, "out of memory");
                close_client(w, cl);
                return;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }
        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            close_client(w, cl);
            return;
        }
        if (n == 0) {
            close_client(w, cl);
            return;
        }
        conn->rbuf_len += (int)n;

        rlen = resp_response_len(conn->rbuf, conn->rbuf_len);
        if (rlen < 0) {
            client_send_error(w, cl, "bad backend response");
            close_client(w, cl);
            return;
        }
        if (rlen > 0) break;
    }

    int is_error = (conn->rbuf[0] == '-');

    if (is_error) {
        if (cl->wbuf_cap < rlen) {
            char *tmp = realloc(cl->wbuf, (size_t)rlen);
            if (!tmp) {
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
    }

    conn->rbuf_len = 0;
    fd_unwatch(w, conn->fd, EL_READABLE);
    conn->waiting_client = NULL;
    pool_release(w, conn);
    cl->pending_conn = NULL;
    cl->waiting_acl_help = 0;

    if (is_error) {
        cl->state = CS_WRITING_RESP;
        fd_watch(w, cl->fd, EL_WRITABLE);
        return;
    }

    static const char *lines[] = {
        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "DRYRUN <username> <command> [<arg> ...]",
        "    Returns whether the user can execute the given command without executing the command.",
        "GETUSER <username>",
        "    Get the user's details.",
        "LIST",
        "    Show users details in config file format.",
        "USERS",
        "    List all the registered usernames.",
        "WHOAMI",
        "    Return the current connection username.",
        "HELP",
        "    Print this help.",
    };
    send_multibulk_reply(w, cl, lines, (int)(sizeof(lines) / sizeof(lines[0])));
}

/* handle_backend_readable()'s POOL_ACL special case. */
void handle_acl_conn_readable(Worker *w, PoolConn *conn) {
    ClientConn *cl = conn->waiting_client;
    if (!cl) {
        fd_unwatch(w, conn->fd, EL_READABLE);
        return;
    }

    /* Drain everything already buffered on this backend fd in one go
     * (edge-triggered epoll won't re-notify for a burst that already
     * arrived just because our per-call cap didn't fully consume it). */
    int rlen;
    for (;;) {
        if (conn->rbuf_cap - conn->rbuf_len < 4096) {
            int nc = conn->rbuf_cap ? conn->rbuf_cap * 2 : 4096;
            if (nc > MAX_RESP_SIZE) {
                client_send_error(w, cl, "response too large");
                close_client(w, cl);
                return;
            }
            char *tmp = realloc(conn->rbuf, (size_t)nc);
            if (!tmp) {
                client_send_error(w, cl, "out of memory");
                close_client(w, cl);
                return;
            }
            conn->rbuf = tmp;
            conn->rbuf_cap = nc;
        }

        ssize_t n = read(conn->fd, conn->rbuf + conn->rbuf_len, (size_t)(conn->rbuf_cap - conn->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return;
            close_client(w, cl);
            return;
        }
        if (n == 0) {
            close_client(w, cl);
            return;
        }
        conn->rbuf_len += (int)n;

        rlen = resp_response_len(conn->rbuf, conn->rbuf_len);
        if (rlen < 0) {
            client_send_error(w, cl, "bad backend response");
            close_client(w, cl);
            return;
        }
        if (rlen > 0) break;
    }

    int ok = (conn->rbuf[0] == '+');

    if (conn->acl_authenticating && conn->acl_hello_login) {
        handle_hello_auth_reply(w, conn, cl, rlen);
        return;
    }

    if (conn->acl_authenticating) {
        int is_login_probe = conn->acl_is_login_probe;
        int node_idx = conn->pool->node_idx;
        conn->acl_authenticating = 0;
        conn->acl_is_login_probe = 0;

        if (!ok) {
            /* Login probe: relay the real backend error (WRONGPASS/etc.)
             * verbatim so the client sees the authoritative reason. A lazy
             * per-node reconnect failure is NOT something the client asked
             * for (their in-flight command is e.g. GET, not AUTH), so it
             * gets a synthesized error instead of a raw AUTH rejection. */
            if (is_login_probe) {
                if (cl->wbuf_cap < rlen) {
                    char *tmp = realloc(cl->wbuf, (size_t)rlen);
                    if (!tmp) {
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
                cl->acl_authenticated = 0;
                cl->acl_userlen = 0;
                cl->acl_passlen = 0;
            }
            conn->rbuf_len = 0;
            cl->acl_conns[node_idx] = NULL;
            conn->waiting_client = NULL;
            pool_destroy_conn(w, conn);

            if (is_login_probe) {
                cl->pbuf_len = 0;
                cl->hbuf_len = 0;
                cl->using_pbuf = 0;
                cl->state = CS_WRITING_RESP;
                fd_watch(w, cl->fd, EL_WRITABLE);
            } else {
                acl_soft_fail(w, cl, "ACL authentication to backend node failed");
            }
            return;
        }

        conn->rbuf_len = 0;
        if (is_login_probe) {
            cl->acl_authenticated = 1;
            fd_unwatch(w, conn->fd, EL_READABLE);
            acl_reply_ok(w, cl);
        } else {
            cl->pending_conn = conn;
            resume_client_after_conn(w, cl, conn);
        }
        return;
    }

    /* Genuine business-command reply on an already-authenticated, pinned
     * connection: relay it, but -- unlike the generic handle_backend_readable
     * path -- never pool_release() it; it stays pinned to cl. */
    {
        int dr = deliver_backend_reply(w, cl, conn, conn->rbuf, rlen);
        if (dr == -2) return; /* already client_send_error()+close_client()'d */
        if (dr < 0) {
            client_send_error(w, cl, "bad backend response");
            close_client(w, cl);
            return;
        }
    }
    conn->rbuf_len = 0;

    fd_unwatch(w, conn->fd, EL_READABLE);
    cl->pending_conn = NULL;
    cl->state = CS_WRITING_RESP;
    fd_watch(w, cl->fd, EL_WRITABLE);
}
