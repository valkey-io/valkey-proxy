#include "proxy.h"
#include <limits.h>
#include <sys/stat.h>

/* ── PROXY CONFIG ───────────────────────────────────────────────────────
 * Reads and changes the proxy's own proxy.conf settings at runtime, the
 * way CONFIG GET/SET does for Valkey (CONFIG itself still goes to the
 * backends):
 *
 *   PROXY CONFIG GET <pattern> [<pattern> ...]
 *   PROXY CONFIG SET <parameter> <value> [<parameter> <value> ...]
 *   PROXY CONFIG SAVE [<file>]
 *   PROXY CONFIG HELP
 *
 * Only settings the proxy reads afresh each time it needs them can be set;
 * the rest (listen address, worker count, fd table size, multiplexing...)
 * are fixed at startup and GET-only. SET checks every pair before changing
 * anything, so a bad pair leaves all of them as they were. Changes live in
 * memory only until PROXY CONFIG SAVE writes them out (see below).
 *
 * Plain settings take effect on their next read. The min-/max-*-connection
 * ones live in g_pool_min/g_pool_max, but each worker owns its pools, so
 * SET only bumps g_pool_config_version and wakes every worker; each one
 * then resizes its own pools at the top of its event loop (see
 * worker_apply_pool_config() in pool.c): lowering max closes idle
 * connections past it (busy ones as they're released), raising min opens
 * connections up to it, raising max lets parked clients have the room.
 *
 * Every subcommand is for an authenticated ACL user named in proxy.conf
 * only: proxy-config-admin-users may run all of them, proxy-config-
 * readonly-users GET and HELP (see may_run()).
 *
 * Writes are serialized by config_lock and done with atomic stores; the
 * existing readers of these ints read them plainly. */

typedef enum { P_INT, P_BOOL, P_STR } ParamType;

typedef struct {
    const char *name;
    ParamType type;
    void *ptr;    /* int * (P_INT, P_BOOL) or char * (P_STR) */
    int settable; /* 0 = fixed at startup, GET only */
    int min;      /* smallest value SET accepts (P_INT) */
    int pool;     /* pool type for min-/max-*-connection, else -1 */
    int pool_max; /* 1 = max-*-connection, 0 = min-*-connection */
} Param;

#define RO_INT(n, p) \
    { n, P_INT, p, 0, 0, -1, 0 }
#define RO_BOOL(n, p) \
    { n, P_BOOL, p, 0, 0, -1, 0 }
#define RO_STR(n, p) \
    { n, P_STR, p, 0, 0, -1, 0 }
#define RW_INT(n, p, lo) \
    { n, P_INT, p, 1, lo, -1, 0 }
#define RW_POOL_MIN(n, pt) \
    { n, P_INT, &g_pool_min[pt], 1, 0, pt, 0 }
#define RW_POOL_MAX(n, pt) \
    { n, P_INT, &g_pool_max[pt], 1, 1, pt, 1 }

/* In proxy.conf order; GET answers in this order. tls-key is left out on
 * purpose. */
static const Param PARAMS[] = {
    RO_STR("server-ip", g_config.server_ip),
    RO_INT("server-port", &g_config.server_port),
    RW_INT("cluster-refresh-interval-ms", &g_config.cluster_refresh_interval_ms, 0),
    RW_INT("cluster-node-fail-grace-ms", &g_config.cluster_node_fail_grace_ms, 0),
    RW_INT("backend-connect-timeout-ms", &g_config.backend_connect_timeout_ms, 1),
    RO_STR("bind", g_config.bind_ip),
    RO_INT("port", &g_config.proxy_port),
    RO_STR("unix-socket", g_config.unix_socket_path),
    RO_INT("max-connection", &g_config.max_connection),
    RW_INT("max-multikey-payload", &g_config.max_multikey_payload, 1),
    RO_INT("worker-thread-no", &g_config.worker_thread_no),
    RO_INT("db", &g_config.db),
    RO_BOOL("backend-multiplexing", &g_config.backend_multiplexing),
    RO_INT("shared-connection-per-node", &g_config.shared_connection_per_node),
    RW_INT("max-pipeline-inflight", &g_config.max_pipeline_inflight, 1),
    RW_POOL_MIN("min-normal-connection", POOL_NORMAL),
    RW_POOL_MAX("max-normal-connection", POOL_NORMAL),
    RW_POOL_MIN("min-blocking-connection", POOL_BLOCKING),
    RW_POOL_MAX("max-blocking-connection", POOL_BLOCKING),
    RW_POOL_MIN("min-subscription-connection", POOL_SUBSCRIPTION),
    RW_POOL_MAX("max-subscription-connection", POOL_SUBSCRIPTION),
    RW_POOL_MIN("min-transaction-connection", POOL_TRANSACTION),
    RW_POOL_MAX("max-transaction-connection", POOL_TRANSACTION),
    RW_INT("transaction-replay-buffer-size", &g_config.transaction_replay_buffer_size, 0),
    RW_POOL_MIN("min-acl-connection", POOL_ACL),
    RW_POOL_MAX("max-acl-connection", POOL_ACL),
    RO_BOOL("tls-enabled", &g_config.tls_enabled),
    RO_STR("tls-cert", g_config.tls_cert),
    RO_STR("reported-version", g_config.reported_version),
    RO_STR("reported-role", g_config.reported_role),
    RO_STR("reported-modules", g_config.reported_modules),
    RO_STR("proxy-config-admin-users", g_config.proxy_config_admin_users),
    RO_STR("proxy-config-readonly-users", g_config.proxy_config_readonly_users),
};
#define NPARAMS ((int)(sizeof(PARAMS) / sizeof(PARAMS[0])))

static pthread_mutex_t config_lock = PTHREAD_MUTEX_INITIALIZER;

static int int_load(const Param *p) {
    return __atomic_load_n((int *)p->ptr, __ATOMIC_RELAXED);
}

/* Settable parameters' values right after parse_conf(), indexed like
 * PARAMS: SAVE appends a parameter the file doesn't mention only if it no
 * longer has this value. */
static int startup_val[sizeof(PARAMS) / sizeof(PARAMS[0])];

void proxy_config_init(void) {
    for (int i = 0; i < NPARAMS; i++)
        if (PARAMS[i].settable) startup_val[i] = int_load(&PARAMS[i]);
}

/* Case-insensitive glob over pattern[0..plen) with *, ? and \ escapes,
 * enough for CONFIG GET's patterns (no [] classes). */
static int glob_match(const char *pat, int plen, const char *s) {
    while (plen > 0) {
        if (*pat == '*') {
            while (plen > 1 && pat[1] == '*') pat++, plen--;
            if (plen == 1) return 1;
            for (const char *t = s;; t++) {
                if (glob_match(pat + 1, plen - 1, t)) return 1;
                if (!*t) return 0;
            }
        }
        if (!*s) return 0;
        if (*pat == '?') {
            pat++, plen--, s++;
            continue;
        }
        if (*pat == '\\' && plen > 1) pat++, plen--;
        if (tolower((unsigned char)*pat) != tolower((unsigned char)*s)) return 0;
        pat++, plen--, s++;
    }
    return *s == '\0';
}

static int name_eq(const Param *p, const char *a, int al) {
    return (int)strlen(p->name) == al && !strncasecmp(p->name, a, (size_t)al);
}

static const Param *param_find(const char *a, int al) {
    for (int i = 0; i < NPARAMS; i++)
        if (name_eq(&PARAMS[i], a, al)) return &PARAMS[i];
    return NULL;
}

static int gb_bulk(GrowBuf *g, const char *s, int len) {
    if (gb_append_fmt(g, "$%d\r\n", len) < 0 || gb_append(g, s, len) < 0) return -1;
    return gb_append(g, "\r\n", 2);
}

static void send_ok(Worker *w, ClientConn *client) {
    char *r = strdup("+OK\r\n");
    if (!r) {
        client_reject_error(w, client, "out of memory");
        return;
    }
    fanout_send_response(w, client, r, 5);
}

/* ── GET ── */
static void proxy_config_get(Worker *w, ClientConn *client) {
    int nargs = 0;
    const char *a;
    int al;
    while (resp_get_nth_arg(client->hbuf, client->hbuf_len, nargs, &a, &al)) nargs++;
    if (nargs < 4) {
        client_reject_error(w, client, "wrong number of arguments for 'proxy|config|get' command");
        return;
    }
    int match[NPARAMS], nmatch = 0;
    for (int i = 0; i < NPARAMS; i++) {
        match[i] = 0;
        for (int k = 3; k < nargs && !match[i]; k++) {
            resp_get_nth_arg(client->hbuf, client->hbuf_len, k, &a, &al);
            match[i] = glob_match(a, al, PARAMS[i].name);
        }
        nmatch += match[i];
    }

    GrowBuf g = {0};
    int rc = client->resp == 3 ? gb_append_fmt(&g, "%%%d\r\n", nmatch) : gb_append_fmt(&g, "*%d\r\n", nmatch * 2);
    for (int i = 0; i < NPARAMS && rc == 0; i++) {
        if (!match[i]) continue;
        const Param *p = &PARAMS[i];
        char num[16];
        const char *val = num;
        if (p->type == P_STR)
            val = (const char *)p->ptr;
        else if (p->type == P_BOOL)
            val = int_load(p) ? "yes" : "no";
        else
            snprintf(num, sizeof(num), "%d", int_load(p));
        if (gb_bulk(&g, p->name, (int)strlen(p->name)) < 0 || gb_bulk(&g, val, (int)strlen(val)) < 0) rc = -1;
    }
    if (rc < 0) {
        free(g.buf);
        client_reject_error(w, client, "out of memory");
        return;
    }
    fanout_send_response(w, client, g.buf, g.len);
}

/* ── SET ── */
static int set_fail(Worker *w, ClientConn *client, const char *a, int al, const char *why) {
    char msg[256];
    snprintf(msg, sizeof(msg), "CONFIG SET failed (possibly related to argument '%.*s') - %s", al > 64 ? 64 : al, a,
             why);
    client_reject_error(w, client, msg);
    return -1;
}

static void proxy_config_set(Worker *w, ClientConn *client) {
    int nargs = 0;
    const char *a, *v;
    int al, vl;
    while (resp_get_nth_arg(client->hbuf, client->hbuf_len, nargs, &a, &al)) nargs++;
    if (nargs < 5 || (nargs - 3) % 2 != 0) {
        client_reject_error(w, client, "wrong number of arguments for 'proxy|config|set' command");
        return;
    }
    int npairs = (nargs - 3) / 2;
    const Param *which[NPARAMS];
    int newval[NPARAMS];

    /* Under the lock from validation through the stores, so two SETs on
     * different workers can't each pass the min <= max check against the
     * other's old value. Nothing below blocks. */
    pthread_mutex_lock(&config_lock);
    int pmin[POOL_TYPE_COUNT], pmax[POOL_TYPE_COUNT];
    for (int t = 0; t < POOL_TYPE_COUNT; t++) {
        pmin[t] = pool_min_load(t);
        pmax[t] = pool_max_load(t);
    }
    for (int i = 0; i < npairs; i++) {
        resp_get_nth_arg(client->hbuf, client->hbuf_len, 3 + 2 * i, &a, &al);
        resp_get_nth_arg(client->hbuf, client->hbuf_len, 4 + 2 * i, &v, &vl);
        const Param *p = param_find(a, al);
        if (!p) {
            char msg[160];
            snprintf(msg, sizeof(msg), "Unknown option or number of arguments for CONFIG SET - '%.*s'",
                     al > 64 ? 64 : al, a);
            client_reject_error(w, client, msg);
            goto out;
        }
        if (!p->settable) {
            set_fail(w, client, a, al, "can't set immutable config");
            goto out;
        }
        for (int j = 0; j < i; j++)
            if (which[j] == p) {
                set_fail(w, client, a, al, "duplicate parameter");
                goto out;
            }
        char buf[24];
        char *end;
        if (vl <= 0 || vl >= (int)sizeof(buf)) {
            set_fail(w, client, a, al, "argument must be an integer");
            goto out;
        }
        memcpy(buf, v, (size_t)vl);
        buf[vl] = '\0';
        errno = 0;
        long long n = strtoll(buf, &end, 10);
        if (errno || *end || n > INT_MAX || n < INT_MIN) {
            set_fail(w, client, a, al, "argument must be an integer");
            goto out;
        }
        if (n < p->min) {
            char why[64];
            snprintf(why, sizeof(why), "argument must be %d or greater", p->min);
            set_fail(w, client, a, al, why);
            goto out;
        }
        /* Each max-*-connection is per node, per db, per worker, and
         * every connection takes an fd from max-connection's budget. */
        if (p->pool >= 0 && n > g_config.max_connection) {
            char why[80];
            snprintf(why, sizeof(why), "argument can't exceed max-connection (%d)", g_config.max_connection);
            set_fail(w, client, a, al, why);
            goto out;
        }
        which[i] = p;
        newval[i] = (int)n;
        if (p->pool >= 0) (p->pool_max ? pmax : pmin)[p->pool] = (int)n;
    }
    for (int i = 0; i < npairs; i++) {
        const Param *p = which[i];
        if (p->pool >= 0 && pmin[p->pool] > pmax[p->pool]) {
            char why[96];
            snprintf(why, sizeof(why), "min-%s-connection (%d) can't exceed max-%s-connection (%d)", POOL_NAME[p->pool],
                     pmin[p->pool], POOL_NAME[p->pool], pmax[p->pool]);
            set_fail(w, client, p->name, (int)strlen(p->name), why);
            goto out;
        }
    }

    int pools_changed = 0, refresh_on = 0;
    for (int i = 0; i < npairs; i++) {
        const Param *p = which[i];
        int old = int_load(p);
        if (old == newval[i]) continue;
        __atomic_store_n((int *)p->ptr, newval[i], __ATOMIC_RELAXED);
        printf("[config] %s %d -> %d (PROXY CONFIG SET from client id=%llu)\n", p->name, old, newval[i],
               (unsigned long long)client->id);
        if (p->pool >= 0) pools_changed = 1;
        if (p->ptr == &g_config.cluster_refresh_interval_ms && newval[i] > 0) refresh_on = 1;
    }
    if (pools_changed) atomic_fetch_add_explicit(&g_pool_config_version, 1, memory_order_release);
    pthread_mutex_unlock(&config_lock);

    /* Every worker, this one included, resizes its pools on waking. */
    if (pools_changed) wake_all_workers();
    /* Refresh was off at startup, so its thread never started. */
    if (refresh_on) topology_start_refresh_thread();
    send_ok(w, client);
    return;
out:
    pthread_mutex_unlock(&config_lock);
}

/* ── SAVE ──
 * Writes the startup config file again with the settable parameters' current
 * values, to the startup file itself (no argument) or to <file>. The startup
 * file is the template either way, so the result is a whole config the proxy
 * can start from: every other line, comments included, is kept as is, and a
 * settable parameter keeps its place and only gets its value replaced
 * (whatever followed the value on that line is kept too). Settable
 * parameters the file doesn't mention are appended, but only those whose
 * value has changed since startup -- the others are still at the default
 * the file relied on. The file is written beside its destination and
 * renamed over it, so a failure never leaves a half-written config. */

/* Appends src_path, rewritten with vals[], to g. 0 on success, -1 with
 * errno set. */
static int render_config(const char *src_path, const int *vals, GrowBuf *g) {
    FILE *f = fopen(src_path, "r");
    if (!f) return -1;
    int seen[NPARAMS];
    memset(seen, 0, sizeof(seen));
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;
    int rc = 0;
    while (rc == 0 && (len = getline(&line, &cap, f)) > 0) {
        const char *k = line, *end = line + len;
        while (k < end && (*k == ' ' || *k == '\t')) k++;
        const char *ke = k;
        while (ke < end && !isspace((unsigned char)*ke)) ke++;
        int pi = -1;
        if (ke > k && *k != '#')
            for (int i = 0; i < NPARAMS; i++)
                if (PARAMS[i].settable && (int)strlen(PARAMS[i].name) == ke - k &&
                    !memcmp(PARAMS[i].name, k, (size_t)(ke - k)))
                    pi = i;
        if (pi < 0) {
            rc = gb_append(g, line, (int)len);
            if (rc == 0 && line[len - 1] != '\n') rc = gb_append(g, "\n", 1);
            continue;
        }
        seen[pi] = 1;
        const char *v = ke;
        while (v < end && (*v == ' ' || *v == '\t')) v++;
        const char *ve = v;
        while (ve < end && !isspace((unsigned char)*ve)) ve++;
        if (ve == v) { /* key without a value: give it the usual spacing */
            rc = gb_append(g, line, (int)(ke - line));
            if (rc == 0) rc = gb_append_fmt(g, "  %d\n", vals[pi]);
            continue;
        }
        rc = gb_append(g, line, (int)(v - line));
        if (rc == 0) rc = gb_append_fmt(g, "%d", vals[pi]);
        if (rc == 0) rc = gb_append(g, ve, (int)(end - ve));
        if (rc == 0 && line[len - 1] != '\n') rc = gb_append(g, "\n", 1);
    }
    int read_err = ferror(f);
    free(line);
    fclose(f);
    if (read_err) {
        errno = EIO;
        return -1;
    }
    int header = 0;
    for (int i = 0; i < NPARAMS && rc == 0; i++) {
        if (!PARAMS[i].settable || seen[i] || vals[i] == startup_val[i]) continue;
        if (!header++) rc = gb_append_fmt(g, "\n# Added by PROXY CONFIG SAVE\n");
        if (rc == 0) rc = gb_append_fmt(g, "%s  %d\n", PARAMS[i].name, vals[i]);
    }
    if (rc < 0) errno = ENOMEM;
    return rc;
}

/* Writes data to dst through a temporary file renamed over it, keeping an
 * existing dst's permission bits. 0 on success, -1 with errno set. */
static int write_file_atomic(const char *dst, const char *data, int len) {
    char tmp[PATH_MAX + 32];
    if (snprintf(tmp, sizeof(tmp), "%s.tmp-%d", dst, (int)getpid()) >= (int)sizeof(tmp)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    struct stat st;
    mode_t mode = stat(dst, &st) == 0 ? (st.st_mode & 07777) : 0644;
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) return -1;
    int off = 0;
    while (off < len) {
        ssize_t n = write(fd, data + off, (size_t)(len - off));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) goto fail;
        off += (int)n;
    }
    if (fchmod(fd, mode) < 0 || fsync(fd) < 0) goto fail;
    if (close(fd) < 0) {
        fd = -1;
        goto fail;
    }
    fd = -1;
    if (rename(tmp, dst) < 0) goto fail;
    return 0;
fail: {
    int e = errno;
    if (fd >= 0) close(fd);
    unlink(tmp);
    errno = e;
    return -1;
}
}

static void proxy_config_save(Worker *w, ClientConn *client) {
    int nargs = 0;
    const char *a;
    int al;
    while (resp_get_nth_arg(client->hbuf, client->hbuf_len, nargs, &a, &al)) nargs++;
    if (nargs > 4) {
        client_reject_error(w, client, "wrong number of arguments for 'proxy|config|save' command");
        return;
    }
    const char *src = info_config_file();
    char dst[PATH_MAX];
    if (nargs == 4) {
        resp_get_nth_arg(client->hbuf, client->hbuf_len, 3, &a, &al);
        if (al <= 0 || al >= (int)sizeof(dst) || memchr(a, '\0', (size_t)al)) {
            client_reject_error(w, client, "invalid file name");
            return;
        }
        memcpy(dst, a, (size_t)al);
        dst[al] = '\0';
    } else {
        snprintf(dst, sizeof(dst), "%s", src);
    }

    /* Under the lock, so the values are one consistent set and two SAVEs
     * don't race on the same temporary file. */
    pthread_mutex_lock(&config_lock);
    int vals[NPARAMS];
    for (int i = 0; i < NPARAMS; i++) vals[i] = PARAMS[i].settable ? int_load(&PARAMS[i]) : 0;
    GrowBuf g = {0};
    char msg[PATH_MAX + 128];
    int rc = render_config(src, vals, &g);
    if (rc < 0)
        snprintf(msg, sizeof(msg), "can't read the startup config file %s: %s", src, strerror(errno));
    else if ((rc = write_file_atomic(dst, g.buf ? g.buf : "", g.len)) < 0)
        snprintf(msg, sizeof(msg), "can't write %s: %s", dst, strerror(errno));
    pthread_mutex_unlock(&config_lock);
    free(g.buf);

    if (rc < 0) {
        client_reject_error(w, client, msg);
        return;
    }
    printf("[config] PROXY CONFIG SAVE wrote %s (client id=%llu)\n", dst, (unsigned long long)client->id);
    send_ok(w, client);
}

/* ── HELP ── */
static void proxy_config_help(Worker *w, ClientConn *client) {
    const char *extra;
    int extralen;
    if (resp_get_nth_arg(client->hbuf, client->hbuf_len, 3, &extra, &extralen)) {
        client_reject_error(w, client, "wrong number of arguments for 'proxy|config|help' command");
        return;
    }
    static const char *head[] = {
        "PROXY CONFIG <subcommand> [<arg> [value] ...]. Subcommands are:",
        "GET <pattern> [<pattern> ...]",
        "    Return the proxy's own settings whose names match any of the glob-style",
        "    patterns, as name/value pairs.",
        "SET <parameter> <value> [<parameter> <value> ...]",
        "    Change settings at runtime, all or none. Only the parameters listed below",
        "    can be set; changes are lost on restart unless saved.",
        "SAVE [<file>]",
        "    Write the startup config file again with the current values: back over",
        "    itself, or to <file> when one is given.",
        "HELP",
        "    Print this help.",
        "Access: users AUTHed as one in proxy-config-admin-users may run every",
        "subcommand; those in proxy-config-readonly-users only GET and HELP.",
        "Parameters settable at runtime:",
    };
    enum { NHEAD = sizeof(head) / sizeof(head[0]) };
    const char *lines[NHEAD + NPARAMS];
    char buf[NPARAMS][96];
    int n = 0;
    for (; n < NHEAD; n++) lines[n] = head[n];
    for (int i = 0; i < NPARAMS; i++) {
        const Param *p = &PARAMS[i];
        if (!p->settable) continue;
        if (p->pool >= 0 && p->pool_max)
            snprintf(buf[i], sizeof(buf[i]), "    %-32s %d..max-connection, >= min-%s-connection", p->name, p->min,
                     POOL_NAME[p->pool]);
        else if (p->pool >= 0)
            snprintf(buf[i], sizeof(buf[i]), "    %-32s >= %d, <= max-%s-connection", p->name, p->min,
                     POOL_NAME[p->pool]);
        else
            snprintf(buf[i], sizeof(buf[i]), "    %-32s >= %d", p->name, p->min);
        lines[n++] = buf[i];
    }
    send_multibulk_reply(w, client, lines, n);
}

/* ── who may run what ──
 * Authentication stays with Valkey: acl_authenticated means the backend
 * accepted AUTH <user> <password>. Whether that user may run PROXY CONFIG
 * is the proxy's own decision, from proxy-config-admin-users (every
 * subcommand) and proxy-config-readonly-users (GET and HELP), whatever the
 * user's ACL rules on the backend say. The default identity is never let
 * in, so with neither setting nobody can run PROXY CONFIG. */
typedef enum { SUB_GET, SUB_SET, SUB_SAVE, SUB_HELP } Sub;

static const char *sub_name(Sub sub) {
    static const char *names[] = {"proxy|config|get", "proxy|config|set", "proxy|config|save", "proxy|config|help"};
    return names[sub];
}

static int user_listed(const char *list, const char *user, int userlen) {
    for (const char *p = list; *p;) {
        const char *e = strchr(p, ',');
        int n = e ? (int)(e - p) : (int)strlen(p);
        if (n == userlen && !memcmp(p, user, (size_t)n)) return 1;
        if (!e) break;
        p = e + 1;
    }
    return 0;
}

/* 1 if client may run sub, else refuses it and returns 0. */
static int may_run(Worker *w, ClientConn *client, Sub sub) {
    char buf[256];
    int n;
    int write_sub = sub == SUB_SET || sub == SUB_SAVE;
    if (!client->acl_authenticated) {
        n = snprintf(
            buf, sizeof(buf), "-NOPERM the default user can't run the '%s' command; AUTH as a user in %s first\r\n",
            sub_name(sub),
            write_sub ? "proxy-config-admin-users" : "proxy-config-admin-users or proxy-config-readonly-users");
    } else if (user_listed(g_config.proxy_config_admin_users, client->acl_user, client->acl_userlen) ||
               (!write_sub &&
                user_listed(g_config.proxy_config_readonly_users, client->acl_user, client->acl_userlen))) {
        return 1;
    } else {
        n = snprintf(buf, sizeof(buf), "-NOPERM User %.*s has no permissions to run the '%s' command (not in %s)\r\n",
                     client->acl_userlen > 64 ? 64 : client->acl_userlen, client->acl_user, sub_name(sub),
                     write_sub ? "proxy-config-admin-users"
                               : "proxy-config-admin-users or proxy-config-readonly-users");
    }
    if (n >= (int)sizeof(buf)) n = (int)sizeof(buf) - 1;
    client_reject_cmd(w, client, buf, n);
    return 0;
}

void handle_proxy_config_cmd(Worker *w, ClientConn *client) {
    const char *a;
    int al;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &a, &al)) {
        client_reject_error(w, client, "wrong number of arguments for 'proxy|config' command");
        return;
    }
    Sub sub;
    if (al == 3 && !strncasecmp(a, "GET", 3))
        sub = SUB_GET;
    else if (al == 3 && !strncasecmp(a, "SET", 3))
        sub = SUB_SET;
    else if (al == 4 && !strncasecmp(a, "SAVE", 4))
        sub = SUB_SAVE;
    else if (al == 4 && !strncasecmp(a, "HELP", 4))
        sub = SUB_HELP;
    else {
        client_reject_error(w, client, "unknown PROXY CONFIG subcommand, try PROXY CONFIG HELP");
        return;
    }
    if (!may_run(w, client, sub)) return;
    switch (sub) {
        case SUB_GET: proxy_config_get(w, client); break;
        case SUB_SET: proxy_config_set(w, client); break;
        case SUB_SAVE: proxy_config_save(w, client); break;
        case SUB_HELP: proxy_config_help(w, client); break;
    }
}
