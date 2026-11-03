#include "proxy.h"
#include <limits.h>
#include <sys/time.h>
#include <sys/utsname.h>

/* ── INFO ───────────────────────────────────────────────────────────────
 * Clients talk to the proxy as if it were one standalone server, so INFO
 * describes that server instead of relaying whichever shard happens to be
 * first (see issue #6 for the design discussion).
 *
 * Each section has one source:
 *   - LOCAL: answered from the proxy's own state, no backend round trip
 *     (server, tls, clients, replication, cluster, proxy, pools).
 *   - SUM: fanned out to every master and summed (keyspace), so it agrees
 *     with DBSIZE.
 *   - SHARDS: fanned out to every master and listed per shard, unmerged
 *     (memory, persistence, stats, commandstats, errorstats, latencystats).
 *     Each shard starts with a "shard<idx>:addr=...,node_id=...,status=..."
 *     line and its own lines are prefixed "shard<idx>_", so the reply stays
 *     a flat key:value list that existing INFO parsers can read without
 *     one shard's values overwriting another's.
 *
 * Sections come out in Valkey's own order, separated by a blank line, and
 * unknown section names are ignored, as in Valkey.
 *
 * PROXY INFO [section ...] [SHARD <idx>|<ip:port>] relays each shard's raw
 * INFO reply, labelled, for anything the merged view leaves out. */

enum { SRC_LOCAL, SRC_SUM, SRC_SHARDS };

/* Lowest INFO level a section is part of: INFO / INFO default (1), INFO
 * all (2), INFO everything (3). */
enum { LVL_DEFAULT = 1, LVL_ALL = 2, LVL_EVERYTHING = 3 };

enum {
    SEC_SERVER,
    SEC_TLS,
    SEC_CLIENTS,
    SEC_MEMORY,
    SEC_PERSISTENCE,
    SEC_STATS,
    SEC_REPLICATION,
    SEC_COMMANDSTATS,
    SEC_ERRORSTATS,
    SEC_LATENCYSTATS,
    SEC_CLUSTER,
    SEC_KEYSPACE,
    SEC_PROXY,
    SEC_POOLS,
    SEC_COUNT
};

typedef struct {
    const char *name;   /* lowercase argument name */
    const char *header; /* "# <header>" in the reply */
    int src;
    int level;
} InfoSection;

/* Indexed by SEC_*; that order is also the output order. */
static const InfoSection SECTIONS[SEC_COUNT] = {
    {"server", "Server", SRC_LOCAL, LVL_DEFAULT},        {"tls", "TLS", SRC_LOCAL, LVL_ALL},
    {"clients", "Clients", SRC_LOCAL, LVL_DEFAULT},      {"memory", "Memory", SRC_SHARDS, LVL_ALL},
    {"persistence", "Persistence", SRC_SHARDS, LVL_ALL}, {"stats", "Stats", SRC_SHARDS, LVL_ALL},
    {"replication", "Replication", SRC_LOCAL, LVL_ALL},  {"commandstats", "Commandstats", SRC_SHARDS, LVL_ALL},
    {"errorstats", "Errorstats", SRC_SHARDS, LVL_ALL},   {"latencystats", "Latencystats", SRC_SHARDS, LVL_ALL},
    {"cluster", "Cluster", SRC_LOCAL, LVL_DEFAULT},      {"keyspace", "Keyspace", SRC_SUM, LVL_DEFAULT},
    {"proxy", "Proxy", SRC_LOCAL, LVL_DEFAULT},          {"pools", "Pools", SRC_LOCAL, LVL_EVERYTHING},
};

/* ── process identity, set once by info_init() before any worker runs ── */
static time_t g_start_time;
static char g_run_id[41];
static char g_executable[PATH_MAX];
static char g_config_file[PATH_MAX];

/* Lowest backend versions, and the modules loaded on every master, seen
 * by the last topology probe (see probe_backend_versions() in cluster.c).
 * Written by the main thread at startup and later by the refresh thread,
 * read by every worker. */
static pthread_mutex_t g_backend_ver_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_valkey_version[32];
static char g_redis_version[32];
static BackendModule g_modules[MAX_BACKEND_MODULES];
static int g_nmodules;

static void abs_path_or_copy(const char *path, char *out, size_t cap) {
    char tmp[PATH_MAX];
    if (path && path[0] && realpath(path, tmp))
        snprintf(out, cap, "%s", tmp);
    else
        snprintf(out, cap, "%s", path ? path : "");
}

void info_init(const char *argv0, const char *conf_path) {
    g_start_time = time(NULL);
    abs_path_or_copy(argv0, g_executable, sizeof(g_executable));
    abs_path_or_copy(conf_path, g_config_file, sizeof(g_config_file));

    unsigned char rnd[20];
    int fd = open("/dev/urandom", O_RDONLY);
    ssize_t got = fd >= 0 ? read(fd, rnd, sizeof(rnd)) : -1;
    if (fd >= 0) close(fd);
    if (got != (ssize_t)sizeof(rnd)) {
        srand((unsigned)(time(NULL) ^ getpid()));
        for (size_t i = 0; i < sizeof(rnd); i++) rnd[i] = (unsigned char)rand();
    }
    for (size_t i = 0; i < sizeof(rnd); i++) snprintf(g_run_id + i * 2, 3, "%02x", rnd[i]);
}

void info_set_backend_info(const char *valkey_version, const char *redis_version, const BackendModule *mods,
                           int nmods) {
    if (nmods > MAX_BACKEND_MODULES) nmods = MAX_BACKEND_MODULES;
    pthread_mutex_lock(&g_backend_ver_lock);
    snprintf(g_valkey_version, sizeof(g_valkey_version), "%s", valkey_version);
    snprintf(g_redis_version, sizeof(g_redis_version), "%s", redis_version);
    memcpy(g_modules, mods, sizeof(*mods) * (size_t)nmods);
    g_nmodules = nmods;
    pthread_mutex_unlock(&g_backend_ver_lock);
}

/* reported_version replaces the version the backend itself reports in
 * HELLO: valkey_version, or redis_version for a Redis backend. */
void info_get_reported_versions(char *valkey_version, size_t vcap, char *redis_version, size_t rcap) {
    pthread_mutex_lock(&g_backend_ver_lock);
    snprintf(valkey_version, vcap, "%s", g_valkey_version);
    snprintf(redis_version, rcap, "%s", g_redis_version);
    pthread_mutex_unlock(&g_backend_ver_lock);
    if (!g_config.reported_version[0]) return;
    if (valkey_version[0] || !redis_version[0])
        snprintf(valkey_version, vcap, "%s", g_config.reported_version);
    else
        snprintf(redis_version, rcap, "%s", g_config.reported_version);
}

const char *info_reported_role(void) {
    return g_config.reported_role[0] ? g_config.reported_role : "master";
}

/* reported_modules is "none", or a comma-separated list of name or
 * name:ver (ver defaults to 0). */
int info_get_reported_modules(BackendModule *out, int cap) {
    if (cap > MAX_BACKEND_MODULES) cap = MAX_BACKEND_MODULES;
    if (!g_config.reported_modules[0]) {
        pthread_mutex_lock(&g_backend_ver_lock);
        int n = g_nmodules < cap ? g_nmodules : cap;
        memcpy(out, g_modules, sizeof(*out) * (size_t)n);
        pthread_mutex_unlock(&g_backend_ver_lock);
        return n;
    }
    if (!strcasecmp(g_config.reported_modules, "none")) return 0;
    char tmp[sizeof(g_config.reported_modules)];
    snprintf(tmp, sizeof(tmp), "%s", g_config.reported_modules);
    int n = 0;
    char *save = NULL;
    for (char *tok = strtok_r(tmp, ",", &save); tok && n < cap; tok = strtok_r(NULL, ",", &save)) {
        char *colon = strchr(tok, ':');
        if (colon) *colon = '\0';
        if (!tok[0]) continue;
        snprintf(out[n].name, sizeof(out[n].name), "%s", tok);
        out[n].ver = colon ? strtoll(colon + 1, NULL, 10) : 0;
        n++;
    }
    return n;
}

/* ── section selection ──────────────────────────────────────────────── */
static uint32_t level_mask(int level) {
    uint32_t m = 0;
    for (int i = 0; i < SEC_COUNT; i++)
        if (SECTIONS[i].level <= level) m |= 1u << i;
    return m;
}

static int arg_is(const char *arg, int len, const char *word) {
    return len == (int)strlen(word) && !strncasecmp(arg, word, (size_t)len);
}

static uint32_t parse_sections(const char **args, const int *lens, int n) {
    if (n == 0) return level_mask(LVL_DEFAULT);
    uint32_t m = 0;
    for (int a = 0; a < n; a++) {
        if (arg_is(args[a], lens[a], "default"))
            m |= level_mask(LVL_DEFAULT);
        else if (arg_is(args[a], lens[a], "all"))
            m |= level_mask(LVL_ALL);
        else if (arg_is(args[a], lens[a], "everything"))
            m |= level_mask(LVL_EVERYTHING);
        else
            for (int i = 0; i < SEC_COUNT; i++)
                if (arg_is(args[a], lens[a], SECTIONS[i].name)) m |= 1u << i;
    }
    return m;
}

/* ── local sections ─────────────────────────────────────────────────── */
static int append_server(GrowBuf *g) {
    char valkey_version[32], redis_version[32];
    info_get_reported_versions(valkey_version, sizeof(valkey_version), redis_version, sizeof(redis_version));
    struct utsname un;
    if (uname(&un) != 0) memset(&un, 0, sizeof(un));
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long uptime = (long long)(time(NULL) - g_start_time);
#ifdef __GNUC__
    int gcc_major = __GNUC__, gcc_minor = __GNUC_MINOR__, gcc_patch = __GNUC_PATCHLEVEL__;
#else
    int gcc_major = 0, gcc_minor = 0, gcc_patch = 0;
#endif

    /* valkey_version and server_name are left out for a Redis backend,
     * which has neither: clients tell Valkey from Redis by whether they
     * exist. */
    if (gb_append_fmt(g,
                      "# Server\r\n"
                      "redis_version:%s\r\n"
                      "%s%s%s%s"
                      "server_mode:standalone\r\n"
                      "os:%s %s %s\r\n"
                      "multiplexing_api:%s\r\n"
                      "gcc_version:%d.%d.%d\r\n"
                      "process_id:%ld\r\n"
                      "run_id:%s\r\n"
                      "tcp_port:%d\r\n"
                      "server_time_usec:%lld\r\n"
                      "uptime_in_seconds:%lld\r\n"
                      "uptime_in_days:%lld\r\n"
                      "executable:%s\r\n"
                      "config_file:%s\r\n"
                      "listener0:name=%s,bind=%s,port=%d\r\n",
                      redis_version, valkey_version[0] ? "valkey_version:" : "", valkey_version,
                      valkey_version[0] ? "\r\n" : "", valkey_version[0] ? "server_name:valkey\r\n" : "", un.sysname,
                      un.release, un.machine, el_backend_name(), gcc_major, gcc_minor, gcc_patch, (long)getpid(),
                      g_run_id, g_config.proxy_port, (long long)tv.tv_sec * 1000000LL + tv.tv_usec, uptime,
                      uptime / 86400, g_executable, g_config_file, g_config.tls_enabled ? "tls" : "tcp",
                      g_config.bind_ip, g_config.proxy_port) < 0)
        return -1;
    if (g_config.unix_socket_path[0] &&
        gb_append_fmt(g, "listener1:name=unix,bind=%s\r\n", g_config.unix_socket_path) < 0)
        return -1;
    return gb_append_fmt(g, "proxy_version:%s\r\n", PROXY_VERSION);
}

#ifdef USE_TLS
/* Serial (hex) and seconds until expiry of the proxy's server certificate,
 * or "none"/0 when TLS isn't in use. */
static void server_cert_info(char *serial, size_t cap, long long *expires_in) {
    snprintf(serial, cap, "none");
    *expires_in = 0;
    if (!g_config.tls_enabled || !g_ssl_ctx) return;
    X509 *cert = SSL_CTX_get0_certificate(g_ssl_ctx);
    if (!cert) return;
    BIGNUM *bn = ASN1_INTEGER_to_BN(X509_get_serialNumber(cert), NULL);
    char *hex = bn ? BN_bn2hex(bn) : NULL;
    if (hex) snprintf(serial, cap, "%s", hex);
    OPENSSL_free(hex);
    BN_free(bn);
    int days = 0, secs = 0;
    if (ASN1_TIME_diff(&days, &secs, NULL, X509_get0_notAfter(cert))) *expires_in = (long long)days * 86400 + secs;
}
#endif

/* Same fields as Valkey's own TLS section. The proxy has its own server
 * certificate only: it doesn't verify client certificates and doesn't
 * speak TLS to the backends, so the client and CA fields stay none/0. */
static int append_tls(GrowBuf *g) {
    char serial[128] = "none";
    long long expires_in = 0;
#ifdef USE_TLS
    server_cert_info(serial, sizeof(serial), &expires_in);
#endif
    return gb_append_fmt(g,
                         "# TLS\r\n"
                         "tls_server_cert_serial:%s\r\n"
                         "tls_server_cert_expires_in_seconds:%lld\r\n"
                         "tls_client_cert_serial:none\r\n"
                         "tls_client_cert_expires_in_seconds:0\r\n"
                         "tls_ca_cert_serial:none\r\n"
                         "tls_ca_cert_expires_in_seconds:0\r\n",
                         serial, expires_in);
}

static int append_clients(GrowBuf *g) {
    long long connected = 0;
    for (int i = 0; i < g_config.worker_thread_no; i++)
        connected += atomic_load_explicit(&g_workers[i].client_count, memory_order_relaxed);
    return gb_append_fmt(g, "# Clients\r\nconnected_clients:%lld\r\nmaxclients:%d\r\n", connected,
                         g_config.max_connection);
}

static int append_proxy(Worker *w, GrowBuf *g, int queried, int ok) {
    int unmapped = 0;
    for (int s = 0; s < CLUSTER_SLOTS; s++)
        if (w->topo.slot_map[s] < 0) unmapped++;
    return gb_append_fmt(g,
                         "# Proxy\r\n"
                         "proxy_workers:%d\r\n"
                         "proxy_topology_version:%d\r\n"
                         "proxy_shards:%d\r\n"
                         "proxy_unmapped_slots:%d\r\n"
                         "proxy_cluster_refresh_interval_ms:%d\r\n"
                         "info_shards_queried:%d\r\n"
                         "info_shards_ok:%d\r\n"
                         "info_shards_failed:%d\r\n",
                         g_config.worker_thread_no, w->topo.version, w->topo.node_count, unmapped,
                         g_config.cluster_refresh_interval_ms, queried, ok, queried - ok);
}

/* ── shard replies ──────────────────────────────────────────────────── */
/* The INFO text inside a shard's bulk-string reply, or NULL if the shard
 * failed or answered with anything else (e.g. an error). Replies were
 * already normalized to RESP2 on arrival, so a RESP3 verbatim string shows
 * up here as a plain bulk string. */
static const char *shard_info_text(const FanoutSub *s, int *len) {
    if (!s || s->error || !s->rbuf || s->rbuf_len < 4 || s->rbuf[0] != '$') return NULL;
    const char *nl = memchr(s->rbuf, '\n', (size_t)s->rbuf_len);
    if (!nl) return NULL;
    int off = (int)(nl - s->rbuf) + 1;
    int bl = atoi(s->rbuf + 1);
    if (bl < 0 || off + bl > s->rbuf_len) return NULL;
    *len = bl;
    return s->rbuf + off;
}

/* Calls fn(line, linelen, arg) for each non-empty line of text's "# header"
 * section. Stops and returns -1 as soon as fn does. */
static int for_each_section_line(const char *text, int len, const char *header, int (*fn)(const char *, int, void *),
                                 void *arg) {
    int in_section = 0;
    size_t hlen = strlen(header);
    const char *p = text, *end = text + len;
    while (p < end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        const char *eol = nl ? nl : end;
        int ll = (int)(eol - p);
        if (ll > 0 && p[ll - 1] == '\r') ll--;
        if (ll > 0 && p[0] == '#') {
            const char *h = p + 1;
            int hl = ll - 1;
            while (hl > 0 && *h == ' ') h++, hl--;
            in_section = hl == (int)hlen && !strncasecmp(h, header, hlen);
        } else if (ll > 0 && in_section && fn(p, ll, arg) < 0) {
            return -1;
        }
        p = nl ? nl + 1 : end;
    }
    return 0;
}

typedef struct {
    GrowBuf *g;
    int idx;
} PrefixArg;

static int append_prefixed_line(const char *line, int len, void *arg) {
    PrefixArg *pa = arg;
    return gb_append_fmt(pa->g, "shard%d_%.*s\r\n", pa->idx, len, line);
}

static int append_shard_label(Worker *w, GrowBuf *g, const FanoutSub *s, int ok) {
    const ClusterNode *nd = &w->topo.nodes[s->node_idx];
    return gb_append_fmt(g, "shard%d:addr=%s:%d,node_id=%s,status=%s\r\n", s->node_idx, nd->ip, nd->port, nd->node_id,
                         ok ? "ok" : "error");
}

static int append_shards_section(Worker *w, GrowBuf *g, FanoutCtx *ctx, int sec) {
    if (gb_append_fmt(g, "# %s\r\n", SECTIONS[sec].header) < 0) return -1;
    for (int i = 0; i < ctx->nsubs; i++) {
        const FanoutSub *s = &ctx->subs[i];
        int tlen = 0;
        const char *text = shard_info_text(s, &tlen);
        if (append_shard_label(w, g, s, text != NULL) < 0) return -1;
        if (!text) continue;
        PrefixArg pa = {g, s->node_idx};
        if (for_each_section_line(text, tlen, SECTIONS[sec].header, append_prefixed_line, &pa) < 0) return -1;
    }
    return 0;
}

/* Keyspace lines ("db0:keys=1,expires=0,avg_ttl=0,...") summed per db
 * across shards. avg_ttl is averaged weighted by each shard's expires,
 * skipping shards that report 0 (Valkey's "not sampled yet"); every other
 * field is a plain sum. Fields keep the order they first appear in. */
#define KS_MAX_DBS    64
#define KS_MAX_FIELDS 8

typedef struct {
    int db;
    int nfields;
    char names[KS_MAX_FIELDS][48];
    long long vals[KS_MAX_FIELDS];
    double ttl_weighted;   /* sum of avg_ttl * expires, over shards with avg_ttl > 0 */
    long long ttl_expires; /* sum of expires, over the same shards */
} KsDb;

typedef struct {
    KsDb dbs[KS_MAX_DBS];
    int ndbs;
} KsTotals;

static long long *ks_field(KsDb *d, const char *name, int nlen) {
    for (int i = 0; i < d->nfields; i++)
        if ((int)strlen(d->names[i]) == nlen && !strncmp(d->names[i], name, (size_t)nlen)) return &d->vals[i];
    if (d->nfields == KS_MAX_FIELDS || nlen >= (int)sizeof(d->names[0])) return NULL;
    memcpy(d->names[d->nfields], name, (size_t)nlen);
    d->names[d->nfields][nlen] = '\0';
    d->vals[d->nfields] = 0;
    return &d->vals[d->nfields++];
}

static int ks_add_line(const char *line, int len, void *arg) {
    KsTotals *t = arg;
    if (len < 4 || strncmp(line, "db", 2)) return 0;
    const char *colon = memchr(line, ':', (size_t)len);
    if (!colon) return 0;
    int db = atoi(line + 2);
    KsDb *d = NULL;
    for (int i = 0; i < t->ndbs; i++)
        if (t->dbs[i].db == db) d = &t->dbs[i];
    if (!d) {
        if (t->ndbs == KS_MAX_DBS) return 0;
        d = &t->dbs[t->ndbs++];
        memset(d, 0, sizeof(*d));
        d->db = db;
    }
    long long expires = 0, avg_ttl = 0;
    const char *p = colon + 1, *end = line + len;
    while (p < end) {
        const char *comma = memchr(p, ',', (size_t)(end - p));
        const char *fe = comma ? comma : end;
        const char *eq = memchr(p, '=', (size_t)(fe - p));
        if (eq) {
            int nlen = (int)(eq - p);
            long long v = strtoll(eq + 1, NULL, 10);
            long long *slot = ks_field(d, p, nlen);
            if (nlen == 7 && !strncmp(p, "avg_ttl", 7))
                avg_ttl = v;
            else if (slot)
                *slot += v;
            if (nlen == 7 && !strncmp(p, "expires", 7)) expires = v;
        }
        p = comma ? comma + 1 : end;
    }
    if (avg_ttl > 0) {
        d->ttl_weighted += (double)avg_ttl * (double)expires;
        d->ttl_expires += expires;
    }
    return 0;
}

static int ks_cmp(const void *a, const void *b) {
    return ((const KsDb *)a)->db - ((const KsDb *)b)->db;
}

static int append_keyspace(GrowBuf *g, FanoutCtx *ctx) {
    KsTotals *t = calloc(1, sizeof(KsTotals));
    if (!t) return -1;
    for (int i = 0; i < ctx->nsubs; i++) {
        int tlen = 0;
        const char *text = shard_info_text(&ctx->subs[i], &tlen);
        if (text) for_each_section_line(text, tlen, "Keyspace", ks_add_line, t);
    }
    qsort(t->dbs, (size_t)t->ndbs, sizeof(KsDb), ks_cmp);
    int rc = gb_append(g, "# Keyspace\r\n", 12);
    for (int i = 0; i < t->ndbs && rc == 0; i++) {
        KsDb *d = &t->dbs[i];
        rc = gb_append_fmt(g, "db%d:", d->db);
        for (int f = 0; f < d->nfields && rc == 0; f++) {
            long long v = d->vals[f];
            if (!strcmp(d->names[f], "avg_ttl"))
                v = d->ttl_expires ? (long long)(d->ttl_weighted / (double)d->ttl_expires) : 0;
            rc = gb_append_fmt(g, "%s%s=%lld", f ? "," : "", d->names[f], v);
        }
        if (rc == 0) rc = gb_append(g, "\r\n", 2);
    }
    free(t);
    return rc;
}

/* ── reply assembly ─────────────────────────────────────────────────── */
/* Wraps INFO text as the reply Valkey itself sends: a bulk string, or a
 * verbatim "txt" string for a RESP3 client. */
static char *wrap_info_reply(int resp3, const char *text, int len, int *out_len) {
    GrowBuf g = {0};
    int rc = resp3 ? gb_append_fmt(&g, "=%d\r\ntxt:", len + 4) : gb_append_fmt(&g, "$%d\r\n", len);
    if (rc == 0) rc = gb_append(&g, text, len);
    if (rc == 0) rc = gb_append(&g, "\r\n", 2);
    if (rc < 0) {
        free(g.buf);
        return NULL;
    }
    *out_len = g.len;
    return g.buf;
}

/* Builds the INFO text for mask. ctx carries the shard replies, or is NULL
 * when no requested section needs a backend. */
static char *build_info(Worker *w, ClientConn *client, uint32_t mask, FanoutCtx *ctx, int *out_len) {
    int queried = ctx ? ctx->nsubs : 0, ok = 0;
    for (int i = 0; i < queried; i++) {
        int tlen;
        if (shard_info_text(&ctx->subs[i], &tlen)) ok++;
    }

    GrowBuf g = {0};
    int rc = 0;
    for (int sec = 0; sec < SEC_COUNT && rc == 0; sec++) {
        if (!(mask & (1u << sec))) continue;
        if (g.len > 0) rc = gb_append(&g, "\r\n", 2);
        if (rc < 0) break;
        switch (sec) {
            case SEC_SERVER: rc = append_server(&g); break;
            case SEC_TLS: rc = append_tls(&g); break;
            case SEC_CLIENTS: rc = append_clients(&g); break;
            /* The same role HELLO reports. The proxy has no replicas of its
             * own; the backends' replication is in PROXY INFO replication. */
            case SEC_REPLICATION:
                rc = gb_append_fmt(&g, "# Replication\r\nrole:%s\r\nconnected_slaves:0\r\n", info_reported_role());
                break;
            case SEC_CLUSTER: rc = gb_append_fmt(&g, "# Cluster\r\ncluster_enabled:0\r\n"); break;
            case SEC_KEYSPACE: rc = ctx ? append_keyspace(&g, ctx) : -1; break;
            case SEC_PROXY: rc = append_proxy(w, &g, queried, ok); break;
            case SEC_POOLS: rc = proxy_pools_append(w, &g); break;
            default: rc = ctx ? append_shards_section(w, &g, ctx, sec) : -1; break;
        }
    }
    char *reply = rc == 0 ? wrap_info_reply(client->resp == 3, g.len ? g.buf : "", g.len, out_len) : NULL;
    free(g.buf);
    return reply;
}

/* Most arguments INFO, or PROXY INFO after its subcommand, takes. The
 * arrays hold one more, so a longer command is rejected instead of being
 * cut off: resp_extract_all_args() stops at max without saying so. */
#define INFO_MAX_ARGS 64

void handle_info_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *args[INFO_MAX_ARGS + 1];
    int lens[INFO_MAX_ARGS + 1];
    int n = resp_extract_all_args(client->hbuf, cmdlen, args, lens, INFO_MAX_ARGS + 1);
    if (n > INFO_MAX_ARGS) {
        client_send_error(w, client, "wrong number of arguments for 'info' command");
        client->hbuf_len = 0;
        return;
    }
    if (n < 0) n = 0;
    uint32_t mask = parse_sections(args, lens, n);

    /* Ask the shards only for the sections that need them. */
    const char *bargs[SEC_COUNT];
    int blens[SEC_COUNT], nb = 0;
    for (int sec = 0; sec < SEC_COUNT; sec++) {
        if (!(mask & (1u << sec)) || SECTIONS[sec].src == SRC_LOCAL) continue;
        bargs[nb] = SECTIONS[sec].name;
        blens[nb++] = (int)strlen(SECTIONS[sec].name);
    }

    if (nb == 0) {
        int len;
        char *reply = build_info(w, client, mask, NULL, &len);
        if (!reply) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }
        fanout_send_response(w, client, reply, len);
        return;
    }

    int built_len;
    char *built = build_resp_cmd("INFO", 4, bargs, blens, nb, &built_len);
    FanoutCtx *ctx = built ? calloc(1, sizeof(FanoutCtx)) : NULL;
    if (!ctx) {
        free(built);
        client_send_error(w, client, "out of memory");
        client->hbuf_len = 0;
        return;
    }
    ctx->client = client;
    snprintf(ctx->cmd, sizeof(ctx->cmd), "INFO");
    ctx->info_sections = mask;
    client->fanout_ctx = ctx;

    int targets[MAX_NODES];
    int ntargets = topo_all_masters(w, targets);
    fanout_to_nodes(w, client, ctx, built, built_len, targets, ntargets);
    free(built);
}

char *info_aggregate(Worker *w, FanoutCtx *ctx, int *out_len) {
    return build_info(w, ctx->client, ctx->info_sections, ctx, out_len);
}

/* ── PROXY INFO ─────────────────────────────────────────────────────── */
/* Resolves a SHARD argument (a node index or "ip:port") against this
 * worker's topology; -1 if it matches no shard. */
static int resolve_shard(Worker *w, const char *arg, int len) {
    char buf[96];
    if (len <= 0 || len >= (int)sizeof(buf)) return -1;
    memcpy(buf, arg, (size_t)len);
    buf[len] = '\0';
    char *end;
    long idx = strtol(buf, &end, 10);
    if (*end == '\0') return idx >= 0 && idx < w->topo.node_count ? (int)idx : -1;
    for (int i = 0; i < w->topo.node_count; i++) {
        char addr[96];
        snprintf(addr, sizeof(addr), "%s:%d", w->topo.nodes[i].ip, w->topo.nodes[i].port);
        if (!strcmp(addr, buf)) return i;
    }
    return -1;
}

void handle_proxy_info_cmd(Worker *w, ClientConn *client, int cmdlen) {
    /* args[0] is "INFO" (PROXY's subcommand); sections follow. */
    const char *args[INFO_MAX_ARGS + 2];
    int lens[INFO_MAX_ARGS + 2];
    int n = resp_extract_all_args(client->hbuf, cmdlen, args, lens, INFO_MAX_ARGS + 2);
    if (n < 1 || n - 1 > INFO_MAX_ARGS) {
        client_send_error(w, client, "wrong number of arguments for 'proxy|info' command");
        client->hbuf_len = 0;
        return;
    }
    const char **sections = args + 1;
    const int *section_lens = lens + 1;
    int nsections = n - 1;

    /* SHARD <idx>|<ip:port> may come anywhere among the sections; take it
     * out so it isn't sent on as a section name. */
    int targets[MAX_NODES];
    int ntargets = 0;
    for (int a = 0; a < nsections; a++) {
        if (!arg_is(sections[a], section_lens[a], "shard")) continue;
        if (ntargets || a + 1 >= nsections) {
            client_send_error(w, client, "syntax error");
            client->hbuf_len = 0;
            return;
        }
        int idx = resolve_shard(w, sections[a + 1], section_lens[a + 1]);
        if (idx < 0) {
            client_send_error(w, client, "unknown shard");
            client->hbuf_len = 0;
            return;
        }
        targets[0] = idx;
        ntargets = 1;
        memmove(&args[a + 1], &args[a + 3], sizeof(*args) * (size_t)(nsections - a - 2));
        memmove(&lens[a + 1], &lens[a + 3], sizeof(*lens) * (size_t)(nsections - a - 2));
        nsections -= 2;
        a--;
    }
    if (!ntargets) ntargets = topo_all_masters(w, targets);

    int built_len;
    char *built = build_resp_cmd("INFO", 4, sections, section_lens, nsections, &built_len);
    FanoutCtx *ctx = built ? calloc(1, sizeof(FanoutCtx)) : NULL;
    if (!ctx) {
        free(built);
        client_send_error(w, client, "out of memory");
        client->hbuf_len = 0;
        return;
    }
    ctx->client = client;
    snprintf(ctx->cmd, sizeof(ctx->cmd), "PROXY_INFO");
    client->fanout_ctx = ctx;
    fanout_to_nodes(w, client, ctx, built, built_len, targets, ntargets);
    free(built);
}

/* An array of label/reply pairs (a map for a RESP3 client): the label is
 * "<idx>:<ip>:<port>", the reply is that shard's INFO text as-is, or an
 * error if the shard couldn't be reached. */
char *proxy_info_aggregate(Worker *w, FanoutCtx *ctx, int *out_len) {
    int resp3 = ctx->client->resp == 3;
    GrowBuf g = {0};
    int rc = resp3 ? gb_append_fmt(&g, "%%%d\r\n", ctx->nsubs) : gb_append_fmt(&g, "*%d\r\n", ctx->nsubs * 2);
    for (int i = 0; i < ctx->nsubs && rc == 0; i++) {
        const FanoutSub *s = &ctx->subs[i];
        const ClusterNode *nd = &w->topo.nodes[s->node_idx];
        char label[96];
        int ll = snprintf(label, sizeof(label), "%d:%s:%d", s->node_idx, nd->ip, nd->port);
        rc = gb_append_fmt(&g, "$%d\r\n%s\r\n", ll, label);
        if (rc < 0) break;
        int tlen;
        const char *text = shard_info_text(s, &tlen);
        if (text)
            rc = gb_append_fmt(&g, "$%d\r\n%.*s\r\n", tlen, tlen, text);
        else if (!s->error && s->rbuf && s->rbuf_len > 0 && s->rbuf[0] == '-')
            rc = gb_append(&g, s->rbuf, s->rbuf_len); /* the shard's own error */
        else
            rc = gb_append_fmt(&g, "-ERR shard unavailable\r\n");
    }
    if (rc < 0) {
        free(g.buf);
        return NULL;
    }
    *out_len = g.len;
    return g.buf;
}
