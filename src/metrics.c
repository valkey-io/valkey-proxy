#include "proxy.h"

/* ── PROXY command ──────────────────────────────────────────────────────
 * Proxy introspection. PROXY POOLS is answered locally; PROXY INFO relays
 * each shard's own INFO (see info.c).
 *
 * PROXY POOLS reports connection pool usage per backend node and pool type,
 * summed across worker threads. The reply is a bulk string in the same
 * "key:value" line format as INFO, so it can later be folded into an INFO
 * section as-is.
 *
 * Each worker owns its own pools and updates their PoolStats itself (see
 * pool.c); here we only read them. A worker's db_groups array can be
 * realloc'd by db_group_create(), which does that under the worker's
 * db_lock, so we take that lock just long enough to copy the group list
 * (see snapshot_groups()), one worker at a time, and read the stats after
 * releasing it. db_lock is never held across backend connects either, so
 * neither side can stall the other. */

typedef struct {
    int live;
    long long max, open, in_use, waiting;
    unsigned long long acquires, waits, handoffs, wait_us_sum, wait_us_max;
    unsigned long long wait_abandoned, wait_failed;
    unsigned long long connects, connect_failures, reconnects, destroys;
} PoolTotals;

typedef struct {
    int db;
    PoolTotals t[MAX_NODES][POOL_TYPE_COUNT];
} DbTotals;

static void pool_totals_add(PoolTotals *t, PoolStats *s, int pool_max) {
    if (!atomic_load_explicit(&s->live, memory_order_acquire)) return;
    t->live = 1;
    t->max += pool_max;
    t->open += atomic_load_explicit(&s->open, memory_order_relaxed);
    t->in_use += atomic_load_explicit(&s->in_use, memory_order_relaxed);
    t->waiting += atomic_load_explicit(&s->waiting, memory_order_relaxed);
    t->acquires += atomic_load_explicit(&s->acquires, memory_order_relaxed);
    t->waits += atomic_load_explicit(&s->waits, memory_order_relaxed);
    t->handoffs += atomic_load_explicit(&s->handoffs, memory_order_relaxed);
    t->wait_us_sum += atomic_load_explicit(&s->wait_us_sum, memory_order_relaxed);
    unsigned long long wmax = atomic_load_explicit(&s->wait_us_max, memory_order_relaxed);
    if (wmax > t->wait_us_max) t->wait_us_max = wmax;
    t->wait_abandoned += atomic_load_explicit(&s->wait_abandoned, memory_order_relaxed);
    t->wait_failed += atomic_load_explicit(&s->wait_failed, memory_order_relaxed);
    t->connects += atomic_load_explicit(&s->connects, memory_order_relaxed);
    t->connect_failures += atomic_load_explicit(&s->connect_failures, memory_order_relaxed);
    t->reconnects += atomic_load_explicit(&s->reconnects, memory_order_relaxed);
    t->destroys += atomic_load_explicit(&s->destroys, memory_order_relaxed);
}

/* Returns the index of db's entry in *dbs, adding a zeroed one if needed,
 * or -1 on allocation failure. */
static int db_totals_index(DbTotals **dbs, int *ndb, int db) {
    for (int i = 0; i < *ndb; i++)
        if ((*dbs)[i].db == db) return i;
    DbTotals *tmp = realloc(*dbs, (size_t)(*ndb + 1) * sizeof(DbTotals));
    if (!tmp) return -1;
    *dbs = tmp;
    memset(&tmp[*ndb], 0, sizeof(DbTotals));
    tmp[*ndb].db = db;
    return (*ndb)++;
}

typedef struct {
    int db;
    NodePools *node_pools;
} GroupRef;

/* Copies ow's (db, node_pools) pairs into *refs, growing it as needed, and
 * returns how many there are, or -1 on allocation failure. db_lock is held
 * only for the copy itself -- the owner takes it on every command -- and
 * the node_pools pointers stay valid afterwards: a group's NodePools array
 * is allocated once and never freed or moved, only db_groups is realloc'd. */
static int snapshot_groups(Worker *ow, GroupRef **refs, int *cap) {
    for (;;) {
        pthread_mutex_lock(&ow->db_lock);
        int n = ow->db_group_count;
        if (n <= *cap) {
            for (int gi = 0; gi < n; gi++) {
                (*refs)[gi].db = ow->db_groups[gi].db;
                (*refs)[gi].node_pools = ow->db_groups[gi].node_pools;
            }
            pthread_mutex_unlock(&ow->db_lock);
            return n;
        }
        pthread_mutex_unlock(&ow->db_lock);
        GroupRef *tmp = realloc(*refs, (size_t)n * sizeof(GroupRef));
        if (!tmp) return -1;
        *refs = tmp;
        *cap = n;
    }
}

/* Sums every worker's pool stats, per db / node index / pool type, into a
 * malloc'd array (*dbs_out, caller frees; NULL when there's nothing yet) of
 * *ndb_out entries. Returns 0 on success, -1 on allocation failure. */
static int collect_pool_totals(DbTotals **dbs_out, int *ndb_out) {
    DbTotals *dbs = NULL;
    GroupRef *refs = NULL;
    int ndb = 0, refs_cap = 0, rc = 0;
    for (int wi = 0; wi < g_config.worker_thread_no && rc == 0; wi++) {
        Worker *ow = &g_workers[wi];
        if (!atomic_load_explicit(&ow->stats_ready, memory_order_acquire)) continue;
        int ngroups = snapshot_groups(ow, &refs, &refs_cap);
        if (ngroups < 0) rc = -1;
        for (int gi = 0; gi < ngroups && rc == 0; gi++) {
            int di = db_totals_index(&dbs, &ndb, refs[gi].db);
            if (di < 0) {
                rc = -1;
                break;
            }
            for (int ni = 0; ni < MAX_NODES; ni++)
                for (int pt = 0; pt < POOL_TYPE_COUNT; pt++)
                    pool_totals_add(&dbs[di].t[ni][pt], &refs[gi].node_pools[ni].pools[pt].stats, g_pool_max[pt]);
        }
    }
    free(refs);
    if (rc < 0) {
        free(dbs);
        return -1;
    }
    *dbs_out = dbs;
    *ndb_out = ndb;
    return 0;
}

/* Node address and generation come from the calling worker's own topology
 * snapshot. The worker that owns a pool may be one refresh behind (or
 * ahead), so right after a failover the labels can briefly disagree with
 * the counters; gen lets a consumer notice that an index was reassigned.
 * Returns 0 on success, -1 on allocation failure. */
static int format_pool_totals(Worker *w, const DbTotals *dbs, int ndb, GrowBuf *g) {
    if (gb_append_fmt(g, "# Pools\r\nworkers:%d\r\n", g_config.worker_thread_no) < 0) return -1;
    for (int di = 0; di < ndb; di++) {
        for (int ni = 0; ni < MAX_NODES; ni++) {
            for (int pt = 0; pt < POOL_TYPE_COUNT; pt++) {
                const PoolTotals *t = &dbs[di].t[ni][pt];
                if (!t->live) continue;
                const char *ip = "?";
                int port = 0, gen = -1, has_slots = 0;
                if (ni < w->topo.node_count) {
                    ip = w->topo.nodes[ni].ip;
                    port = w->topo.nodes[ni].port;
                    gen = w->topo.node_gen[ni];
                    has_slots = w->topo.node_has_slots[ni];
                }
                if (gb_append_fmt(g,
                                  "pool_db%d_n%d_%s:addr=%s:%d,gen=%d,has_slots=%d,max=%lld,open=%lld,in_use=%lld,"
                                  "waiting=%lld,acquires=%llu,waits=%llu,handoffs=%llu,wait_us_sum=%llu,"
                                  "wait_us_max=%llu,wait_abandoned=%llu,wait_failed=%llu,connects=%llu,"
                                  "connect_failures=%llu,reconnects=%llu,destroys=%llu\r\n",
                                  dbs[di].db, ni, POOL_NAME[pt], ip, port, gen, has_slots, t->max, t->open, t->in_use,
                                  t->waiting, t->acquires, t->waits, t->handoffs, t->wait_us_sum, t->wait_us_max,
                                  t->wait_abandoned, t->wait_failed, t->connects, t->connect_failures, t->reconnects,
                                  t->destroys) < 0)
                    return -1;
            }
        }
    }
    return 0;
}

int proxy_pools_append(Worker *w, GrowBuf *g) {
    DbTotals *dbs;
    int ndb;
    if (collect_pool_totals(&dbs, &ndb) < 0) return -1;
    int rc = format_pool_totals(w, dbs, ndb, g);
    free(dbs);
    return rc;
}

static void handle_proxy_pools(Worker *w, ClientConn *client) {
    GrowBuf g = {0};
    if (proxy_pools_append(w, &g) < 0) {
        free(g.buf);
        client_send_error(w, client, "oom");
        client->hbuf_len = 0;
        return;
    }
    send_bulk_reply(w, client, g.buf, g.len);
    free(g.buf);
}

void handle_proxy_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, cmdlen, 1, &sub, &sublen)) {
        client_send_error(w, client, "wrong number of arguments for 'proxy' command");
        client->hbuf_len = 0;
        return;
    }
    const char *extra;
    int extralen;
    int has_extra = resp_get_nth_arg(client->hbuf, cmdlen, 2, &extra, &extralen);

    if (sublen == 5 && !strncasecmp(sub, "POOLS", 5)) {
        if (has_extra) {
            client_send_error(w, client, "wrong number of arguments for 'proxy|pools' command");
            client->hbuf_len = 0;
            return;
        }
        handle_proxy_pools(w, client);
        return;
    }
    if (sublen == 4 && !strncasecmp(sub, "INFO", 4)) {
        handle_proxy_info_cmd(w, client, cmdlen);
        return;
    }
    if (sublen == 4 && !strncasecmp(sub, "HELP", 4)) {
        static const char *lines[] = {
            "PROXY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "POOLS",
            "    Return connection pool usage per backend node and pool type, summed",
            "    across worker threads.",
            "INFO [<section> ...] [SHARD <index>|<ip:port>]",
            "    Return each shard's own INFO reply, labelled <index>:<ip>:<port>,",
            "    without merging. SHARD limits the reply to one shard.",
            "HELP",
            "    Print this help.",
        };
        send_multibulk_reply(w, client, lines, (int)(sizeof(lines) / sizeof(lines[0])));
        return;
    }
    client_send_error(w, client, "unknown PROXY subcommand");
    client->hbuf_len = 0;
}
