#include "proxy.h"
#include <time.h>
#include <sys/time.h>

/* ── recv_bulk (cluster discovery and version probe) ────────────────── */
/* NULL with *out_len 0 on allocation failure: this also runs on the
 * refresh thread, where one failed allocation shouldn't end the process. */
static char *recv_bulk(int fd, size_t *out_len) {
    size_t cap = 65536, len = 0;
    *out_len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        perror("malloc");
        return NULL;
    }
    for (;;) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *tmp = realloc(buf, cap);
            if (!tmp) {
                perror("realloc");
                free(buf);
                return NULL;
            }
            buf = tmp;
        }
        ssize_t n = read(fd, buf + len, cap - len - 1);
        if (n <= 0) break;
        len += n;
        buf[len] = '\0';
        if (buf[0] == '$') {
            char *eol = memchr(buf, '\n', len);
            if (!eol) continue;
            int bl = atoi(buf + 1);
            size_t h = (size_t)(eol - buf) + 1;
            if (len >= h + (size_t)bl + 2) break;
        } else if (buf[0] == '-' || buf[0] == '+') {
            if (memchr(buf, '\n', len)) break;
        }
    }
    *out_len = len;
    return buf;
}

/* ── backend version probe ─────────────────────────────────────────── */
/* Compares dotted versions numerically, so "8.10.0" > "8.9.1". */
static int version_cmp(const char *a, const char *b) {
    for (int i = 0; i < 3; i++) {
        char *ea, *eb;
        long x = strtol(a, &ea, 10), y = strtol(b, &eb, 10);
        if (x != y) return x < y ? -1 : 1;
        a = *ea == '.' ? ea + 1 : ea;
        b = *eb == '.' ? eb + 1 : eb;
    }
    return 0;
}

/* Copies the value of an INFO "<field>:<value>" line into out, or "" if
 * info has no such line. */
static void info_field(const char *info, const char *field, char *out, size_t cap) {
    char key[64];
    snprintf(key, sizeof(key), "\n%s:", field);
    out[0] = '\0';
    const char *p = strstr(info, key);
    if (!p) return;
    p += strlen(key);
    size_t n = strcspn(p, "\r\n");
    snprintf(out, cap, "%.*s", (int)(n < cap ? n : cap - 1), p);
}

/* Parses the "module:name=<name>,ver=<ver>,..." lines of an INFO modules
 * section into out. Returns how many were found. */
static int info_modules(const char *info, BackendModule *out, int cap) {
    int n = 0;
    for (const char *p = strstr(info, "\nmodule:name="); p && n < cap; p = strstr(p + 1, "\nmodule:name=")) {
        p += strlen("\nmodule:name=");
        size_t len = strcspn(p, ",\r\n");
        snprintf(out[n].name, sizeof(out[n].name), "%.*s",
                 (int)(len < sizeof(out[n].name) ? len : sizeof(out[n].name) - 1), p);
        const char *ver = strstr(p, ",ver=");
        const char *eol = p + strcspn(p, "\r\n");
        out[n].ver = ver && ver < eol ? strtoll(ver + 5, NULL, 10) : 0;
        n++;
    }
    return n;
}

/* 1 if resp holds a whole bulk string. recv_bulk() returns whatever arrived
 * before SO_RCVTIMEO, and a cut-off INFO would drop modules and fields. */
static int bulk_complete(const char *resp, size_t len) {
    if (len == 0 || resp[0] != '$') return 0;
    const char *eol = memchr(resp, '\n', len);
    if (!eol) return 0;
    long bl = strtol(resp + 1, NULL, 10);
    return bl >= 0 && len >= (size_t)(eol - resp) + 1 + (size_t)bl + 2;
}

/* Keeps only the modules in mods that are also in other, with the lower
 * of the two versions. Returns the new count. */
static int intersect_modules(BackendModule *mods, int n, const BackendModule *other, int nother) {
    int kept = 0;
    for (int i = 0; i < n; i++)
        for (int j = 0; j < nother; j++)
            if (!strcmp(mods[i].name, other[j].name)) {
                mods[kept] = mods[i];
                if (other[j].ver < mods[kept].ver) mods[kept].ver = other[j].ver;
                kept++;
                break;
            }
    return kept;
}

/* Records the lowest valkey_version and redis_version across nodes, and
 * the modules loaded on every node, via a blocking INFO server modules to
 * each, for INFO and HELLO to report. The oldest shard bounds what clients
 * can rely on, e.g. during a rolling upgrade, and a module only some
 * shards have would fail for keys on the others. Only nodes that own slots
 * are asked: a dead master stays in nodes[] until its index is reused, and
 * each one would cost a connect timeout on the refresh thread. The previous
 * values are kept if no node answers. Runs on every topology publish, and
 * every BACKEND_PROBE_INTERVAL_SEC otherwise, to catch in-place upgrades
 * and module loads that don't change the topology. */
#define BACKEND_PROBE_INTERVAL_SEC 60
static time_t last_probe;

static void probe_backend_versions(const ClusterNode *nodes, int count, const int *has_slots) {
    last_probe = time(NULL);
    static const char cmd[] = "*3\r\n$4\r\nINFO\r\n$6\r\nserver\r\n$7\r\nmodules\r\n";
    int ms = g_config.backend_connect_timeout_ms;
    char valkey_version[32] = "", redis_version[32] = "";
    BackendModule mods[MAX_BACKEND_MODULES], node_mods[MAX_BACKEND_MODULES];
    int nmods = 0, answered = 0;
    for (int i = 0; i < count; i++) {
        if (!has_slots[i]) continue;
        int fd = tcp_connect_timeout(nodes[i].ip, nodes[i].port, ms);
        if (fd < 0) continue;
        struct timeval tv = {ms / 1000, (ms % 1000) * 1000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        if (write(fd, cmd, sizeof(cmd) - 1) != (ssize_t)(sizeof(cmd) - 1)) {
            close(fd);
            continue;
        }
        size_t len;
        char *resp = recv_bulk(fd, &len);
        close(fd);
        if (bulk_complete(resp, len)) {
            char v[32], r[32];
            info_field(resp, "valkey_version", v, sizeof(v));
            info_field(resp, "redis_version", r, sizeof(r));
            if (v[0] && (!valkey_version[0] || version_cmp(v, valkey_version) < 0))
                snprintf(valkey_version, sizeof(valkey_version), "%s", v);
            if (r[0] && (!redis_version[0] || version_cmp(r, redis_version) < 0))
                snprintf(redis_version, sizeof(redis_version), "%s", r);
            int n = info_modules(resp, node_mods, MAX_BACKEND_MODULES);
            if (answered++ == 0) {
                memcpy(mods, node_mods, sizeof(*mods) * (size_t)n);
                nmods = n;
            } else {
                nmods = intersect_modules(mods, nmods, node_mods, n);
            }
        }
        free(resp);
    }
    if (valkey_version[0] || redis_version[0]) info_set_backend_info(valkey_version, redis_version, mods, nmods);
}

/* Exact match against one comma-separated token in a CLUSTER NODES flags
 * field (e.g. "master,fail") -- a substring search would also match "fail"
 * inside "fail?" (PFAIL), which must stay distinct from confirmed FAIL. */
static int has_flag(const char *flags, const char *want) {
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "%s", flags);
    for (char *tok = strtok(tmp, ","); tok; tok = strtok(NULL, ","))
        if (strcmp(tok, want) == 0) return 1;
    return 0;
}

/* ── CLUSTER NODES ──────────────────────────────────────────────────── */
/* Parses into caller-owned buffers instead of the global topology, so both
 * the one-shot startup discovery and the periodic background refresh can
 * share this without either one touching live global state mid-parse.
 * out_slot must already be initialized (typically all -1) by the caller. */
static void parse_cluster_nodes_into(const char *data, ClusterNode *out_nodes, int *out_count, int *out_slot,
                                     int verbose) {
    char buf[65536];
    snprintf(buf, sizeof(buf), "%s", data);
    char *p = buf;
    while (*p && *out_count < MAX_NODES) {
        char *eol = strchr(p, '\n');
        char saved = 0;
        if (eol) {
            saved = *eol;
            *eol = '\0';
        }
        char *cr = strchr(p, '\r');
        if (cr) *cr = '\0';
        if (*p == '\0') {
            p = eol ? eol + 1 : p + strlen(p);
            continue;
        }

        char id[64], addr[128], flags[128], master[64], ping[32], pong[32], epoch[32], st[32];
        int nc = 0;
        if (sscanf(p, "%63s %127s %127s %63s %31s %31s %31s %31s%n", id, addr, flags, master, ping, pong, epoch, st,
                   &nc) < 8) {
            if (eol) *eol = saved;
            p = eol ? eol + 1 : p + strlen(p);
            continue;
        }
        /* Only masters take live traffic; replicas are never routed to
         * (the slot map only ever points at a master index), so skip them
         * here rather than pooling/connecting to them for nothing. A
         * master with confirmed FAIL has no reachable data either, so
         * skip it too -- its slots are left unmapped (-1) and any command
         * that hashes there gets the existing "slot not mapped" error
         * instead of a connection to a dead node. PFAIL ("fail?", not yet
         * confirmed by cluster consensus) is kept -- the node may still be
         * reachable. */
        int is_master = (strstr(flags, "master") != NULL);
        int is_failed = has_flag(flags, "fail");
        if (!is_master || is_failed) {
            if (verbose) printf("  skipping node %s %s (flags=%s)\n", id, addr, flags);
            if (eol) *eol = saved;
            p = eol ? eol + 1 : p + strlen(p);
            continue;
        }

        char *at = strchr(addr, '@');
        if (at) *at = '\0';
        char *colon = strrchr(addr, ':');
        if (!colon) {
            if (eol) *eol = saved;
            p = eol ? eol + 1 : p + strlen(p);
            continue;
        }
        *colon = '\0';
        int nport = atoi(colon + 1);
        if (!addr[0] || nport == 0) {
            if (eol) *eol = saved;
            p = eol ? eol + 1 : p + strlen(p);
            continue;
        }

        int ni = *out_count;
        ClusterNode *nd = &out_nodes[(*out_count)++];
        snprintf(nd->node_id, sizeof(nd->node_id), "%.63s", id);
        snprintf(nd->ip, sizeof(nd->ip), "%.63s", addr);
        nd->port = nport;
        snprintf(nd->flags, sizeof(nd->flags), "%.127s", flags);
        nd->is_master = 1;

        /* parse slot ranges */
        const char *tp = p + nc;
        while (*tp) {
            while (*tp == ' ' || *tp == '\t') tp++;
            if (!*tp) break;
            char tok[32];
            int ti = 0;
            while (*tp && *tp != ' ' && *tp != '\t' && ti < 31) tok[ti++] = *tp++;
            tok[ti] = '\0';
            if (tok[0] == '[') continue;
            char *dash = strchr(tok, '-');
            if (dash) {
                int s = atoi(tok), e = atoi(dash + 1);
                if (s >= 0 && e < CLUSTER_SLOTS && s <= e)
                    for (int x = s; x <= e; x++) out_slot[x] = ni;
            } else {
                int s = atoi(tok);
                if (s >= 0 && s < CLUSTER_SLOTS) out_slot[s] = ni;
            }
        }
        if (eol) *eol = saved;
        p = eol ? eol + 1 : p + strlen(p);
    }
}

/* Connect to ip:port, run CLUSTER NODES, parse into caller-owned buffers.
 * Returns 0 on success, -1 on any failure -- never exits, so it is safe to
 * call from the background refresh thread once the process is serving
 * traffic (unlike fetch_cluster_nodes() below, which is startup-only and
 * fatal on failure). */
static int fetch_cluster_nodes_from(const char *ip, int port, ClusterNode *out_nodes, int *out_count, int *out_slot) {
    /* Short timeout, not tcp_connect()'s fixed 5s: this runs every refresh
     * cycle, and the seed configured in proxy.conf keeps getting retried
     * first even after it's confirmed dead (see topology_refresh_loop()) --
     * at 5s that alone would stretch every single cycle out to 5+ seconds,
     * badly slowing down how fast a real failover gets noticed. */
    int fd = tcp_connect_timeout(ip, port, g_config.backend_connect_timeout_ms);
    if (fd < 0) return -1;
    ssize_t wn = write(fd, CLUSTER_NODES_CMD, strlen(CLUSTER_NODES_CMD));
    if (wn < 0) {
        close(fd);
        return -1;
    }
    size_t len;
    char *resp = recv_bulk(fd, &len);
    close(fd);
    if (len == 0 || resp[0] == '-') {
        free(resp);
        return -1;
    }
    char *body = resp;
    if (resp[0] == '$') {
        char *eol = memchr(resp, '\n', len);
        if (eol) body = eol + 1;
    }
    *out_count = 0;
    for (int i = 0; i < CLUSTER_SLOTS; i++) out_slot[i] = -1;
    /* Quiet by design: this runs every refresh cycle forever, and most
     * cycles see the same handful of replicas/failed masters skipped again
     * -- logging each one would spam the log with no new information.
     * reconcile_and_publish() already prints whenever something actually
     * changes, which is the signal worth surfacing here. */
    parse_cluster_nodes_into(body, out_nodes, out_count, out_slot, /*verbose=*/0);
    free(resp);
    return 0;
}

void fetch_cluster_nodes(void) {
    printf("Connecting to %s:%d to fetch cluster topology...\n", g_config.server_ip, g_config.server_port);
    int fd = tcp_connect(g_config.server_ip, g_config.server_port);
    if (fd < 0) exit(1);
    ssize_t wn = write(fd, CLUSTER_NODES_CMD, strlen(CLUSTER_NODES_CMD));
    if (wn < 0) {
        perror("write");
        close(fd);
        exit(1);
    }
    size_t len;
    char *resp = recv_bulk(fd, &len);
    close(fd);
    if (len == 0 || resp[0] == '-') {
        fprintf(stderr, "CLUSTER NODES failed: %.*s\n", (int)len, resp ? resp : "");
        free(resp);
        exit(1);
    }
    char *body = resp;
    if (resp[0] == '$') {
        char *eol = memchr(resp, '\n', len);
        if (eol) body = eol + 1;
    }

    /* This is the one-time startup discovery: it runs single-threaded,
     * before any worker thread exists, so writing the global topology
     * directly (no lock, no reconcile) is safe -- same as before this
     * feature existed. */
    g_node_count = 0;
    memset(g_slot_map, -1, sizeof(g_slot_map));
    parse_cluster_nodes_into(body, g_nodes, &g_node_count, g_slot_map, /*verbose=*/1);
    free(resp);
    memset(g_node_gen, 0, sizeof(g_node_gen));
    memset(g_node_has_slots, 0, sizeof(g_node_has_slots));
    for (int s = 0; s < CLUSTER_SLOTS; s++)
        if (g_slot_map[s] >= 0) g_node_has_slots[g_slot_map[s]] = 1;
    atomic_store_explicit(&g_topology_version, 1, memory_order_release);

    printf("Discovered %d cluster node(s):\n", g_node_count);
    for (int i = 0; i < g_node_count; i++)
        printf("  [%d] %s:%d  flags=%-24s %s\n", i, g_nodes[i].ip, g_nodes[i].port, g_nodes[i].flags,
               g_nodes[i].is_master ? "(master)" : "(replica)");
    printf("\n");
    probe_backend_versions(g_nodes, g_node_count, g_node_has_slots);
}

/* ── periodic topology refresh ──────────────────────────────────────── */
/* How long (wall-clock seconds) index i's owning master has been missing
 * from the latest discovery with nobody else having taken over its slots.
 * File-local: only ever touched from the single background refresh thread,
 * so it needs no locking of its own. */
static time_t fail_since[MAX_NODES];
/* Latches the grace-period gen bump to a single edge instead of firing on
 * every refresh once the threshold has passed. Cleared as soon as index i
 * is matched again. */
static int grace_released[MAX_NODES];

/* Last known owning index for each slot, kept even after that index's
 * master has dropped out of discovery and g_slot_map has gone unmapped
 * (-1) for it. This is deliberately a *different* array from g_slot_map:
 * g_slot_map must go unmapped immediately so routing fails fast instead of
 * hammering a dead address (see the design note above), but the fallback
 * "did a new node_id take over an old index's slots" match below needs the
 * pre-outage ownership to still be there whenever the promoted replica
 * shows up -- which may be many refresh cycles later. Seeded from the live
 * slot map the first time this runs. */
static int owner_idx[CLUSTER_SLOTS];
static int owner_idx_seeded = 0;

/* Nudges every worker out of its blocking el_poll(-1) right away instead of
 * leaving it to notice g_topology_version changed only whenever it next
 * wakes up for unrelated client/backend traffic. Best-effort: a worker
 * whose wake_w isn't ready yet (still starting up) or whose pipe is
 * momentarily full (a wake is already pending for it) is simply skipped --
 * either way it will still pick up the new topology, just not instantly. */
static void wake_all_workers(void) {
    int nw = g_config.worker_thread_no; /* clamped to [1, MAX_WORKERS] by parse_conf() */
    for (int i = 0; i < nw; i++) {
        int fd = g_workers[i].wake_w;
        if (fd < 0) continue;
        char b = 1;
        ssize_t r = write(fd, &b, 1);
        (void)r;
    }
}

/* Reconciles a freshly discovered topology (dn/dcount/dslot) against the
 * live global one and publishes the result under g_topology_lock. Keeps
 * index assignment stable across refreshes:
 *   - same node_id found again                    -> same index, refreshed
 *   - a *different* node_id now owns an old        -> same index reused,
 *     index's slots (and that old node_id is gone)    gen++ (scenario 3:
 *                                                      failover to a promoted
 *                                                      replica)
 *   - an old index's slots have no claimant at all -> gen++ once the
 *     (grace period elapsed)                          fail-grace has elapsed
 *                                                      (scenario 2), slots
 *                                                      go unmapped (-1)
 *   - slots claimed by a node_id never seen before  -> brand new index
 *     (scenario 1)
 */
static void reconcile_and_publish(ClusterNode *dn, int dcount, int *dslot, const char *src_ip, int src_port) {
    time_t now = time(NULL);

    ClusterNode nodes[MAX_NODES];
    int gen[MAX_NODES], orig_gen[MAX_NODES];
    int old_slot[CLUSTER_SLOTS];
    int old_count;

    pthread_rwlock_rdlock(&g_topology_lock);
    memcpy(nodes, g_nodes, sizeof(nodes));
    memcpy(gen, g_node_gen, sizeof(gen));
    memcpy(old_slot, g_slot_map, sizeof(old_slot));
    old_count = g_node_count;
    pthread_rwlock_unlock(&g_topology_lock);
    memcpy(orig_gen, gen, sizeof(gen));
    if (!owner_idx_seeded) {
        memcpy(owner_idx, old_slot, sizeof(owner_idx));
        owner_idx_seeded = 1;
    }

    int count = old_count;
    int matched_old[MAX_NODES] = {0};
    int disc_to_idx[MAX_NODES];
    /* Marks index i as "worth calling out to the user this cycle": either a
     * brand new shard (scenario 1) or an existing shard's master address
     * changing under it (scenario 3, failover to a promoted replica). Not
     * set for scenario 2 (grace release) -- nothing new appeared there. */
    int is_new[MAX_NODES] = {0};

    for (int j = 0; j < dcount; j++) {
        int idx = -1;
        for (int i = 0; i < old_count; i++)
            if (strcmp(nodes[i].node_id, dn[j].node_id) == 0) {
                idx = i;
                break;
            }
        if (idx < 0) {
            /* Fallback: does dn[j] now own slots that used to belong to some
             * old index whose old node_id is nowhere in this round's
             * discovery any more? If so this is a failover onto a promoted
             * replica -- reuse that index instead of minting a new one. */
            for (int s = 0; s < CLUSTER_SLOTS && idx < 0; s++) {
                if (dslot[s] != j) continue;
                int cand = owner_idx[s];
                if (cand < 0) continue;
                int still_present = 0;
                for (int k = 0; k < dcount; k++)
                    if (strcmp(nodes[cand].node_id, dn[k].node_id) == 0) {
                        still_present = 1;
                        break;
                    }
                if (!still_present) idx = cand;
            }
        }
        if (idx >= 0) {
            matched_old[idx] = 1;
            int addr_changed = strcmp(nodes[idx].node_id, dn[j].node_id) != 0 || strcmp(nodes[idx].ip, dn[j].ip) != 0 ||
                               nodes[idx].port != dn[j].port;
            nodes[idx] = dn[j];
            nodes[idx].is_master = 1;
            if (addr_changed) {
                gen[idx]++;
                is_new[idx] = 1;
            }
            fail_since[idx] = 0;
            grace_released[idx] = 0;
            disc_to_idx[j] = idx;
        } else if (count < MAX_NODES) {
            int ni = count++;
            nodes[ni] = dn[j];
            nodes[ni].is_master = 1;
            gen[ni] = 0;
            fail_since[ni] = 0;
            grace_released[ni] = 0;
            matched_old[ni] = 1;
            is_new[ni] = 1;
            disc_to_idx[j] = ni;
        } else {
            fprintf(stderr,
                    "[topology] MAX_NODES(%d) reached, dropping newly discovered "
                    "shard %s (%s:%d) -- its slots stay unmapped\n",
                    MAX_NODES, dn[j].node_id, dn[j].ip, dn[j].port);
            disc_to_idx[j] = -1;
        }
    }

    /* Old indices nobody claimed this round: either still transiently
     * missing (start/continue the grace timer) or grace has now elapsed
     * (bump gen once so worker_sync_topology flushes idle connections). */
    for (int i = 0; i < old_count; i++) {
        if (matched_old[i]) continue;
        if (fail_since[i] == 0) {
            fail_since[i] = now;
        } else if (!grace_released[i] &&
                   difftime(now, fail_since[i]) * 1000.0 > (double)g_config.cluster_node_fail_grace_ms) {
            gen[i]++;
            grace_released[i] = 1;
        }
    }

    int slot[CLUSTER_SLOTS];
    int has_slots[MAX_NODES] = {0};
    for (int s = 0; s < CLUSTER_SLOTS; s++) {
        int j = dslot[s];
        slot[s] = (j >= 0 && disc_to_idx[j] >= 0) ? disc_to_idx[j] : -1;
        /* Remember the last real owner even once slot[s] goes unmapped, so
         * a promoted replica discovered many cycles later can still be
         * recognized as taking over this index (see owner_idx above). */
        if (slot[s] >= 0) {
            owner_idx[s] = slot[s];
            has_slots[slot[s]] = 1;
        }
    }

    int changed = (count != old_count);
    if (!changed)
        for (int i = 0; i < old_count; i++)
            if (gen[i] != orig_gen[i]) {
                changed = 1;
                break;
            }
    if (!changed) changed = (memcmp(old_slot, slot, sizeof(slot)) != 0);

    if (!changed) {
        if (difftime(now, last_probe) >= BACKEND_PROBE_INTERVAL_SEC) probe_backend_versions(nodes, count, has_slots);
        return;
    }

    /* Only printed when the master set/topology actually changed this
     * round, with whatever's new/updated called out. */
    printf("[topology] current master(s) (%d), source=%s:%d:\n", count, src_ip, src_port);
    for (int i = 0; i < count; i++)
        printf("  [%d] %s:%d%s\n", i, nodes[i].ip, nodes[i].port, is_new[i] ? "   <-- NEW/UPDATED MASTER" : "");

    pthread_rwlock_wrlock(&g_topology_lock);
    memcpy(g_nodes, nodes, sizeof(g_nodes));
    memcpy(g_node_gen, gen, sizeof(g_node_gen));
    memcpy(g_node_has_slots, has_slots, sizeof(g_node_has_slots));
    memcpy(g_slot_map, slot, sizeof(g_slot_map));
    g_node_count = count;
    pthread_rwlock_unlock(&g_topology_lock);
    atomic_fetch_add_explicit(&g_topology_version, 1, memory_order_release);
    wake_all_workers();

    printf("[topology] refresh published: %d node(s) (was %d), source=%s:%d\n", count, old_count, src_ip, src_port);
    probe_backend_versions(nodes, count, has_slots);
}

static void *topology_refresh_loop(void *arg) {
    (void)arg;
    int interval_ms = g_config.cluster_refresh_interval_ms;
    for (;;) {
        struct timespec ts;
        ts.tv_sec = interval_ms / 1000;
        ts.tv_nsec = (long)(interval_ms % 1000) * 1000000L;
        nanosleep(&ts, NULL);

        ClusterNode dn[MAX_NODES];
        int dcount = 0, dslot[CLUSTER_SLOTS];
        char used_ip[64];
        int used_port;
        snprintf(used_ip, sizeof(used_ip), "%s", g_config.server_ip);
        used_port = g_config.server_port;
        int ok = fetch_cluster_nodes_from(used_ip, used_port, dn, &dcount, dslot);

        if (ok < 0) {
            /* Seed unreachable: fall back to any node we already know about,
             * so a permanently-down seed doesn't stall refresh forever. */
            ClusterNode fallback[MAX_NODES];
            int fcount;
            pthread_rwlock_rdlock(&g_topology_lock);
            fcount = g_node_count;
            memcpy(fallback, g_nodes, sizeof(fallback));
            pthread_rwlock_unlock(&g_topology_lock);
            for (int i = 0; i < fcount && ok < 0; i++) {
                ok = fetch_cluster_nodes_from(fallback[i].ip, fallback[i].port, dn, &dcount, dslot);
                if (ok == 0) {
                    snprintf(used_ip, sizeof(used_ip), "%s", fallback[i].ip);
                    used_port = fallback[i].port;
                }
            }
        }

        if (ok < 0) {
            fprintf(stderr, "[topology] refresh failed: seed and all known nodes unreachable, "
                            "keeping last-known-good topology\n");
            continue;
        }
        if (dcount == 0) {
            fprintf(stderr,
                    "[topology] refresh returned 0 master(s) from %s:%d, ignoring "
                    "(keeping last-known-good topology)\n",
                    used_ip, used_port);
            continue;
        }
        reconcile_and_publish(dn, dcount, dslot, used_ip, used_port);
    }
    return NULL;
}

void topology_start_refresh_thread(void) {
    if (g_config.cluster_refresh_interval_ms <= 0) return;
    pthread_t tid;
    if (pthread_create(&tid, NULL, topology_refresh_loop, NULL) != 0) {
        perror("pthread_create(topology_refresh_loop)");
        return;
    }
    pthread_detach(tid);
    printf("Cluster topology refresh: every %dms (fail-grace=%dms)\n\n", g_config.cluster_refresh_interval_ms,
           g_config.cluster_node_fail_grace_ms);
}

/* ── CRC16 for slot computation ─────────────────────────────────────── */
static uint16_t crc16(const char *buf, int len) {
    uint16_t crc = 0;
    while (len--) {
        crc ^= (uint16_t)((unsigned char)*buf++) << 8;
        for (int i = 0; i < 8; i++) crc = (crc & 0x8000) ? (uint16_t)((crc << 1) ^ 0x1021) : (uint16_t)(crc << 1);
    }
    return crc;
}

int key_slot(const char *key, int klen) {
    const char *s = memchr(key, '{', klen);
    if (s) {
        const char *e = memchr(s + 1, '}', klen - (int)(s - key) - 1);
        if (e && e > s + 1) return crc16(s + 1, (int)(e - s) - 1) & 0x3FFF;
    }
    return crc16(key, klen) & 0x3FFF;
}
