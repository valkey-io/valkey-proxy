#include "proxy.h"
#include <sys/time.h>

#define FANOUT_RESET_CLIENT(client)             \
    do {                                        \
        (client)->pbuf_len = 0;                 \
        (client)->hbuf_len = 0;                 \
        (client)->using_pbuf = 0;               \
        (client)->state = CS_READING_HDR;       \
        fd_watch(w, (client)->fd, EL_READABLE); \
    } while (0)

/* ── fanout sub-request completion ─────────────────────────────────── */
void fanout_ctx_free(FanoutCtx *ctx) {
    if (!ctx) return;
    for (int i = 0; i < ctx->nsubs; i++) free(ctx->subs[i].rbuf);
    free(ctx);
}

/* First master in this worker's current topology snapshot, or 0 if the
 * snapshot has no nodes at all -- matches every call site's pre-existing
 * "int ni = 0; for(...) if(is_master){ni=x;break;}" idiom (used for
 * HELP-style/keyless commands that must land on *some* node). */
int topo_first_master(Worker *w) {
    for (int x = 0; x < w->topo.node_count; x++)
        if (w->topo.nodes[x].is_master) return x;
    return 0;
}

/* Collects every master index into out (capacity MAX_NODES), falling back
 * to every known index if somehow none are flagged master (matches the
 * pre-existing broadcast-to-all-masters idiom). Returns the count. */
int topo_all_masters(Worker *w, int *out) {
    int n = 0;
    for (int i = 0; i < w->topo.node_count; i++)
        if (w->topo.nodes[i].is_master) out[n++] = i;
    if (n == 0)
        for (int i = 0; i < w->topo.node_count; i++) out[n++] = i;
    return n;
}

void handle_fanout_sub_readable(Worker *w, PoolConn *conn) {
    FanoutSub *sub = conn->fanout_sub;
    FanoutCtx *ctx = sub->ctx;

    /* epoll is edge-triggered (see el_epoll.c's EPOLLET), which only
     * re-notifies on *new* arrivals. A reply that lands in one network
     * burst larger than a single read() (e.g. COMMAND INFO's multi-KB
     * array) must be drained in a loop here -- returning after one
     * partial read would leave the rest sitting in the kernel buffer
     * with no further event ever firing for it, hanging this sub
     * forever. */
    for (;;) {
        if (sub->rbuf_cap - sub->rbuf_len < 4096) {
            int nc = sub->rbuf_cap ? sub->rbuf_cap * 2 : 4096;
            char *tmp = realloc(sub->rbuf, (size_t)nc);
            if (!tmp) {
                sub->error = 1;
                break;
            }
            sub->rbuf = tmp;
            sub->rbuf_cap = nc;
        }
        ssize_t n = read(conn->fd, sub->rbuf + sub->rbuf_len, (size_t)(sub->rbuf_cap - sub->rbuf_len));
        if (n < 0) {
            if (errno == EAGAIN) return; /* drained; wait for more data */
            sub->error = 1;
            break;
        }
        if (n == 0) {
            sub->error = 1;
            break;
        }
        sub->rbuf_len += (int)n;

        int rlen = resp_response_len(sub->rbuf, sub->rbuf_len);
        if (rlen < 0) {
            sub->error = 1;
            break;
        }
        if (rlen > 0) {
            sub->rbuf_len = rlen;
            break;
        }
        /* rlen == 0: incomplete, keep draining the socket */
    }

    /* This POOL_NORMAL connection may have been lazily upgraded to RESP3 by
     * some *other* client's traffic in the meantime (upgrades are global and
     * per-connection, not per-command -- see resp3_conn_needs_upgrade() in
     * hello.c), in which case the backend just replied in RESP3 regardless
     * of what this fan-out sub-request itself asked for. Every
     * fanout_agg_*() function downstream (fanout_agg_mget, _array, _scan,
     * ...) parses sub->rbuf as raw RESP2 bytes, so normalize it right here
     * at the ingestion point -- once, generically -- instead of teaching
     * each of them about RESP3. nil_array_depth=-1 (never): none of the
     * fanned-out commands' *individual per-node* replies are themselves a
     * nil array anywhere in them (MGET/KEYS/SCAN/etc. all reply with a
     * plain array or per-key/per-element nil *bulk*), only the client-
     * facing aggregated reply could be, and that's assembled fresh by
     * fanout_agg_*() from scratch, never copied from sub->rbuf verbatim. */
    if (!sub->error && conn->resp3) {
        char *out;
        int out_len;
        if (resp3_downgrade(sub->rbuf, sub->rbuf_len, -1, &out, &out_len) < 0) {
            sub->error = 1;
        } else {
            free(sub->rbuf);
            sub->rbuf = out;
            sub->rbuf_len = out_len;
            sub->rbuf_cap = out_len;
        }
    }

    sub->done = 1;
    conn->fanout_sub = NULL;
    fd_unwatch(w, conn->fd, EL_READABLE);
    conn->waiting_client = NULL;
    /* A conn that just errored/EOF'd must not be recycled as if healthy --
     * destroy it instead, mirroring the single-key error path. */
    if (sub->error)
        pool_destroy_conn(w, conn);
    else
        pool_release(w, conn);
    sub->conn = NULL;

    if (sub->error) ctx->had_error = 1;
    ctx->ndone++;
    if (ctx->ndone < ctx->nsubs) return;

    fanout_aggregate_and_reply(w, ctx);
}

/* Same as the tail of handle_fanout_sub_readable(), for a sub sent over a
 * shared connection (see mux.c), which owns the socket and hands over just
 * this sub's reply: buf[0..len), or buf == NULL if the sub failed. */
void fanout_sub_complete(Worker *w, FanoutSub *sub, const char *buf, int len, int backend_resp3) {
    FanoutCtx *ctx = sub->ctx;
    if (!buf) {
        sub->error = 1;
    } else if (backend_resp3) {
        /* nil_array_depth=-1, as in handle_fanout_sub_readable(). */
        if (resp3_downgrade(buf, len, -1, &sub->rbuf, &sub->rbuf_len) < 0)
            sub->error = 1;
        else
            sub->rbuf_cap = sub->rbuf_len;
    } else if ((sub->rbuf = malloc((size_t)len)) != NULL) {
        memcpy(sub->rbuf, buf, (size_t)len);
        sub->rbuf_len = sub->rbuf_cap = len;
    } else {
        sub->error = 1;
    }
    sub->done = 1;
    if (sub->error) ctx->had_error = 1;
    ctx->ndone++;
    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
}

/* ── resp helpers (fanout-only) ─────────────────────────────────────── */
static const char *resp_array_nth(const char *buf, int len, int n, int *elem_len) {
    if (len < 4 || buf[0] != '*') return NULL;
    const char *p = memchr(buf + 1, '\n', len - 1);
    if (!p) return NULL;
    int off = (int)(p - buf) + 1;
    for (int i = 0; i < atoi(buf + 1); i++) {
        if (off >= len) return NULL;
        int el = resp_response_len(buf + off, len - off);
        if (el <= 0) return NULL;
        if (i == n) {
            *elem_len = el;
            return buf + off;
        }
        off += el;
    }
    return NULL;
}

static const char *resp_parse_bulk_str(const char *buf, int buflen, int *val_len) {
    if (!buf || buflen < 3 || buf[0] != '$') return NULL;
    int n = atoi(buf + 1);
    if (n < 0) return NULL;
    const char *nl = (const char *)memchr(buf + 1, '\n', buflen - 1);
    if (!nl) return NULL;
    int hdr = (int)(nl - buf) + 1;
    if (hdr + n + 2 > buflen) return NULL;
    *val_len = n;
    return nl + 1;
}

/* ── aggregation helpers ────────────────────────────────────────────── */
static char *fanout_agg_sum(FanoutCtx *ctx, int *out_len) {
    long long total = 0;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf) continue;
        if (s->rbuf[0] == ':')
            total += atoll(s->rbuf + 1);
        else if (s->rbuf[0] == '-') {
            char *r = malloc((size_t)s->rbuf_len);
            if (!r) return NULL;
            memcpy(r, s->rbuf, (size_t)s->rbuf_len);
            *out_len = s->rbuf_len;
            return r;
        }
    }
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), ":%lld\r\n", total);
    char *r = malloc((size_t)n);
    if (!r) return NULL;
    memcpy(r, tmp, (size_t)n);
    *out_len = n;
    return r;
}

static char *fanout_agg_ok(FanoutCtx *ctx, int *out_len) {
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] != '+') {
            if (s->rbuf && s->rbuf_len > 0) {
                char *r = malloc((size_t)s->rbuf_len);
                if (!r) return NULL;
                memcpy(r, s->rbuf, (size_t)s->rbuf_len);
                *out_len = s->rbuf_len;
                return r;
            }
            const char *e = "-ERR partial failure\r\n";
            char *r = strdup(e);
            *out_len = (int)strlen(e);
            return r;
        }
    }
    char *r = strdup("+OK\r\n");
    *out_len = 5;
    return r;
}

static char *fanout_agg_any_ok(FanoutCtx *ctx, int *out_len) {
    FanoutSub *last_err = NULL;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (!s->error && s->rbuf && s->rbuf[0] == '+') {
            char *r = strdup("+OK\r\n");
            *out_len = 5;
            return r;
        }
        if (s->rbuf && s->rbuf_len > 0) last_err = s;
    }
    if (last_err) {
        char *r = malloc((size_t)last_err->rbuf_len);
        if (!r) return NULL;
        memcpy(r, last_err->rbuf, (size_t)last_err->rbuf_len);
        *out_len = last_err->rbuf_len;
        return r;
    }
    const char *e = "-ERR fan-out backend error\r\n";
    char *r = strdup(e);
    *out_len = (int)strlen(e);
    return r;
}

/* Like fanout_agg_any_ok, but returns the first successful node's raw
 * response verbatim (e.g. the sha1 bulk string from SCRIPT LOAD) instead of
 * collapsing it to a generic +OK. */
static char *fanout_agg_first_ok(FanoutCtx *ctx, int *out_len) {
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (!s->error && s->rbuf && s->rbuf[0] != '-') {
            char *r = malloc((size_t)s->rbuf_len);
            if (!r) return NULL;
            memcpy(r, s->rbuf, (size_t)s->rbuf_len);
            *out_len = s->rbuf_len;
            return r;
        }
    }
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->rbuf && s->rbuf_len > 0) {
            char *r = malloc((size_t)s->rbuf_len);
            if (!r) return NULL;
            memcpy(r, s->rbuf, (size_t)s->rbuf_len);
            *out_len = s->rbuf_len;
            return r;
        }
    }
    const char *e = "-ERR fan-out backend error\r\n";
    char *r = strdup(e);
    *out_len = (int)strlen(e);
    return r;
}

/* Requires every node to succeed (unlike fanout_agg_first_ok, which is
 * satisfied by any single success). Used for commands whose success reply
 * isn't a simple '+' string (e.g. FUNCTION LOAD's library-name bulk string),
 * so fanout_agg_ok's '+'-prefix check can't be reused: it would mistake the
 * first node's successful bulk reply for a failure and return early without
 * checking whether a later node actually errored. */
static char *fanout_agg_all_ok_bulk(FanoutCtx *ctx, int *out_len) {
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] == '-') {
            if (s->rbuf && s->rbuf_len > 0) {
                char *r = malloc((size_t)s->rbuf_len);
                if (!r) return NULL;
                memcpy(r, s->rbuf, (size_t)s->rbuf_len);
                *out_len = s->rbuf_len;
                return r;
            }
            const char *e = "-ERR partial failure\r\n";
            char *r = strdup(e);
            *out_len = (int)strlen(e);
            return r;
        }
    }
    FanoutSub *s0 = &ctx->subs[0];
    char *r = malloc((size_t)s0->rbuf_len);
    if (!r) return NULL;
    memcpy(r, s0->rbuf, (size_t)s0->rbuf_len);
    *out_len = s0->rbuf_len;
    return r;
}

static char *fanout_agg_always_ok(FanoutCtx *ctx, int *out_len) {
    (void)ctx;
    char *r = strdup("+OK\r\n");
    *out_len = 5;
    return r;
}

static char *fanout_agg_array(FanoutCtx *ctx, int *out_len) {
    int total_elems = 0, total_data = 0;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] != '*') continue;
        const char *nl = memchr(s->rbuf + 1, '\n', s->rbuf_len - 1);
        if (!nl) continue;
        int cnt = atoi(s->rbuf + 1);
        if (cnt <= 0) continue;
        total_elems += cnt;
        total_data += s->rbuf_len - (int)(nl - s->rbuf) - 1;
    }
    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", total_elems);
    char *out = malloc((size_t)(hdr_len + total_data + 1));
    if (!out) return NULL;
    int off = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    off += hdr_len;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] != '*') continue;
        const char *nl = memchr(s->rbuf + 1, '\n', s->rbuf_len - 1);
        if (!nl) continue;
        if (atoi(s->rbuf + 1) <= 0) continue;
        int data_off = (int)(nl - s->rbuf) + 1;
        int data_len = s->rbuf_len - data_off;
        memcpy(out + off, s->rbuf + data_off, (size_t)data_len);
        off += data_len;
    }
    *out_len = off;
    return out;
}

/* SLOWLOG GET: each node's reply is an array of entries, newest first, each
 * entry itself an array [id, timestamp, duration, args, client-addr,
 * client-name]. Every node was asked for the same count, so the global
 * newest-count entries are guaranteed to be among the union; entries are
 * merged by timestamp (newest first) and truncated to ctx->slowlog_count
 * (-1 = keep all). Entries are copied verbatim.
 *
 * Also used for COMMANDLOG GET, whose entries have the same shape (element 2
 * is a byte size instead of a duration for large-request/large-reply, which
 * doesn't matter since merging only looks at the timestamp).
 *
 * Known limitation: slowlog ids are per-node counters, so ids in the merged
 * reply are not unique cluster-wide, and timestamps only have 1s resolution
 * -- entries from different nodes within the same second are ordered by
 * node, not by true arrival order. */
typedef struct {
    const char *p;
    int len;
    long long ts;
    int sub, pos;
} SlowlogEntry;

static int slowlog_entry_cmp(const void *a, const void *b) {
    const SlowlogEntry *x = a, *y = b;
    if (x->ts != y->ts) return x->ts > y->ts ? -1 : 1;
    if (x->sub != y->sub) return x->sub - y->sub;
    return x->pos - y->pos;
}

/* Timestamp (element 1) of one slowlog entry, or 0 if malformed. */
static long long slowlog_entry_ts(const char *p, int len) {
    if (len < 1 || p[0] != '*') return 0;
    const char *nl = memchr(p, '\n', (size_t)len);
    if (!nl) return 0;
    int off = (int)(nl - p) + 1;
    int el = resp_response_len(p + off, len - off); /* skip id */
    if (el <= 0) return 0;
    off += el;
    if (off >= len || p[off] != ':') return 0;
    return atoll(p + off + 1);
}

static char *fanout_agg_slowlog_get(FanoutCtx *ctx, int *out_len) {
    int total = 0;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf_len < 1) continue;
        if (s->rbuf[0] == '-') {
            /* e.g. invalid count -- relay the backend's error verbatim */
            char *r = malloc((size_t)s->rbuf_len);
            if (!r) return NULL;
            memcpy(r, s->rbuf, (size_t)s->rbuf_len);
            *out_len = s->rbuf_len;
            return r;
        }
        if (s->rbuf[0] == '*' && atoi(s->rbuf + 1) > 0) total += atoi(s->rbuf + 1);
    }

    SlowlogEntry *ents = malloc(sizeof(SlowlogEntry) * (size_t)(total > 0 ? total : 1));
    if (!ents) return NULL;
    int n = 0;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf_len < 1 || s->rbuf[0] != '*') continue;
        int cnt = atoi(s->rbuf + 1);
        const char *nl = memchr(s->rbuf, '\n', (size_t)s->rbuf_len);
        if (cnt <= 0 || !nl) continue;
        int off = (int)(nl - s->rbuf) + 1;
        for (int j = 0; j < cnt && n < total; j++) {
            int el = resp_response_len(s->rbuf + off, s->rbuf_len - off);
            if (el <= 0) break;
            ents[n].p = s->rbuf + off;
            ents[n].len = el;
            ents[n].ts = slowlog_entry_ts(s->rbuf + off, el);
            ents[n].sub = i;
            ents[n].pos = j;
            n++;
            off += el;
        }
    }
    qsort(ents, (size_t)n, sizeof(SlowlogEntry), slowlog_entry_cmp);

    int keep = n;
    if (ctx->slowlog_count >= 0 && ctx->slowlog_count < keep) keep = (int)ctx->slowlog_count;

    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", keep);
    int data = 0;
    for (int i = 0; i < keep; i++) data += ents[i].len;
    char *out = malloc((size_t)(hdr_len + data + 1));
    if (!out) {
        free(ents);
        return NULL;
    }
    int o = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    o += hdr_len;
    for (int i = 0; i < keep; i++) {
        memcpy(out + o, ents[i].p, (size_t)ents[i].len);
        o += ents[i].len;
    }
    free(ents);
    *out_len = o;
    return out;
}

static char *fanout_agg_mget(FanoutCtx *ctx, int *out_len) {
    int total = ctx->total_keys;
    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", total);
    int total_size = hdr_len;
    for (int i = 0; i < total; i++) {
        FanoutSub *s = &ctx->subs[ctx->mget_sub[i]];
        if (s->error || !s->rbuf) {
            total_size += 5;
            continue;
        }
        int el;
        if (!resp_array_nth(s->rbuf, s->rbuf_len, ctx->mget_pos[i], &el))
            total_size += 5;
        else
            total_size += el;
    }
    char *out = malloc((size_t)total_size + 1);
    if (!out) return NULL;
    int off = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    off += hdr_len;
    for (int i = 0; i < total; i++) {
        FanoutSub *s = &ctx->subs[ctx->mget_sub[i]];
        if (s->error || !s->rbuf) {
            memcpy(out + off, "$-1\r\n", 5);
            off += 5;
            continue;
        }
        int el;
        const char *elem = resp_array_nth(s->rbuf, s->rbuf_len, ctx->mget_pos[i], &el);
        if (!elem) {
            memcpy(out + off, "$-1\r\n", 5);
            off += 5;
            continue;
        }
        memcpy(out + off, elem, (size_t)el);
        off += el;
    }
    *out_len = off;
    return out;
}

static uint64_t scan_parse_cursor(const char *buf, int len) {
    if (!buf || len < 10 || buf[0] != '*') return 0;
    const char *nl = memchr(buf + 1, '\n', len - 1);
    if (!nl) return 0;
    int off = (int)(nl - buf) + 1;
    if (off >= len || buf[off] != '$') return 0;
    const char *p = buf + off + 1;
    const char *nl2 = memchr(p, '\n', len - off - 1);
    if (!nl2) return 0;
    int clen = atoi(p);
    if (clen < 0 || clen > 20) return 0;
    int ds = (int)(nl2 - buf) + 1;
    if (ds + clen > len) return 0;
    char tmp[24];
    memcpy(tmp, buf + ds, (size_t)clen);
    tmp[clen] = '\0';
    return strtoull(tmp, NULL, 10);
}

static const char *scan_keys_array(const char *buf, int len, int *arr_len) {
    if (!buf || len < 10 || buf[0] != '*') return NULL;
    const char *nl = memchr(buf + 1, '\n', len - 1);
    if (!nl) return NULL;
    int off = (int)(nl - buf) + 1;
    if (off >= len || buf[off] != '$') return NULL;
    const char *p = buf + off + 1;
    const char *nl2 = memchr(p, '\n', len - off - 1);
    if (!nl2) return NULL;
    int clen = atoi(p);
    off = (int)(nl2 - buf) + 1 + clen + 2;
    if (off >= len) return NULL;
    int alen = resp_response_len(buf + off, len - off);
    if (alen <= 0) return NULL;
    *arr_len = alen;
    return buf + off;
}

static int scan_encode_cursor(char *out, int outsz, const uint64_t *cursors, int nnodes) {
    int n = 0;
    for (int i = 0; i < nnodes; i++)
        n += snprintf(out + n, (size_t)(outsz - n), "%016llx", (unsigned long long)cursors[i]);
    return n;
}

static void scan_decode_cursor(const char *s, int slen, uint64_t *cursors, int nnodes) {
    for (int i = 0; i < nnodes; i++) {
        if (i * 16 + 16 > slen) {
            cursors[i] = 0;
            continue;
        }
        char tmp[17];
        memcpy(tmp, s + i * 16, 16);
        tmp[16] = '\0';
        cursors[i] = strtoull(tmp, NULL, 16);
    }
}

static char *fanout_agg_scan(FanoutCtx *ctx, int *out_len) {
    uint64_t new_cursors[MAX_NODES];
    memcpy(new_cursors, ctx->scan_node_cursors, (size_t)ctx->scan_ntargets * sizeof(uint64_t));

    for (int si = 0; si < ctx->nsubs; si++) {
        FanoutSub *s = &ctx->subs[si];
        if (s->error || !s->rbuf) {
            new_cursors[s->node_idx] = 0;
            continue;
        }
        new_cursors[s->node_idx] = scan_parse_cursor(s->rbuf, s->rbuf_len);
    }

    int total_elems = 0, total_data = 0;
    for (int si = 0; si < ctx->nsubs; si++) {
        FanoutSub *s = &ctx->subs[si];
        if (s->error || !s->rbuf) continue;
        int al;
        const char *arr = scan_keys_array(s->rbuf, s->rbuf_len, &al);
        if (!arr || arr[0] != '*') continue;
        const char *nl = memchr(arr + 1, '\n', al - 1);
        if (!nl) continue;
        int cnt = atoi(arr + 1);
        if (cnt <= 0) continue;
        total_elems += cnt;
        total_data += al - (int)(nl - arr) - 1;
    }

    int all_done = 1;
    for (int ti = 0; ti < ctx->scan_ntargets; ti++) {
        if (new_cursors[ctx->scan_target_nodes[ti]] != 0) {
            all_done = 0;
            break;
        }
    }

    char cursor_str[MAX_NODES * 16 + 4];
    int cursor_len;
    if (all_done) {
        cursor_str[0] = '0';
        cursor_str[1] = '\0';
        cursor_len = 1;
    } else {
        uint64_t enc[MAX_NODES];
        memset(enc, 0, sizeof(enc));
        for (int ti = 0; ti < ctx->scan_ntargets; ti++) enc[ti] = new_cursors[ctx->scan_target_nodes[ti]];
        cursor_len = scan_encode_cursor(cursor_str, sizeof(cursor_str), enc, ctx->scan_ntargets);
    }

    char keys_hdr[32];
    int keys_hdr_len = snprintf(keys_hdr, sizeof(keys_hdr), "*%d\r\n", total_elems);
    char resp_hdr[64];
    int resp_hdr_len = snprintf(resp_hdr, sizeof(resp_hdr), "*2\r\n$%d\r\n", cursor_len);
    int total_size = resp_hdr_len + cursor_len + 2 + keys_hdr_len + total_data;
    char *out = malloc((size_t)total_size + 1);
    if (!out) return NULL;
    int off = 0;
    memcpy(out + off, resp_hdr, (size_t)resp_hdr_len);
    off += resp_hdr_len;
    memcpy(out + off, cursor_str, (size_t)cursor_len);
    off += cursor_len;
    out[off++] = '\r';
    out[off++] = '\n';
    memcpy(out + off, keys_hdr, (size_t)keys_hdr_len);
    off += keys_hdr_len;
    for (int si = 0; si < ctx->nsubs; si++) {
        FanoutSub *s = &ctx->subs[si];
        if (s->error || !s->rbuf) continue;
        int al;
        const char *arr = scan_keys_array(s->rbuf, s->rbuf_len, &al);
        if (!arr || arr[0] != '*') continue;
        const char *nl = memchr(arr + 1, '\n', al - 1);
        if (!nl || atoi(arr + 1) <= 0) continue;
        int data_off = (int)(nl - arr) + 1;
        int data_len = al - data_off;
        memcpy(out + off, arr + data_off, (size_t)data_len);
        off += data_len;
    }
    *out_len = off;
    return out;
}

static int lcs_compute_len(const char *s1, int l1, const char *s2, int l2) {
    if (l1 <= 0 || l2 <= 0) return 0;
    if (l1 < l2) {
        const char *t = s1;
        s1 = s2;
        s2 = t;
        int x = l1;
        l1 = l2;
        l2 = x;
    }
    int *prev = (int *)calloc((size_t)(l2 + 1), sizeof(int));
    int *curr = (int *)calloc((size_t)(l2 + 1), sizeof(int));
    if (!prev || !curr) {
        free(prev);
        free(curr);
        return 0;
    }
    for (int i = 1; i <= l1; i++) {
        memset(curr, 0, (size_t)(l2 + 1) * sizeof(int));
        for (int j = 1; j <= l2; j++) {
            if (s1[i - 1] == s2[j - 1])
                curr[j] = prev[j - 1] + 1;
            else
                curr[j] = prev[j] > curr[j - 1] ? prev[j] : curr[j - 1];
        }
        int *tmp = prev;
        prev = curr;
        curr = tmp;
    }
    int result = prev[l2];
    free(prev);
    free(curr);
    return result;
}

static char *lcs_compute_string(const char *s1, int l1, const char *s2, int l2, int *out_len) {
    if (l1 <= 0 || l2 <= 0) {
        *out_len = 0;
        return strdup("");
    }
    int cols = l2 + 1;
    int *dp = (int *)calloc((size_t)(l1 + 1) * (size_t)cols, sizeof(int));
    if (!dp) return NULL;
    for (int i = 1; i <= l1; i++)
        for (int j = 1; j <= l2; j++) {
            if (s1[i - 1] == s2[j - 1])
                dp[i * cols + j] = dp[(i - 1) * cols + (j - 1)] + 1;
            else
                dp[i * cols + j] =
                    dp[(i - 1) * cols + j] > dp[i * cols + (j - 1)] ? dp[(i - 1) * cols + j] : dp[i * cols + (j - 1)];
        }
    int len = dp[l1 * cols + l2];
    char *result = (char *)malloc((size_t)len + 1);
    if (!result) {
        free(dp);
        return NULL;
    }
    result[len] = '\0';
    int pos = len, i = l1, j = l2;
    while (i > 0 && j > 0) {
        if (s1[i - 1] == s2[j - 1]) {
            result[--pos] = s1[i - 1];
            i--;
            j--;
        } else if (dp[(i - 1) * cols + j] >= dp[i * cols + (j - 1)])
            i--;
        else
            j--;
    }
    *out_len = len;
    free(dp);
    return result;
}

static char *fanout_agg_lcs(FanoutCtx *ctx, int *out_len) {
    int v1len = 0, v2len = 0;
    const char *v1 = NULL, *v2 = NULL;
    if (!ctx->subs[0].error && ctx->subs[0].rbuf)
        v1 = resp_parse_bulk_str(ctx->subs[0].rbuf, ctx->subs[0].rbuf_len, &v1len);
    if (!ctx->subs[1].error && ctx->subs[1].rbuf)
        v2 = resp_parse_bulk_str(ctx->subs[1].rbuf, ctx->subs[1].rbuf_len, &v2len);
    if (!v1) {
        v1 = "";
        v1len = 0;
    }
    if (!v2) {
        v2 = "";
        v2len = 0;
    }

    char *resp = NULL;
    if (ctx->lcs_mode == 1) {
        int len = lcs_compute_len(v1, v1len, v2, v2len);
        char tmp[32];
        int n = snprintf(tmp, sizeof(tmp), ":%d\r\n", len);
        resp = (char *)malloc((size_t)n);
        if (resp) {
            memcpy(resp, tmp, (size_t)n);
            *out_len = n;
        }
    } else {
        int lcs_len = 0;
        char *lcs = lcs_compute_string(v1, v1len, v2, v2len, &lcs_len);
        if (!lcs) return NULL;
        char hdr[32];
        int hdr_len = snprintf(hdr, sizeof(hdr), "$%d\r\n", lcs_len);
        resp = (char *)malloc((size_t)(hdr_len + lcs_len + 2));
        if (resp) {
            memcpy(resp, hdr, (size_t)hdr_len);
            memcpy(resp + hdr_len, lcs, (size_t)lcs_len);
            resp[hdr_len + lcs_len] = '\r';
            resp[hdr_len + lcs_len + 1] = '\n';
            *out_len = hdr_len + lcs_len + 2;
        }
        free(lcs);
    }
    return resp;
}

/* Re-encode a (RESP2-downgraded) reply element for a RESP3 client: a nil
 * becomes `_`, and a flat key/value array becomes a `%` map, recursing
 * into the values for `depth` levels (depth 0 = only the nil fixup; keys
 * and deeper values are copied verbatim). Writes into dst, which must hold
 * at least len bytes -- the result is never longer than the input. Returns
 * the number of bytes written. */
static int resp2_pairs_to_map(const char *src, int len, int depth, char *dst) {
    if ((src[0] == '$' || src[0] == '*') && atoi(src + 1) < 0) {
        memcpy(dst, "_\r\n", 3);
        return 3;
    }
    int n = src[0] == '*' ? atoi(src + 1) : 0;
    if (depth <= 0 || src[0] != '*' || n % 2) {
        memcpy(dst, src, (size_t)len);
        return len;
    }
    int off = snprintf(dst, 32, "%%%d\r\n", n / 2);
    for (int i = 0; i < n; i++) {
        int el;
        const char *e = resp_array_nth(src, len, i, &el);
        if (!e) {
            memcpy(dst, src, (size_t)len);
            return len;
        }
        off += resp2_pairs_to_map(e, el, i % 2 ? depth - 1 : 0, dst + off);
    }
    return off;
}

/* FUNCTION STATS fan-out: rather than merging every node's running_script/
 * engines into one summary, report one flat map per responding master
 * (node-ip / running_script / engines), since each node's function
 * execution state is independent of the others. Nodes that errored or
 * didn't reply with the expected *4 array are skipped. sub->rbuf is always
 * RESP2 by now, so for a RESP3 client the per-node entry, running_script
 * and engines (plus each engine's stats) are re-encoded as maps. */
static char *fanout_agg_function_stats(Worker *w, FanoutCtx *ctx, int *out_len) {
    int resp3 = ctx->client->resp == 3;
    char *entries[MAX_FANOUT_SUBS];
    int entry_lens[MAX_FANOUT_SUBS];
    int nentries = 0;

    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] != '*') continue;
        int rs_len = 0, en_len = 0;
        const char *rs = resp_array_nth(s->rbuf, s->rbuf_len, 1, &rs_len);
        const char *en = resp_array_nth(s->rbuf, s->rbuf_len, 3, &en_len);
        if (!rs || !en) continue;

        const ClusterNode *nd = &w->topo.nodes[s->node_idx];
        char ipport[96];
        int ipport_len = snprintf(ipport, sizeof(ipport), "%s:%d", nd->ip, nd->port);

        char hdr[196];
        int hdr_len = snprintf(hdr, sizeof(hdr), "%s\r\n$7\r\nnode-ip\r\n$%d\r\n%s\r\n$14\r\nrunning_script\r\n",
                               resp3 ? "%3" : "*6", ipport_len, ipport);
        static const char *engines_hdr = "$7\r\nengines\r\n";
        int engines_hdr_len = (int)strlen(engines_hdr);

        int total = hdr_len + rs_len + engines_hdr_len + en_len;
        char *entry = malloc((size_t)total);
        if (!entry) continue;
        int off = 0;
        memcpy(entry + off, hdr, (size_t)hdr_len);
        off += hdr_len;
        if (resp3) {
            /* running_script: nil or a name/command/duration_ms map;
             * engines: engine name -> map of that engine's counters. */
            off += resp2_pairs_to_map(rs, rs_len, 1, entry + off);
            memcpy(entry + off, engines_hdr, (size_t)engines_hdr_len);
            off += engines_hdr_len;
            off += resp2_pairs_to_map(en, en_len, 2, entry + off);
        } else {
            memcpy(entry + off, rs, (size_t)rs_len);
            off += rs_len;
            memcpy(entry + off, engines_hdr, (size_t)engines_hdr_len);
            off += engines_hdr_len;
            memcpy(entry + off, en, (size_t)en_len);
            off += en_len;
        }

        entries[nentries] = entry;
        entry_lens[nentries] = off;
        nentries++;
    }

    char outer_hdr[32];
    int outer_hdr_len = snprintf(outer_hdr, sizeof(outer_hdr), "*%d\r\n", nentries);
    int total = outer_hdr_len;
    for (int i = 0; i < nentries; i++) total += entry_lens[i];

    char *out = malloc((size_t)total);
    if (!out) {
        for (int i = 0; i < nentries; i++) free(entries[i]);
        return NULL;
    }
    int off = 0;
    memcpy(out, outer_hdr, (size_t)outer_hdr_len);
    off += outer_hdr_len;
    for (int i = 0; i < nentries; i++) {
        memcpy(out + off, entries[i], (size_t)entry_lens[i]);
        off += entry_lens[i];
        free(entries[i]);
    }
    *out_len = off;
    return out;
}

/* ── COMMAND INFO reply post-processing ─────────────────────────────── */
/* Defensive cap on the temporary pointer arrays below; HDR_BUF_SIZE (16384
 * bytes) bounds how many RESP args a single request can carry, which is
 * far below this, so it's never actually hit. */
#define CMD_INFO_MAX_ENTRIES 2048

static int command_info_name_supported(const char *name, int namelen) {
    char buf[64];
    int n = namelen < 63 ? namelen : 63;
    memcpy(buf, name, (size_t)n);
    buf[n] = '\0';
    for (int i = 0; buf[i]; i++) buf[i] = (char)toupper((unsigned char)buf[i]);
    return cmd_is_supported(buf);
}

static char *command_info_build_array(const char **elems, const int *elem_lens, int n, int *out_len) {
    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", n);
    int total = hdr_len;
    for (int i = 0; i < n; i++) total += elem_lens[i];
    char *out = malloc((size_t)total);
    if (!out) return NULL;
    int off = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    off += hdr_len;
    for (int i = 0; i < n; i++) {
        memcpy(out + off, elems[i], (size_t)elem_lens[i]);
        off += elem_lens[i];
    }
    *out_len = off;
    return out;
}

/* COMMAND INFO (no arguments): the backend returns info for every command
 * it knows about; keep only the entries whose name this proxy also
 * supports (rather than padding unsupported ones with nil — real Valkey
 * never emits nil for the no-argument form, so the output shrinks). */
static char *fanout_agg_command_info_all(FanoutCtx *ctx, int *out_len) {
    FanoutSub *s = &ctx->subs[0];
    if (s->error || !s->rbuf) {
        const char *e = "-ERR fan-out backend error\r\n";
        char *r = strdup(e);
        if (r) *out_len = (int)strlen(e);
        return r;
    }
    if (s->rbuf[0] != '*') {
        char *r = malloc((size_t)s->rbuf_len);
        if (!r) return NULL;
        memcpy(r, s->rbuf, (size_t)s->rbuf_len);
        *out_len = s->rbuf_len;
        return r;
    }

    int n = atoi(s->rbuf + 1);
    if (n < 0) n = 0;
    if (n > CMD_INFO_MAX_ENTRIES) n = CMD_INFO_MAX_ENTRIES;

    const char *elems[CMD_INFO_MAX_ENTRIES];
    int elem_lens[CMD_INFO_MAX_ENTRIES];
    int kept = 0;
    for (int i = 0; i < n; i++) {
        int elen;
        const char *elem = resp_array_nth(s->rbuf, s->rbuf_len, i, &elen);
        if (!elem) break;
        int nlen;
        const char *name = resp_array_nth(elem, elen, 0, &nlen);
        int vlen;
        const char *val = name ? resp_parse_bulk_str(name, nlen, &vlen) : NULL;
        if (val && command_info_name_supported(val, vlen)) {
            elems[kept] = elem;
            elem_lens[kept] = elen;
            kept++;
        }
    }
    return command_info_build_array(elems, elem_lens, kept, out_len);
}

/* COMMAND INFO <cmd> [<cmd> ...]: output length must equal the number of
 * requested command names (real Valkey semantics), so entries are kept or
 * replaced with nil in place rather than dropped. */
static char *fanout_agg_command_info_named(FanoutCtx *ctx, int *out_len) {
    FanoutSub *s = &ctx->subs[0];
    if (s->error || !s->rbuf) {
        const char *e = "-ERR fan-out backend error\r\n";
        char *r = strdup(e);
        if (r) *out_len = (int)strlen(e);
        return r;
    }
    if (s->rbuf[0] != '*') {
        char *r = malloc((size_t)s->rbuf_len);
        if (!r) return NULL;
        memcpy(r, s->rbuf, (size_t)s->rbuf_len);
        *out_len = s->rbuf_len;
        return r;
    }

    int n = atoi(s->rbuf + 1);
    if (n < 0) n = 0;
    if (n > CMD_INFO_MAX_ENTRIES) n = CMD_INFO_MAX_ENTRIES;

    static const char *NIL = "*-1\r\n";
    const char *elems[CMD_INFO_MAX_ENTRIES];
    int elem_lens[CMD_INFO_MAX_ENTRIES];
    for (int i = 0; i < n; i++) {
        int elen;
        const char *elem = resp_array_nth(s->rbuf, s->rbuf_len, i, &elen);
        if (!elem) {
            elems[i] = NIL;
            elem_lens[i] = 5;
            continue;
        }
        int is_nil = (elem[0] == '*' && atoi(elem + 1) < 0);
        if (is_nil) {
            elems[i] = NIL;
            elem_lens[i] = 5;
            continue;
        }

        int nlen;
        const char *name = resp_array_nth(elem, elen, 0, &nlen);
        int vlen;
        const char *val = name ? resp_parse_bulk_str(name, nlen, &vlen) : NULL;
        if (val && command_info_name_supported(val, vlen)) {
            elems[i] = elem;
            elem_lens[i] = elen;
        } else {
            elems[i] = NIL;
            elem_lens[i] = 5;
        }
    }
    return command_info_build_array(elems, elem_lens, n, out_len);
}

/* True if the RESP2 element e (el bytes) is a bulk string equal to key. */
static int resp_bulk_equals(const char *e, int el, const char *key) {
    int vlen;
    const char *v = resp_parse_bulk_str(e, el, &vlen);
    return v && vlen == (int)strlen(key) && !memcmp(v, key, (size_t)vlen);
}

/* Copy a RESP2 array as-is except for its type byte (e.g. '~' for a set).
 * Writes into dst, which must hold at least len bytes. */
static int resp2_array_retype(const char *src, int len, char type, char *dst) {
    memcpy(dst, src, (size_t)len);
    if (src[0] == '*' && atoi(src + 1) >= 0) dst[0] = type;
    return len;
}

/* Re-encode one COMMAND DOCS argument (a flat key/value array) as a RESP3
 * map: "flags" becomes a set and nested "arguments" recurse. Same dst/
 * length contract as resp2_pairs_to_map(). */
static int command_docs_arg_to_resp3(const char *src, int len, char *dst);

static int command_docs_args_to_resp3(const char *src, int len, char *dst) {
    int n = src[0] == '*' ? atoi(src + 1) : -1;
    if (n < 0) {
        memcpy(dst, src, (size_t)len);
        return len;
    }
    int off = snprintf(dst, 32, "*%d\r\n", n);
    for (int i = 0; i < n; i++) {
        int el;
        const char *e = resp_array_nth(src, len, i, &el);
        if (!e) {
            memcpy(dst, src, (size_t)len);
            return len;
        }
        off += command_docs_arg_to_resp3(e, el, dst + off);
    }
    return off;
}

static int command_docs_arg_to_resp3(const char *src, int len, char *dst) {
    int n = src[0] == '*' ? atoi(src + 1) : -1;
    if (n < 0 || n % 2) {
        memcpy(dst, src, (size_t)len);
        return len;
    }
    int off = snprintf(dst, 32, "%%%d\r\n", n / 2);
    for (int i = 0; i + 1 < n; i += 2) {
        int kl, vl;
        const char *k = resp_array_nth(src, len, i, &kl);
        const char *v = resp_array_nth(src, len, i + 1, &vl);
        if (!k || !v) {
            memcpy(dst, src, (size_t)len);
            return len;
        }
        memcpy(dst + off, k, (size_t)kl);
        off += kl;
        if (resp_bulk_equals(k, kl, "flags"))
            off += resp2_array_retype(v, vl, '~', dst + off);
        else if (resp_bulk_equals(k, kl, "arguments"))
            off += command_docs_args_to_resp3(v, vl, dst + off);
        else {
            memcpy(dst + off, v, (size_t)vl);
            off += vl;
        }
    }
    return off;
}

/* Re-encode one command's COMMAND DOCS entry (a flat key/value array) the
 * way Valkey replies to a RESP3 client: the entry itself is a map,
 * "history" and "doc_flags" are sets, "arguments" is an array of argument
 * maps, and "subcommands" is a map of subcommand name -> doc map. Same
 * dst/length contract as resp2_pairs_to_map(). */
static int command_docs_to_resp3(const char *src, int len, char *dst) {
    int n = src[0] == '*' ? atoi(src + 1) : -1;
    if (n < 0 || n % 2) {
        memcpy(dst, src, (size_t)len);
        return len;
    }
    int off = snprintf(dst, 32, "%%%d\r\n", n / 2);
    for (int i = 0; i + 1 < n; i += 2) {
        int kl, vl;
        const char *k = resp_array_nth(src, len, i, &kl);
        const char *v = resp_array_nth(src, len, i + 1, &vl);
        if (!k || !v) {
            memcpy(dst, src, (size_t)len);
            return len;
        }
        memcpy(dst + off, k, (size_t)kl);
        off += kl;
        if (resp_bulk_equals(k, kl, "history") || resp_bulk_equals(k, kl, "doc_flags")) {
            off += resp2_array_retype(v, vl, '~', dst + off);
        } else if (resp_bulk_equals(k, kl, "arguments")) {
            off += command_docs_args_to_resp3(v, vl, dst + off);
        } else if (resp_bulk_equals(k, kl, "subcommands") && v[0] == '*' && atoi(v + 1) % 2 == 0) {
            int sn = atoi(v + 1);
            int sub_off = snprintf(dst + off, 32, "%%%d\r\n", sn / 2);
            for (int j = 0; j + 1 < sn; j += 2) {
                int snl, sdl;
                const char *sname = resp_array_nth(v, vl, j, &snl);
                const char *sdoc = resp_array_nth(v, vl, j + 1, &sdl);
                if (!sname || !sdoc) {
                    memcpy(dst, src, (size_t)len);
                    return len;
                }
                memcpy(dst + off + sub_off, sname, (size_t)snl);
                sub_off += snl;
                sub_off += command_docs_to_resp3(sdoc, sdl, dst + off + sub_off);
            }
            off += sub_off;
        } else {
            memcpy(dst + off, v, (size_t)vl);
            off += vl;
        }
    }
    return off;
}

/* COMMAND DOCS [<cmd> ...]: the backend reply is a flat array alternating
 * command name and that command's doc map (name0, docs0, name1, docs1,
 * ...), not an array of per-command sub-arrays like COMMAND INFO. Real
 * Valkey/Redis semantics already drop unknown names from the output
 * entirely rather than padding with nil, so both the no-argument and the
 * with-arguments forms use the same filter-and-shrink strategy: keep only
 * the (name, docs) pairs whose name this proxy supports. sub->rbuf is
 * always RESP2 by now, so for a RESP3 client the kept pairs are re-encoded
 * as a map of name -> doc map (see command_docs_to_resp3()). */
static char *fanout_agg_command_docs(FanoutCtx *ctx, int *out_len) {
    FanoutSub *s = &ctx->subs[0];
    if (s->error || !s->rbuf) {
        const char *e = "-ERR fan-out backend error\r\n";
        char *r = strdup(e);
        if (r) *out_len = (int)strlen(e);
        return r;
    }
    if (s->rbuf[0] != '*') {
        char *r = malloc((size_t)s->rbuf_len);
        if (!r) return NULL;
        memcpy(r, s->rbuf, (size_t)s->rbuf_len);
        *out_len = s->rbuf_len;
        return r;
    }

    int n = atoi(s->rbuf + 1);
    if (n < 0) n = 0;
    if (n > CMD_INFO_MAX_ENTRIES) n = CMD_INFO_MAX_ENTRIES;

    const char *elems[CMD_INFO_MAX_ENTRIES];
    int elem_lens[CMD_INFO_MAX_ENTRIES];
    int kept = 0;
    for (int i = 0; i + 1 < n; i += 2) {
        int nlen;
        const char *name_elem = resp_array_nth(s->rbuf, s->rbuf_len, i, &nlen);
        int dlen;
        const char *docs_elem = resp_array_nth(s->rbuf, s->rbuf_len, i + 1, &dlen);
        if (!name_elem || !docs_elem) break;
        int vlen;
        const char *val = resp_parse_bulk_str(name_elem, nlen, &vlen);
        if (val && command_info_name_supported(val, vlen)) {
            elems[kept] = name_elem;
            elem_lens[kept] = nlen;
            kept++;
            elems[kept] = docs_elem;
            elem_lens[kept] = dlen;
            kept++;
        }
    }
    if (ctx->client->resp != 3) return command_info_build_array(elems, elem_lens, kept, out_len);

    /* The re-encoded reply is never longer than the RESP2 one. */
    int total = 32;
    for (int i = 0; i < kept; i++) total += elem_lens[i];
    char *out = malloc((size_t)total);
    if (!out) return NULL;
    int off = snprintf(out, 32, "%%%d\r\n", kept / 2);
    for (int i = 0; i + 1 < kept; i += 2) {
        memcpy(out + off, elems[i], (size_t)elem_lens[i]);
        off += elem_lens[i];
        off += command_docs_to_resp3(elems[i + 1], elem_lens[i + 1], out + off);
    }
    *out_len = off;
    return out;
}

/* COMMAND LIST [FILTERBY ...]: unlike COMMAND INFO, the backend reply is
 * already a flat array of bulk-string command names (no per-command
 * sub-array), so each element is checked directly instead of drilling into
 * a nested name field. */
static char *fanout_agg_command_list(FanoutCtx *ctx, int *out_len) {
    FanoutSub *s = &ctx->subs[0];
    if (s->error || !s->rbuf) {
        const char *e = "-ERR fan-out backend error\r\n";
        char *r = strdup(e);
        if (r) *out_len = (int)strlen(e);
        return r;
    }
    if (s->rbuf[0] != '*') {
        char *r = malloc((size_t)s->rbuf_len);
        if (!r) return NULL;
        memcpy(r, s->rbuf, (size_t)s->rbuf_len);
        *out_len = s->rbuf_len;
        return r;
    }

    int n = atoi(s->rbuf + 1);
    if (n < 0) n = 0;
    if (n > CMD_INFO_MAX_ENTRIES) n = CMD_INFO_MAX_ENTRIES;

    const char *elems[CMD_INFO_MAX_ENTRIES];
    int elem_lens[CMD_INFO_MAX_ENTRIES];
    int kept = 0;
    for (int i = 0; i < n; i++) {
        int elen;
        const char *elem = resp_array_nth(s->rbuf, s->rbuf_len, i, &elen);
        if (!elem) break;
        int vlen;
        const char *val = resp_parse_bulk_str(elem, elen, &vlen);
        if (val && command_info_name_supported(val, vlen)) {
            elems[kept] = elem;
            elem_lens[kept] = elen;
            kept++;
        }
    }
    return command_info_build_array(elems, elem_lens, kept, out_len);
}

/* COMMAND COUNT: a "COMMAND LIST" is sent to the backend on the client's
 * behalf (see handle_command_cmd), and this counts how many of the returned
 * names this proxy itself supports, replying with that count as an
 * integer rather than relaying the backend's array. */
static char *fanout_agg_command_count(FanoutCtx *ctx, int *out_len) {
    FanoutSub *s = &ctx->subs[0];
    if (s->error || !s->rbuf || s->rbuf[0] != '*') {
        const char *e = "-ERR fan-out backend error\r\n";
        char *r = strdup(e);
        if (r) *out_len = (int)strlen(e);
        return r;
    }

    int n = atoi(s->rbuf + 1);
    if (n < 0) n = 0;
    if (n > CMD_INFO_MAX_ENTRIES) n = CMD_INFO_MAX_ENTRIES;

    int count = 0;
    for (int i = 0; i < n; i++) {
        int elen;
        const char *elem = resp_array_nth(s->rbuf, s->rbuf_len, i, &elen);
        if (!elem) break;
        int vlen;
        const char *val = resp_parse_bulk_str(elem, elen, &vlen);
        if (val && command_info_name_supported(val, vlen)) count++;
    }
    char tmp[32];
    int tl = snprintf(tmp, sizeof(tmp), ":%d\r\n", count);
    char *r = malloc((size_t)tl);
    if (!r) return NULL;
    memcpy(r, tmp, (size_t)tl);
    *out_len = tl;
    return r;
}

void fanout_send_response(Worker *w, ClientConn *client, char *resp, int resp_len) {
    if (client->wbuf_cap < resp_len) {
        char *tmp = realloc(client->wbuf, (size_t)resp_len);
        if (!tmp) {
            free(resp);
            client_send_error(w, client, "out of memory");
            /* Drop the command, or the next read would parse and run it
             * again. */
            client->pbuf_len = 0;
            client->hbuf_len = 0;
            client->using_pbuf = 0;
            client->state = CS_READING_HDR;
            fd_watch(w, client->fd, EL_READABLE);
            return;
        }
        client->wbuf = tmp;
        client->wbuf_cap = resp_len;
    }
    memcpy(client->wbuf, resp, (size_t)resp_len);
    client->wbuf_len = resp_len;
    client->wbuf_off = 0;
    free(resp);
    client->pbuf_len = 0;
    client->hbuf_len = 0;
    client->using_pbuf = 0;
    client->state = CS_WRITING_RESP;
    fd_watch(w, client->fd, EL_WRITABLE);
}

static char *fanout_agg_pubsub_channels(FanoutCtx *ctx, int *out_len);
static char *fanout_agg_pubsub_numsub(FanoutCtx *ctx, int *out_len);

void fanout_aggregate_and_reply(Worker *w, FanoutCtx *ctx) {
    ClientConn *client = ctx->client;
    Req *req = ctx->req;
    char *resp = NULL;
    int resp_len = 0;

    if (req && !client) {
        /* Multiplexed, and the client is gone: nothing to aggregate for. */
        fanout_ctx_free(ctx);
        mux_req_complete(w, req, NULL, 0);
        return;
    }

    if (ctx->had_error && !ctx->nsubs) {
        const char *e = "-ERR fan-out backend error\r\n";
        resp = strdup(e);
        resp_len = (int)strlen(e);
    } else if (!strcmp(ctx->cmd, "MGET") || !strcmp(ctx->cmd, "JSON.MGET")) {
        resp = fanout_agg_mget(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "MSET") || !strcmp(ctx->cmd, "MSETNX") || !strcmp(ctx->cmd, "MSETEX") ||
               !strcmp(ctx->cmd, "JSON.MSET")) {
        resp = fanout_agg_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "LCS")) {
        resp = fanout_agg_lcs(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "KEYS")) {
        resp = fanout_agg_array(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SCAN")) {
        resp = fanout_agg_scan(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FLUSHALL") || !strcmp(ctx->cmd, "FLUSHDB")) {
        resp = fanout_agg_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SCRIPT_KILL")) {
        resp = fanout_agg_any_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SCRIPT_FLUSH")) {
        resp = fanout_agg_always_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SCRIPT_LOAD")) {
        resp = fanout_agg_first_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FUNCTION_LOAD")) {
        resp = fanout_agg_all_ok_bulk(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FUNCTION_DELETE")) {
        resp = fanout_agg_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FUNCTION_FLUSH")) {
        resp = fanout_agg_always_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FUNCTION_KILL")) {
        resp = fanout_agg_any_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "FUNCTION_STATS")) {
        resp = fanout_agg_function_stats(w, ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "LEN")) {
        resp = fanout_agg_sum(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SLOWLOG_GET") || !strcmp(ctx->cmd, "COMMANDLOG_GET")) {
        resp = fanout_agg_slowlog_get(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "RESET")) {
        resp = fanout_agg_ok(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CHANNELS")) {
        resp = fanout_agg_pubsub_channels(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "NUMPAT")) {
        resp = fanout_agg_sum(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "NUMSUB") || !strcmp(ctx->cmd, "SHARDNUMSUB")) {
        resp = fanout_agg_pubsub_numsub(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "SHARDCHANNELS")) {
        resp = fanout_agg_array(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CMDINFO_ALL")) {
        resp = fanout_agg_command_info_all(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CMDINFO_NAMED")) {
        resp = fanout_agg_command_info_named(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CMDDOCS")) {
        resp = fanout_agg_command_docs(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CMDLIST")) {
        resp = fanout_agg_command_list(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "CMDCOUNT")) {
        resp = fanout_agg_command_count(ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "INFO")) {
        resp = info_aggregate(w, ctx, &resp_len);
    } else if (!strcmp(ctx->cmd, "PROXY_INFO")) {
        resp = proxy_info_aggregate(w, ctx, &resp_len);
    } else {
        resp = fanout_agg_sum(ctx, &resp_len);
    }

    if (!resp) resp_len = 0;
    if (!resp) {
        const char *e = "-ERR out of memory\r\n";
        resp = strdup(e);
        resp_len = (int)strlen(e);
    }

    fanout_ctx_free(ctx);
    if (req) {
        req->fanout = NULL;
        mux_req_complete(w, req, resp, resp_len);
        free(resp);
        return;
    }
    client->fanout_ctx = NULL;
    fanout_send_response(w, client, resp, resp_len);
}

static void fanout_dispatch_sub(Worker *w, FanoutCtx *ctx, int sub_idx, const char *cmdbuf, int cmdlen) {
    FanoutSub *sub = &ctx->subs[sub_idx];
    if (ctx->req) {
        /* Multiplexed: the reply comes back through fanout_sub_complete(). */
        if (mux_send_sub(w, sub, ctx->client->db, cmdbuf, cmdlen) < 0) {
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
        }
        return;
    }
    DbGroup *grp = db_group_get(w, ctx->client->db);
    if (!grp) {
        sub->error = 1;
        sub->done = 1;
        ctx->had_error = 1;
        ctx->ndone++;
        return;
    }
    ConnPool *pool = &grp->node_pools[sub->node_idx].pools[POOL_NORMAL];
    PoolConn *conn = pool_acquire_nb(w, pool);
    if (!conn) {
        sub->error = 1;
        sub->done = 1;
        ctx->had_error = 1;
        ctx->ndone++;
        return;
    }
    conn->waiting_client = NULL;
    conn->fanout_sub = sub;
    sub->conn = conn;
    if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;

    if (resp3_conn_needs_upgrade(conn)) {
        /* No client-side hbuf/pbuf to fall back on here (unlike the
         * resume_client_after_conn() hook) -- stash the real command and
         * start the handshake; handle_resp3_handshake_readable() flushes it
         * once the connection has upgraded. conn->fanout_sub is already set
         * above, but handle_backend_readable() checks resp3_handshake first,
         * so the handshake's own reply can't be misrouted to
         * handle_fanout_sub_readable(). */
        if (resp3_defer_append(conn, cmdbuf, cmdlen) < 0 || resp3_start_conn_upgrade(w, conn) < 0) {
            conn->fanout_sub = NULL;
            pool_destroy_conn(w, conn);
            sub->conn = NULL;
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
        }
        return;
    }

    if (conn->wbuf_cap < cmdlen) {
        char *tmp = realloc(conn->wbuf, (size_t)cmdlen);
        if (!tmp) {
            conn->fanout_sub = NULL;
            pool_release(w, conn);
            sub->conn = NULL;
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            return;
        }
        conn->wbuf = tmp;
        conn->wbuf_cap = cmdlen;
    }
    memcpy(conn->wbuf, cmdbuf, (size_t)cmdlen);
    conn->wbuf_len = cmdlen;
    conn->wbuf_off = 0;
    fd_watch(w, conn->fd, EL_WRITABLE);
}

static void fanout_route_single(Worker *w, ClientConn *client, int node_idx, int slot) {
    if (client->mux_cmd) {
        if (mux_send_cmd(w, client, node_idx, slot, client->pbuf, client->pbuf_len) == 0) mux_input_done(w, client);
        return;
    }
    client->using_pbuf = 1;
    client->node_idx = node_idx;
    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        client_send_error(w, client, "db group error");
        close_client(w, client);
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
}

/* Hands a freshly built ctx to client: as its one fan-out in flight, or --
 * multiplexed -- as a request of its own in the client's reply queue. -1 if
 * the client was closed (ctx is freed). */
static int fanout_ctx_attach(Worker *w, ClientConn *client, FanoutCtx *ctx) {
    if (!client->mux_cmd) {
        client->fanout_ctx = ctx;
        return 0;
    }
    Req *req = mux_req_new(client);
    if (!req) {
        fanout_ctx_free(ctx);
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return -1;
    }
    req->fanout = ctx;
    ctx->req = req;
    return 0;
}

/* After a multiplexed fan-out's subs are out: the client goes straight on
 * to its next command (unless the fan-out's own reply closed it). */
static void fanout_mux_dispatched(Worker *w, ClientConn *client, int fd) {
    if (w->client_map[fd] == client && client->mux_cmd) mux_input_done(w, client);
}

void handle_fanout_cmd(Worker *w, ClientConn *client, const char *cmd) {
    int fd = client->fd;
    int is_mset = (!strcmp(cmd, "MSET") || !strcmp(cmd, "MSETNX"));
    int is_msetex = !strcmp(cmd, "MSETEX");
    int is_json_mset = !strcmp(cmd, "JSON.MSET");
    int is_json_mget = !strcmp(cmd, "JSON.MGET");
    int is_lcs = !strcmp(cmd, "LCS");

    const char *ap[MAX_FANOUT_KEYS * 3 + 4];
    int al[MAX_FANOUT_KEYS * 3 + 4];
    int nargs = resp_extract_all_args(client->pbuf, client->pbuf_len, ap, al, MAX_FANOUT_KEYS * 3 + 4);
    if (nargs < 0) {
        client_send_error(w, client, "protocol error");
        FANOUT_RESET_CLIENT(client);
        return;
    }

    int nkeys;
    if (is_mset)
        nkeys = nargs / 2;
    else if (is_msetex || is_json_mset)
        nkeys = nargs / 3;
    else if (is_json_mget)
        nkeys = nargs > 0 ? nargs - 1 : 0;
    else if (is_lcs)
        nkeys = nargs >= 2 ? 2 : nargs;
    else
        nkeys = nargs;
    if (nkeys <= 0) {
        client_send_error(w, client, "missing key");
        FANOUT_RESET_CLIENT(client);
        return;
    }
    if (nkeys > MAX_FANOUT_KEYS) nkeys = MAX_FANOUT_KEYS;

    int key_node[MAX_FANOUT_KEYS];
    for (int i = 0; i < nkeys; i++) {
        int ki = is_mset ? i * 2 : (is_msetex || is_json_mset) ? i * 3 : i;
        int slot = key_slot(ap[ki], al[ki]);
        key_node[i] = w->topo.slot_map[slot];
        if (key_node[i] < 0 || key_node[i] >= w->topo.node_count) {
            client_send_error(w, client, "slot not mapped");
            FANOUT_RESET_CLIENT(client);
            return;
        }
    }

    if (nkeys == 1 && !is_msetex) {
        fanout_route_single(w, client, key_node[0], key_slot(ap[0], al[0]));
        return;
    }

    if (is_lcs) {
        int lcs_slot0 = key_slot(ap[0], al[0]);
        int lcs_slot1 = key_slot(ap[1], al[1]);
        if (lcs_slot0 == lcs_slot1) {
            fanout_route_single(w, client, key_node[0], lcs_slot0);
            return;
        }
        int lcs_mode = 0;
        for (int i = 2; i < nargs; i++) {
            if (al[i] == 3 && (ap[i][0] == 'L' || ap[i][0] == 'l') && (ap[i][1] == 'E' || ap[i][1] == 'e') &&
                (ap[i][2] == 'N' || ap[i][2] == 'n')) {
                lcs_mode = 1;
                break;
            }
            if (al[i] == 3 && (ap[i][0] == 'I' || ap[i][0] == 'i') && (ap[i][1] == 'D' || ap[i][1] == 'd') &&
                (ap[i][2] == 'X' || ap[i][2] == 'x')) {
                lcs_mode = 2;
                break;
            }
        }
        if (lcs_mode == 2) {
            client_send_error(w, client, "LCS IDX is not supported for cross-slot keys");
            FANOUT_RESET_CLIENT(client);
            return;
        }
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", cmd);
        ctx->nsubs = 2;
        ctx->ndone = 0;
        ctx->had_error = 0;
        ctx->lcs_mode = lcs_mode;
        if (fanout_ctx_attach(w, client, ctx) < 0) return;
        for (int i = 0; i < 2; i++) {
            ctx->subs[i].ctx = ctx;
            ctx->subs[i].sub_idx = i;
            ctx->subs[i].node_idx = key_node[i];
            ctx->subs[i].slot = i ? lcs_slot1 : lcs_slot0;
            ctx->subs[i].conn = NULL;
            ctx->subs[i].rbuf = NULL;
            ctx->subs[i].rbuf_len = ctx->subs[i].rbuf_cap = 0;
            ctx->subs[i].done = 0;
            ctx->subs[i].error = 0;
        }
        for (int i = 0; i < 2; i++) {
            int cmdlen;
            char *cmdbuf = build_resp_cmd("GET", 3, &ap[i], &al[i], 1, &cmdlen);
            if (!cmdbuf) {
                ctx->subs[i].error = 1;
                ctx->subs[i].done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            fanout_dispatch_sub(w, ctx, i, cmdbuf, cmdlen);
            free(cmdbuf);
        }
        if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
        fanout_mux_dispatched(w, client, fd);
        return;
    }

    if (is_msetex) {
        if (nkeys > MAX_FANOUT_SUBS) nkeys = MAX_FANOUT_SUBS;
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", cmd);
        ctx->nsubs = nkeys;
        ctx->ndone = 0;
        ctx->had_error = 0;
        if (fanout_ctx_attach(w, client, ctx) < 0) return;
        for (int i = 0; i < nkeys; i++) {
            ctx->subs[i].ctx = ctx;
            ctx->subs[i].sub_idx = i;
            ctx->subs[i].node_idx = key_node[i];
            ctx->subs[i].slot = key_slot(ap[i * 3], al[i * 3]);
            ctx->subs[i].conn = NULL;
            ctx->subs[i].rbuf = NULL;
            ctx->subs[i].rbuf_len = ctx->subs[i].rbuf_cap = 0;
            ctx->subs[i].done = 0;
            ctx->subs[i].error = 0;
        }
        for (int i = 0; i < nkeys; i++) {
            int ki = i * 3;
            const char *sa[3] = {ap[ki], ap[ki + 1], ap[ki + 2]};
            int sl[3] = {al[ki], al[ki + 1], al[ki + 2]};
            int cmdlen;
            char *cmdbuf = build_resp_cmd("SETEX", 5, sa, sl, 3, &cmdlen);
            if (!cmdbuf) {
                ctx->subs[i].error = 1;
                ctx->subs[i].done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            fanout_dispatch_sub(w, ctx, i, cmdbuf, cmdlen);
            free(cmdbuf);
        }
        if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
        fanout_mux_dispatched(w, client, fd);
        return;
    }

    if (is_mset || is_json_mset) {
        int stride = is_mset ? 2 : 3;
        int sub_for_key[MAX_FANOUT_KEYS];
        int sub_slot_val[MAX_FANOUT_SUBS];
        int sub_node_val[MAX_FANOUT_SUBS];
        int nsubs = 0;

        for (int i = 0; i < nkeys; i++) {
            int ki = i * stride;
            int slt = key_slot(ap[ki], al[ki]);
            int found = -1;
            for (int s = 0; s < nsubs; s++)
                if (sub_slot_val[s] == slt) {
                    found = s;
                    break;
                }
            if (found < 0) {
                if (nsubs >= MAX_FANOUT_SUBS) {
                    client_send_error(w, client, "too many slot groups");
                    FANOUT_RESET_CLIENT(client);
                    return;
                }
                sub_slot_val[nsubs] = slt;
                sub_node_val[nsubs] = key_node[i];
                found = nsubs++;
            }
            sub_for_key[i] = found;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", cmd);
        ctx->nsubs = nsubs;
        ctx->ndone = 0;
        ctx->had_error = 0;
        if (fanout_ctx_attach(w, client, ctx) < 0) return;

        for (int s = 0; s < nsubs; s++) {
            ctx->subs[s].ctx = ctx;
            ctx->subs[s].sub_idx = s;
            ctx->subs[s].node_idx = sub_node_val[s];
            ctx->subs[s].slot = sub_slot_val[s];
            ctx->subs[s].conn = NULL;
            ctx->subs[s].rbuf = NULL;
            ctx->subs[s].rbuf_len = ctx->subs[s].rbuf_cap = 0;
            ctx->subs[s].done = 0;
            ctx->subs[s].error = 0;
        }

        for (int s = 0; s < nsubs; s++) {
            const char *sa[MAX_FANOUT_KEYS * 3];
            int sl[MAX_FANOUT_KEYS * 3];
            int sn = 0;
            for (int i = 0; i < nkeys; i++) {
                if (sub_for_key[i] != s) continue;
                int ki = i * stride;
                for (int j = 0; j < stride; j++) {
                    sa[sn] = ap[ki + j];
                    sl[sn] = al[ki + j];
                    sn++;
                }
            }
            int cmdlen;
            char *cmdbuf = build_resp_cmd(cmd, (int)strlen(cmd), sa, sl, sn, &cmdlen);
            if (!cmdbuf) {
                ctx->subs[s].error = 1;
                ctx->subs[s].done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            fanout_dispatch_sub(w, ctx, s, cmdbuf, cmdlen);
            free(cmdbuf);
        }

        if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
        fanout_mux_dispatched(w, client, fd);
        return;
    }

    int sub_for_key[MAX_FANOUT_KEYS];
    int sub_slot_val[MAX_FANOUT_SUBS];
    int sub_node_val[MAX_FANOUT_SUBS];
    int nsubs = 0;

    for (int i = 0; i < nkeys; i++) {
        int slt = key_slot(ap[i], al[i]);
        int found = -1;
        for (int s = 0; s < nsubs; s++)
            if (sub_slot_val[s] == slt) {
                found = s;
                break;
            }
        if (found < 0) {
            if (nsubs >= MAX_FANOUT_SUBS) {
                client_send_error(w, client, "too many slot groups");
                FANOUT_RESET_CLIENT(client);
                return;
            }
            sub_slot_val[nsubs] = slt;
            sub_node_val[nsubs] = key_node[i];
            found = nsubs++;
        }
        sub_for_key[i] = found;
    }

    FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
    if (!ctx) {
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return;
    }
    ctx->client = client;
    snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", cmd);
    ctx->nsubs = nsubs;
    ctx->ndone = 0;
    ctx->had_error = 0;
    ctx->total_keys = nkeys;
    if (fanout_ctx_attach(w, client, ctx) < 0) return;

    {
        int pos_counter[MAX_FANOUT_SUBS];
        memset(pos_counter, 0, sizeof(pos_counter));
        for (int i = 0; i < nkeys; i++) {
            int si = sub_for_key[i];
            ctx->mget_sub[i] = si;
            ctx->mget_pos[i] = pos_counter[si]++;
        }
    }

    for (int s = 0; s < nsubs; s++) {
        ctx->subs[s].ctx = ctx;
        ctx->subs[s].sub_idx = s;
        ctx->subs[s].node_idx = sub_node_val[s];
        ctx->subs[s].slot = sub_slot_val[s];
        ctx->subs[s].conn = NULL;
        ctx->subs[s].rbuf = NULL;
        ctx->subs[s].rbuf_len = ctx->subs[s].rbuf_cap = 0;
        ctx->subs[s].done = 0;
        ctx->subs[s].error = 0;
    }

    for (int s = 0; s < nsubs; s++) {
        const char *sub_args[MAX_FANOUT_KEYS];
        int sub_arglens[MAX_FANOUT_KEYS];
        int sub_nargs = 0;
        for (int i = 0; i < nkeys; i++) {
            if (sub_for_key[i] != s) continue;
            sub_args[sub_nargs] = ap[i];
            sub_arglens[sub_nargs] = al[i];
            sub_nargs++;
        }
        if (is_json_mget && sub_nargs > 0) {
            sub_args[sub_nargs] = ap[nargs - 1];
            sub_arglens[sub_nargs] = al[nargs - 1];
            sub_nargs++;
        }
        int cmdlen;
        char *cmdbuf = build_resp_cmd(cmd, (int)strlen(cmd), sub_args, sub_arglens, sub_nargs, &cmdlen);
        if (!cmdbuf) {
            ctx->subs[s].error = 1;
            ctx->subs[s].done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            continue;
        }
        fanout_dispatch_sub(w, ctx, s, cmdbuf, cmdlen);
        free(cmdbuf);
    }

    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
    fanout_mux_dispatched(w, client, fd);
}

/* ── PUBSUB inspect aggregation helpers ─────────────────────────────── */

/* Merge channel arrays from all nodes, deduplicating by exact string match.
 * Used for PUBSUB CHANNELS where the same channel can appear on multiple nodes. */
static char *fanout_agg_pubsub_channels(FanoutCtx *ctx, int *out_len) {
    /* Collect all (pointer, length) pairs from sub-responses */
    const char *ptrs[4096];
    int lens[4096];
    int ntotal = 0;
    for (int i = 0; i < ctx->nsubs; i++) {
        FanoutSub *s = &ctx->subs[i];
        if (s->error || !s->rbuf || s->rbuf[0] != '*') continue;
        const char *nl = memchr(s->rbuf + 1, '\n', s->rbuf_len - 1);
        if (!nl) continue;
        int cnt = atoi(s->rbuf + 1);
        int off = (int)(nl - s->rbuf) + 1;
        for (int j = 0; j < cnt && ntotal < 4096; j++) {
            int el = resp_response_len(s->rbuf + off, s->rbuf_len - off);
            if (el <= 0) break;
            int vl = 0;
            const char *v = resp_parse_bulk_str(s->rbuf + off, el, &vl);
            if (v) {
                ptrs[ntotal] = v;
                lens[ntotal] = vl;
                ntotal++;
            }
            off += el;
        }
    }

    /* Deduplicate O(n²): mark duplicates */
    int keep[4096];
    int nkeep = 0, data_bytes = 0;
    for (int i = 0; i < ntotal; i++) {
        int dup = 0;
        for (int j = 0; j < i; j++) {
            if (keep[j] && lens[i] == lens[j] && memcmp(ptrs[i], ptrs[j], (size_t)lens[i]) == 0) {
                dup = 1;
                break;
            }
        }
        keep[i] = !dup;
        if (!dup) {
            nkeep++;
            data_bytes += 1 + 20 + 2 + lens[i] + 2;
        } /* $len\r\ndata\r\n */
    }

    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", nkeep);
    char *out = malloc((size_t)(hdr_len + data_bytes + 1));
    if (!out) return NULL;
    int off = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    off += hdr_len;
    for (int i = 0; i < ntotal; i++) {
        if (!keep[i]) continue;
        char elem_hdr[32];
        int ehlen = snprintf(elem_hdr, sizeof(elem_hdr), "$%d\r\n", lens[i]);
        memcpy(out + off, elem_hdr, (size_t)ehlen);
        off += ehlen;
        memcpy(out + off, ptrs[i], (size_t)lens[i]);
        off += lens[i];
        out[off++] = '\r';
        out[off++] = '\n';
    }
    *out_len = off;
    return out;
}

/* Look up a channel name in a PUBSUB NUMSUB node response (alternating str/int pairs).
 * Returns the subscriber count for the channel, or 0 if not found. */
static long long numsub_lookup(const char *resp, int rlen, const char *ch, int chlen) {
    if (!resp || rlen < 4 || resp[0] != '*') return 0;
    const char *nl = memchr(resp + 1, '\n', rlen - 1);
    if (!nl) return 0;
    int cnt = atoi(resp + 1);
    int off = (int)(nl - resp) + 1;
    for (int i = 0; i + 1 < cnt; i += 2) {
        int nel = resp_response_len(resp + off, rlen - off);
        if (nel <= 0) break;
        int vl = 0;
        const char *v = resp_parse_bulk_str(resp + off, nel, &vl);
        off += nel;
        int cel = resp_response_len(resp + off, rlen - off);
        if (cel <= 0) break;
        long long count = 0;
        if (resp[off] == ':') count = atoll(resp + off + 1);
        off += cel;
        if (v && vl == chlen && memcmp(v, ch, (size_t)chlen) == 0) return count;
    }
    return 0;
}

/* Aggregate PUBSUB NUMSUB / SHARDNUMSUB responses across all nodes.
 * Reads the original requested channel names from ctx->client->hbuf (args[2+]).
 * Sums per-channel counts across all nodes. */
static char *fanout_agg_pubsub_numsub(FanoutCtx *ctx, int *out_len) {
    ClientConn *client = ctx->client;
    const char *ch_ptrs[256];
    int ch_lens[256];
    int nchans = 0;
    for (int i = 2; nchans < 256; i++) {
        const char *ch;
        int chl;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, i, &ch, &chl)) break;
        ch_ptrs[nchans] = ch;
        ch_lens[nchans] = chl;
        nchans++;
    }
    if (nchans == 0) {
        char *r = strdup("*0\r\n");
        if (r) *out_len = 4;
        return r;
    }

    /* Calculate output size */
    int data_bytes = 0;
    for (int ci = 0; ci < nchans; ci++)
        data_bytes += 1 + 20 + 2 + ch_lens[ci] + 2 + 1 + 20 + 2; /* $len\r\nch\r\n:count\r\n */

    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "*%d\r\n", nchans * 2);
    char *out = malloc((size_t)(hdr_len + data_bytes + 1));
    if (!out) return NULL;
    int off = 0;
    memcpy(out, hdr, (size_t)hdr_len);
    off += hdr_len;

    for (int ci = 0; ci < nchans; ci++) {
        long long total = 0;
        for (int si = 0; si < ctx->nsubs; si++) {
            FanoutSub *s = &ctx->subs[si];
            if (!s->error && s->rbuf) total += numsub_lookup(s->rbuf, s->rbuf_len, ch_ptrs[ci], ch_lens[ci]);
        }
        char elem_hdr[32];
        int ehlen = snprintf(elem_hdr, sizeof(elem_hdr), "$%d\r\n", ch_lens[ci]);
        memcpy(out + off, elem_hdr, (size_t)ehlen);
        off += ehlen;
        memcpy(out + off, ch_ptrs[ci], (size_t)ch_lens[ci]);
        off += ch_lens[ci];
        out[off++] = '\r';
        out[off++] = '\n';
        int clen = snprintf(elem_hdr, sizeof(elem_hdr), ":%lld\r\n", total);
        memcpy(out + off, elem_hdr, (size_t)clen);
        off += clen;
    }
    *out_len = off;
    return out;
}

/* Fan-out a pre-built RESP command to a single arbitrary master node (same
 * "any one master" convention used by COMMAND HELP / MODULE LIST / etc.),
 * but through the FanoutCtx/FanoutSub pipeline so the reply lands in
 * ctx->subs[0] and gets run through fanout_aggregate_and_reply() for
 * custom post-processing instead of being relayed to the client raw.
 * ctx->cmd must be set by the caller before calling this. */
static void fanout_one_master(Worker *w, ClientConn *client, FanoutCtx *ctx, const char *cmd_buf, int cmd_len) {
    int ni = topo_first_master(w);

    ctx->nsubs = 1;
    ctx->ndone = 0;
    ctx->had_error = 0;

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        fanout_ctx_free(ctx);
        client->fanout_ctx = NULL;
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }

    FanoutSub *sub = &ctx->subs[0];
    sub->ctx = ctx;
    sub->sub_idx = 0;
    sub->node_idx = ni;
    sub->rbuf = NULL;
    sub->rbuf_len = sub->rbuf_cap = 0;
    sub->done = 0;
    sub->error = 0;
    sub->conn = NULL;

    ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
    PoolConn *conn = pool_acquire_nb(w, pool);
    if (!conn) {
        sub->error = 1;
        sub->done = 1;
        ctx->had_error = 1;
        ctx->ndone++;
    } else {
        int ok = 1;
        if (conn->wbuf_cap < cmd_len) {
            char *tmp = realloc(conn->wbuf, (size_t)cmd_len);
            if (!tmp) {
                pool_release(w, conn);
                sub->error = 1;
                sub->done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                ok = 0;
            } else {
                conn->wbuf = tmp;
                conn->wbuf_cap = cmd_len;
            }
        }
        if (ok) {
            memcpy(conn->wbuf, cmd_buf, (size_t)cmd_len);
            conn->wbuf_len = cmd_len;
            conn->wbuf_off = 0;
            conn->waiting_client = NULL;
            conn->fanout_sub = sub;
            sub->conn = conn;
            if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
            fd_watch(w, conn->fd, EL_WRITABLE);
        }
    }

    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
}

/* Fan-out a pre-built RESP command to all master nodes.
 * cmd_buf/cmd_len is the RESP-encoded command to send to each node.
 * ctx->cmd must be set by the caller before calling this. */
static void fanout_all_masters(Worker *w, ClientConn *client, FanoutCtx *ctx, const char *cmd_buf, int cmd_len) {
    int target_nodes[MAX_NODES];
    int ntargets = topo_all_masters(w, target_nodes);
    fanout_to_nodes(w, client, ctx, cmd_buf, cmd_len, target_nodes, ntargets);
}

void fanout_to_nodes(Worker *w, ClientConn *client, FanoutCtx *ctx, const char *cmd_buf, int cmd_len,
                     const int *target_nodes, int ntargets) {
    ctx->nsubs = ntargets;
    ctx->ndone = 0;
    ctx->had_error = 0;

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        fanout_ctx_free(ctx);
        client->fanout_ctx = NULL;
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }

    for (int ti = 0; ti < ntargets; ti++) {
        int ni = target_nodes[ti];
        FanoutSub *sub = &ctx->subs[ti];
        sub->ctx = ctx;
        sub->sub_idx = ti;
        sub->node_idx = ni;
        sub->rbuf = NULL;
        sub->rbuf_len = sub->rbuf_cap = 0;
        sub->done = 0;
        sub->error = 0;
        sub->conn = NULL;

        ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
        PoolConn *conn = pool_acquire_nb(w, pool);
        if (!conn) {
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            continue;
        }

        if (conn->wbuf_cap < cmd_len) {
            char *tmp = realloc(conn->wbuf, (size_t)cmd_len);
            if (!tmp) {
                pool_release(w, conn);
                sub->error = 1;
                sub->done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            conn->wbuf = tmp;
            conn->wbuf_cap = cmd_len;
        }
        memcpy(conn->wbuf, cmd_buf, (size_t)cmd_len);
        conn->wbuf_len = cmd_len;
        conn->wbuf_off = 0;
        conn->waiting_client = NULL;
        conn->fanout_sub = sub;
        sub->conn = conn;
        if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
        fd_watch(w, conn->fd, EL_WRITABLE);
    }

    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
}

void handle_pubsub_inspect_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing PUBSUB subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* PUBSUB HELP — route to one master, no fan-out */
    if (!strcmp(subcmd, "HELP")) {
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
        return;
    }

    /* PUBSUB CHANNELS [pattern] — fan-out to all masters, dedup merge */
    if (!strcmp(subcmd, "CHANNELS") || !strcmp(subcmd, "SHARDCHANNELS")) {
        const char *pattern = NULL;
        int patlen = 0;
        resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &pattern, &patlen);

        const char *args[2];
        int arglens[2];
        int nargs = 1;
        args[0] = subcmd;
        arglens[0] = (int)strlen(subcmd);
        if (pattern && patlen > 0) {
            args[1] = pattern;
            arglens[1] = patlen;
            nargs = 2;
        }

        int built_len;
        char *built = build_resp_cmd("PUBSUB", 6, args, arglens, nargs, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", subcmd);
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* PUBSUB NUMPAT — fan-out to all masters, sum results */
    if (!strcmp(subcmd, "NUMPAT")) {
        const char *args[1] = {"NUMPAT"};
        int arglens[1] = {6};
        int built_len;
        char *built = build_resp_cmd("PUBSUB", 6, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "NUMPAT");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* PUBSUB NUMSUB / SHARDNUMSUB [channel...] */
    if (!strcmp(subcmd, "NUMSUB") || !strcmp(subcmd, "SHARDNUMSUB")) {
        /* Parse channel args */
        const char *ch_ptrs[256];
        int ch_lens[256];
        int nchans = 0;
        for (int i = 2; nchans < 256; i++) {
            const char *ch;
            int chl;
            if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, i, &ch, &chl)) break;
            ch_ptrs[nchans] = ch;
            ch_lens[nchans] = chl;
            nchans++;
        }

        /* No channels → return empty array immediately */
        if (nchans == 0) {
            const char *empty = "*0\r\n";
            if (client->wbuf_cap < 4) {
                char *tmp = realloc(client->wbuf, 8);
                if (!tmp) {
                    client_send_error(w, client, "oom");
                    client->hbuf_len = 0;
                    return;
                }
                client->wbuf = tmp;
                client->wbuf_cap = 8;
            }
            memcpy(client->wbuf, empty, 4);
            client->wbuf_len = 4;
            client->wbuf_off = 0;
            client->hbuf_len = 0;
            client->state = CS_WRITING_RESP;
            fd_watch(w, client->fd, EL_WRITABLE);
            return;
        }

        /* Build RESP command with channels */
        const char *args[257];
        int arglens[257];
        args[0] = subcmd;
        arglens[0] = (int)strlen(subcmd);
        for (int i = 0; i < nchans; i++) {
            args[i + 1] = ch_ptrs[i];
            arglens[i + 1] = ch_lens[i];
        }
        int built_len;
        char *built = build_resp_cmd("PUBSUB", 6, args, arglens, nchans + 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", subcmd);
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    client_send_error(w, client, "unknown PUBSUB subcommand");
    client->hbuf_len = 0;
}

void handle_slowlog_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing SLOWLOG subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* SLOWLOG HELP — route to a single master node, no fan-out */
    if (!strcmp(subcmd, "HELP")) {
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
        return;
    }

    /* SLOWLOG LEN — fan-out to all master nodes, sum the per-node counts */
    if (!strcmp(subcmd, "LEN")) {
        const char *args[1] = {"LEN"};
        int arglens[1] = {3};
        int built_len;
        char *built = build_resp_cmd("SLOWLOG", 7, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "LEN");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* SLOWLOG GET [count] — fan-out to all master nodes, merge newest first.
     * The original request is forwarded verbatim so each node applies the
     * same count (default 10, -1 = all) and validates the argument itself;
     * an invalid count comes back as a backend error that is relayed as-is. */
    if (!strcmp(subcmd, "GET")) {
        long long count = 10;
        const char *carg;
        int carglen;
        if (resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &carg, &carglen)) {
            char tmp[32];
            int tl = carglen < 31 ? carglen : 31;
            memcpy(tmp, carg, (size_t)tl);
            tmp[tl] = '\0';
            char *end;
            count = strtoll(tmp, &end, 10);
            if (end == tmp || *end || count < -1) count = -1;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        ctx->slowlog_count = count;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "SLOWLOG_GET");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* SLOWLOG RESET — fan-out to all master nodes, reply +OK if all succeed */
    if (!strcmp(subcmd, "RESET")) {
        const char *args[1] = {"RESET"};
        int arglens[1] = {5};
        int built_len;
        char *built = build_resp_cmd("SLOWLOG", 7, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "RESET");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    client_send_error(w, client, "unknown SLOWLOG subcommand");
    client->hbuf_len = 0;
}

void handle_commandlog_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing COMMANDLOG subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* COMMANDLOG HELP — route to a single master node, no fan-out */
    if (!strcmp(subcmd, "HELP")) {
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
        return;
    }

    /* COMMANDLOG LEN <slow|large-request|large-reply> — fan-out to all master
     * nodes, sum the per-node counts. The type argument is forwarded verbatim
     * from the client's original request; the backend validates it. */
    if (!strcmp(subcmd, "LEN")) {
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "LEN");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* COMMANDLOG GET <count> <slow|large-request|large-reply> — fan-out to all
     * master nodes, merge newest first. Entries have the same shape as
     * SLOWLOG GET's, so the same aggregator is reused. The request is
     * forwarded verbatim so each node applies the same count (-1 = all) and
     * validates count/type itself; errors are relayed as-is. */
    if (!strcmp(subcmd, "GET")) {
        long long count = -1;
        const char *carg;
        int carglen;
        if (resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &carg, &carglen)) {
            char tmp[32];
            int tl = carglen < 31 ? carglen : 31;
            memcpy(tmp, carg, (size_t)tl);
            tmp[tl] = '\0';
            char *end;
            count = strtoll(tmp, &end, 10);
            if (end == tmp || *end || count < -1) count = -1;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        ctx->slowlog_count = count;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "COMMANDLOG_GET");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* COMMANDLOG RESET <slow|large-request|large-reply> — fan-out to all
     * master nodes, reply +OK if all succeed. */
    if (!strcmp(subcmd, "RESET")) {
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "RESET");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    client_send_error(w, client, "unknown COMMANDLOG subcommand");
    client->hbuf_len = 0;
}

void handle_module_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing MODULE subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* MODULE LIST — forward to any one master node, relay the reply as-is */
    if (!strcmp(subcmd, "LIST")) {
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
        return;
    }

    client_send_error(w, client, "unknown MODULE subcommand");
    client->hbuf_len = 0;
}

void send_multibulk_reply(Worker *w, ClientConn *client, const char **lines, int n) {
    int total = 32;
    for (int i = 0; i < n; i++) total += 32 + (int)strlen(lines[i]);
    if (client->wbuf_cap < total) {
        char *tmp = realloc(client->wbuf, (size_t)total);
        if (!tmp) {
            client_send_error(w, client, "oom");
            client->hbuf_len = 0;
            return;
        }
        client->wbuf = tmp;
        client->wbuf_cap = total;
    }
    int off = snprintf(client->wbuf, (size_t)client->wbuf_cap, "*%d\r\n", n);
    for (int i = 0; i < n; i++) {
        off += snprintf(client->wbuf + off, (size_t)(client->wbuf_cap - off), "$%d\r\n%s\r\n", (int)strlen(lines[i]),
                        lines[i]);
    }
    client->wbuf_len = off;
    client->wbuf_off = 0;
    client->hbuf_len = 0;
    client->state = CS_WRITING_RESP;
    fd_watch(w, client->fd, EL_WRITABLE);
}

/* Formats the CLIENT INFO line for `client` into `out` (capacity `cap`),
 * returning its length. Kept as a standalone helper so CLIENT LIST could
 * reuse it per-connection in the future. */
static int format_client_info_line(Worker *w, ClientConn *client, char *out, int cap) {
    time_t now = time(NULL);
    char events[3];
    int ei = 0;
    if (w->fd_mask[client->fd] & EL_READABLE) events[ei++] = 'r';
    if (w->fd_mask[client->fd] & EL_WRITABLE) events[ei++] = 'w';
    events[ei] = '\0';
    const char *user = client->acl_authenticated ? client->acl_user : "default";

    return snprintf(out, (size_t)cap,
                    "id=%lld addr=%s laddr=%s fd=%d name=%s age=%lld idle=%lld flags=N db=%d "
                    "sub=%d psub=%d ssub=%d multi=%d watch=%d events=%s cmd=%s user=%s resp=%d",
                    client->id, client->addr, client->laddr, client->fd, client->name,
                    (long long)(now - client->connect_time), (long long)(now - client->last_interaction), client->db,
                    client->sub_count, client->psub_count, client->ssub_count, client->multi_count, client->watch_count,
                    events, client->last_cmd, user, client->resp);
}

/* Validates a client name (CLIENT SETNAME / HELLO ... SETNAME) without
 * storing it. Like real Valkey, only printable non-space ASCII ('!'..'~')
 * is allowed. On an invalid name, sends the error, clears hbuf and returns
 * -1; returns 0 if the name is acceptable. */
int client_name_check(Worker *w, ClientConn *client, const char *name, int namelen) {
    if (namelen >= (int)sizeof(client->name)) {
        client_send_error(w, client, "Client names cannot exceed 63 characters");
        client->hbuf_len = 0;
        return -1;
    }
    for (int i = 0; i < namelen; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c < '!' || c > '~') {
            client_send_error(w, client, "Client names cannot contain spaces, newlines or special characters.");
            client->hbuf_len = 0;
            return -1;
        }
    }
    return 0;
}

/* Validates and stores a client name. An empty name clears it. Same error
 * contract as client_name_check(). */
int client_set_name(Worker *w, ClientConn *client, const char *name, int namelen) {
    if (client_name_check(w, client, name, namelen) < 0) return -1;
    memcpy(client->name, name, (size_t)namelen);
    client->name[namelen] = '\0';
    client->namelen = namelen;
    return 0;
}

void handle_client_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing CLIENT subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* CLIENT HELP — answered directly by the proxy, no backend round-trip */
    if (!strcmp(subcmd, "HELP")) {
        static const char *lines[] = {
            "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "GETNAME",
            "    Return the name of the current connection.",
            "ID",
            "    Return the ID of the current connection.",
            "INFO",
            "    Return information about the current client connection.",
            "KILL <ip:port>",
            "    Kill connection made from <ip:port>.",
            "KILL <option> <value> [<option> <value> [...]]",
            "    Kill connections. Options are:",
            "    * ADDR (<ip:port>|<unixsocket>:0)",
            "      Kill connections made from the specified address.",
            "    * LADDR (<ip:port>|<unixsocket>:0)",
            "      Kill connections made to the specified local address.",
            "    * TYPE (NORMAL|PRIMARY|REPLICA|PUBSUB)",
            "      Kill connections by type.",
            "    * USER <username>",
            "      Kill connections authenticated by <username>.",
            "    * SKIPME (YES|NO)",
            "      Skip killing current connection (default: yes).",
            "    * ID <client-id> [<client-id>...]",
            "      Kill connections by client IDs.",
            "    * MAXAGE <maxage>",
            "      Kill connections older than the specified age.",
            "    * FLAGS <flags>",
            "      Kill connections that include the specified flags.",
            "    * NAME <client-name>",
            "      Kill connections with the specified name.",
            "    * IDLE <idle>",
            "      Kill connections with idle time greater than or equal to <idle> seconds.",
            "    * IP <ip>",
            "      Kill connections made from the specified ip.",
            "LIST [options ...]",
            "    Return information about client connections. Options:",
            "    * TYPE (NORMAL|PRIMARY|REPLICA|PUBSUB)",
            "      Return clients of specified type.",
            "    * USER <username>",
            "      Return clients authenticated by <username>.",
            "    * ADDR <ip:port>",
            "      Return clients connected from the specified address.",
            "    * LADDR <ip:port>",
            "      Return clients connected to the specified local address.",
            "    * ID <client-id> [<client-id>...]",
            "      Return clients with the specified IDs.",
            "    * SKIPME (YES|NO)",
            "      Exclude the current client from the list (default: no).",
            "    * MAXAGE <maxage>",
            "      List connections older than the specified age.",
            "    * FLAGS <flags>",
            "      Return clients with the specified flags.",
            "    * NAME <client-name>",
            "      Return clients with the specified name.",
            "    * IDLE <idle>",
            "      Return clients with idle time greater than or equal to <idle> seconds.",
            "    * DB <db-id>",
            "      Return clients currently operating on the specified database ID.",
            "    * IP <ip>",
            "      Return connections made from the specified ip.",
            "SETNAME <name>",
            "    Assign the name <name> to the current connection.",
            "HELP",
            "    Print this help.",
        };
        send_multibulk_reply(w, client, lines, (int)(sizeof(lines) / sizeof(lines[0])));
        return;
    }

    /* CLIENT ID — the id assigned at accept() time (do_accept() in
     * valkey-proxy.c), answered locally, no backend round-trip. */
    if (!strcmp(subcmd, "ID")) {
        char idbuf[32];
        int idlen = snprintf(idbuf, sizeof(idbuf), ":%lld\r\n", client->id);
        if (client->wbuf_cap < idlen) {
            char *tmp = realloc(client->wbuf, (size_t)idlen);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = idlen;
        }
        memcpy(client->wbuf, idbuf, (size_t)idlen);
        client->wbuf_len = idlen;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }

    /* CLIENT GETNAME — returns client->name (empty bulk string, not nil,
     * when no name has been set), answered locally. */
    if (!strcmp(subcmd, "GETNAME")) {
        int total = client->namelen + 16;
        if (client->wbuf_cap < total) {
            char *tmp = realloc(client->wbuf, (size_t)total);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = total;
        }
        int off = snprintf(client->wbuf, (size_t)client->wbuf_cap, "$%d\r\n", client->namelen);
        memcpy(client->wbuf + off, client->name, (size_t)client->namelen);
        off += client->namelen;
        client->wbuf[off++] = '\r';
        client->wbuf[off++] = '\n';
        client->wbuf_len = off;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }

    /* CLIENT INFO — a single-line summary of this connection's own state,
     * answered locally like the other CLIENT subcommands. */
    if (!strcmp(subcmd, "INFO")) {
        char line[1024];
        int len = format_client_info_line(w, client, line, sizeof(line) - 1);
        if (len < 0) len = 0;
        if (len > (int)sizeof(line) - 2) len = (int)sizeof(line) - 2; /* snprintf truncated */
        /* Like Valkey, the bulk payload ends with '\n' so redis-cli breaks
         * the line before printing the next prompt. */
        line[len++] = '\n';
        int total = len + 16;
        if (client->wbuf_cap < total) {
            char *tmp = realloc(client->wbuf, (size_t)total);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = total;
        }
        int off = snprintf(client->wbuf, (size_t)client->wbuf_cap, "$%d\r\n", len);
        memcpy(client->wbuf + off, line, (size_t)len);
        off += len;
        client->wbuf[off++] = '\r';
        client->wbuf[off++] = '\n';
        client->wbuf_len = off;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }

    /* CLIENT SETNAME <name> — stored on this ClientConn only, never sent to
     * a backend; names may not contain spaces or newlines. */
    if (!strcmp(subcmd, "SETNAME")) {
        const char *name;
        int namelen;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &name, &namelen)) {
            client_send_error(w, client, "wrong number of arguments for 'client|setname' command");
            client->hbuf_len = 0;
            return;
        }
        if (client_set_name(w, client, name, namelen) < 0) return;

        const char *ok = "+OK\r\n";
        if (client->wbuf_cap < 5) {
            char *tmp = realloc(client->wbuf, 8);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = 8;
        }
        memcpy(client->wbuf, ok, 5);
        client->wbuf_len = 5;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }

    client_send_error(w, client, "unknown CLIENT subcommand or wrong number of arguments");
    client->hbuf_len = 0;
}

void handle_script_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing SCRIPT subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* SCRIPT HELP — answered directly by the proxy, no backend round-trip */
    if (!strcmp(subcmd, "HELP")) {
        static const char *lines[] = {
            "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "EXISTS <sha1> [<sha1> ...]",
            "    Return information about the existence of the scripts in the script cache.",
            "FLUSH [ASYNC|SYNC]",
            "    Flush the Lua scripts cache. Very dangerous on replicas.",
            "    When called without the optional mode argument, the behavior is determined",
            "     by the lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the scripts cache.",
            "    * SYNC: Synchronously flush the scripts cache.",
            "KILL",
            "    Kill the currently executing Lua script.",
            "LOAD <script>",
            "    Load a script into the scripts cache without executing it.",
            "SHOW <sha1>",
            "    Show a script from the scripts cache.",
            "HELP",
            "    Print this help.",
        };
        send_multibulk_reply(w, client, lines, (int)(sizeof(lines) / sizeof(lines[0])));
        return;
    }

    /* SCRIPT EXISTS / SCRIPT SHOW — forwarded verbatim to a single master
     * node; the backend's raw response is relayed straight back to the
     * client, same convention as OBJECT HELP / PUBSUB HELP. */
    if (!strcmp(subcmd, "EXISTS") || !strcmp(subcmd, "SHOW")) {
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
        return;
    }

    /* SCRIPT KILL — fan-out to all master nodes; the client sees success if
     * ANY node reports success, and failure only if ALL nodes fail. */
    if (!strcmp(subcmd, "KILL")) {
        const char *args[1] = {"KILL"};
        int arglens[1] = {4};
        int built_len;
        char *built = build_resp_cmd("SCRIPT", 6, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "SCRIPT_KILL");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* SCRIPT FLUSH [ASYNC|SYNC] — regardless of the client's original
     * argument, always forwarded as SCRIPT FLUSH ASYNC to every master
     * node, and the client always sees success. */
    if (!strcmp(subcmd, "FLUSH")) {
        const char *args[2] = {"FLUSH", "ASYNC"};
        int arglens[2] = {5, 5};
        int built_len;
        char *built = build_resp_cmd("SCRIPT", 6, args, arglens, 2, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "SCRIPT_FLUSH");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* SCRIPT LOAD <script> — fan-out to all master nodes so any subsequent
     * EVALSHA on any shard can find it; every node returns the same sha1,
     * so only one value is relayed back to the client. */
    if (!strcmp(subcmd, "LOAD")) {
        const char *script;
        int scriptlen;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &script, &scriptlen)) {
            client_send_error(w, client, "wrong number of arguments for 'script|load' command");
            client->hbuf_len = 0;
            return;
        }
        const char *args[2] = {"LOAD", script};
        int arglens[2] = {4, scriptlen};
        int built_len;
        char *built = build_resp_cmd("SCRIPT", 6, args, arglens, 2, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "SCRIPT_LOAD");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    client_send_error(w, client, "unknown SCRIPT subcommand");
    client->hbuf_len = 0;
}

void handle_function_cmd(Worker *w, ClientConn *client, int cmdlen) {
    (void)cmdlen;
    const char *sub;
    int sublen;
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        client_send_error(w, client, "missing FUNCTION subcommand");
        client->hbuf_len = 0;
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* FUNCTION HELP — answered directly by the proxy, no backend round-trip */
    if (!strcmp(subcmd, "HELP")) {
        static const char *lines[] = {
            "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "LOAD [REPLACE] <FUNCTION CODE>",
            "    Create a new library with the given library name and code.",
            "DELETE <LIBRARY NAME>",
            "    Delete the given library.",
            "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
            "    Return general information on all the libraries:",
            "    * Library name",
            "    * The engine used to run the Library",
            "    * Functions list",
            "    * Library code (if WITHCODE is given)",
            "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
            "STATS",
            "    Return information about the current function running:",
            "    * Function name",
            "    * Command used to run the function",
            "    * Duration in MS that the function is running",
            "    If no function is running, return nil",
            "    In addition, returns a list of available engines.",
            "KILL",
            "    Kill the current running function.",
            "FLUSH [ASYNC|SYNC]",
            "    Delete all the libraries.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the libraries.",
            "    * SYNC: Synchronously flush the libraries.",
            "HELP",
            "    Print this help.",
        };
        send_multibulk_reply(w, client, lines, (int)(sizeof(lines) / sizeof(lines[0])));
        return;
    }

    /* FUNCTION LIST — forwarded verbatim to a single master node; the
     * backend's raw response is relayed straight back to the client, same
     * convention as SCRIPT EXISTS / SCRIPT SHOW / MODULE LIST. */
    if (!strcmp(subcmd, "LIST")) {
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
        return;
    }

    /* FUNCTION KILL — fan-out to all master nodes; success if ANY node
     * reports success (only one shard can have a script actually running
     * for a given client at a time), failure only if ALL nodes fail. */
    if (!strcmp(subcmd, "KILL")) {
        const char *args[1] = {"KILL"};
        int arglens[1] = {4};
        int built_len;
        char *built = build_resp_cmd("FUNCTION", 8, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "FUNCTION_KILL");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* FUNCTION FLUSH [ASYNC|SYNC] — regardless of the client's original
     * argument, always forwarded as FUNCTION FLUSH ASYNC to every master
     * node, and the client always sees success. */
    if (!strcmp(subcmd, "FLUSH")) {
        const char *args[2] = {"FLUSH", "ASYNC"};
        int arglens[2] = {5, 5};
        int built_len;
        char *built = build_resp_cmd("FUNCTION", 8, args, arglens, 2, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "FUNCTION_FLUSH");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* FUNCTION DELETE <library-name> — fan-out to all master nodes so the
     * library is removed everywhere; the client sees success only if every
     * node succeeds, keeping library state identical across shards. */
    if (!strcmp(subcmd, "DELETE")) {
        const char *libname;
        int libnamelen;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &libname, &libnamelen)) {
            client_send_error(w, client, "wrong number of arguments for 'function|delete' command");
            client->hbuf_len = 0;
            return;
        }
        const char *args[2] = {"DELETE", libname};
        int arglens[2] = {6, libnamelen};
        int built_len;
        char *built = build_resp_cmd("FUNCTION", 8, args, arglens, 2, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "FUNCTION_DELETE");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* FUNCTION STATS — fan-out to all master nodes; each node's execution
     * state is reported separately rather than merged (see
     * fanout_agg_function_stats). */
    if (!strcmp(subcmd, "STATS")) {
        const char *args[1] = {"STATS"};
        int arglens[1] = {5};
        int built_len;
        char *built = build_resp_cmd("FUNCTION", 8, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "FUNCTION_STATS");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* FUNCTION LOAD [REPLACE] <code> — fan-out to all master nodes so any
     * subsequent FCALL on any shard finds the library; the client sees
     * success only if every node succeeds. */
    if (!strcmp(subcmd, "LOAD")) {
        const char *arg2;
        int arg2len;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &arg2, &arg2len)) {
            client_send_error(w, client, "wrong number of arguments for 'function|load' command");
            client->hbuf_len = 0;
            return;
        }
        int replace = (arg2len == 7 && strncasecmp(arg2, "REPLACE", 7) == 0);
        const char *code;
        int codelen;
        if (replace) {
            if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 3, &code, &codelen)) {
                client_send_error(w, client, "wrong number of arguments for 'function|load' command");
                client->hbuf_len = 0;
                return;
            }
        } else {
            code = arg2;
            codelen = arg2len;
        }

        int built_len;
        char *built;
        if (replace) {
            const char *args[3] = {"LOAD", "REPLACE", code};
            int arglens[3] = {4, 7, codelen};
            built = build_resp_cmd("FUNCTION", 8, args, arglens, 3, &built_len);
        } else {
            const char *args[2] = {"LOAD", code};
            int arglens[2] = {4, codelen};
            built = build_resp_cmd("FUNCTION", 8, args, arglens, 2, &built_len);
        }
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "FUNCTION_LOAD");
        client->fanout_ctx = ctx;

        fanout_all_masters(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    client_send_error(w, client, "unknown FUNCTION subcommand");
    client->hbuf_len = 0;
}

void send_bulk_reply(Worker *w, ClientConn *client, const char *data, int len) {
    char hdr[32];
    int hdr_len = snprintf(hdr, sizeof(hdr), "$%d\r\n", len);
    int reply_len = hdr_len + len + 2;
    if (client->wbuf_cap < reply_len) {
        char *tmp = realloc(client->wbuf, (size_t)reply_len);
        if (!tmp) {
            client_send_error(w, client, "oom");
            client->hbuf_len = 0;
            return;
        }
        client->wbuf = tmp;
        client->wbuf_cap = reply_len;
    }
    memcpy(client->wbuf, hdr, hdr_len);
    memcpy(client->wbuf + hdr_len, data, len);
    memcpy(client->wbuf + hdr_len + len, "\r\n", 2);
    client->wbuf_len = reply_len;
    client->wbuf_off = 0;
    client->hbuf_len = 0;
    client->state = CS_WRITING_RESP;
    fd_watch(w, client->fd, EL_WRITABLE);
}

void handle_command_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *sub;
    int sublen;
    /* Bare COMMAND (no subcommand) has the same reply shape as COMMAND INFO
     * with no arguments, so it reuses that path verbatim. */
    if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen)) {
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "CMDINFO_ALL");
        client->fanout_ctx = ctx;

        fanout_one_master(w, client, ctx, client->hbuf, cmdlen);
        return;
    }
    char subcmd[32];
    int sclen = sublen < 31 ? sublen : 31;
    memcpy(subcmd, sub, (size_t)sclen);
    subcmd[sclen] = '\0';
    for (int i = 0; subcmd[i]; i++) subcmd[i] = (char)toupper((unsigned char)subcmd[i]);

    /* COMMAND HELP — forward to any one master node, relay the reply as-is */
    if (!strcmp(subcmd, "HELP")) {
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
        return;
    }

    /* COMMAND INFO [command-name ...] — forwarded to any one master node;
     * the reply is post-processed so it only reflects what this proxy
     * itself supports (see fanout_agg_command_info_all/_named). */
    if (!strcmp(subcmd, "INFO")) {
        const char *tmp;
        int tmplen;
        int has_args = resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &tmp, &tmplen);

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), has_args ? "CMDINFO_NAMED" : "CMDINFO_ALL");
        client->fanout_ctx = ctx;

        fanout_one_master(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* COMMAND DOCS [command-name ...] — forwarded to any one master node;
     * the reply is post-processed so it only reflects what this proxy
     * itself supports (see fanout_agg_command_docs). */
    if (!strcmp(subcmd, "DOCS")) {
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "CMDDOCS");
        client->fanout_ctx = ctx;

        fanout_one_master(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* COMMAND LIST [FILTERBY ...] — forwarded to any one master node as-is;
     * the reply is post-processed so it only lists commands this proxy
     * itself supports (see fanout_agg_command_list). */
    if (!strcmp(subcmd, "LIST")) {
        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "CMDLIST");
        client->fanout_ctx = ctx;

        fanout_one_master(w, client, ctx, client->hbuf, cmdlen);
        return;
    }

    /* COMMAND COUNT — the client's request carries no forwardable command,
     * so a fresh "COMMAND LIST" is built and sent to a master node instead;
     * the reply is post-processed into a count of proxy-supported commands
     * (see fanout_agg_command_count). */
    if (!strcmp(subcmd, "COUNT")) {
        const char *args[1] = {"LIST"};
        int arglens[1] = {4};
        int built_len;
        char *built = build_resp_cmd("COMMAND", 7, args, arglens, 1, &built_len);
        if (!built) {
            client_send_error(w, client, "out of memory");
            client->hbuf_len = 0;
            return;
        }

        FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
        if (!ctx) {
            free(built);
            client_send_error(w, client, "out of memory");
            close_client(w, client);
            return;
        }
        ctx->client = client;
        snprintf(ctx->cmd, sizeof(ctx->cmd), "CMDCOUNT");
        client->fanout_ctx = ctx;

        fanout_one_master(w, client, ctx, built, built_len);
        free(built);
        return;
    }

    /* COMMAND GETKEYS / COMMAND GETKEYSANDFLAGS <command> [<arg> ...] — the
     * target command must be one this proxy supports; if it is, forward the
     * request verbatim to any one master node and relay the reply as-is,
     * otherwise reject the client without a backend round-trip. */
    if (!strcmp(subcmd, "GETKEYS") || !strcmp(subcmd, "GETKEYSANDFLAGS")) {
        const char *tgt;
        int tgtlen;
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &tgt, &tgtlen)) {
            client_send_error(w, client, "wrong number of arguments for 'command|getkeys' command");
            client->hbuf_len = 0;
            return;
        }
        char tgtcmd[64];
        int tclen = tgtlen < 63 ? tgtlen : 63;
        memcpy(tgtcmd, tgt, (size_t)tclen);
        tgtcmd[tclen] = '\0';
        for (int i = 0; tgtcmd[i]; i++) tgtcmd[i] = (char)toupper((unsigned char)tgtcmd[i]);

        if (!cmd_is_supported(tgtcmd)) {
            client_send_error(w, client, "Invalid command specified");
            client->hbuf_len = 0;
            return;
        }

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
        return;
    }

    client_send_error(w, client, "unknown COMMAND subcommand");
    client->hbuf_len = 0;
}

void handle_nokey_cmd(Worker *w, ClientConn *client, const char *cmd, int cmdlen) {
    if (!strcmp(cmd, "CLIENT")) {
        handle_client_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "PUBSUB")) {
        handle_pubsub_inspect_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "SLOWLOG")) {
        handle_slowlog_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "COMMANDLOG")) {
        handle_commandlog_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "MODULE")) {
        handle_module_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "SCRIPT")) {
        handle_script_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "FUNCTION")) {
        handle_function_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "COMMAND")) {
        handle_command_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "HELLO")) {
        handle_hello_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "PROXY")) {
        handle_proxy_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "INFO")) {
        handle_info_cmd(w, client, cmdlen);
        return;
    }
    if (!strcmp(cmd, "PING")) {
        const char *msg;
        int msglen;
        if (resp_get_nth_arg(client->hbuf, cmdlen, 1, &msg, &msglen)) {
            send_bulk_reply(w, client, msg, msglen);
            return;
        }
        const char *pong = "+PONG\r\n";
        if (client->wbuf_cap < 7) {
            char *tmp = realloc(client->wbuf, 8);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = 8;
        }
        memcpy(client->wbuf, pong, 7);
        client->wbuf_len = 7;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }
    if (!strcmp(cmd, "ECHO")) {
        const char *msg;
        int msglen;
        if (!resp_get_nth_arg(client->hbuf, cmdlen, 1, &msg, &msglen)) {
            client_send_error(w, client, "wrong number of arguments for 'echo' command");
            client->hbuf_len = 0;
            return;
        }
        send_bulk_reply(w, client, msg, msglen);
        return;
    }
    if (!strcmp(cmd, "TIME")) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        char sec[16], usec[16];
        int sec_len = snprintf(sec, sizeof(sec), "%ld", (long)tv.tv_sec);
        int usec_len = snprintf(usec, sizeof(usec), "%ld", (long)tv.tv_usec);
        char reply[64];
        int reply_len =
            snprintf(reply, sizeof(reply), "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", sec_len, sec, usec_len, usec);
        if (client->wbuf_cap < reply_len) {
            char *tmp = realloc(client->wbuf, (size_t)reply_len);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = reply_len;
        }
        memcpy(client->wbuf, reply, (size_t)reply_len);
        client->wbuf_len = reply_len;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }
    if (!strcmp(cmd, "SELECT")) {
        const char *key;
        int kl;
        int db = 0;
        if (resp_get_nth_arg(client->hbuf, cmdlen, 1, &key, &kl)) {
            char tmp[16];
            int tl = kl < 15 ? kl : 15;
            memcpy(tmp, key, (size_t)tl);
            tmp[tl] = '\0';
            db = atoi(tmp);
        }
        if (db != 0) {
            const char *e = "-ERR DB index is out of range\r\n";
            ssize_t wr = write(client->fd, e, strlen(e));
            (void)wr;
            client->hbuf_len = 0;
            return;
        }
        const char *ok = "+OK\r\n";
        if (client->wbuf_cap < 5) {
            char *tmp = realloc(client->wbuf, 8);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = 8;
        }
        memcpy(client->wbuf, ok, 5);
        client->wbuf_len = 5;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }
    if (!strcmp(cmd, "RANDOMKEY") || !strcmp(cmd, "LOLWUT") || !strcmp(cmd, "FT._LIST") || !strcmp(cmd, "FT.INFO") ||
        !strcmp(cmd, "FT.CREATE") || !strcmp(cmd, "FT.DROPINDEX") || !strcmp(cmd, "FT.SEARCH") ||
        !strcmp(cmd, "FT.AGGREGATE")) {
        /* FT.INFO <index> with no scope options defaults to the backend's
         * LOCAL mode (only that one shard's stats); rewrite it to
         * FT.INFO <index> PRIMARY ALLSHARDS so the reply covers every
         * primary. Explicit options are forwarded as-is. */
        const char *idx, *extra;
        int idxlen, extralen;
        if (!strcmp(cmd, "FT.INFO") && resp_get_nth_arg(client->hbuf, cmdlen, 1, &idx, &idxlen) &&
            !resp_get_nth_arg(client->hbuf, cmdlen, 2, &extra, &extralen)) {
            const char *args[3] = {idx, "PRIMARY", "ALLSHARDS"};
            int arglens[3] = {idxlen, 7, 9};
            int built_len;
            char *built = build_resp_cmd("FT.INFO", 7, args, arglens, 3, &built_len);
            if (!built) {
                client_send_error(w, client, "out of memory");
                client->hbuf_len = 0;
                return;
            }
            if (client->pbuf_cap < built_len) {
                char *tmp = realloc(client->pbuf, (size_t)built_len);
                if (!tmp) {
                    free(built);
                    client_send_error(w, client, "out of memory");
                    client->hbuf_len = 0;
                    return;
                }
                client->pbuf = tmp;
                client->pbuf_cap = built_len;
            }
            memcpy(client->pbuf, built, (size_t)built_len);
            client->pbuf_len = built_len;
            client->using_pbuf = 1;
            free(built);
        }
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
        return;
    }

    int target_nodes[MAX_NODES];
    int ntargets = topo_all_masters(w, target_nodes);

    FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
    if (!ctx) {
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return;
    }
    ctx->client = client;
    snprintf(ctx->cmd, sizeof(ctx->cmd), "%s", cmd);
    ctx->nsubs = ntargets;
    ctx->ndone = 0;
    ctx->had_error = 0;
    client->fanout_ctx = ctx;

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        fanout_ctx_free(ctx);
        client->fanout_ctx = NULL;
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }

    for (int ti = 0; ti < ntargets; ti++) {
        int ni = target_nodes[ti];
        FanoutSub *sub = &ctx->subs[ti];
        sub->ctx = ctx;
        sub->sub_idx = ti;
        sub->node_idx = ni;
        sub->rbuf = NULL;
        sub->rbuf_len = sub->rbuf_cap = 0;
        sub->done = 0;
        sub->error = 0;
        sub->conn = NULL;

        ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
        PoolConn *conn = pool_acquire_nb(w, pool);
        if (!conn) {
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            continue;
        }

        if (conn->wbuf_cap < cmdlen) {
            char *tmp = realloc(conn->wbuf, (size_t)cmdlen);
            if (!tmp) {
                pool_release(w, conn);
                sub->error = 1;
                sub->done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            conn->wbuf = tmp;
            conn->wbuf_cap = cmdlen;
        }
        memcpy(conn->wbuf, client->hbuf, (size_t)cmdlen);
        conn->wbuf_len = cmdlen;
        conn->wbuf_off = 0;
        conn->waiting_client = NULL;
        conn->fanout_sub = sub;
        sub->conn = conn;
        if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;
        fd_watch(w, conn->fd, EL_WRITABLE);
    }

    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
}

void handle_scan_cmd(Worker *w, ClientConn *client, int cmdlen) {
    const char *cursor_str;
    int cursor_len;
    if (!resp_get_nth_arg(client->hbuf, cmdlen, 1, &cursor_str, &cursor_len)) {
        client_send_error(w, client, "protocol error");
        client->hbuf_len = 0;
        return;
    }

    int target_nodes[MAX_NODES];
    int ntargets = topo_all_masters(w, target_nodes);

    uint64_t node_cursor[MAX_NODES];
    memset(node_cursor, 0, sizeof(node_cursor));
    int is_fresh = (cursor_len == 1 && cursor_str[0] == '0');
    if (!is_fresh) scan_decode_cursor(cursor_str, cursor_len, node_cursor, ntargets);

    int active_nodes[MAX_NODES];
    int nactive = 0;
    uint64_t active_cursors[MAX_NODES];
    for (int ti = 0; ti < ntargets; ti++) {
        if (is_fresh || node_cursor[ti] != 0) {
            active_cursors[nactive] = is_fresh ? 0 : node_cursor[ti];
            active_nodes[nactive++] = target_nodes[ti];
        }
    }

    if (nactive == 0) {
        const char *resp = "*2\r\n$1\r\n0\r\n*0\r\n";
        int rlen = (int)strlen(resp);
        if (client->wbuf_cap < rlen) {
            char *tmp = realloc(client->wbuf, (size_t)rlen + 1);
            if (!tmp) {
                client_send_error(w, client, "oom");
                client->hbuf_len = 0;
                return;
            }
            client->wbuf = tmp;
            client->wbuf_cap = rlen + 1;
        }
        memcpy(client->wbuf, resp, (size_t)rlen);
        client->wbuf_len = rlen;
        client->wbuf_off = 0;
        client->hbuf_len = 0;
        client->state = CS_WRITING_RESP;
        fd_watch(w, client->fd, EL_WRITABLE);
        return;
    }

    FanoutCtx *ctx = calloc(1, sizeof(FanoutCtx));
    if (!ctx) {
        client_send_error(w, client, "out of memory");
        close_client(w, client);
        return;
    }
    ctx->client = client;
    strcpy(ctx->cmd, "SCAN");
    ctx->nsubs = nactive;
    ctx->ndone = 0;
    ctx->had_error = 0;
    ctx->scan_ntargets = ntargets;
    memcpy(ctx->scan_target_nodes, target_nodes, (size_t)ntargets * sizeof(int));
    memset(ctx->scan_node_cursors, 0, sizeof(ctx->scan_node_cursors));
    for (int ti = 0; ti < ntargets; ti++) ctx->scan_node_cursors[target_nodes[ti]] = is_fresh ? 0 : node_cursor[ti];
    client->fanout_ctx = ctx;

    const char *opt_args[8];
    int opt_arglens[8];
    int nopt = 0;
    for (int i = 2; nopt < 8; i++) {
        const char *p;
        int pl;
        if (!resp_get_nth_arg(client->hbuf, cmdlen, i, &p, &pl)) break;
        opt_args[nopt] = p;
        opt_arglens[nopt] = pl;
        nopt++;
    }

    DbGroup *grp = db_group_get(w, client->db);
    if (!grp) {
        fanout_ctx_free(ctx);
        client->fanout_ctx = NULL;
        client_send_error(w, client, "db group error");
        close_client(w, client);
        return;
    }

    for (int ai = 0; ai < nactive; ai++) {
        int ni = active_nodes[ai];
        FanoutSub *sub = &ctx->subs[ai];
        sub->ctx = ctx;
        sub->sub_idx = ai;
        sub->node_idx = ni;
        sub->rbuf = NULL;
        sub->rbuf_len = sub->rbuf_cap = 0;
        sub->done = 0;
        sub->error = 0;
        sub->conn = NULL;

        char curs_buf[24];
        int curs_len = snprintf(curs_buf, sizeof(curs_buf), "%llu", (unsigned long long)active_cursors[ai]);
        const char *args[9];
        int arglens[9];
        int nargs = 0;
        args[nargs] = curs_buf;
        arglens[nargs] = curs_len;
        nargs++;
        for (int oi = 0; oi < nopt; oi++) {
            args[nargs] = opt_args[oi];
            arglens[nargs] = opt_arglens[oi];
            nargs++;
        }
        int sub_cmdlen;
        char *sub_cmd = build_resp_cmd("SCAN", 4, args, arglens, nargs, &sub_cmdlen);
        if (!sub_cmd) {
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            continue;
        }

        ConnPool *pool = &grp->node_pools[ni].pools[POOL_NORMAL];
        PoolConn *conn = pool_acquire_nb(w, pool);
        if (!conn) {
            free(sub_cmd);
            sub->error = 1;
            sub->done = 1;
            ctx->had_error = 1;
            ctx->ndone++;
            continue;
        }

        conn->waiting_client = NULL;
        conn->fanout_sub = sub;
        sub->conn = conn;
        if (conn->fd >= 0 && conn->fd < g_config.max_connection) w->backend_map[conn->fd] = conn;

        if (resp3_conn_needs_upgrade(conn)) {
            /* Same reasoning as fanout_dispatch_sub() above: stash the real
             * SCAN sub-command and let handle_resp3_handshake_readable()
             * flush it once this connection has upgraded. */
            int derr = resp3_defer_append(conn, sub_cmd, sub_cmdlen) < 0 || resp3_start_conn_upgrade(w, conn) < 0;
            free(sub_cmd);
            if (derr) {
                conn->fanout_sub = NULL;
                pool_destroy_conn(w, conn);
                sub->conn = NULL;
                sub->error = 1;
                sub->done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
            }
            continue;
        }

        if (conn->wbuf_cap < sub_cmdlen) {
            char *tmp = realloc(conn->wbuf, (size_t)sub_cmdlen);
            if (!tmp) {
                free(sub_cmd);
                conn->fanout_sub = NULL;
                pool_release(w, conn);
                sub->conn = NULL;
                sub->error = 1;
                sub->done = 1;
                ctx->had_error = 1;
                ctx->ndone++;
                continue;
            }
            conn->wbuf = tmp;
            conn->wbuf_cap = sub_cmdlen;
        }
        memcpy(conn->wbuf, sub_cmd, (size_t)sub_cmdlen);
        conn->wbuf_len = sub_cmdlen;
        conn->wbuf_off = 0;
        free(sub_cmd);
        fd_watch(w, conn->fd, EL_WRITABLE);
    }

    if (ctx->ndone == ctx->nsubs) fanout_aggregate_and_reply(w, ctx);
}

void handle_multikey_complete(Worker *w, ClientConn *client, const char *cmd) {
    if (cmd_is_fanout(cmd)) {
        handle_fanout_cmd(w, client, cmd);
        return;
    }

    const char *kp[256];
    int kl[256];
    int nkeys = resp_extract_keys(client->pbuf, client->pbuf_len, cmd, kp, kl, 256);
    if (nkeys < 0) {
        client_send_error(w, client, "protocol error");
        client->pbuf_len = 0;
        client->hbuf_len = 0;
        client->using_pbuf = 0;
        client->state = CS_READING_HDR;
        fd_watch(w, client->fd, EL_READABLE);
        return;
    }
    int is_keyless_script = (nkeys == 0) && (!strcmp(cmd, "EVAL") || !strcmp(cmd, "EVAL_RO") ||
                                             !strcmp(cmd, "EVALSHA") || !strcmp(cmd, "EVALSHA_RO"));
    if (nkeys == 0 && !is_keyless_script) {
        client_send_error(w, client, "missing key");
        client->pbuf_len = 0;
        client->hbuf_len = 0;
        client->using_pbuf = 0;
        client->state = CS_READING_HDR;
        fd_watch(w, client->fd, EL_READABLE);
        return;
    }

    int node_idx;
    if (is_keyless_script) {
        /* No key to hash a slot from; route to the first master, same
         * convention as OBJECT HELP / XGROUP HELP. */
        node_idx = topo_first_master(w);
    } else {
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

        node_idx = w->topo.slot_map[slot];
        if (node_idx < 0 || node_idx >= w->topo.node_count) {
            client_send_error(w, client, "slot not mapped");
            client->pbuf_len = 0;
            client->hbuf_len = 0;
            client->using_pbuf = 0;
            client->state = CS_READING_HDR;
            fd_watch(w, client->fd, EL_READABLE);
            return;
        }
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
    PoolType mkey_pool = POOL_NORMAL;
    if ((!strcmp(cmd, "XREAD") || !strcmp(cmd, "XREADGROUP")) && resp_xread_has_block(client->pbuf, client->pbuf_len))
        mkey_pool = POOL_BLOCKING;
    ConnPool *pool = &grp->node_pools[node_idx].pools[mkey_pool];
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
