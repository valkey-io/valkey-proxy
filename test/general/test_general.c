#include "test_common.h"
#include <time.h>
#include <limits.h>

typedef struct {
    int ok;
    const char *label;
} _GnRes;
#define _GN_MAX 200

#undef CHECK
#define CHECK(cond, _lbl, pass, fail) \
    do {                              \
        int _ok = !!(cond);           \
        if (_ok)                      \
            (*(pass))++;              \
        else                          \
            (*(fail))++;              \
        if (_ntr < _GN_MAX) {         \
            _tr[_ntr].ok = _ok;       \
            _tr[_ntr].label = (_lbl); \
            _ntr++;                   \
        }                             \
    } while (0)

/* Read one SLOWLOG GET entry [id, timestamp, duration, args, addr, name].
 * Stores the timestamp in *ts. Returns 1 if one of the args equals key,
 * 0 if not, -1 on a malformed entry. */
static int read_slowlog_entry(int fd, long long *ts, const char *key) {
    char t[512];
    resp_next_str(fd, t, sizeof(t));
    if (t[0] != '*') return -1;
    int nel = atoi(t + 1);
    if (nel < 4) return -1;
    resp_next_str(fd, t, sizeof(t)); /* id */
    resp_next_str(fd, t, sizeof(t)); /* timestamp */
    if (t[0] != ':') return -1;
    *ts = atoll(t + 1);
    resp_next_str(fd, t, sizeof(t)); /* duration */
    resp_next_str(fd, t, sizeof(t)); /* args header */
    if (t[0] != '*') return -1;
    int nargs = atoi(t + 1), found = 0;
    for (int i = 0; i < nargs; i++) {
        resp_next_str(fd, t, sizeof(t));
        if (strcmp(t, key) == 0) found = 1;
    }
    for (int i = 4; i < nel; i++) resp_skip_reply(fd); /* addr, name */
    return found;
}

/* SET key to a len-byte value, streamed in chunks (resp_send_raw() caps a
 * whole command at 128KB). Returns the reply's first token in out. */
static char *send_big_set(int fd, const char *key, size_t len, char *out, int outsz) {
    char hdr[256], chunk[65536];
    int hl = snprintf(hdr, sizeof(hdr), "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%zu\r\n", (int)strlen(key), key, len);
    memset(chunk, 'x', sizeof(chunk));
    int ok = write(fd, hdr, (size_t)hl) == hl;
    for (size_t rem = len; ok && rem > 0;) {
        size_t c = rem < sizeof(chunk) ? rem : sizeof(chunk);
        ssize_t n = write(fd, chunk, c);
        if (n <= 0)
            ok = 0;
        else
            rem -= (size_t)n;
    }
    if (ok) ok = write(fd, "\r\n", 2) == 2;
    if (!ok) {
        snprintf(out, outsz, "(send-error)");
        return NULL;
    }
    return resp_next_str(fd, out, outsz);
}

/* Sum every "<field><number>" occurrence in a PROXY POOLS reply. */
static unsigned long long sum_pool_field(const char *s, const char *field) {
    unsigned long long total = 0;
    size_t fl = strlen(field);
    for (const char *p = strstr(s, field); p; p = strstr(p + fl, field)) total += strtoull(p + fl, NULL, 10);
    return total;
}

void run_general_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== General Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    long long dbsize_before, dbsize_after;
    _GnRes _tr[_GN_MAX];
    int _ntr = 0;

    /* ── DEL ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del", "val");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:del");
    CHECK(strcmp(r, ":1") == 0, "DEL: existing key returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:del");
    CHECK(strcmp(r, ":0") == 0, "DEL: non-existent key returns 0", pass, fail);

    /* DEL with 3 keys across different slots (fan-out)
     * tc:gn:del:k1 → slot 4251, k2 → 8440, k3 → 12505 */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del:k1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del:k2", "v2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del:k3", "v3");
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_before = atoll(r + 1);
    proxy_cmd(fd, r, sizeof(r), 4, "DEL", "tc:gn:del:k1", "tc:gn:del:k2", "tc:gn:del:k3");
    CHECK(strcmp(r, ":3") == 0, "DEL 3-key: all 3 exist across slots returns 3", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_after = atoll(r + 1);
    CHECK(dbsize_before - dbsize_after == 3, "DEL 3-key: DBSIZE decreases by 3 after deletion", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del:k1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:del:k2", "v2");
    /* tc:gn:del:k3 intentionally not set */
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_before = atoll(r + 1);
    proxy_cmd(fd, r, sizeof(r), 4, "DEL", "tc:gn:del:k1", "tc:gn:del:k2", "tc:gn:del:k3");
    CHECK(strcmp(r, ":2") == 0, "DEL 3-key: 2 exist, 1 missing returns 2", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_after = atoll(r + 1);
    CHECK(dbsize_before - dbsize_after == 2, "DEL 3-key: DBSIZE decreases by 2 after partial deletion", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "DEL", "tc:gn:del:k1", "tc:gn:del:k2", "tc:gn:del:k3");
    CHECK(strcmp(r, ":0") == 0, "DEL 3-key: none exist returns 0", pass, fail);

    /* ── DBSIZE ──────────────────────────────────────────────────────── */
    /* DBSIZE fans out to all master nodes and sums the results */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dbsz:k1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dbsz:k2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dbsz:k3");

    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    CHECK(r[0] == ':', "DBSIZE: returns integer", pass, fail);
    dbsize_before = atoll(r + 1);

    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:dbsz:k1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:dbsz:k2", "v2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:dbsz:k3", "v3");
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_after = atoll(r + 1);
    CHECK(dbsize_after - dbsize_before == 3, "DBSIZE: increases by 3 after 3 SETs", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "DEL", "tc:gn:dbsz:k1", "tc:gn:dbsz:k2", "tc:gn:dbsz:k3");
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    dbsize_after = atoll(r + 1);
    CHECK(dbsize_after == dbsize_before, "DBSIZE: returns to original count after DEL", pass, fail);

    /* ── EXISTS ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:exists", "val");
    proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:gn:exists");
    CHECK(strcmp(r, ":1") == 0, "EXISTS: existing key returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:gn:noexist");
    CHECK(strcmp(r, ":0") == 0, "EXISTS: non-existent key returns 0", pass, fail);

    /* EXISTS with 3 keys across different slots (fan-out)
     * tc:gn:ex:a → slot 3383 (node 0), d → slot 7570 (node 1), b → slot 15700 (node 2) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ex:a", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ex:d", "v2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ex:b", "v3");
    proxy_cmd(fd, r, sizeof(r), 4, "EXISTS", "tc:gn:ex:a", "tc:gn:ex:d", "tc:gn:ex:b");
    CHECK(strcmp(r, ":3") == 0, "EXISTS 3-key: all 3 exist across slots returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ex:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ex:d");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ex:b");
    proxy_cmd(fd, r, sizeof(r), 4, "EXISTS", "tc:gn:ex:a", "tc:gn:ex:d", "tc:gn:ex:b");
    CHECK(strcmp(r, ":0") == 0, "EXISTS 3-key: none exist returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ex:a", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ex:d", "v2");
    /* tc:gn:ex:b intentionally not set */
    proxy_cmd(fd, r, sizeof(r), 4, "EXISTS", "tc:gn:ex:a", "tc:gn:ex:d", "tc:gn:ex:b");
    CHECK(strcmp(r, ":2") == 0, "EXISTS 3-key: 2 exist, 1 missing returns 2", pass, fail);

    /* EXISTS counts each occurrence of the same key separately */
    proxy_cmd(fd, r, sizeof(r), 4, "EXISTS", "tc:gn:ex:a", "tc:gn:ex:a", "tc:gn:ex:a");
    CHECK(strcmp(r, ":3") == 0, "EXISTS 3-key: same key repeated 3x returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ex:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ex:d");

    /* ── EXPIRE ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:expire", "val");
    proxy_cmd(fd, r, sizeof(r), 3, "EXPIRE", "tc:gn:expire", "100");
    CHECK(strcmp(r, ":1") == 0, "EXPIRE: set expiry returns 1", pass, fail);

    /* ── EXPIREAT ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:expireat", "val");
    {
        char ts_str[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", (long long)time(NULL) + 300);
        proxy_cmd(fd, r, sizeof(r), 3, "EXPIREAT", "tc:gn:expireat", ts_str);
        CHECK(strcmp(r, ":1") == 0, "EXPIREAT: set expiry at timestamp returns 1", pass, fail);
    }

    /* ── EXPIRETIME ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:expiretime", "val");
    {
        long long ts = (long long)time(NULL) + 500;
        char ts_str[32], expect[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", ts);
        snprintf(expect, sizeof(expect), ":%lld", ts);
        proxy_cmd(fd, r, sizeof(r), 3, "EXPIREAT", "tc:gn:expiretime", ts_str);
        proxy_cmd(fd, r, sizeof(r), 2, "EXPIRETIME", "tc:gn:expiretime");
        CHECK(strcmp(r, expect) == 0, "EXPIRETIME: returns the set expiry timestamp", pass, fail);
    }

    /* ── TTL ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "SET", "tc:gn:ttl", "val", "EX", "100");
    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:gn:ttl");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 100, "TTL: returns value in (0, 100]", pass, fail);
    }

    /* ── PTTL ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "SET", "tc:gn:pttl", "val", "PX", "100000");
    proxy_cmd(fd, r, sizeof(r), 2, "PTTL", "tc:gn:pttl");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 100000, "PTTL: returns value in (0, 100000]", pass, fail);
    }

    /* ── PERSIST ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "SET", "tc:gn:persist", "val", "EX", "100");
    proxy_cmd(fd, r, sizeof(r), 2, "PERSIST", "tc:gn:persist");
    CHECK(strcmp(r, ":1") == 0, "PERSIST: removes expiry returns 1", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:gn:persist");
    CHECK(strcmp(r, ":-1") == 0, "PERSIST: TTL after persist is -1", pass, fail);

    /* ── PEXPIRE ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:pexpire", "val");
    proxy_cmd(fd, r, sizeof(r), 3, "PEXPIRE", "tc:gn:pexpire", "100000");
    CHECK(strcmp(r, ":1") == 0, "PEXPIRE: set expiry returns 1", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "PTTL", "tc:gn:pexpire");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 100000, "PEXPIRE: PTTL in (0, 100000]", pass, fail);
    }

    /* ── PEXPIREAT ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:pexpireat", "val");
    {
        char ts_str[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", ((long long)time(NULL) + 300) * 1000LL);
        proxy_cmd(fd, r, sizeof(r), 3, "PEXPIREAT", "tc:gn:pexpireat", ts_str);
        CHECK(strcmp(r, ":1") == 0, "PEXPIREAT: set expiry at ms-timestamp returns 1", pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 2, "PTTL", "tc:gn:pexpireat");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 300000, "PEXPIREAT: PTTL in (0, 300000]", pass, fail);
    }

    /* ── PEXPIRETIME ─────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:pexpiretime", "val");
    {
        long long ts_ms = ((long long)time(NULL) + 500) * 1000LL;
        char ts_str[32], expect[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", ts_ms);
        snprintf(expect, sizeof(expect), ":%lld", ts_ms);
        proxy_cmd(fd, r, sizeof(r), 3, "PEXPIREAT", "tc:gn:pexpiretime", ts_str);
        proxy_cmd(fd, r, sizeof(r), 2, "PEXPIRETIME", "tc:gn:pexpiretime");
        CHECK(strcmp(r, expect) == 0, "PEXPIRETIME: returns the set ms-expiry timestamp", pass, fail);
    }

    /* ── TYPE ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:type", "val");
    proxy_cmd(fd, r, sizeof(r), 2, "TYPE", "tc:gn:type");
    CHECK(strcmp(r, "+string") == 0, "TYPE: string key returns +string", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "TYPE", "tc:gn:noexist");
    CHECK(strcmp(r, "+none") == 0, "TYPE: non-existent key returns +none", pass, fail);

    /* ── RENAME (same slot) ──────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rn}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rn}dst");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:gn:rn}src", "rn_val");
    proxy_cmd(fd, r, sizeof(r), 3, "RENAME", "{tc:gn:rn}src", "{tc:gn:rn}dst");
    CHECK(strcmp(r, "+OK") == 0, "RENAME: same-slot returns +OK", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:gn:rn}dst");
    CHECK(strcmp(r, "rn_val") == 0, "RENAME: renamed key has correct value", pass, fail);

    /* ── RENAME (cross-slot, should return CROSSSLOT error) ──────────
     * tc:cs:foo → slot 4418,  tc:cs:bar → slot 11537  (verified via CRC16)
     */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:foo", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "RENAME", "tc:cs:foo", "tc:cs:bar");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "RENAME: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── RENAMENX (same slot) ────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rnx}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rnx}dst");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:gn:rnx}src", "rnx_val");
    proxy_cmd(fd, r, sizeof(r), 3, "RENAMENX", "{tc:gn:rnx}src", "{tc:gn:rnx}dst");
    CHECK(strcmp(r, ":1") == 0, "RENAMENX: same-slot, to non-existent dst returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:gn:rnx}src2", "v");
    proxy_cmd(fd, r, sizeof(r), 3, "RENAMENX", "{tc:gn:rnx}src2", "{tc:gn:rnx}dst");
    CHECK(strcmp(r, ":0") == 0, "RENAMENX: same-slot, to existing dst returns 0", pass, fail);

    /* ── RENAMENX (cross-slot, should return CROSSSLOT error) ────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:foo", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "RENAMENX", "tc:cs:foo", "tc:cs:bar");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "RENAMENX: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── COPY (same slot) ────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:cp}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:cp}dst");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:gn:cp}src", "cp_val");
    proxy_cmd(fd, r, sizeof(r), 3, "COPY", "{tc:gn:cp}src", "{tc:gn:cp}dst");
    CHECK(strcmp(r, ":1") == 0, "COPY: same-slot returns 1 on success", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:gn:cp}dst");
    CHECK(strcmp(r, "cp_val") == 0, "COPY: destination has correct value", pass, fail);

    /* ── COPY (cross-slot, should return CROSSSLOT error) ────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:foo", "v1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:cs:bar");
    proxy_cmd(fd, r, sizeof(r), 3, "COPY", "tc:cs:foo", "tc:cs:bar");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "COPY: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── DUMP / RESTORE ──────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dump:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dump:dst");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:dump:src", "hello_restore");

    {
        char dump_buf[4096];
        resp_send(fd, 2, "DUMP", "tc:gn:dump:src");
        int dump_len = resp_next_binary(fd, dump_buf, sizeof(dump_buf));
        CHECK(dump_len > 0, "DUMP: returns binary serialized data", pass, fail);

        if (dump_len > 0) {
            const char *args[] = {"RESTORE", "tc:gn:dump:dst", "0", dump_buf};
            int lens[] = {7, 14, 1, dump_len};
            resp_send_raw(fd, 4, args, lens);
            resp_next_str(fd, r, sizeof(r));
            CHECK(strcmp(r, "+OK") == 0, "RESTORE: restores key successfully", pass, fail);

            proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:gn:dump:dst");
            CHECK(strcmp(r, "hello_restore") == 0, "RESTORE: GET after restore returns original value", pass, fail);
        }
    }

    /* ── OBJECT ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:obj", "hello");

    proxy_cmd(fd, r, sizeof(r), 3, "OBJECT", "ENCODING", "tc:gn:obj");
    CHECK(r[0] != '-', "OBJECT ENCODING: returns encoding (not an error)", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "OBJECT", "REFCOUNT", "tc:gn:obj");
    CHECK(r[0] == ':', "OBJECT REFCOUNT: returns integer refcount", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "OBJECT", "IDLETIME", "tc:gn:obj");
    CHECK(r[0] == ':', "OBJECT IDLETIME: returns integer idle seconds", pass, fail);

    /* FREQ requires LFU maxmemory-policy; Valkey returns -ERR if not configured */
    proxy_cmd(fd, r, sizeof(r), 3, "OBJECT", "FREQ", "tc:gn:obj");
    CHECK(r[0] == ':' || r[0] == '-', "OBJECT FREQ: proxy routes correctly (integer or Valkey error)", pass, fail);

    /* OBJECT HELP has no key — routed to first master */
    resp_send(fd, 2, "OBJECT", "HELP");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "OBJECT HELP: returns non-empty help array", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_next_str(fd, r, sizeof(r));
    }

    /* ── MEMORY USAGE ────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "MEMORY", "USAGE", "tc:gn:obj");
    CHECK(r[0] == ':', "MEMORY USAGE: returns integer byte size", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "MEMORY", "USAGE", "tc:gn:obj", "SAMPLES", "5");
    CHECK(r[0] == ':', "MEMORY USAGE with SAMPLES: returns integer byte size", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "MEMORY", "DOCTOR");
    CHECK(r[0] == '-', "MEMORY DOCTOR: unsupported subcommand returns error", pass, fail);

    /* ── TOUCH ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:touch", "val");
    proxy_cmd(fd, r, sizeof(r), 2, "TOUCH", "tc:gn:touch");
    CHECK(strcmp(r, ":1") == 0, "TOUCH: existing key returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "TOUCH", "tc:gn:noexist");
    CHECK(strcmp(r, ":0") == 0, "TOUCH: non-existent key returns 0", pass, fail);

    /* TOUCH multi-node fan-out: keys span all 3 cluster nodes
     * tc:gn:touch:k1 → slot 1840 (Node 0), k2 → slot 14163 (Node 2), k3 → slot 10098 (Node 1) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:touch:k1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:touch:k2", "v2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:touch:k3", "v3");
    proxy_cmd(fd, r, sizeof(r), 4, "TOUCH", "tc:gn:touch:k1", "tc:gn:touch:k2", "tc:gn:touch:k3");
    CHECK(strcmp(r, ":3") == 0, "TOUCH multi-node: 3 keys on nodes 0/1/2 returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "TOUCH", "tc:gn:touch:k1", "tc:gn:touch:k2", "tc:gn:noexist");
    CHECK(strcmp(r, ":2") == 0, "TOUCH multi-node: 2 exist 1 missing returns 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:touch:k1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:touch:k2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:touch:k3");

    /* ── UNLINK ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:unlink", "val");
    proxy_cmd(fd, r, sizeof(r), 2, "UNLINK", "tc:gn:unlink");
    CHECK(strcmp(r, ":1") == 0, "UNLINK: existing key returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "UNLINK", "tc:gn:noexist");
    CHECK(strcmp(r, ":0") == 0, "UNLINK: non-existent key returns 0", pass, fail);

    /* UNLINK with 3 keys across different slots (fan-out) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:k1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:k2", "v2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:k3", "v3");
    proxy_cmd(fd, r, sizeof(r), 4, "UNLINK", "tc:gn:ul:k1", "tc:gn:ul:k2", "tc:gn:ul:k3");
    CHECK(strcmp(r, ":3") == 0, "UNLINK 3-key: all 3 exist returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "UNLINK", "tc:gn:ul:k1", "tc:gn:ul:k2", "tc:gn:ul:k3");
    CHECK(strcmp(r, ":0") == 0, "UNLINK 3-key: none exist returns 0", pass, fail);

    /* UNLINK multi-node fan-out: keys span all 3 cluster nodes
     * tc:gn:ul:nd0 → slot 14192 (Node 2), nd1 → slot 10065 (Node 1), nd3 → slot 1811 (Node 0) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:nd0", "v0");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:nd1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:ul:nd3", "v3");
    proxy_cmd(fd, r, sizeof(r), 4, "UNLINK", "tc:gn:ul:nd0", "tc:gn:ul:nd1", "tc:gn:ul:nd3");
    CHECK(strcmp(r, ":3") == 0, "UNLINK multi-node: 3 keys on nodes 0/1/2 returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "UNLINK", "tc:gn:ul:nd0", "tc:gn:ul:nd1", "tc:gn:ul:nd3");
    CHECK(strcmp(r, ":0") == 0, "UNLINK multi-node: none exist returns 0", pass, fail);

    /* ── SORT ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:sort");
    proxy_cmd(fd, r, sizeof(r), 2, "SORT", "tc:gn:sort");
    CHECK(strcmp(r, "*0") == 0, "SORT: non-existent key returns empty array", pass, fail);

    /* ── SORT_RO ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:sort_ro");
    proxy_cmd(fd, r, sizeof(r), 2, "SORT_RO", "tc:gn:sort_ro");
    CHECK(strcmp(r, "*0") == 0, "SORT_RO: non-existent key returns empty array", pass, fail);

    /* ── SCAN multi-node ─────────────────────────────────────────────── */
    /* tc:gn:scan:nd0 → slot 13662 (Node 2), nd1 → slot 9599 (Node 1), nd2 → slot 5404 (Node 0) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:scan:nd0", "sv0");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:scan:nd1", "sv1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:scan:nd2", "sv2");
    {
        int has_nd0 = 0, has_nd1 = 0, has_nd2 = 0;
        char scan_cursor[512] = "0";
        do {
            resp_send(fd, 6, "SCAN", scan_cursor, "MATCH", "tc:gn:scan:*", "COUNT", "100");
            resp_next_str(fd, r, sizeof(r));                     /* *2 */
            resp_next_str(fd, scan_cursor, sizeof(scan_cursor)); /* cursor */
            resp_next_str(fd, r, sizeof(r));                     /* *N */
            int n = atoi(r + 1);
            for (int i = 0; i < n; i++) {
                resp_next_str(fd, r, sizeof(r));
                if (strcmp(r, "tc:gn:scan:nd0") == 0) has_nd0 = 1;
                if (strcmp(r, "tc:gn:scan:nd1") == 0) has_nd1 = 1;
                if (strcmp(r, "tc:gn:scan:nd2") == 0) has_nd2 = 1;
            }
        } while (strcmp(scan_cursor, "0") != 0);
        CHECK(has_nd0, "SCAN multi-node: nd0 (Node2 slot 13662) returned", pass, fail);
        CHECK(has_nd1, "SCAN multi-node: nd1 (Node1 slot 9599) returned", pass, fail);
        CHECK(has_nd2, "SCAN multi-node: nd2 (Node0 slot 5404) returned", pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd0");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd2");

    /* ── KEYS ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:keys:alpha", "1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:keys:beta", "2");

    resp_send(fd, 2, "KEYS", "tc:gn:keys:*");
    resp_next_str(fd, r, sizeof(r)); /* "*N" header */
    {
        int n = atoi(r + 1);
        int has_alpha = resp_array_contains(fd, n, "tc:gn:keys:alpha");
        CHECK(has_alpha, "KEYS: pattern matches tc:gn:keys:alpha", pass, fail);
    }

    resp_send(fd, 2, "KEYS", "tc:gn:keys:*");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_beta = resp_array_contains(fd, n, "tc:gn:keys:beta");
        CHECK(has_beta, "KEYS: pattern matches tc:gn:keys:beta", pass, fail);
    }

    /* ── KEYS multi-node fan-out ─────────────────────────────────────── */
    /* Insert one key per cluster node to verify fan-out aggregation:
     * tc:gn:keys:nd0  → slot 1523  (Node 0: slots 0–5460)
     * tc:gn:keys:nd1  → slot 5586  (Node 1: slots 5461–10922)
     * tc:gn:keys:nd2x → slot 11216 (Node 2: slots 10923–16383) */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:keys:nd0", "v0");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:keys:nd1", "v1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:keys:nd2x", "v2");

    resp_send(fd, 2, "KEYS", "tc:gn:keys:nd*");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_nd0 = resp_array_contains(fd, n, "tc:gn:keys:nd0");
        CHECK(has_nd0, "KEYS multi-node: nd0 (Node0 slot 1523) returned", pass, fail);
    }

    resp_send(fd, 2, "KEYS", "tc:gn:keys:nd*");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_nd1 = resp_array_contains(fd, n, "tc:gn:keys:nd1");
        CHECK(has_nd1, "KEYS multi-node: nd1 (Node1 slot 5586) returned", pass, fail);
    }

    resp_send(fd, 2, "KEYS", "tc:gn:keys:nd*");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_nd2x = resp_array_contains(fd, n, "tc:gn:keys:nd2x");
        CHECK(has_nd2x, "KEYS multi-node: nd2x (Node2 slot 11216) returned", pass, fail);
    }

    /* ── SLOWLOG ─────────────────────────────────────────────────────── */
    /* SLOWLOG HELP has no key — routed to a single master node */
    resp_send(fd, 2, "SLOWLOG", "HELP");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "SLOWLOG HELP: returns non-empty help array", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_next_str(fd, r, sizeof(r));
    }

    /* SLOWLOG RESET fans out to all master nodes, replies +OK */
    proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "RESET");
    CHECK(strcmp(r, "+OK") == 0, "SLOWLOG RESET: returns +OK", pass, fail);

    /* SLOWLOG LEN fans out to all master nodes and sums counts;
     * right after RESET the sum across all nodes must be 0 */
    proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "LEN");
    CHECK(strcmp(r, ":0") == 0, "SLOWLOG LEN: returns 0 immediately after RESET", pass, fail);

    /* SLOWLOG GET [count] fans out to all master nodes and merges the
     * entries newest first; right after RESET every form is an empty array */
    proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "GET");
    CHECK(strcmp(r, "*0") == 0, "SLOWLOG GET: returns empty array after RESET", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "5");
    CHECK(strcmp(r, "*0") == 0, "SLOWLOG GET 5: returns empty array after RESET", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "-1");
    CHECK(strcmp(r, "*0") == 0, "SLOWLOG GET -1: returns empty array after RESET", pass, fail);

    /* An invalid count is rejected by the backend and relayed verbatim */
    proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "-2");
    CHECK(r[0] == '-', "SLOWLOG GET -2: relays backend error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "abc");
    CHECK(r[0] == '-', "SLOWLOG GET abc: relays backend error", pass, fail);

    /* SLOWLOG GET merge across nodes: a ~20ms Lua busy loop (os.clock(),
     * since TIME is cached inside a script) exceeds the
     * default slowlog-log-slower-than (10000us), so each EVAL leaves one
     * entry on the node owning its key. Node 0 → Node 1 → Node 2, >1s apart
     * so the (second-resolution) timestamps are strictly increasing. */
    {
        const char *busy = "local s=os.clock() repeat until os.clock()-s >= 0.02 return 1";
        proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "RESET");
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k1"); /* Node 0 */
        usleep(1100000);
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k3"); /* Node 1 */
        usleep(1100000);
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k2"); /* Node 2 */

        proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "LEN");
        long long len = atoll(r + 1);
        CHECK(r[0] == ':' && len >= 3, "SLOWLOG LEN: sums entries from all 3 nodes (>= 3)", pass, fail);

        /* GET -1: every entry, merged newest first */
        proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "-1");
        int n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == len, "SLOWLOG GET -1: entry count equals SLOWLOG LEN", pass, fail);
        int ordered = n > 0;
        long long prev = LLONG_MAX, ts = 0;
        for (int i = 0; i < n; i++) {
            if (read_slowlog_entry(fd, &ts, "") < 0 || ts > prev) ordered = 0;
            prev = ts;
        }
        CHECK(ordered, "SLOWLOG GET -1: merged entries ordered newest first", pass, fail);

        /* GET 2: truncated to the two newest entries, Node 2 then Node 1 */
        proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "2");
        n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == 2, "SLOWLOG GET 2: truncated to 2 entries", pass, fail);
        int first = 0, second = 0;
        if (n == 2) {
            first = read_slowlog_entry(fd, &ts, "tc:gn:touch:k2");
            second = read_slowlog_entry(fd, &ts, "tc:gn:touch:k3");
        } else {
            for (int i = 0; i < n; i++) resp_skip_reply(fd);
        }
        CHECK(first == 1 && second == 1, "SLOWLOG GET 2: newest entries are Node 2's then Node 1's EVAL", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 3, "SLOWLOG", "GET", "0");
        CHECK(strcmp(r, "*0") == 0, "SLOWLOG GET 0: returns empty array", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 2, "SLOWLOG", "RESET");
    }

    /* ── COMMANDLOG ──────────────────────────────────────────────────── */
    /* COMMANDLOG HELP has no key — routed to a single master node */
    resp_send(fd, 2, "COMMANDLOG", "HELP");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "COMMANDLOG HELP: returns non-empty help array", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_next_str(fd, r, sizeof(r));
    }

    /* COMMANDLOG RESET/LEN take a required type argument: slow, large-request,
     * large-reply. Each fans out to all master nodes. */
    {
        const char *types[] = {"slow", "large-request", "large-reply"};
        for (int i = 0; i < 3; i++) {
            proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", types[i]);
            CHECK(strcmp(r, "+OK") == 0, "COMMANDLOG RESET: returns +OK", pass, fail);

            proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "LEN", types[i]);
            CHECK(strcmp(r, ":0") == 0, "COMMANDLOG LEN: returns 0 immediately after RESET", pass, fail);
        }
    }

    /* An invalid type is rejected by the backend and relayed verbatim,
     * not silently aggregated into a number or +OK */
    proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "LEN", "foo");
    CHECK(r[0] == '-', "COMMANDLOG LEN: invalid type relays backend error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", "foo");
    CHECK(r[0] == '-', "COMMANDLOG RESET: invalid type relays backend error", pass, fail);

    /* COMMANDLOG GET <count> slow — same busy-loop EVALs as the SLOWLOG GET
     * test: Node 0 → Node 1 → Node 2, >1s apart, merged newest first. */
    {
        const char *busy = "local s=os.clock() repeat until os.clock()-s >= 0.02 return 1";
        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", "slow");
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k1"); /* Node 0 */
        usleep(1100000);
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k3"); /* Node 1 */
        usleep(1100000);
        proxy_cmd(fd, r, sizeof(r), 4, "EVAL", busy, "1", "tc:gn:touch:k2"); /* Node 2 */

        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "LEN", "slow");
        long long len = atoll(r + 1);
        CHECK(r[0] == ':' && len >= 3, "COMMANDLOG LEN slow: sums entries from all 3 nodes (>= 3)", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "-1", "slow");
        int n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == len, "COMMANDLOG GET -1 slow: entry count equals COMMANDLOG LEN", pass, fail);
        int ordered = n > 0;
        long long prev = LLONG_MAX, ts = 0;
        for (int i = 0; i < n; i++) {
            if (read_slowlog_entry(fd, &ts, "") < 0 || ts > prev) ordered = 0;
            prev = ts;
        }
        CHECK(ordered, "COMMANDLOG GET -1 slow: merged entries ordered newest first", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "2", "slow");
        n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == 2, "COMMANDLOG GET 2 slow: truncated to 2 entries", pass, fail);
        int first = 0, second = 0;
        if (n == 2) {
            first = read_slowlog_entry(fd, &ts, "tc:gn:touch:k2");
            second = read_slowlog_entry(fd, &ts, "tc:gn:touch:k3");
        } else {
            for (int i = 0; i < n; i++) resp_skip_reply(fd);
        }
        CHECK(first == 1 && second == 1, "COMMANDLOG GET 2 slow: newest entries are Node 2's then Node 1's EVAL", pass,
              fail);

        proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "0", "slow");
        CHECK(strcmp(r, "*0") == 0, "COMMANDLOG GET 0 slow: returns empty array", pass, fail);
    }

    /* COMMANDLOG GET large-request / large-reply: a 2MB SET and GET on
     * Node 0 and Node 1 exceed the default 1MB thresholds. */
    {
        const size_t big_len = 2 * 1024 * 1024;
        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", "large-request");
        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", "large-reply");
        send_big_set(fd, "tc:gn:cl:big0", big_len, r, sizeof(r));
        send_big_set(fd, "tc:gn:cl:big1", big_len, r, sizeof(r));
        proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:gn:cl:big0"); /* 2MB reply, overflow discarded */
        proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:gn:cl:big1");

        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "LEN", "large-request");
        long long len = atoll(r + 1);
        CHECK(r[0] == ':' && len >= 2, "COMMANDLOG LEN large-request: >= 2 after two 2MB SETs", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "1", "large-request");
        int n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == 1, "COMMANDLOG GET 1 large-request: truncated to 1 entry", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);

        proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "LEN", "large-reply");
        len = atoll(r + 1);
        CHECK(r[0] == ':' && len >= 2, "COMMANDLOG LEN large-reply: >= 2 after two 2MB GETs", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "-1", "large-reply");
        n = r[0] == '*' ? atoi(r + 1) : -1;
        CHECK(n == len, "COMMANDLOG GET -1 large-reply: entry count equals COMMANDLOG LEN", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);

        proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:cl:big0");
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:cl:big1");
    }

    /* Invalid type / count / arity are rejected by the backend and relayed */
    proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "5", "foo");
    CHECK(r[0] == '-', "COMMANDLOG GET: invalid type relays backend error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "COMMANDLOG", "GET", "abc", "slow");
    CHECK(r[0] == '-', "COMMANDLOG GET: invalid count relays backend error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "GET", "5");
    CHECK(r[0] == '-', "COMMANDLOG GET: missing type relays backend error", pass, fail);

    {
        const char *types[] = {"slow", "large-request", "large-reply"};
        for (int i = 0; i < 3; i++) proxy_cmd(fd, r, sizeof(r), 3, "COMMANDLOG", "RESET", types[i]);
    }

    /* ── TIME ────────────────────────────────────────────────────────── */
    /* TIME is answered directly by the proxy without contacting a backend
     * node; it returns a 2-element array: unix seconds and microseconds */
    resp_send(fd, 1, "TIME");
    resp_next_str(fd, r, sizeof(r)); /* *2 header */
    CHECK(strcmp(r, "*2") == 0, "TIME: returns a 2-element array", pass, fail);
    {
        char sec[64], usec[64];
        resp_next_str(fd, sec, sizeof(sec));
        resp_next_str(fd, usec, sizeof(usec));
        long long sv = atoll(sec), uv = atoll(usec);
        CHECK(sv > 1700000000LL, "TIME: seconds look like a plausible unix timestamp", pass, fail);
        CHECK(uv >= 0 && uv < 1000000LL, "TIME: microseconds is in [0, 1000000)", pass, fail);
    }

    /* ── PING ────────────────────────────────────────────────────────── */
    /* PING is answered directly by the proxy without contacting a backend
     * node; with no argument it returns +PONG, with an argument it echoes
     * the argument back as a bulk string */
    /* 
    proxy_cmd(fd, r, sizeof(r), 1, "PING");
    CHECK(strcmp(r, "+PONG") == 0, "PING: no-arg returns +PONG", pass, fail);

    resp_send(fd, 2, "PING", "hello");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "$5") == 0, "PING hello: returns bulk string header", pass, fail);
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "hello") == 0, "PING hello: echoes the message back", pass, fail);
    */
    /* ── ECHO ────────────────────────────────────────────────────────── */
    /* ECHO is answered directly by the proxy without contacting a backend
     * node; it echoes the given message back as a bulk string */
    /* 
    resp_send(fd, 2, "ECHO", "hello world");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "$11") == 0, "ECHO: returns bulk string header", pass, fail);
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "hello world") == 0, "ECHO: echoes the message back", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "ECHO");
    CHECK(r[0] == '-', "ECHO: missing argument returns an error", pass, fail);
    */
    /* ── RANDOMKEY ───────────────────────────────────────────────────── */
    /* ensure at least one key exists, then verify no error response */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:randkey:seed", "exists");
    proxy_cmd(fd, r, sizeof(r), 1, "RANDOMKEY");
    CHECK(r[0] != '-', "RANDOMKEY: returns key or nil (not an error)", pass, fail);

    /* ── LOLWUT ──────────────────────────────────────────────────────── */
    /* LOLWUT has no key — routed to a single arbitrary master node */
    proxy_cmd(fd, r, sizeof(r), 1, "LOLWUT");
    CHECK(r[0] != '-', "LOLWUT: returns a reply (not an error)", pass, fail);

    /* ── MODULE LIST ─────────────────────────────────────────────────── */
    /* MODULE LIST has no key — routed to a single arbitrary master node,
     * the raw backend reply is relayed back to the client unchanged */
    resp_send(fd, 2, "MODULE", "LIST");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*', "MODULE LIST: returns an array reply", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── COMMAND HELP ────────────────────────────────────────────────── */
    /* COMMAND HELP has no key — routed to a single arbitrary master node,
     * the raw backend reply is relayed back to the client unchanged */
    resp_send(fd, 2, "COMMAND", "HELP");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*', "COMMAND HELP: returns an array reply", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── COMMAND GETKEYS ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:cmdgetkeys", "val");

    resp_send(fd, 4, "COMMAND", "GETKEYS", "GET", "tc:gn:cmdgetkeys");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*', "COMMAND GETKEYS GET key: returns an array reply", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        int found = resp_array_contains(fd, n, "tc:gn:cmdgetkeys");
        CHECK(found, "COMMAND GETKEYS GET key: array contains the key", pass, fail);
    }

    resp_send(fd, 3, "COMMAND", "GETKEYS", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-', "COMMAND GETKEYS: unsupported command returns an error", pass, fail);

    /* ── COMMAND GETKEYSANDFLAGS ─────────────────────────────────────── */
    resp_send(fd, 4, "COMMAND", "GETKEYSANDFLAGS", "GET", "tc:gn:cmdgetkeys");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*', "COMMAND GETKEYSANDFLAGS GET key: returns an array reply", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    resp_send(fd, 3, "COMMAND", "GETKEYSANDFLAGS", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-', "COMMAND GETKEYSANDFLAGS: unsupported command returns an error", pass, fail);

    /* ── COMMAND INFO ────────────────────────────────────────────────── */
    /* forwarded to a single arbitrary master node, but the reply is
     * post-processed so it only reflects commands this proxy supports */
    resp_send(fd, 3, "COMMAND", "INFO", "GET");
    resp_next_str(fd, r, sizeof(r)); /* *1 */
    CHECK(strcmp(r, "*1") == 0, "COMMAND INFO GET: returns a single-element array", pass, fail);
    resp_next_str(fd, r, sizeof(r)); /* *N: GET's info sub-array header */
    CHECK(r[0] == '*', "COMMAND INFO GET: element is an array", pass, fail);
    if (r[0] == '*') {
        int sub_n = atoi(r + 1);
        resp_next_str(fd, r, sizeof(r)); /* name field */
        CHECK(strcmp(r, "get") == 0, "COMMAND INFO GET: name field is 'get'", pass, fail);
        for (int i = 1; i < sub_n; i++) resp_skip_reply(fd);
    }

    resp_send(fd, 4, "COMMAND", "INFO", "GET", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r)); /* *2 */
    CHECK(strcmp(r, "*2") == 0, "COMMAND INFO GET NOTACOMMAND: returns a two-element array", pass, fail);
    resp_skip_reply(fd);             /* GET's real info array */
    resp_next_str(fd, r, sizeof(r)); /* *-1 for the unsupported name */
    CHECK(strcmp(r, "*-1") == 0, "COMMAND INFO GET NOTACOMMAND: unsupported name is nil", pass, fail);

    resp_send(fd, 3, "COMMAND", "INFO", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r)); /* *1 */
    CHECK(strcmp(r, "*1") == 0, "COMMAND INFO NOTACOMMAND: returns a single-element array", pass, fail);
    resp_next_str(fd, r, sizeof(r)); /* *-1 */
    CHECK(strcmp(r, "*-1") == 0, "COMMAND INFO NOTACOMMAND: element is nil", pass, fail);

    resp_send(fd, 2, "COMMAND", "INFO");
    resp_next_str(fd, r, sizeof(r)); /* *N: filtered down to proxy-supported commands */
    CHECK(r[0] == '*', "COMMAND INFO (no args): returns an array reply", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        CHECK(n > 0, "COMMAND INFO (no args): array is non-empty", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── COMMAND DOCS ────────────────────────────────────────────────── */
    /* unlike COMMAND INFO, real Valkey/Redis semantics for DOCS drop an
     * unknown command name from the output entirely rather than emitting
     * nil, so the proxy filters (name, docs) pairs down to what it
     * supports for both the with-args and no-args forms. */
    resp_send(fd, 3, "COMMAND", "DOCS", "GET");
    resp_next_str(fd, r, sizeof(r)); /* *2: [name, docs] for GET */
    CHECK(strcmp(r, "*2") == 0, "COMMAND DOCS GET: returns a two-element array", pass, fail);
    resp_next_str(fd, r, sizeof(r)); /* name field */
    CHECK(strcmp(r, "get") == 0, "COMMAND DOCS GET: name field is 'get'", pass, fail);
    resp_skip_reply(fd); /* GET's docs map */

    resp_send(fd, 4, "COMMAND", "DOCS", "GET", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r)); /* *2: NOTACOMMAND dropped, only GET's pair remains */
    CHECK(strcmp(r, "*2") == 0, "COMMAND DOCS GET NOTACOMMAND: unsupported name is dropped, not nil-padded", pass,
          fail);
    resp_skip_reply(fd); /* name */
    resp_skip_reply(fd); /* docs */

    resp_send(fd, 3, "COMMAND", "DOCS", "NOTACOMMAND");
    resp_next_str(fd, r, sizeof(r)); /* *0: nothing left */
    CHECK(strcmp(r, "*0") == 0, "COMMAND DOCS NOTACOMMAND: returns an empty array", pass, fail);

    resp_send(fd, 2, "COMMAND", "DOCS");
    resp_next_str(fd, r, sizeof(r)); /* *2N: filtered down to proxy-supported commands */
    CHECK(r[0] == '*', "COMMAND DOCS (no args): returns an array reply", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        CHECK(n > 0 && n % 2 == 0, "COMMAND DOCS (no args): array is non-empty and even-length", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── COMMAND (no subcommand) ─────────────────────────────────────── */
    /* bare COMMAND has the same reply shape as COMMAND INFO with no args */
    resp_send(fd, 1, "COMMAND");
    resp_next_str(fd, r, sizeof(r)); /* *N: filtered down to proxy-supported commands */
    CHECK(r[0] == '*', "COMMAND (no subcommand): returns an array reply", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        CHECK(n > 0, "COMMAND (no subcommand): array is non-empty", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── COMMAND LIST ────────────────────────────────────────────────── */
    /* forwarded to a single arbitrary master node, but the reply is
     * post-processed so it only lists commands this proxy supports */
    resp_send(fd, 2, "COMMAND", "LIST");
    resp_next_str(fd, r, sizeof(r)); /* *N: filtered down to proxy-supported commands */
    CHECK(r[0] == '*', "COMMAND LIST: returns an array reply", pass, fail);
    int cmd_list_n = 0;
    if (r[0] == '*') {
        cmd_list_n = atoi(r + 1);
        CHECK(cmd_list_n > 0, "COMMAND LIST: array is non-empty", pass, fail);
        CHECK(resp_array_contains(fd, cmd_list_n, "get"), "COMMAND LIST: array contains 'get'", pass, fail);
    }

    /* ── COMMAND COUNT ───────────────────────────────────────────────── */
    /* a COMMAND LIST is sent to a master node on the client's behalf; the
     * reply is the count of names in it that this proxy supports, which
     * must equal the length of the COMMAND LIST array above. */
    resp_send(fd, 2, "COMMAND", "COUNT");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == ':', "COMMAND COUNT: returns an integer reply", pass, fail);
    if (r[0] == ':') {
        long long n = atoll(r + 1);
        CHECK(n > 0, "COMMAND COUNT: count is positive", pass, fail);
        CHECK(n == cmd_list_n, "COMMAND COUNT: matches COMMAND LIST array length", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:cmdgetkeys");

    /* ── CLIENT ──────────────────────────────────────────────────────── */
    /* CLIENT HELP is answered directly by the proxy, no backend round-trip */
    resp_send(fd, 2, "CLIENT", "HELP");
    resp_next_str(fd, r, sizeof(r)); /* *N header */
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "CLIENT HELP: returns non-empty help array", pass, fail);
    {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_next_str(fd, r, sizeof(r));
    }

    /* CLIENT ID is assigned once at accept() time: stable across repeated
     * calls on the same connection, and distinct/monotonic across connections */
    {
        long long id1a, id1b, id2;

        proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "ID");
        CHECK(r[0] == ':', "CLIENT ID: returns an integer reply", pass, fail);
        id1a = atoll(r + 1);

        proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "ID");
        id1b = atoll(r + 1);
        CHECK(id1a == id1b, "CLIENT ID: stable across repeated calls on same connection", pass, fail);

        int fd2 = proxy_connect(host, port);
        CHECK(fd2 >= 0, "CLIENT ID: second connection established", pass, fail);
        if (fd2 >= 0) {
            proxy_cmd(fd2, r, sizeof(r), 2, "CLIENT", "ID");
            id2 = atoll(r + 1);
            CHECK(id2 > id1a, "CLIENT ID: a later connection gets a larger id", pass, fail);

            /* CLIENT GETNAME/SETNAME are per-connection state: fd2 must not
             * see anything set on fd */
            proxy_cmd(fd2, r, sizeof(r), 2, "CLIENT", "GETNAME");
            CHECK(strcmp(r, "") == 0, "CLIENT GETNAME: empty on a fresh connection that never called SETNAME", pass,
                  fail);
            close(fd2);
        }
    }

    /* CLIENT GETNAME/SETNAME round-trip */
    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "") == 0, "CLIENT GETNAME: empty before SETNAME", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "CLIENT", "SETNAME", "tc:gn:myconn");
    CHECK(strcmp(r, "+OK") == 0, "CLIENT SETNAME: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc:gn:myconn") == 0, "CLIENT GETNAME: returns the name set by SETNAME", pass, fail);

    /* names may not contain spaces or newlines */
    proxy_cmd(fd, r, sizeof(r), 3, "CLIENT", "SETNAME", "bad name");
    CHECK(r[0] == '-', "CLIENT SETNAME: name with a space is rejected", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "CLIENT", "SETNAME", "bad\nname");
    CHECK(r[0] == '-', "CLIENT SETNAME: name with a newline is rejected", pass, fail);

    /* a rejected SETNAME must not clobber the previously set name */
    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc:gn:myconn") == 0, "CLIENT GETNAME: unchanged after a rejected SETNAME", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "SETNAME");
    CHECK(r[0] == '-', "CLIENT SETNAME: missing name argument returns an error", pass, fail);

    /* missing/unknown subcommands */
    proxy_cmd(fd, r, sizeof(r), 1, "CLIENT");
    CHECK(r[0] == '-', "CLIENT: missing subcommand returns an error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "NOSUCHSUBCMD");
    CHECK(r[0] == '-', "CLIENT: unknown subcommand returns an error", pass, fail);

    /* CLIENT INFO: answered locally, one bulk-string line of key=value pairs */
    proxy_cmd(fd, r, sizeof(r), 2, "CLIENT", "INFO");
    CHECK(r[0] != '-', "CLIENT INFO: succeeds", pass, fail);
    CHECK(strstr(r, "id=") != NULL, "CLIENT INFO: contains id=", pass, fail);
    CHECK(strstr(r, "fd=") != NULL, "CLIENT INFO: contains fd=", pass, fail);
    CHECK(strstr(r, "addr=") != NULL, "CLIENT INFO: contains addr=", pass, fail);
    CHECK(strstr(r, "laddr=") != NULL, "CLIENT INFO: contains laddr=", pass, fail);
    CHECK(strstr(r, "cmd=client|info") != NULL, "CLIENT INFO: cmd shows client|info", pass, fail);
    CHECK(strstr(r, "multi=-1") != NULL, "CLIENT INFO: multi=-1 outside a transaction", pass, fail);
    CHECK(strstr(r, "watch=0") != NULL, "CLIENT INFO: watch=0 by default", pass, fail);
    CHECK(strstr(r, "sub=0 psub=0 ssub=0") != NULL, "CLIENT INFO: sub/psub/ssub default to 0", pass, fail);
    CHECK(strstr(r, "name=tc:gn:myconn") != NULL, "CLIENT INFO: reflects name set by SETNAME", pass, fail);

    /* cleanup */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:obj");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:exists");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:expire");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:expireat");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:expiretime");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ttl");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:pttl");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:persist");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:pexpire");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:pexpireat");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:pexpiretime");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:type");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rn}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rn}dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rnx}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rnx}src2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:rnx}dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:cp}src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:gn:cp}dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dump:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:dump:dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:touch");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:keys:alpha");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:keys:beta");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:keys:nd0");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:keys:nd1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:keys:nd2x");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:randkey:seed");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ul:nd0");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ul:nd1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:ul:nd3");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd0");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:scan:nd2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:del:k1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:del:k2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:del:k3");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:cs:foo");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:cs:bar");

    /* ── PROXY POOLS / PROXY HELP (answered by the proxy itself) ─────── */
    /* ~330 bytes per pool line, 5 lines per node: 1MB covers well over 500 nodes. */
    const int pools_cap = 1 << 20;
    char *pools = malloc((size_t)pools_cap);
    int plen = pools ? resp_send_raw_reply(fd, pools, pools_cap, 2, "PROXY", "POOLS") : -1;
    CHECK(plen > 0 && pools[0] == '$' && strstr(pools, "# Pools"),
          "PROXY POOLS: returns a bulk string with a Pools section", pass, fail);
    CHECK(plen > 0 && strstr(pools, "_normal:addr=") && strstr(pools, ",open="),
          "PROXY POOLS: reports the normal pool of each node", pass, fail);
    unsigned long long acq_before = plen > 0 ? sum_pool_field(pools, "acquires=") : 0;
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:gn:pools", "v");
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:gn:pools");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:gn:pools");
    plen = pools ? resp_send_raw_reply(fd, pools, pools_cap, 2, "PROXY", "POOLS") : -1;
    CHECK(plen > 0 && sum_pool_field(pools, "acquires=") >= acq_before + 3,
          "PROXY POOLS: acquires grows by at least 3 after SET/GET/DEL", pass, fail);

    plen = pools ? resp_send_raw_reply(fd, pools, pools_cap, 2, "PROXY", "HELP") : -1;
    CHECK(plen > 0 && pools[0] == '*' && strstr(pools, "POOLS"), "PROXY HELP: lists the POOLS subcommand", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "PROXY", "NOSUCH");
    CHECK(strstr(r, "unknown PROXY subcommand") != NULL, "PROXY NOSUCH: unknown subcommand error", pass, fail);

    /* ── INFO (answered as one standalone server, see src/info.c) ─────── */
    char *info = pools;
    int ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 1, "INFO") : -1;
    CHECK(ilen > 0 && info[0] == '$' && strstr(info, "# Server\r\n") && strstr(info, "# Clients\r\n") &&
              strstr(info, "# Cluster\r\ncluster_enabled:0\r\n") && strstr(info, "# Keyspace\r\n") &&
              strstr(info, "# Proxy\r\n") && !strstr(info, "# Memory\r\n"),
          "INFO: default sections are server, clients, cluster, keyspace and proxy", pass, fail);
    CHECK(ilen > 0 && strstr(info, "server_mode:standalone\r\n") && strstr(info, "redis_version:") &&
              strstr(info, "proxy_version:") &&
              (strstr(info, "\nserver_name:valkey\r\n") != NULL) == (strstr(info, "\nvalkey_version:") != NULL),
          "INFO: server section reports a standalone server and versions", pass, fail);

    for (int i = 0; i < 12; i++) {
        char key[32];
        snprintf(key, sizeof(key), "tc:gn:info:%d", i);
        proxy_cmd(fd, r, sizeof(r), 3, "SET", key, "v");
    }
    proxy_cmd(fd, r, sizeof(r), 1, "DBSIZE");
    long long dbsize = atoll(r + 1);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "INFO", "keyspace") : -1;
    const char *db0 = ilen > 0 ? strstr(info, "db0:keys=") : NULL;
    CHECK(db0 && atoll(db0 + 9) == dbsize && dbsize >= 12, "INFO keyspace: keys summed across shards equals DBSIZE",
          pass, fail);
    for (int i = 0; i < 12; i++) {
        char key[32];
        snprintf(key, sizeof(key), "tc:gn:info:%d", i);
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", key);
    }

    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 3, "INFO", "CLUSTER", "Server") : -1;
    const char *srv = ilen > 0 ? strstr(info, "# Server\r\n") : NULL;
    const char *clu = ilen > 0 ? strstr(info, "# Cluster\r\n") : NULL;
    CHECK(srv && clu && srv < clu && !strstr(info, "# Keyspace"),
          "INFO CLUSTER Server: case-insensitive, canonical order, only those sections", pass, fail);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "INFO", "nosuchsection") : -1;
    CHECK(ilen > 0 && !strcmp(info, "$0\r\n\r\n"), "INFO nosuchsection: empty reply", pass, fail);

    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "INFO", "all") : -1;
    CHECK(ilen > 0 && strstr(info, "# TLS\r\n") && strstr(info, "# Memory\r\nshard") &&
              strstr(info, "# Commandstats\r\n") && !strstr(info, "# Pools\r\n"),
          "INFO all: adds tls and the per-shard sections, not pools", pass, fail);
    CHECK(ilen > 0 && strstr(info, "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"),
          "INFO all: adds a local replication section with HELLO's role", pass, fail);
    CHECK(ilen > 0 && strstr(info, ",status=ok\r\n") && strstr(info, "_used_memory:"),
          "INFO all: per-shard sections label each shard and prefix its lines", pass, fail);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "INFO", "everything") : -1;
    CHECK(ilen > 0 && strstr(info, "# Pools\r\n"), "INFO everything: adds pools", pass, fail);

    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 3, "PROXY", "INFO", "keyspace") : -1;
    CHECK(ilen > 0 && info[0] == '*' && strstr(info, "\r\n0:") && strstr(info, "# Keyspace"),
          "PROXY INFO keyspace: each shard's own reply, labelled", pass, fail);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 5, "PROXY", "INFO", "server", "SHARD", "0") : -1;
    CHECK(ilen > 0 && !strncmp(info, "*2\r\n", 4) && strstr(info, "_mode:cluster"),
          "PROXY INFO server SHARD 0: one shard's raw reply", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "INFO", "server", "SHARD", "999");
    CHECK(strstr(r, "unknown shard") != NULL, "PROXY INFO SHARD 999: unknown shard error", pass, fail);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 5, "PROXY", "INFO", "SHARD", "0", "server") : -1;
    CHECK(ilen > 0 && !strncmp(info, "*2\r\n", 4) && strstr(info, "_mode:cluster"),
          "PROXY INFO SHARD 0 server: SHARD may come before the sections", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 3, "PROXY", "INFO", "SHARD");
    CHECK(strstr(r, "syntax error") != NULL, "PROXY INFO SHARD: missing shard is a syntax error", pass, fail);

    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "HELLO", "2") : -1;
    CHECK(ilen > 0 && strstr(info, "$4\r\nmode\r\n$10\r\nstandalone\r\n"), "HELLO: mode is standalone, matching INFO",
          pass, fail);
    CHECK(ilen > 0 && strstr(info, "$6\r\nserver\r\n$12\r\nvalkey-proxy\r\n"), "HELLO: server is valkey-proxy", pass,
          fail);

    /* HELLO version is the one INFO server reports. */
    char hello_ver[64] = "", info_ver[64] = "";
    const char *hv = ilen > 0 ? strstr(info, "$7\r\nversion\r\n$") : NULL;
    if (hv) {
        hv = strchr(hv + 13, '\n');
        if (hv) sscanf(hv + 1, "%63[^\r]", hello_ver);
    }
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "INFO", "server") : -1;
    const char *iv = ilen > 0 ? strstr(info, "\nvalkey_version:") : NULL;
    if (!iv && ilen > 0) iv = strstr(info, "\nredis_version:");
    if (iv) sscanf(strchr(iv, ':') + 1, "%63[^\r]", info_ver);
    CHECK(hello_ver[0] && !strcmp(hello_ver, info_ver), "HELLO: version matches INFO server", pass, fail);

    /* Every module the first shard has loaded shows up in HELLO (the test
     * cluster's masters all load the same modules). */
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 5, "PROXY", "INFO", "modules", "SHARD", "0") : -1;
    char shard_mods[2048] = "";
    if (ilen > 0) snprintf(shard_mods, sizeof(shard_mods), "%s", info);
    ilen = info ? resp_send_raw_reply(fd, info, pools_cap, 2, "HELLO", "2") : -1;
    int mods_ok = ilen > 0 && shard_mods[0] != '\0', compared = 0;
    for (const char *m = strstr(shard_mods, "module:name="); m && mods_ok; m = strstr(m + 1, "module:name=")) {
        char want[96], name[64];
        if (sscanf(m + 12, "%63[^,\r]", name) != 1) break;
        snprintf(want, sizeof(want), "$4\r\nname\r\n$%zu\r\n%s\r\n", strlen(name), name);
        if (!strstr(info, want)) mods_ok = 0;
        compared++;
    }
    /* A backend without modules (Valkey before 9.0) must give an empty
     * list, so the check never passes without comparing anything. */
    if (mods_ok && compared == 0) mods_ok = strstr(info, "$7\r\nmodules\r\n*0\r\n") != NULL;
    CHECK(mods_ok, "HELLO: modules are the ones the backends have loaded", pass, fail);
    free(pools);

    for (int i = 0; i < _ntr; i++) printf("  [%s] %s\n", _tr[i].ok ? "PASS" : "FAIL", _tr[i].label);

    close(fd);
}
