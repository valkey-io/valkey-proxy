#include "test_common.h"
#include <time.h>

void run_hash_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Hash Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    char r2[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── HSET / HGET ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:main", "f1", "hello");
    CHECK(strcmp(r, ":1") == 0, "HSET: new field returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:main", "f1", "world");
    CHECK(strcmp(r, ":0") == 0, "HSET: update existing field returns 0", pass, fail);

    /* set f1=hello and add f2=world in one call */
    proxy_cmd(fd, r, sizeof(r), 6, "HSET", "tc:hs:main", "f1", "hello", "f2", "world");
    CHECK(strcmp(r, ":1") == 0, "HSET: 2 fields, 1 new returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "HGET", "tc:hs:main", "f1");
    CHECK(strcmp(r, "hello") == 0, "HGET: existing field returns value", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "HGET", "tc:hs:main", "noexist");
    CHECK(strcmp(r, "(nil)") == 0, "HGET: non-existent field returns nil", pass, fail);

    /* ── HSETNX ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSETNX", "tc:hs:main", "f3", "new");
    CHECK(strcmp(r, ":1") == 0, "HSETNX: new field returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "HSETNX", "tc:hs:main", "f3", "other");
    CHECK(strcmp(r, ":0") == 0, "HSETNX: existing field returns 0", pass, fail);

    /* ── HMSET / HMGET ────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 6, "HMSET", "tc:hs:hmget", "a", "va", "b", "vb");
    CHECK(strcmp(r, "+OK") == 0, "HMSET: returns +OK", pass, fail);

    resp_send(fd, 5, "HMGET", "tc:hs:hmget", "a", "b", "noexist");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "HMGET: returns 3-element array", pass, fail);
    {
        char v[128];
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "va") == 0, "HMGET: first field value correct", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "vb") == 0, "HMGET: second field value correct", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "(nil)") == 0, "HMGET: missing field returns nil", pass, fail);
    }

    /* ── HEXISTS ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "HEXISTS", "tc:hs:main", "f1");
    CHECK(strcmp(r, ":1") == 0, "HEXISTS: existing field returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "HEXISTS", "tc:hs:main", "noexist");
    CHECK(strcmp(r, ":0") == 0, "HEXISTS: non-existent field returns 0", pass, fail);

    /* ── HLEN ─────────────────────────────────────────────────────────── */
    /* tc:hs:main has f1, f2, f3 at this point */
    proxy_cmd(fd, r, sizeof(r), 2, "HLEN", "tc:hs:main");
    CHECK(strcmp(r, ":3") == 0, "HLEN: 3-field hash returns 3", pass, fail);

    /* ── HSTRLEN ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "HSTRLEN", "tc:hs:main", "f1");
    CHECK(strcmp(r, ":5") == 0, "HSTRLEN: 'hello' has length 5", pass, fail);

    /* ── HKEYS ────────────────────────────────────────────────────────── */
    resp_send(fd, 2, "HKEYS", "tc:hs:main");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 3, "HKEYS: 3-field hash returns 3 keys", pass, fail);
        int has_f1 = resp_array_contains(fd, n, "f1");
        CHECK(has_f1, "HKEYS: contains field f1", pass, fail);
    }

    resp_send(fd, 2, "HKEYS", "tc:hs:main");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_f2 = resp_array_contains(fd, n, "f2");
        CHECK(has_f2, "HKEYS: contains field f2", pass, fail);
    }

    /* ── HVALS ────────────────────────────────────────────────────────── */
    resp_send(fd, 2, "HVALS", "tc:hs:main");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 3, "HVALS: 3-field hash returns 3 values", pass, fail);
        int has_hello = resp_array_contains(fd, n, "hello");
        CHECK(has_hello, "HVALS: contains value 'hello'", pass, fail);
    }

    /* ── HGETALL ──────────────────────────────────────────────────────── */
    resp_send(fd, 2, "HGETALL", "tc:hs:main");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*6") == 0, "HGETALL: 3-field hash returns *6", pass, fail);
    {
        int has_f1 = resp_array_contains(fd, 6, "f1");
        CHECK(has_f1, "HGETALL: contains field name f1", pass, fail);
    }

    /* ── HDEL ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "HDEL", "tc:hs:main", "f3");
    CHECK(strcmp(r, ":1") == 0, "HDEL: existing field returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "HDEL", "tc:hs:main", "noexist");
    CHECK(strcmp(r, ":0") == 0, "HDEL: non-existent field returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 6, "HSET", "tc:hs:main", "d1", "v1", "d2", "v2");
    proxy_cmd(fd, r, sizeof(r), 4, "HDEL", "tc:hs:main", "d1", "d2");
    CHECK(strcmp(r, ":2") == 0, "HDEL: delete 2 fields at once returns 2", pass, fail);
    /* tc:hs:main now has f1=hello, f2=world */

    /* ── HINCRBY ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HINCRBY", "tc:hs:incr", "counter", "5");
    CHECK(strcmp(r, ":5") == 0, "HINCRBY: new field += 5 returns 5", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "HINCRBY", "tc:hs:incr", "counter", "3");
    CHECK(strcmp(r, ":8") == 0, "HINCRBY: += 3 returns 8", pass, fail);

    /* ── HINCRBYFLOAT ─────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HINCRBYFLOAT", "tc:hs:incr", "score", "1.5");
    CHECK(strcmp(r, "1.5") == 0, "HINCRBYFLOAT: new field += 1.5 returns 1.5", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "HINCRBYFLOAT", "tc:hs:incr", "score", "0.1");
    {
        double v = atof(r);
        CHECK(v > 1.59 && v < 1.61, "HINCRBYFLOAT: 1.5 + 0.1 ≈ 1.6", pass, fail);
    }

    /* ── HRANDFIELD ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "HRANDFIELD", "tc:hs:main");
    CHECK(r[0] != '-', "HRANDFIELD: returns field name (not error)", pass, fail);

    resp_send(fd, 3, "HRANDFIELD", "tc:hs:main", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "HRANDFIELD count=2: returns *2 array", pass, fail);
    {
        char tmp[128];
        for (int i = 0; i < 2; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    resp_send(fd, 4, "HRANDFIELD", "tc:hs:main", "2", "WITHVALUES");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*4") == 0, "HRANDFIELD WITHVALUES count=2: returns *4 array", pass, fail);
    {
        char tmp[128];
        for (int i = 0; i < 4; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    /* ── HSCAN ────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "HSCAN", "tc:hs:main", "0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "HSCAN: returns outer *2 array", pass, fail);
    {
        char cur[64];
        resp_next_str(fd, cur, sizeof(cur)); /* cursor value */
        resp_next_str(fd, r, sizeof(r));     /* "*N" inner array header */
        int n = atoi(r + 1);
        CHECK(n >= 0, "HSCAN: inner array count is non-negative", pass, fail);
        char tmp[128];
        for (int i = 0; i < n; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    /* ── HEXPIRE / HTTL ───────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:exp", "ef1", "expval");

    proxy_cmd(fd, r, sizeof(r), 6, "HEXPIRE", "tc:hs:exp", "100", "FIELDS", "1", "ef1");
    CHECK(strcmp(r, "*1") == 0, "HEXPIRE: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, ":1") == 0, "HEXPIRE: field expiry set returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "HTTL", "tc:hs:exp", "FIELDS", "1", "ef1");
    CHECK(strcmp(r, "*1") == 0, "HTTL: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    {
        long long v = atoll(r2 + 1);
        CHECK(r2[0] == ':' && v > 0 && v <= 100, "HTTL: TTL in (0, 100]", pass, fail);
    }

    /* ── HPEXPIRE / HPTTL ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:pexp", "pf1", "pexpval");

    proxy_cmd(fd, r, sizeof(r), 6, "HPEXPIRE", "tc:hs:pexp", "100000", "FIELDS", "1", "pf1");
    CHECK(strcmp(r, "*1") == 0, "HPEXPIRE: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, ":1") == 0, "HPEXPIRE: field expiry set returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "HPTTL", "tc:hs:pexp", "FIELDS", "1", "pf1");
    CHECK(strcmp(r, "*1") == 0, "HPTTL: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    {
        long long v = atoll(r2 + 1);
        CHECK(r2[0] == ':' && v > 0 && v <= 100000, "HPTTL: TTL in (0, 100000]", pass, fail);
    }

    /* ── HEXPIREAT / HEXPIRETIME ──────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:expat", "eaf1", "eatval");
    {
        long long ts = (long long)time(NULL) + 300;
        char ts_str[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", ts);

        proxy_cmd(fd, r, sizeof(r), 6, "HEXPIREAT", "tc:hs:expat", ts_str, "FIELDS", "1", "eaf1");
        CHECK(strcmp(r, "*1") == 0, "HEXPIREAT: returns *1 array", pass, fail);
        resp_next_str(fd, r2, sizeof(r2));
        CHECK(strcmp(r2, ":1") == 0, "HEXPIREAT: field expiry set returns 1", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 5, "HEXPIRETIME", "tc:hs:expat", "FIELDS", "1", "eaf1");
        CHECK(strcmp(r, "*1") == 0, "HEXPIRETIME: returns *1 array", pass, fail);
        resp_next_str(fd, r2, sizeof(r2));
        {
            long long got = atoll(r2 + 1);
            CHECK(r2[0] == ':' && got == ts, "HEXPIRETIME: returns the set expiry timestamp", pass, fail);
        }
    }

    /* ── HPEXPIREAT / HPEXPIRETIME ────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:pexpat", "peaf1", "peatval");
    {
        long long ts_ms = ((long long)time(NULL) + 300) * 1000LL;
        char ts_str[32];
        snprintf(ts_str, sizeof(ts_str), "%lld", ts_ms);

        proxy_cmd(fd, r, sizeof(r), 6, "HPEXPIREAT", "tc:hs:pexpat", ts_str, "FIELDS", "1", "peaf1");
        CHECK(strcmp(r, "*1") == 0, "HPEXPIREAT: returns *1 array", pass, fail);
        resp_next_str(fd, r2, sizeof(r2));
        CHECK(strcmp(r2, ":1") == 0, "HPEXPIREAT: field expiry set returns 1", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 5, "HPEXPIRETIME", "tc:hs:pexpat", "FIELDS", "1", "peaf1");
        CHECK(strcmp(r, "*1") == 0, "HPEXPIRETIME: returns *1 array", pass, fail);
        resp_next_str(fd, r2, sizeof(r2));
        {
            long long got = atoll(r2 + 1);
            CHECK(r2[0] == ':' && got >= ts_ms - 1000 && got <= ts_ms + 1000,
                  "HPEXPIRETIME: returns timestamp in expected range", pass, fail);
        }
    }

    /* ── HPERSIST ─────────────────────────────────────────────────────── */
    /* tc:hs:exp already has HEXPIRE set on ef1 */
    proxy_cmd(fd, r, sizeof(r), 5, "HPERSIST", "tc:hs:exp", "FIELDS", "1", "ef1");
    CHECK(strcmp(r, "*1") == 0, "HPERSIST: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, ":1") == 0, "HPERSIST: removes expiry returns 1", pass, fail);

    /* ── HGETDEL ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:getdel", "gdf1", "gdval");

    proxy_cmd(fd, r, sizeof(r), 5, "HGETDEL", "tc:hs:getdel", "FIELDS", "1", "gdf1");
    CHECK(strcmp(r, "*1") == 0, "HGETDEL: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, "gdval") == 0, "HGETDEL: returns the deleted field value", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "HEXISTS", "tc:hs:getdel", "gdf1");
    CHECK(strcmp(r, ":0") == 0, "HGETDEL: field no longer exists after deletion", pass, fail);

    /* ── HGETEX ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "HSET", "tc:hs:getex", "gef1", "geval");

    proxy_cmd(fd, r, sizeof(r), 5, "HGETEX", "tc:hs:getex", "FIELDS", "1", "gef1");
    CHECK(strcmp(r, "*1") == 0, "HGETEX: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, "geval") == 0, "HGETEX: returns field value", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 7, "HGETEX", "tc:hs:getex", "EX", "100", "FIELDS", "1", "gef1");
    CHECK(strcmp(r, "*1") == 0, "HGETEX: returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    CHECK(strcmp(r2, "geval") == 0, "HGETEX: returns field value", pass, fail);

    /* ── HSETEX ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "HSETEX", "tc:hs:setex", "EX", "100", "FIELDS", "1", "sef1", "seval");
    CHECK(strcmp(r, ":1") == 0, "HSETEX: new field returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "HTTL", "tc:hs:setex", "FIELDS", "1", "sef1");
    CHECK(strcmp(r, "*1") == 0, "HSETEX: HTTL returns *1 array", pass, fail);
    resp_next_str(fd, r2, sizeof(r2));
    {
        long long v = atoll(r2 + 1);
        CHECK(r2[0] == ':' && v > 0 && v <= 100, "HSETEX: field TTL is in (0, 100]", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
