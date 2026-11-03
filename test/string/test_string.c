#include "test_common.h"

void run_string_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== String Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── SET / GET ─────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:key", "hello");
    CHECK(strcmp(r, "+OK") == 0, "SET: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:key");
    CHECK(strcmp(r, "hello") == 0, "GET: returns stored value", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:noexist");
    CHECK(strcmp(r, "(nil)") == 0, "GET: nonexistent key returns nil", pass, fail);

    /* SET with NX option */
    proxy_cmd(fd, r, sizeof(r), 5, "SET", "tc:str:key", "other", "NX", "");
    /* NX means don't overwrite; key exists so SET NX fails */
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:key");
    CHECK(strcmp(r, "hello") == 0, "SET NX: does not overwrite existing key", pass, fail);

    /* SET with EX option */
    proxy_cmd(fd, r, sizeof(r), 5, "SET", "tc:str:setex_opt", "withex", "EX", "100");
    CHECK(strcmp(r, "+OK") == 0, "SET EX: returns +OK", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:str:setex_opt");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 100, "SET EX: TTL in (0,100]", pass, fail);
    }

    /* ── SETNX ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SETNX", "tc:str:nx", "val1");
    CHECK(strcmp(r, ":1") == 0, "SETNX: new key returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SETNX", "tc:str:nx", "val2");
    CHECK(strcmp(r, ":0") == 0, "SETNX: existing key returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:nx");
    CHECK(strcmp(r, "val1") == 0, "SETNX: value unchanged after failed SETNX", pass, fail);

    /* ── SETEX / GETEX ─────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SETEX", "tc:str:ex", "100", "exval");
    CHECK(strcmp(r, "+OK") == 0, "SETEX: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:ex");
    CHECK(strcmp(r, "exval") == 0, "SETEX: stored value correct", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:str:ex");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 100, "SETEX: TTL in (0,100]", pass, fail);
    }

    /* GETEX with EX option updates TTL */
    proxy_cmd(fd, r, sizeof(r), 4, "GETEX", "tc:str:ex", "EX", "200");
    CHECK(strcmp(r, "exval") == 0, "GETEX EX: returns value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:str:ex");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 100 && v <= 200, "GETEX EX: updated TTL in (100,200]", pass, fail);
    }

    /* GETEX PERSIST removes TTL */
    proxy_cmd(fd, r, sizeof(r), 3, "GETEX", "tc:str:ex", "PERSIST");
    CHECK(strcmp(r, "exval") == 0, "GETEX PERSIST: returns value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "TTL", "tc:str:ex");
    CHECK(strcmp(r, ":-1") == 0, "GETEX PERSIST: TTL removed", pass, fail);

    /* GETEX on nonexistent key */
    proxy_cmd(fd, r, sizeof(r), 2, "GETEX", "tc:str:noexist");
    CHECK(strcmp(r, "(nil)") == 0, "GETEX: nonexistent key returns nil", pass, fail);

    /* ── PSETEX ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "PSETEX", "tc:str:pex", "10000", "pexval");
    CHECK(strcmp(r, "+OK") == 0, "PSETEX: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:pex");
    CHECK(strcmp(r, "pexval") == 0, "PSETEX: value stored correctly", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "PTTL", "tc:str:pex");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0 && v <= 10000, "PSETEX: PTTL in (0,10000]", pass, fail);
    }

    /* ── GETSET (deprecated but still supported) ───────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:gs", "oldval");
    proxy_cmd(fd, r, sizeof(r), 3, "GETSET", "tc:str:gs", "newval");
    CHECK(strcmp(r, "oldval") == 0, "GETSET: returns old value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:gs");
    CHECK(strcmp(r, "newval") == 0, "GETSET: new value stored", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "GETSET", "tc:str:gs:noexist", "first");
    CHECK(strcmp(r, "(nil)") == 0, "GETSET: returns nil when key did not exist", pass, fail);

    /* ── GETDEL ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:gd", "gdval");
    proxy_cmd(fd, r, sizeof(r), 2, "GETDEL", "tc:str:gd");
    CHECK(strcmp(r, "gdval") == 0, "GETDEL: returns value before deletion", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:gd");
    CHECK(strcmp(r, ":0") == 0, "GETDEL: key no longer exists after deletion", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GETDEL", "tc:str:noexist");
    CHECK(strcmp(r, "(nil)") == 0, "GETDEL: nonexistent key returns nil", pass, fail);

    /* ── APPEND ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:app", "Hello");
    proxy_cmd(fd, r, sizeof(r), 3, "APPEND", "tc:str:app", ", World");
    CHECK(strcmp(r, ":12") == 0, "APPEND: returns new total length 12", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:app");
    CHECK(strcmp(r, "Hello, World") == 0, "APPEND: value appended correctly", pass, fail);

    /* APPEND to nonexistent key (creates it) */
    proxy_cmd(fd, r, sizeof(r), 3, "APPEND", "tc:str:app:new", "created");
    CHECK(strcmp(r, ":7") == 0, "APPEND: creates new key, returns 7", pass, fail);

    /* ── STRLEN ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "STRLEN", "tc:str:app");
    CHECK(strcmp(r, ":12") == 0, "STRLEN: 'Hello, World' has length 12", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "STRLEN", "tc:str:noexist");
    CHECK(strcmp(r, ":0") == 0, "STRLEN: nonexistent key returns 0", pass, fail);

    /* ── GETRANGE / SUBSTR ──────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "GETRANGE", "tc:str:app", "0", "4");
    CHECK(strcmp(r, "Hello") == 0, "GETRANGE [0,4]: returns 'Hello'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "GETRANGE", "tc:str:app", "7", "11");
    CHECK(strcmp(r, "World") == 0, "GETRANGE [7,11]: returns 'World'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "GETRANGE", "tc:str:app", "-5", "-1");
    CHECK(strcmp(r, "World") == 0, "GETRANGE negative indices: returns 'World'", pass, fail);

    /* SUBSTR is an alias for GETRANGE */
    proxy_cmd(fd, r, sizeof(r), 4, "SUBSTR", "tc:str:app", "0", "4");
    CHECK(strcmp(r, "Hello") == 0, "SUBSTR [0,4]: alias for GETRANGE, returns 'Hello'", pass, fail);

    /* ── SETRANGE ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:sr", "Hello World");
    proxy_cmd(fd, r, sizeof(r), 4, "SETRANGE", "tc:str:sr", "6", "Redis");
    CHECK(strcmp(r, ":11") == 0, "SETRANGE: returns new total length 11", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:str:sr");
    CHECK(strcmp(r, "Hello Redis") == 0, "SETRANGE: value overwritten correctly", pass, fail);

    /* ── INCR / INCRBY / INCRBYFLOAT ────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:ctr", "10");
    proxy_cmd(fd, r, sizeof(r), 2, "INCR", "tc:str:ctr");
    CHECK(strcmp(r, ":11") == 0, "INCR: 10+1=11", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "INCRBY", "tc:str:ctr", "5");
    CHECK(strcmp(r, ":16") == 0, "INCRBY: 11+5=16", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "INCRBYFLOAT", "tc:str:ctr", "1.5");
    {
        double v = atof(r);
        CHECK(v > 17.4 && v < 17.6, "INCRBYFLOAT: 16+1.5=17.5", pass, fail);
    }

    /* INCR on nonexistent key (initialises to 0 then increments) */
    proxy_cmd(fd, r, sizeof(r), 2, "INCR", "tc:str:incr:new");
    CHECK(strcmp(r, ":1") == 0, "INCR: nonexistent key initialises to 0, returns 1", pass, fail);

    /* ── DECR / DECRBY ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:ctr2", "20");
    proxy_cmd(fd, r, sizeof(r), 2, "DECR", "tc:str:ctr2");
    CHECK(strcmp(r, ":19") == 0, "DECR: 20-1=19", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "DECRBY", "tc:str:ctr2", "4");
    CHECK(strcmp(r, ":15") == 0, "DECRBY: 19-4=15", pass, fail);

    /* ── LCS: same-slot keys required ──────────────────────────────────── */
    /* Use hash tags to guarantee same slot */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:lcs}k1", "ohmytext");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:lcs}k2", "mynewtext");

    proxy_cmd(fd, r, sizeof(r), 3, "LCS", "{tc:str:lcs}k1", "{tc:str:lcs}k2");
    CHECK(strcmp(r, "mytext") == 0, "LCS same-slot: LCS of 'ohmytext'/'mynewtext' is 'mytext'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "LCS", "{tc:str:lcs}k1", "{tc:str:lcs}k2", "LEN");
    CHECK(strcmp(r, ":6") == 0, "LCS LEN same-slot: LCS length is 6", pass, fail);

    /* LCS cross-slot: proxy fetches both values and computes LCS locally */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:lcs:a}key", "ohmytext");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:lcs:b}key", "mynewtext");
    proxy_cmd(fd, r, sizeof(r), 3, "LCS", "{tc:str:lcs:a}key", "{tc:str:lcs:b}key");
    CHECK(strcmp(r, "mytext") == 0, "LCS cross-slot: returns correct LCS string", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "LCS", "{tc:str:lcs:a}key", "{tc:str:lcs:b}key", "LEN");
    CHECK(strcmp(r, ":6") == 0, "LCS cross-slot LEN: returns 6", pass, fail);

    /* ── MGET (multi-key fan-out) ───────────────────────────────────────── */
    /* Different hash tags guarantee different slots, triggering cross-slot fanout */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:mg:1}k", "mv1");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:mg:2}k", "mv2");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:mg:3}k", "mv3");

    /* single key */
    resp_send(fd, 2, "MGET", "{tc:str:mg:1}k");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "MGET single key: returns *1 array", pass, fail);
    {
        char v[64];
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "mv1") == 0, "MGET single: value is mv1", pass, fail);
    }

    /* multiple keys across different slots (fanout) */
    resp_send(fd, 4, "MGET", "{tc:str:mg:1}k", "{tc:str:mg:2}k", "{tc:str:mg:3}k");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "MGET 3 keys in cross-slot: fanout returns *3 array", pass, fail);
    {
        char v1[64], v2[64], v3[64];
        resp_next_str(fd, v1, sizeof(v1));
        resp_next_str(fd, v2, sizeof(v2));
        resp_next_str(fd, v3, sizeof(v3));
        CHECK(strcmp(v1, "mv1") == 0, "MGET 3 keys in cross-slot: k1 result preserved in original order", pass, fail);
        CHECK(strcmp(v2, "mv2") == 0, "MGET 3 keys in cross-slot: k2 result preserved in original order", pass, fail);
        CHECK(strcmp(v3, "mv3") == 0, "MGET 3 keys in cross-slot: k3 result preserved in original order", pass, fail);
    }

    /* missing key returns nil in its position, order preserved across slots */
    resp_send(fd, 4, "MGET", "{tc:str:mg:1}k", "{tc:str:mg:noexist}k", "{tc:str:mg:3}k");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "MGET 3 keys in cross-slot with 1 missing: fanout returns *3 array", pass, fail);
    {
        char v1[64], v2[64], v3[64];
        resp_next_str(fd, v1, sizeof(v1));
        resp_next_str(fd, v2, sizeof(v2));
        resp_next_str(fd, v3, sizeof(v3));
        CHECK(strcmp(v1, "mv1") == 0, "MGET 3 keys in cross-slot with 1 missing: k1 value correct", pass, fail);
        CHECK(strcmp(v2, "(nil)") == 0, "MGET 3 keys in cross-slot with 1 missing: absent key returns nil in position",
              pass, fail);
        CHECK(strcmp(v3, "mv3") == 0, "MGET 3 keys in cross-slot with 1 missing: k3 value correct", pass, fail);
    }

    /* ── MSET (multi-key fan-out) ───────────────────────────────────────── */
    /* single pair */
    proxy_cmd(fd, r, sizeof(r), 3, "MSET", "{tc:str:ms:1}k", "msv1");
    CHECK(strcmp(r, "+OK") == 0, "MSET single pair: returns +OK", pass, fail);

    /* multiple pairs across different slots (fanout) — different hash tags */
    proxy_cmd(fd, r, sizeof(r), 7, "MSET", "{tc:str:ms:1}k", "msv1", "{tc:str:ms:2}k", "msv2", "{tc:str:ms:3}k",
              "msv3");
    CHECK(strcmp(r, "+OK") == 0, "MSET 3 key-value pairs in cross-slot: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ms:1}k");
    CHECK(strcmp(r, "msv1") == 0, "MSET 3 key-value pairs in cross-slot: k1 stored correctly", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ms:2}k");
    CHECK(strcmp(r, "msv2") == 0, "MSET 3 key-value pairs in cross-slot: k2 stored correctly", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ms:3}k");
    CHECK(strcmp(r, "msv3") == 0, "MSET 3 key-value pairs in cross-slot: k3 stored correctly", pass, fail);

    /* ── MSETNX: all keys must share one slot ───────────────────────────── */
    /* same slot: hash tags ensure same slot */
    proxy_cmd(fd, r, sizeof(r), 5, "MSETNX", "{tc:str:ssnx}k1", "v1", "{tc:str:ssnx}k2", "v2");
    CHECK(strcmp(r, ":1") == 0, "MSETNX same-slot: all new keys returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ssnx}k1");
    CHECK(strcmp(r, "v1") == 0, "MSETNX same-slot: k1 set correctly", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ssnx}k2");
    CHECK(strcmp(r, "v2") == 0, "MSETNX same-slot: k2 set correctly", pass, fail);

    /* MSETNX when any key already exists: nothing is set */
    proxy_cmd(fd, r, sizeof(r), 5, "MSETNX", "{tc:str:ssnx}k1", "newv1", "{tc:str:ssnx}k3", "v3");
    CHECK(strcmp(r, ":0") == 0, "MSETNX same-slot: existing key causes 0 (all-or-nothing)", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "{tc:str:ssnx}k3");
    CHECK(strcmp(r, ":0") == 0, "MSETNX: k3 not created when k1 already exists", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:ssnx}k1");
    CHECK(strcmp(r, "v1") == 0, "MSETNX: k1 value unchanged", pass, fail);

    /* cross-slot: must return CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 5, "MSETNX", "{tc:str:nx:a}key", "va", "{tc:str:nx:b}key", "vb");
    CHECK(r[0] == '-', "MSETNX cross-slot: returns error (CROSSSLOT)", pass, fail);

    /* ── DELIFEQ ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:delifeq", "delval");
    proxy_cmd(fd, r, sizeof(r), 3, "DELIFEQ", "tc:str:delifeq", "wrongval");
    if (r[0] == '-') {
        printf("  [SKIP] DELIFEQ: command not supported by this server\n");
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:str:delifeq");
    } else {
        CHECK(strcmp(r, ":0") == 0, "DELIFEQ: wrong value, not deleted (returns 0)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delifeq");
        CHECK(strcmp(r, ":1") == 0, "DELIFEQ wrong val: key still exists", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 3, "DELIFEQ", "tc:str:delifeq", "delval");
        CHECK(strcmp(r, ":1") == 0, "DELIFEQ: correct value, key deleted (returns 1)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delifeq");
        CHECK(strcmp(r, ":0") == 0, "DELIFEQ correct val: key no longer exists", pass, fail);
    }

    /* ── DELEX (Valkey 9.2+) ────────────────────────────────────────────── */
    /* format: DELEX key <IFEQ | IFNE> value */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:delex", "delexval");
    proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex", "IFEQ", "wrongval");
    if (r[0] == '-') {
        printf("  [SKIP] DELEX: command not supported by this server\n");
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:str:delex");
    } else {
        /* IFEQ */
        CHECK(strcmp(r, ":0") == 0, "DELEX IFEQ wrong value: not deleted (returns 0)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delex");
        CHECK(strcmp(r, ":1") == 0, "DELEX IFEQ wrong value: key still exists", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex", "IFEQ", "delexval");
        CHECK(strcmp(r, ":1") == 0, "DELEX IFEQ matching value: key deleted (returns 1)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delex");
        CHECK(strcmp(r, ":0") == 0, "DELEX IFEQ matching value: key no longer exists", pass, fail);

        /* IFNE */
        proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:str:delex", "delexval");
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex", "IFNE", "delexval");
        CHECK(strcmp(r, ":0") == 0, "DELEX IFNE same value: not deleted (returns 0)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delex");
        CHECK(strcmp(r, ":1") == 0, "DELEX IFNE same value: key still exists", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex", "IFNE", "otherval");
        CHECK(strcmp(r, ":1") == 0, "DELEX IFNE different value: key deleted (returns 1)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "EXISTS", "tc:str:delex");
        CHECK(strcmp(r, ":0") == 0, "DELEX IFNE different value: key no longer exists", pass, fail);

        /* missing key: nothing to delete */
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex", "IFNE", "otherval");
        CHECK(strcmp(r, ":0") == 0, "DELEX missing key: returns 0", pass, fail);

        /* condition is mandatory */
        proxy_cmd(fd, r, sizeof(r), 2, "DELEX", "tc:str:delex");
        CHECK(r[0] == '-', "DELEX without condition: returns error", pass, fail);

        /* non-string key: error, key untouched */
        proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:str:delex:list", "a");
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "tc:str:delex:list", "IFEQ", "a");
        CHECK(r[0] == '-', "DELEX on non-string key: returns error", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:str:delex:list");

        /* keys in different slots are each routed to their owning node */
        proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:delex:a}k", "va");
        proxy_cmd(fd, r, sizeof(r), 3, "SET", "{tc:str:delex:b}k", "vb");
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "{tc:str:delex:a}k", "IFEQ", "va");
        CHECK(strcmp(r, ":1") == 0, "DELEX slot a: key deleted (returns 1)", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 4, "DELEX", "{tc:str:delex:b}k", "IFEQ", "vb");
        CHECK(strcmp(r, ":1") == 0, "DELEX slot b: key deleted (returns 1)", pass, fail);
    }

    /* ── MSETEX (multi-key SET with per-key TTL, fan-out) ───────────────── */
    /* format: MSETEX key1 seconds1 val1 [key2 seconds2 val2 ...] */
    /* single key first to probe support */
    proxy_cmd(fd, r, sizeof(r), 4, "MSETEX", "{tc:str:mse:1}k", "50", "msev1probe");
    if (r[0] == '-') {
        printf("  [SKIP] MSETEX: command not supported by this server\n");
    } else {
        CHECK(strcmp(r, "+OK") == 0, "MSETEX single key: returns +OK", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:mse:1}k");
        CHECK(strcmp(r, "msev1probe") == 0, "MSETEX single: value correct", pass, fail);

        /* multiple keys across different slots (fanout) — different hash tags */
        proxy_cmd(fd, r, sizeof(r), 7, "MSETEX", "{tc:str:mse:1}k", "100", "msev1", "{tc:str:mse:2}k", "100", "msev2");
        CHECK(strcmp(r, "+OK") == 0, "MSETEX 2 keys in cross-slot: returns +OK", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:mse:1}k");
        CHECK(strcmp(r, "msev1") == 0, "MSETEX 2 keys in cross-slot: k1 value correct", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:mse:2}k");
        CHECK(strcmp(r, "msev2") == 0, "MSETEX 2 keys in cross-slot: k2 value correct", pass, fail);

        proxy_cmd(fd, r, sizeof(r), 2, "TTL", "{tc:str:mse:1}k");
        {
            long long v = atoll(r + 1);
            CHECK(r[0] == ':' && v > 0 && v <= 100, "MSETEX 2 keys in cross-slot: k1 TTL in (0,100]", pass, fail);
        }
        proxy_cmd(fd, r, sizeof(r), 2, "TTL", "{tc:str:mse:2}k");
        {
            long long v = atoll(r + 1);
            CHECK(r[0] == ':' && v > 0 && v <= 100, "MSETEX 2 keys in cross-slot: k2 TTL in (0,100]", pass, fail);
        }

        /* three keys across three different slots */
        proxy_cmd(fd, r, sizeof(r), 10, "MSETEX", "{tc:str:mse:1}k", "100", "msev1b", "{tc:str:mse:2}k", "100",
                  "msev2b", "{tc:str:mse:3}k", "100", "msev3b");
        CHECK(strcmp(r, "+OK") == 0, "MSETEX 3 keys in cross-slot: returns +OK", pass, fail);
        proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:str:mse:3}k");
        CHECK(strcmp(r, "msev3b") == 0, "MSETEX 3 keys in cross-slot: k3 value correct", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
