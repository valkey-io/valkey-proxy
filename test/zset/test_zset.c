#include "test_common.h"

void run_zset_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== ZSet Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* All multi-key tests use {tc:zset} hashtag to ensure same slot */

    /* ── ZADD ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:s1", "1", "a", "2", "b", "3", "c");
    CHECK(strcmp(r, ":3") == 0, "ZADD: 3 new members returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZADD", "{tc:zset}:s1", "NX", "4", "a");
    CHECK(strcmp(r, ":0") == 0, "ZADD NX: existing member not added, returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 7, "ZADD", "{tc:zset}:s1", "CH", "10", "a", "4", "d");
    CHECK(strcmp(r, ":2") == 0, "ZADD CH: 1 updated + 1 new = 2 changed", pass, fail);

    /* Remove d to restore s1 = {a/10, b/2, c/3} */
    proxy_cmd(fd, r, sizeof(r), 3, "ZREM", "{tc:zset}:s1", "d");
    /* Reset a score back to 1 */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "{tc:zset}:s1", "1", "a");

    /* Build s2 = {b/2, c/3, d/4} */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:s2", "2", "b", "3", "c", "4", "d");
    CHECK(strcmp(r, ":3") == 0, "ZADD: s2 3 new members returns 3", pass, fail);

    /* ── ZCARD ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:s1");
    CHECK(strcmp(r, ":3") == 0, "ZCARD: 3-member zset returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:noexist");
    CHECK(strcmp(r, ":0") == 0, "ZCARD: non-existent key returns 0", pass, fail);

    /* ── ZSCORE ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "ZSCORE", "{tc:zset}:s1", "b");
    CHECK(strcmp(r, "2") == 0, "ZSCORE: member b has score 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZSCORE", "{tc:zset}:s1", "nothere");
    CHECK(strcmp(r, "(nil)") == 0, "ZSCORE: absent member returns nil", pass, fail);

    /* ── ZINCRBY ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "ZINCRBY", "{tc:zset}:s1", "5", "b");
    CHECK(strcmp(r, "7") == 0, "ZINCRBY: b score 2+5=7", pass, fail);

    /* Restore b to 2 */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "{tc:zset}:s1", "2", "b");

    /* ── ZCOUNT ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "ZCOUNT", "{tc:zset}:s1", "-inf", "+inf");
    CHECK(strcmp(r, ":3") == 0, "ZCOUNT -inf +inf: all 3 members", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZCOUNT", "{tc:zset}:s1", "2", "3");
    CHECK(strcmp(r, ":2") == 0, "ZCOUNT 2 3: members b and c", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZCOUNT", "{tc:zset}:s1", "(1", "3");
    CHECK(strcmp(r, ":2") == 0, "ZCOUNT (1 3: exclusive lower bound", pass, fail);

    /* ── ZLEXCOUNT ────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "ZLEXCOUNT", "{tc:zset}:s1", "-", "+");
    CHECK(strcmp(r, ":3") == 0, "ZLEXCOUNT - +: all 3 members", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZLEXCOUNT", "{tc:zset}:s1", "[a", "[c");
    CHECK(strcmp(r, ":3") == 0, "ZLEXCOUNT [a [c: a,b,c = 3", pass, fail);

    /* ── ZRANK / ZREVRANK ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "ZRANK", "{tc:zset}:s1", "a");
    CHECK(strcmp(r, ":0") == 0, "ZRANK: a is rank 0 (lowest score)", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZRANK", "{tc:zset}:s1", "c");
    CHECK(strcmp(r, ":2") == 0, "ZRANK: c is rank 2 (highest score)", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZRANK", "{tc:zset}:s1", "nothere");
    CHECK(strcmp(r, "(nil)") == 0, "ZRANK: absent member returns nil", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZREVRANK", "{tc:zset}:s1", "c");
    CHECK(strcmp(r, ":0") == 0, "ZREVRANK: c is revrank 0 (highest score)", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZREVRANK", "{tc:zset}:s1", "a");
    CHECK(strcmp(r, ":2") == 0, "ZREVRANK: a is revrank 2 (lowest score)", pass, fail);

    /* ── ZREM ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:rem", "1", "r1", "2", "r2", "3", "r3");
    proxy_cmd(fd, r, sizeof(r), 3, "ZREM", "{tc:zset}:rem", "r1");
    CHECK(strcmp(r, ":1") == 0, "ZREM: existing member returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "ZREM", "{tc:zset}:rem", "nothere");
    CHECK(strcmp(r, ":0") == 0, "ZREM: absent member returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:rem");
    CHECK(strcmp(r, ":2") == 0, "ZREM: cardinality reduced to 2", pass, fail);

    /* ── ZRANGE ───────────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZRANGE", "{tc:zset}:s1", "0", "-1");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "ZRANGE 0 -1: returns *3 array", pass, fail);
        int has_a = resp_array_contains(fd, n, "a");
        CHECK(has_a, "ZRANGE: result contains 'a'", pass, fail);
    }

    /* ZRANGE with WITHSCORES: re-send and read 2*3=6 elements */
    resp_send(fd, 5, "ZRANGE", "{tc:zset}:s1", "0", "-1", "WITHSCORES");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 6, "ZRANGE WITHSCORES: returns *6 (3 member+score pairs)", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZRANGEBYSCORE ────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZRANGEBYSCORE", "{tc:zset}:s1", "1", "2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 2, "ZRANGEBYSCORE 1 2: returns a and b (*2)", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZRANGEBYLEX ──────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZRANGEBYLEX", "{tc:zset}:s1", "[a", "[b");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 2, "ZRANGEBYLEX [a [b: returns a and b (*2)", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZREVRANGE ────────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZREVRANGE", "{tc:zset}:s1", "0", "-1");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "ZREVRANGE 0 -1: returns *3 array", pass, fail);
        int has_c = resp_array_contains(fd, n, "c");
        CHECK(has_c, "ZREVRANGE: result contains 'c'", pass, fail);
    }

    /* ── ZREVRANGEBYSCORE ─────────────────────────────────────────────── */
    resp_send(fd, 4, "ZREVRANGEBYSCORE", "{tc:zset}:s1", "+inf", "-inf");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "ZREVRANGEBYSCORE +inf -inf: all 3 members", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZREVRANGEBYLEX ───────────────────────────────────────────────── */
    resp_send(fd, 4, "ZREVRANGEBYLEX", "{tc:zset}:s1", "+", "-");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "ZREVRANGEBYLEX + -: all 3 members in reverse lex", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZREMRANGEBYRANK ──────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:rr", "1", "x1", "2", "x2", "3", "x3");
    proxy_cmd(fd, r, sizeof(r), 4, "ZREMRANGEBYRANK", "{tc:zset}:rr", "0", "0");
    CHECK(strcmp(r, ":1") == 0, "ZREMRANGEBYRANK: removes rank 0, returns 1", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:rr");
    CHECK(strcmp(r, ":2") == 0, "ZREMRANGEBYRANK: 2 members remain", pass, fail);

    /* ── ZREMRANGEBYSCORE ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "ZREMRANGEBYSCORE", "{tc:zset}:rr", "2", "3");
    CHECK(strcmp(r, ":2") == 0, "ZREMRANGEBYSCORE: removes scores 2-3, returns 2", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:rr");
    CHECK(strcmp(r, ":0") == 0, "ZREMRANGEBYSCORE: key now empty", pass, fail);

    /* ── ZREMRANGEBYLEX ───────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:rl", "0", "alpha", "0", "bar", "0", "cool");
    proxy_cmd(fd, r, sizeof(r), 4, "ZREMRANGEBYLEX", "{tc:zset}:rl", "[alpha", "[bar");
    CHECK(strcmp(r, ":2") == 0, "ZREMRANGEBYLEX: removes alpha and bar, returns 2", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:rl");
    CHECK(strcmp(r, ":1") == 0, "ZREMRANGEBYLEX: 1 member remains", pass, fail);

    /* ── ZMSCORE ──────────────────────────────────────────────────────── */
    resp_send(fd, 5, "ZMSCORE", "{tc:zset}:s1", "a", "nothere", "c");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "ZMSCORE: returns *3 array", pass, fail);
    {
        char v[64];
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "1") == 0, "ZMSCORE: a=1", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "(nil)") == 0, "ZMSCORE: nothere=nil", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "3") == 0, "ZMSCORE: c=3", pass, fail);
    }

    /* ── ZPOPMIN / ZPOPMAX ────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:pop", "1", "p1", "2", "p2", "3", "p3");

    resp_send(fd, 2, "ZPOPMIN", "{tc:zset}:pop");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "ZPOPMIN: returns *2 [member, score]", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:pop");
    CHECK(strcmp(r, ":2") == 0, "ZPOPMIN: cardinality reduced to 2", pass, fail);

    resp_send(fd, 2, "ZPOPMAX", "{tc:zset}:pop");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "ZPOPMAX: returns *2 [member, score]", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:pop");
    CHECK(strcmp(r, ":1") == 0, "ZPOPMAX: cardinality reduced to 1", pass, fail);

    /* ── ZRANDMEMBER ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:rand", "1", "r1", "2", "r2", "3", "r3", "4", "r4");

    proxy_cmd(fd, r, sizeof(r), 2, "ZRANDMEMBER", "{tc:zset}:rand");
    CHECK(r[0] != '-', "ZRANDMEMBER: returns a member (not error)", pass, fail);

    resp_send(fd, 3, "ZRANDMEMBER", "{tc:zset}:rand", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "ZRANDMEMBER count=2: returns *2 array", pass, fail);
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64];
        for (int i = 0; i < cnt; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZSCAN ────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "ZSCAN", "{tc:zset}:rand", "0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "ZSCAN: returns outer *2 array", pass, fail);
    {
        char cur[64];
        resp_next_str(fd, cur, sizeof(cur)); /* cursor */
        resp_next_str(fd, r, sizeof(r));     /* inner *2N header (member+score pairs) */
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n >= 0 && n % 2 == 0, "ZSCAN: inner count is non-negative even number", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── ZDIFF ────────────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZDIFF", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        /* s1={a,b,c}, s2={b,c,d}; diff = {a} */
        CHECK(n == 1, "ZDIFF: s1-s2 = {a}, returns *1", pass, fail);
        int has_a = resp_array_contains(fd, n, "a");
        CHECK(has_a, "ZDIFF: result contains 'a'", pass, fail);
    }

    /* ── ZDIFFSTORE ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "ZDIFFSTORE", "{tc:zset}:ddst", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    CHECK(strcmp(r, ":1") == 0, "ZDIFFSTORE: stores 1-member diff, returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:ddst");
    CHECK(strcmp(r, ":1") == 0, "ZDIFFSTORE: destination has 1 member", pass, fail);

    /* ── ZINTER ───────────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZINTER", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        /* s1={a,b,c}, s2={b,c,d}; inter = {b,c} */
        CHECK(n == 2, "ZINTER: s1∩s2 = {b,c}, returns *2", pass, fail);
        int has_b = resp_array_contains(fd, n, "b");
        CHECK(has_b, "ZINTER: result contains 'b'", pass, fail);
    }

    /* ── ZINTERSTORE ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "ZINTERSTORE", "{tc:zset}:idst", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    CHECK(strcmp(r, ":2") == 0, "ZINTERSTORE: stores 2-member intersection, returns 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:idst");
    CHECK(strcmp(r, ":2") == 0, "ZINTERSTORE: destination has 2 members", pass, fail);

    /* ── ZINTERCARD ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "ZINTERCARD", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    CHECK(strcmp(r, ":2") == 0, "ZINTERCARD: intersection cardinality is 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 6, "ZINTERCARD", "2", "{tc:zset}:s1", "{tc:zset}:s2", "LIMIT", "1");
    CHECK(strcmp(r, ":1") == 0, "ZINTERCARD LIMIT 1: capped at 1", pass, fail);

    /* ── ZUNION ───────────────────────────────────────────────────────── */
    resp_send(fd, 4, "ZUNION", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        /* s1={a,b,c}, s2={b,c,d}; union = {a,b,c,d} */
        CHECK(n == 4, "ZUNION: s1∪s2 = {a,b,c,d}, returns *4", pass, fail);
        int has_d = resp_array_contains(fd, n, "d");
        CHECK(has_d, "ZUNION: result contains 'd'", pass, fail);
    }

    /* ── ZUNIONSTORE ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "ZUNIONSTORE", "{tc:zset}:udst", "2", "{tc:zset}:s1", "{tc:zset}:s2");
    CHECK(strcmp(r, ":4") == 0, "ZUNIONSTORE: stores 4-member union, returns 4", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:udst");
    CHECK(strcmp(r, ":4") == 0, "ZUNIONSTORE: destination has 4 members", pass, fail);

    /* ── ZMPOP ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 8, "ZADD", "{tc:zset}:mp", "1", "m1", "2", "m2", "3", "m3");
    resp_send(fd, 4, "ZMPOP", "1", "{tc:zset}:mp", "MIN");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "ZMPOP: outer *2 [key, elements-array]", pass, fail);
    {
        char kname[128];
        resp_next_str(fd, kname, sizeof(kname)); /* key name */
        resp_next_str(fd, r, sizeof(r));         /* inner *N header */
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 1, "ZMPOP MIN: 1 element popped", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) {
            resp_next_str(fd, v, sizeof(v)); /* *2 sub-array header */
            resp_next_str(fd, v, sizeof(v)); /* member */
            resp_next_str(fd, v, sizeof(v)); /* score */
        }
    }
    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:mp");
    CHECK(strcmp(r, ":2") == 0, "ZMPOP MIN: one element popped, 2 remain", pass, fail);

    /* ── ZRANGESTORE ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 6, "ZRANGESTORE", "{tc:zset}:rdst", "{tc:zset}:s1", "1", "3", "BYSCORE");
    CHECK(strcmp(r, ":3") == 0, "ZRANGESTORE BYSCORE 1 3: stores 3 members from s1, returns 3", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "ZRANGESTORE", "{tc:zset}:rdst", "{tc:zset}:s1", "0", "-1");
    CHECK(strcmp(r, ":3") == 0, "ZRANGESTORE: stores 3 members from s1, returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "ZCARD", "{tc:zset}:rdst");
    CHECK(strcmp(r, ":3") == 0, "ZRANGESTORE: destination has 3 members", pass, fail);

    /* ── Crossslot errors for multi-key ZSet commands ─────────────────── */
    /* Use plain keys (no hashtag) that hash to different slots */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "tc_zcs_x", "1", "x1");
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "tc_zcs_y", "1", "y1");

    proxy_cmd(fd, r, sizeof(r), 4, "ZDIFF", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZDIFF crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZDIFFSTORE", "tc_zcs_z", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZDIFFSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZINTER", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZINTER crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZINTERCARD", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZINTERCARD crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZINTERSTORE", "tc_zcs_z", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZINTERSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZMPOP", "2", "tc_zcs_x", "tc_zcs_y", "MIN");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZMPOP crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZRANGESTORE", "tc_zcs_z", "tc_zcs_x", "0", "-1");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZRANGESTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "ZUNION", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZUNION crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "ZUNIONSTORE", "tc_zcs_z", "2", "tc_zcs_x", "tc_zcs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "ZUNIONSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
