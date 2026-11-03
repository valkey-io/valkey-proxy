#include "test_common.h"

void run_set_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Set Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* All multi-key tests use {tc:set} hashtag to ensure same slot */

    /* ── SADD ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SADD", "{tc:set}:a", "m1", "m2");
    CHECK(strcmp(r, ":2") == 0, "SADD: 2 new members returns 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SADD", "{tc:set}:a", "m1");
    CHECK(strcmp(r, ":0") == 0, "SADD: duplicate member returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SADD", "{tc:set}:a", "m3");
    CHECK(strcmp(r, ":1") == 0, "SADD: 1 new member returns 1", pass, fail);

    /* ── SCARD ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "SCARD", "{tc:set}:a");
    CHECK(strcmp(r, ":3") == 0, "SCARD: 3-member set returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "SCARD", "{tc:set}:noexist");
    CHECK(strcmp(r, ":0") == 0, "SCARD: non-existent key returns 0", pass, fail);

    /* ── SISMEMBER ────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:a", "m1");
    CHECK(strcmp(r, ":1") == 0, "SISMEMBER: existing member returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:a", "nothere");
    CHECK(strcmp(r, ":0") == 0, "SISMEMBER: absent member returns 0", pass, fail);

    /* ── SMISMEMBER ───────────────────────────────────────────────────── */
    resp_send(fd, 5, "SMISMEMBER", "{tc:set}:a", "m1", "nothere", "m2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "SMISMEMBER: returns *3 array", pass, fail);
    {
        char v[64];
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":1") == 0, "SMISMEMBER: m1 present → 1", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":0") == 0, "SMISMEMBER: nothere absent → 0", pass, fail);
        resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":1") == 0, "SMISMEMBER: m2 present → 1", pass, fail);
    }

    /* ── SMEMBERS ─────────────────────────────────────────────────────── */
    resp_send(fd, 2, "SMEMBERS", "{tc:set}:a");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 3, "SMEMBERS: 3-member set returns *3", pass, fail);
        int has_m1 = resp_array_contains(fd, n, "m1");
        CHECK(has_m1, "SMEMBERS: contains m1", pass, fail);
    }

    resp_send(fd, 2, "SMEMBERS", "{tc:set}:a");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_m2 = resp_array_contains(fd, n, "m2");
        CHECK(has_m2, "SMEMBERS: contains m2", pass, fail);
    }

    /* ── SREM ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SREM", "{tc:set}:a", "m3");
    CHECK(strcmp(r, ":1") == 0, "SREM: existing member returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SREM", "{tc:set}:a", "nothere");
    CHECK(strcmp(r, ":0") == 0, "SREM: absent member returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "SCARD", "{tc:set}:a");
    CHECK(strcmp(r, ":2") == 0, "SREM: cardinality reduced to 2", pass, fail);

    /* ── SPOP ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "SADD", "{tc:set}:pop", "p1", "p2", "p3");

    proxy_cmd(fd, r, sizeof(r), 2, "SPOP", "{tc:set}:pop");
    CHECK(r[0] != '-', "SPOP: returns a member (not error)", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "SCARD", "{tc:set}:pop");
    CHECK(strcmp(r, ":2") == 0, "SPOP: cardinality decremented to 2", pass, fail);

    resp_send(fd, 3, "SPOP", "{tc:set}:pop", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "SPOP count=2: returns *2 array", pass, fail);
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64];
        for (int i = 0; i < cnt; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── SRANDMEMBER ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 6, "SADD", "{tc:set}:rand", "r1", "r2", "r3", "r4");

    proxy_cmd(fd, r, sizeof(r), 2, "SRANDMEMBER", "{tc:set}:rand");
    CHECK(r[0] != '-', "SRANDMEMBER: returns a member (not error)", pass, fail);

    resp_send(fd, 3, "SRANDMEMBER", "{tc:set}:rand", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "SRANDMEMBER count=2: returns *2 array", pass, fail);
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64];
        for (int i = 0; i < cnt; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── SSCAN ────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "SSCAN", "{tc:set}:rand", "0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "SSCAN: returns outer *2 array", pass, fail);
    {
        char cur[64];
        resp_next_str(fd, cur, sizeof(cur));
        resp_next_str(fd, r, sizeof(r));
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n >= 0, "SSCAN: inner array count non-negative", pass, fail);
        char v[64];
        for (int i = 0; i < n; i++) resp_next_str(fd, v, sizeof(v));
    }

    /* ── SMOVE ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SADD", "{tc:set}:src", "mv1");
    proxy_cmd(fd, r, sizeof(r), 4, "SMOVE", "{tc:set}:src", "{tc:set}:dst", "mv1");
    CHECK(strcmp(r, ":1") == 0, "SMOVE: moves existing member, returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:src", "mv1");
    CHECK(strcmp(r, ":0") == 0, "SMOVE: member removed from source", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:dst", "mv1");
    CHECK(strcmp(r, ":1") == 0, "SMOVE: member added to destination", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SMOVE", "{tc:set}:src", "{tc:set}:dst", "noexist");
    CHECK(strcmp(r, ":0") == 0, "SMOVE: absent member returns 0", pass, fail);

    /* ── SUNION ───────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SADD", "{tc:set}:u1", "a", "b");
    proxy_cmd(fd, r, sizeof(r), 4, "SADD", "{tc:set}:u2", "b", "c");

    resp_send(fd, 3, "SUNION", "{tc:set}:u1", "{tc:set}:u2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 3, "SUNION: union of {a,b} and {b,c} returns 3 members", pass, fail);
        int has_a = resp_array_contains(fd, n, "a");
        CHECK(has_a, "SUNION: result contains 'a'", pass, fail);
    }

    resp_send(fd, 3, "SUNION", "{tc:set}:u1", "{tc:set}:u2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        int has_c = resp_array_contains(fd, n, "c");
        CHECK(has_c, "SUNION: result contains 'c'", pass, fail);
    }

    /* ── SUNIONSTORE ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SUNIONSTORE", "{tc:set}:udst", "{tc:set}:u1", "{tc:set}:u2");
    CHECK(strcmp(r, ":3") == 0, "SUNIONSTORE: stores 3-member union, returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "SCARD", "{tc:set}:udst");
    CHECK(strcmp(r, ":3") == 0, "SUNIONSTORE: destination has 3 members", pass, fail);

    /* ── SINTER ───────────────────────────────────────────────────────── */
    resp_send(fd, 3, "SINTER", "{tc:set}:u1", "{tc:set}:u2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 1, "SINTER: intersection of {a,b} and {b,c} is {b}", pass, fail);
        int has_b = resp_array_contains(fd, n, "b");
        CHECK(has_b, "SINTER: result contains 'b'", pass, fail);
    }

    /* ── SINTERSTORE ──────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SINTERSTORE", "{tc:set}:idst", "{tc:set}:u1", "{tc:set}:u2");
    CHECK(strcmp(r, ":1") == 0, "SINTERSTORE: stores 1-member intersection, returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:idst", "b");
    CHECK(strcmp(r, ":1") == 0, "SINTERSTORE: destination contains 'b'", pass, fail);

    /* ── SINTERCARD ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SINTERCARD", "2", "{tc:set}:u1", "{tc:set}:u2");
    CHECK(strcmp(r, ":1") == 0, "SINTERCARD: intersection cardinality is 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 6, "SINTERCARD", "2", "{tc:set}:u1", "{tc:set}:u2", "LIMIT", "0");
    CHECK(strcmp(r, ":1") == 0, "SINTERCARD LIMIT 0: no cap, returns 1", pass, fail);

    /* ── SDIFF ────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "SDIFF", "{tc:set}:u1", "{tc:set}:u2");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n == 1, "SDIFF: {a,b} - {b,c} = {a}, returns 1 member", pass, fail);
        int has_a = resp_array_contains(fd, n, "a");
        CHECK(has_a, "SDIFF: result contains 'a'", pass, fail);
    }

    /* ── SDIFFSTORE ───────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SDIFFSTORE", "{tc:set}:ddst", "{tc:set}:u1", "{tc:set}:u2");
    CHECK(strcmp(r, ":1") == 0, "SDIFFSTORE: stores 1-member diff, returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SISMEMBER", "{tc:set}:ddst", "a");
    CHECK(strcmp(r, ":1") == 0, "SDIFFSTORE: destination contains 'a'", pass, fail);

    /* ── Crossslot errors for multi-key Set commands ──────────────────── */
    /* Use plain keys (no hashtag) that hash to different slots */
    proxy_cmd(fd, r, sizeof(r), 3, "SADD", "tc_cs_x", "x1");
    proxy_cmd(fd, r, sizeof(r), 3, "SADD", "tc_cs_y", "y1");

    proxy_cmd(fd, r, sizeof(r), 3, "SDIFF", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SDIFF crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SDIFFSTORE", "tc_cs_z", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SDIFFSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SINTER", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SINTER crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SINTERCARD", "2", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SINTERCARD crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SINTERSTORE", "tc_cs_z", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SINTERSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SMOVE", "tc_cs_x", "tc_cs_y", "x1");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SMOVE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SUNION", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SUNION crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SUNIONSTORE", "tc_cs_z", "tc_cs_x", "tc_cs_y");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "SUNIONSTORE crossslot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
