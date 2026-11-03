#include "test_common.h"

/* Drain the nested LMPOP response after reading the *2 header.
 * Format: *2 → [bulk-key, *N → [elem, elem, ...]] */
static void drain_lmpop(int fd) {
    char tmp[512];
    resp_next_str(fd, tmp, sizeof(tmp)); /* key name (bulk string) */
    resp_next_str(fd, tmp, sizeof(tmp)); /* *N values-array header  */
    int n = (tmp[0] == '*') ? atoi(tmp + 1) : 0;
    for (int i = 0; i < n; i++) resp_next_str(fd, tmp, sizeof(tmp));
}

void run_list_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== List Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    char tmp[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── RPUSH / LPUSH ────────────────────────────────────────────────────── */

    /* RPUSH: append elements to the right; returns new length */
    proxy_cmd(fd, r, sizeof(r), 5, "RPUSH", "tc:list:a", "x", "y", "z");
    CHECK(strcmp(r, ":3") == 0, "RPUSH: returns new length 3", pass, fail);

    /* LPUSH with 2 elements; each is pushed to head so list = [v, w, x, y, z] */
    proxy_cmd(fd, r, sizeof(r), 4, "LPUSH", "tc:list:a", "w", "v");
    CHECK(strcmp(r, ":5") == 0, "LPUSH: returns new length 5", pass, fail);

    /* ── LLEN ─────────────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "tc:list:a");
    CHECK(strcmp(r, ":5") == 0, "LLEN: returns 5", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "tc:list:noexist");
    CHECK(strcmp(r, ":0") == 0, "LLEN: non-existent key returns 0", pass, fail);

    /* ── LRANGE ───────────────────────────────────────────────────────────── */

    /* list is now [v, w, x, y, z] */
    proxy_cmd(fd, r, sizeof(r), 4, "LRANGE", "tc:list:a", "0", "-1");
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(r[0] == '*' && cnt == 5, "LRANGE 0 -1: returns 5-element array", pass, fail);
        int found = resp_array_contains(fd, cnt, "v");
        CHECK(found, "LRANGE 0 -1: contains head element 'v'", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 4, "LRANGE", "tc:list:a", "0", "1");
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(r[0] == '*' && cnt == 2, "LRANGE 0 1: returns 2-element array", pass, fail);
        int found = resp_array_contains(fd, cnt, "v");
        CHECK(found, "LRANGE 0 1: contains head element 'v'", pass, fail);
    }

    /* ── LINDEX ───────────────────────────────────────────────────────────── */

    /* resp_next_str returns bulk-string content directly (no $N prefix) */
    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "tc:list:a", "0");
    CHECK(strcmp(r, "v") == 0, "LINDEX 0: returns head element 'v'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "tc:list:a", "-1");
    CHECK(strcmp(r, "z") == 0, "LINDEX -1: returns tail element 'z'", pass, fail);

    /* out-of-range → nil bulk string → "(nil)" */
    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "tc:list:a", "99");
    CHECK(strcmp(r, "(nil)") == 0, "LINDEX out-of-range: returns nil", pass, fail);

    /* ── LSET ─────────────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "LSET", "tc:list:a", "0", "V");
    CHECK(strcmp(r, "+OK") == 0, "LSET: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "tc:list:a", "0");
    CHECK(strcmp(r, "V") == 0, "LSET: head element updated to 'V'", pass, fail);

    /* ── LINSERT ──────────────────────────────────────────────────────────── */

    /* list: [V, w, x, y, z]; insert 'ins' BEFORE 'x' → [V, w, ins, x, y, z] */
    proxy_cmd(fd, r, sizeof(r), 5, "LINSERT", "tc:list:a", "BEFORE", "x", "ins");
    CHECK(strcmp(r, ":6") == 0, "LINSERT BEFORE: returns new length 6", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "tc:list:a", "2");
    CHECK(strcmp(r, "ins") == 0, "LINSERT: inserted element at index 2", pass, fail);

    /* insert 'aft' AFTER 'y' → [V, w, ins, x, y, aft, z] → length 7 */
    proxy_cmd(fd, r, sizeof(r), 5, "LINSERT", "tc:list:a", "AFTER", "y", "aft");
    CHECK(strcmp(r, ":7") == 0, "LINSERT AFTER: returns new length 7", pass, fail);

    /* ── LPOS ─────────────────────────────────────────────────────────────── */

    /* list: [V, w, ins, x, y, aft, z]; 'x' is at index 3 */
    proxy_cmd(fd, r, sizeof(r), 3, "LPOS", "tc:list:a", "x");
    CHECK(strcmp(r, ":3") == 0, "LPOS: returns index 3 for 'x'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LPOS", "tc:list:a", "nothere");
    CHECK(strcmp(r, "(nil)") == 0, "LPOS: element not found returns nil", pass, fail);

    /* ── LREM ─────────────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 5, "RPUSH", "tc:list:b", "a", "b", "a");
    proxy_cmd(fd, r, sizeof(r), 4, "LREM", "tc:list:b", "0", "a");
    CHECK(strcmp(r, ":2") == 0, "LREM: removes all occurrences, returns count 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "tc:list:b");
    CHECK(strcmp(r, ":1") == 0, "LREM: list length is 1 after removal", pass, fail);

    /* ── LTRIM ────────────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 6, "RPUSH", "tc:list:c", "1", "2", "3", "4");
    proxy_cmd(fd, r, sizeof(r), 4, "LTRIM", "tc:list:c", "1", "2");
    CHECK(strcmp(r, "+OK") == 0, "LTRIM: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "tc:list:c");
    CHECK(strcmp(r, ":2") == 0, "LTRIM: list trimmed to 2 elements", pass, fail);

    /* ── LPOP / RPOP ──────────────────────────────────────────────────────── */

    /* list tc:list:a = [V, w, ins, x, y, aft, z] */
    proxy_cmd(fd, r, sizeof(r), 2, "LPOP", "tc:list:a");
    CHECK(strcmp(r, "V") == 0, "LPOP: returns and removes head 'V'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "RPOP", "tc:list:a");
    CHECK(strcmp(r, "z") == 0, "RPOP: returns and removes tail 'z'", pass, fail);

    /* LPOP with count argument → array response */
    proxy_cmd(fd, r, sizeof(r), 3, "LPOP", "tc:list:a", "2");
    {
        int cnt = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(r[0] == '*' && cnt == 2, "LPOP count: returns 2-element array", pass, fail);
        /* drain elements before next command */
        for (int i = 0; i < cnt; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    /* LPOP on non-existent key → nil */
    proxy_cmd(fd, r, sizeof(r), 2, "LPOP", "tc:list:noexist");
    CHECK(strcmp(r, "(nil)") == 0, "LPOP: non-existent key returns nil", pass, fail);

    /* ── RPUSHX / LPUSHX ──────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 3, "RPUSHX", "tc:list:nox", "val");
    CHECK(strcmp(r, ":0") == 0, "RPUSHX: non-existent key returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LPUSHX", "tc:list:nox", "val");
    CHECK(strcmp(r, ":0") == 0, "LPUSHX: non-existent key returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:list:d", "base");
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSHX", "tc:list:d", "extra");
    CHECK(strcmp(r, ":2") == 0, "RPUSHX: existing key appends, returns 2", pass, fail);

    /* ── LMOVE (multi-key, same slot) ────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 5, "RPUSH", "{lst}:src", "m1", "m2", "m3");

    /* LMOVE LEFT RIGHT: pop from left of src, push to right of dst */
    proxy_cmd(fd, r, sizeof(r), 5, "LMOVE", "{lst}:src", "{lst}:dst", "LEFT", "RIGHT");
    CHECK(strcmp(r, "m1") == 0, "LMOVE LEFT RIGHT: returns moved element 'm1'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "{lst}:src");
    CHECK(strcmp(r, ":2") == 0, "LMOVE: src length decremented to 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "{lst}:dst");
    CHECK(strcmp(r, ":1") == 0, "LMOVE: dst length incremented to 1", pass, fail);

    /* LMOVE RIGHT LEFT */
    proxy_cmd(fd, r, sizeof(r), 5, "LMOVE", "{lst}:src", "{lst}:dst", "RIGHT", "LEFT");
    CHECK(strcmp(r, "m3") == 0, "LMOVE RIGHT LEFT: returns tail element 'm3'", pass, fail);

    /* LMOVE cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 5, "LMOVE", "{a}:lst", "{b}:lst", "LEFT", "RIGHT");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "LMOVE cross-slot: returns CROSSSLOT error", pass, fail);

    /* ── RPOPLPUSH (multi-key, same slot) ────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 5, "RPUSH", "{rp}:src", "r1", "r2", "r3");

    proxy_cmd(fd, r, sizeof(r), 3, "RPOPLPUSH", "{rp}:src", "{rp}:dst");
    CHECK(strcmp(r, "r3") == 0, "RPOPLPUSH: returns popped element 'r3'", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "{rp}:src");
    CHECK(strcmp(r, ":2") == 0, "RPOPLPUSH: src length decremented to 2", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "LINDEX", "{rp}:dst", "0");
    CHECK(strcmp(r, "r3") == 0, "RPOPLPUSH: element pushed to head of dst", pass, fail);

    /* RPOPLPUSH cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPOPLPUSH", "{a}:lst", "{b}:lst");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "RPOPLPUSH cross-slot: returns CROSSSLOT error", pass, fail);

    /* ── LMPOP (multi-key, same slot) ────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 5, "RPUSH", "{mp}:a", "p1", "p2", "p3");
    proxy_cmd(fd, r, sizeof(r), 4, "RPUSH", "{mp}:b", "q1", "q2");

    /* LMPOP 1 key LEFT → *2 [key, [p1]] */
    proxy_cmd(fd, r, sizeof(r), 4, "LMPOP", "1", "{mp}:a", "LEFT");
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "LMPOP 1 key LEFT: returns *2 [key, [elements]]", pass, fail);
    if (r[0] == '*' && atoi(r + 1) == 2) drain_lmpop(fd);

    proxy_cmd(fd, r, sizeof(r), 2, "LLEN", "{mp}:a");
    CHECK(strcmp(r, ":2") == 0, "LMPOP: src list length decremented to 2", pass, fail);

    /* LMPOP 2 keys LEFT COUNT 2 */
    proxy_cmd(fd, r, sizeof(r), 7, "LMPOP", "2", "{mp}:a", "{mp}:b", "LEFT", "COUNT", "2");
    CHECK(r[0] == '*' && atoi(r + 1) == 2, "LMPOP 2 keys COUNT 2: returns *2 array response", pass, fail);
    if (r[0] == '*' && atoi(r + 1) == 2) drain_lmpop(fd);

    /* LMPOP on empty/non-existent keys → *-1 (null array) */
    proxy_cmd(fd, r, sizeof(r), 4, "LMPOP", "1", "{mp}:noexist", "LEFT");
    CHECK(strcmp(r, "*-1") == 0, "LMPOP: empty keylist returns null array", pass, fail);

    /* LMPOP cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 5, "LMPOP", "2", "{a}:lst", "{b}:lst", "LEFT");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "LMPOP cross-slot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
