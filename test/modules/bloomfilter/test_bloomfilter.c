#include "test_common.h"

void run_bloomfilter_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Bloom Filter Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── BF.RESERVE ──────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "BF.RESERVE", "tc:bf:reserve", "0.001", "1000");
    CHECK(strcmp(r, "+OK") == 0, "BF.RESERVE: creates filter with error_rate=0.001 capacity=1000", pass, fail);

    /* reserving the same key again returns an error */
    proxy_cmd(fd, r, sizeof(r), 4, "BF.RESERVE", "tc:bf:reserve", "0.001", "1000");
    CHECK(r[0] == '-', "BF.RESERVE: duplicate key returns error", pass, fail);

    /* with EXPANSION option */
    proxy_cmd(fd, r, sizeof(r), 6, "BF.RESERVE", "tc:bf:reserve_exp", "0.01", "500", "EXPANSION", "2");
    CHECK(strcmp(r, "+OK") == 0, "BF.RESERVE: EXPANSION option accepted", pass, fail);

    /* with NONSCALING option */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.RESERVE", "tc:bf:reserve_ns", "0.01", "200", "NONSCALING");
    CHECK(strcmp(r, "+OK") == 0, "BF.RESERVE: NONSCALING option accepted", pass, fail);

    /* ── BF.ADD ──────────────────────────────────────────────────────────── */

    /* auto-create filter and add first item → returns 1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.ADD", "tc:bf:add", "item1");
    CHECK(strcmp(r, ":1") == 0, "BF.ADD: new item returns 1", pass, fail);

    /* re-adding the same item → returns 0 */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.ADD", "tc:bf:add", "item1");
    CHECK(strcmp(r, ":0") == 0, "BF.ADD: duplicate item returns 0", pass, fail);

    /* add a second distinct item → returns 1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.ADD", "tc:bf:add", "item2");
    CHECK(strcmp(r, ":1") == 0, "BF.ADD: second new item returns 1", pass, fail);

    /* add to a pre-reserved filter */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.ADD", "tc:bf:reserve", "hello");
    CHECK(strcmp(r, ":1") == 0, "BF.ADD: add to pre-reserved filter returns 1", pass, fail);

    /* ── BF.EXISTS ───────────────────────────────────────────────────────── */

    /* item that was added → 1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.EXISTS", "tc:bf:add", "item1");
    CHECK(strcmp(r, ":1") == 0, "BF.EXISTS: present item returns 1", pass, fail);

    /* item that was never added → 0 (definite non-member) */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.EXISTS", "tc:bf:add", "nothere");
    CHECK(strcmp(r, ":0") == 0, "BF.EXISTS: absent item returns 0", pass, fail);

    /* non-existent key → 0 */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.EXISTS", "tc:bf:noexist", "item1");
    CHECK(strcmp(r, ":0") == 0, "BF.EXISTS: non-existent key returns 0", pass, fail);

    /* ── BF.CARD ─────────────────────────────────────────────────────────── */

    /* non-existent key → 0 */
    proxy_cmd(fd, r, sizeof(r), 2, "BF.CARD", "tc:bf:noexist");
    CHECK(strcmp(r, ":0") == 0, "BF.CARD: non-existent key returns 0", pass, fail);

    /* after adding 2 items → at least 1 (Bloom filter is probabilistic) */
    proxy_cmd(fd, r, sizeof(r), 2, "BF.CARD", "tc:bf:add");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 1, "BF.CARD: 2 items added → count >= 1", pass, fail);
    }

    /* ── BF.INFO ─────────────────────────────────────────────────────────── */

    /* BF.INFO key — returns array of field/value pairs */
    proxy_cmd(fd, r, sizeof(r), 2, "BF.INFO", "tc:bf:reserve");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n > 0, "BF.INFO: full info returns non-empty array", pass, fail);
        /* drain all elements */
        char tmp[512];
        for (int i = 0; i < n; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    /* BF.INFO key CAPACITY → integer equal to reserved capacity */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.INFO", "tc:bf:reserve", "CAPACITY");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v == 1000, "BF.INFO CAPACITY: returns reserved capacity 1000", pass, fail);
    }

    /* BF.INFO key ITEMS → item count after 1 add */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.INFO", "tc:bf:reserve", "ITEMS");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 1, "BF.INFO ITEMS: count >= 1 after one BF.ADD", pass, fail);
    }

    /* BF.INFO key FILTERS → at least 1 filter layer */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.INFO", "tc:bf:reserve", "FILTERS");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 1, "BF.INFO FILTERS: at least 1 filter layer", pass, fail);
    }

    /* BF.INFO key SIZE → positive byte count */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.INFO", "tc:bf:reserve", "SIZE");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v > 0, "BF.INFO SIZE: positive byte size", pass, fail);
    }

    /* BF.INFO on non-existent key → error */
    proxy_cmd(fd, r, sizeof(r), 2, "BF.INFO", "tc:bf:noexist");
    CHECK(r[0] == '-', "BF.INFO: non-existent key returns error", pass, fail);

    /* ── BF.MADD ─────────────────────────────────────────────────────────── */

    /* add 3 new items → all return 1 */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.MADD", "tc:bf:madd", "a", "b", "c");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 3, "BF.MADD: 3 items → *3 array", pass, fail);
        char v[3][64];
        for (int i = 0; i < 3; i++) resp_next_str(fd, v[i], sizeof(v[i]));
        CHECK(strcmp(v[0], ":1") == 0 && strcmp(v[1], ":1") == 0 && strcmp(v[2], ":1") == 0,
              "BF.MADD: all 3 new items return 1", pass, fail);
    }

    /* re-add same items → all return 0 */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.MADD", "tc:bf:madd", "a", "b", "c");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 3, "BF.MADD re-add: *3 array", pass, fail);
        char v[3][64];
        for (int i = 0; i < 3; i++) resp_next_str(fd, v[i], sizeof(v[i]));
        CHECK(strcmp(v[0], ":0") == 0 && strcmp(v[1], ":0") == 0 && strcmp(v[2], ":0") == 0,
              "BF.MADD re-add: all 3 existing items return 0", pass, fail);
    }

    /* mixed: one new item, two existing */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.MADD", "tc:bf:madd", "a", "d", "b");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 3, "BF.MADD mixed: *3 array", pass, fail);
        char v0[64], v1[64], v2[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        resp_next_str(fd, v2, sizeof(v2));
        CHECK(strcmp(v0, ":0") == 0, "BF.MADD mixed: existing item 'a' returns 0", pass, fail);
        CHECK(strcmp(v1, ":1") == 0, "BF.MADD mixed: new item 'd' returns 1", pass, fail);
        CHECK(strcmp(v2, ":0") == 0, "BF.MADD mixed: existing item 'b' returns 0", pass, fail);
    }

    /* ── BF.MEXISTS ──────────────────────────────────────────────────────── */

    /* check 3 items: 2 present, 1 absent */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.MEXISTS", "tc:bf:madd", "a", "nothere", "b");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 3, "BF.MEXISTS: 3 items → *3 array", pass, fail);
        char v0[64], v1[64], v2[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        resp_next_str(fd, v2, sizeof(v2));
        CHECK(strcmp(v0, ":1") == 0, "BF.MEXISTS: present item 'a' returns 1", pass, fail);
        CHECK(strcmp(v1, ":0") == 0, "BF.MEXISTS: absent item 'nothere' returns 0", pass, fail);
        CHECK(strcmp(v2, ":1") == 0, "BF.MEXISTS: present item 'b' returns 1", pass, fail);
    }

    /* all items absent (new key) → all 0 */
    proxy_cmd(fd, r, sizeof(r), 4, "BF.MEXISTS", "tc:bf:noexist2", "x", "y");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 2, "BF.MEXISTS non-existent key: *2 array", pass, fail);
        char v0[64], v1[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        CHECK(strcmp(v0, ":0") == 0 && strcmp(v1, ":0") == 0, "BF.MEXISTS non-existent key: all 0", pass, fail);
    }

    /* ── BF.INSERT ───────────────────────────────────────────────────────── */

    /* basic insert: auto-create and add 2 items */
    proxy_cmd(fd, r, sizeof(r), 5, "BF.INSERT", "tc:bf:insert", "ITEMS", "x", "y");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 2, "BF.INSERT basic: 2 items → *2 array", pass, fail);
        char v0[64], v1[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        CHECK(strcmp(v0, ":1") == 0 && strcmp(v1, ":1") == 0, "BF.INSERT basic: both new items return 1", pass, fail);
    }

    /* insert with CAPACITY and ERROR options */
    proxy_cmd(fd, r, sizeof(r), 9, "BF.INSERT", "tc:bf:insert_opts", "CAPACITY", "2000", "ERROR", "0.005", "ITEMS", "p",
              "q");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 2, "BF.INSERT with CAPACITY/ERROR: *2 array", pass, fail);
        char v0[64], v1[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        CHECK(strcmp(v0, ":1") == 0 && strcmp(v1, ":1") == 0, "BF.INSERT with CAPACITY/ERROR: both items return 1",
              pass, fail);
    }

    /* verify inserted capacity */
    proxy_cmd(fd, r, sizeof(r), 3, "BF.INFO", "tc:bf:insert_opts", "CAPACITY");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v == 2000, "BF.INSERT with CAPACITY=2000: BF.INFO CAPACITY returns 2000", pass, fail);
    }

    /* NOCREATE: error if key does not exist */
    proxy_cmd(fd, r, sizeof(r), 6, "BF.INSERT", "tc:bf:nocreate_new", "NOCREATE", "ITEMS", "a", "b");
    CHECK(r[0] == '-', "BF.INSERT NOCREATE: error when key does not exist", pass, fail);

    /* NOCREATE on existing key: works fine */
    proxy_cmd(fd, r, sizeof(r), 6, "BF.INSERT", "tc:bf:insert", "NOCREATE", "ITEMS", "z", "x");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 2, "BF.INSERT NOCREATE on existing key: *2 array", pass, fail);
        char v0[64], v1[64];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        CHECK(strcmp(v0, ":1") == 0, "BF.INSERT NOCREATE: new item 'z' returns 1", pass, fail);
        CHECK(strcmp(v1, ":0") == 0, "BF.INSERT NOCREATE: existing item 'x' returns 0", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
