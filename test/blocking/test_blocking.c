#include "test_common.h"

/*
 * Blocking command integration tests.
 *
 * Same-slot keys use a {tag} hash tag so CRC16({tag}) routes all keys in a
 * group to the same slot.  Cross-slot keys intentionally omit hash tags so
 * different keys land on different slots (confirmed by the existing BLPOP
 * cross-slot test).
 *
 * Commands covered: BLPOP  BRPOP  BRPOPLPUSH  BLMOVE
 *                   BLMPOP BZPOPMIN BZPOPMAX  BZMPOP
 */

void run_blocking_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Blocking Commands ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    char tmp[512];

    /* ══════════════════════════════════════════════════════════════════════
     * BLPOP
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BLPOP ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bl:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bl:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:bl:a", "x");
    proxy_cmd(fd, r, sizeof(r), 4, "BLPOP", "tc:bl:a", "tc:bl:b", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BLPOP multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bl:a");

    /* 2a. Same slot, first key has data */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{bl}:list1", "item1");
    proxy_cmd(fd, r, sizeof(r), 4, "BLPOP", "{bl}:list1", "{bl}:list2", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bl}:list1") == 0, "BLPOP multi-key same slot (first has data): returns first key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "item1") == 0, "BLPOP multi-key same slot (first has data): returns correct value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list2");

    /* 2b. Same slot, first key empty, second key has data */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{bl}:list2", "item2");
    proxy_cmd(fd, r, sizeof(r), 4, "BLPOP", "{bl}:list1", "{bl}:list2", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bl}:list2") == 0, "BLPOP multi-key same slot (second has data): returns second key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "item2") == 0, "BLPOP multi-key same slot (second has data): returns correct value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list2");

    /* 3a. Non-zero timeout with data — returns immediately */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{bl}:list", "ready");
    proxy_cmd(fd, r, sizeof(r), 3, "BLPOP", "{bl}:list", "5");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bl}:list") == 0, "BLPOP non-zero timeout with data: returns correct key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "ready") == 0, "BLPOP non-zero timeout with data: returns correct value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list");

    /* 3b. Non-zero timeout on empty key — blocks then returns nil array */
    proxy_cmd(fd, r, sizeof(r), 3, "BLPOP", "{bl}:list", "1");
    CHECK(strcmp(r, "*-1") == 0, "BLPOP non-zero timeout on empty key: returns *-1 after timeout", pass, fail);

    /* 4a. Zero timeout with data — pops from left (head) */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{bl}:list", "hello");
    proxy_cmd(fd, r, sizeof(r), 3, "BLPOP", "{bl}:list", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bl}:list") == 0, "BLPOP zero timeout with data: returns correct key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "hello") == 0, "BLPOP zero timeout with data: pops from left (head)", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bl}:list");

    /* ══════════════════════════════════════════════════════════════════════
     * BRPOP — mirrors BLPOP but pops from the right (tail)
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BRPOP ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:br:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:br:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:br:a", "x");
    proxy_cmd(fd, r, sizeof(r), 4, "BRPOP", "tc:br:a", "tc:br:b", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BRPOP multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:br:a");

    /* 2. Same slot, first key has data */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{br}:list1", "item1");
    proxy_cmd(fd, r, sizeof(r), 4, "BRPOP", "{br}:list1", "{br}:list2", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{br}:list1") == 0, "BRPOP multi-key same slot (first has data): returns first key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "item1") == 0, "BRPOP multi-key same slot (first has data): returns correct value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list2");

    /* 3. Non-zero timeout with data — pops from right (tail) */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{br}:list", "head");
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{br}:list", "tail");
    proxy_cmd(fd, r, sizeof(r), 3, "BRPOP", "{br}:list", "5");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{br}:list") == 0, "BRPOP non-zero timeout with data: returns correct key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "tail") == 0, "BRPOP non-zero timeout with data: pops from right (tail)", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list");

    /* 4. Non-zero timeout on empty key — returns *-1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BRPOP", "{br}:list", "1");
    CHECK(strcmp(r, "*-1") == 0, "BRPOP non-zero timeout on empty key: returns *-1 after timeout", pass, fail);

    /* 5. Zero timeout with data — pops from right (tail) */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{br}:list", "world");
    proxy_cmd(fd, r, sizeof(r), 3, "BRPOP", "{br}:list", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{br}:list") == 0, "BRPOP zero timeout with data: returns correct key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "world") == 0, "BRPOP zero timeout with data: pops from right (tail)", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{br}:list");

    /* ══════════════════════════════════════════════════════════════════════
     * BRPOPLPUSH  src dst timeout
     *   Pops from right of src and pushes to left of dst.
     *   src and dst must be on the same slot.
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BRPOPLPUSH ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{brpl}:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{brpl}:dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:brpl:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:brpl:dst");

    /* 1. Cross-slot src/dst → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:brpl:src", "x");
    proxy_cmd(fd, r, sizeof(r), 4, "BRPOPLPUSH", "tc:brpl:src", "tc:brpl:dst", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BRPOPLPUSH cross-slot src/dst: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:brpl:src");

    /* 2. Same slot, src has data — pops tail, pushes to head of dst */
    proxy_cmd(fd, r, sizeof(r), 4, "RPUSH", "{brpl}:src", "first", "second");
    proxy_cmd(fd, r, sizeof(r), 4, "BRPOPLPUSH", "{brpl}:src", "{brpl}:dst", "0");
    CHECK(strcmp(r, "second") == 0, "BRPOPLPUSH same slot with data: returns popped element", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "LRANGE", "{brpl}:dst", "0", "-1");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "second") == 0, "BRPOPLPUSH same slot with data: element pushed to head of dst", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{brpl}:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{brpl}:dst");

    /* 3. Empty src with timeout — returns nil array */
    proxy_cmd(fd, r, sizeof(r), 4, "BRPOPLPUSH", "{brpl}:src", "{brpl}:dst", "1");
    CHECK(strcmp(r, "*-1") == 0, "BRPOPLPUSH empty src with timeout: returns *-1 after timeout", pass, fail);

    /* ══════════════════════════════════════════════════════════════════════
     * BLMOVE  src dst LEFT|RIGHT LEFT|RIGHT timeout
     *   src and dst must be on the same slot.
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BLMOVE ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:dst");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blm:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blm:dst");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:blm:src", "x");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMOVE", "tc:blm:src", "tc:blm:dst", "LEFT", "RIGHT", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BLMOVE cross-slot src/dst: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blm:src");

    /* 2. Same slot, LEFT RIGHT — pops head of src, appends to tail of dst */
    proxy_cmd(fd, r, sizeof(r), 4, "RPUSH", "{blm}:src", "first", "second");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMOVE", "{blm}:src", "{blm}:dst", "LEFT", "RIGHT", "0");
    CHECK(strcmp(r, "first") == 0, "BLMOVE LEFT RIGHT: returns element popped from head of src", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "LRANGE", "{blm}:dst", "0", "-1");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "first") == 0, "BLMOVE LEFT RIGHT: element pushed to tail of dst", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:dst");

    /* 3. Same slot, RIGHT LEFT — pops tail of src, prepends to head of dst */
    proxy_cmd(fd, r, sizeof(r), 4, "RPUSH", "{blm}:src", "head", "tail");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMOVE", "{blm}:src", "{blm}:dst", "RIGHT", "LEFT", "0");
    CHECK(strcmp(r, "tail") == 0, "BLMOVE RIGHT LEFT: returns element popped from tail of src", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:src");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blm}:dst");

    /* 4. Empty src with timeout — returns nil array */
    proxy_cmd(fd, r, sizeof(r), 6, "BLMOVE", "{blm}:src", "{blm}:dst", "LEFT", "RIGHT", "1");
    CHECK(strcmp(r, "*-1") == 0, "BLMOVE empty src with timeout: returns *-1 after timeout", pass, fail);

    /* ══════════════════════════════════════════════════════════════════════
     * BLMPOP  timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
     *   Response: *2 → key name, *N → flat list of popped elements
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BLMPOP ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blmp:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blmp:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "tc:blmp:a", "x");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMPOP", "0", "2", "tc:blmp:a", "tc:blmp:b", "LEFT");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BLMPOP multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:blmp:a");

    /* 2. Same slot, first key has data, LEFT — pops head */
    proxy_cmd(fd, r, sizeof(r), 4, "RPUSH", "{blmp}:list1", "a", "b");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMPOP", "0", "2", "{blmp}:list1", "{blmp}:list2", "LEFT");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{blmp}:list1") == 0, "BLMPOP LEFT same slot (first has data): returns key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *1 — inner array header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* "a" */
    CHECK(strcmp(tmp, "a") == 0, "BLMPOP LEFT same slot (first has data): pops from left (head)", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list2");

    /* 3. Same slot, first key empty, second has data */
    proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", "{blmp}:list2", "x");
    proxy_cmd(fd, r, sizeof(r), 6, "BLMPOP", "0", "2", "{blmp}:list1", "{blmp}:list2", "LEFT");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{blmp}:list2") == 0, "BLMPOP LEFT (first empty, second has data): returns second key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *1 — inner array header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* "x" */
    CHECK(strcmp(tmp, "x") == 0, "BLMPOP LEFT (first empty, second has data): pops correct element", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{blmp}:list2");

    /* 4. Empty keys with timeout — returns *-1 */
    proxy_cmd(fd, r, sizeof(r), 5, "BLMPOP", "1", "1", "{blmp}:list", "LEFT");
    CHECK(strcmp(r, "*-1") == 0, "BLMPOP timeout on empty key: returns *-1", pass, fail);

    /* ══════════════════════════════════════════════════════════════════════
     * BZPOPMIN  key [key ...] timeout
     *   Response: *3 → key name, member, score  (or *-1 on timeout)
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BZPOPMIN ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmin:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmin:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "tc:bzmin:a", "1", "m");
    proxy_cmd(fd, r, sizeof(r), 4, "BZPOPMIN", "tc:bzmin:a", "tc:bzmin:b", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BZPOPMIN multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmin:a");

    /* 2. Same slot, first key has data — pops member with minimum score */
    proxy_cmd(fd, r, sizeof(r), 6, "ZADD", "{bzmin}:zset1", "1", "low", "10", "high");
    proxy_cmd(fd, r, sizeof(r), 4, "BZPOPMIN", "{bzmin}:zset1", "{bzmin}:zset2", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmin}:zset1") == 0, "BZPOPMIN same slot (first has data): returns key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "low") == 0, "BZPOPMIN same slot: pops member with minimum score", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "1") == 0, "BZPOPMIN same slot: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset2");

    /* 3. Non-zero timeout with data — returns immediately */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "{bzmin}:zset", "5", "member");
    proxy_cmd(fd, r, sizeof(r), 3, "BZPOPMIN", "{bzmin}:zset", "5");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmin}:zset") == 0, "BZPOPMIN non-zero timeout with data: returns correct key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "member") == 0, "BZPOPMIN non-zero timeout with data: returns correct member", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "5") == 0, "BZPOPMIN non-zero timeout with data: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmin}:zset");

    /* 4. Empty zset with timeout — returns *-1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BZPOPMIN", "{bzmin}:zset", "1");
    CHECK(strcmp(r, "*-1") == 0, "BZPOPMIN timeout on empty key: returns *-1", pass, fail);

    /* ══════════════════════════════════════════════════════════════════════
     * BZPOPMAX  key [key ...] timeout
     *   Response: *3 → key name, member, score  (or *-1 on timeout)
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BZPOPMAX ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmax:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmax:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "tc:bzmax:a", "1", "m");
    proxy_cmd(fd, r, sizeof(r), 4, "BZPOPMAX", "tc:bzmax:a", "tc:bzmax:b", "0");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BZPOPMAX multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmax:a");

    /* 2. Same slot, first key has data — pops member with maximum score */
    proxy_cmd(fd, r, sizeof(r), 6, "ZADD", "{bzmax}:zset1", "1", "low", "10", "high");
    proxy_cmd(fd, r, sizeof(r), 4, "BZPOPMAX", "{bzmax}:zset1", "{bzmax}:zset2", "0");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmax}:zset1") == 0, "BZPOPMAX same slot (first has data): returns key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "high") == 0, "BZPOPMAX same slot: pops member with maximum score", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "10") == 0, "BZPOPMAX same slot: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset2");

    /* 3. Non-zero timeout with data — returns immediately */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "{bzmax}:zset", "5", "member");
    proxy_cmd(fd, r, sizeof(r), 3, "BZPOPMAX", "{bzmax}:zset", "5");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmax}:zset") == 0, "BZPOPMAX non-zero timeout with data: returns correct key name", pass,
          fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "member") == 0, "BZPOPMAX non-zero timeout with data: returns correct member", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "5") == 0, "BZPOPMAX non-zero timeout with data: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmax}:zset");

    /* 4. Empty zset with timeout — returns *-1 */
    proxy_cmd(fd, r, sizeof(r), 3, "BZPOPMAX", "{bzmax}:zset", "1");
    CHECK(strcmp(r, "*-1") == 0, "BZPOPMAX timeout on empty key: returns *-1", pass, fail);

    /* ══════════════════════════════════════════════════════════════════════
     * BZMPOP  timeout numkeys key [key ...] MIN|MAX [COUNT count]
     *   Response: *2 → key name, *N → array of *2 [member, score] nested pairs
     *             (or *-1 on timeout)
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- BZMPOP ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset2");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmp:a");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmp:b");

    /* 1. Cross-slot → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 4, "ZADD", "tc:bzmp:a", "1", "m");
    proxy_cmd(fd, r, sizeof(r), 6, "BZMPOP", "0", "2", "tc:bzmp:a", "tc:bzmp:b", "MIN");
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "BZMPOP multi-key cross-slot: returns CROSSSLOT error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:bzmp:a");

    /* 2. Same slot, MIN — pops member with minimum score */
    proxy_cmd(fd, r, sizeof(r), 6, "ZADD", "{bzmp}:zset1", "1", "low", "10", "high");
    proxy_cmd(fd, r, sizeof(r), 6, "BZMPOP", "0", "2", "{bzmp}:zset1", "{bzmp}:zset2", "MIN");
    /* *2: key name, then *N array of *2 [member, score] nested pairs */
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmp}:zset1") == 0, "BZMPOP MIN same slot (first has data): returns key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *1 — count of elements popped */
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 — nested [member, score] pair header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* "low" */
    CHECK(strcmp(tmp, "low") == 0, "BZMPOP MIN same slot: pops member with minimum score", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* "1" */
    CHECK(strcmp(tmp, "1") == 0, "BZMPOP MIN same slot: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset2");

    /* 3. Same slot, MAX — pops member with maximum score */
    proxy_cmd(fd, r, sizeof(r), 6, "ZADD", "{bzmp}:zset", "1", "low", "10", "high");
    proxy_cmd(fd, r, sizeof(r), 5, "BZMPOP", "0", "1", "{bzmp}:zset", "MAX");
    resp_next_str(fd, tmp, sizeof(tmp));
    CHECK(strcmp(tmp, "{bzmp}:zset") == 0, "BZMPOP MAX: returns key name", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *1 — count of elements popped */
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 — nested [member, score] pair header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* "high" */
    CHECK(strcmp(tmp, "high") == 0, "BZMPOP MAX: pops member with maximum score", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* "10" */
    CHECK(strcmp(tmp, "10") == 0, "BZMPOP MAX: returns correct score", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{bzmp}:zset");

    /* 4. Empty zsets with timeout — returns *-1 */
    proxy_cmd(fd, r, sizeof(r), 5, "BZMPOP", "1", "1", "{bzmp}:zset", "MIN");
    CHECK(strcmp(r, "*-1") == 0, "BZMPOP timeout on empty key: returns *-1", pass, fail);

    close(fd);
}
