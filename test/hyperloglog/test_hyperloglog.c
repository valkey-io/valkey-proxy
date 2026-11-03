#include "test_common.h"

void run_hyperloglog_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== HyperLogLog Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── PFADD ────────────────────────────────────────────────────────────── */

    /* adding new elements modifies the HLL → returns 1 */
    proxy_cmd(fd, r, sizeof(r), 6, "PFADD", "tc:hll:a", "a", "b", "c", "d");
    CHECK(strcmp(r, ":1") == 0, "PFADD: new elements returns 1", pass, fail);

    /* re-adding identical elements does not modify the HLL → returns 0 */
    proxy_cmd(fd, r, sizeof(r), 6, "PFADD", "tc:hll:a", "a", "b", "c", "d");
    CHECK(strcmp(r, ":0") == 0, "PFADD: re-adding same elements returns 0", pass, fail);

    /* adding one more new element → returns 1 */
    proxy_cmd(fd, r, sizeof(r), 3, "PFADD", "tc:hll:a", "e");
    CHECK(strcmp(r, ":1") == 0, "PFADD: adding one new element returns 1", pass, fail);

    /* PFADD with no elements on a new key creates the key → returns 1 */
    proxy_cmd(fd, r, sizeof(r), 2, "PFADD", "tc:hll:newkey");
    CHECK(strcmp(r, ":1") == 0, "PFADD: no elements on new key returns 1 (key created)", pass, fail);

    /* PFADD with no elements on existing key → HLL unchanged → returns 0 */
    proxy_cmd(fd, r, sizeof(r), 2, "PFADD", "tc:hll:newkey");
    CHECK(strcmp(r, ":0") == 0, "PFADD: no elements on existing key returns 0", pass, fail);

    /* ── PFCOUNT ──────────────────────────────────────────────────────────── */

    /* tc:hll:a has {a,b,c,d,e} — 5 unique elements; sparse encoding is exact */
    proxy_cmd(fd, r, sizeof(r), 2, "PFCOUNT", "tc:hll:a");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 4 && v <= 6, "PFCOUNT: 5 distinct elements → count in [4, 6]", pass, fail);
    }

    /* non-existent key returns 0 */
    proxy_cmd(fd, r, sizeof(r), 2, "PFCOUNT", "tc:hll:noexist");
    CHECK(strcmp(r, ":0") == 0, "PFCOUNT: non-existent key returns 0", pass, fail);

    /* set up two hash-tagged keys on the same slot for multi-key tests */
    proxy_cmd(fd, r, sizeof(r), 6, "PFADD", "{hll}:x", "a", "b", "c", "d");
    proxy_cmd(fd, r, sizeof(r), 5, "PFADD", "{hll}:y", "e", "f", "g");

    /* PFCOUNT on two same-slot keys → union cardinality */
    proxy_cmd(fd, r, sizeof(r), 3, "PFCOUNT", "{hll}:x", "{hll}:y");
    {
        long long v = atoll(r + 1);
        /* union of {a,b,c,d} and {e,f,g} = 7 unique elements */
        CHECK(r[0] == ':' && v >= 5 && v <= 9, "PFCOUNT multi same-slot: union of 7 elements in [5, 9]", pass, fail);
    }

    /* PFCOUNT on three same-slot keys */
    proxy_cmd(fd, r, sizeof(r), 3, "PFADD", "{hll}:z", "h", "i");
    proxy_cmd(fd, r, sizeof(r), 4, "PFCOUNT", "{hll}:x", "{hll}:y", "{hll}:z");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 7 && v <= 11, "PFCOUNT 3 same-slot keys: union of 9 elements in [7, 11]", pass, fail);
    }

    /* PFCOUNT with cross-slot keys → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 3, "PFCOUNT", "{src}:hll", "{dst}:hll");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "PFCOUNT cross-slot: returns CROSSSLOT error", pass, fail);

    /* ── PFMERGE ──────────────────────────────────────────────────────────── */

    /* merge two same-slot sources into a destination */
    proxy_cmd(fd, r, sizeof(r), 4, "PFMERGE", "{hll}:merged", "{hll}:x", "{hll}:y");
    CHECK(strcmp(r, "+OK") == 0, "PFMERGE same slot: returns +OK", pass, fail);

    /* merged key contains union of source elements */
    proxy_cmd(fd, r, sizeof(r), 2, "PFCOUNT", "{hll}:merged");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 5, "PFMERGE: merged count >= 5 (union of sources)", pass, fail);
    }

    /* PFMERGE with a single source (copy) */
    proxy_cmd(fd, r, sizeof(r), 3, "PFMERGE", "{hll}:copy", "{hll}:x");
    CHECK(strcmp(r, "+OK") == 0, "PFMERGE single source: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "PFCOUNT", "{hll}:copy");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 2, "PFMERGE single source: copy count >= 2", pass, fail);
    }

    /* PFMERGE where dest = one of the sources (self-merge) */
    proxy_cmd(fd, r, sizeof(r), 4, "PFMERGE", "{hll}:x", "{hll}:x", "{hll}:y");
    CHECK(strcmp(r, "+OK") == 0, "PFMERGE: dest == one source returns +OK", pass, fail);

    /* PFMERGE with cross-slot keys → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 4, "PFMERGE", "{src}:dst", "{src}:s1", "{dst}:s2");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "PFMERGE cross-slot: returns CROSSSLOT error", pass, fail);

    /* dest cross-slot from source also triggers error */
    proxy_cmd(fd, r, sizeof(r), 3, "PFMERGE", "{dst}:out", "{src}:in");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "PFMERGE dest cross-slot from source: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
