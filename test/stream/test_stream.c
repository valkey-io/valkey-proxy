#include "test_common.h"

/*
 * Recursively drain one complete RESP value.
 * For *N arrays: reads the header then recurses N times.
 * For bulk strings / simple replies: resp_next_str already reads them fully.
 */
static void drain_resp(int fd) {
    char tmp[512];
    if (!resp_next_str(fd, tmp, sizeof(tmp))) return;
    if (tmp[0] == '*') {
        int n = atoi(tmp + 1);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }
}

/*
 * After the outer *N array header of an XREAD/XREADGROUP response has been
 * consumed, drain `nstreams` stream-result pairs.
 * Each pair is: *2 [ $stream-name, *M [ entry ... ] ]
 */
static void drain_xread_streams(int fd, int nstreams) {
    char tmp[512];
    for (int i = 0; i < nstreams; i++) {
        resp_next_str(fd, tmp, sizeof(tmp)); /* *2  stream-pair header   */
        resp_next_str(fd, tmp, sizeof(tmp)); /* $.. stream name           */
        resp_next_str(fd, tmp, sizeof(tmp)); /* *M  entries array header  */
        int m = (tmp[0] == '*') ? atoi(tmp + 1) : 0;
        for (int j = 0; j < m; j++) drain_resp(fd); /* each entry: [id, fields]  */
    }
}

void run_stream_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Stream Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    char tmp[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── XADD ─────────────────────────────────────────────────────────── */
    /* odd number of field-value args must return an error */
    proxy_cmd(fd, r, sizeof(r), 6, "XADD", "tc:st:s1", "*", "f1", "v1", "f2");
    CHECK(r[0] == '-', "XADD: odd field-value count returns error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:s1", "*", "f1", "hello", "f2", "world");
    CHECK(r[0] != '-' && r[0] != '\0', "XADD: returns entry ID", pass, fail);
    char id1[512];
    snprintf(id1, sizeof(id1), "%s", r);

    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:s1", "*", "name", "alice", "age", "30");
    CHECK(r[0] != '-', "XADD: second entry added", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:s1", "*", "name", "bob", "age", "25");
    CHECK(r[0] != '-', "XADD: third entry added", pass, fail);

    /* ── XLEN ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "XLEN", "tc:st:s1");
    CHECK(strcmp(r, ":3") == 0, "XLEN: 3 entries returns 3", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "XLEN", "tc:st:nosuchkey");
    CHECK(strcmp(r, ":0") == 0, "XLEN: non-existent stream returns 0", pass, fail);

    /* ── XRANGE ───────────────────────────────────────────────────────── */
    resp_send(fd, 4, "XRANGE", "tc:st:s1", "-", "+");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "XRANGE: all entries returns 3-element array", pass, fail);
    /* each entry is *2 [id, fields_array] — use recursive drain */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    resp_send(fd, 6, "XRANGE", "tc:st:s1", "-", "+", "COUNT", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "XRANGE COUNT 2: returns 2 entries", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    resp_send(fd, 4, "XRANGE", "tc:st:s1", id1, id1);
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XRANGE: exact ID match returns 1 entry", pass, fail);
    drain_resp(fd);

    /* ── XREVRANGE ────────────────────────────────────────────────────── */
    resp_send(fd, 4, "XREVRANGE", "tc:st:s1", "+", "-");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "XREVRANGE: all entries returns 3 in reverse", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    resp_send(fd, 6, "XREVRANGE", "tc:st:s1", "+", "-", "COUNT", "1");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREVRANGE COUNT 1: returns 1 entry", pass, fail);
    drain_resp(fd);

    /* ── XREAD single key ─────────────────────────────────────────────── */
    /*
     * XREAD response layout:
     *   *N                    <- N streams
     *     *2                  <- stream-pair: [name, entries]
     *       $stream-name
     *       *M                <- M entries
     *         *2              <- entry: [id, fields_array]
     *           ...
     *
     * resp_next_str only reads ONE token at a time; we must step through
     * the three header tokens (*N, *2, $name) before getting to *M.
     */
    resp_send(fd, 4, "XREAD", "STREAMS", "tc:st:s1", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREAD single stream: outer array has 1 stream", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2  stream-pair header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* $.. stream name        */
    resp_next_str(fd, r, sizeof(r));     /* *N  entries header     */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "XREAD single stream: 3 entries", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* XREAD with COUNT */
    resp_send(fd, 6, "XREAD", "COUNT", "2", "STREAMS", "tc:st:s1", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREAD COUNT 2: outer array has 1 stream", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 stream-pair */
    resp_next_str(fd, tmp, sizeof(tmp)); /* stream name    */
    resp_next_str(fd, r, sizeof(r));     /* *N entries     */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 2, "XREAD COUNT 2: 2 entries returned", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XREAD multi-key same slot (hash tag) ─────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "{tc:st}:ms1", "*", "k", "v1", "x", "1");
    CHECK(r[0] != '-', "XADD: multi-slot stream 1 created", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "{tc:st}:ms2", "*", "k", "v2", "x", "2");
    CHECK(r[0] != '-', "XADD: multi-slot stream 2 created", pass, fail);

    resp_send(fd, 6, "XREAD", "STREAMS", "{tc:st}:ms1", "{tc:st}:ms2", "0-0", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "XREAD 2 keys same slot: outer array has 2 streams", pass, fail);
    /* drain both stream results using the helper */
    drain_xread_streams(fd, 2);

    /* ── XREAD different slots → CROSSSLOT error ──────────────────────── */
    /* tc:st:cross1 = slot 2743, tc:st:cross2 = slot 15060 (guaranteed different) */
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "tc:st:cross1", "*", "k", "v");
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "tc:st:cross2", "*", "k", "v");
    resp_send(fd, 6, "XREAD", "STREAMS", "tc:st:cross1", "tc:st:cross2", "0-0", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "XREAD different slots: returns CROSSSLOT error", pass, fail);

    /* ── XSETID (unsupported — rejected locally by the proxy) ──────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "XSETID", "tc:st:s1", "9999999999999-0");
    CHECK(r[0] == '-' && strstr(r, "unsupport command") != NULL, "XSETID: unsupported command returns error", pass,
          fail);

    /* ── XDEL ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "XDEL", "tc:st:s1", id1);
    CHECK(strcmp(r, ":1") == 0, "XDEL: delete existing entry returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "XDEL", "tc:st:s1", "0-1");
    CHECK(strcmp(r, ":0") == 0, "XDEL: delete non-existent entry returns 0", pass, fail);

    /* ── XTRIM ────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:st:trim");
    for (int i = 0; i < 10; i++) proxy_cmd(fd, r, sizeof(r), 5, "XADD", "tc:st:trim", "*", "n", "v");

    /* XTRIM returns the number of entries *removed* (10 - 5 = 5) */
    proxy_cmd(fd, r, sizeof(r), 4, "XTRIM", "tc:st:trim", "MAXLEN", "5");
    CHECK(strcmp(r, ":5") == 0, "XTRIM MAXLEN 5: removes 5 entries, returns 5", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "XLEN", "tc:st:trim");
    CHECK(strcmp(r, ":5") == 0, "XTRIM MAXLEN 5: stream now has 5 entries", pass, fail);

    /* ── XGROUP CREATE ────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:st:grp");
    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:grp", "*", "f", "v1", "x", "1");
    char grp_id1[512];
    snprintf(grp_id1, sizeof(grp_id1), "%s", r);
    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:grp", "*", "f", "v2", "x", "2");
    char grp_id2[512];
    snprintf(grp_id2, sizeof(grp_id2), "%s", r);
    proxy_cmd(fd, r, sizeof(r), 7, "XADD", "tc:st:grp", "*", "f", "v3", "x", "3");
    char grp_id3[512];
    snprintf(grp_id3, sizeof(grp_id3), "%s", r);

    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATE", "tc:st:grp", "grp1", "0");
    CHECK(strcmp(r, "+OK") == 0, "XGROUP CREATE: returns OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATE", "tc:st:grp", "grp1", "0");
    CHECK(r[0] == '-', "XGROUP CREATE: duplicate group returns error", pass, fail);

    /* ── XGROUP CREATE with MKSTREAM ──────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 6, "XGROUP", "CREATE", "tc:st:newstrm", "g1", "$", "MKSTREAM");
    CHECK(strcmp(r, "+OK") == 0, "XGROUP CREATE MKSTREAM: creates stream and group", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "XLEN", "tc:st:newstrm");
    CHECK(strcmp(r, ":0") == 0, "XGROUP CREATE MKSTREAM: new stream is empty", pass, fail);

    /* ── XREADGROUP ───────────────────────────────────────────────────── */
    /*
     * XREADGROUP response has the same layout as XREAD:
     *   *N streams → *2 [name, *M entries] → *2 [id, fields_array]
     * Same three-step header read before we reach the entry count.
     */
    resp_send(fd, 7, "XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "tc:st:grp", ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREADGROUP: outer array has 1 stream", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 stream-pair  */
    resp_next_str(fd, tmp, sizeof(tmp)); /* stream name     */
    resp_next_str(fd, r, sizeof(r));     /* *N entries      */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "XREADGROUP: all 3 entries delivered", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* second call with > returns nil/empty — nothing new to deliver */
    resp_send(fd, 7, "XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "tc:st:grp", ">");
    resp_next_str(fd, r, sizeof(r));
    /* nil array (*-1) or empty array (*0) depending on server version */
    CHECK(r[0] == '*' && atoi(r + 1) <= 0, "XREADGROUP: no new entries after all delivered", pass, fail);

    /* ── XPENDING ─────────────────────────────────────────────────────── */
    /*
     * XPENDING key group start end COUNT  (COUNT is required for range form)
     * Each pending entry: *4 [id, consumer, idle-ms, delivery-count]
     */
    resp_send(fd, 6, "XPENDING", "tc:st:grp", "grp1", "-", "+", "10");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 3, "XPENDING: 3 pending entries for consumer1", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XACK ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "XACK", "tc:st:grp", "grp1", grp_id1);
    CHECK(strcmp(r, ":1") == 0, "XACK: acknowledge one entry returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "XACK", "tc:st:grp", "grp1", grp_id1);
    CHECK(strcmp(r, ":0") == 0, "XACK: re-acknowledge returns 0", pass, fail);

    /* ── XCLAIM ───────────────────────────────────────────────────────── */
    /* claim grp_id2 and grp_id3 for consumer2; min-idle-time=0 */
    resp_send(fd, 7, "XCLAIM", "tc:st:grp", "grp1", "consumer2", "0", grp_id2, grp_id3);
    resp_next_str(fd, r, sizeof(r));
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 2, "XCLAIM: claimed 2 entries for consumer2", pass, fail);
        /* each claimed entry is *2 [id, fields_array] */
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XAUTOCLAIM ───────────────────────────────────────────────────── */
    /* XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
     * Returns *3 [next-start-id, claimed-entries-array, deleted-ids-array] */
    resp_send(fd, 6, "XAUTOCLAIM", "tc:st:grp", "grp1", "consumer1", "0", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*3") == 0, "XAUTOCLAIM: returns 3-element reply", pass, fail);
    drain_resp(fd); /* next-start-id (bulk string)       */
    drain_resp(fd); /* claimed entries (*N of entries)   */
    drain_resp(fd); /* deleted entry ids (*N of strings) */

    /* ── XINFO STREAM ─────────────────────────────────────────────────── */
    /*
     * Returns a flat *N array of key-value pairs, but "first-entry" and
     * "last-entry" values are themselves *2 arrays — drain_resp handles that.
     */
    resp_send(fd, 3, "XINFO", "STREAM", "tc:st:grp");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*', "XINFO STREAM: returns array", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XINFO GROUPS ─────────────────────────────────────────────────── */
    resp_send(fd, 3, "XINFO", "GROUPS", "tc:st:grp");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*', "XINFO GROUPS: returns array", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        /* each group is a sub-array of flat field-value pairs */
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XINFO CONSUMERS ──────────────────────────────────────────────── */
    resp_send(fd, 4, "XINFO", "CONSUMERS", "tc:st:grp", "grp1");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*', "XINFO CONSUMERS: returns array", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XINFO HELP ───────────────────────────────────────────────────── */
    resp_send(fd, 2, "XINFO", "HELP");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*', "XINFO HELP: returns array", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XGROUP SETID ─────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "SETID", "tc:st:grp", "grp1", "0");
    CHECK(strcmp(r, "+OK") == 0, "XGROUP SETID: returns OK", pass, fail);

    /* ── XGROUP CREATECONSUMER ────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATECONSUMER", "tc:st:grp", "grp1", "newconsumer");
    CHECK(strcmp(r, ":1") == 0, "XGROUP CREATECONSUMER: new consumer returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATECONSUMER", "tc:st:grp", "grp1", "newconsumer");
    CHECK(strcmp(r, ":0") == 0, "XGROUP CREATECONSUMER: existing consumer returns 0", pass, fail);

    /* ── XGROUP DELCONSUMER ───────────────────────────────────────────── */
    /* newconsumer has 0 pending entries → returns 0 */
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "DELCONSUMER", "tc:st:grp", "grp1", "newconsumer");
    CHECK(strcmp(r, ":0") == 0, "XGROUP DELCONSUMER: returns pending-count (0)", pass, fail);

    /* ── XGROUP DESTROY ───────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "XGROUP", "DESTROY", "tc:st:grp", "grp1");
    CHECK(strcmp(r, ":1") == 0, "XGROUP DESTROY: returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "XGROUP", "DESTROY", "tc:st:grp", "grp1");
    CHECK(strcmp(r, ":0") == 0, "XGROUP DESTROY: non-existent group returns 0", pass, fail);

    /* ── XGROUP HELP ──────────────────────────────────────────────────── */
    resp_send(fd, 2, "XGROUP", "HELP");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '*', "XGROUP HELP: returns array", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* ── XREADGROUP multi-key same slot ───────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:st}:mg1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:st}:mg2");
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "{tc:st}:mg1", "*", "k", "a");
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "{tc:st}:mg2", "*", "k", "b");
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATE", "{tc:st}:mg1", "g1", "0");
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATE", "{tc:st}:mg2", "g1", "0");

    resp_send(fd, 9, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "{tc:st}:mg1", "{tc:st}:mg2", ">", ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "XREADGROUP 2 keys same slot: outer array has 2 streams", pass, fail);
    drain_xread_streams(fd, 2);

    /* ── XREADGROUP different slots → CROSSSLOT error ─────────────────── */
    /* tc:st:cross1 (slot 2743) and tc:st:cross2 (slot 15060); proxy rejects
     * before reaching the backend so groups need not exist on these streams. */
    resp_send(fd, 9, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "tc:st:cross1", "tc:st:cross2", ">", ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "XREADGROUP different slots: returns CROSSSLOT error", pass,
          fail);

    /* ── XREAD BLOCK ──────────────────────────────────────────────────── */
    /*
     * XREAD BLOCK milliseconds STREAMS key [key ...] id [id ...]
     *
     * When the BLOCK keyword is present, the proxy routes via POOL_BLOCKING.
     * Cross-slot keys return CROSSSLOT immediately (proxy-side check).
     * Timeout on empty stream returns *-1 (nil array).
     * With existing data the response format is identical to non-blocking XREAD.
     */
    printf("\n--- XREAD BLOCK ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{xs}:bl1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{xs}:empty");

    /* 1. Cross-slot → CROSSSLOT error */
    resp_send(fd, 8, "XREAD", "BLOCK", "0", "STREAMS", "tc:xs:bl:a", "tc:xs:bl:b", "0-0", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "XREAD BLOCK cross-slot: returns CROSSSLOT error", pass, fail);

    /* 2. Single key, existing data, BLOCK 0 → returns immediately */
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "{xs}:bl1", "*", "field", "value");
    resp_send(fd, 6, "XREAD", "BLOCK", "0", "STREAMS", "{xs}:bl1", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREAD BLOCK 0 with data: outer array has 1 stream", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 stream-pair header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* stream name           */
    CHECK(strcmp(tmp, "{xs}:bl1") == 0, "XREAD BLOCK 0 with data: returns correct stream name", pass, fail);
    resp_next_str(fd, r, sizeof(r)); /* *N entries header     */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 1, "XREAD BLOCK 0 with data: 1 entry returned", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* 3. Non-zero timeout, empty stream → *-1 after timeout */
    resp_send(fd, 6, "XREAD", "BLOCK", "1000", "STREAMS", "{xs}:empty", "0-0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*-1") == 0, "XREAD BLOCK timeout on empty stream: returns *-1", pass, fail);

    /* ── XREADGROUP BLOCK ─────────────────────────────────────────────── */
    /*
     * XREADGROUP GROUP group consumer BLOCK milliseconds STREAMS key id
     *
     * '>' delivers new (not yet delivered) messages.
     * Cross-slot keys return CROSSSLOT immediately.
     * When no new messages arrive within the timeout, returns *-1.
     */
    printf("\n--- XREADGROUP BLOCK ---\n");

    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{xg}:bl1");

    /* 1. Cross-slot → CROSSSLOT error */
    resp_send(fd, 11, "XREADGROUP", "GROUP", "grpbl", "c1", "BLOCK", "0", "STREAMS", "tc:xg:bl:a", "tc:xg:bl:b", ">",
              ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-' && strstr(r, "CROSSSLOT") != NULL, "XREADGROUP BLOCK cross-slot: returns CROSSSLOT error", pass,
          fail);

    /* 2. New message available, BLOCK 0 → returns immediately */
    proxy_cmd(fd, r, sizeof(r), 5, "XADD", "{xg}:bl1", "*", "f", "v1");
    proxy_cmd(fd, r, sizeof(r), 5, "XGROUP", "CREATE", "{xg}:bl1", "grpbl", "0");
    resp_send(fd, 9, "XREADGROUP", "GROUP", "grpbl", "c1", "BLOCK", "0", "STREAMS", "{xg}:bl1", ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "XREADGROUP BLOCK 0 with data: outer array has 1 stream", pass, fail);
    resp_next_str(fd, tmp, sizeof(tmp)); /* *2 stream-pair header */
    resp_next_str(fd, tmp, sizeof(tmp)); /* stream name           */
    CHECK(strcmp(tmp, "{xg}:bl1") == 0, "XREADGROUP BLOCK 0 with data: returns correct stream name", pass, fail);
    resp_next_str(fd, r, sizeof(r)); /* *N entries header     */
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(n == 1, "XREADGROUP BLOCK 0 with data: 1 entry returned", pass, fail);
        for (int i = 0; i < n; i++) drain_resp(fd);
    }

    /* 3. No new messages, non-zero timeout → *-1 after timeout */
    /* all entries already delivered to c1 above; no new entries exist */
    resp_send(fd, 9, "XREADGROUP", "GROUP", "grpbl", "c1", "BLOCK", "1000", "STREAMS", "{xg}:bl1", ">");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*-1") == 0, "XREADGROUP BLOCK timeout on no-new-messages: returns *-1", pass, fail);

    close(fd);
}
