#include "test_common.h"

void run_search_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Search (FT.*) Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    const char *idxs[] = {"my_index_1", "my_index_2", "my_index_3"};
    const int nidx = 3;

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    /* clean up stale indexes left over from a previous failed run; result
     * (+OK or -ERR no such index) is irrelevant, just don't assert on it */
    for (int i = 0; i < nidx; i++) proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", idxs[i]);

    /* ── FT._LIST (before creation) ─────────────────────────────────────── */

    /* resp_array_contains() consumes the whole array, so re-issue FT._LIST
     * for every index checked */
    for (int i = 0; i < nidx; i++) {
        char desc[128];
        proxy_cmd(fd, r, sizeof(r), 1, "FT._LIST");
        CHECK(r[0] == '*', "FT._LIST: returns an array", pass, fail);
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        int had = (n > 0) ? resp_array_contains(fd, n, idxs[i]) : 0;
        snprintf(desc, sizeof(desc), "FT._LIST: %s not present before FT.CREATE", idxs[i]);
        CHECK(!had, desc, pass, fail);
    }

    /* ── FT.CREATE ───────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 17, "FT.CREATE", "my_index_1", "SCHEMA", "my_hash_field_key", "VECTOR", "HNSW", "10",
              "TYPE", "FLOAT32", "DIM", "20", "DISTANCE_METRIC", "COSINE", "M", "4", "EF_CONSTRUCTION", "100");
    CHECK(strcmp(r, "+OK") == 0, "FT.CREATE: HNSW vector index", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 19, "FT.CREATE", "my_index_2", "SCHEMA", "my_vector_field_key", "VECTOR", "HNSW", "10",
              "TYPE", "FLOAT32", "DIM", "20", "DISTANCE_METRIC", "COSINE", "M", "4", "EF_CONSTRUCTION", "100",
              "my_numeric_field_key", "NUMERIC");
    CHECK(strcmp(r, "+OK") == 0, "FT.CREATE: HNSW vector index with a numeric field", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 28, "FT.CREATE", "my_index_3", "SCHEMA", "my_vector_field_key", "VECTOR", "HNSW", "10",
              "TYPE", "FLOAT32", "DIM", "20", "DISTANCE_METRIC", "COSINE", "M", "4", "EF_CONSTRUCTION", "100",
              "my_tag_field_key_1", "TAG", "SEPARATOR", "@", "CASESENSITIVE", "my_numeric_field_key_1", "NUMERIC",
              "my_numeric_field_key_2", "NUMERIC", "my_tag_field_key_2", "TAG");
    CHECK(strcmp(r, "+OK") == 0, "FT.CREATE: HNSW vector index with multiple tag and numeric fields", pass, fail);

    /* FT.CREATE goes to a single primary whose reply is relayed verbatim;
     * recreating the same index name must surface its error */
    proxy_cmd(fd, r, sizeof(r), 17, "FT.CREATE", "my_index_1", "SCHEMA", "my_hash_field_key", "VECTOR", "HNSW", "10",
              "TYPE", "FLOAT32", "DIM", "20", "DISTANCE_METRIC", "COSINE", "M", "4", "EF_CONSTRUCTION", "100");
    CHECK(r[0] == '-', "FT.CREATE: duplicate index name returns an error", pass, fail);

    /* ── FT._LIST (after creation) ──────────────────────────────────────── */

    for (int i = 0; i < nidx; i++) {
        char desc[128];
        proxy_cmd(fd, r, sizeof(r), 1, "FT._LIST");
        CHECK(r[0] == '*', "FT._LIST: returns an array", pass, fail);
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        int had = (n > 0) ? resp_array_contains(fd, n, idxs[i]) : 0;
        snprintf(desc, sizeof(desc), "FT._LIST: %s present after FT.CREATE", idxs[i]);
        CHECK(had, desc, pass, fail);
    }

    /* ── FT.INFO ─────────────────────────────────────────────────────────── */

    /* the bare form is rewritten by the proxy to FT.INFO <index> PRIMARY
     * ALLSHARDS, whose reply starts with "mode" "primary"; an explicit
     * LOCAL is forwarded as-is and has no leading "mode" field */
    for (int t = 0; t < 3; t++) {
        const char *desc = t == 0   ? "FT.INFO: bare form replies in PRIMARY ALLSHARDS mode"
                           : t == 1 ? "FT.INFO: explicit PRIMARY ALLSHARDS replies in primary mode"
                                    : "FT.INFO: explicit LOCAL is forwarded as-is";
        if (t == 0)
            resp_send(fd, 2, "FT.INFO", "my_index_1");
        else if (t == 1)
            resp_send(fd, 4, "FT.INFO", "my_index_1", "PRIMARY", "ALLSHARDS");
        else
            resp_send(fd, 3, "FT.INFO", "my_index_1", "LOCAL");
        char line[256];
        read_line(fd, line, sizeof(line));
        int n = (line[0] == '*') ? atoi(line + 1) : 0;
        int primary_mode = 0;
        if (n >= 2) {
            char k[128], v[128];
            resp_next_str(fd, k, sizeof(k));
            resp_next_str(fd, v, sizeof(v));
            primary_mode = !strcmp(k, "+mode") && !strcmp(v, "+primary");
            for (int i = 2; i < n; i++) resp_skip_reply(fd);
        } else {
            for (int i = 0; i < n; i++) resp_skip_reply(fd);
        }
        CHECK(n >= 2 && primary_mode == (t != 2), desc, pass, fail);
    }

    /* ── seed data, then FT.SEARCH ──────────────────────────────────────── */

    /* 20 x FLOAT32 = 80 bytes; every byte is '?' (0x3f), so each float is
     * 0x3f3f3f3f (~0.747) and the blob has no NUL for strlen() to stop at */
    char vec[81];
    memset(vec, '?', 80);
    vec[80] = '\0';
    const char *docs[] = {"tc:ft:doc:1", "tc:ft:doc:2", "tc:ft:doc:3", "tc:ft:doc:4"};
    const int ndocs = 4;
    for (int i = 0; i < ndocs; i++) {
        char num[8];
        snprintf(num, sizeof(num), "%d", i + 1);
        proxy_cmd(fd, r, sizeof(r), 6, "HSET", docs[i], "my_vector_field_key", vec, "my_numeric_field_key", num);
    }

    /* the backend coordinates the search across every shard itself, so a
     * single primary's reply already holds each matching doc exactly once */
    {
        char raw[4096];
        long long total = -1;
        int n = 0;
        for (int tries = 0; tries < 20; tries++) {
            resp_send(fd, 4, "FT.SEARCH", "my_index_2", "@my_numeric_field_key:[0 100]", "NOCONTENT");
            read_line(fd, raw, sizeof(raw));
            n = (raw[0] == '*') ? atoi(raw + 1) : 0;
            total = -1;
            if (n >= 1) {
                read_line(fd, raw, sizeof(raw));
                if (raw[0] == ':') total = atoll(raw + 1);
                for (int i = 1; i < n; i++) resp_skip_reply(fd);
            }
            if (total == ndocs) break;
            usleep(100000); /* indexing may lag the HSETs briefly */
        }
        CHECK(total == ndocs && n == ndocs + 1, "FT.SEARCH: numeric range returns every doc exactly once", pass, fail);
    }

    resp_send(fd, 7, "FT.SEARCH", "my_index_2", "@my_numeric_field_key:[0 100]", "NOCONTENT", "LIMIT", "0", "2");
    {
        char line[256];
        read_line(fd, line, sizeof(line));
        int n = (line[0] == '*') ? atoi(line + 1) : 0;
        long long total = -1;
        if (n >= 1) {
            read_line(fd, line, sizeof(line));
            if (line[0] == ':') total = atoll(line + 1);
            for (int i = 1; i < n; i++) resp_skip_reply(fd);
        }
        CHECK(total == ndocs && n == 3, "FT.SEARCH: LIMIT 0 2 is applied cluster-wide", pass, fail);
    }

    resp_send(fd, 7, "FT.SEARCH", "my_index_2", "*=>[KNN 2 @my_vector_field_key $v]", "PARAMS", "2", "v", vec);
    {
        char line[256];
        read_line(fd, line, sizeof(line));
        int n = (line[0] == '*') ? atoi(line + 1) : 0;
        long long total = -1;
        if (n >= 1) {
            read_line(fd, line, sizeof(line));
            if (line[0] == ':') total = atoll(line + 1);
            for (int i = 1; i < n; i++) resp_skip_reply(fd);
        }
        /* [total, id, fields, id, fields] */
        CHECK(total == 2 && n == 5, "FT.SEARCH: KNN 2 returns the 2 nearest docs", pass, fail);
    }

    for (int i = 0; i < ndocs; i++) proxy_cmd(fd, r, sizeof(r), 2, "DEL", docs[i]);

    /* ── FT.AGGREGATE (JSON index) ──────────────────────────────────────── */

    const char *agg_idx = "tc:ft:agg";
    proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", agg_idx);
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:ft:agg:1", "$",
              "[{\"arr\": [1, 2, 3]}, {\"val\": \"hello\"}, {\"val\": \"world\"}]");
    CHECK(strcmp(r, "+OK") == 0, "FT.AGGREGATE: seed JSON doc", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 16, "FT.CREATE", agg_idx, "ON", "JSON", "PREFIX", "1", "tc:ft:agg:", "SCHEMA", "$..arr",
              "AS", "arr", "NUMERIC", "$..val", "AS", "val", "TEXT");
    CHECK(strcmp(r, "+OK") == 0, "FT.CREATE: JSON index with NUMERIC and TEXT fields", pass, fail);

    /* reply is [count, row, ...] with each row a flat [field, value, ...]
     * array; the doc was written before the index, so allow for backfill.
     * valkey-search rejects a bare "*" query, and LOAD returns the TEXT field
     * under its JSON path ("$..val") while the NUMERIC array field is omitted */
    {
        int rows = 0, has_val = 0;
        for (int tries = 0; tries < 20 && rows == 0; tries++) {
            char line[256];
            resp_send(fd, 7, "FT.AGGREGATE", agg_idx, "@val:hello", "LOAD", "2", "arr", "val");
            read_line(fd, line, sizeof(line));
            int n = (line[0] == '*') ? atoi(line + 1) : 0;
            if (n >= 1) resp_skip_reply(fd); /* count */
            for (int i = 1; i < n; i++) {
                char rl[64];
                read_line(fd, rl, sizeof(rl));
                int m = (rl[0] == '*') ? atoi(rl + 1) : 0;
                for (int j = 0; j < m; j += 2) {
                    char k[64];
                    resp_next_str(fd, k, sizeof(k));
                    const char *kn = (k[0] == '+') ? k + 1 : k; /* field name may be a simple string */
                    if (!strcmp(kn, "val") || !strcmp(kn, "$..val")) has_val = 1;
                    if (j + 1 < m) resp_skip_reply(fd);
                }
                rows++;
            }
            if (rows == 0) usleep(100000);
        }
        CHECK(rows >= 1 && has_val, "FT.AGGREGATE: @val:hello LOAD 2 arr val returns a row with the val field", pass,
              fail);
    }

    proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", agg_idx);
    CHECK(strcmp(r, "+OK") == 0, "FT.DROPINDEX: drops the JSON index", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:ft:agg:1");

    /* ── FT.AGGREGATE / FT.HYBRID WITHCURSOR, FT.CURSOR ─────────────────── */

    const char *cur_idx = "tc:ft:cur";
    const char *cur_docs[] = {"tc:ft:cur:1", "tc:ft:cur:2", "tc:ft:cur:3", "tc:ft:cur:4", "tc:ft:cur:5"};
    const int ncur = 5;
    /* 2 x FLOAT32; "AAAA" is 0x41414141, and the blob has no NUL */
    const char *cvec = "AAAAAAAA";
    proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", cur_idx);
    proxy_cmd(fd, r, sizeof(r), 22, "FT.CREATE", cur_idx, "ON", "HASH", "PREFIX", "1", "tc:ft:cur:", "SCHEMA", "title",
              "TEXT", "price", "NUMERIC", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
              "DISTANCE_METRIC", "L2");
    CHECK(strcmp(r, "+OK") == 0, "FT.CREATE: HASH index with TEXT, NUMERIC and VECTOR fields", pass, fail);
    for (int i = 0; i < ncur; i++) {
        char title[32], price[8];
        snprintf(title, sizeof(title), "red shoes %d", i + 1);
        snprintf(price, sizeof(price), "%d", (i + 1) * 10);
        proxy_cmd(fd, r, sizeof(r), 8, "HSET", cur_docs[i], "title", title, "price", price, "vec", cvec);
    }

    /* wait until every doc is indexed */
    for (int tries = 0; tries < 20; tries++) {
        char line[256];
        long long total = -1;
        resp_send(fd, 4, "FT.SEARCH", cur_idx, "@price:[-inf inf]", "NOCONTENT");
        read_line(fd, line, sizeof(line));
        int n = (line[0] == '*') ? atoi(line + 1) : 0;
        if (n >= 1) {
            read_line(fd, line, sizeof(line));
            if (line[0] == ':') total = atoll(line + 1);
            for (int i = 1; i < n; i++) resp_skip_reply(fd);
        }
        if (total == ncur) break;
        usleep(100000);
    }

    /* a cursor reply is [[count, row, ...], cursor-id]; rows < 0 on a
     * malformed reply */
    long long cid = 0;
    int rows = -1;
#define READ_CURSOR_REPLY(sock)                                                       \
    do {                                                                              \
        char _l[256];                                                                 \
        rows = -1;                                                                    \
        cid = -1;                                                                     \
        read_line(sock, _l, sizeof(_l));                                              \
        if (_l[0] == '*' && atoi(_l + 1) == 2) {                                      \
            read_line(sock, _l, sizeof(_l));                                          \
            int _n = (_l[0] == '*') ? atoi(_l + 1) : 0;                               \
            for (int _i = 0; _i < _n; _i++) resp_skip_reply(sock);                    \
            if (_n >= 1) rows = _n - 1;                                               \
            read_line(sock, _l, sizeof(_l));                                          \
            if (_l[0] == ':') cid = atoll(_l + 1);                                    \
        } else if (_l[0] == '*') {                                                    \
            for (int _i = 0, _n = atoi(_l + 1); _i < _n; _i++) resp_skip_reply(sock); \
        }                                                                             \
    } while (0)

    resp_send(fd, 11, "FT.AGGREGATE", cur_idx, "@price:[-inf inf]", "LOAD", "1", "@price", "SORTBY", "2", "@price",
              "ASC", "WITHCURSOR");
    /* the WITHCURSOR above has no COUNT, so every row fits in one read */
    READ_CURSOR_REPLY(fd);
    CHECK(rows == ncur && cid == 0, "FT.AGGREGATE: WITHCURSOR without COUNT returns every row and cursor 0", pass,
          fail);

    resp_send(fd, 13, "FT.AGGREGATE", cur_idx, "@price:[-inf inf]", "LOAD", "1", "@price", "SORTBY", "2", "@price",
              "ASC", "WITHCURSOR", "COUNT", "2");
    READ_CURSOR_REPLY(fd);
    CHECK(rows == 2 && cid > 0, "FT.AGGREGATE: WITHCURSOR COUNT 2 returns 2 rows and a cursor id", pass, fail);
    long long agg_cid = cid;

    /* read from a second client connection, which may be served by another
     * worker thread: the cursor must still be found on the same node */
    int fd2 = proxy_connect(host, port);
    char cidbuf[32];
    snprintf(cidbuf, sizeof(cidbuf), "%lld", agg_cid);
    resp_send(fd2, 6, "FT.CURSOR", "READ", cur_idx, cidbuf, "COUNT", "2");
    READ_CURSOR_REPLY(fd2);
    CHECK(rows == 2 && cid == agg_cid, "FT.CURSOR READ: COUNT 2 from another connection returns the next 2 rows", pass,
          fail);

    resp_send(fd, 4, "FT.CURSOR", "READ", cur_idx, cidbuf);
    READ_CURSOR_REPLY(fd);
    CHECK(rows == 1 && cid == 0, "FT.CURSOR READ: last read returns the remaining row and cursor 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "FT.CURSOR", "READ", cur_idx, cidbuf);
    CHECK(r[0] == '-', "FT.CURSOR READ: an exhausted cursor returns an error", pass, fail);

    /* ── FT.HYBRID ───────────────────────────────────────────────────────── */

    /* the query vector must come from PARAMS; the backend's error is relayed */
    proxy_cmd(fd, r, sizeof(r), 7, "FT.HYBRID", cur_idx, "SEARCH", "@title:shoes", "VSIM", "@vec", "$q");
    CHECK(r[0] == '-', "FT.HYBRID: VSIM without PARAMS returns an error", pass, fail);

    resp_send(fd, 11, "FT.HYBRID", cur_idx, "SEARCH", "@title:shoes", "VSIM", "@vec", "$q", "PARAMS", "2", "q", cvec);
    {
        char line[256];
        read_line(fd, line, sizeof(line));
        int n = (line[0] == '*') ? atoi(line + 1) : 0;
        long long total = -1;
        int has_key = 0;
        if (n >= 1) {
            read_line(fd, line, sizeof(line));
            if (line[0] == ':') total = atoll(line + 1);
            for (int i = 1; i < n; i++) {
                char rl[64];
                read_line(fd, rl, sizeof(rl));
                int m = (rl[0] == '*') ? atoi(rl + 1) : 0;
                for (int j = 0; j < m; j += 2) {
                    char k[64];
                    resp_next_str(fd, k, sizeof(k));
                    const char *kn = (k[0] == '+') ? k + 1 : k;
                    if (!strcmp(kn, "__key")) has_key++;
                    if (j + 1 < m) resp_skip_reply(fd);
                }
            }
        }
        CHECK(total == ncur && n == ncur + 1 && has_key == ncur,
              "FT.HYBRID: SEARCH + VSIM returns every doc with its __key", pass, fail);
    }

    resp_send(fd, 14, "FT.HYBRID", cur_idx, "SEARCH", "@title:shoes", "VSIM", "@vec", "$q", "PARAMS", "2", "q", cvec,
              "WITHCURSOR", "COUNT", "2");
    READ_CURSOR_REPLY(fd);
    CHECK(rows == 2 && cid > 0, "FT.HYBRID: WITHCURSOR COUNT 2 returns 2 rows and a cursor id", pass, fail);
    snprintf(cidbuf, sizeof(cidbuf), "%lld", cid);

    proxy_cmd(fd2, r, sizeof(r), 4, "FT.CURSOR", "DEL", cur_idx, cidbuf);
    CHECK(strcmp(r, "+OK") == 0, "FT.CURSOR DEL: deletes an FT.HYBRID cursor from another connection", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "FT.CURSOR", "DEL", cur_idx, cidbuf);
    CHECK(r[0] == '-', "FT.CURSOR DEL: deleting an already-deleted cursor returns an error", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "FT.CURSOR", "READ", cur_idx, cidbuf);
    CHECK(r[0] == '-', "FT.CURSOR READ: a deleted cursor returns an error", pass, fail);
#undef READ_CURSOR_REPLY

    if (fd2 >= 0) close(fd2);
    proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", cur_idx);
    for (int i = 0; i < ncur; i++) proxy_cmd(fd, r, sizeof(r), 2, "DEL", cur_docs[i]);

    /* ── FT.DROPINDEX ────────────────────────────────────────────────────── */

    for (int i = 0; i < nidx; i++) {
        char desc[128];
        proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", idxs[i]);
        snprintf(desc, sizeof(desc), "FT.DROPINDEX: drops %s", idxs[i]);
        CHECK(strcmp(r, "+OK") == 0, desc, pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 2, "FT.DROPINDEX", "my_index_1");
    CHECK(r[0] == '-', "FT.DROPINDEX: dropping an already-dropped index returns an error", pass, fail);

    close(fd);
}
