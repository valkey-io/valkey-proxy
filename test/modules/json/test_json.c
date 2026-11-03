#include "test_common.h"

/* Recursively drain one complete RESP value from the socket. */
static void resp_drain(int fd) {
    char line[256];
    if (read_line(fd, line, sizeof(line)) < 0) return;
    if (line[0] == '+' || line[0] == '-' || line[0] == ':') return;
    if (line[0] == '$') {
        int n = atoi(line + 1);
        if (n < 0) return; /* nil bulk string */
        char tmp[256];
        int rem = n + 2; /* content + CRLF */
        while (rem > 0) {
            int chunk = rem < (int)sizeof(tmp) ? rem : (int)sizeof(tmp);
            if (read_exact(fd, tmp, chunk) < 0) return;
            rem -= chunk;
        }
        return;
    }
    if (line[0] == '*') {
        int n = atoi(line + 1);
        if (n < 0) return; /* null array */
        for (int i = 0; i < n; i++) resp_drain(fd);
    }
}

/*
 * JSON.TYPE (and similar commands) may return either:
 *   - *1\r\n$N\r\n<type>\r\n   (JSONPath array, RedisJSON/ValkeyJSON 2.x)
 *   - $N\r\n<type>\r\n          (legacy scalar, already decoded to plain string by resp_next_str)
 *   - +<type>\r\n               (simple string, decoded with leading '+')
 *
 * This macro normalises any of those into `_out` (char[]) ready for strcmp.
 * `_r` is the string already in the buffer from proxy_cmd().
 */
#define JSON_TYPE_READ(_fd, _r, _out, _outsz)                                  \
    do {                                                                       \
        if ((_r)[0] == '*') {                                                  \
            int _n = atoi((_r) + 1);                                           \
            if (_n > 0)                                                        \
                resp_next_str((_fd), (_out), (_outsz));                        \
            else                                                               \
                (_out)[0] = '\0';                                              \
            for (int _i = 1; _i < _n; _i++) {                                  \
                char _t[64];                                                   \
                resp_next_str((_fd), _t, sizeof(_t));                          \
            }                                                                  \
            /* strip leading '+' from simple-string element */                 \
            if ((_out)[0] == '+') memmove((_out), (_out) + 1, strlen((_out))); \
        } else {                                                               \
            /* scalar: r IS the type string (or "+type") */                    \
            const char *_src = ((_r)[0] == '+') ? (_r) + 1 : (_r);             \
            strncpy((_out), _src, (_outsz) - 1);                               \
            (_out)[(_outsz) - 1] = '\0';                                       \
        }                                                                      \
    } while (0)

void run_json_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== JSON Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[1024];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* base document used across many tests */
    const char *base_doc = "{\"name\":\"Alice\",\"age\":30,\"scores\":[10,20,30],"
                           "\"active\":true,\"info\":{\"city\":\"NYC\"}}";

    /* ── JSON.SET ────────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:json:doc", "$", base_doc);
    CHECK(strcmp(r, "+OK") == 0, "JSON.SET: set root document returns +OK", pass, fail);

    /* set a nested path */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:json:doc", "$.info.zip", "\"10001\"");
    CHECK(strcmp(r, "+OK") == 0, "JSON.SET: set nested path returns +OK", pass, fail);

    /* NX: succeeds on a new key */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.SET", "tc:json:nx", "$", "1", "NX");
    CHECK(strcmp(r, "+OK") == 0, "JSON.SET NX: new key returns +OK", pass, fail);

    /* NX: fails (returns nil) if key already exists */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.SET", "tc:json:nx", "$", "2", "NX");
    CHECK(strcmp(r, "(nil)") == 0, "JSON.SET NX: existing key returns nil", pass, fail);

    /* XX: succeeds on existing key */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.SET", "tc:json:doc", "$.age", "31", "XX");
    CHECK(strcmp(r, "+OK") == 0, "JSON.SET XX: existing key returns +OK", pass, fail);

    /* XX: fails (returns nil) on missing key */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.SET", "tc:json:noexist", "$", "1", "XX");
    CHECK(strcmp(r, "(nil)") == 0, "JSON.SET XX: missing key returns nil", pass, fail);

    /* ── JSON.GET ────────────────────────────────────────────────────────── */

    /* get root — returns bulk JSON string */
    proxy_cmd(fd, r, sizeof(r), 2, "JSON.GET", "tc:json:doc");
    CHECK(r[0] != '-' && strlen(r) > 0, "JSON.GET: root returns non-empty JSON string", pass, fail);

    /* get $.name — JSONPath returns array-wrapped value */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.GET", "tc:json:doc", "$.name");
    CHECK(strstr(r, "Alice") != NULL, "JSON.GET $.name: contains 'Alice'", pass, fail);

    /* get $.age — after XX update it should be 31 */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.GET", "tc:json:doc", "$.age");
    CHECK(strstr(r, "31") != NULL, "JSON.GET $.age: reflects updated value 31", pass, fail);

    /* non-existent key → nil */
    proxy_cmd(fd, r, sizeof(r), 2, "JSON.GET", "tc:json:noexist");
    CHECK(strcmp(r, "(nil)") == 0, "JSON.GET: non-existent key returns nil", pass, fail);

    /* ── JSON.TYPE ───────────────────────────────────────────────────────── */

    /* $.name → "string" */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TYPE", "tc:json:doc", "$.name");
    {
        char v[64] = "";
        JSON_TYPE_READ(fd, r, v, sizeof(v));
        CHECK(strcmp(v, "string") == 0, "JSON.TYPE $.name: returns 'string'", pass, fail);
    }

    /* $.age → "integer" (some implementations return "number" for all numerics) */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TYPE", "tc:json:doc", "$.age");
    {
        char v[64] = "";
        JSON_TYPE_READ(fd, r, v, sizeof(v));
        CHECK(strcmp(v, "integer") == 0 || strcmp(v, "number") == 0, "JSON.TYPE $.age: returns 'integer' or 'number'",
              pass, fail);
    }

    /* $.active → "boolean" */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TYPE", "tc:json:doc", "$.active");
    {
        char v[64] = "";
        JSON_TYPE_READ(fd, r, v, sizeof(v));
        CHECK(strcmp(v, "boolean") == 0, "JSON.TYPE $.active: returns 'boolean'", pass, fail);
    }

    /* $.scores → "array" */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TYPE", "tc:json:doc", "$.scores");
    {
        char v[64] = "";
        JSON_TYPE_READ(fd, r, v, sizeof(v));
        CHECK(strcmp(v, "array") == 0, "JSON.TYPE $.scores: returns 'array'", pass, fail);
    }

    /* $.info → "object" */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TYPE", "tc:json:doc", "$.info");
    {
        char v[64] = "";
        JSON_TYPE_READ(fd, r, v, sizeof(v));
        CHECK(strcmp(v, "object") == 0, "JSON.TYPE $.info: returns 'object'", pass, fail);
    }

    /* ── JSON.STRLEN ─────────────────────────────────────────────────────── */

    /* $.name ("Alice" = 5 chars) */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.STRLEN", "tc:json:doc", "$.name");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":5") == 0, "JSON.STRLEN $.name: length of 'Alice' is 5", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.STRAPPEND ──────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.STRAPPEND", "tc:json:doc", "$.name", "\" Smith\"");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        /* "Alice Smith" = 11 chars */
        CHECK(strcmp(v, ":11") == 0, "JSON.STRAPPEND $.name: new length is 11", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* verify via GET */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.GET", "tc:json:doc", "$.name");
    CHECK(strstr(r, "Alice Smith") != NULL, "JSON.STRAPPEND: JSON.GET confirms 'Alice Smith'", pass, fail);

    /* ── JSON.NUMINCRBY ──────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.NUMINCRBY", "tc:json:doc", "$.age", "5");
    /* response is a bulk string like "[36]" */
    CHECK(strstr(r, "36") != NULL, "JSON.NUMINCRBY $.age +5: result contains 36", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.NUMINCRBY", "tc:json:doc", "$.age", "-6");
    CHECK(strstr(r, "30") != NULL, "JSON.NUMINCRBY $.age -6: result contains 30", pass, fail);

    /* ── JSON.NUMMULTBY ──────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.NUMMULTBY", "tc:json:doc", "$.age", "2");
    /* 30 * 2 = 60 */
    CHECK(strstr(r, "60") != NULL, "JSON.NUMMULTBY $.age *2: result contains 60", pass, fail);

    /* reset age back to 30 */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:json:doc", "$.age", "30");

    /* ── JSON.TOGGLE ─────────────────────────────────────────────────────── */

    /* $.active is true → toggle to false → returns [0] */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TOGGLE", "tc:json:doc", "$.active");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":0") == 0, "JSON.TOGGLE: true→false returns 0", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* toggle again → true → returns [1] */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.TOGGLE", "tc:json:doc", "$.active");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":1") == 0, "JSON.TOGGLE: false→true returns 1", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.CLEAR ──────────────────────────────────────────────────────── */

    /* clear $.scores array → returns 1 (one container cleared) */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.CLEAR", "tc:json:doc", "$.scores");
    {
        long long v = atoll(r + 1);
        CHECK(r[0] == ':' && v >= 1, "JSON.CLEAR $.scores: returns >= 1", pass, fail);
    }

    /* verify array is now empty */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.ARRLEN", "tc:json:doc", "$.scores");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":0") == 0, "JSON.CLEAR: $.scores length is 0 after clear", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* restore scores for subsequent tests */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:json:doc", "$.scores", "[10,20,30]");

    /* ── JSON.ARRLEN ─────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 3, "JSON.ARRLEN", "tc:json:doc", "$.scores");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":3") == 0, "JSON.ARRLEN $.scores: length is 3", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* non-array path → null element in result */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.ARRLEN", "tc:json:doc", "$.name");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "(nil)") == 0, "JSON.ARRLEN on non-array: returns null element", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.ARRAPPEND ──────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.ARRAPPEND", "tc:json:doc", "$.scores", "40");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":4") == 0, "JSON.ARRAPPEND: new length is 4", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* append multiple values */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.ARRAPPEND", "tc:json:doc", "$.scores", "50", "60");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":6") == 0, "JSON.ARRAPPEND multi: new length is 6", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.ARRINDEX ───────────────────────────────────────────────────── */

    /* $.scores = [10,20,30,40,50,60]; find 30 → index 2 */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.ARRINDEX", "tc:json:doc", "$.scores", "30");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":2") == 0, "JSON.ARRINDEX: value 30 is at index 2", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* value not present → -1 */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.ARRINDEX", "tc:json:doc", "$.scores", "99");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":-1") == 0, "JSON.ARRINDEX: absent value returns -1", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.ARRINSERT ──────────────────────────────────────────────────── */

    /* insert 5 at index 0 → new length 7 */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.ARRINSERT", "tc:json:doc", "$.scores", "0", "5");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":7") == 0, "JSON.ARRINSERT at index 0: new length is 7", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.ARRPOP ─────────────────────────────────────────────────────── */

    /* pop last element (60) */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.ARRPOP", "tc:json:doc", "$.scores");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "60") == 0, "JSON.ARRPOP: pops last element 60", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* pop at explicit index 0 → first element (5) */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.ARRPOP", "tc:json:doc", "$.scores", "0");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, "5") == 0, "JSON.ARRPOP at index 0: pops first element 5", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.ARRTRIM ────────────────────────────────────────────────────── */

    /* current $.scores after inserts/pops: [10,20,30,40,50]; trim to [0,1] */
    proxy_cmd(fd, r, sizeof(r), 5, "JSON.ARRTRIM", "tc:json:doc", "$.scores", "0", "1");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":2") == 0, "JSON.ARRTRIM [0,1]: new length is 2", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.OBJLEN ─────────────────────────────────────────────────────── */

    /* $.info has {city, zip} → 2 keys */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.OBJLEN", "tc:json:doc", "$.info");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":2") == 0, "JSON.OBJLEN $.info: 2 keys (city + zip)", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* root $ has 5 top-level keys */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.OBJLEN", "tc:json:doc", "$");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        char v[64] = "";
        if (n > 0) resp_next_str(fd, v, sizeof(v));
        CHECK(strcmp(v, ":5") == 0, "JSON.OBJLEN $: root has 5 keys", pass, fail);
        for (int i = 1; i < n; i++) {
            char t[64];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.OBJKEYS ────────────────────────────────────────────────────── */

    /* $.info keys: ["city","zip"] */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.OBJKEYS", "tc:json:doc", "$.info");
    {
        int outer = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(outer >= 1, "JSON.OBJKEYS $.info: outer array has >= 1 element", pass, fail);
        /* each element is itself an array of key names */
        char inner_hdr[64];
        resp_next_str(fd, inner_hdr, sizeof(inner_hdr));
        int inner = (inner_hdr[0] == '*') ? atoi(inner_hdr + 1) : 0;
        CHECK(inner == 2, "JSON.OBJKEYS $.info: inner array has 2 keys", pass, fail);
        char k1[64], k2[64];
        resp_next_str(fd, k1, sizeof(k1));
        resp_next_str(fd, k2, sizeof(k2));
        int has_city = (strcmp(k1, "city") == 0 || strcmp(k2, "city") == 0);
        int has_zip = (strcmp(k1, "zip") == 0 || strcmp(k2, "zip") == 0);
        CHECK(has_city, "JSON.OBJKEYS $.info: contains 'city'", pass, fail);
        CHECK(has_zip, "JSON.OBJKEYS $.info: contains 'zip'", pass, fail);
        for (int i = 1; i < outer; i++) {
            char t[256];
            resp_next_str(fd, t, sizeof(t));
        }
    }

    /* ── JSON.RESP ───────────────────────────────────────────────────────── */

    /* root path returns a RESP array */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.RESP", "tc:json:doc", "$");
    CHECK(r[0] == '*', "JSON.RESP $: returns array representation", pass, fail);
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        for (int i = 0; i < n; i++) resp_drain(fd);
    }

    /* ── JSON.DEL ────────────────────────────────────────────────────────── */

    /* delete $.info.zip → 1 path deleted */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.DEL", "tc:json:doc", "$.info.zip");
    CHECK(strcmp(r, ":1") == 0, "JSON.DEL $.info.zip: deletes 1 path", pass, fail);

    /* delete non-existent path → 0 */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.DEL", "tc:json:doc", "$.missing");
    CHECK(strcmp(r, ":0") == 0, "JSON.DEL non-existent path: returns 0", pass, fail);

    /* delete whole key */
    proxy_cmd(fd, r, sizeof(r), 2, "JSON.DEL", "tc:json:doc");
    CHECK(strcmp(r, ":1") == 0, "JSON.DEL whole key: returns 1", pass, fail);

    /* key is gone */
    proxy_cmd(fd, r, sizeof(r), 2, "JSON.GET", "tc:json:doc");
    CHECK(strcmp(r, "(nil)") == 0, "JSON.DEL: key no longer exists after delete", pass, fail);

    /* ── JSON.FORGET ─────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "tc:json:forget", "$", "{\"x\":1}");
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.FORGET", "tc:json:forget", "$.x");
    CHECK(strcmp(r, ":1") == 0, "JSON.FORGET: deletes path, returns 1", pass, fail);

    /* ── JSON.MGET ───────────────────────────────────────────────────────── */

    /* set up two same-slot keys */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "{json}:a", "$", "{\"v\":1}");
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.SET", "{json}:b", "$", "{\"v\":2}");

    proxy_cmd(fd, r, sizeof(r), 4, "JSON.MGET", "{json}:a", "{json}:b", "$.v");
    {
        int n = atoi(r + 1);
        CHECK(r[0] == '*' && n == 2, "JSON.MGET same-slot: returns *2 array", pass, fail);
        char v0[256], v1[256];
        resp_next_str(fd, v0, sizeof(v0));
        resp_next_str(fd, v1, sizeof(v1));
        CHECK(strstr(v0, "1") != NULL, "JSON.MGET: first key value contains 1", pass, fail);
        CHECK(strstr(v1, "2") != NULL, "JSON.MGET: second key value contains 2", pass, fail);
    }

    /* cross-slot keys: proxy fans out, returns *2 with nils (keys don't exist) */
    proxy_cmd(fd, r, sizeof(r), 4, "JSON.MGET", "{src}:doc", "{dst}:doc", "$.v");
    {
        int n = (r[0] == '*') ? atoi(r + 1) : 0;
        CHECK(r[0] == '*' && n == 2, "JSON.MGET cross-slot: fan-out returns *2 array", pass, fail);
        char tmp[256];
        for (int i = 0; i < n; i++) resp_next_str(fd, tmp, sizeof(tmp));
    }

    /* ── JSON.MSET ───────────────────────────────────────────────────────── */

    proxy_cmd(fd, r, sizeof(r), 7, "JSON.MSET", "{json}:x", "$", "{\"n\":10}", "{json}:y", "$", "{\"n\":20}");
    CHECK(strcmp(r, "+OK") == 0, "JSON.MSET same-slot: returns +OK", pass, fail);

    /* verify */
    proxy_cmd(fd, r, sizeof(r), 3, "JSON.GET", "{json}:x", "$.n");
    CHECK(strstr(r, "10") != NULL, "JSON.MSET: {json}:x $.n is 10", pass, fail);

    /* cross-slot keys: proxy fans out, returns +OK */
    proxy_cmd(fd, r, sizeof(r), 7, "JSON.MSET", "{src}:doc", "$", "1", "{dst}:doc", "$", "2");
    CHECK(strcmp(r, "+OK") == 0, "JSON.MSET cross-slot: fan-out returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
