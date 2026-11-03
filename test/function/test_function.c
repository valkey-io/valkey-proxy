#include "test_common.h"

void run_function_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Function Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    /* ── FUNCTION HELP (answered locally by the proxy) ───────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "HELP");
    CHECK(strcmp(r, "*29") == 0, "FUNCTION HELP: returns the 29-line help array", pass, fail);
    for (int i = 0; i < 29; i++) resp_next_str(fd, r, sizeof(r));

    /* clean slate: remove any library left over from a previous run */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "DELETE", "tclib");
    (void)r;

    /* ── FUNCTION LOAD ─────────────────────────────────────────────────── */
    const char *libcode =
        "#!lua name=tclib\n"
        "redis.register_function('tcset', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n"
        "redis.register_function{function_name='tcget', callback=function(keys, args) return redis.call('GET', "
        "keys[1]) end, flags={'no-writes'}}";

    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "LOAD", libcode);
    CHECK(strcmp(r, "tclib") == 0, "FUNCTION LOAD: returns the library name", pass, fail);

    /* reloading without REPLACE must fail (library already exists) */
    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "LOAD", libcode);
    CHECK(r[0] == '-', "FUNCTION LOAD: reloading without REPLACE fails", pass, fail);

    /* reloading with REPLACE must succeed */
    {
        const char *args[4] = {"FUNCTION", "LOAD", "REPLACE", libcode};
        int arglens[4];
        for (int i = 0; i < 4; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 4, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "tclib") == 0, "FUNCTION LOAD REPLACE: succeeds and returns the library name", pass, fail);

    /* ── FUNCTION LIST ─────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "LIST");
    CHECK(r[0] == '*', "FUNCTION LIST: returns an array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── FCALL: single key, same slot, write function ────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:fn:k1");
    proxy_cmd(fd, r, sizeof(r), 5, "FCALL", "tcset", "1", "tc:fn:k1", "v1");
    CHECK(strcmp(r, "+OK") == 0, "FCALL: single key SET via function returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:fn:k1");
    CHECK(strcmp(r, "v1") == 0, "FCALL: single key SET actually stored the value", pass, fail);

    /* ── FCALL cross-slot (should return CROSSSLOT error) ─────────────────
     * tc:cs:foo -> slot 4418, tc:cs:bar -> slot 11537 (verified via CRC16,
     * same key pair used by the SCRIPT/EVAL cross-slot tests). */
    {
        const char *args[5] = {"FCALL", "tcset", "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "FCALL: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── FCALL with numkeys=0 (no key -> missing key error, unlike EVAL) ─── */
    proxy_cmd(fd, r, sizeof(r), 3, "FCALL", "tcset", "0");
    CHECK(r[0] == '-', "FCALL: numkeys=0 is rejected (no key to route on)", pass, fail);

    /* ── FCALL_RO reads a key ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:fn:ro", "rv1");
    proxy_cmd(fd, r, sizeof(r), 4, "FCALL_RO", "tcget", "1", "tc:fn:ro");
    CHECK(strcmp(r, "rv1") == 0, "FCALL_RO: reads back the key's value", pass, fail);

    /* ── FCALL_RO rejects write functions (enforced by the backend) ─────── */
    proxy_cmd(fd, r, sizeof(r), 5, "FCALL_RO", "tcset", "1", "tc:fn:ro", "rv2");
    CHECK(r[0] == '-', "FCALL_RO: write function is rejected", pass, fail);

    /* ── FCALL_RO cross-slot (should return CROSSSLOT error) ─────────────── */
    {
        const char *args[5] = {"FCALL_RO", "tcget", "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "FCALL_RO: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── FUNCTION STATS ───────────────────────────────────────────────────
     * fanned out to every master; the proxy reports one entry per node
     * (node-ip / running_script / engines) rather than a single merged view. */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "STATS");
    CHECK(r[0] == '*', "FUNCTION STATS: returns an array (one entry per master)", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        CHECK(n >= 1, "FUNCTION STATS: at least one master reported", pass, fail);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── FUNCTION KILL (no function running on any node) ──────────────────
     * Fanned out to all master nodes; since none are busy, every node fails
     * and the proxy must relay a failure (same convention as SCRIPT KILL). */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "KILL");
    CHECK(r[0] == '-', "FUNCTION KILL: no function running on any node returns an error", pass, fail);

    /* ── FUNCTION DELETE ───────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "DELETE", "tclib");
    CHECK(strcmp(r, "+OK") == 0, "FUNCTION DELETE: removes the library from every node", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "DELETE", "tclib");
    CHECK(r[0] == '-', "FUNCTION DELETE: deleting an already-removed library fails", pass, fail);

    /* ── FUNCTION FLUSH — always succeeds regardless of the argument form ── */
    proxy_cmd(fd, r, sizeof(r), 2, "FUNCTION", "FLUSH");
    CHECK(strcmp(r, "+OK") == 0, "FUNCTION FLUSH: no argument returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "FLUSH", "ASYNC");
    CHECK(strcmp(r, "+OK") == 0, "FUNCTION FLUSH ASYNC: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "FUNCTION", "FLUSH", "SYNC");
    CHECK(strcmp(r, "+OK") == 0, "FUNCTION FLUSH SYNC: returns +OK", pass, fail);
}
