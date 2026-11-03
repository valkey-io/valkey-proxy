#include "test_common.h"

void run_script_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Script Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    /* ── EVAL with numkeys=0 (no key, routed to first master) ─────────── */
    resp_send(fd, 3, "EVAL", "return 1", "0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, ":1") == 0, "EVAL: numkeys=0 executes and returns 1", pass, fail);

    /* ── EVAL with a single key ────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:sc:k1");
    resp_send(fd, 5, "EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "tc:sc:k1", "v1");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "+OK") == 0, "EVAL: single key SET via redis.call returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:sc:k1");
    CHECK(strcmp(r, "v1") == 0, "EVAL: single key SET actually stored the value", pass, fail);

    /* ── EVAL with multiple keys in the same slot (hash tag) ───────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:sc:same}k1");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "{tc:sc:same}k2");
    resp_send(
        fd, 7, "EVAL",
        "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[2]); return redis.call('GET', KEYS[1])",
        "2", "{tc:sc:same}k1", "{tc:sc:same}k2", "sv1", "sv2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "sv1") == 0, "EVAL: same-slot 2-key script returns first key's value", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "{tc:sc:same}k2");
    CHECK(strcmp(r, "sv2") == 0, "EVAL: same-slot 2-key script also stored the second key", pass, fail);

    /* ── EVAL cross-slot (should return CROSSSLOT error) ────────────────
     * tc:cs:foo → slot 4418,  tc:cs:bar → slot 11537  (verified via CRC16)
     */
    {
        const char *args[5] = {"EVAL", "return 1", "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "EVAL: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── EVAL_RO reads a key ─────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:sc:ro", "rv1");
    resp_send(fd, 4, "EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "tc:sc:ro");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "rv1") == 0, "EVAL_RO: reads back the key's value", pass, fail);

    /* ── EVAL_RO rejects write commands (enforced by the backend) ───────── */
    resp_send(fd, 5, "EVAL_RO", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "tc:sc:ro", "rv2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-', "EVAL_RO: write command inside the script is rejected", pass, fail);

    /* ── EVAL_RO cross-slot (should return CROSSSLOT error) ─────────────── */
    {
        const char *args[5] = {"EVAL_RO", "return 1", "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "EVAL_RO: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── SCRIPT LOAD / EXISTS / SHOW ──────────────────────────────────── */
    char sha_ret1[512];
    proxy_cmd(fd, sha_ret1, sizeof(sha_ret1), 3, "SCRIPT", "LOAD", "return 1");
    CHECK(strlen(sha_ret1) == 40, "SCRIPT LOAD: returns a 40-char sha1", pass, fail);

    {
        const char *bogus = "0000000000000000000000000000000000000f";
        resp_send(fd, 4, "SCRIPT", "EXISTS", sha_ret1, bogus);
        resp_next_str(fd, r, sizeof(r)); /* array header */
        CHECK(strcmp(r, "*2") == 0, "SCRIPT EXISTS: returns a 2-element array", pass, fail);
        resp_next_str(fd, r, sizeof(r));
        CHECK(strcmp(r, ":1") == 0, "SCRIPT EXISTS: loaded sha1 reports 1", pass, fail);
        resp_next_str(fd, r, sizeof(r));
        CHECK(strcmp(r, ":0") == 0, "SCRIPT EXISTS: unknown sha1 reports 0", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 3, "SCRIPT", "SHOW", sha_ret1);
    CHECK(strcmp(r, "return 1") == 0, "SCRIPT SHOW: returns the original script source", pass, fail);

    /* ── EVALSHA with numkeys=0 (no key, routed to first master) ───────── */
    resp_send(fd, 3, "EVALSHA", sha_ret1, "0");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, ":1") == 0, "EVALSHA: numkeys=0 executes and returns 1", pass, fail);

    /* ── EVALSHA with a single key ───────────────────────────────────────── */
    char sha_set[512];
    proxy_cmd(fd, sha_set, sizeof(sha_set), 3, "SCRIPT", "LOAD", "return redis.call('SET', KEYS[1], ARGV[1])");
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:sc:k2");
    resp_send(fd, 5, "EVALSHA", sha_set, "1", "tc:sc:k2", "v2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "+OK") == 0, "EVALSHA: single key SET via redis.call returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:sc:k2");
    CHECK(strcmp(r, "v2") == 0, "EVALSHA: single key SET actually stored the value", pass, fail);

    /* ── EVALSHA cross-slot (should return CROSSSLOT error) ─────────────── */
    {
        const char *args[5] = {"EVALSHA", sha_ret1, "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "EVALSHA: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── EVALSHA_RO reads a key ────────────────────────────────────────── */
    char sha_get[512];
    proxy_cmd(fd, sha_get, sizeof(sha_get), 3, "SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:sc:ro2", "rv2");
    resp_send(fd, 4, "EVALSHA_RO", sha_get, "1", "tc:sc:ro2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "rv2") == 0, "EVALSHA_RO: reads back the key's value", pass, fail);

    /* ── EVALSHA_RO rejects write commands (enforced by the backend) ───── */
    resp_send(fd, 5, "EVALSHA_RO", sha_set, "1", "tc:sc:ro2", "rv3");
    resp_next_str(fd, r, sizeof(r));
    CHECK(r[0] == '-', "EVALSHA_RO: write command inside the script is rejected", pass, fail);

    /* ── EVALSHA_RO cross-slot (should return CROSSSLOT error) ──────────── */
    {
        const char *args[5] = {"EVALSHA_RO", sha_ret1, "2", "tc:cs:foo", "tc:cs:bar"};
        int arglens[5];
        for (int i = 0; i < 5; i++) arglens[i] = (int)strlen(args[i]);
        resp_send_raw(fd, 5, args, arglens);
    }
    resp_next_str(fd, r, sizeof(r));
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "EVALSHA_RO: cross-slot returns CROSSSLOT error", pass, fail);

    /* ── SCRIPT KILL (no script running on any node) ─────────────────────
     * Fanned out to all master nodes; since none are busy running a
     * script, every node fails and the proxy must relay a failure. */
    proxy_cmd(fd, r, sizeof(r), 2, "SCRIPT", "KILL");
    CHECK(r[0] == '-', "SCRIPT KILL: no script running on any node returns an error", pass, fail);

    /* ── SCRIPT FLUSH — always succeeds regardless of the argument form ── */
    proxy_cmd(fd, r, sizeof(r), 2, "SCRIPT", "FLUSH");
    CHECK(strcmp(r, "+OK") == 0, "SCRIPT FLUSH: no argument returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SCRIPT", "FLUSH", "ASYNC");
    CHECK(strcmp(r, "+OK") == 0, "SCRIPT FLUSH ASYNC: returns +OK", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "SCRIPT", "FLUSH", "SYNC");
    CHECK(strcmp(r, "+OK") == 0, "SCRIPT FLUSH SYNC: returns +OK", pass, fail);
}
