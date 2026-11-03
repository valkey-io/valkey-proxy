#include "test_common.h"

/*
 * RESP3 support integration tests.
 *
 * Covers the whole feature added on top of main:
 *   - HELLO negotiation (bare / "2" / bad protover / SETNAME / AUTH, incl.
 *     its all-or-nothing semantics / the real "3" probe that flips the
 *     process-wide backend upgrade)
 *   - once RESP3 is active, a RESP3 client gets untouched native replies
 *     (map / set / double / null / nested WITHSCORES pairs / XREAD's map)
 *   - a RESP2 client -- even one that connects *after* some other client
 *     already triggered RESP3, and never itself said "HELLO 3" -- must see
 *     byte-for-byte the same RESP2 shapes it always did (this is the
 *     headline correctness requirement: RESP3 elsewhere in the process must
 *     never leak into an RESP2 client's replies)
 *   - the handful of commands whose RESP3 reply restructures nesting depth
 *     with no distinguishing type marker at all (WITHSCORES/WITHVALUES
 *     family, XREAD/XREADGROUP, GEOPOS's per-slot nil) -- the generic
 *     marker-level downgrade can't fix these on its own, see
 *     cmd_resp3_pair_transform()/cmd_resp2_nil_array_depth() in src/cmd.c
 *   - pub/sub push-type ('>') vs plain array ('*') delivery to a RESP3 vs
 *     RESP2 subscriber
 *
 * IMPORTANT ORDERING NOTE: g_backend_resp3_active (src/valkey-proxy.c) is a
 * process-wide, sticky flag -- once *any* client's "HELLO 3" successfully
 * probes the backend, every connection in the whole proxy process is
 * eligible to lazily upgrade from then on, for the rest of that process's
 * life. This test suite is the one that actually triggers that probe (see
 * "REAL HELLO 3 PROBE" below), so run-test.c runs it *last* among the
 * default suites -- every other suite gets to run under pure, never-
 * triggered RESP2 first. Within this file itself, the "before" checks all
 * happen on a connection opened before the probe.
 */

void run_resp3_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== RESP3 Support Tests ===\n");

    char raw[65536];
    char r[1024];
    int n;

    /* ── baseline: a connection that never says HELLO behaves exactly like
     * it always did (this is the fd we also use for the "before the probe"
     * HELLO edge cases below, all of which are answered locally without
     * ever touching the backend) ──────────────────────────────────────── */
    printf("\n--- baseline (pre-RESP3) ---\n");
    int fd_before = proxy_connect(host, port);
    if (fd_before < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }
    proxy_cmd(fd_before, r, sizeof(r), 1, "PING");
    CHECK(strcmp(r, "+PONG") == 0, "baseline PING: +PONG", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 3, "SET", "tc:resp3:k1", "v1");
    CHECK(strcmp(r, "+OK") == 0, "baseline SET: +OK", pass, fail);

    /* ── HELLO edge cases, all answered locally, none of which should ever
     * touch a backend or flip the global flag ──────────────────────────── */
    printf("\n--- HELLO negotiation ---\n");

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 1, "HELLO");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "bare HELLO: returns RESP2 *14 array", pass, fail);
    CHECK(n > 0 && strstr(raw, "proto\r\n:2\r\n") != NULL, "bare HELLO: proto field is 2", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 2, "HELLO", "2");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2: returns RESP2 *14 array", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 2, "HELLO", "99");
    CHECK(n > 0 && raw[0] == '-' && strstr(raw, "NOPROTO") != NULL, "HELLO 99: -NOPROTO unsupported protocol version",
          pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 2, "HELLO", "abc");
    CHECK(n > 0 && strstr(raw, "Protocol version is not an integer or out of range") != NULL,
          "HELLO abc: not-an-integer error, not NOPROTO", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 2, "HELLO", "03");
    CHECK(n > 0 && strstr(raw, "not an integer") != NULL, "HELLO 03: leading zero is not an integer (matches Valkey)",
          pass, fail);

    /* HELLO ... AUTH with bad credentials: the backend's own error is
     * relayed and nothing changes -- in particular a failed "HELLO 3 AUTH"
     * must not count as a successful RESP3 probe (the real probe below
     * still has to be the "first ever"). */
    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 5, "HELLO", "2", "AUTH", "tc-nosuchuser", "badpass");
    CHECK(n > 0 && strncmp(raw, "-WRONGPASS", 10) == 0, "HELLO 2 AUTH bad credentials: backend -WRONGPASS relayed",
          pass, fail);
    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 5, "HELLO", "3", "AUTH", "tc-nosuchuser", "badpass");
    CHECK(n > 0 && strncmp(raw, "-WRONGPASS", 10) == 0, "HELLO 3 AUTH bad credentials: backend -WRONGPASS relayed",
          pass, fail);
    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 1, "HELLO");
    CHECK(n > 0 && strstr(raw, "proto\r\n:2\r\n") != NULL, "failed HELLO 3 AUTH: protocol unchanged (still RESP2)",
          pass, fail);

    /* ── HELLO ... SETNAME: name stored locally, same as CLIENT SETNAME.
     * Only protover 2 (or a HELLO rejected before the protocol switch) is
     * used here, so none of these can trigger the RESP3 probe. */
    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "SETNAME", "tc-hello");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2 SETNAME: returns RESP2 *14 array", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc-hello") == 0, "HELLO 2 SETNAME: CLIENT GETNAME returns the name", pass, fail);

    resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "setname", "tc-hello2");
    proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc-hello2") == 0, "HELLO 2 setname: option is case-insensitive", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "SETNAME", "bad name");
    CHECK(n > 0 && raw[0] == '-', "HELLO 2 SETNAME with a space: rejected", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "SETNAME", "bad\x01name");
    CHECK(n > 0 && raw[0] == '-', "HELLO 2 SETNAME with a control char: rejected", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 3, "CLIENT", "SETNAME", "bad\x7fname");
    CHECK(n > 0 && raw[0] == '-', "CLIENT SETNAME with DEL (0x7f): rejected", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 3, "HELLO", "2", "SETNAME");
    CHECK(n > 0 && strstr(raw, "Syntax error in HELLO option 'SETNAME'") != NULL,
          "HELLO 2 SETNAME without a name: syntax error", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 3, "HELLO", "2", "FOO");
    CHECK(n > 0 && strstr(raw, "Syntax error in HELLO option 'FOO'") != NULL, "HELLO 2 FOO: syntax error", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "99", "SETNAME", "tc-x");
    CHECK(n > 0 && strstr(raw, "NOPROTO") != NULL, "HELLO 99 SETNAME: -NOPROTO", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "abc", "SETNAME", "tc-x");
    CHECK(n > 0 && strstr(raw, "not an integer") != NULL, "HELLO abc SETNAME: not-an-integer error", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 7, "HELLO", "2", "AUTH", "tc-nosuchuser", "badpass", "SETNAME",
                            "tc-x");
    CHECK(n > 0 && strncmp(raw, "-WRONGPASS", 10) == 0, "HELLO 2 AUTH bad credentials SETNAME: -WRONGPASS", pass, fail);

    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "AUTH", "tc-x");
    CHECK(n > 0 && strstr(raw, "Syntax error in HELLO option 'AUTH'") != NULL,
          "HELLO 2 AUTH with only a username: syntax error", pass, fail);

    proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc-hello2") == 0,
          "rejected HELLOs (bad name/syntax/NOPROTO/non-integer/AUTH) leave the name unchanged", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 2, "ACL", "WHOAMI");
    CHECK(strcmp(r, "default") == 0, "failed HELLO AUTH: identity still default", pass, fail);

    /* AUTH default: like the AUTH command, never checked against the
     * backend -- applied together with SETNAME on success. */
    n = resp_send_raw_reply(fd_before, raw, sizeof(raw), 7, "HELLO", "2", "AUTH", "default", "anything", "SETNAME",
                            "tc-d");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2 AUTH default SETNAME: returns *14", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc-d") == 0, "HELLO 2 AUTH default SETNAME: name applied", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 2, "ACL", "WHOAMI");
    CHECK(strcmp(r, "default") == 0, "HELLO 2 AUTH default: identity is default", pass, fail);

    /* The name arrives in a separate TCP segment: the proxy must wait for
     * the whole command instead of reading a half-received argument. */
    {
        const char *p1 = "*4\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$7\r\nSETNAME\r\n$8\r\ntc-";
        const char *p2 = "split\r\n";
        ssize_t wr = write(fd_before, p1, strlen(p1));
        usleep(100000);
        wr = write(fd_before, p2, strlen(p2));
        (void)wr;
        n = read_raw_reply(fd_before, raw, sizeof(raw));
        CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2 SETNAME split across writes: returns *14", pass,
              fail);
        proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
        CHECK(strcmp(r, "tc-split") == 0, "HELLO 2 SETNAME split across writes: full name stored", pass, fail);
    }

    resp_send_raw_reply(fd_before, raw, sizeof(raw), 4, "HELLO", "2", "SETNAME", "");
    proxy_cmd(fd_before, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "") == 0, "HELLO 2 SETNAME \"\": clears the name", pass, fail);
    proxy_cmd(fd_before, r, sizeof(r), 1, "PING");
    CHECK(strcmp(r, "+PONG") == 0, "connection still usable after a rejected HELLO", pass, fail);

    /* ── the real probe: this is the one HELLO 3 in the whole test run that
     * actually asks the backend and flips g_backend_resp3_active ─────────
     * for the rest of this process's life. A fresh connection, so the
     * "before" fd above (fd_before) is untouched by it. */
    /* ── HELLO 2 AUTH as a real, read-only user (still pre-probe, RESP2):
     * a SET coming back -NOPERM proves the client's business traffic moved
     * onto its own ACL connections instead of the shared default-identity
     * pools; a failed re-login must keep it there (atomic HELLO). ─────── */
    if (g_test_acl_ro_user) {
        int fdr = proxy_connect(host, port);
        n = resp_send_raw_reply(fdr, raw, sizeof(raw), 7, "HELLO", "2", "AUTH", g_test_acl_ro_user, g_test_acl_ro_pass,
                                "SETNAME", "tc-ro");
        CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2 AUTH read-only user SETNAME: returns *14", pass,
              fail);
        proxy_cmd(fdr, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, g_test_acl_ro_user) == 0, "HELLO 2 AUTH read-only user: ACL WHOAMI", pass, fail);
        proxy_cmd(fdr, r, sizeof(r), 2, "GET", "tc:resp3:k1");
        CHECK(strcmp(r, "v1") == 0, "HELLO 2 AUTH read-only user: GET allowed", pass, fail);
        proxy_cmd(fdr, r, sizeof(r), 3, "SET", "tc:resp3:k1", "denied");
        CHECK(strncmp(r, "-NOPERM", 7) == 0, "HELLO 2 AUTH read-only user: SET is -NOPERM (runs as that user)", pass,
              fail);

        n = resp_send_raw_reply(fdr, raw, sizeof(raw), 7, "HELLO", "2", "AUTH", g_test_acl_user, "tc-wrong-pass",
                                "SETNAME", "tc-other");
        CHECK(n > 0 && strncmp(raw, "-WRONGPASS", 10) == 0, "HELLO 2 AUTH other user, wrong password: -WRONGPASS", pass,
              fail);
        proxy_cmd(fdr, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, g_test_acl_ro_user) == 0, "failed HELLO AUTH: read-only user kept", pass, fail);
        proxy_cmd(fdr, r, sizeof(r), 3, "SET", "tc:resp3:k1", "denied");
        CHECK(strncmp(r, "-NOPERM", 7) == 0, "failed HELLO AUTH: still on the read-only user's connections", pass,
              fail);
        proxy_cmd(fdr, r, sizeof(r), 2, "CLIENT", "GETNAME");
        CHECK(strcmp(r, "tc-ro") == 0, "failed HELLO AUTH: name unchanged", pass, fail);

        /* Switching user in place: the old user's pinned conns are replaced. */
        n = resp_send_raw_reply(fdr, raw, sizeof(raw), 5, "HELLO", "2", "AUTH", g_test_acl_user, g_test_acl_pass);
        CHECK(n > 0 && raw[0] == '*', "HELLO 2 AUTH full user while logged in as read-only: *14", pass, fail);
        proxy_cmd(fdr, r, sizeof(r), 3, "SET", "tc:resp3:k1", "v1");
        CHECK(strcmp(r, "+OK") == 0, "after switching to the full user: SET allowed", pass, fail);
        close(fdr);
    } else {
        SKIP("HELLO 2 AUTH as a real read-only ACL user");
    }

    printf("\n--- REAL HELLO 3 PROBE (flips process-wide RESP3 state) ---\n");
    /* With a real user available, the first HELLO 3 of this run carries
     * AUTH: on a fresh proxy that exercises the combined path, where the
     * login's own "HELLO 3 AUTH ..." doubles as the process-wide probe. */
    if (g_test_acl_user) {
        int fdp = proxy_connect(host, port);
        n = resp_send_raw_reply(fdp, raw, sizeof(raw), 5, "HELLO", "3", "AUTH", g_test_acl_user, g_test_acl_pass);
        CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 7 && strstr(raw, "proto\r\n:3\r\n") != NULL,
              "HELLO 3 AUTH as the first HELLO 3: login + RESP3 in one step, %7 map", pass, fail);
        n = resp_send_raw_reply(fdp, raw, sizeof(raw), 2, "HGETALL", "tc:resp3:nosuchhash");
        CHECK(n > 0 && strncmp(raw, "%0", 2) == 0, "HELLO 3 AUTH: replies on the ACL connection are native RESP3", pass,
              fail);
        close(fdp);
    }
    int fd3 = proxy_connect(host, port);
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 2, "HELLO", "3");
    CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 7, "HELLO 3 (first plain HELLO 3): returns RESP3 %7 map", pass,
          fail);
    CHECK(n > 0 && strstr(raw, "proto\r\n:3\r\n") != NULL, "HELLO 3: proto field is 3", pass, fail);

    /* A second client asking for HELLO 3 *after* the flag is already active
     * doesn't need to probe again -- still answered correctly. */
    int fd3b = proxy_connect(host, port);
    n = resp_send_raw_reply(fd3b, raw, sizeof(raw), 2, "HELLO", "3");
    CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 7,
          "HELLO 3 (already active): still returns RESP3 %7 map, no re-probe needed", pass, fail);
    proxy_cmd(fd3b, r, sizeof(r), 1, "PING");
    CHECK(strcmp(r, "+PONG") == 0, "HELLO-3 connection: PING still works", pass, fail);

    n = resp_send_raw_reply(fd3b, raw, sizeof(raw), 4, "HELLO", "3", "SETNAME", "tc-hello3");
    CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 7, "HELLO 3 SETNAME: returns RESP3 %7 map", pass, fail);
    proxy_cmd(fd3b, r, sizeof(r), 2, "CLIENT", "GETNAME");
    CHECK(strcmp(r, "tc-hello3") == 0, "HELLO 3 SETNAME: CLIENT GETNAME returns the name", pass, fail);
    close(fd3b);

    /* ── HELLO 3 AUTH as a real non-default ACL user (needs run-test
     * --cluster=..., see test/acl/acl_fixture.c). ────────────────────── */
    const char *acl_user = g_test_acl_user;
    const char *acl_pass = g_test_acl_pass;
    if (acl_user) {
        int fda = proxy_connect(host, port);
        n = resp_send_raw_reply(fda, raw, sizeof(raw), 7, "HELLO", "3", "AUTH", acl_user, acl_pass, "SETNAME",
                                "tc-acl");
        CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 7, "HELLO 3 AUTH user pass SETNAME: returns RESP3 %7 map",
              pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, acl_user) == 0, "HELLO 3 AUTH: ACL WHOAMI reports the new user", pass, fail);
        n = resp_send_raw_reply(fda, raw, sizeof(raw), 2, "CLIENT", "INFO");
        {
            char want[128];
            snprintf(want, sizeof(want), "user=%s ", acl_user);
            CHECK(n > 0 && strstr(raw, want) != NULL && strstr(raw, "name=tc-acl ") != NULL &&
                      strstr(raw, "resp=3") != NULL,
                  "HELLO 3 AUTH: CLIENT INFO shows user, name and resp=3", pass, fail);
        }
        proxy_cmd(fda, r, sizeof(r), 3, "SET", "tc:resp3:acl", "v-acl");
        CHECK(strcmp(r, "+OK") == 0, "HELLO 3 AUTH: SET via the ACL connection", pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "GET", "tc:resp3:acl");
        CHECK(strcmp(r, "v-acl") == 0, "HELLO 3 AUTH: GET via the ACL connection", pass, fail);

        /* Atomicity: a failed re-login keeps the current identity, name
         * and protocol -- unlike a failed AUTH command. */
        n = resp_send_raw_reply(fda, raw, sizeof(raw), 7, "HELLO", "2", "AUTH", acl_user, "tc-wrong-pass", "SETNAME",
                                "tc-other");
        CHECK(n > 0 && strncmp(raw, "-WRONGPASS", 10) == 0, "HELLO 2 AUTH wrong password while logged in: -WRONGPASS",
              pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, acl_user) == 0, "failed HELLO AUTH: previous user kept", pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "CLIENT", "GETNAME");
        CHECK(strcmp(r, "tc-acl") == 0, "failed HELLO AUTH: name unchanged", pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "GET", "tc:resp3:acl");
        CHECK(strcmp(r, "v-acl") == 0, "failed HELLO AUTH: ACL connection still usable", pass, fail);

        n = resp_send_raw_reply(fda, raw, sizeof(raw), 5, "HELLO", "2", "AUTH", "default", "x");
        CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 14, "HELLO 2 AUTH default: back to RESP2 *14", pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, "default") == 0, "HELLO 2 AUTH default: identity back to default", pass, fail);
        proxy_cmd(fda, r, sizeof(r), 2, "DEL", "tc:resp3:acl");
        close(fda);
    } else {
        SKIP("HELLO 3 AUTH as a real ACL user");
    }

    /* ── RESP3 native passthrough: fd3 talks RESP3 now, backend replies
     * relayed verbatim, no downgrade applied ──────────────────────────── */
    printf("\n--- RESP3 client: native passthrough ---\n");

    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 2, "GET", "tc:resp3:nosuchkey");
    CHECK(n > 0 && strcmp(raw, "_\r\n") == 0, "RESP3 GET missing key: native null (_)", pass, fail);

    resp_send_raw_reply(fd3, raw, sizeof(raw), 4, "HSET", "tc:resp3:hash", "f1", "v1");
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 2, "HGETALL", "tc:resp3:hash");
    CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 1, "RESP3 HGETALL: native map (%1)", pass, fail);

    resp_send_raw_reply(fd3, raw, sizeof(raw), 3, "SADD", "tc:resp3:set", "a");
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 2, "SMEMBERS", "tc:resp3:set");
    CHECK(n > 0 && raw[0] == '~' && atoi(raw + 1) == 1, "RESP3 SMEMBERS: native set (~1)", pass, fail);

    resp_send_raw_reply(fd3, raw, sizeof(raw), 4, "ZADD", "tc:resp3:zset", "1", "a");
    resp_send_raw_reply(fd3, raw, sizeof(raw), 4, "ZADD", "tc:resp3:zset", "2", "b");
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 3, "ZSCORE", "tc:resp3:zset", "a");
    CHECK(n > 0 && raw[0] == ',', "RESP3 ZSCORE: native double (,)", pass, fail);

    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 5, "ZRANGE", "tc:resp3:zset", "0", "-1", "WITHSCORES");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 2,
          "RESP3 ZRANGE WITHSCORES: NOT flattened -- stays nested *2 [member,score]", pass, fail);
    CHECK(n > 0 && strstr(raw, "*2\r\n$1\r\na\r\n,1\r\n") != NULL,
          "RESP3 ZRANGE WITHSCORES: first pair is a nested *2 with a native double", pass, fail);

    resp_send_raw_reply(fd3, raw, sizeof(raw), 5, "XADD", "tc:resp3:stream", "*", "f1", "v1");
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 4, "XREAD", "STREAMS", "tc:resp3:stream", "0");
    CHECK(n > 0 && raw[0] == '%' && atoi(raw + 1) == 1, "RESP3 XREAD: NOT re-nested -- stays native map (%1)", pass,
          fail);

    resp_send_raw_reply(fd3, raw, sizeof(raw), 5, "GEOADD", "tc:resp3:geo", "13.36", "38.11", "Palermo");
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 4, "GEOPOS", "tc:resp3:geo", "Palermo", "NoSuchPlace");
    CHECK(n > 0 && strstr(raw, "_\r\n") != NULL, "RESP3 GEOPOS: missing member's slot is native null (_), not *-1",
          pass, fail);

    /* FUNCTION STATS is a fan-out whose per-node entries the proxy builds
     * itself (see fanout_agg_function_stats()), so it must re-encode them
     * as maps for a RESP3 client: *N of %3 {node-ip, running_script,
     * engines}, with engines and each engine's stats also maps. */
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 2, "FUNCTION", "STATS");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) >= 1, "RESP3 FUNCTION STATS: outer array, one entry per master", pass,
          fail);
    CHECK(n > 0 && strstr(raw, "%3\r\n$7\r\nnode-ip\r\n") != NULL && strstr(raw, "*6\r\n") == NULL,
          "RESP3 FUNCTION STATS: per-node entry is a %3 map, not a flat *6", pass, fail);
    CHECK(n > 0 && strstr(raw, "running_script\r\n_\r\n") != NULL,
          "RESP3 FUNCTION STATS: idle running_script is native null (_)", pass, fail);
    CHECK(n > 0 && strstr(raw, "engines\r\n%") != NULL && strstr(raw, "$3\r\nLUA\r\n%2\r\n") != NULL,
          "RESP3 FUNCTION STATS: engines is a map, LUA stats a %2 map", pass, fail);

    /* COMMAND DOCS is filtered by the proxy (see fanout_agg_command_docs()),
     * so it must re-encode the RESP2 reply for a RESP3 client: a map of
     * name -> doc map, history/flags as sets, arguments as arrays of maps. */
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 3, "COMMAND", "DOCS", "SET");
    CHECK(n > 0 && !strncmp(raw, "%1\r\n$3\r\nset\r\n%", 14), "RESP3 COMMAND DOCS: outer map of name -> doc map", pass,
          fail);
    CHECK(n > 0 && strstr(raw, "$7\r\nhistory\r\n~") != NULL, "RESP3 COMMAND DOCS: history is a set", pass, fail);
    CHECK(n > 0 && strstr(raw, "$9\r\narguments\r\n*") != NULL &&
              strstr(raw, "%4\r\n$4\r\nname\r\n$3\r\nkey\r\n") != NULL,
          "RESP3 COMMAND DOCS: arguments is an array of maps", pass, fail);
    CHECK(n > 0 && strstr(raw, "$5\r\nflags\r\n~1\r\n+optional\r\n") != NULL && strstr(raw, "flags\r\n*") == NULL,
          "RESP3 COMMAND DOCS: argument flags is a set", pass, fail);

    /* A container command's "subcommands" is itself a map of subcommand
     * name -> doc map, re-encoded recursively (history/arguments inside
     * each subcommand get the same treatment). */
    n = resp_send_raw_reply(fd3, raw, sizeof(raw), 3, "COMMAND", "DOCS", "CLIENT");
    CHECK(n > 0 && !strncmp(raw, "%1\r\n$6\r\nclient\r\n%", 17) && strstr(raw, "$11\r\nsubcommands\r\n%") != NULL,
          "RESP3 COMMAND DOCS: subcommands is a map", pass, fail);
    CHECK(n > 0 && strstr(raw, "$11\r\nclient|kill\r\n%") != NULL, "RESP3 COMMAND DOCS: each subcommand's doc is a map",
          pass, fail);
    CHECK(n > 0 && strstr(raw, "history\r\n~") != NULL && strstr(raw, "history\r\n*") == NULL &&
              strstr(raw, "flags\r\n*") == NULL,
          "RESP3 COMMAND DOCS: subcommand history/flags are sets", pass, fail);

    /* ── the headline requirement: a brand-new RESP2 connection, opened
     * *after* the global flag flipped and that has never itself said
     * "HELLO 3", must still see exactly the pre-RESP3 byte shapes ─────── */
    printf("\n--- RESP2 client after global RESP3 activation: must be byte-identical to before ---\n");
    int fd2 = proxy_connect(host, port);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 2, "GET", "tc:resp3:nosuchkey");
    CHECK(n > 0 && strcmp(raw, "$-1\r\n") == 0, "RESP2 GET missing key: nil bulk ($-1), not native null", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 2, "HGETALL", "tc:resp3:hash");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 2, "RESP2 HGETALL: downgraded to flat *2 array, not a map", pass,
          fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 2, "SMEMBERS", "tc:resp3:set");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 1, "RESP2 SMEMBERS: downgraded to flat *1 array, not a set", pass,
          fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "ZSCORE", "tc:resp3:zset", "a");
    CHECK(n > 0 && strcmp(raw, "$1\r\n1\r\n") == 0, "RESP2 ZSCORE: downgraded to bulk string, not a double", pass,
          fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 5, "ZRANGE", "tc:resp3:zset", "0", "-1", "WITHSCORES");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 4,
          "RESP2 ZRANGE WITHSCORES: flattened to *4 (2 member+score pairs)", pass, fail);
    CHECK(n > 0 && strstr(raw, "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n") != NULL,
          "RESP2 ZRANGE WITHSCORES: exact flat byte layout", pass, fail);

    resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "HSET", "tc:resp3:hrand", "f1", "v1");
    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "HRANDFIELD", "tc:resp3:hrand", "1", "WITHVALUES");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 2,
          "RESP2 HRANDFIELD WITHVALUES: flattened to *2, not nested pairs", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "XREAD", "STREAMS", "tc:resp3:stream", "0");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 1,
          "RESP2 XREAD: re-nested to *1 array-of-pairs, not a flattened map", pass, fail);
    CHECK(n > 0 && strstr(raw, "*1\r\n*2\r\n$15\r\n") != NULL,
          "RESP2 XREAD: outer element is a nested *2 [stream, entries] pair", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "GEOPOS", "tc:resp3:geo", "Palermo", "NoSuchPlace");
    CHECK(n > 0 && strstr(raw, "*2\r\n") != NULL && strstr(raw, "*-1\r\n") != NULL,
          "RESP2 GEOPOS: missing member's slot is a nil array (*-1), not nil bulk", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "BLPOP", "tc:resp3:nolist", "0.1");
    CHECK(n > 0 && strcmp(raw, "*-1\r\n") == 0, "RESP2 BLPOP timeout: nil array (*-1)", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "LMPOP", "1", "tc:resp3:nolist", "LEFT");
    CHECK(n > 0 && strcmp(raw, "*-1\r\n") == 0, "RESP2 LMPOP on empty key: nil array (*-1)", pass, fail);

    /* cross-slot fan-out (MGET) must still return a plain RESP2 array */
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", "tc:resp3:m1", "v1");
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", "tc:resp3:m2", "v2");
    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "MGET", "tc:resp3:m1", "tc:resp3:m2", "tc:resp3:nosuchkey");
    CHECK(n > 0 && raw[0] == '*' && atoi(raw + 1) == 3, "RESP2 cross-slot MGET (fan-out): still a plain *3 array", pass,
          fail);
    CHECK(n > 0 && strstr(raw, "$-1\r\n") != NULL,
          "RESP2 cross-slot MGET: missing key is nil bulk within the aggregated reply", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 2, "FUNCTION", "STATS");
    CHECK(n > 0 && raw[0] == '*' && strstr(raw, "*6\r\n$7\r\nnode-ip\r\n") != NULL && strchr(raw, '%') == NULL,
          "RESP2 FUNCTION STATS: per-node entry stays a flat *6 array, no maps", pass, fail);
    CHECK(n > 0 && strstr(raw, "running_script\r\n$-1\r\n") != NULL && strstr(raw, "$3\r\nLUA\r\n*4\r\n") != NULL,
          "RESP2 FUNCTION STATS: nil bulk running_script, LUA stats a flat *4", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "COMMAND", "DOCS", "SET");
    CHECK(n > 0 && !strncmp(raw, "*2\r\n$3\r\nset\r\n*", 14) && strchr(raw, '%') == NULL && strchr(raw, '~') == NULL,
          "RESP2 COMMAND DOCS: flat arrays, no maps or sets", pass, fail);

    n = resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "COMMAND", "DOCS", "CLIENT");
    CHECK(n > 0 && strstr(raw, "$11\r\nsubcommands\r\n*") != NULL && strstr(raw, "$11\r\nclient|kill\r\n*") != NULL,
          "RESP2 COMMAND DOCS: subcommands stays a flat array", pass, fail);

    /* ── pub/sub: push type ('>') for a RESP3 subscriber, plain array ('*')
     * for a RESP2 one, for both the confirmation and the pushed message ── */
    printf("\n--- pub/sub: push type vs plain array ---\n");

    int sub2 = proxy_connect(host, port);
    n = resp_send_raw_reply(sub2, raw, sizeof(raw), 2, "SUBSCRIBE", "tc:resp3:chan");
    CHECK(n > 0 && raw[0] == '*', "RESP2 subscriber: SUBSCRIBE confirmation is a plain array", pass, fail);

    int sub3 = proxy_connect(host, port);
    resp_send_raw_reply(sub3, raw, sizeof(raw), 2, "HELLO", "3");
    n = resp_send_raw_reply(sub3, raw, sizeof(raw), 2, "SUBSCRIBE", "tc:resp3:chan");
    CHECK(n > 0 && raw[0] == '>', "RESP3 subscriber: SUBSCRIBE confirmation is push type (>)", pass, fail);

    int pub = proxy_connect(host, port);
    proxy_cmd(pub, r, sizeof(r), 3, "PUBLISH", "tc:resp3:chan", "hello-resp3");
    close(pub);

    n = read_raw_reply(sub2, raw, sizeof(raw));
    CHECK(n > 0 && raw[0] == '*' && strstr(raw, "hello-resp3") != NULL,
          "RESP2 subscriber: pushed message delivered as a plain array (*)", pass, fail);

    n = read_raw_reply(sub3, raw, sizeof(raw));
    CHECK(n > 0 && raw[0] == '>' && strstr(raw, "hello-resp3") != NULL,
          "RESP3 subscriber: pushed message delivered as push type (>)", pass, fail);

    close(sub2);
    close(sub3);

    /* ── cleanup ──────────────────────────────────────────────────────── */
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:k1");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:hash");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:set");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:zset");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:hrand");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:stream");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:geo");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:m1");
    proxy_cmd(fd2, r, sizeof(r), 2, "DEL", "tc:resp3:m2");

    close(fd_before);
    close(fd3);
    close(fd2);
}
