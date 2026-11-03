#include "test_common.h"

void run_acl_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== ACL Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[1024];

    /* ── ACL HELP (forwarded to a backend node; on success the proxy
     * substitutes its own fixed 13-line help array) ────────────────────── */
    static const char *help_lines[] = {
        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "DRYRUN <username> <command> [<arg> ...]",
        "    Returns whether the user can execute the given command without executing the command.",
        "GETUSER <username>",
        "    Get the user's details.",
        "LIST",
        "    Show users details in config file format.",
        "USERS",
        "    List all the registered usernames.",
        "WHOAMI",
        "    Return the current connection username.",
        "HELP",
        "    Print this help.",
    };
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "HELP");
    CHECK(strcmp(r, "*13") == 0, "ACL HELP: returns the 13-line help array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        int matched = 1;
        for (int i = 0; i < n; i++) {
            resp_next_str(fd, r, sizeof(r));
            if (i >= (int)(sizeof(help_lines) / sizeof(help_lines[0])) || strcmp(r, help_lines[i]) != 0) matched = 0;
        }
        CHECK(matched, "ACL HELP: help text matches the expected fixed lines", pass, fail);
    }

    /* ── ACL LIST — forwarded verbatim, one entry per configured user ───── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "LIST");
    CHECK(r[0] == '*', "ACL LIST: returns an array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── ACL USERS — forwarded verbatim, list of usernames ───────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "USERS");
    CHECK(r[0] == '*', "ACL USERS: returns an array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── ACL WHOAMI — forwarded verbatim; proxy's backend connections use
     * the default identity, so this must read back "default". ──────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "WHOAMI");
    CHECK(strcmp(r, "default") == 0, "ACL WHOAMI: reports the default identity", pass, fail);

    /* ── ACL GETUSER default — forwarded verbatim ────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 3, "ACL", "GETUSER", "default");
    CHECK(r[0] == '*', "ACL GETUSER default: returns the user's details as an array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1);
        for (int i = 0; i < n; i++) resp_skip_reply(fd);
    }

    /* ── ACL DRYRUN default GET <key> — forwarded verbatim; the default
     * user can run any command, so this must succeed. ──────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "ACL", "DRYRUN", "default", "GET", "tc:acl:dryrun");
    CHECK(strcmp(r, "+OK") == 0, "ACL DRYRUN default GET: reports the command is allowed", pass, fail);

    /* ── unsupported ACL subcommand — must be rejected locally by the
     * proxy, without ever reaching a backend node. ─────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "CAT");
    CHECK(r[0] == '-' && strstr(r, "unknown ACL subcommand") != NULL,
          "ACL CAT: unsupported subcommand rejected locally", pass, fail);

    /* ── bare ACL (no subcommand) — must be rejected with an error ───────── */
    proxy_cmd(fd, r, sizeof(r), 1, "ACL");
    CHECK(r[0] == '-', "ACL: missing subcommand is rejected", pass, fail);

    /* ── AUTH: argument handling and the default identity (never checked
     * against the backend, which is assumed nopass) ────────────────────── */
    int fa = proxy_connect(host, port);
    char big[4096];
    proxy_cmd(fa, r, sizeof(r), 3, "AUTH", "tc-nosuchuser", "badpass");
    CHECK(strncmp(r, "-WRONGPASS", 10) == 0, "AUTH bad credentials: backend -WRONGPASS relayed", pass, fail);
    proxy_cmd(fa, r, sizeof(r), 4, "AUTH", "a", "b", "c");
    CHECK(r[0] == '-' && strstr(r, "wrong number of arguments") != NULL,
          "AUTH with 3 arguments: wrong number of arguments", pass, fail);
    proxy_cmd(fa, r, sizeof(r), 2, "AUTH", "anything");
    CHECK(strcmp(r, "+OK") == 0, "AUTH <password>: accepted as the default user", pass, fail);
    proxy_cmd(fa, r, sizeof(r), 3, "AUTH", "default", "anything");
    CHECK(strcmp(r, "+OK") == 0, "AUTH default <password>: accepted", pass, fail);
    proxy_cmd(fa, r, sizeof(r), 2, "ACL", "WHOAMI");
    CHECK(strcmp(r, "default") == 0, "AUTH default: ACL WHOAMI is default", pass, fail);

    /* ── AUTH as a real non-default user: business commands must run as
     * that user on its own ACL connections, not on the shared pools. ──── */
    if (g_test_acl_user) {
        proxy_cmd(fa, r, sizeof(r), 3, "AUTH", g_test_acl_user, g_test_acl_pass);
        CHECK(strcmp(r, "+OK") == 0, "AUTH user pass: +OK", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, g_test_acl_user) == 0, "AUTH user pass: ACL WHOAMI reports the user", pass, fail);
        proxy_cmd(fa, big, sizeof(big), 2, "CLIENT", "INFO");
        {
            char want[128];
            snprintf(want, sizeof(want), "user=%s ", g_test_acl_user);
            /* The earlier failed "AUTH tc-nosuchuser ..." left a longer name
             * behind in the proxy's buffer: this also guards against it
             * leaking into the new one (e.g. "user=tc-proxy-aclr"). */
            CHECK(strstr(big, want) != NULL, "AUTH user pass: CLIENT INFO shows exactly the user", pass, fail);
        }
        proxy_cmd(fa, r, sizeof(r), 3, "SET", "tc:acl:auth", "v-auth");
        CHECK(strcmp(r, "+OK") == 0, "AUTH user pass: SET as the user", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "GET", "tc:acl:auth");
        CHECK(strcmp(r, "v-auth") == 0, "AUTH user pass: GET as the user", pass, fail);

        /* Existing AUTH semantics: a failed AUTH drops the client back to
         * the default identity (HELLO ... AUTH instead keeps the current
         * user -- see the resp3 suite). */
        proxy_cmd(fa, r, sizeof(r), 3, "AUTH", g_test_acl_user, "tc-wrong-pass");
        CHECK(strncmp(r, "-WRONGPASS", 10) == 0, "AUTH wrong password: -WRONGPASS", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, "default") == 0, "failed AUTH: back to the default identity", pass, fail);

        proxy_cmd(fa, r, sizeof(r), 3, "AUTH", g_test_acl_ro_user, g_test_acl_ro_pass);
        CHECK(strcmp(r, "+OK") == 0, "AUTH read-only user: +OK", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "GET", "tc:acl:auth");
        CHECK(strcmp(r, "v-auth") == 0, "AUTH read-only user: GET allowed", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 3, "SET", "tc:acl:auth", "v-denied");
        CHECK(strncmp(r, "-NOPERM", 7) == 0, "AUTH read-only user: SET is -NOPERM (runs as that user, not default)",
              pass, fail);

        proxy_cmd(fa, r, sizeof(r), 3, "AUTH", "default", "x");
        CHECK(strcmp(r, "+OK") == 0, "AUTH default after a real user: +OK", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "DEL", "tc:acl:auth");
        CHECK(strcmp(r, ":1") == 0, "AUTH default: write allowed again on the shared pools", pass, fail);
    } else {
        SKIP("AUTH as a real ACL user");
    }
    close(fa);
}
