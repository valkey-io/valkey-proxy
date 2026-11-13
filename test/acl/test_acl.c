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
     * substitutes its own fixed 16-line help array) ────────────────────── */
    static const char *help_lines[] = {
        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CAT [<category>]",
        "    List all commands that belong to <category>, or all command categories",
        "    when no category is specified.",
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
    CHECK(strcmp(r, "*16") == 0, "ACL HELP: returns the 16-line help array", pass, fail);
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

    /* ── ACL CAT [category] — answered by the proxy from its built-in table
     * (src/acl_cat_data.h), listing only commands the proxy supports. ──── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "CAT");
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "ACL CAT: returns the category list", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1), has_string = 0, has_connection = 0;
        for (int i = 0; i < n; i++) {
            resp_next_str(fd, r, sizeof(r));
            if (!strcmp(r, "string")) has_string = 1;
            if (!strcmp(r, "connection")) has_connection = 1;
        }
        CHECK(has_string && has_connection, "ACL CAT: lists the string and connection categories", pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 3, "ACL", "CAT", "StrInG");
    CHECK(r[0] == '*' && atoi(r + 1) > 0, "ACL CAT StrInG: category name is case-insensitive", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1), has_get = 0, has_set = 0;
        for (int i = 0; i < n; i++) {
            resp_next_str(fd, r, sizeof(r));
            if (!strcmp(r, "get")) has_get = 1;
            if (!strcmp(r, "set")) has_set = 1;
        }
        CHECK(has_get && has_set, "ACL CAT string: lists get and set", pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 3, "ACL", "CAT", "admin");
    CHECK(r[0] == '*', "ACL CAT admin: returns an array", pass, fail);
    if (r[0] == '*') {
        int n = atoi(r + 1), has_unsupported = 0, has_supported = 0;
        for (int i = 0; i < n; i++) {
            resp_next_str(fd, r, sizeof(r));
            if (!strcmp(r, "acl|setuser") || !strcmp(r, "client|kill")) has_unsupported = 1;
            if (!strcmp(r, "acl|list")) has_supported = 1;
        }
        CHECK(has_supported && !has_unsupported,
              "ACL CAT admin: lists acl|list but not unsupported acl|setuser / client|kill", pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 3, "ACL", "CAT", "tc-nosuchcat");
    CHECK(strcmp(r, "-ERR Unknown category 'tc-nosuchcat'") == 0, "ACL CAT unknown category: -ERR Unknown category",
          pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "ACL", "CAT", "string", "extra");
    CHECK(r[0] == '-' && strstr(r, "wrong number of arguments") != NULL, "ACL CAT with 2 arguments: rejected", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "PING", "tc-after-acl-cat");
    CHECK(strcmp(r, "tc-after-acl-cat") == 0, "ACL CAT: connection still in sync afterwards", pass, fail);

    /* ── unsupported ACL subcommand — must be rejected locally by the
     * proxy, without ever reaching a backend node. ─────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 2, "ACL", "SETUSER");
    CHECK(r[0] == '-' && strstr(r, "unknown ACL subcommand") != NULL,
          "ACL SETUSER: unsupported subcommand rejected locally", pass, fail);

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
        /* ACL CAT never reaches a backend, so even a restricted user gets it. */
        proxy_cmd(fa, r, sizeof(r), 3, "ACL", "CAT", "string");
        CHECK(r[0] == '*' && atoi(r + 1) > 0, "AUTH read-only user: ACL CAT answered by the proxy", pass, fail);
        if (r[0] == '*') {
            int n = atoi(r + 1);
            for (int i = 0; i < n; i++) resp_skip_reply(fa);
        }

        proxy_cmd(fa, r, sizeof(r), 3, "AUTH", "default", "x");
        CHECK(strcmp(r, "+OK") == 0, "AUTH default after a real user: +OK", pass, fail);
        proxy_cmd(fa, r, sizeof(r), 2, "DEL", "tc:acl:auth");
        CHECK(strcmp(r, ":1") == 0, "AUTH default: write allowed again on the shared pools", pass, fail);
    } else {
        SKIP("AUTH as a real ACL user");
    }
    close(fa);

    /* ── CLIENT LIST / PROXY POOLS / PROXY INFO need the permission to run
     * CLIENT LIST, which Valkey decides for a logged-in user. Each session
     * starts with a different one, so each goes through the probe (later
     * ones within 5s are answered from the per-user cache). ──────────── */
    if (g_test_acl_user) {
        const int cap = 1 << 20;
        char *big2 = malloc((size_t)cap);
        int fn = proxy_connect(host, port);
        proxy_cmd(fn, r, sizeof(r), 3, "AUTH", g_test_acl_noadmin_user, g_test_acl_noadmin_pass);
        CHECK(strcmp(r, "+OK") == 0, "admin check: AUTH user without @admin", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 2, "PROXY", "POOLS");
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|pools'"),
              "admin check: PROXY POOLS without @admin is -NOPERM naming proxy|pools", pass, fail);
        /* The probe's reply was used up. ACL WHOAMI goes on the probe's own
         * connection (the first master's), so a leftover reply would show. */
        proxy_cmd(fn, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, g_test_acl_noadmin_user) == 0, "admin check: ACL WHOAMI in step after -NOPERM", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 3, "SET", "tc:acl:admin", "v-admin");
        CHECK(strcmp(r, "+OK") == 0, "admin check: SET as the user after -NOPERM", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 2, "GET", "tc:acl:admin");
        CHECK(strcmp(r, "v-admin") == 0, "admin check: GET as the user after -NOPERM", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 3, "PROXY", "INFO", "server");
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|info'"),
              "admin check: PROXY INFO without @admin is -NOPERM naming proxy|info", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 2, "CLIENT", "LIST");
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'client|list'"),
              "admin check: CLIENT LIST without @admin is -NOPERM", pass, fail);
        /* INFO is allowed, but its Pools section is PROXY POOLS' data. */
        int n = big2 ? resp_send_raw_reply(fn, big2, cap, 2, "INFO", "pools") : -1;
        CHECK(n > 0 && big2[0] == '$' && !strstr(big2, "# Pools"),
              "admin check: INFO pools without @admin leaves Pools out", pass, fail);
        n = big2 ? resp_send_raw_reply(fn, big2, cap, 2, "INFO", "everything") : -1;
        CHECK(n > 0 && big2[0] == '$' && strstr(big2, "# Server") && !strstr(big2, "# Pools"),
              "admin check: INFO everything without @admin has the rest, not Pools", pass, fail);
        proxy_cmd(fn, r, sizeof(r), 3, "PROXY", "POOLS", "extra");
        CHECK(strstr(r, "wrong number of arguments") != NULL, "admin check: PROXY POOLS arity error comes first", pass,
              fail);
        proxy_cmd(fn, r, sizeof(r), 2, "DEL", "tc:acl:admin");
        CHECK(strcmp(r, ":1") == 0, "admin check: DEL as the user", pass, fail);
        /* AUTH default goes back to the default identity, which isn't checked. */
        proxy_cmd(fn, r, sizeof(r), 3, "AUTH", "default", "x");
        n = big2 ? resp_send_raw_reply(fn, big2, cap, 2, "CLIENT", "LIST") : -1;
        CHECK(n > 0 && big2[0] == '$' && strstr(big2, "user=default"), "admin check: default identity isn't checked",
              pass, fail);
        close(fn);

        const char *ok_users[2][2] = {{g_test_acl_user, g_test_acl_pass}, {g_test_acl_cl_user, g_test_acl_cl_pass}};
        const char *ok_labels[2] = {"+@all", "only +client|list"};
        for (int u = 0; u < 2; u++) {
            char label[128];
            int fo = proxy_connect(host, port);
            proxy_cmd(fo, r, sizeof(r), 3, "AUTH", ok_users[u][0], ok_users[u][1]);
            snprintf(label, sizeof(label), "admin check: AUTH user with %s", ok_labels[u]);
            CHECK(strcmp(r, "+OK") == 0, label, pass, fail);
            /* First command: PROXY INFO for one user, PROXY POOLS for the other. */
            if (u == 0)
                n = big2 ? resp_send_raw_reply(fo, big2, cap, 3, "PROXY", "INFO", "server") : -1;
            else
                n = big2 ? resp_send_raw_reply(fo, big2, cap, 2, "PROXY", "POOLS") : -1;
            snprintf(label, sizeof(label), "admin check: PROXY %s allowed with %s", u == 0 ? "INFO" : "POOLS",
                     ok_labels[u]);
            CHECK(n > 0 && big2[0] != '-' && strstr(big2, u == 0 ? "# Server" : "# Pools"), label, pass, fail);
            /* ACL WHOAMI right after, on the probe's own connection: the
             * probe's reply didn't leak into it. The CLIENT LIST-only user
             * gets Valkey's own -NOPERM. */
            proxy_cmd(fo, r, sizeof(r), 2, "ACL", "WHOAMI");
            snprintf(label, sizeof(label), "admin check: ACL WHOAMI in step after the probe, with %s", ok_labels[u]);
            CHECK(u == 0 ? strcmp(r, ok_users[u][0]) == 0
                         : strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'acl|whoami'") != NULL,
                  label, pass, fail);
            /* Pipelined, so a reply out of order would show. */
            char want[128];
            snprintf(want, sizeof(want), "user=%s ", ok_users[u][0]);
            const char *pipe2 = "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n";
            ssize_t wr = write(fo, pipe2, strlen(pipe2));
            (void)wr;
            int got = big2 ? resp_next_str(fo, big2, cap) != NULL : 0;
            snprintf(label, sizeof(label), "admin check: CLIENT LIST allowed with %s", ok_labels[u]);
            CHECK(got && strstr(big2, want), label, pass, fail);
            got = resp_next_str(fo, r, sizeof(r)) != NULL;
            snprintf(label, sizeof(label), "admin check: a pipelined ACL WHOAMI after it, with %s", ok_labels[u]);
            CHECK(got && (u == 0 ? strcmp(r, ok_users[u][0]) == 0 : strstr(r, "'acl|whoami'") != NULL), label, pass,
                  fail);
            if (u == 0)
                n = big2 ? resp_send_raw_reply(fo, big2, cap, 2, "PROXY", "POOLS") : -1;
            else
                n = big2 ? resp_send_raw_reply(fo, big2, cap, 3, "PROXY", "INFO", "server") : -1;
            snprintf(label, sizeof(label), "admin check: PROXY %s allowed with %s", u == 0 ? "POOLS" : "INFO",
                     ok_labels[u]);
            CHECK(n > 0 && big2[0] != '-' && strstr(big2, u == 0 ? "# Pools" : "# Server"), label, pass, fail);
            n = big2 ? resp_send_raw_reply(fo, big2, cap, 2, "INFO", "pools") : -1;
            snprintf(label, sizeof(label), "admin check: INFO pools has Pools with %s", ok_labels[u]);
            CHECK(n > 0 && big2[0] == '$' && strstr(big2, "# Pools"), label, pass, fail);
            close(fo);
        }

        /* The backend closes the user's idle connection (timeout, restart):
         * the probe finds it closed. INFO just leaves Pools out, other
         * commands fail, and neither is cached, so the next probe opens a
         * new connection. */
        int fk = proxy_connect(host, port);
        proxy_cmd(fk, r, sizeof(r), 3, "AUTH", g_test_acl_kill_user, g_test_acl_kill_pass);
        CHECK(strcmp(r, "+OK") == 0, "admin check: AUTH user whose connection gets closed", pass, fail);
        test_acl_fixture_kill_user(g_test_acl_kill_user);
        n = big2 ? resp_send_raw_reply(fk, big2, cap, 2, "INFO", "everything") : -1;
        CHECK(n > 0 && big2[0] == '$' && strstr(big2, "# Server") && !strstr(big2, "# Pools"),
              "admin check: INFO everything on a closed connection leaves Pools out", pass, fail);
        /* ACL WHOAMI goes on the probe's connection: it opens a new one. */
        proxy_cmd(fk, r, sizeof(r), 2, "ACL", "WHOAMI");
        CHECK(strcmp(r, g_test_acl_kill_user) == 0, "admin check: ACL WHOAMI on a new connection after INFO", pass,
              fail);
        /* AUTH again for a new connection to close. */
        proxy_cmd(fk, r, sizeof(r), 3, "AUTH", g_test_acl_kill_user, g_test_acl_kill_pass);
        CHECK(strcmp(r, "+OK") == 0, "admin check: AUTH again for a new connection", pass, fail);
        test_acl_fixture_kill_user(g_test_acl_kill_user);
        proxy_cmd(fk, r, sizeof(r), 2, "CLIENT", "LIST");
        CHECK(strncmp(r, "-ERR backend connection", 23) == 0,
              "admin check: CLIENT LIST on a closed connection fails just the command", pass, fail);
        /* Pipelined with an ACL WHOAMI on the same connection, so input
         * buffered during the probe is read once it's done, and a reply out
         * of order would show. */
        const char *pipe3 = "*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n";
        int okw = write(fk, pipe3, strlen(pipe3)) == (ssize_t)strlen(pipe3);
        int got = okw && big2 && resp_next_str(fk, big2, cap) != NULL;
        CHECK(got && strstr(big2, "user=tc-proxy-acl-kill "),
              "admin check: the next CLIENT LIST probes on a new connection", pass, fail);
        got = okw && resp_next_str(fk, r, sizeof(r)) != NULL;
        CHECK(got && strcmp(r, g_test_acl_kill_user) == 0, "admin check: a pipelined ACL WHOAMI after that probe", pass,
              fail);
        close(fk);
        free(big2);
    } else {
        SKIP("admin check for CLIENT LIST / PROXY POOLS / PROXY INFO");
    }
}
