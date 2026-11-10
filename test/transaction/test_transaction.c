#include "test_common.h"
#include <sys/time.h>

/* MULTI / EXEC / DISCARD / WATCH / UNWATCH (see src/txn.c). Every key a
 * transaction touches has to be in one slot, so the keys here share the
 * {tc:txn} hash tag unless a test is about crossing slots. */

#define K(x) "{tc:txn}" x

/* Sends an inline (telnet-style) line and reads one whole reply. */
static int inline_cmd(int fd, const char *line, char *out, int outsz) {
    int len = (int)strlen(line);
    if (write(fd, line, (size_t)len) != len) return -1;
    return read_raw_reply(fd, out, outsz);
}

/* SET key <value of n bytes>, raw reply in out. */
static int set_big(int fd, const char *key, int n, char *out, int outsz) {
    char *v = malloc((size_t)n);
    if (!v) return -1;
    memset(v, 'x', (size_t)n);
    const char *args[3] = {"SET", key, v};
    int lens[3] = {3, (int)strlen(key), n};
    int r = resp_send_raw(fd, 3, args, lens) < 0 ? -1 : read_raw_reply(fd, out, outsz);
    free(v);
    return r;
}

void run_transaction_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Transaction Tests ===\n");

    int fd = proxy_connect(host, port);
    int fd2 = proxy_connect(host, port);
    if (fd < 0 || fd2 < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        if (fd >= 0) close(fd);
        if (fd2 >= 0) close(fd2);
        return;
    }
    char raw[65536];
    int n;
    resp_send_raw_reply(fd, raw, sizeof(raw), 7, "DEL", K("foo"), K("bar"), K("a"), K("k"), K("big"), K("m"));

    /* ── example 1: MULTI / INCR / INCR / EXEC ────────────────────────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    CHECK(!strcmp(raw, "+OK\r\n"), "MULTI: +OK", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "INCR", K("foo"));
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "INCR inside MULTI: +QUEUED", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "INCR", K("bar"));
    n = resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(n > 0 && !strcmp(raw, "*2\r\n:1\r\n:1\r\n"), "EXEC: both INCRs run, *2 :1 :1", pass, fail);

    /* foo and bar hash to different slots: CROSSSLOT, then EXECABORT */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "INCR", "foo");
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "cross-slot transaction: first key queued", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "INCR", "bar");
    CHECK(!strncmp(raw, "-CROSSSLOT", 10), "cross-slot transaction: second slot gets CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "-EXECABORT Transaction discarded because of previous errors.\r\n"),
          "cross-slot transaction: EXEC answers EXECABORT", pass, fail);

    /* ── example 2: an error at run time is one element of EXEC's reply ── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "abc");
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "LPOP", K("a"));
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "LPOP on a string inside MULTI: still +QUEUED", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*2\r\n+OK\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
          "EXEC: *2 +OK -WRONGTYPE", pass, fail);

    /* ── example 3: an error at queue time aborts the transaction ─────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 4, "INCR", K("a"), "b", "c");
    CHECK(!strcmp(raw, "-ERR wrong number of arguments for 'incr' command\r\n"),
          "INCR with extra arguments inside MULTI: the backend's arity error", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "-EXECABORT Transaction discarded because of previous errors.\r\n"),
          "EXEC after a queue-time error: EXECABORT", pass, fail);

    /* ── examples 2 and 3 typed inline, as in telnet ──────────────────── */
    {
        int t = proxy_connect(host, port);
        char a[4][256];
        inline_cmd(t, "DEL {tc:txn}a\r\n", raw, sizeof(raw));
        inline_cmd(t, "MULTI\r\n", a[0], sizeof(a[0]));
        inline_cmd(t, "SET {tc:txn}a abc\r\n", a[1], sizeof(a[1]));
        inline_cmd(t, "LPOP {tc:txn}a\r\n", a[2], sizeof(a[2]));
        inline_cmd(t, "EXEC\r\n", raw, sizeof(raw));
        CHECK(!strcmp(a[0], "+OK\r\n") && !strcmp(a[1], "+QUEUED\r\n") && !strcmp(a[2], "+QUEUED\r\n") &&
                  !strcmp(raw, "*2\r\n+OK\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
              "inline (telnet): example 2 -- MULTI / SET / LPOP / EXEC", pass, fail);
        inline_cmd(t, "MULTI\r\n", a[0], sizeof(a[0]));
        inline_cmd(t, "INCR {tc:txn}a b c\r\n", a[1], sizeof(a[1]));
        inline_cmd(t, "EXEC\r\n", raw, sizeof(raw));
        CHECK(!strcmp(a[1], "-ERR wrong number of arguments for 'incr' command\r\n") &&
                  !strcmp(raw, "-EXECABORT Transaction discarded because of previous errors.\r\n"),
              "inline (telnet): example 3 -- arity error, then EXECABORT", pass, fail);
        /* every command can be inline now, not just PING / ECHO / AUTH /
         * SELECT / QUIT */
        inline_cmd(t, "SET {tc:txn}a \"x y\\x41\"\r\n", raw, sizeof(raw));
        inline_cmd(t, "GET {tc:txn}a\r\n", raw, sizeof(raw));
        CHECK(!strcmp(raw, "$4\r\nx yA\r\n"), "inline: quoted argument with \\x escape", pass, fail);
        inline_cmd(t, "MSET {tc:txn}m1 1 {tc:txn}m2 2 {tc:txn}m3 3 {tc:txn}m4 4 {tc:txn}m5 5\r\n", raw, sizeof(raw));
        CHECK(!strcmp(raw, "+OK\r\n"), "inline: more than 8 arguments", pass, fail);
        inline_cmd(t, "FOOBAR x\r\n", raw, sizeof(raw));
        CHECK(!strcmp(raw, "-ERR unsupport command\r\n"), "inline: unsupported command refused like over RESP", pass,
              fail);
        inline_cmd(t, "\r\nPING\r\n", raw, sizeof(raw));
        CHECK(!strcmp(raw, "+PONG\r\n"), "inline: empty line ignored", pass, fail);
        inline_cmd(t, "DEL {tc:txn}m1 {tc:txn}m2 {tc:txn}m3 {tc:txn}m4 {tc:txn}m5\r\n", raw, sizeof(raw));
        close(t);
    }

    /* ── example 6: DISCARD ───────────────────────────────────────────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("foo"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "INCR", K("foo"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "DISCARD");
    CHECK(!strcmp(raw, "+OK\r\n"), "DISCARD: +OK", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "GET", K("foo"));
    CHECK(!strcmp(raw, "$1\r\n1\r\n"), "DISCARD: queued INCR never ran", pass, fail);

    /* ── example 7: WATCH ─────────────────────────────────────────────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    CHECK(!strcmp(raw, "+OK\r\n"), "WATCH: +OK", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "GET", K("k"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "2");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*1\r\n+OK\r\n"), "WATCH / GET / MULTI / SET / EXEC, untouched: runs", pass, fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", K("k"), "other");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "3");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*-1\r\n"), "WATCH, then another client writes the key: EXEC answers nil", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "GET", K("k"));
    CHECK(!strcmp(raw, "$5\r\nother\r\n"), "aborted transaction left the other client's write", pass, fail);

    /* nothing queued: EXEC still has to check the WATCH */
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", K("k"), "4");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*-1\r\n"), "WATCH / MULTI / EXEC with the key changed: nil", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*0\r\n"), "WATCH / MULTI / EXEC with the key untouched: *0", pass, fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "UNWATCH");
    CHECK(!strcmp(raw, "+OK\r\n"), "UNWATCH: +OK", pass, fail);
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", K("k"), "5");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "6");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*1\r\n+OK\r\n"), "after UNWATCH, another client's write no longer aborts", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "UNWATCH");
    CHECK(!strcmp(raw, "+OK\r\n"), "UNWATCH with nothing watched: +OK", pass, fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "7");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "DISCARD");
    resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", K("k"), "8");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("k"), "9");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*1\r\n+OK\r\n"), "DISCARD also ends the WATCH", pass, fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "WATCH", "foo", "bar");
    CHECK(!strncmp(raw, "-CROSSSLOT", 10), "WATCH keys in different slots: CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("k"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", "foo");
    CHECK(!strncmp(raw, "-CROSSSLOT", 10), "second WATCH in another slot: CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", "foo", "1");
    CHECK(!strncmp(raw, "-CROSSSLOT", 10), "queued key in another slot than the WATCH: CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strncmp(raw, "-EXECABORT", 10), "... and EXECABORT", pass, fail);

    /* ── errors around MULTI ──────────────────────────────────────────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "-ERR EXEC without MULTI\r\n"), "EXEC without MULTI", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "DISCARD");
    CHECK(!strcmp(raw, "-ERR DISCARD without MULTI\r\n"), "DISCARD without MULTI", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*0\r\n"), "MULTI / EXEC with nothing queued: *0", pass, fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    CHECK(!strcmp(raw, "-ERR MULTI calls can not be nested\r\n"), "nested MULTI: error", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "WATCH", K("a"));
    CHECK(!strcmp(raw, "-ERR WATCH inside MULTI is not allowed\r\n"), "WATCH inside MULTI: error", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*1\r\n+OK\r\n"), "nested MULTI / WATCH inside MULTI don't abort the transaction", pass, fail);

    /* keyless commands: only once a key has picked the node */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "PING");
    CHECK(raw[0] == '-', "PING before any key inside MULTI: refused", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strncmp(raw, "-EXECABORT", 10), "... and the transaction is aborted", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "PING");
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "PING after a key: queued", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "ECHO", "hi");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "EVAL", "return 7", "0");
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "EVAL with no keys after a key: queued", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*4\r\n+OK\r\n+PONG\r\n$2\r\nhi\r\n:7\r\n"), "EXEC: SET / PING / ECHO / EVAL results", pass,
          fail);

    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "DBSIZE");
    CHECK(!strcmp(raw, "-ERR Command not allowed inside a transaction\r\n"), "DBSIZE inside MULTI: not allowed", pass,
          fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "SUBSCRIBE", "tc:txn:ch");
    CHECK(raw[0] == '-', "SUBSCRIBE inside MULTI: not allowed", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "NOSUCHCMD", "x");
    CHECK(!strcmp(raw, "-ERR unsupport command\r\n"), "unsupported command inside MULTI: refused", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strncmp(raw, "-EXECABORT", 10), "... and EXEC answers EXECABORT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "PING");
    CHECK(!strcmp(raw, "+PONG\r\n"), "client still usable (not subscribed) after the aborted transaction", pass, fail);

    /* same-slot multi-key commands are forwarded whole */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 5, "MSET", K("m1"), "1", K("m2"), "2");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "MGET", K("m1"), K("m2"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "DEL", K("m1"), K("m2"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*3\r\n+OK\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n:2\r\n"), "same-slot MSET / MGET / DEL in one EXEC",
          pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "MGET", "foo", "bar");
    CHECK(!strncmp(raw, "-CROSSSLOT", 10), "cross-slot MGET inside MULTI: CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "DISCARD");

    /* ── big values: whole before the deferred MULTI, streamed after ──── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    n = set_big(fd, K("big"), 100000, raw, sizeof(raw));
    CHECK(n > 0 && !strcmp(raw, "+QUEUED\r\n"), "100KB SET as the first queued command: +QUEUED", pass, fail);
    n = set_big(fd, K("big"), 90000, raw, sizeof(raw));
    CHECK(n > 0 && !strcmp(raw, "+QUEUED\r\n"), "100KB SET queued after it: +QUEUED", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "STRLEN", K("big"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strcmp(raw, "*3\r\n+OK\r\n+OK\r\n:90000\r\n"), "EXEC with big values", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    n = set_big(fd, "tc:txn:otherslot", 100000, raw, sizeof(raw));
    CHECK(n > 0 && !strncmp(raw, "-CROSSSLOT", 10), "100KB SET in another slot: CROSSSLOT", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "PING");
    CHECK(!strcmp(raw, "+QUEUED\r\n"), "the rejected 100KB value was skipped: next command parses", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    CHECK(!strncmp(raw, "-EXECABORT", 10), "... and EXEC answers EXECABORT", pass, fail);

    /* ── CLIENT INFO multi= / watch= / flags ───────────────────────────── */
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "WATCH", K("k"), K("a"));
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "CLIENT", "INFO");
    CHECK(strstr(raw, " multi=-1 watch=2 ") != NULL, "CLIENT INFO after WATCH of 2 keys: watch=2", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "MULTI");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "1");
    resp_send_raw_reply(fd, raw, sizeof(raw), 3, "SET", K("a"), "2");
    resp_send_raw_reply(fd2, raw, sizeof(raw), 4, "CLIENT", "LIST", "TYPE", "normal");
    CHECK(strstr(raw, " flags=x ") && strstr(raw, " multi=2 watch=2 "),
          "CLIENT LIST from another client: flags=x multi=2 watch=2", pass, fail);
    resp_send_raw_reply(fd, raw, sizeof(raw), 1, "EXEC");
    resp_send_raw_reply(fd, raw, sizeof(raw), 2, "CLIENT", "INFO");
    CHECK(strstr(raw, " flags=N ") && strstr(raw, " multi=-1 watch=0 "), "CLIENT INFO after EXEC: multi=-1 watch=0",
          pass, fail);

    /* ── more transactions at once than max_transaction_connection: the
     * rest wait for a connection instead of failing ───────────────────── */
    {
#define NTX 40
        int fds[NTX], okc = 0;
        struct timeval tv = {10, 0};
        for (int i = 0; i < NTX; i++) {
            fds[i] = proxy_connect(host, port);
            if (fds[i] >= 0) setsockopt(fds[i], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        for (int i = 0; i < NTX; i++)
            if (fds[i] >= 0) resp_send(fds[i], 2, "WATCH", K("k"));
        for (int i = 0; i < NTX; i++)
            if (fds[i] >= 0) {
                resp_send(fds[i], 1, "MULTI");
                resp_send(fds[i], 2, "INCR", K("n"));
                resp_send(fds[i], 1, "EXEC");
            }
        for (int i = 0; i < NTX; i++) {
            if (fds[i] < 0) continue;
            char a[4][128];
            int good = 1;
            for (int j = 0; j < 4; j++)
                if (read_raw_reply(fds[i], a[j], sizeof(a[j])) <= 0) good = 0;
            if (good && !strcmp(a[0], "+OK\r\n") && !strcmp(a[1], "+OK\r\n") && !strcmp(a[2], "+QUEUED\r\n") &&
                (!strncmp(a[3], "*1\r\n:", 5) || !strcmp(a[3], "*-1\r\n")))
                okc++;
            close(fds[i]);
        }
        char msg[96];
        snprintf(msg, sizeof(msg), "%d concurrent WATCH / MULTI / EXEC clients all answered (%d ok)", NTX, okc);
        CHECK(okc == NTX, msg, pass, fail);
    }

    /* ── as a real ACL user: runs on the user's own connection ─────────── */
    if (g_test_acl_user) {
        int fa = proxy_connect(host, port);
        resp_send_raw_reply(fa, raw, sizeof(raw), 3, "AUTH", g_test_acl_user, g_test_acl_pass);
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "WATCH", K("k"));
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "MULTI");
        resp_send_raw_reply(fa, raw, sizeof(raw), 3, "SET", K("acl"), "v");
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "GET", K("acl"));
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "ACL");
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "EXEC");
        CHECK(!strncmp(raw, "-EXECABORT", 10), "ACL user: ACL inside MULTI aborts the transaction", pass, fail);
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "WATCH", K("k"));
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "MULTI");
        resp_send_raw_reply(fa, raw, sizeof(raw), 3, "SET", K("acl"), "v");
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "GET", K("acl"));
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "EXEC");
        CHECK(!strcmp(raw, "*2\r\n+OK\r\n$1\r\nv\r\n"), "ACL user: WATCH / MULTI / SET / GET / EXEC", pass, fail);
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "WATCH", K("k"));
        resp_send_raw_reply(fd2, raw, sizeof(raw), 3, "SET", K("k"), "acl-other");
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "MULTI");
        resp_send_raw_reply(fa, raw, sizeof(raw), 3, "SET", K("acl"), "w");
        resp_send_raw_reply(fa, raw, sizeof(raw), 1, "EXEC");
        CHECK(!strcmp(raw, "*-1\r\n"), "ACL user: WATCH abort answers nil", pass, fail);
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "GET", K("acl"));
        CHECK(!strcmp(raw, "$1\r\nv\r\n"), "ACL user: still usable after the transactions", pass, fail);
        resp_send_raw_reply(fa, raw, sizeof(raw), 2, "DEL", K("acl"));
        close(fa);
    } else {
        SKIP("transactions as a real ACL user");
    }

    resp_send_raw_reply(fd, raw, sizeof(raw), 7, "DEL", K("foo"), K("bar"), K("a"), K("k"), K("big"), K("n"));
    close(fd);
    close(fd2);
}
