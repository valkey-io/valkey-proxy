#include "test_common.h"
#include <time.h>

/* PROXY CONFIG GET / SET / SAVE / HELP (see src/proxy_config.c). Only users
 * named in proxy.conf may run them, so beyond checking that the default
 * identity is refused, the suite needs run-test's --cluster option (it
 * creates the ACL users) and a proxy started with
 *   proxy-config-admin-users     tc-proxy-acl
 *   proxy-config-readonly-users  tc-proxy-acl-ro
 * as tools/ci/integration.sh does; otherwise it SKIPs. Every value it
 * changes is put back at the end, and PROXY CONFIG SAVE is only run with a
 * file name: SAVE without one would rewrite the config file the proxy under
 * test was started with. */

#define ADMIN_LIST "proxy-config-admin-users"
#define BOTH_LISTS "proxy-config-admin-users or proxy-config-readonly-users"

static void sleep_ms(int ms) {
    struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* Sums "<field>=<n>" over the PROXY POOLS lines of one pool type ("blocking"
 * ...), whose node index is node, or every node when node < 0. -1 on error. */
static long long pools_sum(int fd, const char *type, int node, const char *field) {
    const int cap = 1 << 18;
    char *buf = malloc((size_t)cap);
    if (!buf) return -1;
    int n = resp_send_raw_reply(fd, buf, cap, 2, "PROXY", "POOLS");
    if (n <= 0 || buf[0] != '$') {
        free(buf);
        return -1;
    }
    char want[64], key[32];
    snprintf(want, sizeof(want), "_%s:", type);
    snprintf(key, sizeof(key), ",%s=", field);
    long long sum = 0;
    for (char *line = strstr(buf, "\npool_db"); line; line = strstr(line + 1, "\npool_db")) {
        char *eol = strstr(line + 1, "\r\n");
        if (!eol) break;
        *eol = '\0';
        int ni = -1;
        sscanf(line + 1, "pool_db%*d_n%d_", &ni);
        char *f = strstr(line, key);
        if (strstr(line, want) && f && (node < 0 || ni == node)) sum += atoll(f + strlen(key));
        *eol = '\r';
    }
    free(buf);
    return sum;
}

/* Polls until pools_sum(...) satisfies cmp against want, for up to ~3s: each
 * worker resizes its pools only once it wakes up. */
static long long pools_wait(int fd, const char *type, int node, const char *field, char cmp, long long want) {
    long long v = -1;
    for (int i = 0; i < 60; i++) {
        v = pools_sum(fd, type, node, field);
        if (v >= 0 && ((cmp == '=' && v == want) || (cmp == '<' && v <= want) || (cmp == '>' && v >= want))) return v;
        sleep_ms(50);
    }
    return v;
}

/* The node index whose blocking pools' in_use went up since before[], i.e.
 * the node the parked BLPOPs (all on one hash tag) went to; -1 if none. */
static int blocking_node_of(int fd, int nnodes, const long long *before) {
    for (int ni = 0; ni < nnodes; ni++)
        if (pools_sum(fd, "blocking", ni, "in_use") > before[ni]) return ni;
    return -1;
}

static int read_file(const char *path, char *buf, int cap) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    int n = (int)fread(buf, 1, (size_t)cap - 1, f);
    fclose(f);
    buf[n] = '\0';
    return n;
}

/* Whether text has a non-comment line "<name><spaces><value>". */
static int file_has_setting(const char *text, const char *name, int value) {
    size_t nl = strlen(name);
    for (const char *l = text; l && *l; l = strchr(l, '\n') ? strchr(l, '\n') + 1 : NULL) {
        while (*l == ' ' || *l == '\t') l++;
        if (strncmp(l, name, nl) != 0 || (l[nl] != ' ' && l[nl] != '\t')) continue;
        if (atoi(l + nl) == value) return 1;
    }
    return 0;
}

void run_proxy_config_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== PROXY CONFIG Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }
    char r[512];
    const int cap = 1 << 16;
    char *big = malloc((size_t)cap);
    if (!big) {
        close(fd);
        (*fail)++;
        return;
    }

    /* ── the default identity is refused, whatever the subcommand ─────── */
    static const char *subs[4][2] = {{"GET", "proxy|config|get"},
                                     {"SET", "proxy|config|set"},
                                     {"SAVE", "proxy|config|save"},
                                     {"HELP", "proxy|config|help"}};
    for (int i = 0; i < 4; i++) {
        char label[96], want[32];
        snprintf(want, sizeof(want), "'%s'", subs[i][1]);
        if (i == 0)
            proxy_cmd(fd, r, sizeof(r), 4, "PROXY", "CONFIG", "GET", "*");
        else if (i == 1)
            proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "1");
        else
            proxy_cmd(fd, r, sizeof(r), 3, "PROXY", "CONFIG", subs[i][0]);
        snprintf(label, sizeof(label), "PROXY CONFIG %s as the default user: -NOPERM", subs[i][0]);
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "default user") && strstr(r, want), label, pass, fail);
    }
    proxy_cmd(fd, r, sizeof(r), 3, "PROXY", "CONFIG", "NOSUCH");
    CHECK(strstr(r, "unknown PROXY CONFIG subcommand") != NULL, "PROXY CONFIG NOSUCH: unknown subcommand error", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 2, "PROXY", "CONFIG");
    CHECK(strstr(r, "wrong number of arguments for 'proxy|config'") != NULL, "PROXY CONFIG: arity error", pass, fail);
    int n = resp_send_raw_reply(fd, big, cap, 2, "PROXY", "HELP");
    CHECK(n > 0 && strstr(big, "CONFIG GET") && strstr(big, "CONFIG SET") && strstr(big, "CONFIG SAVE") &&
              strstr(big, "CONFIG HELP"),
          "PROXY HELP: lists the CONFIG subcommands (not restricted)", pass, fail);

    if (!g_test_acl_user) {
        SKIP("PROXY CONFIG as an authenticated ACL user");
        free(big);
        close(fd);
        return;
    }
    proxy_cmd(fd, r, sizeof(r), 3, "AUTH", g_test_acl_user, g_test_acl_pass);
    CHECK(strcmp(r, "+OK") == 0, "AUTH as the admin test user", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 3, "PROXY", "CONFIG", "HELP");
    if (strstr(r, "(not in ")) {
        printf("  [SKIP] PROXY CONFIG as an allowed user (start the proxy with \"proxy-config-admin-users %s\" and "
               "\"proxy-config-readonly-users %s\")\n",
               g_test_acl_user, g_test_acl_ro_user);
        free(big);
        close(fd);
        return;
    }
    if (r[0] == '*') { /* drain HELP's lines */
        int lines = atoi(r + 1);
        for (int i = 0; i < lines; i++) resp_skip_reply(fd);
    }

    /* ── who may run what: decided by proxy.conf's user lists, not by the
     * users' ACL rules on the backend ─────────────────────────────────── */
    n = resp_send_raw_reply(fd, big, cap, 4, "PROXY", "CONFIG", "GET", "proxy-config-*-users");
    CHECK(n > 0 && strstr(big, g_test_acl_user) && strstr(big, g_test_acl_ro_user),
          "GET proxy-config-*-users: shows both lists", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "proxy-config-admin-users", "someone");
    CHECK(strstr(r, "can't set immutable config") != NULL, "SET proxy-config-admin-users: fixed at startup", pass,
          fail);
    int cur_inflight = proxy_config_get_int(fd, "max-pipeline-inflight");
    char cur[16];
    snprintf(cur, sizeof(cur), "%d", cur_inflight);

    /* Read-only user: its backend rules are +@read only, like a monitoring
     * account, and it's in proxy-config-readonly-users. */
    int fro = proxy_connect(host, port);
    proxy_cmd(fro, r, sizeof(r), 3, "AUTH", g_test_acl_ro_user, g_test_acl_ro_pass);
    CHECK(proxy_config_get_int(fro, "max-pipeline-inflight") == cur_inflight, "read-only user: GET allowed", pass,
          fail);
    n = resp_send_raw_reply(fro, big, cap, 3, "PROXY", "CONFIG", "HELP");
    CHECK(n > 0 && big[0] == '*', "read-only user: HELP allowed", pass, fail);
    proxy_cmd(fro, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", cur);
    CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|config|set'") && strstr(r, "(not in " ADMIN_LIST ")"),
          "read-only user: SET is -NOPERM", pass, fail);
    proxy_cmd(fro, r, sizeof(r), 4, "PROXY", "CONFIG", "SAVE", "/tmp/tc-proxy-config-ro.conf");
    CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|config|save'"), "read-only user: SAVE is -NOPERM", pass,
          fail);
    CHECK(access("/tmp/tc-proxy-config-ro.conf", F_OK) != 0, "read-only user: refused SAVE wrote nothing", pass, fail);
    close(fro);

    /* Users in neither list are refused everything, whatever their ACL
     * rules: +@all -@admin, and one allowed CLIENT LIST. */
    const char *unlisted[2][3] = {{g_test_acl_noadmin_user, g_test_acl_noadmin_pass, "+@all -@admin"},
                                  {g_test_acl_cl_user, g_test_acl_cl_pass, "+client|list"}};
    for (int u = 0; u < 2; u++) {
        char label[128];
        int fu = proxy_connect(host, port);
        proxy_cmd(fu, r, sizeof(r), 3, "AUTH", unlisted[u][0], unlisted[u][1]);
        proxy_cmd(fu, r, sizeof(r), 4, "PROXY", "CONFIG", "GET", "*");
        snprintf(label, sizeof(label), "unlisted user (%s): GET is -NOPERM", unlisted[u][2]);
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|config|get'") && strstr(r, "(not in " BOTH_LISTS ")"),
              label, pass, fail);
        proxy_cmd(fu, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", cur);
        snprintf(label, sizeof(label), "unlisted user (%s): SET is -NOPERM", unlisted[u][2]);
        CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "'proxy|config|set'"), label, pass, fail);
        close(fu);
    }
    /* AUTH default goes back to the default identity: refused again. */
    int fdef = proxy_connect(host, port);
    proxy_cmd(fdef, r, sizeof(r), 3, "AUTH", g_test_acl_user, g_test_acl_pass);
    proxy_cmd(fdef, r, sizeof(r), 3, "AUTH", "default", "x");
    proxy_cmd(fdef, r, sizeof(r), 3, "PROXY", "CONFIG", "HELP");
    CHECK(strncmp(r, "-NOPERM", 7) == 0 && strstr(r, "default user"),
          "PROXY CONFIG HELP after AUTH default: -NOPERM again", pass, fail);
    close(fdef);

    /* ── HELP ─────────────────────────────────────────────────────────── */
    n = resp_send_raw_reply(fd, big, cap, 3, "PROXY", "CONFIG", "HELP");
    CHECK(n > 0 && big[0] == '*' && strstr(big, "GET <pattern>") && strstr(big, "SET <parameter> <value>") &&
              strstr(big, "SAVE [<file>]"),
          "PROXY CONFIG HELP: GET, SET and SAVE syntax", pass, fail);
    CHECK(n > 0 && strstr(big, "max-pipeline-inflight") && strstr(big, "min-normal-connection") &&
              strstr(big, "max-acl-connection") && strstr(big, "cluster-refresh-interval-ms"),
          "PROXY CONFIG HELP: lists the settable parameters", pass, fail);
    CHECK(n > 0 && !strstr(big, "worker-thread-no") && !strstr(big, "shared-connection-per-node"),
          "PROXY CONFIG HELP: doesn't list startup-only parameters", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "PROXY", "CONFIG", "HELP", "extra");
    CHECK(strstr(r, "wrong number of arguments for 'proxy|config|help'") != NULL,
          "PROXY CONFIG HELP extra: arity error", pass, fail);

    /* ── GET ──────────────────────────────────────────────────────────── */
    n = resp_send_raw_reply(fd, big, cap, 4, "PROXY", "CONFIG", "GET", "max-pipeline-inflight");
    CHECK(n > 0 && strncmp(big, "*2\r\n$21\r\nmax-pipeline-inflight\r\n$", 33) == 0,
          "GET max-pipeline-inflight: one name/value pair", pass, fail);
    int orig_inflight = proxy_config_get_int(fd, "max-pipeline-inflight");
    CHECK(orig_inflight >= 1, "GET max-pipeline-inflight: a positive integer", pass, fail);
    n = resp_send_raw_reply(fd, big, cap, 4, "PROXY", "CONFIG", "GET", "*");
    CHECK(n > 0 && strstr(big, "\r\nserver-port\r\n") && strstr(big, "\r\nworker-thread-no\r\n") &&
              strstr(big, "\r\nmax-acl-connection\r\n") && strstr(big, "\r\nreported-modules\r\n"),
          "GET *: every setting, startup-only ones too", pass, fail);
    CHECK(n > 0 && !strstr(big, "tls-key"), "GET *: never shows tls-key", pass, fail);
    n = resp_send_raw_reply(fd, big, cap, 4, "PROXY", "CONFIG", "GET", "MAX-*-CONNECTION");
    /* The five pools' max-*-connection; max-connection has no second '-'. */
    CHECK(n > 0 && strncmp(big, "*10\r\n", 5) == 0 && strstr(big, "\r\nmax-normal-connection\r\n") &&
              strstr(big, "\r\nmax-acl-connection\r\n") && !strstr(big, "\r\nmax-connection\r\n"),
          "GET MAX-*-CONNECTION: case-insensitive glob, lowercase names", pass, fail);
    n = resp_send_raw_reply(fd, big, cap, 5, "PROXY", "CONFIG", "GET", "port", "p?rt");
    CHECK(n > 0 && strncmp(big, "*2\r\n$4\r\nport\r\n$", 15) == 0, "GET port p?rt: a name matched twice comes once",
          pass, fail);
    n = resp_send_raw_reply(fd, big, cap, 4, "PROXY", "CONFIG", "GET", "no-such-*");
    CHECK(n > 0 && strcmp(big, "*0\r\n") == 0, "GET no-such-*: empty array", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 3, "PROXY", "CONFIG", "GET");
    CHECK(strstr(r, "wrong number of arguments for 'proxy|config|get'") != NULL, "GET without a pattern: arity error",
          pass, fail);

    /* ── SET: errors, and nothing changes ─────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "port", "1");
    CHECK(strstr(r, "'port'") && strstr(r, "can't set immutable config"), "SET port: immutable", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "no-such-param", "1");
    CHECK(strstr(r, "Unknown option") && strstr(r, "'no-such-param'"), "SET no-such-param: unknown option", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "abc");
    CHECK(strstr(r, "argument must be an integer") != NULL, "SET max-pipeline-inflight abc: not an integer", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "99999999999");
    CHECK(strstr(r, "argument must be an integer") != NULL, "SET max-pipeline-inflight past INT_MAX: refused", pass,
          fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "0");
    CHECK(strstr(r, "must be 1 or greater") != NULL, "SET max-pipeline-inflight 0: below the minimum", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "transaction-replay-buffer-size", "-1");
    CHECK(strstr(r, "must be 0 or greater") != NULL, "SET transaction-replay-buffer-size -1: below the minimum", pass,
          fail);
    int maxconn = proxy_config_get_int(fd, "max-connection");
    char toobig[32];
    snprintf(toobig, sizeof(toobig), "%d", maxconn + 1);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-normal-connection", toobig);
    CHECK(strstr(r, "can't exceed max-connection") != NULL, "SET max-normal-connection past max-connection: refused",
          pass, fail);
    int orig_bmin = proxy_config_get_int(fd, "min-blocking-connection");
    int orig_bmax = proxy_config_get_int(fd, "max-blocking-connection");
    char above[32];
    snprintf(above, sizeof(above), "%d", orig_bmax + 1);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "min-blocking-connection", above);
    CHECK(strstr(r, "can't exceed max-blocking-connection") != NULL, "SET min above max: refused", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 7, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "5", "max-pipeline-inflight",
              "6");
    CHECK(strstr(r, "duplicate parameter") != NULL, "SET the same parameter twice: refused", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "PROXY", "CONFIG", "SET", "max-pipeline-inflight");
    CHECK(strstr(r, "wrong number of arguments for 'proxy|config|set'") != NULL, "SET without a value: arity error",
          pass, fail);
    proxy_cmd(fd, r, sizeof(r), 7, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", "77", "port", "1");
    CHECK(strstr(r, "can't set immutable config") != NULL, "SET a good pair and a bad one: refused", pass, fail);
    CHECK(proxy_config_get_int(fd, "max-pipeline-inflight") == orig_inflight,
          "SET a good pair and a bad one: the good one isn't applied either", pass, fail);

    /* ── SET: plain values ────────────────────────────────────────────── */
    char val[32];
    snprintf(val, sizeof(val), "%d", orig_inflight + 1);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", val);
    CHECK(strcmp(r, "+OK") == 0, "SET max-pipeline-inflight: +OK", pass, fail);
    CHECK(proxy_config_get_int(fd, "max-pipeline-inflight") == orig_inflight + 1,
          "SET max-pipeline-inflight: GET shows the new value", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:pcfg:k", "v");
    proxy_cmd(fd, r, sizeof(r), 2, "GET", "tc:pcfg:k");
    CHECK(strcmp(r, "v") == 0, "SET max-pipeline-inflight: commands still served", pass, fail);
    int orig_refresh = proxy_config_get_int(fd, "cluster-refresh-interval-ms");
    snprintf(val, sizeof(val), "%d", orig_refresh > 0 ? orig_refresh + 100 : 0);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "cluster-refresh-interval-ms", val);
    CHECK(strcmp(r, "+OK") == 0 && proxy_config_get_int(fd, "cluster-refresh-interval-ms") == atoi(val),
          "SET cluster-refresh-interval-ms: applied", pass, fail);

    /* ── SET: pool sizes. Parks more BLPOPs on one node than every worker's
     * blocking pool can hold, then raises max-blocking-connection: the
     * parked ones must get connections, and every BLPOP its own value. ── */
    int workers = 0;
    n = resp_send_raw_reply(fd, big, cap, 2, "PROXY", "POOLS");
    const char *wl = n > 0 ? strstr(big, "workers:") : NULL;
    if (wl) workers = atoi(wl + 8);
    proxy_cmd(fd, r, sizeof(r), 7, "PROXY", "CONFIG", "SET", "min-blocking-connection", "0", "max-blocking-connection",
              "1");
    CHECK(strcmp(r, "+OK") == 0, "SET min/max-blocking-connection 0/1 together: +OK", pass, fail);
    /* Idle connections past the new max close right away. */
    long long bmax = pools_wait(fd, "blocking", -1, "max", '<', workers * 64);
    long long open_total = pools_wait(fd, "blocking", -1, "open", '<', bmax);
    CHECK(workers > 0 && open_total >= 0 && open_total <= bmax,
          "lower max-blocking-connection: idle connections past it are closed", pass, fail);

    int nclients = workers + 1, nnodes = 64;
    long long before[64];
    for (int ni = 0; ni < nnodes; ni++) before[ni] = pools_sum(fd, "blocking", ni, "in_use");
    int cl[17];
    if (nclients > 17) nclients = 17;
    char keys[17][32];
    for (int i = 0; i < nclients; i++) {
        snprintf(keys[i], sizeof(keys[i]), "{tc:pcfg}:b%d", i);
        proxy_cmd(fd, r, sizeof(r), 2, "DEL", keys[i]);
        cl[i] = proxy_connect(host, port);
        resp_send(cl[i], 3, "BLPOP", keys[i], "20");
    }
    /* With one connection per worker and more clients than workers, at
     * least one worker has a client waiting. */
    long long waiting = pools_wait(fd, "blocking", -1, "waiting", '>', 1);
    int node = blocking_node_of(fd, nnodes, before);
    CHECK(waiting >= 1 && node >= 0, "max-blocking-connection 1: extra BLPOPs wait for a connection", pass, fail);
    snprintf(val, sizeof(val), "%d", nclients);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-blocking-connection", val);
    CHECK(strcmp(r, "+OK") == 0, "raise max-blocking-connection: +OK", pass, fail);
    waiting = pools_wait(fd, "blocking", -1, "waiting", '=', 0);
    long long in_use = pools_wait(fd, "blocking", node, "in_use", '>', before[node] + nclients);
    CHECK(waiting == 0 && in_use >= before[node] + nclients,
          "raise max-blocking-connection: waiting BLPOPs get connections at once", pass, fail);
    int all_ok = 1;
    for (int i = 0; i < nclients; i++) {
        char v[32];
        snprintf(v, sizeof(v), "v%d", i);
        proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", keys[i], v);
    }
    for (int i = 0; i < nclients; i++) {
        char hdr[32], k[64], v[64], want[32];
        snprintf(want, sizeof(want), "v%d", i);
        resp_next_str(cl[i], hdr, sizeof(hdr));
        resp_next_str(cl[i], k, sizeof(k));
        resp_next_str(cl[i], v, sizeof(v));
        if (strcmp(hdr, "*2") || strcmp(k, keys[i]) || strcmp(v, want)) all_ok = 0;
        close(cl[i]);
    }
    CHECK(all_ok, "raise max-blocking-connection: every BLPOP gets its own value", pass, fail);

    /* Busy connections past a lowered max close as they're released. */
    for (int i = 0; i < 2; i++) {
        cl[i] = proxy_connect(host, port);
        resp_send(cl[i], 3, "BLPOP", keys[i], "20");
    }
    pools_wait(fd, "blocking", node, "in_use", '>', 2);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-blocking-connection", "1");
    CHECK(strcmp(r, "+OK") == 0, "lower max-blocking-connection while busy: +OK", pass, fail);
    long long node_open_busy = pools_wait(fd, "blocking", node, "open", '>', 2);
    for (int i = 0; i < 2; i++) {
        proxy_cmd(fd, r, sizeof(r), 3, "RPUSH", keys[i], "x");
        resp_skip_reply(cl[i]);
        close(cl[i]);
    }
    long long node_max = pools_sum(fd, "blocking", node, "max");
    long long node_open = pools_wait(fd, "blocking", node, "open", '<', node_max);
    CHECK(node_open_busy >= 2 && node_open <= node_max,
          "lower max-blocking-connection while busy: closed once released", pass, fail);

    /* Raising min opens connections up to it, on every node. Both values
     * differ from the defaults (10/2) so SAVE below writes them even when
     * the startup file doesn't mention them. */
    proxy_cmd(fd, r, sizeof(r), 7, "PROXY", "CONFIG", "SET", "max-blocking-connection", "4", "min-blocking-connection",
              "3");
    CHECK(strcmp(r, "+OK") == 0, "SET max/min-blocking-connection 4/3: +OK", pass, fail);
    long long nlive = 0;
    for (int ni = 0; ni < nnodes; ni++)
        if (pools_sum(fd, "blocking", ni, "max") > 0) nlive++;
    long long opened = pools_wait(fd, "blocking", -1, "open", '>', 3 * nlive);
    CHECK(nlive > 0 && opened >= 3 * nlive, "raise min-blocking-connection: connections opened up to it", pass, fail);
    for (int i = 0; i < nclients; i++) proxy_cmd(fd, r, sizeof(r), 2, "DEL", keys[i]);

    /* ── SAVE ─────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SAVE", "a", "b");
    CHECK(strstr(r, "wrong number of arguments for 'proxy|config|save'") != NULL, "SAVE with two files: arity error",
          pass, fail);
    proxy_cmd(fd, r, sizeof(r), 4, "PROXY", "CONFIG", "SAVE", "/nonexistent-dir-tc-pcfg/x.conf");
    CHECK(strstr(r, "can't write /nonexistent-dir-tc-pcfg/x.conf") != NULL, "SAVE to a missing directory: refused",
          pass, fail);
    char path[64];
    snprintf(path, sizeof(path), "/tmp/tc-proxy-config-%d.conf", (int)getpid());
    snprintf(val, sizeof(val), "%d", orig_inflight + 7);
    proxy_cmd(fd, r, sizeof(r), 5, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", val);
    proxy_cmd(fd, r, sizeof(r), 4, "PROXY", "CONFIG", "SAVE", path);
    CHECK(strcmp(r, "+OK") == 0, "SAVE <file>: +OK", pass, fail);
    /* The file is on the proxy's host: only checkable from the same one. */
    int flen = read_file(path, big, cap);
    if (flen >= 0) {
        CHECK(file_has_setting(big, "max-pipeline-inflight", orig_inflight + 7), "SAVE <file>: has the value SET gave",
              pass, fail);
        CHECK(file_has_setting(big, "max-blocking-connection", 4) &&
                  file_has_setting(big, "min-blocking-connection", 3),
              "SAVE <file>: has the pool sizes SET gave", pass, fail);
        CHECK(strstr(big, "server-port") != NULL, "SAVE <file>: a whole config, startup-only settings kept", pass,
              fail);
        unlink(path);
    } else {
        printf("  [SKIP] SAVE <file> contents (%s isn't readable here: proxy on another host?)\n", path);
    }

    /* ── put everything back ──────────────────────────────────────────── */
    char s1[16], s2[16], s3[16], s4[16];
    snprintf(s1, sizeof(s1), "%d", orig_inflight);
    snprintf(s2, sizeof(s2), "%d", orig_refresh);
    snprintf(s3, sizeof(s3), "%d", orig_bmin);
    snprintf(s4, sizeof(s4), "%d", orig_bmax);
    proxy_cmd(fd, r, sizeof(r), 11, "PROXY", "CONFIG", "SET", "max-pipeline-inflight", s1,
              "cluster-refresh-interval-ms", s2, "max-blocking-connection", s4, "min-blocking-connection", s3);
    CHECK(strcmp(r, "+OK") == 0 && proxy_config_get_int(fd, "max-pipeline-inflight") == orig_inflight &&
              proxy_config_get_int(fd, "max-blocking-connection") == orig_bmax &&
              proxy_config_get_int(fd, "min-blocking-connection") == orig_bmin,
          "SET: original values restored", pass, fail);
    proxy_cmd(fd, r, sizeof(r), 2, "DEL", "tc:pcfg:k");

    free(big);
    close(fd);
}
