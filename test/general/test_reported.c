#include "../test_common.h"

/* Runs only with --reported=<host:port>: a second proxy started with these
 * overrides in its config (see README.md):
 *   reported_version  8.0.2
 *   reported_role     replica
 *   reported_modules  search:10002,json
 * The general suite covers the defaults. */
void run_reported_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Reported Identity Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char buf[8192];
    int len = resp_send_raw_reply(fd, buf, sizeof(buf), 2, "HELLO", "2");
    CHECK(len > 0 && strstr(buf, "$7\r\nversion\r\n$5\r\n8.0.2\r\n"), "reported_version: HELLO version", pass, fail);
    CHECK(len > 0 && strstr(buf, "$4\r\nrole\r\n$7\r\nreplica\r\n"), "reported_role: HELLO role", pass, fail);
    CHECK(len > 0 &&
              strstr(buf, "$7\r\nmodules\r\n*2\r\n*8\r\n$4\r\nname\r\n$6\r\nsearch\r\n$3\r\nver\r\n:10002\r\n") &&
              strstr(buf, "$4\r\nname\r\n$4\r\njson\r\n$3\r\nver\r\n:0\r\n"),
          "reported_modules: HELLO lists exactly the configured modules", pass, fail);

    len = resp_send_raw_reply(fd, buf, sizeof(buf), 2, "INFO", "server");
    CHECK(len > 0 && (strstr(buf, "\nvalkey_version:8.0.2\r\n") || strstr(buf, "\nredis_version:8.0.2\r\n")),
          "reported_version: INFO server reports it too", pass, fail);

    len = resp_send_raw_reply(fd, buf, sizeof(buf), 2, "INFO", "replication");
    CHECK(len > 0 && strstr(buf, "# Replication\r\nrole:replica\r\n"), "reported_role: INFO replication reports it too",
          pass, fail);

    len = resp_send_raw_reply(fd, buf, sizeof(buf), 2, "HELLO", "3");
    CHECK(len > 0 && strstr(buf, "$7\r\nmodules\r\n*2\r\n%4\r\n$4\r\nname\r\n$6\r\nsearch\r\n"),
          "reported_modules: RESP3 HELLO sends each module as a map", pass, fail);

    close(fd);
}
