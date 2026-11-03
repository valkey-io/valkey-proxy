#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void run_bitmap_tests(const char *host, int port, int *pass, int *fail);
void run_blocking_tests(const char *host, int port, int *pass, int *fail);
void run_general_tests(const char *host, int port, int *pass, int *fail);
void run_script_tests(const char *host, int port, int *pass, int *fail);
void run_function_tests(const char *host, int port, int *pass, int *fail);
void run_string_tests(const char *host, int port, int *pass, int *fail);
void run_geospatial_tests(const char *host, int port, int *pass, int *fail);
void run_hash_tests(const char *host, int port, int *pass, int *fail);
void run_hyperloglog_tests(const char *host, int port, int *pass, int *fail);
void run_list_tests(const char *host, int port, int *pass, int *fail);
void run_set_tests(const char *host, int port, int *pass, int *fail);
void run_stream_tests(const char *host, int port, int *pass, int *fail);
void run_zset_tests(const char *host, int port, int *pass, int *fail);
void run_bloomfilter_tests(const char *host, int port, int *pass, int *fail);
void run_json_tests(const char *host, int port, int *pass, int *fail);
void run_pubsub_tests(const char *host, int port, int *pass, int *fail);
void run_acl_tests(const char *host, int port, int *pass, int *fail);
void run_resp3_tests(const char *host, int port, int *pass, int *fail);
void run_search_tests(const char *host, int port, int *pass, int *fail);
void run_reported_tests(const char *host, int port, int *pass, int *fail);
int test_acl_fixture_setup(const char *seed);
void test_acl_fixture_teardown(void);
extern const char *g_test_acl_user;

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--bloomfilter] [--json] [--search] [--string] [--pubsub] [-h=<host>] [-p=<port>] "
            "[--cluster=<host:port>] [--reported=<host:port>]\n",
            prog);
    fprintf(stderr, "  -h=<host>      proxy IP address (default: 127.0.0.1)\n");
    fprintf(stderr, "  -p=<port>      proxy port      (default: 7777)\n");
    fprintf(stderr, "  --cluster=<host:port>  a backend cluster node: creates temporary ACL users\n"
                    "                 on every node for the AUTH / HELLO AUTH tests, deletes\n"
                    "                 them afterwards (without it those tests SKIP)\n");
    fprintf(stderr, "  --reported=<host:port>  a second proxy whose config sets reported_version 8.0.2,\n"
                    "                 reported_role replica and reported_modules search:10002,json:\n"
                    "                 checks HELLO and INFO report the overrides (without it they SKIP)\n");
    fprintf(stderr, "  --bloomfilter  run Bloom Filter module tests only\n");
    fprintf(stderr, "  --json         run JSON module tests only\n");
    fprintf(stderr, "  --search       run Search (FT.*) module tests only\n");
    fprintf(stderr, "  --string       run String tests only\n");
    fprintf(stderr, "  --pubsub       run Pub/Sub tests only\n");
    fprintf(stderr, "  (any of the suite options above runs only the selected suites, not the defaults)\n");
}

int main(int argc, char *argv[]) {
    char host[64] = "127.0.0.1";
    int port = 7777;
    int run_bloomfilter = 0;
    int run_json = 0;
    int run_search = 0;
    int run_string = 0;
    int run_pubsub = 0;
    int run_defaults = 1;
    char cluster_seed[128] = "";
    char reported_host[64] = "";
    int reported_port = 0;

    for (int i = 1; i < argc; i++) {
        if (sscanf(argv[i], "-h=%63s", host) == 1) continue;
        if (sscanf(argv[i], "-p=%d", &port) == 1) continue;
        if (sscanf(argv[i], "--cluster=%127s", cluster_seed) == 1) continue;
        if (sscanf(argv[i], "--reported=%63[^:]:%d", reported_host, &reported_port) == 2) continue;
        if (strcmp(argv[i], "--bloomfilter") == 0) {
            run_bloomfilter = 1;
            run_defaults = 0;
            continue;
        }
        if (strcmp(argv[i], "--json") == 0) {
            run_json = 1;
            run_defaults = 0;
            continue;
        }
        if (strcmp(argv[i], "--search") == 0) {
            run_search = 1;
            run_defaults = 0;
            continue;
        }
        if (strcmp(argv[i], "--string") == 0) {
            run_string = 1;
            run_defaults = 0;
            continue;
        }
        if (strcmp(argv[i], "--pubsub") == 0) {
            run_pubsub = 1;
            run_defaults = 0;
            continue;
        }
        fprintf(stderr, "Unknown argument: %s\n", argv[i]);
        usage(argv[0]);
        return 1;
    }

    printf("Running integration tests against proxy %s:%d\n", host, port);

    if (cluster_seed[0] && test_acl_fixture_setup(cluster_seed) < 0)
        fprintf(stderr, "warning: ACL fixture unavailable, AUTH-as-real-user tests will SKIP\n");

    int pass = 0, fail = 0;
    int p0, f0;

#define RUN_SUITE(name, fn)                                                  \
    do {                                                                     \
        p0 = pass;                                                           \
        f0 = fail;                                                           \
        fn(host, port, &pass, &fail);                                        \
        printf("  Summary: Passed: %d, Failed: %d\n", pass - p0, fail - f0); \
    } while (0)

    if (run_defaults) {
        RUN_SUITE("bitmap", run_bitmap_tests);
        RUN_SUITE("hash", run_hash_tests);
        RUN_SUITE("string", run_string_tests);
        RUN_SUITE("general", run_general_tests);
        RUN_SUITE("script", run_script_tests);
        RUN_SUITE("function", run_function_tests);
        RUN_SUITE("geospatial", run_geospatial_tests);
        RUN_SUITE("hyperloglog", run_hyperloglog_tests);
        RUN_SUITE("list", run_list_tests);
        RUN_SUITE("set", run_set_tests);
        RUN_SUITE("stream", run_stream_tests);
        RUN_SUITE("zset", run_zset_tests);
        RUN_SUITE("blocking", run_blocking_tests);
        RUN_SUITE("pubsub", run_pubsub_tests);
        RUN_SUITE("acl", run_acl_tests);
        if (reported_port) {
            p0 = pass;
            f0 = fail;
            run_reported_tests(reported_host, reported_port, &pass, &fail);
            printf("  Summary: Passed: %d, Failed: %d\n", pass - p0, fail - f0);
        } else
            printf("\n=== Reported Identity Tests ===\n  [SKIP] reported_* overrides (needs --reported=<host:port>)\n");
        /* Must run last: this is the one suite that actually triggers the
         * real "HELLO 3" backend probe, which flips a process-wide sticky
         * flag (g_backend_resp3_active) for the rest of the proxy's life --
         * every suite above gets to run against a proxy that has never seen
         * RESP3 at all, exactly like before this feature existed. */
        RUN_SUITE("resp3", run_resp3_tests);
    }

    if (run_bloomfilter) {
        RUN_SUITE("bloomfilter", run_bloomfilter_tests);
    }

    if (run_json) {
        RUN_SUITE("json", run_json_tests);
    }

    if (run_search) {
        RUN_SUITE("search", run_search_tests);
    }

    if (run_string) {
        RUN_SUITE("string", run_string_tests);
    }

    if (run_pubsub) {
        RUN_SUITE("pubsub", run_pubsub_tests);
    }

    if (g_test_acl_user) test_acl_fixture_teardown();

    printf("\n========================================\n");
    printf("Total: Passed: %d, Failed: %d\n", pass, fail);
    printf("========================================\n");

    return (fail > 0) ? 1 : 0;
}
