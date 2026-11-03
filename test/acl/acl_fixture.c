#include "../test_common.h"

/*
 * Temporary ACL users for the AUTH / HELLO ... AUTH tests.
 *
 * ACLs are per node, and the proxy only ever forwards a handful of
 * read-only ACL subcommands (never SETUSER/DELUSER), so run-test's
 * --cluster=<host:port> option talks to the cluster directly: it reads
 * CLUSTER NODES from that seed, creates both users on every reachable
 * node (replicas too, so a failover mid-run doesn't lose them), and
 * deletes them again once all suites have run. Without --cluster the
 * globals below stay NULL and the tests that need a real user SKIP.
 *
 *   g_test_acl_user    -- full access (~* &* +@all)
 *   g_test_acl_ro_user -- read-only (~* +@read +@connection +acl|whoami): lets a test
 *                         prove that an authenticated client's business
 *                         traffic really runs as that user (a SET must come
 *                         back -NOPERM) instead of on the shared,
 *                         default-identity pools.
 */

const char *g_test_acl_user = NULL;
const char *g_test_acl_pass = NULL;
const char *g_test_acl_ro_user = NULL;
const char *g_test_acl_ro_pass = NULL;

#define FIXTURE_USER    "tc-proxy-acl"
#define FIXTURE_RO_USER "tc-proxy-acl-ro"

static char fixture_pass[64], fixture_ro_pass[64];
static char node_host[64][256];
static int node_port[64];
static int node_count = 0;

/* Runs one command directly against a cluster node; returns 1 if the reply
 * is +OK or an integer (ACL DELUSER), 0 otherwise. */
static int node_cmd_ok(const char *host, int port, int argc, const char **args) {
    int fd = proxy_connect(host, port);
    if (fd < 0) return 0;
    int arglens[16];
    for (int i = 0; i < argc; i++) arglens[i] = (int)strlen(args[i]);
    char r[256];
    int ok = resp_send_raw(fd, argc, args, arglens) == 0 && resp_next_str(fd, r, sizeof(r)) != NULL &&
             (strcmp(r, "+OK") == 0 || r[0] == ':');
    close(fd);
    return ok;
}

static void create_on_node(const char *host, int port, int *ok) {
    char full_pw[80], ro_pw[80];
    snprintf(full_pw, sizeof(full_pw), ">%s", fixture_pass);
    snprintf(ro_pw, sizeof(ro_pw), ">%s", fixture_ro_pass);
    const char *full[] = {"ACL", "SETUSER", FIXTURE_USER, "reset", "on", full_pw, "~*", "&*", "+@all"};
    const char *ro[] = {"ACL", "SETUSER", FIXTURE_RO_USER, "reset",        "on",
                        ro_pw, "~*",      "+@read",        "+@connection", "+acl|whoami"};
    if (!node_cmd_ok(host, port, 9, full) || !node_cmd_ok(host, port, 10, ro)) *ok = 0;
}

void test_acl_fixture_teardown(void) {
    for (int i = 0; i < node_count; i++) {
        const char *del[] = {"ACL", "DELUSER", FIXTURE_USER, FIXTURE_RO_USER};
        if (!node_cmd_ok(node_host[i], node_port[i], 4, del))
            fprintf(stderr, "warning: could not delete test ACL users on %s:%d\n", node_host[i], node_port[i]);
    }
    node_count = 0;
    g_test_acl_user = g_test_acl_pass = NULL;
    g_test_acl_ro_user = g_test_acl_ro_pass = NULL;
}

/* seed is "host:port". Returns 0 on success (globals set), -1 otherwise
 * (anything already created is removed again, globals stay NULL). */
int test_acl_fixture_setup(const char *seed) {
    char host[64];
    int port;
    if (sscanf(seed, "%63[^:]:%d", host, &port) != 2) {
        fprintf(stderr, "--cluster: expected <host:port>, got '%s'\n", seed);
        return -1;
    }

    int fd = proxy_connect(host, port);
    if (fd < 0) return -1;
    static char nodes[65536];
    int sent = resp_send(fd, 2, "CLUSTER", "NODES");
    char *got = sent == 0 ? resp_next_str(fd, nodes, sizeof(nodes)) : NULL;
    close(fd);
    if (!got || nodes[0] == '-' || nodes[0] == '(') {
        fprintf(stderr, "--cluster: CLUSTER NODES against %s failed: %s\n", seed, nodes);
        return -1;
    }

    /* <id> <ip:port@cport[,hostname]> <flags> ... -- one node per line. */
    for (char *line = strtok(nodes, "\n"); line && node_count < 64; line = strtok(NULL, "\n")) {
        char id[64], addr[256], flags[256];
        if (sscanf(line, "%63s %255s %255s", id, addr, flags) != 3) continue;
        if (strstr(flags, "fail") || strstr(flags, "noaddr") || strstr(flags, "handshake")) continue;
        char *at = strchr(addr, '@');
        if (at) *at = '\0';
        char *colon = strrchr(addr, ':');
        if (!colon) continue;
        *colon = '\0';
        snprintf(node_host[node_count], sizeof(node_host[0]), "%s", addr);
        node_port[node_count] = atoi(colon + 1);
        node_count++;
    }
    if (node_count == 0) {
        fprintf(stderr, "--cluster: no usable nodes found\n");
        return -1;
    }

    snprintf(fixture_pass, sizeof(fixture_pass), "tc-pw-%d", (int)getpid());
    snprintf(fixture_ro_pass, sizeof(fixture_ro_pass), "tc-ro-pw-%d", (int)getpid());
    int ok = 1;
    for (int i = 0; i < node_count; i++) create_on_node(node_host[i], node_port[i], &ok);
    if (!ok) {
        fprintf(stderr, "--cluster: could not create test ACL users on every node\n");
        test_acl_fixture_teardown();
        return -1;
    }

    g_test_acl_user = FIXTURE_USER;
    g_test_acl_pass = fixture_pass;
    g_test_acl_ro_user = FIXTURE_RO_USER;
    g_test_acl_ro_pass = fixture_ro_pass;
    printf("Created test ACL users %s / %s on %d cluster node(s)\n", FIXTURE_USER, FIXTURE_RO_USER, node_count);
    return 0;
}
