#include "proxy.h"
#include <assert.h>
#include <sys/wait.h>

Config g_config;
int g_pool_min[POOL_TYPE_COUNT], g_pool_max[POOL_TYPE_COUNT];

static void check_config(const char *host) {
    char path[] = "/tmp/valkey-proxy-config-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    FILE *f = fdopen(fd, "w");
    assert(f);
    fprintf(f, "server-ip %s\nserver-port 6379\nbind 0.0.0.0\nport 7777\n", host);
    assert(fclose(f) == 0);
    if (strlen(host) >= sizeof(g_config.server_ip)) {
        pid_t pid = fork();
        assert(pid >= 0);
        if (pid == 0) {
            parse_conf(path);
            _exit(0);
        }
        int status;
        assert(waitpid(pid, &status, 0) == pid);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 1);
    } else {
        parse_conf(path);
        assert(strcmp(g_config.server_ip, host) == 0);
    }
    assert(unlink(path) == 0);
}

/* Writes body to a temp config and reports parse_conf()'s exit status
 * (0 if it returned). */
static int parse_status(const char *body) {
    char path[] = "/tmp/valkey-proxy-config-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    FILE *f = fdopen(fd, "w");
    assert(f);
    fputs(body, f);
    assert(fclose(f) == 0);
    fflush(NULL);
    pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        freopen("/dev/null", "w", stderr);
        parse_conf(path);
        _exit(0);
    }
    int status;
    assert(waitpid(pid, &status, 0) == pid);
    assert(unlink(path) == 0);
    assert(WIFEXITED(status));
    return WEXITSTATUS(status);
}

static void check_validation(void) {
    const char *base = "server-ip 127.0.0.1\nserver-port 6379\nport 7777\n";
    char buf[512];
    assert(parse_status(base) == 0);
    /* comments, blank lines and indented comments are fine */
    snprintf(buf, sizeof(buf), "# c\n\n   # indented\n\t\n%smax-multikey-payload 1048576\n", base);
    assert(parse_status(buf) == 0);
    /* every renamed option is accepted */
    snprintf(buf, sizeof(buf),
             "%smax-connection 100\ncluster-refresh-interval-ms 0\nbackend-multiplexing yes\n"
             "min-normal-connection 1\nmax-acl-connection 5\nreported-role master\n",
             base);
    assert(parse_status(buf) == 0);
    /* missing or invalid ports */
    assert(parse_status("server-ip 127.0.0.1\nport 7777\n") == 1);
    assert(parse_status("server-ip 127.0.0.1\nserver-port 6379\n") == 1);
    assert(parse_status("server-ip 127.0.0.1\nserver-port 0\nport 7777\n") == 1);
    assert(parse_status("server-ip 127.0.0.1\nserver-port 6379\nport 70000\n") == 1);
    /* unknown options, including the old underscore spellings */
    snprintf(buf, sizeof(buf), "%sno-such-option 1\n", base);
    assert(parse_status(buf) == 1);
    snprintf(buf, sizeof(buf), "%smax_connection 100\n", base);
    assert(parse_status(buf) == 1);
    /* an option with no value */
    snprintf(buf, sizeof(buf), "%smax-connection\n", base);
    assert(parse_status(buf) == 1);
}

int main(void) {
    check_config("127.0.0.1");
    check_config("valkey-cluster-0-0-0.valkey-cluster.session-storage-development.svc.cluster.local");
    char longest[254];
    memset(longest, 'a', sizeof(longest) - 1);
    longest[63] = longest[127] = longest[191] = '.';
    longest[253] = '\0';
    check_config(longest);

    char too_long[255];
    memset(too_long, 'a', sizeof(too_long) - 1);
    too_long[254] = '\0';
    check_config(too_long);

    check_validation();

    int listener = socket(AF_INET, SOCK_STREAM, 0);
    assert(listener >= 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    assert(bind(listener, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    assert(getsockname(listener, (struct sockaddr *)&addr, &len) == 0);
    int port = ntohs(addr.sin_port);
    assert(listen(listener, 8) == 0);

    const char *hosts[] = {"127.0.0.1", "localhost", "localhost"};
    for (size_t i = 0; i < sizeof(hosts) / sizeof(hosts[0]); i++) {
        int client = tcp_connect_timeout(hosts[i], port, 100);
        assert(client >= 0);
        assert(!(fcntl(client, F_GETFL, 0) & O_NONBLOCK));
        int accepted = accept(listener, NULL, NULL);
        assert(accepted >= 0);
        close(accepted);
        close(client);
    }
    /* An empty hostname fails resolution without an external DNS dependency. */
    assert(tcp_connect_timeout("", port, 100) == -1);
    close(listener);
    /* A bound, non-listening socket gives a deterministic refused connection. */
    int refused = socket(AF_INET, SOCK_STREAM, 0);
    assert(refused >= 0);
    addr.sin_port = 0;
    assert(bind(refused, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    assert(getsockname(refused, (struct sockaddr *)&addr, &len) == 0);
    assert(tcp_connect_timeout("localhost", ntohs(addr.sin_port), 100) == -1);
    close(refused);
    puts("Configuration and hostname connection tests passed");
    return 0;
}
