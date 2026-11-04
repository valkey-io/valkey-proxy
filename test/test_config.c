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
