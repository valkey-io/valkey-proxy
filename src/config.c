#include "proxy.h"
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>

#define DEFAULT_BACKEND_CONNECT_TIMEOUT_MS  1000
#define DEFAULT_SHARED_CONNECTION_PER_NODE  4
#define DEFAULT_MAX_PIPELINE_INFLIGHT       256
#define DEFAULT_CLUSTER_REFRESH_INTERVAL_MS 500
#define DEFAULT_MAX_MULTIKEY_PAYLOAD        (64 * 1024 * 1024)

/* ── config parsing ─────────────────────────────────────────────────── */
/* For the reported_* settings, where a value cut short at a space or at
 * the field size would silently report something else. */
static void set_reported(const char *key, const char *line, const char *val, char *dst, size_t cap) {
    char extra[2];
    if (sscanf(line, "%*s %*s %1s", extra) == 1)
        fprintf(stderr, "Warning: %s takes one value without spaces; ignoring everything after \"%s\"\n", key, val);
    if (strlen(val) >= cap) fprintf(stderr, "Warning: %s is longer than %zu characters; truncating\n", key, cap - 1);
    snprintf(dst, cap, "%.*s", (int)(cap - 1), val);
}

/* For proxy-config-admin-users / proxy-config-readonly-users: appends val's
 * comma-separated user names to the list in dst, so the setting can be
 * split over several lines. Any mistake is fatal, since it decides who may
 * reconfigure the proxy. */
static void add_users(const char *path, int lineno, const char *key, const char *line, const char *val, char *dst,
                      size_t cap) {
    char extra[2];
    if (sscanf(line, "%*s %*s %1s", extra) == 1) {
        fprintf(stderr, "%s:%d: %s takes a comma-separated list of users without spaces\n", path, lineno, key);
        exit(1);
    }
    for (const char *p = val;;) {
        const char *e = strchr(p, ',');
        size_t n = e ? (size_t)(e - p) : strlen(p);
        if (n == 0) {
            fprintf(stderr, "%s:%d: %s has an empty user name\n", path, lineno, key);
            exit(1);
        }
        if (n == 7 && !strncmp(p, "default", 7))
            fprintf(stderr, "Warning: %s lists \"default\", which never has PROXY CONFIG access\n", key);
        if (!e) break;
        p = e + 1;
    }
    size_t used = strlen(dst);
    if (used + (used ? 1 : 0) + strlen(val) >= cap) {
        fprintf(stderr, "%s:%d: %s lists more than %zu characters of user names\n", path, lineno, key, cap - 1);
        exit(1);
    }
    if (used) dst[used++] = ',';
    memcpy(dst + used, val, strlen(val) + 1);
}

void parse_conf(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        perror(path);
        exit(1);
    }
    g_config.worker_thread_no = 1;
    g_config.db = 0;
    g_config.tls_enabled = 0;
    g_config.max_multikey_payload = DEFAULT_MAX_MULTIKEY_PAYLOAD;
    g_config.max_connection = DEFAULT_MAX_CONNECTIONS;
    g_config.cluster_refresh_interval_ms = DEFAULT_CLUSTER_REFRESH_INTERVAL_MS; /* 0 = periodic refresh disabled */
    g_config.cluster_node_fail_grace_ms = 10000;
    g_config.backend_connect_timeout_ms = DEFAULT_BACKEND_CONNECT_TIMEOUT_MS;
    g_config.backend_multiplexing = 1;
    g_config.shared_connection_per_node = DEFAULT_SHARED_CONNECTION_PER_NODE;
    g_config.max_pipeline_inflight = DEFAULT_MAX_PIPELINE_INFLIGHT;
    g_config.transaction_replay_buffer_size = 1024 * 1024;
    int have_server_port = 0, have_port = 0;
    char line[512], key[64], val[448];
    int lineno = 0;
    while (fgets(line, sizeof(line), f)) {
        lineno++;
        const char *p = line;
        while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
        if (*p == '\0' || *p == '#') continue;
        int n = sscanf(p, "%63s %447s", key, val);
        if (n != 2) {
            fprintf(stderr, "%s:%d: config option \"%s\" has no value\n", path, lineno, key);
            exit(1);
        }
        if (!strcmp(key, "server-ip")) {
            if (strlen(val) >= sizeof(g_config.server_ip)) {
                fprintf(stderr, "server-ip exceeds the maximum hostname length of %zu characters\n",
                        sizeof(g_config.server_ip) - 1);
                exit(1);
            }
            snprintf(g_config.server_ip, sizeof(g_config.server_ip), "%s", val);
        } else if (!strcmp(key, "server-port")) {
            g_config.server_port = atoi(val);
            have_server_port = 1;
        } else if (!strcmp(key, "bind"))
            snprintf(g_config.bind_ip, 64, "%.63s", val);
        else if (!strcmp(key, "port")) {
            g_config.proxy_port = atoi(val);
            have_port = 1;
        } else if (!strcmp(key, "worker-thread-no"))
            g_config.worker_thread_no = atoi(val);
        else if (!strcmp(key, "db"))
            g_config.db = atoi(val);
        else if (!strcmp(key, "unix-socket"))
            snprintf(g_config.unix_socket_path, 256, "%.255s", val);
        else if (!strcmp(key, "tls-enabled"))
            g_config.tls_enabled = (!strcmp(val, "yes"));
        else if (!strcmp(key, "tls-cert"))
            snprintf(g_config.tls_cert, 256, "%.255s", val);
        else if (!strcmp(key, "tls-key"))
            snprintf(g_config.tls_key, 256, "%.255s", val);
        else if (!strcmp(key, "max-multikey-payload"))
            g_config.max_multikey_payload = atoi(val);
        else if (!strcmp(key, "max-connection"))
            g_config.max_connection = atoi(val);
        else if (!strcmp(key, "cluster-refresh-interval-ms"))
            g_config.cluster_refresh_interval_ms = atoi(val);
        else if (!strcmp(key, "cluster-node-fail-grace-ms"))
            g_config.cluster_node_fail_grace_ms = atoi(val);
        else if (!strcmp(key, "backend-connect-timeout-ms"))
            g_config.backend_connect_timeout_ms = atoi(val);
        else if (!strcmp(key, "backend-multiplexing"))
            g_config.backend_multiplexing = (!strcmp(val, "yes"));
        else if (!strcmp(key, "shared-connection-per-node"))
            g_config.shared_connection_per_node = atoi(val);
        else if (!strcmp(key, "max-pipeline-inflight"))
            g_config.max_pipeline_inflight = atoi(val);
        else if (!strcmp(key, "transaction-replay-buffer-size"))
            g_config.transaction_replay_buffer_size = atoi(val);
        else if (!strcmp(key, "min-normal-connection"))
            g_pool_min[POOL_NORMAL] = atoi(val);
        else if (!strcmp(key, "max-normal-connection"))
            g_pool_max[POOL_NORMAL] = atoi(val);
        else if (!strcmp(key, "min-blocking-connection"))
            g_pool_min[POOL_BLOCKING] = atoi(val);
        else if (!strcmp(key, "max-blocking-connection"))
            g_pool_max[POOL_BLOCKING] = atoi(val);
        else if (!strcmp(key, "min-subscription-connection"))
            g_pool_min[POOL_SUBSCRIPTION] = atoi(val);
        else if (!strcmp(key, "max-subscription-connection"))
            g_pool_max[POOL_SUBSCRIPTION] = atoi(val);
        else if (!strcmp(key, "min-transaction-connection"))
            g_pool_min[POOL_TRANSACTION] = atoi(val);
        else if (!strcmp(key, "max-transaction-connection"))
            g_pool_max[POOL_TRANSACTION] = atoi(val);
        else if (!strcmp(key, "min-acl-connection"))
            g_pool_min[POOL_ACL] = atoi(val);
        else if (!strcmp(key, "max-acl-connection"))
            g_pool_max[POOL_ACL] = atoi(val);
        else if (!strcmp(key, "proxy-config-admin-users"))
            add_users(path, lineno, key, p, val, g_config.proxy_config_admin_users,
                      sizeof(g_config.proxy_config_admin_users));
        else if (!strcmp(key, "proxy-config-readonly-users"))
            add_users(path, lineno, key, p, val, g_config.proxy_config_readonly_users,
                      sizeof(g_config.proxy_config_readonly_users));
        else if (!strcmp(key, "config-command-enable")) {
            if (strcmp(val, "0") != 0 && strcmp(val, "1") != 0) {
                fprintf(stderr, "%s:%d: config-command-enable must be 0 or 1\n", path, lineno);
                exit(1);
            }
            g_config.config_command_enable = atoi(val);
        } else if (!strcmp(key, "reported-version"))
            set_reported(key, p, val, g_config.reported_version, sizeof(g_config.reported_version));
        else if (!strcmp(key, "reported-role"))
            set_reported(key, p, val, g_config.reported_role, sizeof(g_config.reported_role));
        else if (!strcmp(key, "reported-modules"))
            set_reported(key, p, val, g_config.reported_modules, sizeof(g_config.reported_modules));
        else {
            fprintf(stderr, "%s:%d: unknown config option \"%s\"\n", path, lineno, key);
            exit(1);
        }
    }
    fclose(f);

    /* The ports have no sensible default: a proxy listening on, or dialing,
     * port 0 would come up looking healthy and serve nothing. */
    if (!have_server_port || g_config.server_port < 1 || g_config.server_port > 65535) {
        fprintf(stderr, "%s: server-port must be set to a port number between 1 and 65535\n", path);
        exit(1);
    }
    if (!have_port || g_config.proxy_port < 1 || g_config.proxy_port > 65535) {
        fprintf(stderr, "%s: port must be set to a port number between 1 and 65535\n", path);
        exit(1);
    }
    if (g_config.db < 0) {
        fprintf(stderr, "Warning: db can't be negative (got %d); falling back to default 0\n", g_config.db);
        g_config.db = 0;
    }
    if (g_config.max_multikey_payload <= 0) {
        fprintf(stderr, "Warning: max-multikey-payload must be positive (got %d); falling back to default %d\n",
                g_config.max_multikey_payload, DEFAULT_MAX_MULTIKEY_PAYLOAD);
        g_config.max_multikey_payload = DEFAULT_MAX_MULTIKEY_PAYLOAD;
    }
    if (g_config.max_connection <= 0) {
        fprintf(stderr, "Warning: max-connection must be positive (got %d); falling back to default %d\n",
                g_config.max_connection, DEFAULT_MAX_CONNECTIONS);
        g_config.max_connection = DEFAULT_MAX_CONNECTIONS;
    }
    /* 0 would make every backend connect time out at once, and as an
     * SO_RCVTIMEO it means no timeout at all. */
    if (g_config.backend_connect_timeout_ms <= 0) {
        fprintf(stderr, "Warning: backend-connect-timeout-ms must be positive (got %d); falling back to default %d\n",
                g_config.backend_connect_timeout_ms, DEFAULT_BACKEND_CONNECT_TIMEOUT_MS);
        g_config.backend_connect_timeout_ms = DEFAULT_BACKEND_CONNECT_TIMEOUT_MS;
    }
    if (g_config.shared_connection_per_node <= 0) {
        fprintf(stderr, "Warning: shared-connection-per-node must be positive (got %d); falling back to default %d\n",
                g_config.shared_connection_per_node, DEFAULT_SHARED_CONNECTION_PER_NODE);
        g_config.shared_connection_per_node = DEFAULT_SHARED_CONNECTION_PER_NODE;
    }
    if (g_config.max_pipeline_inflight <= 0) {
        fprintf(stderr, "Warning: max-pipeline-inflight must be positive (got %d); falling back to default %d\n",
                g_config.max_pipeline_inflight, DEFAULT_MAX_PIPELINE_INFLIGHT);
        g_config.max_pipeline_inflight = DEFAULT_MAX_PIPELINE_INFLIGHT;
    }
    if (g_config.transaction_replay_buffer_size < 0) {
        fprintf(stderr, "Warning: transaction-replay-buffer-size can't be negative (got %d); using 0 (no replay)\n",
                g_config.transaction_replay_buffer_size);
        g_config.transaction_replay_buffer_size = 0;
    }
    /* Shared connections are picked by slot % count, so the pool must hold
     * exactly this many at all times; with multiplexing off it's unused. */
    g_pool_min[POOL_SHARED] = g_pool_max[POOL_SHARED] =
        g_config.backend_multiplexing ? g_config.shared_connection_per_node : 0;
    /* Clamp here, before any thread starts, so everything that walks
     * g_workers (main(), the refresh thread, PROXY POOLS) agrees on how many
     * workers there are. */
    if (g_config.worker_thread_no < 1) g_config.worker_thread_no = 1;
    if (g_config.worker_thread_no > MAX_WORKERS) g_config.worker_thread_no = MAX_WORKERS;
}

/* ── TCP / Unix helpers ─────────────────────────────────────────────── */
void set_nonblocking(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* timeout_ms bounds only the "still connecting" wait (TCP handshake in
 * flight); ECONNREFUSED and similar immediate errors return right away
 * regardless of it. Kept separate from tcp_connect() below because the
 * right value differs sharply by caller: fetch_cluster_nodes()'s one-shot
 * startup discovery (before any thread exists to block) can afford a
 * generous wait, but pool_new_fd() (worker thread, reconnecting to a
 * possibly still-booting promoted replica) and the periodic topology
 * refresh (its own thread, but retried every cycle) both want to fail fast
 * instead of tying up a thread for seconds on end. */
static int tcp_connect_ipv4(const char *ip, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int ret = connect(fd, (struct sockaddr *)&a, sizeof(a));
    if (ret < 0 && errno != EINPROGRESS) {
        if (errno == ECONNREFUSED)
            fprintf(stderr, "Could not connect to Valkey at %s:%d: Connection refused\n", ip, port);
        else
            perror("connect");
        close(fd);
        return -1;
    }

    if (ret != 0) {
        /* poll(), not select(): fd_set only holds fds below FD_SETSIZE
         * (1024), and with enough clients connected this fd is above it --
         * FD_SET would then abort under _FORTIFY_SOURCE (or write past
         * the fd_set without it). */
        struct pollfd pfd = {.fd = fd, .events = POLLOUT};
        int pr;
        do pr = poll(&pfd, 1, timeout_ms);
        while (pr < 0 && errno == EINTR);
        if (pr <= 0) {
            fprintf(stderr, "Could not connect to Valkey at %s:%d: Connection timed out\n", ip, port);
            close(fd);
            return -1;
        }
        int err = 0;
        socklen_t elen = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
        if (err != 0) {
            if (err == ECONNREFUSED)
                fprintf(stderr, "Could not connect to Valkey at %s:%d: Connection refused\n", ip, port);
            else
                fprintf(stderr, "Could not connect to Valkey at %s:%d: %s\n", ip, port, strerror(err));
            close(fd);
            return -1;
        }
    }

    /* Pipelined commands sharing one backend connection go out as separate
     * small writes; under Nagle each would wait for the previous one's ACK. */
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    fcntl(fd, F_SETFL, flags);
    return fd;
}

/* Numeric backend addresses avoid DNS entirely. Hostnames (including the
 * configured seed) are resolved on each connection, so pod IP changes are
 * picked up by topology refresh. The timeout bounds each TCP handshake;
 * getaddrinfo() uses the system resolver's own timeout. */
int tcp_connect_timeout(const char *host, int port, int timeout_ms) {
    struct in_addr numeric;
    if (inet_pton(AF_INET, host, &numeric) == 1) return tcp_connect_ipv4(host, port, timeout_ms);

    struct addrinfo hints = {0}, *addresses = NULL;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err = getaddrinfo(host, NULL, &hints, &addresses);
    if (err != 0) {
        fprintf(stderr, "Could not resolve Valkey host %s: %s\n", host, gai_strerror(err));
        return -1;
    }

    int fd = -1;
    for (struct addrinfo *a = addresses; a; a = a->ai_next) {
        char ip[INET_ADDRSTRLEN];
        struct sockaddr_in *addr = (struct sockaddr_in *)a->ai_addr;
        if (!inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip))) continue;
        fd = tcp_connect_ipv4(ip, port, timeout_ms);
        if (fd >= 0) break;
    }
    freeaddrinfo(addresses);
    return fd;
}

int tcp_connect(const char *ip, int port) {
    return tcp_connect_timeout(ip, port, 5000);
}

int create_tcp_listener(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    set_nonblocking(fd);
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (ip[0])
        inet_pton(AF_INET, ip, &a.sin_addr);
    else
        a.sin_addr.s_addr = INADDR_ANY;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 128) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

int create_unix_listener(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket(unix)");
        return -1;
    }
    set_nonblocking(fd);
    unlink(path);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%.107s", path);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
        perror("bind(unix)");
        close(fd);
        return -1;
    }
    if (listen(fd, 128) < 0) {
        perror("listen(unix)");
        close(fd);
        return -1;
    }
    return fd;
}

/* ── TLS ────────────────────────────────────────────────────────────── */
#ifdef USE_TLS
int init_tls(void) {
    if (!g_config.tls_cert[0] || !g_config.tls_key[0]) {
        fprintf(stderr, "TLS: cert/key not set\n");
        return -1;
    }
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    g_ssl_ctx = SSL_CTX_new(TLS_server_method());
    if (!g_ssl_ctx) {
        ERR_print_errors_fp(stderr);
        return -1;
    }
    if (SSL_CTX_use_certificate_file(g_ssl_ctx, g_config.tls_cert, SSL_FILETYPE_PEM) <= 0) {
        ERR_print_errors_fp(stderr);
        return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(g_ssl_ctx, g_config.tls_key, SSL_FILETYPE_PEM) <= 0) {
        ERR_print_errors_fp(stderr);
        return -1;
    }
    if (!SSL_CTX_check_private_key(g_ssl_ctx)) {
        fprintf(stderr, "TLS: cert/key mismatch\n");
        return -1;
    }
    printf("TLS enabled  cert=%s\n", g_config.tls_cert);
    return 0;
}
#endif
