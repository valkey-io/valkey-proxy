#include "proxy.h"
#include <sys/resource.h>
#include <netinet/tcp.h>
#include <signal.h>

/* ── global definitions ─────────────────────────────────────────────── */
/* POOL_SHARED's entries are overwritten by shared_connection_per_node in
 * parse_conf(): that pool is always exactly that size. */
int g_pool_min[POOL_TYPE_COUNT] = {2, 2, 0, 0, 0, 4};
int g_pool_max[POOL_TYPE_COUNT] = {20, 10, 10, 10, 10, 4};

Config g_config;
ClusterNode g_nodes[MAX_NODES];
int g_node_count = 0;
int g_slot_map[CLUSTER_SLOTS];
int g_node_gen[MAX_NODES];
int g_node_has_slots[MAX_NODES];
atomic_int g_topology_version = 0;
atomic_int g_backend_resp3_active = 0;
atomic_llong g_next_client_id = 0;
pthread_rwlock_t g_topology_lock = PTHREAD_RWLOCK_INITIALIZER;
Worker g_workers[MAX_WORKERS];
int g_tcp_fd = -1, g_unix_fd = -1;
#ifdef USE_TLS
SSL_CTX *g_ssl_ctx = NULL;
#endif

/* ── resource limits ───────────────────────────────────────────────── */
/* g_config.max_connection only caps how many fds the proxy's own bookkeeping
 * arrays can index; the OS-level RLIMIT_NOFILE soft limit is what actually
 * determines whether accept()/connect() can hand out fds that high. Raise
 * it to cover max_connection plus a small margin for stdio/log fds, falling
 * back to whatever the hard limit allows. */
static void adjust_open_files_limit(void) {
    rlim_t want = (rlim_t)g_config.max_connection + 64;
    struct rlimit lim;
    if (getrlimit(RLIMIT_NOFILE, &lim) < 0) {
        perror("getrlimit(RLIMIT_NOFILE)");
        return;
    }
    if (lim.rlim_cur >= want) return;

    rlim_t old_soft = lim.rlim_cur;
    lim.rlim_cur = (lim.rlim_max == RLIM_INFINITY || lim.rlim_max > want) ? want : lim.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &lim) < 0 || lim.rlim_cur < want) {
        fprintf(stderr,
                "Warning: could not raise RLIMIT_NOFILE to %llu (stuck at %llu); "
                "effective connection capacity is reduced accordingly, since "
                "client and backend connections share this fd budget. Raise the "
                "'nofile' limit for this process/user to reach max_connection=%d.\n",
                (unsigned long long)want, (unsigned long long)lim.rlim_cur, g_config.max_connection);
    } else {
        printf("Raised RLIMIT_NOFILE: %llu -> %llu (max_connection=%d)\n", (unsigned long long)old_soft,
               (unsigned long long)lim.rlim_cur, g_config.max_connection);
    }
}

/* Formats sa as "ip:port" into out (size cap); returns 1 on success
 * (AF_INET/AF_INET6), 0 otherwise (e.g. AF_UNIX, whose sockaddr carries no
 * usable address -- caller falls back to the configured unix socket path). */
static int format_sockaddr(struct sockaddr_storage *sa, char *out, int cap) {
    if (sa->ss_family == AF_INET) {
        char ip[INET_ADDRSTRLEN];
        struct sockaddr_in *sin = (struct sockaddr_in *)sa;
        inet_ntop(AF_INET, &sin->sin_addr, ip, sizeof(ip));
        snprintf(out, (size_t)cap, "%s:%d", ip, ntohs(sin->sin_port));
        return 1;
    }
    if (sa->ss_family == AF_INET6) {
        char ip[INET6_ADDRSTRLEN];
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)sa;
        inet_ntop(AF_INET6, &sin6->sin6_addr, ip, sizeof(ip));
        snprintf(out, (size_t)cap, "%s:%d", ip, ntohs(sin6->sin6_port));
        return 1;
    }
    return 0;
}

/* ── accept ─────────────────────────────────────────────────────────── */
static void do_accept(Worker *w, int listen_fd) {
    /* listen_fd is registered edge-triggered (EPOLLET): a single readiness
     * notification can represent multiple queued connections, so every one
     * of them must be drained here or the leftover ones never get another
     * edge and sit in the kernel backlog forever. */
    for (;;) {
        struct sockaddr_storage sa;
        socklen_t salen = sizeof(sa);
        int fd = accept(listen_fd, (struct sockaddr *)&sa, &salen);
        if (fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) perror("accept");
            return;
        }
        if (fd >= g_config.max_connection) {
            close(fd);
            continue;
        }
        set_nonblocking(fd);
        if (listen_fd == g_tcp_fd) {
            /* A pipelined batch is answered with several small writes in a
             * row; under Nagle each would wait out the client's delayed ACK. */
            int yes = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        }

        ClientConn *client = calloc(1, sizeof(ClientConn));
        if (!client) {
            close(fd);
            continue;
        }
        client->fd = fd;
        client->db = g_config.db;
        client->state = CS_READING_HDR;
        client->resp = 2;
        client->id = atomic_fetch_add(&g_next_client_id, 1) + 1;
        client->connect_time = client->last_interaction = time(NULL);
        client->multi_count = -1;

        if (!format_sockaddr(&sa, client->addr, sizeof(client->addr)))
            snprintf(client->addr, sizeof(client->addr), "%s:0", g_config.unix_socket_path);

        struct sockaddr_storage lsa;
        socklen_t lsalen = sizeof(lsa);
        if (getsockname(fd, (struct sockaddr *)&lsa, &lsalen) != 0 ||
            !format_sockaddr(&lsa, client->laddr, sizeof(client->laddr)))
            snprintf(client->laddr, sizeof(client->laddr), "%s:0", g_config.unix_socket_path);

#ifdef USE_TLS
        if (g_config.tls_enabled && g_ssl_ctx) {
            client->ssl = SSL_new(g_ssl_ctx);
            SSL_set_fd(client->ssl, fd);
            int rc;
            do {
                rc = SSL_accept(client->ssl);
            } while (rc <= 0 && SSL_get_error(client->ssl, rc) == SSL_ERROR_WANT_READ);
            if (rc <= 0) {
                ERR_print_errors_fp(stderr);
                SSL_free(client->ssl);
                close(fd);
                free(client);
                continue;
            }
        }
#endif

        if (sa.ss_family == AF_INET || sa.ss_family == AF_INET6) {
            printf("[worker %d] client connected: %s  fd=%d%s\n", w->thread_id, client->addr, fd,
                   g_config.tls_enabled ? "  (TLS)" : "");
        } else {
            printf("[worker %d] client connected: unix socket  fd=%d\n", w->thread_id, fd);
        }
        w->client_map[fd] = client;
        atomic_fetch_add_explicit(&w->client_count, 1, memory_order_relaxed);
        fd_watch(w, fd, EL_READABLE);
    }
}

/* ── worker thread ──────────────────────────────────────────────────── */
static void *worker_thread(void *arg) {
    Worker *w = (Worker *)arg;
    w->el = el_create(g_config.max_connection);
    w->db_groups = NULL;
    w->db_group_count = 0;
    w->db_group_cap = 0;
    w->client_map = calloc((size_t)g_config.max_connection, sizeof(*w->client_map));
    w->backend_map = calloc((size_t)g_config.max_connection, sizeof(*w->backend_map));
    w->fd_mask = calloc((size_t)g_config.max_connection, sizeof(*w->fd_mask));
    w->input_ready = calloc((size_t)g_config.max_connection, sizeof(*w->input_ready));
    w->input_ready_len = 0;
    if (!w->client_map || !w->backend_map || !w->fd_mask || !w->input_ready) {
        fprintf(stderr, "out of memory allocating connection tables (max_connection=%d)\n", g_config.max_connection);
        return NULL;
    }
    pthread_mutex_init(&w->db_lock, NULL);
    atomic_store_explicit(&w->stats_ready, 1, memory_order_release);
    if (!w->el) {
        fprintf(stderr, "el_create failed\n");
        return NULL;
    }

    if (g_tcp_fd >= 0) fd_watch(w, g_tcp_fd, EL_READABLE);
    if (g_unix_fd >= 0) fd_watch(w, g_unix_fd, EL_READABLE);

    /* Self-pipe so the topology refresh thread can pull this worker straight
     * out of el_poll(-1) the moment it publishes a change, instead of this
     * worker only noticing on its next unrelated client/backend event (see
     * wake_all_workers() in cluster.c). */
    int wakefds[2];
    if (pipe(wakefds) == 0) {
        set_nonblocking(wakefds[0]);
        set_nonblocking(wakefds[1]);
        w->wake_r = wakefds[0];
        w->wake_w = wakefds[1];
        fd_watch(w, w->wake_r, EL_READABLE);
    } else {
        perror("pipe(wake)");
        w->wake_r = w->wake_w = -1;
    }

    /* Seed this worker's topology snapshot from the global state before the
     * first db_group_create() reads it (fetch_cluster_nodes() has already
     * populated the globals by the time worker threads are spawned -- see
     * main()). No lock needed here: at this point the refresh thread may
     * already be running, but pthread_create()'s happens-before guarantee
     * plus the fact that a torn read here only means this worker starts
     * from a topology that's at most one refresh cycle stale (it'll pick up
     * the rest on its first worker_sync_topology() call) makes a plain
     * rdlock'd copy sufficient. */
    pthread_rwlock_rdlock(&g_topology_lock);
    memcpy(w->topo.nodes, g_nodes, sizeof(w->topo.nodes));
    memcpy(w->topo.node_gen, g_node_gen, sizeof(w->topo.node_gen));
    memcpy(w->topo.node_has_slots, g_node_has_slots, sizeof(w->topo.node_has_slots));
    memcpy(w->topo.slot_map, g_slot_map, sizeof(w->topo.slot_map));
    w->topo.node_count = g_node_count;
    w->topo.version = atomic_load_explicit(&g_topology_version, memory_order_acquire);
    pthread_rwlock_unlock(&g_topology_lock);
    w->topology_version_seen = w->topo.version;

    db_group_get(w, g_config.db);

    while (1) {
        /* Don't block while some client still has pipelined commands queued
         * in user space (see client_run_buffered_input()). */
        int n = el_poll(w->el, w->input_ready_len ? 0 : -1);
        if (n < 0) break;
        if (w->topology_version_seen != topology_version_load()) worker_sync_topology(w);
        for (int i = 0; i < n; i++) {
            int fd = w->el->fired[i].fd;
            int mask = w->el->fired[i].mask;
            if (fd == g_tcp_fd || fd == g_unix_fd) {
                do_accept(w, fd);
                continue;
            }
            if (fd == w->wake_r) {
                /* Just a nudge -- the actual sync already ran above as soon
                 * as el_poll returned. Drain so the (edge-triggered) pipe
                 * doesn't accumulate unread bytes across many wake-ups. */
                char drain[256];
                while (read(fd, drain, sizeof(drain)) > 0) {}
                continue;
            }
            if (fd < 0 || fd >= g_config.max_connection) continue;
            if (w->client_map[fd]) {
                ClientConn *cl = w->client_map[fd];
                if (mask & EL_READABLE) handle_client_readable(w, cl);
                if (cl == w->client_map[fd] && (mask & EL_WRITABLE)) handle_client_writable(w, cl);
            } else if (w->backend_map[fd]) {
                PoolConn *pc = w->backend_map[fd];
                uint32_t epoch = pc->epoch;
                if (mask & EL_WRITABLE) handle_backend_writable(w, pc);
                /* pc==backend_map[fd] catches destruction/replacement; epoch
                 * catches an in-place hand-off to a new owner (pool_release's
                 * waiter reassignment) -- either way this fd's PoolConn is no
                 * longer the one this event was originally reported for. */
                if (pc == w->backend_map[fd] && pc->epoch == epoch && (mask & EL_READABLE))
                    handle_backend_readable(w, pc);
            }
        }
        client_run_buffered_input(w);
    }
    return NULL;
}

/* ── main ───────────────────────────────────────────────────────────── */
int main(int argc, char **argv) {
    const char *conf_path = argc > 1 ? argv[1] : "proxy.conf";
    setbuf(stdout, NULL);

    /* A write() to a backend/client socket whose peer already sent RST (e.g.
     * the old primary being demoted/killed during a cluster failover) raises
     * SIGPIPE; left at its default disposition that kills the whole process,
     * dropping every client on every worker thread at once. Ignoring it here
     * makes such a write just fail with -1/EPIPE, which every write() call
     * site already handles. */
    signal(SIGPIPE, SIG_IGN);

    parse_conf(conf_path);
    info_init(argv[0], conf_path);
    adjust_open_files_limit();

    memset(g_slot_map, -1, sizeof(g_slot_map));

    printf("Event loop backend : %s\n", el_backend_name());
    printf("Config: server=%s:%d  bind=%s:%d  workers=%d  default-db=%d\n", g_config.server_ip, g_config.server_port,
           g_config.bind_ip, g_config.proxy_port, g_config.worker_thread_no, g_config.db);
    printf("Pool sizes (min/max): normal=%d/%d  blocking=%d/%d"
           "  subscription=%d/%d  transaction=%d/%d  acl=%d/%d\n",
           g_pool_min[POOL_NORMAL], g_pool_max[POOL_NORMAL], g_pool_min[POOL_BLOCKING], g_pool_max[POOL_BLOCKING],
           g_pool_min[POOL_SUBSCRIPTION], g_pool_max[POOL_SUBSCRIPTION], g_pool_min[POOL_TRANSACTION],
           g_pool_max[POOL_TRANSACTION], g_pool_min[POOL_ACL], g_pool_max[POOL_ACL]);
    printf("Backend multiplexing: %s  (shared connections/node=%d  max in-flight/client=%d)\n\n",
           g_config.backend_multiplexing ? "yes" : "no", g_config.shared_connection_per_node,
           g_config.max_pipeline_inflight);

#ifdef USE_TLS
    if (g_config.tls_enabled && init_tls() < 0) exit(1);
#endif

    /* Must be set before the refresh thread can possibly start signalling
     * workers (see wake_all_workers() in cluster.c) -- each worker's real
     * wake_w isn't ready until its own worker_thread() runs, which happens
     * after this point, so -1 here is what tells wake_all_workers() to skip
     * a worker that isn't listening yet instead of writing to fd 0. */
    for (int i = 0; i < MAX_WORKERS; i++) g_workers[i].wake_w = -1;

    fetch_cluster_nodes();
    if (g_config.backend_multiplexing) {
        /* Shared connections are all opened up front, for every node in
         * every worker -- and again for each extra db a client SELECTs. */
        long shared = (long)g_config.shared_connection_per_node * g_node_count * g_config.worker_thread_no;
        if (shared * 2 > g_config.max_connection)
            fprintf(stderr,
                    "Warning: shared connections alone (%ld = %d per node x %d node(s) x %d worker(s), per db) "
                    "take over half of max_connection (%d)\n",
                    shared, g_config.shared_connection_per_node, g_node_count, g_config.worker_thread_no,
                    g_config.max_connection);
    }
    topology_start_refresh_thread();

    g_tcp_fd = create_tcp_listener(g_config.bind_ip, g_config.proxy_port);
    if (g_tcp_fd < 0) exit(1);
    printf("Listening on %s:%d (TCP)\n", g_config.bind_ip, g_config.proxy_port);

    if (g_config.unix_socket_path[0]) {
        g_unix_fd = create_unix_listener(g_config.unix_socket_path);
        if (g_unix_fd < 0) exit(1);
        printf("Listening on %s (Unix socket)\n", g_config.unix_socket_path);
    }
    printf("\n");

    int nw = g_config.worker_thread_no; /* clamped to [1, MAX_WORKERS] by parse_conf() */
    printf("Starting %d worker thread(s)...\n\n", nw);

    for (int i = 0; i < nw; i++) {
        g_workers[i].thread_id = i;
        pthread_create(&g_workers[i].tid, NULL, worker_thread, &g_workers[i]);
    }
    for (int i = 0; i < nw; i++) pthread_join(g_workers[i].tid, NULL);
    return 0;
}
