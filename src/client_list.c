#include "proxy.h"
#include <limits.h>
#include <strings.h>

/* ── CLIENT LIST ────────────────────────────────────────────────────────
 * Lists the proxy's own clients, across every worker thread, in the
 * CLIENT INFO line format. Nothing goes to a backend.
 *
 * A client belongs to one worker and only that worker reads or writes it,
 * so the worker that gets CLIENT LIST can't walk the others' clients. It
 * puts the request in each other worker's inbox and wakes it through its
 * wake pipe (see worker_thread() in valkey-proxy.c). Each worker appends
 * the lines of its own matching clients; whichever finishes last hands the
 * request back to the asking worker through its done list, the same way.
 * Meanwhile the asking client waits in CS_WAITING_RESP, like a fan-out.
 *
 * A request is freed when its last reference goes: one held by the asking
 * worker until it replies, plus one per inbox it was put in. The done list
 * is linked through the requests themselves, so handing one back can't fail
 * for lack of memory. */

enum { CL_TYPE_ANY, CL_TYPE_NORMAL, CL_TYPE_PUBSUB, CL_TYPE_NONE };

typedef struct {
    int type;
    long long *ids;
    int nids;
    int has_user, has_addr, has_laddr, has_name, has_ip;
    char user[64], addr[280], laddr[280], name[64], ip[280];
    char flags[16];    /* empty = no FLAGS filter */
    long long skip_id; /* -1 = SKIPME no */
    long long maxage, idle;
    int db;
} ClientListFilter;

struct ClientListReq {
    pthread_mutex_t lock;
    int refs;
    int pending;
    int oom;
    GrowBuf out;
    ClientListFilter f;
    Worker *owner;
    /* Only the owner worker reads or writes this; NULL once the client is
     * gone (see client_list_forget()). */
    ClientConn *client;
    ClientListReq *done_next; /* owner's done list, under its list_lock */
};

static void req_unref(ClientListReq *req) {
    pthread_mutex_lock(&req->lock);
    int left = --req->refs;
    pthread_mutex_unlock(&req->lock);
    if (left) return;
    pthread_mutex_destroy(&req->lock);
    free(req->f.ids);
    free(req->out.buf);
    free(req);
}

/* ── filter ─────────────────────────────────────────────────────────── */
static int arg_eq(const char *a, int len, const char *word) {
    return len == (int)strlen(word) && !strncasecmp(a, word, (size_t)len);
}

static int parse_ll(const char *a, int len, long long *out) {
    char buf[32];
    if (len <= 0 || len >= (int)sizeof(buf)) return -1;
    memcpy(buf, a, (size_t)len);
    buf[len] = '\0';
    char *end;
    errno = 0;
    long long v = strtoll(buf, &end, 10);
    if (errno || *end) return -1;
    *out = v;
    return 0;
}

static void copy_arg(char *dst, size_t cap, const char *a, int len) {
    if (len >= (int)cap) len = (int)cap - 1;
    memcpy(dst, a, (size_t)len);
    dst[len] = '\0';
}

/* Parses CLIENT LIST's options, with Valkey's error messages. Returns 0, or
 * -1 after copying the error into err. */
static int parse_filter(ClientConn *client, ClientListFilter *f, char *err, size_t errcap) {
    memset(f, 0, sizeof(*f));
    f->type = CL_TYPE_ANY;
    f->skip_id = -1;
    f->db = -1;
    const char *a, *v;
    int al, vl;
    for (int i = 2; resp_get_nth_arg(client->hbuf, client->hbuf_len, i, &a, &al); i += 2) {
        if (!resp_get_nth_arg(client->hbuf, client->hbuf_len, i + 1, &v, &vl)) goto syntax;
        long long n;
        if (arg_eq(a, al, "type")) {
            if (arg_eq(v, vl, "normal"))
                f->type = CL_TYPE_NORMAL;
            else if (arg_eq(v, vl, "pubsub"))
                f->type = CL_TYPE_PUBSUB;
            else if (arg_eq(v, vl, "primary") || arg_eq(v, vl, "master") || arg_eq(v, vl, "replica") ||
                     arg_eq(v, vl, "slave"))
                f->type = CL_TYPE_NONE; /* the proxy has no replication links */
            else {
                /* As much of the value as fits client_send_error()'s line, with
                 * CR/LF turned into spaces like Valkey so it stays one line. */
                snprintf(err, errcap, "Unknown client type '%.*s'", vl > 200 ? 200 : vl, v);
                for (char *p = err; *p; p++)
                    if (*p == '\r' || *p == '\n') *p = ' ';
                return -1;
            }
        } else if (arg_eq(a, al, "id")) {
            /* Takes every id that follows, up to the next option. */
            int j = i + 1;
            while (resp_get_nth_arg(client->hbuf, client->hbuf_len, j, &v, &vl) && parse_ll(v, vl, &n) == 0) {
                long long *tmp = realloc(f->ids, sizeof(*tmp) * (size_t)(f->nids + 1));
                if (!tmp) {
                    snprintf(err, errcap, "out of memory");
                    return -1;
                }
                f->ids = tmp;
                f->ids[f->nids++] = n;
                j++;
            }
            if (j == i + 1) goto syntax;
            i = j - 2;
        } else if (arg_eq(a, al, "user")) {
            f->has_user = 1;
            copy_arg(f->user, sizeof(f->user), v, vl);
        } else if (arg_eq(a, al, "addr")) {
            f->has_addr = 1;
            copy_arg(f->addr, sizeof(f->addr), v, vl);
        } else if (arg_eq(a, al, "laddr")) {
            f->has_laddr = 1;
            copy_arg(f->laddr, sizeof(f->laddr), v, vl);
        } else if (arg_eq(a, al, "name")) {
            f->has_name = 1;
            copy_arg(f->name, sizeof(f->name), v, vl);
        } else if (arg_eq(a, al, "ip")) {
            f->has_ip = 1;
            copy_arg(f->ip, sizeof(f->ip), v, vl);
        } else if (arg_eq(a, al, "flags")) {
            copy_arg(f->flags, sizeof(f->flags), v, vl);
        } else if (arg_eq(a, al, "skipme")) {
            if (arg_eq(v, vl, "yes"))
                f->skip_id = client->id;
            else if (arg_eq(v, vl, "no"))
                f->skip_id = -1;
            else
                goto syntax;
        } else if (arg_eq(a, al, "maxage") || arg_eq(a, al, "idle")) {
            const char *what = arg_eq(a, al, "maxage") ? "maxage" : "idle";
            if (parse_ll(v, vl, &n) < 0) {
                snprintf(err, errcap, "%s is not an integer or out of range", what);
                return -1;
            }
            if (n <= 0) {
                snprintf(err, errcap, "%s should be greater than 0", what);
                return -1;
            }
            if (what[0] == 'm')
                f->maxage = n;
            else
                f->idle = n;
        } else if (arg_eq(a, al, "db")) {
            if (parse_ll(v, vl, &n) < 0 || n < 0 || n > INT_MAX) {
                snprintf(err, errcap, "DB is not an integer or out of range");
                return -1;
            }
            f->db = (int)n;
        } else {
            goto syntax;
        }
    }
    return 0;
syntax:
    snprintf(err, errcap, "syntax error");
    return -1;
}

static int filter_match(const ClientListFilter *f, ClientConn *c, time_t now) {
    if (c->id == f->skip_id) return 0;
    if (f->type != CL_TYPE_ANY) {
        int pubsub = c->sub_count + c->psub_count + c->ssub_count > 0;
        if (f->type == CL_TYPE_NONE || pubsub != (f->type == CL_TYPE_PUBSUB)) return 0;
    }
    if (f->nids) {
        int found = 0;
        for (int i = 0; i < f->nids && !found; i++) found = f->ids[i] == c->id;
        if (!found) return 0;
    }
    if (f->has_user && strcmp(f->user, c->acl_authenticated ? c->acl_user : "default")) return 0;
    if (f->has_addr && strcmp(f->addr, c->addr)) return 0;
    if (f->has_laddr && strcmp(f->laddr, c->laddr)) return 0;
    if (f->has_name && strcmp(f->name, c->name)) return 0;
    if (f->has_ip) {
        const char *colon = strrchr(c->addr, ':');
        size_t iplen = colon ? (size_t)(colon - c->addr) : strlen(c->addr);
        if (strlen(f->ip) != iplen || strncmp(f->ip, c->addr, iplen)) return 0;
    }
    for (const char *p = f->flags; *p; p++)
        if (!strchr(client_info_flags(c), *p)) return 0;
    if (f->maxage && (long long)(now - c->connect_time) < f->maxage) return 0;
    if (f->idle && (long long)(now - c->last_interaction) < f->idle) return 0;
    if (f->db >= 0 && c->db != f->db) return 0;
    return 1;
}

/* Appends one CLIENT INFO line per matching client of w to g. */
static int format_worker_clients(Worker *w, const ClientListFilter *f, GrowBuf *g) {
    time_t now = time(NULL);
    char line[1024];
    for (int fd = 0; fd < g_config.max_connection; fd++) {
        ClientConn *c = w->client_map[fd];
        if (!c || !filter_match(f, c, now)) continue;
        int len = format_client_info_line(w, c, line, sizeof(line) - 1);
        if (len < 0) continue;
        if (len > (int)sizeof(line) - 2) len = (int)sizeof(line) - 2; /* snprintf truncated */
        line[len++] = '\n';
        if (gb_append(g, line, len) < 0) return -1;
    }
    return 0;
}

/* ── hand-off between workers ───────────────────────────────────────── */
/* Puts req in w's inbox. Returns 1 if w no longer takes requests (checked
 * under the lock, so client_list_stop() can't miss one), -1 out of memory. */
static int inbox_push(Worker *w, ClientListReq *req) {
    int rc = 0;
    pthread_mutex_lock(&w->list_lock);
    if (!atomic_load_explicit(&w->list_ready, memory_order_relaxed)) {
        rc = 1;
    } else if (w->list_inbox_len == w->list_inbox_cap) {
        int nc = w->list_inbox_cap ? w->list_inbox_cap * 2 : 8;
        ClientListReq **tmp = realloc(w->list_inbox, sizeof(*tmp) * (size_t)nc);
        if (!tmp)
            rc = -1;
        else {
            w->list_inbox = tmp;
            w->list_inbox_cap = nc;
        }
    }
    if (!rc) w->list_inbox[w->list_inbox_len++] = req;
    pthread_mutex_unlock(&w->list_lock);
    return rc;
}

/* client_list_service() takes each list whole, so requests pushed while a
 * batch is served wait for the next wake-up instead of keeping this worker
 * away from its own clients. */
static ClientListReq **inbox_take(Worker *w, int *n) {
    pthread_mutex_lock(&w->list_lock);
    ClientListReq **batch = w->list_inbox;
    *n = w->list_inbox_len;
    w->list_inbox = NULL;
    w->list_inbox_len = w->list_inbox_cap = 0;
    pthread_mutex_unlock(&w->list_lock);
    return batch;
}

static ClientListReq *done_take(Worker *w) {
    pthread_mutex_lock(&w->list_lock);
    ClientListReq *head = w->list_done;
    w->list_done = NULL;
    pthread_mutex_unlock(&w->list_lock);
    return head;
}

static void wake(Worker *w) {
    char b = 1;
    ssize_t r = write(w->wake_w, &b, 1); /* a full pipe already has a wake pending */
    (void)r;
}

/* Sends the reply on the owner worker, unless the client has gone. */
static void reply(Worker *w, ClientListReq *req) {
    ClientConn *client = req->client;
    if (client) {
        client->list_req = NULL;
        int len = 0;
        char *resp =
            req->oom ? NULL : wrap_text_reply(client->resp == 3, req->out.len ? req->out.buf : "", req->out.len, &len);
        if (!resp) {
            const char *e = "-ERR out of memory\r\n";
            resp = strdup(e);
            len = (int)strlen(e);
        }
        if (resp)
            fanout_send_response(w, client, resp, len);
        else
            close_client(w, client);
    }
    req_unref(req);
}

/* Adds w's lines (already in g) to req. The worker that adds the last
 * share replies, or hands the request back to the owner to reply. */
static void add_share(Worker *w, ClientListReq *req, GrowBuf *g, int failed) {
    pthread_mutex_lock(&req->lock);
    if (failed || (g->len && gb_append(&req->out, g->buf, g->len) < 0)) req->oom = 1;
    int last = --req->pending == 0;
    pthread_mutex_unlock(&req->lock);
    free(g->buf);
    if (!last) return;
    if (w == req->owner) {
        reply(w, req);
        return;
    }
    Worker *ow = req->owner;
    pthread_mutex_lock(&ow->list_lock);
    req->done_next = ow->list_done;
    ow->list_done = req;
    pthread_mutex_unlock(&ow->list_lock);
    wake(ow);
}

void client_list_service(Worker *w) {
    int n;
    ClientListReq **batch = inbox_take(w, &n);
    for (int i = 0; i < n; i++) {
        GrowBuf g = {0};
        int failed = format_worker_clients(w, &batch[i]->f, &g) < 0;
        add_share(w, batch[i], &g, failed);
        req_unref(batch[i]); /* the inbox's reference */
    }
    free(batch);
    for (ClientListReq *req = done_take(w), *next; req; req = next) {
        next = req->done_next;
        reply(w, req);
    }
}

/* Called when w's event loop exits: other workers stop handing it requests,
 * and the ones already in its inbox still get its share. */
void client_list_stop(Worker *w) {
    pthread_mutex_lock(&w->list_lock);
    atomic_store_explicit(&w->list_ready, 0, memory_order_relaxed);
    pthread_mutex_unlock(&w->list_lock);
    client_list_service(w);
}

void client_list_forget(ClientConn *client) {
    if (client->list_req) client->list_req->client = NULL;
    client->list_req = NULL;
}

void handle_client_list(Worker *w, ClientConn *client) {
    char err[256];
    ClientListReq *req = calloc(1, sizeof(*req));
    if (!req) {
        client_send_error(w, client, "out of memory");
        client->hbuf_len = 0;
        return;
    }
    if (parse_filter(client, &req->f, err, sizeof(err)) < 0) {
        client_send_error(w, client, err);
        client->hbuf_len = 0;
        free(req->f.ids);
        free(req);
        return;
    }
    pthread_mutex_init(&req->lock, NULL);
    req->owner = w;
    req->client = client;
    req->refs = 1;
    req->pending = 1; /* this worker's own share, added last */

    for (int i = 0; i < g_config.worker_thread_no; i++) {
        Worker *ow = &g_workers[i];
        if (ow == w || !atomic_load_explicit(&ow->list_ready, memory_order_acquire)) continue;
        pthread_mutex_lock(&req->lock);
        req->refs++;
        req->pending++;
        pthread_mutex_unlock(&req->lock);
        int rc = inbox_push(ow, req);
        if (rc) {
            pthread_mutex_lock(&req->lock);
            req->refs--;
            req->pending--;
            if (rc < 0) req->oom = 1;
            pthread_mutex_unlock(&req->lock);
            continue;
        }
        wake(ow);
    }

    /* Format this worker's clients before parking the caller, so its own
     * line shows it reading (events=r), as on Valkey. */
    GrowBuf g = {0};
    int failed = format_worker_clients(w, &req->f, &g) < 0;
    client->list_req = req;
    client->hbuf_len = 0;
    client->state = CS_WAITING_RESP;
    fd_unwatch(w, client->fd, EL_READABLE);
    add_share(w, req, &g, failed);
}
