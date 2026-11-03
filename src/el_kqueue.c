/* macOS / BSD kqueue backend — included by event_loop.c, not compiled directly */
#include <sys/event.h>
#include <sys/time.h>

typedef struct {
    int kqfd;
    struct kevent *evbuf;
} KqueueState;

static int el_api_create(EventLoop *el) {
    KqueueState *s = malloc(sizeof(*s));
    if (!s) return -1;
    s->evbuf = malloc(sizeof(struct kevent) * el->max_events);
    if (!s->evbuf) {
        free(s);
        return -1;
    }
    s->kqfd = kqueue();
    if (s->kqfd < 0) {
        free(s->evbuf);
        free(s);
        return -1;
    }
    el->state = s;
    return 0;
}

static void el_api_free(EventLoop *el) {
    KqueueState *s = el->state;
    close(s->kqfd);
    free(s->evbuf);
    free(s);
}

static int el_api_add(EventLoop *el, int fd, int mask) {
    KqueueState *s = el->state;
    struct kevent ke;
    if (mask & EL_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, NULL);
        kevent(s->kqfd, &ke, 1, NULL, 0, NULL);
    }
    if (mask & EL_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, NULL);
        kevent(s->kqfd, &ke, 1, NULL, 0, NULL);
    }
    return 0;
}

static void el_api_del(EventLoop *el, int fd) {
    KqueueState *s = el->state;
    struct kevent ke;
    EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    kevent(s->kqfd, &ke, 1, NULL, 0, NULL);
    EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    kevent(s->kqfd, &ke, 1, NULL, 0, NULL);
}

static int el_api_poll(EventLoop *el, int timeout_ms) {
    KqueueState *s = el->state;
    struct timespec ts, *tsp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        tsp = &ts;
    }
    int n = kevent(s->kqfd, NULL, 0, s->evbuf, el->max_events, tsp);
    if (n < 0) return (errno == EINTR) ? 0 : -1;
    for (int i = 0; i < n; i++) {
        el->fired[i].fd = (int)s->evbuf[i].ident;
        el->fired[i].mask = 0;
        if (s->evbuf[i].filter == EVFILT_READ) el->fired[i].mask |= EL_READABLE;
        if (s->evbuf[i].filter == EVFILT_WRITE) el->fired[i].mask |= EL_WRITABLE;
    }
    return n;
}

static const char *el_api_name(void) {
    return "kqueue";
}
