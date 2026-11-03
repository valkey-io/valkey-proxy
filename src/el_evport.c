/* Solaris event ports backend — included by event_loop.c, not compiled directly */
#include <port.h>
#include <poll.h>

typedef struct {
    int portfd;
    port_event_t *evbuf;
    int *masks; /* registered poll mask per fd, for re-association */
} EvportState;

static int el_api_create(EventLoop *el) {
    EvportState *s = calloc(1, sizeof(*s));
    if (!s) return -1;
    s->evbuf = malloc(sizeof(port_event_t) * el->max_events);
    s->masks = calloc(FD_SETSIZE, sizeof(int));
    if (!s->evbuf || !s->masks) {
        free(s->evbuf);
        free(s->masks);
        free(s);
        return -1;
    }
    s->portfd = port_create();
    if (s->portfd < 0) {
        free(s->evbuf);
        free(s->masks);
        free(s);
        return -1;
    }
    el->state = s;
    return 0;
}

static void el_api_free(EventLoop *el) {
    EvportState *s = el->state;
    close(s->portfd);
    free(s->evbuf);
    free(s->masks);
    free(s);
}

static int el_api_add(EventLoop *el, int fd, int mask) {
    EvportState *s = el->state;
    int events = 0;
    if (mask & EL_READABLE) events |= POLLIN;
    if (mask & EL_WRITABLE) events |= POLLOUT;
    if (fd < FD_SETSIZE) s->masks[fd] = events;
    return port_associate(s->portfd, PORT_SOURCE_FD, (uintptr_t)fd, events, (void *)(uintptr_t)fd);
}

static void el_api_del(EventLoop *el, int fd) {
    EvportState *s = el->state;
    if (fd < FD_SETSIZE) s->masks[fd] = 0;
    port_dissociate(s->portfd, PORT_SOURCE_FD, (uintptr_t)fd);
}

static int el_api_poll(EventLoop *el, int timeout_ms) {
    EvportState *s = el->state;
    uint_t nget = 1;
    struct timespec ts, *tsp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        tsp = &ts;
    }
    if (port_getn(s->portfd, s->evbuf, (uint_t)el->max_events, &nget, tsp) < 0)
        return (errno == ETIME || errno == EINTR) ? 0 : -1;
    int n = (int)nget;
    for (int i = 0; i < n; i++) {
        int fd = (int)(uintptr_t)s->evbuf[i].portev_user;
        el->fired[i].fd = fd;
        el->fired[i].mask = 0;
        if (s->evbuf[i].portev_events & POLLIN) el->fired[i].mask |= EL_READABLE;
        if (s->evbuf[i].portev_events & POLLOUT) el->fired[i].mask |= EL_WRITABLE;
        /* evport is oneshot — re-associate for next poll */
        if (fd < FD_SETSIZE && s->masks[fd])
            port_associate(s->portfd, PORT_SOURCE_FD, (uintptr_t)fd, s->masks[fd], (void *)(uintptr_t)fd);
    }
    return n;
}

static const char *el_api_name(void) {
    return "evport";
}
