/* Generic select(2) fallback — included by event_loop.c, not compiled directly */
#include <sys/select.h>

typedef struct {
    fd_set rfds, wfds;
    int maxfd;
} SelectState;

static int el_api_create(EventLoop *el) {
    SelectState *s = calloc(1, sizeof(*s));
    if (!s) return -1;
    s->maxfd = -1;
    el->state = s;
    return 0;
}

static void el_api_free(EventLoop *el) {
    free(el->state);
}

static int el_api_add(EventLoop *el, int fd, int mask) {
    SelectState *s = el->state;
    if (fd >= FD_SETSIZE) return -1;
    if (mask & EL_READABLE) FD_SET(fd, &s->rfds);
    if (mask & EL_WRITABLE) FD_SET(fd, &s->wfds);
    if (fd > s->maxfd) s->maxfd = fd;
    return 0;
}

static void el_api_del(EventLoop *el, int fd) {
    SelectState *s = el->state;
    FD_CLR(fd, &s->rfds);
    FD_CLR(fd, &s->wfds);
}

static int el_api_poll(EventLoop *el, int timeout_ms) {
    SelectState *s = el->state;
    fd_set rfds = s->rfds, wfds = s->wfds;
    struct timeval tv, *tvp = NULL;
    if (timeout_ms >= 0) {
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        tvp = &tv;
    }
    int ret = select(s->maxfd + 1, &rfds, &wfds, NULL, tvp);
    if (ret <= 0) return (ret < 0 && errno != EINTR) ? -1 : 0;
    int nfired = 0;
    for (int fd = 0; fd <= s->maxfd && nfired < el->max_events; fd++) {
        int mask = 0;
        if (FD_ISSET(fd, &rfds)) mask |= EL_READABLE;
        if (FD_ISSET(fd, &wfds)) mask |= EL_WRITABLE;
        if (mask) {
            el->fired[nfired].fd = fd;
            el->fired[nfired].mask = mask;
            nfired++;
        }
    }
    return nfired;
}

static const char *el_api_name(void) {
    return "select";
}
