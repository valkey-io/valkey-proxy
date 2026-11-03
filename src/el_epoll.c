/* Linux epoll backend — included by event_loop.c, not compiled directly */
#include <sys/epoll.h>

typedef struct {
    int epfd;
    struct epoll_event *evbuf;
} EpollState;

static int el_api_create(EventLoop *el) {
    EpollState *s = malloc(sizeof(*s));
    if (!s) return -1;
    s->evbuf = malloc(sizeof(struct epoll_event) * el->max_events);
    if (!s->evbuf) {
        free(s);
        return -1;
    }
    s->epfd = epoll_create1(0);
    if (s->epfd < 0) {
        free(s->evbuf);
        free(s);
        return -1;
    }
    el->state = s;
    return 0;
}

static void el_api_free(EventLoop *el) {
    EpollState *s = el->state;
    close(s->epfd);
    free(s->evbuf);
    free(s);
}

static int el_api_add(EventLoop *el, int fd, int mask) {
    EpollState *s = el->state;
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLET;
    if (mask & EL_READABLE) ev.events |= EPOLLIN;
    if (mask & EL_WRITABLE) ev.events |= EPOLLOUT;
    if (epoll_ctl(s->epfd, EPOLL_CTL_MOD, fd, &ev) < 0) return epoll_ctl(s->epfd, EPOLL_CTL_ADD, fd, &ev);
    return 0;
}

static void el_api_del(EventLoop *el, int fd) {
    EpollState *s = el->state;
    epoll_ctl(s->epfd, EPOLL_CTL_DEL, fd, NULL);
}

static int el_api_poll(EventLoop *el, int timeout_ms) {
    EpollState *s = el->state;
    int n = epoll_wait(s->epfd, s->evbuf, el->max_events, timeout_ms);
    if (n < 0) return (errno == EINTR) ? 0 : -1;
    for (int i = 0; i < n; i++) {
        uint32_t e = s->evbuf[i].events;
        el->fired[i].fd = s->evbuf[i].data.fd;
        el->fired[i].mask = 0;
        if (e & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) el->fired[i].mask |= EL_READABLE;
        if (e & (EPOLLOUT | EPOLLERR | EPOLLHUP)) el->fired[i].mask |= EL_WRITABLE;
    }
    return n;
}

static const char *el_api_name(void) {
    return "epoll";
}
