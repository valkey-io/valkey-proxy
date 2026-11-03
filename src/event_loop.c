#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "event_loop.h"

/* Select the platform backend at compile time. */
#if defined(__linux__)
#include "el_epoll.c"
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include "el_kqueue.c"
#elif defined(__sun)
#include "el_evport.c"
#else
#include "el_select.c"
#endif

EventLoop *el_create(int max_events) {
    EventLoop *el = calloc(1, sizeof(*el));
    if (!el) return NULL;
    el->max_events = max_events;
    el->fired = calloc((size_t)max_events, sizeof(ElFired));
    if (!el->fired || el_api_create(el) < 0) {
        free(el->fired);
        free(el);
        return NULL;
    }
    return el;
}

void el_destroy(EventLoop *el) {
    el_api_free(el);
    free(el->fired);
    free(el);
}

int el_add(EventLoop *el, int fd, int mask) {
    return el_api_add(el, fd, mask);
}

void el_del(EventLoop *el, int fd) {
    el_api_del(el, fd);
}

int el_poll(EventLoop *el, int timeout_ms) {
    el->nfired = el_api_poll(el, timeout_ms);
    return el->nfired;
}

const char *el_backend_name(void) {
    return el_api_name();
}
