#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#define EL_READABLE 1
#define EL_WRITABLE 2

typedef struct {
    int fd;
    int mask;
} ElFired;

typedef struct EventLoop {
    int max_events;
    ElFired *fired; /* filled by el_poll(); size = max_events */
    int nfired;     /* number of events from last el_poll()   */
    void *state;    /* platform-specific backend state        */
} EventLoop;

EventLoop *el_create(int max_events);
void el_destroy(EventLoop *el);
int el_add(EventLoop *el, int fd, int mask);
void el_del(EventLoop *el, int fd);
int el_poll(EventLoop *el, int timeout_ms); /* returns nfired */
const char *el_backend_name(void);

#endif /* EVENT_LOOP_H */
