#include "test_common.h"
#include <pthread.h>

/*
 * Pub/Sub integration tests.
 *
 * Because SUBSCRIBE blocks waiting for pushed messages, these tests use a
 * background pthread for the subscriber side while the main thread publishes.
 * A mutex + condvar pair synchronises the two phases:
 *   1. sub_done: subscriber confirmed the subscription is active
 *   2. msg_done: subscriber has received and stored the pushed message
 */

typedef struct {
    const char *host;
    int port;
    const char *channel;
    char received[256]; /* payload of the pushed message */
    int sub_done;       /* 1 once subscription is confirmed */
    int msg_done;       /* 1 once message payload is stored */
    pthread_mutex_t mu;
    pthread_cond_t cv;
} SubState;

typedef struct {
    const char *host;
    int port;
    const char *channel_a;
    const char *channel_b;
    char received_a[256];
    char received_b[256];
    int sub_count; /* incremented to 2 once both subs confirmed */
    int msg_count; /* incremented to 2 once both messages received */
    pthread_mutex_t mu;
    pthread_cond_t cv;
} SubStateMulti;

typedef struct {
    const char *host;
    int port;
    const char *pattern;
    char recv_type[64];     /* "pmessage" */
    char recv_pattern[128]; /* matched pattern */
    char recv_channel[128]; /* actual channel name */
    char recv_payload[256]; /* message payload */
    int sub_done;           /* 1 once psubscribe confirmed */
    int msg_done;           /* 1 once pmessage received */
    pthread_mutex_t mu;
    pthread_cond_t cv;
} PSubState;

typedef struct {
    const char *host;
    int port;
    const char *channel;
    char recv_type[64];     /* "smessage" */
    char recv_channel[128]; /* 频道名 */
    char recv_payload[256]; /* 消息内容 */
    int sub_done;
    int msg_done;
    pthread_mutex_t mu;
    pthread_cond_t cv;
} SSubState;

/* Read a full subscribe/unsubscribe *3 array from fd.
 * Populates type, channel, and count (as ":N" string). */
static void read_sub_reply(int fd, char *type, int typesz, char *chan, int chansz, char *count, int countsz) {
    char hdr[64];
    read_line(fd, hdr, sizeof(hdr)); /* *3 */
    resp_next_str(fd, type, typesz);
    resp_next_str(fd, chan, chansz);
    resp_next_str(fd, count, countsz);
}

/* Subscriber thread: subscribes to a channel, waits for the first pushed
 * message, stores the payload, then exits. */
static void *subscriber_fn(void *arg) {
    SubState *s = (SubState *)arg;

    int fd = proxy_connect(s->host, s->port);
    if (fd < 0) {
        snprintf(s->received, sizeof(s->received), "(connect-error)");
        pthread_mutex_lock(&s->mu);
        s->sub_done = s->msg_done = 1;
        pthread_cond_signal(&s->cv);
        pthread_mutex_unlock(&s->mu);
        return NULL;
    }

    /* Send SUBSCRIBE <channel> */
    {
        const char *args[] = {"SUBSCRIBE", s->channel};
        int arglens[] = {9, (int)strlen(s->channel)};
        resp_send_raw(fd, 2, args, arglens);
    }

    /* Read subscribe confirmation: *3  subscribe  <channel>  :1 */
    char type[64], chan[128], count[32];
    read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));

    /* Signal: subscription is active */
    pthread_mutex_lock(&s->mu);
    s->sub_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);

    /* Read pushed message: *3  message  <channel>  <payload> */
    char hdr[64];
    read_line(fd, hdr, sizeof(hdr));                     /* *3 */
    resp_next_str(fd, type, sizeof(type));               /* "message" */
    resp_next_str(fd, chan, sizeof(chan));               /* channel name */
    resp_next_str(fd, s->received, sizeof(s->received)); /* payload */

    close(fd);

    pthread_mutex_lock(&s->mu);
    s->msg_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);
    return NULL;
}

static void *subscriber_multi_fn(void *arg) {
    SubStateMulti *s = (SubStateMulti *)arg;
    int fd = proxy_connect(s->host, s->port);
    if (fd < 0) {
        pthread_mutex_lock(&s->mu);
        s->sub_count = 2;
        s->msg_count = 2;
        pthread_cond_broadcast(&s->cv);
        pthread_mutex_unlock(&s->mu);
        return NULL;
    }

    char type[64], chan[128], count[32];

    /* Subscribe to channel_a */
    {
        const char *a[] = {"SUBSCRIBE", s->channel_a};
        int al[] = {9, (int)strlen(s->channel_a)};
        resp_send_raw(fd, 2, a, al);
    }
    read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
    pthread_mutex_lock(&s->mu);
    s->sub_count++;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);

    /* Subscribe to channel_b */
    {
        const char *b[] = {"SUBSCRIBE", s->channel_b};
        int bl[] = {9, (int)strlen(s->channel_b)};
        resp_send_raw(fd, 2, b, bl);
    }
    read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
    pthread_mutex_lock(&s->mu);
    s->sub_count++;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);

    /* Receive 2 pushed messages; dispatch by channel name */
    for (int i = 0; i < 2; i++) {
        char hdr[64], mtype[64], mchan[128], payload[256];
        read_line(fd, hdr, sizeof(hdr));
        resp_next_str(fd, mtype, sizeof(mtype));
        resp_next_str(fd, mchan, sizeof(mchan));
        resp_next_str(fd, payload, sizeof(payload));
        pthread_mutex_lock(&s->mu);
        if (strcmp(mchan, s->channel_a) == 0)
            strncpy(s->received_a, payload, sizeof(s->received_a) - 1);
        else
            strncpy(s->received_b, payload, sizeof(s->received_b) - 1);
        s->msg_count++;
        pthread_cond_signal(&s->cv);
        pthread_mutex_unlock(&s->mu);
    }

    close(fd);
    return NULL;
}

/* Pattern subscriber thread: PSUBSCRIBEs a pattern, waits for the first pushed
 * pmessage (*4 array), stores type/pattern/channel/payload, then exits. */
static void *psubscriber_fn(void *arg) {
    PSubState *s = (PSubState *)arg;
    int fd = proxy_connect(s->host, s->port);
    if (fd < 0) {
        pthread_mutex_lock(&s->mu);
        s->sub_done = s->msg_done = 1;
        pthread_cond_signal(&s->cv);
        pthread_mutex_unlock(&s->mu);
        return NULL;
    }

    /* Send PSUBSCRIBE <pattern> */
    {
        const char *args[] = {"PSUBSCRIBE", s->pattern};
        int arglens[] = {10, (int)strlen(s->pattern)};
        resp_send_raw(fd, 2, args, arglens);
    }

    /* Read psubscribe confirmation: *3  psubscribe  <pattern>  :1 */
    char type[64], chan[128], count[32];
    read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));

    pthread_mutex_lock(&s->mu);
    s->sub_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);

    /* Read pushed pmessage: *4  pmessage  <pattern>  <channel>  <payload> */
    char hdr[64];
    read_line(fd, hdr, sizeof(hdr));
    resp_next_str(fd, s->recv_type, sizeof(s->recv_type));
    resp_next_str(fd, s->recv_pattern, sizeof(s->recv_pattern));
    resp_next_str(fd, s->recv_channel, sizeof(s->recv_channel));
    resp_next_str(fd, s->recv_payload, sizeof(s->recv_payload));

    close(fd);

    pthread_mutex_lock(&s->mu);
    s->msg_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);
    return NULL;
}

/* 分片订阅线程：SSUBSCRIBE 一个频道，等待 smessage (*3) 推送，存储各字段后退出。 */
static void *ssubscriber_fn(void *arg) {
    SSubState *s = (SSubState *)arg;
    int fd = proxy_connect(s->host, s->port);
    if (fd < 0) {
        pthread_mutex_lock(&s->mu);
        s->sub_done = s->msg_done = 1;
        pthread_cond_signal(&s->cv);
        pthread_mutex_unlock(&s->mu);
        return NULL;
    }

    /* 发送 SSUBSCRIBE <channel> */
    {
        const char *args[] = {"SSUBSCRIBE", s->channel};
        int arglens[] = {10, (int)strlen(s->channel)};
        resp_send_raw(fd, 2, args, arglens);
    }

    /* 读取 ssubscribe 确认帧：*3  ssubscribe  <channel>  :1 */
    char type[64], chan[128], count[32];
    read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));

    pthread_mutex_lock(&s->mu);
    s->sub_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);

    /* 读取推送的 smessage：*3  smessage  <channel>  <payload> */
    char hdr[64];
    read_line(fd, hdr, sizeof(hdr));
    resp_next_str(fd, s->recv_type, sizeof(s->recv_type));
    resp_next_str(fd, s->recv_channel, sizeof(s->recv_channel));
    resp_next_str(fd, s->recv_payload, sizeof(s->recv_payload));

    close(fd);

    pthread_mutex_lock(&s->mu);
    s->msg_done = 1;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->mu);
    return NULL;
}

void run_pubsub_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Pub/Sub Commands ===\n");

    /* ══════════════════════════════════════════════════════════════════════
     * Test 1: basic SUBSCRIBE / PUBLISH — subscriber receives the message
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- SUBSCRIBE / PUBLISH ---\n");
    {
        SubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:basic";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, subscriber_fn, &s);

        /* Wait until subscription is confirmed */
        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        /* Publish from a separate connection */
        int pub = proxy_connect(host, port);
        char r[64];
        if (pub >= 0) {
            proxy_cmd(pub, r, sizeof(r), 3, "PUBLISH", "ps:basic", "hello-world");
            close(pub);
        }

        /* Wait until message is received */
        pthread_mutex_lock(&s.mu);
        while (!s.msg_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);

        CHECK(strcmp(s.received, "hello-world") == 0, "SUBSCRIBE/PUBLISH: subscriber receives published message", pass,
              fail);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 2: PUBLISH with no subscribers returns integer 0
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBLISH (no subscribers) ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }
        char r[64];
        proxy_cmd(fd, r, sizeof(r), 3, "PUBLISH", "ps:nobody", "msg");
        CHECK(r[0] == ':', "PUBLISH to empty channel: returns integer reply", pass, fail);
        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 3: UNSUBSCRIBE releases connection; client returns to normal mode
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- UNSUBSCRIBE ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }

        /* Subscribe */
        {
            const char *args[] = {"SUBSCRIBE", "ps:unsub-ch"};
            int arglens[] = {9, 11};
            resp_send_raw(fd, 2, args, arglens);
        }
        char type[64], chan[128], count[32];
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(type, "subscribe") == 0, "SUBSCRIBE confirmation: type is 'subscribe'", pass, fail);
        CHECK(strcmp(count, ":1") == 0, "SUBSCRIBE confirmation: channel count is 1", pass, fail);

        /* Unsubscribe */
        {
            const char *args[] = {"UNSUBSCRIBE", "ps:unsub-ch"};
            int arglens[] = {11, 11};
            resp_send_raw(fd, 2, args, arglens);
        }
        char utype[64], uchan[128], ucount[32];
        read_sub_reply(fd, utype, sizeof(utype), uchan, sizeof(uchan), ucount, sizeof(ucount));
        CHECK(strcmp(utype, "unsubscribe") == 0, "UNSUBSCRIBE confirmation: type is 'unsubscribe'", pass, fail);
        CHECK(strcmp(ucount, ":0") == 0, "UNSUBSCRIBE confirmation: channel count reaches 0", pass, fail);

        /* After unsubscribe the proxy releases sub_conn; normal commands work */
        char r[64];
        proxy_cmd(fd, r, sizeof(r), 1, "PING");
        CHECK(strcmp(r, "+PONG") == 0, "After UNSUBSCRIBE: normal commands work (PING)", pass, fail);

        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 4: multiple channels — SUBSCRIBE two, UNSUBSCRIBE one at a time
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- SUBSCRIBE multiple channels ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }

        /* Subscribe to ch-a then ch-b */
        {
            const char *args[] = {"SUBSCRIBE", "ps:ch-a"};
            int arglens[] = {9, 7};
            resp_send_raw(fd, 2, args, arglens);
        }
        char type[64], chan[128], count[32];
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(count, ":1") == 0, "SUBSCRIBE ch-a: count is 1", pass, fail);

        {
            const char *args[] = {"SUBSCRIBE", "ps:ch-b"};
            int arglens[] = {9, 7};
            resp_send_raw(fd, 2, args, arglens);
        }
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(count, ":2") == 0, "SUBSCRIBE ch-b: count is 2", pass, fail);

        /* Unsubscribe ch-a (count drops to 1) */
        {
            const char *args[] = {"UNSUBSCRIBE", "ps:ch-a"};
            int arglens[] = {11, 7};
            resp_send_raw(fd, 2, args, arglens);
        }
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(count, ":1") == 0, "UNSUBSCRIBE ch-a: count drops to 1", pass, fail);

        /* Unsubscribe ch-b (count drops to 0, sub_conn released) */
        {
            const char *args[] = {"UNSUBSCRIBE", "ps:ch-b"};
            int arglens[] = {11, 7};
            resp_send_raw(fd, 2, args, arglens);
        }
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(count, ":0") == 0, "UNSUBSCRIBE ch-b: count drops to 0", pass, fail);

        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 5: PUBLISH to each of two subscribed channels; verify delivery
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBLISH to multiple subscribed channels ---\n");
    {
        SubStateMulti s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel_a = "ps:multi-a";
        s.channel_b = "ps:multi-b";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, subscriber_multi_fn, &s);

        /* Wait until both subscriptions are active */
        pthread_mutex_lock(&s.mu);
        while (s.sub_count < 2) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        /* Publish one message to each channel from a separate connection */
        int pub = proxy_connect(host, port);
        char r[64];
        if (pub >= 0) {
            proxy_cmd(pub, r, sizeof(r), 3, "PUBLISH", "ps:multi-a", "msg-for-a");
            proxy_cmd(pub, r, sizeof(r), 3, "PUBLISH", "ps:multi-b", "msg-for-b");
            close(pub);
        }

        /* Wait until both messages are received */
        pthread_mutex_lock(&s.mu);
        while (s.msg_count < 2) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);

        CHECK(strcmp(s.received_a, "msg-for-a") == 0, "PUBLISH multi-ch: message delivered to channel A", pass, fail);
        CHECK(strcmp(s.received_b, "msg-for-b") == 0, "PUBLISH multi-ch: message delivered to channel B", pass, fail);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 6: PSUBSCRIBE / PUBLISH — pattern subscriber receives pmessage
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PSUBSCRIBE / PUBLISH ---\n");
    {
        PSubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.pattern = "ps:pat:*";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, psubscriber_fn, &s);

        /* Wait until pattern subscription is confirmed */
        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        /* Publish to a channel matching the pattern from a separate connection */
        int pub = proxy_connect(host, port);
        char r[64];
        if (pub >= 0) {
            proxy_cmd(pub, r, sizeof(r), 3, "PUBLISH", "ps:pat:hello", "hello-world");
            close(pub);
        }

        /* Wait until pmessage is received */
        pthread_mutex_lock(&s.mu);
        while (!s.msg_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);

        CHECK(strcmp(s.recv_type, "pmessage") == 0, "PSUBSCRIBE/PUBLISH: pushed type is pmessage", pass, fail);
        CHECK(strcmp(s.recv_pattern, "ps:pat:*") == 0, "PSUBSCRIBE/PUBLISH: pushed pattern matches subscription", pass,
              fail);
        CHECK(strcmp(s.recv_channel, "ps:pat:hello") == 0,
              "PSUBSCRIBE/PUBLISH: pushed channel is the actual publish target", pass, fail);
        CHECK(strcmp(s.recv_payload, "hello-world") == 0, "PSUBSCRIBE/PUBLISH: subscriber receives published message",
              pass, fail);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 7: PUNSUBSCRIBE releases connection; client returns to normal mode
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUNSUBSCRIBE ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }

        /* Pattern subscribe */
        {
            const char *args[] = {"PSUBSCRIBE", "ps:pun:*"};
            int arglens[] = {10, 8};
            resp_send_raw(fd, 2, args, arglens);
        }
        char type[64], chan[128], count[32];
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(type, "psubscribe") == 0, "PSUBSCRIBE confirmation: type is 'psubscribe'", pass, fail);
        CHECK(strcmp(count, ":1") == 0, "PSUBSCRIBE confirmation: pattern count is 1", pass, fail);

        /* Pattern unsubscribe */
        {
            const char *args[] = {"PUNSUBSCRIBE", "ps:pun:*"};
            int arglens[] = {12, 8};
            resp_send_raw(fd, 2, args, arglens);
        }
        char utype[64], uchan[128], ucount[32];
        read_sub_reply(fd, utype, sizeof(utype), uchan, sizeof(uchan), ucount, sizeof(ucount));
        CHECK(strcmp(utype, "punsubscribe") == 0, "PUNSUBSCRIBE confirmation: type is 'punsubscribe'", pass, fail);
        CHECK(strcmp(ucount, ":0") == 0, "PUNSUBSCRIBE confirmation: pattern count reaches 0", pass, fail);

        /* After PUNSUBSCRIBE the proxy releases sub_conn; normal commands work */
        char r[64];
        proxy_cmd(fd, r, sizeof(r), 1, "PING");
        CHECK(strcmp(r, "+PONG") == 0, "After PUNSUBSCRIBE: normal commands work (PING)", pass, fail);

        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 8: SSUBSCRIBE / SPUBLISH — 分片订阅者收到 smessage
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- SSUBSCRIBE / SPUBLISH ---\n");
    {
        SSubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:shard:ch";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, ssubscriber_fn, &s);

        /* 等待分片订阅确认 */
        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        /* 从独立连接向该分片频道发布消息 */
        int pub = proxy_connect(host, port);
        char r[64];
        if (pub >= 0) {
            proxy_cmd(pub, r, sizeof(r), 3, "SPUBLISH", "ps:shard:ch", "hello-shard");
            close(pub);
        }

        /* 等待 smessage 收到 */
        pthread_mutex_lock(&s.mu);
        while (!s.msg_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);

        CHECK(strcmp(s.recv_type, "smessage") == 0, "SSUBSCRIBE/SPUBLISH: pushed type is smessage", pass, fail);
        CHECK(strcmp(s.recv_channel, "ps:shard:ch") == 0, "SSUBSCRIBE/SPUBLISH: pushed channel matches subscription",
              pass, fail);
        CHECK(strcmp(s.recv_payload, "hello-shard") == 0, "SSUBSCRIBE/SPUBLISH: subscriber receives published message",
              pass, fail);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 9: SUNSUBSCRIBE 释放连接；客户端恢复普通模式
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- SUNSUBSCRIBE ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }

        /* 分片订阅 */
        {
            const char *args[] = {"SSUBSCRIBE", "ps:sun:ch"};
            int arglens[] = {10, 9};
            resp_send_raw(fd, 2, args, arglens);
        }
        char type[64], chan[128], count[32];
        read_sub_reply(fd, type, sizeof(type), chan, sizeof(chan), count, sizeof(count));
        CHECK(strcmp(type, "ssubscribe") == 0, "SSUBSCRIBE confirmation: type is 'ssubscribe'", pass, fail);
        CHECK(strcmp(count, ":1") == 0, "SSUBSCRIBE confirmation: channel count is 1", pass, fail);

        /* 分片取消订阅 */
        {
            const char *args[] = {"SUNSUBSCRIBE", "ps:sun:ch"};
            int arglens[] = {12, 9};
            resp_send_raw(fd, 2, args, arglens);
        }
        char utype[64], uchan[128], ucount[32];
        read_sub_reply(fd, utype, sizeof(utype), uchan, sizeof(uchan), ucount, sizeof(ucount));
        CHECK(strcmp(utype, "sunsubscribe") == 0, "SUNSUBSCRIBE confirmation: type is 'sunsubscribe'", pass, fail);
        CHECK(strcmp(ucount, ":0") == 0, "SUNSUBSCRIBE confirmation: channel count reaches 0", pass, fail);

        /* SUNSUBSCRIBE 后代理释放 sub_conn；普通命令应恢复正常 */
        char r[64];
        proxy_cmd(fd, r, sizeof(r), 1, "PING");
        CHECK(strcmp(r, "+PONG") == 0, "After SUNSUBSCRIBE: normal commands work (PING)", pass, fail);

        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Helper state for introspection tests: subscriber that stays connected
     * ══════════════════════════════════════════════════════════════════════ */

    /* ══════════════════════════════════════════════════════════════════════
     * Test 10: PUBSUB HELP — returns array response from one node
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB HELP ---\n");
    {
        int fd = proxy_connect(host, port);
        if (fd < 0) {
            (*fail)++;
            return;
        }
        char hdr[64];
        proxy_cmd(fd, hdr, sizeof(hdr), 2, "PUBSUB", "HELP");
        CHECK(hdr[0] == '*', "PUBSUB HELP: returns array response", pass, fail);
        int cnt = atoi(hdr + 1);
        char tmp[256];
        for (int i = 0; i < cnt; i++) resp_next_str(fd, tmp, sizeof(tmp));
        close(fd);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 11: PUBSUB CHANNELS — fan-out to all nodes, deduplicated result
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB CHANNELS ---\n");
    {
        /* Establish a subscription that stays alive while we query */
        SubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:inspect:ch";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, subscriber_fn, &s);

        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        /* Query PUBSUB CHANNELS from a separate connection */
        int fd = proxy_connect(host, port);
        if (fd >= 0) {
            char hdr[64];
            proxy_cmd(fd, hdr, sizeof(hdr), 2, "PUBSUB", "CHANNELS");
            CHECK(hdr[0] == '*', "PUBSUB CHANNELS: returns array header", pass, fail);
            int cnt = atoi(hdr + 1);
            CHECK(cnt >= 1, "PUBSUB CHANNELS: at least one channel listed", pass, fail);
            int found = resp_array_contains(fd, cnt, "ps:inspect:ch");
            CHECK(found, "PUBSUB CHANNELS: subscribed channel is present in result", pass, fail);
            close(fd);
        }

        /* Release the subscriber by publishing so it exits cleanly */
        int pub = proxy_connect(host, port);
        char r2[64];
        if (pub >= 0) {
            proxy_cmd(pub, r2, sizeof(r2), 3, "PUBLISH", "ps:inspect:ch", "done");
            close(pub);
        }
        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 12: PUBSUB NUMPAT — fan-out to all nodes, sum pattern counts
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB NUMPAT ---\n");
    {
        PSubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.pattern = "ps:numpat:*";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, psubscriber_fn, &s);

        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        int fd = proxy_connect(host, port);
        if (fd >= 0) {
            char r[64];
            proxy_cmd(fd, r, sizeof(r), 2, "PUBSUB", "NUMPAT");
            CHECK(r[0] == ':', "PUBSUB NUMPAT: returns integer reply", pass, fail);
            long long pat_count = atoll(r + 1);
            CHECK(pat_count >= 1, "PUBSUB NUMPAT: at least 1 pattern active", pass, fail);
            close(fd);
        }

        /* Publish to make psubscriber exit */
        int pub = proxy_connect(host, port);
        char r2[64];
        if (pub >= 0) {
            proxy_cmd(pub, r2, sizeof(r2), 3, "PUBLISH", "ps:numpat:x", "done");
            close(pub);
        }
        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 13: PUBSUB NUMSUB — no args returns empty; with args sums counts
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB NUMSUB ---\n");
    {
        SubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:numsub:ch";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, subscriber_fn, &s);

        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        int fd = proxy_connect(host, port);
        if (fd >= 0) {
            /* No args → empty array */
            char hdr[64];
            proxy_cmd(fd, hdr, sizeof(hdr), 2, "PUBSUB", "NUMSUB");
            CHECK(strcmp(hdr, "*0") == 0, "PUBSUB NUMSUB (no args): returns empty array", pass, fail);

            /* With channel arg → channel + count pairs */
            proxy_cmd(fd, hdr, sizeof(hdr), 3, "PUBSUB", "NUMSUB", "ps:numsub:ch");
            CHECK(hdr[0] == '*' && atoi(hdr + 1) == 2, "PUBSUB NUMSUB ch: response has 2 elements (channel + count)",
                  pass, fail);
            char ch_name[128], count_str[32];
            resp_next_str(fd, ch_name, sizeof(ch_name));
            resp_next_str(fd, count_str, sizeof(count_str));
            CHECK(strcmp(ch_name, "ps:numsub:ch") == 0, "PUBSUB NUMSUB ch: response includes channel name", pass, fail);
            CHECK(count_str[0] == ':' && atoll(count_str + 1) >= 1, "PUBSUB NUMSUB ch: subscriber count >= 1", pass,
                  fail);
            close(fd);
        }

        int pub = proxy_connect(host, port);
        char r2[64];
        if (pub >= 0) {
            proxy_cmd(pub, r2, sizeof(r2), 3, "PUBLISH", "ps:numsub:ch", "done");
            close(pub);
        }
        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 14: PUBSUB SHARDCHANNELS — fan-out result from SSUBSCRIBE nodes
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB SHARDCHANNELS ---\n");
    {
        SSubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:shard:inspect";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, ssubscriber_fn, &s);

        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        int fd = proxy_connect(host, port);
        if (fd >= 0) {
            char hdr[64];
            proxy_cmd(fd, hdr, sizeof(hdr), 2, "PUBSUB", "SHARDCHANNELS");
            CHECK(hdr[0] == '*', "PUBSUB SHARDCHANNELS: returns array header", pass, fail);
            int cnt = atoi(hdr + 1);
            CHECK(cnt >= 1, "PUBSUB SHARDCHANNELS: at least one shard channel listed", pass, fail);
            int found = resp_array_contains(fd, cnt, "ps:shard:inspect");
            CHECK(found, "PUBSUB SHARDCHANNELS: subscribed shard channel is present", pass, fail);
            close(fd);
        }

        int pub = proxy_connect(host, port);
        char r2[64];
        if (pub >= 0) {
            proxy_cmd(pub, r2, sizeof(r2), 3, "SPUBLISH", "ps:shard:inspect", "done");
            close(pub);
        }
        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);
    }

    /* ══════════════════════════════════════════════════════════════════════
     * Test 15: PUBSUB SHARDNUMSUB — no args returns empty; with args sums
     * ══════════════════════════════════════════════════════════════════════ */
    printf("\n--- PUBSUB SHARDNUMSUB ---\n");
    {
        SSubState s;
        memset(&s, 0, sizeof(s));
        s.host = host;
        s.port = port;
        s.channel = "ps:shard:numsub";
        pthread_mutex_init(&s.mu, NULL);
        pthread_cond_init(&s.cv, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, ssubscriber_fn, &s);

        pthread_mutex_lock(&s.mu);
        while (!s.sub_done) pthread_cond_wait(&s.cv, &s.mu);
        pthread_mutex_unlock(&s.mu);

        int fd = proxy_connect(host, port);
        if (fd >= 0) {
            /* No args → empty array */
            char hdr[64];
            proxy_cmd(fd, hdr, sizeof(hdr), 2, "PUBSUB", "SHARDNUMSUB");
            CHECK(strcmp(hdr, "*0") == 0, "PUBSUB SHARDNUMSUB (no args): returns empty array", pass, fail);

            /* With channel arg */
            proxy_cmd(fd, hdr, sizeof(hdr), 3, "PUBSUB", "SHARDNUMSUB", "ps:shard:numsub");
            CHECK(hdr[0] == '*' && atoi(hdr + 1) == 2, "PUBSUB SHARDNUMSUB ch: response has 2 elements", pass, fail);
            char ch_name[128], count_str[32];
            resp_next_str(fd, ch_name, sizeof(ch_name));
            resp_next_str(fd, count_str, sizeof(count_str));
            CHECK(strcmp(ch_name, "ps:shard:numsub") == 0, "PUBSUB SHARDNUMSUB ch: response includes channel name",
                  pass, fail);
            CHECK(count_str[0] == ':' && atoll(count_str + 1) >= 1, "PUBSUB SHARDNUMSUB ch: subscriber count >= 1",
                  pass, fail);
            close(fd);
        }

        int pub = proxy_connect(host, port);
        char r2[64];
        if (pub >= 0) {
            proxy_cmd(pub, r2, sizeof(r2), 3, "SPUBLISH", "ps:shard:numsub", "done");
            close(pub);
        }
        pthread_join(tid, NULL);
        pthread_mutex_destroy(&s.mu);
        pthread_cond_destroy(&s.cv);
    }
}
