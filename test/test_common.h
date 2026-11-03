#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ── low-level I/O ─────────────────────────────────────────────────────── */

static int read_line(int fd, char *buf, int size) {
    int i = 0;
    char c;
    while (i < size - 1) {
        if (read(fd, &c, 1) <= 0) return -1;
        if (c == '\n') {
            if (i > 0 && buf[i - 1] == '\r') i--;
            buf[i] = '\0';
            return i;
        }
        buf[i++] = c;
    }
    buf[i] = '\0';
    return i;
}

static int read_exact(int fd, char *buf, int n) {
    int total = 0;
    while (total < n) {
        int r = (int)read(fd, buf + total, n - total);
        if (r <= 0) return -1;
        total += r;
    }
    return total;
}

/* ── RESP builder & sender ─────────────────────────────────────────────── */

static int resp_send_raw(int fd, int argc, const char **args, const int *arglens) {
    char buf[131072];
    int off = snprintf(buf, sizeof(buf), "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        off += snprintf(buf + off, sizeof(buf) - off, "$%d\r\n", arglens[i]);
        if (off + arglens[i] + 2 >= (int)sizeof(buf)) return -1;
        memcpy(buf + off, args[i], arglens[i]);
        off += arglens[i];
        buf[off++] = '\r';
        buf[off++] = '\n';
    }
    int sent = 0;
    while (sent < off) {
        int n = (int)write(fd, buf + sent, off - sent);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static __attribute__((unused)) int resp_send(int fd, int argc, ...) {
    const char *args[64];
    int arglens[64];
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc && i < 64; i++) {
        args[i] = va_arg(ap, const char *);
        arglens[i] = (int)strlen(args[i]);
    }
    va_end(ap);
    return resp_send_raw(fd, argc, args, arglens);
}

/* ── RESP reader ───────────────────────────────────────────────────────── */

/*
 * Read one RESP value as text into out. Returns out or NULL on error.
 *   +  → "+OK"
 *   -  → "-ERR ..."
 *   :  → ":42"
 *   *  → "*N"  (array header only; caller reads elements)
 *   $  → bulk string content, or "(nil)" for $-1
 */
static char *resp_next_str(int fd, char *out, int outsz) {
    char line[1024];
    if (read_line(fd, line, sizeof(line)) < 0) {
        snprintf(out, outsz, "(read-error)");
        return NULL;
    }
    char t = line[0];
    if (t == '+' || t == '-' || t == ':' || t == '*') {
        snprintf(out, outsz, "%s", line);
        return out;
    }
    if (t == '$') {
        int n = atoi(line + 1);
        if (n < 0) {
            snprintf(out, outsz, "(nil)");
            return out;
        }
        int r = (n < outsz - 1) ? n : outsz - 1;
        if (read_exact(fd, out, r) < 0) return NULL;
        out[r] = '\0';
        /* discard overflow bytes if value was truncated */
        if (n > r) {
            char tmp[256];
            int rem = n - r;
            while (rem > 0) {
                int chunk = rem < (int)sizeof(tmp) ? rem : (int)sizeof(tmp);
                read_exact(fd, tmp, chunk);
                rem -= chunk;
            }
        }
        char crlf[2];
        read_exact(fd, crlf, 2);
        return out;
    }
    snprintf(out, outsz, "(unknown-type)");
    return NULL;
}

/* Read a $ bulk string as raw binary. Returns byte count, -1 on error/nil. */
static __attribute__((unused)) int resp_next_binary(int fd, char *out, int outsz) {
    char line[64];
    if (read_line(fd, line, sizeof(line)) < 0) return -1;
    if (line[0] != '$') return -1;
    int n = atoi(line + 1);
    if (n < 0) return -1;
    int r = (n < outsz) ? n : outsz;
    if (read_exact(fd, out, r) < 0) return -1;
    if (n > r) {
        char tmp[256];
        int rem = n - r;
        while (rem > 0) {
            int c = rem < (int)sizeof(tmp) ? rem : (int)sizeof(tmp);
            read_exact(fd, tmp, c);
            rem -= c;
        }
    }
    char crlf[2];
    read_exact(fd, crlf, 2);
    return r;
}

/* Read a * array and check whether it contains target string. Drains all elements.
 * Elements may be bulk or simple strings (e.g. FT._LIST replies with +name). */
static __attribute__((unused)) int resp_array_contains(int fd, int count, const char *target) {
    int found = 0;
    char tmp[512];
    for (int i = 0; i < count; i++) {
        resp_next_str(fd, tmp, sizeof(tmp));
        if (strcmp(tmp, target) == 0 || (tmp[0] == '+' && strcmp(tmp + 1, target) == 0)) found = 1;
    }
    return found;
}

/* Recursively read and discard one full RESP reply (needed for nested
 * arrays, e.g. MODULE LIST's per-module [name, ver] sub-arrays). */
static __attribute__((unused)) int resp_skip_reply(int fd) {
    char line[1024];
    if (read_line(fd, line, sizeof(line)) < 0) return -1;
    if (line[0] == '*') {
        int n = atoi(line + 1);
        for (int i = 0; i < n; i++)
            if (resp_skip_reply(fd) < 0) return -1;
        return 0;
    }
    if (line[0] == '$') {
        int n = atoi(line + 1);
        if (n < 0) return 0;
        char tmp[1024];
        int rem = n;
        while (rem > 0) {
            int c = rem < (int)sizeof(tmp) ? rem : (int)sizeof(tmp);
            if (read_exact(fd, tmp, c) < 0) return -1;
            rem -= c;
        }
        char crlf[2];
        return read_exact(fd, crlf, 2);
    }
    return 0; /* +, -, : lines are already fully consumed by read_line */
}

/* ── RESP3-aware raw reader ────────────────────────────────────────────── */

/*
 * Reads one complete RESP reply of *any* type -- RESP2 (+ - : $ *) or RESP3
 * (# , _ ( = % ~ >) -- verbatim (type marker, every header line, and every
 * nested element's own CRLF included) into buf. Unlike resp_next_str()
 * above, which only understands the five RESP2 markers and is meant for
 * semantic assertions ("did I get back the string OK?"), this is for
 * RESP3 tests that need to inspect the actual *wire-level* type marker a
 * reply used (e.g. distinguishing a native RESP3 map '%' from a proxy-
 * downgraded RESP2 array '*' for the same HGETALL). Returns the byte count
 * written to buf, or -1 on a read error, unknown/malformed type, or if buf
 * is too small. Null-terminates the bytes it wrote (when there's room) so
 * callers can use strcmp()/strstr() directly, the same as resp_next_str().
 */
static int read_raw_reply_impl(int fd, char *buf, int bufsz);
static int read_raw_reply(int fd, char *buf, int bufsz) {
    int n = read_raw_reply_impl(fd, buf, bufsz > 0 ? bufsz - 1 : 0);
    if (n >= 0 && n < bufsz) buf[n] = '\0';
    return n;
}
static int read_raw_reply_impl(int fd, char *buf, int bufsz) {
    char line[1024];
    if (read_line(fd, line, sizeof(line)) < 0) return -1;
    int linelen = (int)strlen(line);
    if (linelen + 2 >= bufsz) return -1;
    int off = 0;
    memcpy(buf + off, line, (size_t)linelen);
    off += linelen;
    buf[off++] = '\r';
    buf[off++] = '\n';

    switch (line[0]) {
        case '+':
        case '-':
        case ':':
        case '#':
        case ',':
        case '_':
        case '(': return off; /* line-only types: read_line already consumed it all */
        case '$':
        case '=': {
            int n = atoi(line + 1);
            if (n < 0) return off; /* nil bulk / nil verbatim (shouldn't happen) */
            if (off + n + 2 >= bufsz) return -1;
            if (read_exact(fd, buf + off, n) < 0) return -1;
            off += n;
            if (read_exact(fd, buf + off, 2) < 0) return -1;
            off += 2;
            return off;
        }
        case '*':
        case '~':
        case '>':
        case '%': {
            int cnt = atoi(line + 1);
            if (cnt < 0) return off; /* nil array */
            int nelem = (line[0] == '%') ? cnt * 2 : cnt;
            for (int i = 0; i < nelem; i++) {
                int n = read_raw_reply_impl(fd, buf + off, bufsz - off);
                if (n < 0) return -1;
                off += n;
            }
            return off;
        }
        default: return -1;
    }
}

/* Send command (varargs strings), read one full reply as raw bytes (see
 * read_raw_reply() above). Returns the byte count, or -1 on error. */
static __attribute__((unused)) int resp_send_raw_reply(int fd, char *out, int outsz, int argc, ...) {
    const char *args[64];
    int arglens[64];
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc && i < 64; i++) {
        args[i] = va_arg(ap, const char *);
        arglens[i] = (int)strlen(args[i]);
    }
    va_end(ap);
    if (resp_send_raw(fd, argc, args, arglens) < 0) return -1;
    return read_raw_reply(fd, out, outsz);
}

/* ── convenience wrappers ──────────────────────────────────────────────── */

/* Send command (varargs strings), read first response token as text. */
static __attribute__((unused)) char *proxy_cmd(int fd, char *out, int outsz, int argc, ...) {
    const char *args[64];
    int arglens[64];
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc && i < 64; i++) {
        args[i] = va_arg(ap, const char *);
        arglens[i] = (int)strlen(args[i]);
    }
    va_end(ap);
    if (resp_send_raw(fd, argc, args, arglens) < 0) {
        snprintf(out, outsz, "(send-error)");
        return NULL;
    }
    return resp_next_str(fd, out, outsz);
}

/*
 * Send command, expect *N integer array response (e.g. BITFIELD).
 * Returns the first element value, or -9999 on error.
 */
static __attribute__((unused)) long long proxy_cmd_first_int(int fd, int argc, ...) {
    const char *args[64];
    int arglens[64];
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc && i < 64; i++) {
        args[i] = va_arg(ap, const char *);
        arglens[i] = (int)strlen(args[i]);
    }
    va_end(ap);
    if (resp_send_raw(fd, argc, args, arglens) < 0) return -9999;

    char line[64];
    if (read_line(fd, line, sizeof(line)) < 0) return -9999;

    int n, start;
    if (line[0] == '*') {
        n = atoi(line + 1);
        if (n <= 0) return -9999;
        if (read_line(fd, line, sizeof(line)) < 0) return -9999;
        start = 1;
    } else {
        n = 1;
        start = 0;
    }

    if (line[0] != ':') return -9999;
    long long val = atoll(line + 1);

    /* drain remaining elements */
    char tmp[64];
    for (int i = start; i < n; i++) {
        if (read_line(fd, tmp, sizeof(tmp)) < 0) break;
        if (tmp[0] == '$') {
            int len = atoi(tmp + 1);
            if (len > 0) {
                char t2[1024];
                read_exact(fd, t2, len + 2);
            }
        }
    }
    return val;
}

/* ── connection ────────────────────────────────────────────────────────── */

static int proxy_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address: %s\n", host);
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

/* ── test framework ────────────────────────────────────────────────────── */

/* Temporary ACL users created by run-test's --cluster=<host:port> option
 * (see test/acl/acl_fixture.c); all NULL when it wasn't given, in which case
 * tests that need a real non-default user must SKIP. */
extern const char *g_test_acl_user, *g_test_acl_pass;
extern const char *g_test_acl_ro_user, *g_test_acl_ro_pass;

#define SKIP(label) printf("  [SKIP] %s (needs --cluster=<host:port>)\n", (label))

#define CHECK(cond, label, pass, fail)        \
    do {                                      \
        if (cond) {                           \
            printf("  [PASS] %s\n", (label)); \
            (*(pass))++;                      \
        } else {                              \
            printf("  [FAIL] %s\n", (label)); \
            (*(fail))++;                      \
        }                                     \
    } while (0)

#endif /* TEST_COMMON_H */
