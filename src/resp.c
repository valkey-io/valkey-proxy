#include "proxy.h"
#include <stdarg.h>

/* ── RESP parsing ───────────────────────────────────────────────────── */

/*
 * Parse a RESP command header from buf[0..len-1].
 * Reads: *argc\r\n + arg[0] (cmd) fully + arg[1] (key) fully +
 *        only the $len\r\n headers (not data) for arg[2..argc-1].
 * Returns bytes consumed on success (>0), 0 if more data needed, -1 on error.
 * Sets cmd, key, argc_out, total_cmd_len_out (complete command byte count).
 */
int resp_parse_header(const char *buf, int len, char *cmd, int *cmdlen_out, char *key, int *keylen_out, int *argc_out,
                      int *total_cmd_len_out) {
    *cmdlen_out = 0;
    *keylen_out = 0;
    *argc_out = 0;
    *total_cmd_len_out = 0;
    if (len < 4 || buf[0] != '*') return (buf[0] != '*') ? -1 : 0;
    const char *eol = memchr(buf + 1, '\n', len - 1);
    if (!eol) return 0;
    int argc = atoi(buf + 1);
    if (argc <= 0) return -1;
    *argc_out = argc;
    int off = (int)(eol - buf) + 1;
    long long total = off;

    for (int i = 0; i < argc; i++) {
        if (off >= len) return 0;
        if (buf[off] != '$') return -1;
        const char *p2 = buf + off + 1;
        const char *eol2 = memchr(p2, '\n', len - off - 1);
        if (!eol2) return 0;
        int arglen = atoi(p2);
        if (arglen < 0) return -1;
        int hdr_end = (int)(eol2 - buf) + 1;
        total += (hdr_end - off) + arglen + 2;

        if (i == 0) {
            if (hdr_end + arglen + 2 > len) return 0;
            *cmdlen_out = arglen < 63 ? arglen : 63;
            memcpy(cmd, buf + hdr_end, *cmdlen_out);
            cmd[*cmdlen_out] = '\0';
            off = hdr_end + arglen + 2;
        } else if (i == 1) {
            if (hdr_end + arglen + 2 > len) return 0;
            *keylen_out = arglen < 511 ? arglen : 511;
            memcpy(key, buf + hdr_end, *keylen_out);
            key[*keylen_out] = '\0';
            off = hdr_end + arglen + 2;
        } else {
            off = hdr_end + arglen + 2;
        }
    }
    *total_cmd_len_out = (int)total;
    return off;
}

/* Returns total bytes of a complete RESP response; 0=incomplete; -1=error.
 * Handles both RESP2 (+ - : $ *) and RESP3 (# , _ ( = % ~ >) type markers --
 * a connection that has completed a "HELLO 3" handshake with the backend
 * (see src/hello.c) may return any of the latter regardless of which
 * client's command produced the reply, so this function must be able to at
 * least frame them correctly even where no client ever asked for RESP3. */
int resp_response_len(const char *buf, int len) {
    if (len < 1) return 0;
    switch (buf[0]) {
        /* Line-terminated scalars carrying no separate length field: simple
     * string, error, integer (RESP2), and boolean/double/null/big-number
     * (RESP3) -- all of these are just "read up to the next \n". */
        case '+':
        case '-':
        case ':':
        case '#':
        case ',':
        case '_':
        case '(': {
            void *nl = memchr(buf, '\n', len);
            return nl ? (int)((char *)nl - buf) + 1 : 0;
        }
        /* Bulk string (RESP2) and verbatim string (RESP3): identical framing,
     * a byte-count header line followed by that many bytes + CRLF. */
        case '$':
        case '=': {
            void *nl = memchr(buf + 1, '\n', len - 1);
            if (!nl) return 0;
            int bl = atoi(buf + 1);
            int hdr = (int)((char *)nl - buf) + 1;
            if (bl < 0) return hdr;
            return (len >= hdr + bl + 2) ? hdr + bl + 2 : 0;
        }
        /* Array (RESP2), and set/push/map (RESP3): an element-count header line
     * followed by that many (map: *2x* that many, since each entry is a
     * key+value pair) recursively-framed sub-elements. */
        case '*':
        case '~':
        case '>':
        case '%': {
            void *nl = memchr(buf + 1, '\n', len - 1);
            if (!nl) return 0;
            int cnt = atoi(buf + 1);
            int off = (int)((char *)nl - buf) + 1;
            if (cnt < 0) return off;
            if (buf[0] == '%') cnt *= 2;
            for (int i = 0; i < cnt; i++) {
                if (off >= len) return 0;
                int el = resp_response_len(buf + off, len - off);
                if (el <= 0) return el;
                off += el;
            }
            return off;
        }
        default: return -1;
    }
}

/* ── RESP3 → RESP2 downgrade (see resp3_downgrade()/resp3_downgrade_push()
 * in proxy.h) ──────────────────────────────────────────────────────────
 * Purely type-marker-driven and command-agnostic: map→array(2x)/
 * set→array/push→array are header-only rewrites, boolean/double/verbatim/
 * big-number become their nearest RESP2 scalar, and null becomes a nil bulk
 * string except at whichever recursion depth (if any) is semantically a nil
 * *array* for the command in question (see cmd_resp2_nil_array_depth() in
 * cmd.c). A small number of commands change nesting depth in a way no type
 * marker can reveal at all (WITHSCORES/WITHVALUES-family, XREAD/XREADGROUP)
 * -- those need the separate, command-name-driven resp3_flatten_pairs()/
 * resp3_nest_pairs() fixups below, applied *after* this generic pass. */
static int gb_ensure(GrowBuf *g, int need) {
    if (g->cap - g->len >= need) return 0;
    int nc = g->cap ? g->cap * 2 : 256;
    while (nc - g->len < need) nc *= 2;
    char *tmp = realloc(g->buf, (size_t)nc);
    if (!tmp) return -1;
    g->buf = tmp;
    g->cap = nc;
    return 0;
}
int gb_append(GrowBuf *g, const char *data, int len) {
    if (gb_ensure(g, len) < 0) return -1;
    memcpy(g->buf + g->len, data, (size_t)len);
    g->len += len;
    return 0;
}
/* Formats straight into g's spare capacity; only when that's too small does
 * it grow the buffer and format a second time. */
int gb_append_fmt(GrowBuf *g, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = g->buf ? vsnprintf(g->buf + g->len, (size_t)(g->cap - g->len), fmt, ap) : vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) return -1;
    if (g->buf && n < g->cap - g->len) {
        g->len += n;
        return 0;
    }
    if (gb_ensure(g, n + 1) < 0) return -1; /* +1: vsnprintf's NUL */
    va_start(ap, fmt);
    vsnprintf(g->buf + g->len, (size_t)(g->cap - g->len), fmt, ap);
    va_end(ap);
    g->len += n;
    return 0;
}

/* Converts one complete RESP element at buf[0..len) into its RESP2
 * equivalent, appended to g. nil_array_depth selects which recursion depth
 * (0 = the outermost element itself, 1 = an immediate child of it, etc; -1
 * = never) a bare `_` becomes `*-1\r\n` (nil array) instead of the default
 * `$-1\r\n` (nil bulk) -- see cmd_resp2_nil_array_depth() in cmd.c. Most
 * commands only ever need depth 0 (their own top-level reply is nil-array-
 * shaped, e.g. a blocking command's timeout) or -1 (never, the common
 * case), but GEOPOS is a real counter-example: it never returns a bare nil
 * itself, but *each individual member's slot* inside its outer array is a
 * nil array (not nil bulk) when that member doesn't exist -- i.e. depth 1,
 * not depth 0. Returns bytes consumed (>0), or -1 if buf isn't valid/
 * complete RESP. */
static int resp3_downgrade_elem(const char *buf, int len, int nil_array_depth, int depth, GrowBuf *g) {
    if (len < 1) return -1;
    switch (buf[0]) {
        case '+':
        case '-':
        case ':': {
            void *nl = memchr(buf, '\n', len);
            if (!nl) return -1;
            int elen = (int)((char *)nl - buf) + 1;
            return gb_append(g, buf, elen) < 0 ? -1 : elen;
        }
        case '$': {
            void *nl = memchr(buf + 1, '\n', len - 1);
            if (!nl) return -1;
            int hdr = (int)((char *)nl - buf) + 1;
            int bl = atoi(buf + 1);
            int elen = (bl < 0) ? hdr : hdr + bl + 2;
            if (elen > len) return -1;
            return gb_append(g, buf, elen) < 0 ? -1 : elen;
        }
        case '_': {
            void *nl = memchr(buf, '\n', len);
            if (!nl) return -1;
            int elen = (int)((char *)nl - buf) + 1;
            const char *nilrepr = (depth == nil_array_depth) ? "*-1\r\n" : "$-1\r\n";
            return gb_append(g, nilrepr, 5) < 0 ? -1 : elen;
        }
        case '#': {
            void *nl = memchr(buf, '\n', len);
            if (!nl) return -1;
            int elen = (int)((char *)nl - buf) + 1;
            if (elen < 4) return -1;
            int val = (buf[1] == 't');
            return gb_append_fmt(g, ":%d\r\n", val) < 0 ? -1 : elen;
        }
        case ',':
        case '(': {
            void *nl = memchr(buf, '\n', len);
            if (!nl) return -1;
            int elen = (int)((char *)nl - buf) + 1;
            int textlen = elen - 1 - 2; /* minus the type marker, minus the CRLF */
            if (textlen < 0) return -1;
            if (gb_append_fmt(g, "$%d\r\n", textlen) < 0) return -1;
            if (gb_append(g, buf + 1, textlen) < 0) return -1;
            return gb_append(g, "\r\n", 2) < 0 ? -1 : elen;
        }
        case '=': {
            void *nl = memchr(buf + 1, '\n', len - 1);
            if (!nl) return -1;
            int hdr = (int)((char *)nl - buf) + 1;
            int bl = atoi(buf + 1);
            if (bl < 4) return -1; /* must at least carry the "txt:"/"mkd:" prefix */
            int elen = hdr + bl + 2;
            if (elen > len) return -1;
            int newlen = bl - 4;
            if (gb_append_fmt(g, "$%d\r\n", newlen) < 0) return -1;
            if (gb_append(g, buf + hdr + 4, newlen) < 0) return -1;
            return gb_append(g, "\r\n", 2) < 0 ? -1 : elen;
        }
        case '*':
        case '~':
        case '>':
        case '%': {
            void *nl = memchr(buf + 1, '\n', len - 1);
            if (!nl) return -1;
            int hdr = (int)((char *)nl - buf) + 1;
            int cnt = atoi(buf + 1);
            if (cnt < 0) return gb_append(g, "*-1\r\n", 5) < 0 ? -1 : hdr;
            int nelem = (buf[0] == '%') ? cnt * 2 : cnt;
            if (gb_append_fmt(g, "*%d\r\n", nelem) < 0) return -1;
            int off = hdr;
            for (int i = 0; i < nelem; i++) {
                /* One level deeper than this container itself -- keeps
             * nil_array_depth==1 (GEOPOS's case) matching only these direct
             * children, not this container nor anything nested further in. */
                int el = resp3_downgrade_elem(buf + off, len - off, nil_array_depth, depth + 1, g);
                if (el <= 0) return -1;
                off += el;
            }
            return off;
        }
        default: return -1;
    }
}

int resp3_downgrade(const char *buf, int len, int nil_array_depth, char **out, int *out_len) {
    GrowBuf g = {0};
    int consumed = resp3_downgrade_elem(buf, len, nil_array_depth, 0, &g);
    if (consumed <= 0 || consumed != len) {
        free(g.buf);
        return -1;
    }
    *out = g.buf;
    *out_len = g.len;
    return 0;
}

int resp3_downgrade_push(const char *buf, int len, char **out, int *out_len) {
    /* The shared '*'/'~'/'>'/'%' case above already turns a top-level '>'
     * into '*' by itself; nil_array_depth is irrelevant to a push message
     * (there's no such thing as a nil push). */
    return resp3_downgrade(buf, len, -1, out, out_len);
}

/* Some commands (ZRANGE/ZPOPMIN/HRANDFIELD and their WITHSCORES/WITHVALUES-
 * family relatives) return, under RESP3, an array of 2-element sub-arrays
 * (pairs) where RESP2 returns the same information flattened into one array
 * twice as long -- a pure nesting-depth difference with no distinguishing
 * type marker at all, so resp3_downgrade() above has no way to detect or
 * undo it (see cmd_resp3_pair_transform() in cmd.c for the command list).
 * Operates on an already-fully-RESP2-shaped buffer (i.e. call this *after*
 * resp3_downgrade(), not instead of it) and verifies the shape before
 * touching anything: a command in that list whose reply *isn't* actually
 * pair-shaped (WITHSCORES/WITHVALUES wasn't given this time, or the backend
 * was never upgraded to begin with) is safe to pass through this function
 * unchanged -- so callers don't need to track whether the triggering
 * command actually included that option.
 * Returns 0 (flattened; out/out_len set, caller frees), 1 (buf isn't in
 * the pair shape -- caller keeps using the original buffer, out/out_len
 * untouched), or -1 (buf is a malformed RESP array -- shouldn't happen for
 * output that just came out of resp3_downgrade(), but handled defensively;
 * caller should fall back to the original, pre-transform buffer rather than
 * treat this as a hard error). */
int resp3_flatten_pairs(const char *buf, int len, char **out, int *out_len) {
    if (len < 4 || buf[0] != '*') return 1;
    void *nl = memchr(buf + 1, '\n', len - 1);
    if (!nl) return 1;
    int hdr = (int)((char *)nl - buf) + 1;
    int n = atoi(buf + 1);
    if (n <= 0) return 1;

    /* First pass: verify every one of the n top-level elements is itself a
     * plain 2-element array, without emitting anything yet. */
    int off = hdr;
    for (int i = 0; i < n; i++) {
        if (off >= len || buf[off] != '*') return 1;
        void *nl2 = memchr(buf + off + 1, '\n', len - off - 1);
        if (!nl2) return -1;
        if (atoi(buf + off + 1) != 2) return 1;
        int hdr2 = (int)((char *)nl2 - buf) + 1;
        int e1 = resp_response_len(buf + hdr2, len - hdr2);
        if (e1 <= 0) return -1;
        int e2 = resp_response_len(buf + hdr2 + e1, len - hdr2 - e1);
        if (e2 <= 0) return -1;
        off = hdr2 + e1 + e2;
    }
    if (off != len) return -1;

    /* Second pass: emit "*2n" then each pair's two leaf elements back to
     * back, dropping the "*2\r\n" sub-headers this time. */
    GrowBuf g = {0};
    if (gb_append_fmt(&g, "*%d\r\n", n * 2) < 0) {
        free(g.buf);
        return -1;
    }
    off = hdr;
    for (int i = 0; i < n; i++) {
        void *nl2 = memchr(buf + off + 1, '\n', len - off - 1);
        int hdr2 = (int)((char *)nl2 - buf) + 1;
        int e1 = resp_response_len(buf + hdr2, len - hdr2);
        int e2 = resp_response_len(buf + hdr2 + e1, len - hdr2 - e1);
        if (gb_append(&g, buf + hdr2, e1 + e2) < 0) {
            free(g.buf);
            return -1;
        }
        off = hdr2 + e1 + e2;
    }
    *out = g.buf;
    *out_len = g.len;
    return 0;
}

/* The inverse restructuring, needed by XREAD/XREADGROUP: their reply is a
 * map under RESP3 (stream name -> entries), which resp3_downgrade() already
 * flattens into a plain "*2n" array (name1, entries1, name2, entries2, ...)
 * -- correct per the generic map-is-a-flat-array-of-pairs rule that holds
 * for e.g. HGETALL/CONFIG GET, but *not* what a genuine RESP2 backend
 * actually sends for these two commands, which nest each [name, entries]
 * pair in its own sub-array instead. Same safety property as
 * resp3_flatten_pairs(): an odd-length or non-array buffer just isn't our
 * shape, and is returned as "not applicable" rather than forced. Return
 * convention identical to resp3_flatten_pairs(). */
int resp3_nest_pairs(const char *buf, int len, char **out, int *out_len) {
    if (len < 4 || buf[0] != '*') return 1;
    void *nl = memchr(buf + 1, '\n', len - 1);
    if (!nl) return 1;
    int hdr = (int)((char *)nl - buf) + 1;
    int n = atoi(buf + 1);
    if (n <= 0 || (n % 2) != 0) return 1;

    GrowBuf g = {0};
    if (gb_append_fmt(&g, "*%d\r\n", n / 2) < 0) {
        free(g.buf);
        return -1;
    }
    int off = hdr;
    for (int i = 0; i < n / 2; i++) {
        int e1 = resp_response_len(buf + off, len - off);
        if (e1 <= 0) {
            free(g.buf);
            return -1;
        }
        int e2 = resp_response_len(buf + off + e1, len - off - e1);
        if (e2 <= 0) {
            free(g.buf);
            return -1;
        }
        if (gb_append_fmt(&g, "*2\r\n") < 0 || gb_append(&g, buf + off, e1 + e2) < 0) {
            free(g.buf);
            return -1;
        }
        off += e1 + e2;
    }
    if (off != len) {
        free(g.buf);
        return -1;
    }
    *out = g.buf;
    *out_len = g.len;
    return 0;
}

/* Extract the Nth argument (0=command, 1=first arg, 2=second arg...) from a
   partially- or fully-buffered RESP command. */
int resp_get_nth_arg(const char *buf, int len, int n, const char **out, int *out_len) {
    if (len < 4 || buf[0] != '*') return 0;
    const char *eol = memchr(buf + 1, '\n', len - 1);
    if (!eol) return 0;
    int argc = atoi(buf + 1);
    int off = (int)(eol - buf) + 1;
    for (int i = 0; i < argc; i++) {
        if (off >= len || buf[off] != '$') return 0;
        const char *p2 = buf + off + 1;
        const char *eol2 = memchr(p2, '\n', len - off - 1);
        if (!eol2) return 0;
        int arglen = atoi(p2);
        if (arglen < 0) return 0;
        int data_start = (int)(eol2 - buf) + 1;
        if (data_start + arglen + 2 > len) return 0;
        if (i == n) {
            *out = buf + data_start;
            *out_len = arglen;
            return 1;
        }
        off = data_start + arglen + 2;
    }
    return 0;
}

/*
 * Extract key pointers from a fully-buffered RESP command.
 * key_ptrs[i] points into buf (not null-terminated, valid while buf lives).
 * Returns number of keys found, -1 on parse error.
 */
int resp_extract_keys(const char *buf, int len, const char *cmd, const char **key_ptrs, int *key_lens, int max_keys) {
    if (len < 4 || buf[0] != '*') return -1;
    const char *eol = memchr(buf + 1, '\n', len - 1);
    if (!eol) return -1;
    int argc = atoi(buf + 1);
    if (argc < 1) return 0;

    const char *ap[512];
    int al[512];
    int nargs = 0;
    int off = (int)(eol - buf) + 1;
    for (int i = 0; i < argc; i++) {
        if (off >= len || buf[off] != '$') return -1;
        const char *p2 = buf + off + 1;
        const char *eol2 = memchr(p2, '\n', len - off - 1);
        if (!eol2) return -1;
        int arglen = atoi(p2);
        if (arglen < 0) return -1;
        int data_start = (int)(eol2 - buf) + 1;
        if (data_start + arglen + 2 > len) return -1;
        if (i > 0 && nargs < 512) {
            ap[nargs] = buf + data_start;
            al[nargs] = arglen;
            nargs++;
        }
        off = data_start + arglen + 2;
    }

    int nkeys = 0;
    if (!strcmp(cmd, "BITOP")) {
        for (int i = 1; i < nargs && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "COPY") || !strcmp(cmd, "RENAME") || !strcmp(cmd, "RENAMENX") || !strcmp(cmd, "LMOVE") ||
               !strcmp(cmd, "RPOPLPUSH") || !strcmp(cmd, "SMOVE") || !strcmp(cmd, "ZRANGESTORE")) {
        int cnt = nargs < 2 ? nargs : 2;
        for (int i = 0; i < cnt && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "MSET") || !strcmp(cmd, "MSETNX")) {
        for (int i = 0; i < nargs && nkeys < max_keys; i += 2) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "MSETEX")) {
        for (int i = 0; i < nargs && nkeys < max_keys; i += 3) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "JSON.MSET")) {
        for (int i = 0; i < nargs && nkeys < max_keys; i += 3) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "JSON.MGET")) {
        for (int i = 0; i < nargs - 1 && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "LCS")) {
        int cnt = nargs < 2 ? nargs : 2;
        for (int i = 0; i < cnt && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "GEOSEARCHSTORE")) {
        int cnt = nargs < 2 ? nargs : 2;
        for (int i = 0; i < cnt && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "LMPOP") || !strcmp(cmd, "SINTERCARD") || !strcmp(cmd, "ZDIFF") || !strcmp(cmd, "ZINTER") ||
               !strcmp(cmd, "ZINTERCARD") || !strcmp(cmd, "ZMPOP") || !strcmp(cmd, "ZUNION")) {
        if (nargs >= 2) {
            int numkeys = atoi(ap[0]);
            for (int i = 1; i <= numkeys && i < nargs && nkeys < max_keys; i++) {
                key_ptrs[nkeys] = ap[i];
                key_lens[nkeys] = al[i];
                nkeys++;
            }
        }
    } else if (!strcmp(cmd, "EVAL") || !strcmp(cmd, "EVAL_RO") || !strcmp(cmd, "EVALSHA") ||
               !strcmp(cmd, "EVALSHA_RO") || !strcmp(cmd, "FCALL") || !strcmp(cmd, "FCALL_RO")) {
        if (nargs >= 2) {
            int numkeys = atoi(ap[1]);
            for (int i = 0; i < numkeys && 2 + i < nargs && nkeys < max_keys; i++) {
                key_ptrs[nkeys] = ap[2 + i];
                key_lens[nkeys] = al[2 + i];
                nkeys++;
            }
        }
    } else if (!strcmp(cmd, "ZDIFFSTORE") || !strcmp(cmd, "ZINTERSTORE") || !strcmp(cmd, "ZUNIONSTORE")) {
        if (nargs >= 3) {
            key_ptrs[nkeys] = ap[0];
            key_lens[nkeys] = al[0];
            nkeys++;
            int numkeys = atoi(ap[1]);
            for (int i = 2; i <= numkeys + 1 && i < nargs && nkeys < max_keys; i++) {
                key_ptrs[nkeys] = ap[i];
                key_lens[nkeys] = al[i];
                nkeys++;
            }
        }
    } else if (!strcmp(cmd, "GEORADIUS") || !strcmp(cmd, "GEORADIUSBYMEMBER")) {
        if (nargs >= 1) {
            key_ptrs[nkeys] = ap[0];
            key_lens[nkeys] = al[0];
            nkeys++;
        }
        for (int i = 1; i < nargs - 1 && nkeys < max_keys; i++) {
            if ((al[i] == 5 && strncasecmp(ap[i], "STORE", 5) == 0) ||
                (al[i] == 9 && strncasecmp(ap[i], "STOREDIST", 9) == 0)) {
                key_ptrs[nkeys] = ap[i + 1];
                key_lens[nkeys] = al[i + 1];
                nkeys++;
                i++;
            }
        }
    } else if (!strcmp(cmd, "XREAD") || !strcmp(cmd, "XREADGROUP")) {
        int streams_idx = -1;
        for (int i = 0; i < nargs; i++) {
            if (al[i] == 7 && strncasecmp(ap[i], "STREAMS", 7) == 0) {
                streams_idx = i;
                break;
            }
        }
        if (streams_idx >= 0) {
            int after = nargs - streams_idx - 1;
            int nk = after / 2;
            for (int i = 0; i < nk && nkeys < max_keys; i++) {
                key_ptrs[nkeys] = ap[streams_idx + 1 + i];
                key_lens[nkeys] = al[streams_idx + 1 + i];
                nkeys++;
            }
        }
    } else {
        for (int i = 0; i < nargs && nkeys < max_keys; i++) {
            key_ptrs[nkeys] = ap[i];
            key_lens[nkeys] = al[i];
            nkeys++;
        }
    }
    return nkeys;
}

/* Extract all args after the command from a fully-buffered RESP command. */
int resp_extract_all_args(const char *buf, int len, const char **args, int *arglens, int max) {
    if (len < 4 || buf[0] != '*') return -1;
    const char *eol = memchr(buf + 1, '\n', len - 1);
    if (!eol) return -1;
    int argc = atoi(buf + 1);
    int off = (int)(eol - buf) + 1, n = 0;
    for (int i = 0; i < argc; i++) {
        if (off >= len || buf[off] != '$') return -1;
        const char *p2 = buf + off + 1;
        const char *eol2 = memchr(p2, '\n', len - off - 1);
        if (!eol2) return -1;
        int arglen = atoi(p2);
        if (arglen < 0) return -1;
        int ds = (int)(eol2 - buf) + 1;
        if (ds + arglen + 2 > len) return -1;
        if (i > 0 && n < max) {
            args[n] = buf + ds;
            arglens[n] = arglen;
            n++;
        }
        off = ds + arglen + 2;
    }
    return n;
}

int resp_extract_blocking_keys(const char *buf, int len, const char *cmd, const char **kp, int *kl, int max_keys) {
    const char *ap[MAX_FANOUT_KEYS * 3 + 4];
    int al[MAX_FANOUT_KEYS * 3 + 4];
    int nargs = resp_extract_all_args(buf, len, ap, al, (int)(sizeof(ap) / sizeof(ap[0])));
    int nkeys = 0;

    if (!strcmp(cmd, "BRPOP") || !strcmp(cmd, "BLPOP") || !strcmp(cmd, "BZPOPMIN") || !strcmp(cmd, "BZPOPMAX")) {
        for (int i = 0; i < nargs - 1 && nkeys < max_keys; i++) {
            kp[nkeys] = ap[i];
            kl[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "BLMOVE") || !strcmp(cmd, "BRPOPLPUSH")) {
        int cnt = nargs < 2 ? nargs : 2;
        for (int i = 0; i < cnt && nkeys < max_keys; i++) {
            kp[nkeys] = ap[i];
            kl[nkeys] = al[i];
            nkeys++;
        }
    } else if (!strcmp(cmd, "BLMPOP") || !strcmp(cmd, "BZMPOP")) {
        if (nargs >= 2) {
            int numkeys = atoi(ap[1]);
            for (int i = 0; i < numkeys && 2 + i < nargs && nkeys < max_keys; i++) {
                kp[nkeys] = ap[2 + i];
                kl[nkeys] = al[2 + i];
                nkeys++;
            }
        }
    }
    return nkeys;
}

/* Returns 1 if the fully-buffered XREAD/XREADGROUP command contains BLOCK. */
int resp_xread_has_block(const char *buf, int len) {
    const char *ap[64];
    int al[64];
    int nargs = resp_extract_all_args(buf, len, ap, al, (int)(sizeof(ap) / sizeof(ap[0])));
    for (int i = 0; i < nargs; i++) {
        if (al[i] == 5 && strncasecmp(ap[i], "BLOCK", 5) == 0) return 1;
    }
    return 0;
}

/* Build a RESP array command: *N+1\r\n$cmdlen\r\ncmd\r\n + nargs bulk strings. */
char *build_resp_cmd(const char *cmd, int cmdlen, const char **args, const int *arglens, int nargs, int *out_len) {
    char tmp[32];
    int total = 0;
    total += snprintf(tmp, sizeof(tmp), "*%d\r\n$%d\r\n", nargs + 1, cmdlen) + cmdlen + 2;
    for (int i = 0; i < nargs; i++) total += snprintf(tmp, sizeof(tmp), "$%d\r\n", arglens[i]) + arglens[i] + 2;

    char *buf = malloc((size_t)total + 1);
    if (!buf) return NULL;
    int off = snprintf(buf, (size_t)total + 1, "*%d\r\n$%d\r\n", nargs + 1, cmdlen);
    memcpy(buf + off, cmd, (size_t)cmdlen);
    off += cmdlen;
    buf[off++] = '\r';
    buf[off++] = '\n';
    for (int i = 0; i < nargs; i++) {
        off += snprintf(buf + off, (size_t)(total - off + 1), "$%d\r\n", arglens[i]);
        memcpy(buf + off, args[i], (size_t)arglens[i]);
        off += arglens[i];
        buf[off++] = '\r';
        buf[off++] = '\n';
    }
    *out_len = off;
    return buf;
}
