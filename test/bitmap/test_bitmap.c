#include "test_common.h"

void run_bitmap_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Bitmap Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    /* ── SETBIT ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SETBIT", "tc:bm:setbit", "7", "1");
    CHECK(strcmp(r, ":0") == 0, "SETBIT: set bit 7 to 1, returns prev value 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SETBIT", "tc:bm:setbit", "7", "0");
    CHECK(strcmp(r, ":1") == 0, "SETBIT: set bit 7 to 0, returns prev value 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "SETBIT", "tc:bm:setbit", "0", "1");
    CHECK(strcmp(r, ":0") == 0, "SETBIT: set bit 0 to 1, returns prev value 0", pass, fail);

    /* ── GETBIT ──────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "SETBIT", "tc:bm:getbit", "3", "1");

    proxy_cmd(fd, r, sizeof(r), 3, "GETBIT", "tc:bm:getbit", "3");
    CHECK(strcmp(r, ":1") == 0, "GETBIT: get set bit returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "GETBIT", "tc:bm:getbit", "0");
    CHECK(strcmp(r, ":0") == 0, "GETBIT: get unset bit returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "GETBIT", "tc:bm:noexist", "0");
    CHECK(strcmp(r, ":0") == 0, "GETBIT: non-existent key returns 0", pass, fail);

    /* ── BITCOUNT ─────────────────────────────────────────────────────── */
    /* \xff\xf0\x00 → byte0=8 bits, byte1=4 bits, byte2=0 bits → total 12 */
    {
        const char *args[] = {"SET", "tc:bm:bitcount", "\xff\xf0\x00"};
        int lens[] = {3, 14, 3};
        resp_send_raw(fd, 3, args, lens);
        resp_next_str(fd, r, sizeof(r));
    }

    proxy_cmd(fd, r, sizeof(r), 4, "BITCOUNT", "tc:bm:bitcount", "0", "0");
    CHECK(strcmp(r, ":8") == 0, "BITCOUNT: byte 0 (0xff) has 8 set bits", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 4, "BITCOUNT", "tc:bm:bitcount", "1", "1");
    CHECK(strcmp(r, ":4") == 0, "BITCOUNT: byte 1 (0xf0) has 4 set bits", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 2, "BITCOUNT", "tc:bm:bitcount");
    CHECK(strcmp(r, ":12") == 0, "BITCOUNT: total 12 bits set across all bytes", pass, fail);

    /* ── BITPOS ───────────────────────────────────────────────────────── */
    /* \x00\xff → byte0=00000000, byte1=11111111 → first 1-bit at pos 8 */
    {
        const char *args[] = {"SET", "tc:bm:bitpos", "\x00\xff"};
        int lens[] = {3, 12, 2};
        resp_send_raw(fd, 3, args, lens);
        resp_next_str(fd, r, sizeof(r));
    }

    proxy_cmd(fd, r, sizeof(r), 3, "BITPOS", "tc:bm:bitpos", "1");
    CHECK(strcmp(r, ":8") == 0, "BITPOS: first 1-bit at position 8", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 3, "BITPOS", "tc:bm:bitpos", "0");
    CHECK(strcmp(r, ":0") == 0, "BITPOS: first 0-bit at position 0", pass, fail);

    /* ── BITFIELD ─────────────────────────────────────────────────────── */
    long long bfval = proxy_cmd_first_int(fd, 6, "BITFIELD", "tc:bm:bitfield", "SET", "u8", "0", "200");
    CHECK(bfval == 0, "BITFIELD SET u8 0 200: returns previous value 0", pass, fail);

    bfval = proxy_cmd_first_int(fd, 5, "BITFIELD", "tc:bm:bitfield", "GET", "u8", "0");
    CHECK(bfval == 200, "BITFIELD GET u8 0: returns stored value 200", pass, fail);

    bfval = proxy_cmd_first_int(fd, 6, "BITFIELD", "tc:bm:bitfield", "INCRBY", "u8", "0", "10");
    CHECK(bfval == 210, "BITFIELD INCRBY u8 0 10: 200 + 10 = 210", pass, fail);

    /* ── BITFIELD_RO ─────────────────────────────────────────────────── */
    bfval = proxy_cmd_first_int(fd, 5, "BITFIELD_RO", "tc:bm:bitfield", "GET", "u8", "0");
    CHECK(bfval == 210, "BITFIELD_RO GET u8 0: returns 210", pass, fail);

    /* ── BITOP ───────────────────────────────────────────────────────── */
    /* Use hash tag {tc:bm:bt} so all keys land on the same slot */
    /* {tc:bm:bt}a = \xff = 11111111 */
    {
        const char *args[] = {"SET", "{tc:bm:bt}a", "\xff"};
        int lens[] = {3, 11, 1};
        resp_send_raw(fd, 3, args, lens);
        resp_next_str(fd, r, sizeof(r));
    }
    /* {tc:bm:bt}b = \x0f = 00001111 */
    {
        const char *args[] = {"SET", "{tc:bm:bt}b", "\x0f"};
        int lens[] = {3, 11, 1};
        resp_send_raw(fd, 3, args, lens);
        resp_next_str(fd, r, sizeof(r));
    }

    /* AND: 0xff & 0x0f = 0x0f */
    proxy_cmd(fd, r, sizeof(r), 5, "BITOP", "AND", "{tc:bm:bt}dest", "{tc:bm:bt}a", "{tc:bm:bt}b");
    CHECK(strcmp(r, ":1") == 0, "BITOP AND: result key length is 1 byte", pass, fail);
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0x0f, "BITOP AND: 0xff & 0x0f = 0x0f", pass, fail);
    }

    /* OR: 0xff | 0x0f = 0xff */
    proxy_cmd(fd, r, sizeof(r), 5, "BITOP", "OR", "{tc:bm:bt}dest", "{tc:bm:bt}a", "{tc:bm:bt}b");
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0xff, "BITOP OR: 0xff | 0x0f = 0xff", pass, fail);
    }

    /* XOR: 0xff ^ 0x0f = 0xf0 */
    proxy_cmd(fd, r, sizeof(r), 5, "BITOP", "XOR", "{tc:bm:bt}dest", "{tc:bm:bt}a", "{tc:bm:bt}b");
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0xf0, "BITOP XOR: 0xff ^ 0x0f = 0xf0", pass, fail);
    }

    /* NOT: ~0x0f = 0xf0 */
    proxy_cmd(fd, r, sizeof(r), 4, "BITOP", "NOT", "{tc:bm:bt}dest", "{tc:bm:bt}b");
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0xf0, "BITOP NOT: ~0x0f = 0xf0", pass, fail);
    }

    /* ── BITOP with 3 source keys (same slot via hash tag) ──────────────
     * {tc:bm:bt3}a/b/c/dest all land on slot 1824.
     * a=0xff=11111111, b=0x0f=00001111, c=0x55=01010101
     */
    {
        const char *a[] = {"SET", "{tc:bm:bt3}a", "\xff"};
        int la[] = {3, 12, 1};
        resp_send_raw(fd, 3, a, la);
        resp_next_str(fd, r, sizeof(r));
    }
    {
        const char *a[] = {"SET", "{tc:bm:bt3}b", "\x0f"};
        int la[] = {3, 12, 1};
        resp_send_raw(fd, 3, a, la);
        resp_next_str(fd, r, sizeof(r));
    }
    {
        const char *a[] = {"SET", "{tc:bm:bt3}c", "\x55"};
        int la[] = {3, 12, 1};
        resp_send_raw(fd, 3, a, la);
        resp_next_str(fd, r, sizeof(r));
    }

    /* AND: 0xff & 0x0f & 0x55 = 0x05 */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "AND", "{tc:bm:bt3}dest", "{tc:bm:bt3}a", "{tc:bm:bt3}b", "{tc:bm:bt3}c");
    CHECK(strcmp(r, ":1") == 0, "BITOP AND 3-src same-slot: result length 1 byte", pass, fail);
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt3}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0x05, "BITOP AND 3-src: 0xff & 0x0f & 0x55 = 0x05", pass, fail);
    }

    /* OR: 0xff | 0x0f | 0x55 = 0xff */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "OR", "{tc:bm:bt3}dest", "{tc:bm:bt3}a", "{tc:bm:bt3}b", "{tc:bm:bt3}c");
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt3}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0xff, "BITOP OR 3-src: 0xff | 0x0f | 0x55 = 0xff", pass, fail);
    }

    /* XOR: 0xff ^ 0x0f ^ 0x55 = 0xa5 */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "XOR", "{tc:bm:bt3}dest", "{tc:bm:bt3}a", "{tc:bm:bt3}b", "{tc:bm:bt3}c");
    {
        char bin[16];
        resp_send(fd, 2, "GET", "{tc:bm:bt3}dest");
        int blen = resp_next_binary(fd, bin, sizeof(bin));
        CHECK(blen == 1 && (unsigned char)bin[0] == 0xa5, "BITOP XOR 3-src: 0xff ^ 0x0f ^ 0x55 = 0xa5", pass, fail);
    }

    /* ── BITOP cross-slot with 3 keys (should return CROSSSLOT error) ───
     * tc:cs:foo → slot 4418, tc:cs:bar → slot 11537, tc:cs:baz → slot 11289
     * All three are in different slots, so any BITOP across them must fail.
     */
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:foo", "\xff");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:bar", "\x0f");
    proxy_cmd(fd, r, sizeof(r), 3, "SET", "tc:cs:baz", "\x55");

    /* AND: dest=tc:cs:foo, 3 srcs across 3 different slots */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "AND", "tc:cs:foo", "tc:cs:foo", "tc:cs:bar", "tc:cs:baz");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "BITOP AND 3-src cross-slot: returns CROSSSLOT error", pass, fail);

    /* OR: 3 srcs across 3 different slots */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "OR", "tc:cs:foo", "tc:cs:foo", "tc:cs:bar", "tc:cs:baz");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "BITOP OR 3-src cross-slot: returns CROSSSLOT error", pass, fail);

    /* XOR: 3 srcs across 3 different slots */
    proxy_cmd(fd, r, sizeof(r), 6, "BITOP", "XOR", "tc:cs:foo", "tc:cs:foo", "tc:cs:bar", "tc:cs:baz");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "BITOP XOR 3-src cross-slot: returns CROSSSLOT error", pass, fail);

    /* NOT: dest and src in different slots (NOT only takes 1 src) */
    proxy_cmd(fd, r, sizeof(r), 4, "BITOP", "NOT", "tc:cs:foo", "tc:cs:bar");
    CHECK(strncmp(r, "-CROSSSLOT", 10) == 0, "BITOP NOT cross-slot: returns CROSSSLOT error", pass, fail);

    /* cleanup */
    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
