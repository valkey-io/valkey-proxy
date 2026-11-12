#include "proxy.h"

#include <strings.h>

/* ── ACL CAT [category] ─────────────────────────────────────────────────
 * Answered by the proxy itself from a built-in table, never forwarded: each
 * ACL category with the commands in it, limited to the ones this proxy
 * supports (the same "only what the proxy supports" view COMMAND LIST/DOCS/
 * COUNT give). The table lives in acl_cat_data.h, generated from a real
 * Valkey node by tools/gen_acl_cat.py -- re-run it after adding a command
 * to docs/COMMANDS.md. Since no backend sees the command, a user's own ACL
 * permissions don't apply to it. */

typedef struct {
    const char *name;
    const char **cmds;
} AclCategory;

#include "acl_cat_data.h"

/* client->hbuf holds the whole command (see need_whole in client.c);
 * arg 1 is the "CAT" subcommand itself. */
void handle_acl_cat(Worker *w, ClientConn *client) {
    int argc = client->hbuf_len > 1 && client->hbuf[0] == '*' ? atoi(client->hbuf + 1) : 0;

    if (argc == 2) {
        const char *names[sizeof(ACL_CATEGORIES) / sizeof(ACL_CATEGORIES[0])];
        int n = 0;
        for (; ACL_CATEGORIES[n].name; n++) names[n] = ACL_CATEGORIES[n].name;
        send_multibulk_reply(w, client, names, n);
        return;
    }

    const char *arg;
    int arglen;
    if (argc != 3 || !resp_get_nth_arg(client->hbuf, client->hbuf_len, 2, &arg, &arglen)) {
        /* Valkey's own wording, quoting the subcommand as the client sent it. */
        const char *sub = "CAT";
        int sublen = 3;
        resp_get_nth_arg(client->hbuf, client->hbuf_len, 1, &sub, &sublen);
        char msg[160];
        snprintf(msg, sizeof(msg), "unknown subcommand or wrong number of arguments for '%.*s'. Try ACL HELP.", sublen,
                 sub);
        client_send_error(w, client, msg);
        client->hbuf_len = 0;
        return;
    }

    for (int i = 0; ACL_CATEGORIES[i].name; i++) {
        const char *name = ACL_CATEGORIES[i].name;
        if ((int)strlen(name) != arglen || strncasecmp(name, arg, (size_t)arglen) != 0) continue;
        const char **cmds = ACL_CATEGORIES[i].cmds;
        int n = 0;
        while (cmds[n]) n++;
        send_multibulk_reply(w, client, cmds, n);
        return;
    }

    char msg[200];
    snprintf(msg, sizeof(msg), "Unknown category '%.*s'", arglen > 128 ? 128 : arglen, arg);
    client_send_error(w, client, msg);
    client->hbuf_len = 0;
}
