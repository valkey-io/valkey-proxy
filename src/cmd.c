#include "proxy.h"

const char *POOL_NAME[POOL_TYPE_COUNT] = {"normal", "blocking", "subscription", "transaction", "acl", "shared"};

const char *CMD_CLASS_NAME[] = {"SINGLE_KEY", "MULTI_KEY", "NO_KEY", "BLOCKING", "PUBSUB", "ACL"};

/* ── supported command lists ────────────────────────────────────────── */
static const char *NON_TXN_CMDS[] = {"ACL",
                                     "APPEND",
                                     "AUTH",
                                     "BF.ADD",
                                     "BF.CARD",
                                     "BF.EXISTS",
                                     "BF.INFO",
                                     "BF.INSERT",
                                     "BF.MADD",
                                     "BF.MEXISTS",
                                     "BF.RESERVE",
                                     "BITCOUNT",
                                     "BITFIELD",
                                     "BITFIELD_RO",
                                     "BITOP",
                                     "BITPOS",
                                     "BLMOVE",
                                     "BLMPOP",
                                     "BLPOP",
                                     "BRPOP",
                                     "BRPOPLPUSH",
                                     "BZMPOP",
                                     "BZPOPMAX",
                                     "BZPOPMIN",
                                     "CLIENT",
                                     "COMMAND",
                                     "COMMANDLOG",
                                     "COPY",
                                     "DBSIZE",
                                     "DECR",
                                     "DECRBY",
                                     "DEL",
                                     "DELEX",
                                     "DELIFEQ",
                                     "DUMP",
                                     "ECHO",
                                     "EVAL",
                                     "EVAL_RO",
                                     "EVALSHA",
                                     "EVALSHA_RO",
                                     "EXISTS",
                                     "EXPIRE",
                                     "EXPIREAT",
                                     "EXPIRETIME",
                                     "FCALL",
                                     "FCALL_RO",
                                     "FLUSHALL",
                                     "FLUSHDB",
                                     "FT._LIST",
                                     "FT.AGGREGATE",
                                     "FT.CREATE",
                                     "FT.DROPINDEX",
                                     "FT.INFO",
                                     "FT.SEARCH",
                                     "FUNCTION",
                                     "GEOADD",
                                     "GEODIST",
                                     "GEOHASH",
                                     "GEOPOS",
                                     "GEORADIUS",
                                     "GEORADIUSBYMEMBER",
                                     "GEORADIUSBYMEMBER_RO",
                                     "GEORADIUS_RO",
                                     "GEOSEARCH",
                                     "GEOSEARCHSTORE",
                                     "GET",
                                     "GETBIT",
                                     "GETDEL",
                                     "GETEX",
                                     "GETRANGE",
                                     "GETSET",
                                     "HDEL",
                                     "HELLO",
                                     "HEXISTS",
                                     "HEXPIRE",
                                     "HEXPIREAT",
                                     "HEXPIRETIME",
                                     "HGET",
                                     "HGETALL",
                                     "HGETDEL",
                                     "HGETEX",
                                     "HINCRBY",
                                     "HINCRBYFLOAT",
                                     "HKEYS",
                                     "HLEN",
                                     "HMGET",
                                     "HMSET",
                                     "HPERSIST",
                                     "HPEXPIRE",
                                     "HPEXPIREAT",
                                     "HPEXPIRETIME",
                                     "HPTTL",
                                     "HRANDFIELD",
                                     "HSCAN",
                                     "HSET",
                                     "HSETEX",
                                     "HSETNX",
                                     "HSTRLEN",
                                     "HTTL",
                                     "HVALS",
                                     "INCR",
                                     "INCRBY",
                                     "INCRBYFLOAT",
                                     "INFO",
                                     "JSON.ARRAPPEND",
                                     "JSON.ARRINDEX",
                                     "JSON.ARRINSERT",
                                     "JSON.ARRLEN",
                                     "JSON.ARRPOP",
                                     "JSON.ARRTRIM",
                                     "JSON.CLEAR",
                                     "JSON.DEL",
                                     "JSON.FORGET",
                                     "JSON.GET",
                                     "JSON.MGET",
                                     "JSON.MSET",
                                     "JSON.NUMINCRBY",
                                     "JSON.NUMMULTBY",
                                     "JSON.OBJKEYS",
                                     "JSON.OBJLEN",
                                     "JSON.RESP",
                                     "JSON.SET",
                                     "JSON.STRAPPEND",
                                     "JSON.STRLEN",
                                     "JSON.TOGGLE",
                                     "JSON.TYPE",
                                     "KEYS",
                                     "LCS",
                                     "LINDEX",
                                     "LINSERT",
                                     "LLEN",
                                     "LMOVE",
                                     "LMPOP",
                                     "LOLWUT",
                                     "LPOP",
                                     "LPOS",
                                     "LPUSH",
                                     "LPUSHX",
                                     "LRANGE",
                                     "LREM",
                                     "LSET",
                                     "LTRIM",
                                     "MEMORY",
                                     "MGET",
                                     "MODULE",
                                     "MSET",
                                     "MSETEX",
                                     "MSETNX",
                                     "OBJECT",
                                     "PERSIST",
                                     "PEXPIRE",
                                     "PEXPIREAT",
                                     "PEXPIRETIME",
                                     "PFADD",
                                     "PFCOUNT",
                                     "PFMERGE",
                                     "PING",
                                     "PROXY",
                                     "PSETEX",
                                     "PTTL",
                                     "PUBLISH",
                                     "PUBSUB",
                                     "RANDOMKEY",
                                     "RENAME",
                                     "RENAMENX",
                                     "RESTORE",
                                     "RPOP",
                                     "RPOPLPUSH",
                                     "RPUSH",
                                     "RPUSHX",
                                     "SADD",
                                     "SCARD",
                                     "SDIFF",
                                     "SDIFFSTORE",
                                     "SINTER",
                                     "SINTERCARD",
                                     "SINTERSTORE",
                                     "SISMEMBER",
                                     "SMEMBERS",
                                     "SMISMEMBER",
                                     "SMOVE",
                                     "SPOP",
                                     "SPUBLISH",
                                     "SRANDMEMBER",
                                     "SREM",
                                     "SSCAN",
                                     "SUNION",
                                     "SUNIONSTORE",
                                     "ZADD",
                                     "ZCARD",
                                     "ZCOUNT",
                                     "ZDIFF",
                                     "ZDIFFSTORE",
                                     "ZINCRBY",
                                     "ZINTER",
                                     "ZINTERCARD",
                                     "ZINTERSTORE",
                                     "ZLEXCOUNT",
                                     "ZMPOP",
                                     "ZMSCORE",
                                     "ZPOPMAX",
                                     "ZPOPMIN",
                                     "ZRANDMEMBER",
                                     "ZRANGE",
                                     "ZRANGEBYLEX",
                                     "ZRANGEBYSCORE",
                                     "ZRANGESTORE",
                                     "ZRANK",
                                     "ZREM",
                                     "ZREMRANGEBYLEX",
                                     "ZREMRANGEBYRANK",
                                     "ZREMRANGEBYSCORE",
                                     "ZREVRANGE",
                                     "ZREVRANGEBYLEX",
                                     "ZREVRANGEBYSCORE",
                                     "ZREVRANK",
                                     "ZSCAN",
                                     "ZSCORE",
                                     "ZUNION",
                                     "ZUNIONSTORE",
                                     "SCAN",
                                     "SCRIPT",
                                     "SELECT",
                                     "SET",
                                     "SETBIT",
                                     "SETEX",
                                     "SETNX",
                                     "SETRANGE",
                                     "SLOWLOG",
                                     "SORT",
                                     "SORT_RO",
                                     "STRLEN",
                                     "SUBSTR",
                                     "TIME",
                                     "TOUCH",
                                     "TTL",
                                     "TYPE",
                                     "UNLINK",
                                     "WAIT",
                                     "WAITAOF",
                                     "XACK",
                                     "XADD",
                                     "XAUTOCLAIM",
                                     "XCLAIM",
                                     "XDEL",
                                     "XGROUP",
                                     "XINFO",
                                     "XLEN",
                                     "XPENDING",
                                     "XRANGE",
                                     "XREAD",
                                     "XREADGROUP",
                                     "XREVRANGE",
                                     "XTRIM",
                                     NULL};
static const char *TXN_CMDS[] = {"DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH", NULL};
static const char *PUBSUB_CMDS[] = {"PSUBSCRIBE",   "PUNSUBSCRIBE", "SSUBSCRIBE", "SUBSCRIBE",
                                    "SUNSUBSCRIBE", "UNSUBSCRIBE",  NULL};

/* ── command routing ────────────────────────────────────────────────── */
typedef struct {
    const char *cmd;
    PoolType pool;
} CmdRoute;
static const CmdRoute CMD_ROUTE_TABLE[] = {
    {"ACL", POOL_ACL},
    {"AUTH", POOL_ACL},
    {"BLMOVE", POOL_BLOCKING},
    {"BLMPOP", POOL_BLOCKING},
    {"BLPOP", POOL_BLOCKING},
    {"BRPOP", POOL_BLOCKING},
    {"BRPOPLPUSH", POOL_BLOCKING},
    {"BZMPOP", POOL_BLOCKING},
    {"BZPOPMAX", POOL_BLOCKING},
    {"BZPOPMIN", POOL_BLOCKING},
    {"DISCARD", POOL_TRANSACTION},
    {"EXEC", POOL_TRANSACTION},
    {"MULTI", POOL_TRANSACTION},
    {"PSUBSCRIBE", POOL_SUBSCRIPTION},
    {"PUNSUBSCRIBE", POOL_SUBSCRIPTION},
    {"SSUBSCRIBE", POOL_SUBSCRIPTION},
    {"SUBSCRIBE", POOL_SUBSCRIPTION},
    {"SUNSUBSCRIBE", POOL_SUBSCRIPTION},
    {"UNSUBSCRIBE", POOL_SUBSCRIPTION},
    {"UNWATCH", POOL_TRANSACTION},
    {"WAIT", POOL_BLOCKING},
    {"WAITAOF", POOL_BLOCKING},
    {"WATCH", POOL_TRANSACTION},
};
static int cmd_route_cmp(const void *k, const void *e) {
    return strcmp((const char *)k, ((const CmdRoute *)e)->cmd);
}

PoolType cmd_get_pool_type(const char *cmd) {
    const CmdRoute *r = bsearch(cmd, CMD_ROUTE_TABLE, sizeof(CMD_ROUTE_TABLE) / sizeof(CMD_ROUTE_TABLE[0]),
                                sizeof(CmdRoute), cmd_route_cmp);
    return r ? r->pool : POOL_NORMAL;
}

int cmd_in_list(const char *cmd, const char **list) {
    for (int i = 0; list[i]; i++)
        if (!strcmp(cmd, list[i])) return 1;
    return 0;
}

int cmd_is_supported(const char *cmd) {
    return cmd_in_list(cmd, NON_TXN_CMDS) || cmd_in_list(cmd, TXN_CMDS) || cmd_in_list(cmd, PUBSUB_CMDS);
}

void str_toupper(char *s) {
    for (; *s; s++) *s = (char)toupper((unsigned char)*s);
}

PoolType route_command(ClientConn *client, const char *cmd) {
    if (client->in_transaction) {
        if (!strcmp(cmd, "EXEC") || !strcmp(cmd, "DISCARD")) {
            client->in_transaction = 0;
            client->multi_count = -1;
        } else {
            client->multi_count++;
        }
        return POOL_TRANSACTION;
    }
    if (client->in_subscribe) {
        if (!strcmp(cmd, "PUBLISH") || !strcmp(cmd, "SPUBLISH")) return cmd_get_pool_type(cmd);
        return POOL_SUBSCRIPTION;
    }
    PoolType pt = cmd_get_pool_type(cmd);
    if (pt == POOL_TRANSACTION && !strcmp(cmd, "MULTI")) {
        client->in_transaction = 1;
        client->multi_count = 0;
    } else if (pt == POOL_SUBSCRIPTION)
        client->in_subscribe = 1;
    return pt;
}

static const char *MULTI_KEY_CMDS[] = {"BITOP",
                                       "COPY",
                                       "DEL",
                                       "EVAL",
                                       "EVAL_RO",
                                       "EVALSHA",
                                       "EVALSHA_RO",
                                       "EXISTS",
                                       "FCALL",
                                       "FCALL_RO",
                                       "GEORADIUS",
                                       "GEORADIUSBYMEMBER",
                                       "GEOSEARCHSTORE",
                                       "JSON.MGET",
                                       "JSON.MSET",
                                       "LCS",
                                       "LMOVE",
                                       "LMPOP",
                                       "MGET",
                                       "MSET",
                                       "MSETEX",
                                       "MSETNX",
                                       "PFCOUNT",
                                       "PFMERGE",
                                       "RENAME",
                                       "RENAMENX",
                                       "RPOPLPUSH",
                                       "SDIFF",
                                       "SDIFFSTORE",
                                       "SINTER",
                                       "SINTERCARD",
                                       "SINTERSTORE",
                                       "SMOVE",
                                       "SUNION",
                                       "SUNIONSTORE",
                                       "TOUCH",
                                       "UNLINK",
                                       "XREAD",
                                       "XREADGROUP",
                                       "ZDIFF",
                                       "ZDIFFSTORE",
                                       "ZINTER",
                                       "ZINTERCARD",
                                       "ZINTERSTORE",
                                       "ZMPOP",
                                       "ZRANGESTORE",
                                       "ZUNION",
                                       "ZUNIONSTORE",
                                       NULL};
static const char *FANOUT_CMDS[] = {"DEL",  "EXISTS", "JSON.MGET", "JSON.MSET", "LCS", "MGET",
                                    "MSET", "MSETEX", "TOUCH",     "UNLINK",    NULL};
static const char *NO_KEY_CMDS[] = {
    "CLIENT",       "COMMAND",   "COMMANDLOG",   "DBSIZE",  "ECHO",      "FLUSHALL", "FLUSHDB",   "FT._LIST",
    "FT.AGGREGATE", "FT.CREATE", "FT.DROPINDEX", "FT.INFO", "FT.SEARCH", "FUNCTION", "HELLO",     "INFO",
    "KEYS",         "LOLWUT",    "MODULE",       "PING",    "PROXY",     "PUBSUB",   "RANDOMKEY", "SCAN",
    "SCRIPT",       "SELECT",    "SLOWLOG",      "TIME",    NULL};

/* Commands whose RESP2 reply has a nil *array* rather than a nil bulk
 * string somewhere in it -- see resp3_downgrade()'s nil_array_depth
 * argument in resp.c. EXEC is listed for completeness even though
 * transaction commands aren't actually routed to a backend yet on this
 * branch (see classify_command()'s lack of a POOL_TRANSACTION case below)
 * -- harmless now, correct once that gap is closed. */
static const char *RESP3_NIL_ARRAY_DEPTH0_CMDS[] = {
    /* Own top-level reply is nil array (e.g. a blocking command's timeout,
     * or a *MPOP with nothing to pop). */
    "BLPOP",      "BRPOP", "BZPOPMIN", "BZPOPMAX", "BLMPOP",     "BZMPOP", "BLMOVE",
    "BRPOPLPUSH", "LMPOP", "ZMPOP",    "XREAD",    "XREADGROUP", "EXEC",   NULL};
static const char *RESP3_NIL_ARRAY_DEPTH1_CMDS[] = {
    /* GEOPOS never returns a nil reply itself, but each individual queried
     * member's *slot* inside its outer array is a nil array (matching that
     * a present member's slot is always itself an array, [lon,lat]) when
     * that member doesn't exist -- confirmed against a real Valkey server;
     * this is a genuine exception to "nested nils are always nil bulk". */
    "GEOPOS", NULL};
int cmd_resp2_nil_array_depth(const char *cmd) {
    for (int i = 0; RESP3_NIL_ARRAY_DEPTH0_CMDS[i]; i++)
        if (!strcasecmp(cmd, RESP3_NIL_ARRAY_DEPTH0_CMDS[i])) return 0;
    for (int i = 0; RESP3_NIL_ARRAY_DEPTH1_CMDS[i]; i++)
        if (!strcasecmp(cmd, RESP3_NIL_ARRAY_DEPTH1_CMDS[i])) return 1;
    return -1;
}

/* Commands whose RESP3 reply restructures nesting depth with no
 * distinguishing type marker at all (see resp3_flatten_pairs()/
 * resp3_nest_pairs() in resp.c) -- a purely command-name-driven fixup, but
 * both transforms verify the actual byte shape before touching anything, so
 * listing a command here is harmless even for the invocations that don't
 * actually trigger the restructuring (e.g. ZRANGE without WITHSCORES). */
static const char *RESP3_FLATTEN_PAIRS_CMDS[] = {
    /* WITHSCORES/WITHVALUES-family: RESP3 nests each result as its own
     * [member,score] (or [field,value]) pair; RESP2 is the same 2N values
     * flattened into one array. */
    "ZRANGE", "ZREVRANGE",   "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZDIFF",      "ZINTER",
    "ZUNION", "ZRANDMEMBER", "ZPOPMIN",       "ZPOPMAX",          "HRANDFIELD", NULL};
static const char *RESP3_NEST_PAIRS_CMDS[] = {
    /* RESP3 wraps these as a map (stream name -> entries); resp3_downgrade()
     * dutifully flattens that map into "*2n" per the generic map==flat-
     * array-of-pairs rule (correct for e.g. HGETALL/CONFIG GET), but a
     * genuine RESP2 backend actually nests each [name,entries] pair in its
     * own sub-array for these two commands specifically. */
    "XREAD", "XREADGROUP", NULL};
int cmd_resp3_pair_transform(const char *cmd) {
    for (int i = 0; RESP3_FLATTEN_PAIRS_CMDS[i]; i++)
        if (!strcasecmp(cmd, RESP3_FLATTEN_PAIRS_CMDS[i])) return 1;
    for (int i = 0; RESP3_NEST_PAIRS_CMDS[i]; i++)
        if (!strcasecmp(cmd, RESP3_NEST_PAIRS_CMDS[i])) return 2;
    return 0;
}

/* Commands whose first argument is a subcommand rather than a data key (see
 * resp_parse_header()'s "key" = arg[1] convention) -- used to format the
 * CLIENT INFO "cmd" field as "cmd|subcommand", matching real Redis/Valkey. */
static const char *CONTAINER_CMDS[] = {"ACL",    "CLIENT",  "COMMAND", "COMMANDLOG", "FUNCTION",
                                       "MEMORY", "MODULE",  "OBJECT",  "PROXY",      "PUBSUB",
                                       "SCRIPT", "SLOWLOG", "XGROUP",  "XINFO",      NULL};

/* Records this command as the client's most recent one (see the "cmd" field
 * of CLIENT INFO, format_client_info_line() in fanout.c). */
void record_last_cmd(ClientConn *client, const char *cmd, const char *key, int keylen) {
    int cap = (int)sizeof(client->last_cmd);
    int n = (int)strlen(cmd);
    if (n >= cap) n = cap - 1;
    for (int i = 0; i < n; i++) client->last_cmd[i] = (char)tolower((unsigned char)cmd[i]);
    int off = n;

    if (keylen > 0 && off + 1 < cap && cmd_in_list(cmd, CONTAINER_CMDS)) {
        client->last_cmd[off++] = '|';
        int sub_n = keylen;
        if (off + sub_n >= cap) sub_n = cap - 1 - off;
        for (int i = 0; i < sub_n; i++) client->last_cmd[off + i] = (char)tolower((unsigned char)key[i]);
        off += sub_n;
    }
    client->last_cmd[off] = '\0';
}

int cmd_is_fanout(const char *cmd) {
    return cmd_in_list(cmd, FANOUT_CMDS);
}

CmdClass classify_command(const char *cmd, PoolType pt) {
    if (pt == POOL_BLOCKING) return CMD_CLASS_BLOCKING;
    if (pt == POOL_SUBSCRIPTION) return CMD_CLASS_PUBSUB;
    if (pt == POOL_ACL) return CMD_CLASS_ACL;
    for (int i = 0; MULTI_KEY_CMDS[i]; i++)
        if (!strcmp(cmd, MULTI_KEY_CMDS[i])) return CMD_CLASS_MULTI_KEY;
    for (int i = 0; NO_KEY_CMDS[i]; i++)
        if (!strcmp(cmd, NO_KEY_CMDS[i])) return CMD_CLASS_NO_KEY;
    return CMD_CLASS_SINGLE_KEY;
}
