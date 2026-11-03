# Supported Commands
The list below covers every supported command.

### Bitmap

| Command | Notes |
|---|---|
| `BITCOUNT` | |
| `BITFIELD` | |
| `BITFIELD_RO` | |
| `BITOP` | The destkey and every srckey must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend|
| `BITPOS` | |
| `GETBIT` | |
| `SETBIT` | |

### Connection

| Command | Notes |
|---|---|
| `AUTH` | |
| `CLIENT GETNAME` | Answered by proxy |
| `CLIENT HELP` | Answered by proxy |
| `CLIENT ID` | Answered by proxy |
| `CLIENT INFO` | Answered by proxy |
| `CLIENT KILL` | Planned |
| `CLIENT LIST` | Planned |
| `CLIENT SETNAME` | Stored on the proxy only, never sent to a backend |
| `ECHO` | Answered by proxy |
| `HELLO` | Answered by proxy, except the first HELLO 3 (RESP3 probe) and AUTH with a non-default user, which go to the backend. Reports mode standalone. |
| `PING` | Answered by proxy |
| `RESET` | Planned |

### Generic

| Command | Notes |
|---|---|
| `COPY` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `DEL` | Fanned out per key to each owning node |
| `DUMP` | |
| `EXISTS` | Fanned out per key to each owning node |
| `EXPIRE` | |
| `EXPIREAT` | |
| `EXPIRETIME` | |
| `KEYS` | Broadcast to every primary node; results concatenated |
| `OBJECT ENCODING` | |
| `OBJECT FREQ` | |
| `OBJECT HELP` | Forwarded to a single primary node |
| `OBJECT IDLETIME` | |
| `OBJECT REFCOUNT` | |
| `PERSIST` | |
| `PEXPIRE` | |
| `PEXPIREAT` | |
| `PEXPIRETIME` | |
| `PTTL` | |
| `RANDOMKEY` | Forwarded to a single primary node |
| `RENAME` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `RENAMENX` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `RESTORE` | |
| `SCAN` | Broadcast to every primary node; each node's cursor is encoded into the returned cursor |
| `SORT` | |
| `SORT_RO` | |
| `TOUCH` | Fanned out per key to each owning node |
| `TTL` | |
| `TYPE` | |
| `UNLINK` | Fanned out per key to each owning node |

### Geospatial Index

| Command | Notes |
|---|---|
| `GEOADD` | |
| `GEODIST` | |
| `GEOHASH` | |
| `GEOPOS` | |
| `GEORADIUS` | With `STORE`/`STOREDIST`, the source key and the store key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `GEORADIUSBYMEMBER` | With `STORE`/`STOREDIST`, the source key and the store key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `GEORADIUSBYMEMBER_RO` | |
| `GEORADIUS_RO` | |
| `GEOSEARCH` | |
| `GEOSEARCHSTORE` | The destination and source keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |

### Hash

| Command | Notes |
|---|---|
| `HDEL` | |
| `HEXISTS` | |
| `HEXPIRE` | |
| `HEXPIREAT` | |
| `HEXPIRETIME` | |
| `HGET` | |
| `HGETALL` | |
| `HGETDEL` | |
| `HGETEX` | |
| `HINCRBY` | |
| `HINCRBYFLOAT` | |
| `HKEYS` | |
| `HLEN` | |
| `HMGET` | |
| `HMSET` | |
| `HPERSIST` | |
| `HPEXPIRE` | |
| `HPEXPIREAT` | |
| `HPEXPIRETIME` | |
| `HPTTL` | |
| `HRANDFIELD` | |
| `HSCAN` | |
| `HSET` | |
| `HSETEX` | |
| `HSETNX` | |
| `HSTRLEN` | |
| `HTTL` | |
| `HVALS` | |

### HyperLogLog

| Command | Notes |
|---|---|
| `PFADD` | |
| `PFCOUNT` | With multiple keys, every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `PFMERGE` | The destkey and every sourcekey must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |

### List

| Command | Notes |
|---|---|
| `BLMOVE` | the source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BLMPOP` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BLPOP` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BRPOP` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BRPOPLPUSH` | the source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `LINDEX` | |
| `LINSERT` | |
| `LLEN` | |
| `LMOVE` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `LMPOP` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `LPOP` | |
| `LPOS` | |
| `LPUSH` | |
| `LPUSHX` | |
| `LRANGE` | |
| `LREM` | |
| `LSET` | |
| `LTRIM` | |
| `RPOP` | |
| `RPOPLPUSH` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `RPUSH` | |
| `RPUSHX` | |

### Pub/Sub

| Command | Notes |
|---|---|
| `PSUBSCRIBE` | |
| `PUBLISH` | |
| `PUBSUB CHANNELS` | Broadcast to every primary node |
| `PUBSUB HELP` | |
| `PUBSUB NUMPAT` | Broadcast to every primary node |
| `PUBSUB NUMSUB` | Broadcast to every primary node |
| `PUBSUB SHARDCHANNELS` | Broadcast to every primary node |
| `PUBSUB SHARDNUMSUB` | Broadcast to every primary node |
| `PUNSUBSCRIBE` | |
| `SPUBLISH` | |
| `SSUBSCRIBE` | |
| `SUBSCRIBE` | |
| `SUNSUBSCRIBE` | |
| `UNSUBSCRIBE` | |

### Sorted Set

| Command | Notes |
|---|---|
| `BZMPOP` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BZPOPMAX` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `BZPOPMIN` | every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZADD` | |
| `ZCARD` | |
| `ZCOUNT` | |
| `ZDIFF` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZDIFFSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZINCRBY` | |
| `ZINTER` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZINTERCARD` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZINTERSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZLEXCOUNT` | |
| `ZMPOP` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZMSCORE` | |
| `ZPOPMAX` | |
| `ZPOPMIN` | |
| `ZRANDMEMBER` | |
| `ZRANGE` | |
| `ZRANGEBYLEX` | |
| `ZRANGEBYSCORE` | |
| `ZRANGESTORE` | The destination and source keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZRANK` | |
| `ZREM` | |
| `ZREMRANGEBYLEX` | |
| `ZREMRANGEBYRANK` | |
| `ZREMRANGEBYSCORE` | |
| `ZREVRANGE` | |
| `ZREVRANGEBYLEX` | |
| `ZREVRANGEBYSCORE` | |
| `ZREVRANK` | |
| `ZSCAN` | |
| `ZSCORE` | |
| `ZUNION` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `ZUNIONSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |

### Set

| Command | Notes |
|---|---|
| `SADD` | |
| `SCARD` | |
| `SDIFF` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SDIFFSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SINTER` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SINTERCARD` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SINTERSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SISMEMBER` | |
| `SMEMBERS` | |
| `SMISMEMBER` | |
| `SMOVE` | The source and destination keys must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SPOP` | |
| `SRANDMEMBER` | |
| `SREM` | |
| `SSCAN` | |
| `SUNION` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `SUNIONSTORE` | The destination and every source key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |

### Stream

| Command | Notes |
|---|---|
| `XACK` | |
| `XADD` | |
| `XAUTOCLAIM` | |
| `XCLAIM` | |
| `XDEL` | |
| `XGROUP CREATE` | |
| `XGROUP CREATECONSUMER` | |
| `XGROUP DELCONSUMER` | |
| `XGROUP DESTROY` | |
| `XGROUP HELP` | |
| `XGROUP SETID` | |
| `XINFO CONSUMERS` | |
| `XINFO GROUPS` | |
| `XINFO HELP` | |
| `XINFO STREAM` | |
| `XLEN` | |
| `XPENDING` | |
| `XRANGE` | |
| `XREAD` | Every stream key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `XREADGROUP` | Every stream key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `XREVRANGE` | |
| `XTRIM` | |

### String

| Command | Notes |
|---|---|
| `APPEND` | |
| `DECR` | |
| `DECRBY` | |
| `DELEX` | |
| `DELIFEQ` | |
| `GET` | |
| `GETDEL` | |
| `GETEX` | |
| `GETRANGE` | |
| `GETSET` | |
| `INCR` | |
| `INCRBY` | |
| `INCRBYFLOAT` | |
| `LCS` | Fanned out per key to each owning node |
| `MGET` | Fanned out per key to each owning node |
| `MSET` | Fanned out per key to each owning node |
| `MSETEX` | Fanned out per key to each owning node |
| `MSETNX` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `PSETEX` | |
| `SET` | |
| `SETEX` | |
| `SETNX` | |
| `SETRANGE` | |
| `STRLEN` | |
| `SUBSTR` | |

### Transaction

| Command | Notes |
|---|---|
| `DISCARD` | Planned |
| `EXEC` | Planned |
| `MULTI` | Planned |
| `UNWATCH` | Planned |
| `WATCH` | Planned |

### Scripting and Functions

| Command | Notes |
|---|---|
| `EVAL` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `EVALSHA` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `EVALSHA_RO` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `EVAL_RO` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `FCALL` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `FCALL_RO` | Every key must hash to the same slot, otherwise the proxy returns `-CROSSSLOT` without contacting a backend |
| `FUNCTION DELETE` | Broadcast to every primary node; succeeds only if every node succeeds |
| `FUNCTION FLUSH` | Broadcast to every primary node; the `ASYNC`/`SYNC` argument is ignored (always `ASYNC`), always replies `+OK` |
| `FUNCTION HELP` | Answered by proxy |
| `FUNCTION KILL` | Broadcast to every primary node; succeeds if any node succeeds |
| `FUNCTION LIST` |  |
| `FUNCTION LOAD` | Broadcast to every primary node; succeeds only if every node succeeds |
| `FUNCTION STATS` | Broadcast to every primary node; each node's execution state is reported separately |
| `SCRIPT EXISTS` | Forwarded to one primary node |
| `SCRIPT FLUSH` | Broadcast to every primary node; the `ASYNC`/`SYNC` argument is ignored (always `ASYNC`), always replies `+OK` |
| `SCRIPT HELP` | Answered by proxy |
| `SCRIPT KILL` | Broadcast to every primary node; succeeds if any node succeeds |
| `SCRIPT LOAD` | Broadcast to every primary node so any shard can run the script; only one sha1 is relayed back |
| `SCRIPT SHOW` | Forwarded to one primary node |

### Server Management

| Command | Notes |
|---|---|
| `ACL CAT` | Planned |
| `ACL DRYRUN` | Forwarded to one primary node |
| `ACL GETUSER` | Forwarded to one primary node |
| `ACL HELP` | Answered by proxy |
| `ACL LIST` | Forwarded to one primary node |
| `ACL USERS` | Forwarded to one primary node |
| `ACL WHOAMI` | Forwarded to one primary node |
| `COMMAND` | |
| `COMMAND COUNT` | Counts only the commands this proxy supports |
| `COMMAND DOCS` | Forwarded to one primary node; reply filtered to only the commands this proxy supports |
| `COMMAND GETKEYS` | Forwarded to one primary node |
| `COMMAND GETKEYSANDFLAGS` | Forwarded to one primary node |
| `COMMAND HELP` | Forwarded to one primary node |
| `COMMAND INFO` | Forwarded to one primary node; reply filtered to only the commands this proxy supports |
| `COMMAND LIST` | Forwarded to one primary node; reply filtered to only the commands this proxy supports |
| `COMMANDLOG GET` | Broadcast to every primary node |
| `COMMANDLOG HELP` | Forwarded to one primary node |
| `COMMANDLOG LEN` | Broadcast to every primary node |
| `COMMANDLOG RESET` | Broadcast to every primary node |
| `DBSIZE` | Broadcast to every primary node |
| `FLUSHALL` | Broadcast to every primary node |
| `FLUSHDB` | Broadcast to every primary node |
| `INFO` | Answered as one standalone server: proxy sections answered locally, keyspace summed across primaries, other backend sections listed per shard; see FEATURES.md §3.4 |
| `LOLWUT` | Forwarded to one primary node |
| `MEMORY USAGE` | |
| `MODULE LIST` | Forwarded to one primary node |
| `MONITOR` | Planned |
| `PROXY HELP` | Answered by proxy |
| `PROXY INFO` | Each primary's own INFO reply, unmerged; `SHARD <idx>\|<ip:port>` limits it to one |
| `PROXY POOLS` | Answered by proxy; connection pool usage per backend node and pool type |
| `SLOWLOG GET` | Broadcast to every primary node |
| `SLOWLOG HELP` | Forwarded to one primary node |
| `SLOWLOG LEN` | Broadcast to every primary node |
| `SLOWLOG RESET` | Broadcast to every primary node |
| `TIME` | Answered by proxy |

### Bloom Filter

These commands require the Bloom Filter module (valkey-bloom) to be loaded on every backend node; otherwise the backend's "unknown command" error is relayed to the client.

| Command | Notes |
|---|---|
| `BF.ADD` | |
| `BF.CARD` | |
| `BF.EXISTS` | |
| `BF.INFO` | |
| `BF.INSERT` | |
| `BF.MADD` | |
| `BF.MEXISTS` | |
| `BF.RESERVE` | |

### JSON

These commands require the JSON module (valkey-json) to be loaded on every backend node; otherwise the backend's "unknown command" error is relayed to the client.

| Command | Notes |
|---|---|
| `JSON.ARRAPPEND` | |
| `JSON.ARRINDEX` | |
| `JSON.ARRINSERT` | |
| `JSON.ARRLEN` | |
| `JSON.ARRPOP` | |
| `JSON.ARRTRIM` | |
| `JSON.CLEAR` | |
| `JSON.DEL` | |
| `JSON.FORGET` | |
| `JSON.GET` | |
| `JSON.MGET` | Fanned out per key to each owning node |
| `JSON.MSET` | Fanned out per key to each owning node |
| `JSON.NUMINCRBY` | |
| `JSON.NUMMULTBY` | |
| `JSON.OBJKEYS` | |
| `JSON.OBJLEN` | |
| `JSON.RESP` | |
| `JSON.SET` | |
| `JSON.STRAPPEND` | |
| `JSON.STRLEN` | |
| `JSON.TOGGLE` | |
| `JSON.TYPE` | |

### Search

These commands require the Search module (valkey-search) to be loaded on every backend node; otherwise the backend's "unknown command" error is relayed to the client.

| Command | Notes |
|---|---|
| `FT.AGGREGATE` | Forwarded to one primary node |
| `FT.CREATE` | Forwarded to one primary node |
| `FT.CURSOR` | Planned, pending valkey-search 1.3 GA |
| `FT.DROPINDEX` | Forwarded to one primary node |
| `FT.HYBRID` | Planned, pending valkey-search 1.3 GA |
| `FT.INFO` | Forwarded to one primary node; `FT.INFO <index>` with no options is rewritten to `FT.INFO <index> PRIMARY ALLSHARDS`, explicit options are forwarded as-is |
| `FT.SEARCH` | Forwarded to one primary node |
| `FT._LIST` | Forwarded to one primary node |

## Unsupported commands

| Command | Notes |
|---|---|
| `ASKING` | |
| `CLUSTER ADDSLOTS` | |
| `CLUSTER ADDSLOTSRANGE` | |
| `CLUSTER BUMPEPOCH` | |
| `CLUSTER CANCELSLOTMIGRATIONS` | |
| `CLUSTER COUNT-FAILURE-REPORTS` | |
| `CLUSTER COUNTKEYSINSLOT` | |
| `CLUSTER DELSLOTS` | |
| `CLUSTER DELSLOTSRANGE` | |
| `CLUSTER FAILOVER` | |
| `CLUSTER FLUSHSLOTS` | |
| `CLUSTER FORGET` | |
| `CLUSTER GETKEYSINSLOT` | |
| `CLUSTER GETSLOTMIGRATIONS` | |
| `CLUSTER HELP` | |
| `CLUSTER INFO` | |
| `CLUSTER KEYSLOT` | |
| `CLUSTER LINKS` | |
| `CLUSTER MEET` | |
| `CLUSTER MIGRATESLOTS` | |
| `CLUSTER MYID` | |
| `CLUSTER MYSHARDID` | |
| `CLUSTER NODES` | |
| `CLUSTER REPLICAS` | |
| `CLUSTER REPLICATE` | |
| `CLUSTER RESET` | |
| `CLUSTER SAVECONFIG` | |
| `CLUSTER SET-CONFIG-EPOCH` | |
| `CLUSTER SETSLOT` | |
| `CLUSTER SHARDS` | |
| `CLUSTER SLAVES` | |
| `CLUSTER SLOT-STATS` | |
| `CLUSTER SLOTS` | |
| `CLUSTER SYNCSLOTS` | |
| `CLUSTERSCAN` | |
| `READONLY` | |
| `READWRITE` | |
| `CLIENT CACHING` | |
| `CLIENT CAPA` | |
| `CLIENT GETREDIR` | |
| `CLIENT IMPORT-SOURCE` | |
| `CLIENT NO-EVICT` | |
| `CLIENT NO-TOUCH` | |
| `CLIENT PAUSE` | |
| `CLIENT REPLY` | |
| `CLIENT SETINFO` | |
| `CLIENT TRACKING` | |
| `CLIENT TRACKINGINFO` | |
| `CLIENT UNBLOCK` | |
| `CLIENT UNPAUSE` | |
| `QUIT` | |
| `SELECT` | |
| `MIGRATE` | |
| `MOVE` | |
| `WAIT` | |
| `WAITAOF` | |
| `PFDEBUG` | |
| `PFSELFTEST` | |
| `XSETID` | |
| `FUNCTION DUMP` | |
| `FUNCTION RESTORE` | |
| `SCRIPT DEBUG` | |
| `ACL DELUSER` | |
| `ACL GENPASS` | |
| `ACL LOAD` | |
| `ACL LOG` | |
| `ACL SAVE` | |
| `ACL SETUSER` | |
| `BGREWRITEAOF` | |
| `BGSAVE` | |
| `CONFIG GET` | |
| `CONFIG HELP` | |
| `CONFIG RESETSTAT` | |
| `CONFIG REWRITE` | |
| `CONFIG SET` | |
| `FAILOVER` | |
| `LASTSAVE` | |
| `LATENCY DOCTOR` | |
| `LATENCY GRAPH` | |
| `LATENCY HELP` | |
| `LATENCY HISTOGRAM` | |
| `LATENCY HISTORY` | |
| `LATENCY LATEST` | |
| `LATENCY RESET` | |
| `MEMORY DOCTOR` | |
| `MEMORY HELP` | |
| `MEMORY MALLOC-STATS` | |
| `MEMORY PURGE` | |
| `MEMORY STATS` | |
| `MODULE HELP` | |
| `MODULE LOAD` | |
| `MODULE LOADEX` | |
| `MODULE UNLOAD` | |
| `PSYNC` | |
| `REPLCONF` | |
| `REPLICAOF` | |
| `RESTORE-ASKING` | |
| `ROLE` | |
| `SAVE` | |
| `SHUTDOWN` | |
| `SLAVEOF` | |
| `SWAPDB` | |
| `SYNC` | |
| `BF.LOAD` | |
| `JSON.DEBUG` | |
