POOL_CC      = gcc
POOL_CFLAGS  = -O2 -Wall -Wextra -Isrc -pthread
POOL_LDFLAGS = -pthread

# TLS support for client connections (disable with: make valkey-proxy NO_TLS=1)
ifeq ($(NO_TLS),)
OPENSSL_PREFIX ?= $(shell brew --prefix openssl 2>/dev/null || echo /usr)
POOL_CFLAGS  += -DUSE_TLS -I$(OPENSSL_PREFIX)/include
POOL_LDFLAGS += -L$(OPENSSL_PREFIX)/lib -lssl -lcrypto
endif

POOL_TARGET = valkey-proxy
POOL_SRCS   = src/valkey-proxy.c src/config.c src/cluster.c src/resp.c \
              src/pool.c src/cmd.c src/fanout.c src/pubsub.c src/acl.c src/client.c \
              src/hello.c src/event_loop.c src/metrics.c src/info.c src/mux.c
BUILD_DIR   = build
POOL_OBJS   = $(POOL_SRCS:%.c=$(BUILD_DIR)/%.o)

# Print a short "CC <file>" line per step; run "make V=1" to see full commands
ifeq ($(V),1)
Q =
else
Q = @
endif

$(POOL_TARGET): $(POOL_OBJS)
	@echo "    LINK $@"
	$(Q)$(POOL_CC) -o $@ $(POOL_OBJS) $(POOL_LDFLAGS)

# -MMD -MP tracks header dependencies, including the el_*.c files that
# event_loop.c #include's
$(BUILD_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "    CC   $<"
	$(Q)$(POOL_CC) $(POOL_CFLAGS) -MMD -MP -c $< -o $@

-include $(POOL_OBJS:.o=.d)

# ── integration tests ────────────────────────────────────────────────────
TEST_CC     = gcc
TEST_CFLAGS = -O0 -g -Wall -Wextra -Itest -pthread

TEST_SRCS = test/run-test.c test/acl/test_acl.c test/acl/acl_fixture.c test/bitmap/test_bitmap.c \
            test/blocking/test_blocking.c \
            test/general/test_general.c test/general/test_reported.c test/hash/test_hash.c \
            test/geospatial/test_geospatial.c \
            test/hyperloglog/test_hyperloglog.c \
            test/list/test_list.c \
            test/pubsub/test_pubsub.c \
            test/script/test_script.c \
            test/function/test_function.c \
            test/set/test_set.c \
            test/stream/test_stream.c \
            test/string/test_string.c \
            test/zset/test_zset.c \
            test/resp3/test_resp3.c \
            test/modules/bloomfilter/test_bloomfilter.c \
            test/modules/json/test_json.c \
            test/modules/search/test_search.c

TEST_OBJS = $(TEST_SRCS:%.c=$(BUILD_DIR)/%.o)

run-test: $(TEST_OBJS)
	@echo "    LINK $@"
	$(Q)$(TEST_CC) $(TEST_CFLAGS) -o $@ $(TEST_OBJS)

$(TEST_OBJS): $(BUILD_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "    CC   $<"
	$(Q)$(TEST_CC) $(TEST_CFLAGS) -MMD -MP -c $< -o $@

-include $(TEST_OBJS:.o=.d)

# Usage: make test && ./run-test -h=<host> -p=<port>
test: run-test

clean:
	rm -f $(POOL_TARGET) run-test
	rm -rf $(BUILD_DIR)

# ── formatting ───────────────────────────────────────────────────────────
FORMAT_SRCS = $(shell find src test -name '*.[ch]')

format:
	clang-format -i $(FORMAT_SRCS)

format-check:
	clang-format --dry-run --Werror $(FORMAT_SRCS)

.PHONY: clean test format format-check

