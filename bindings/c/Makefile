OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm

# Set LIBSQL_PATH to the default path if not provided
LIBSQL_EXPERIMENTAL_PATH ?= ../../target/release/libsql_experimental.a

ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all
all: example

example: example.c
	$(CC) -o $@ $(CFLAGS) $< $(LIBSQL_EXPERIMENTAL_PATH) $(LDFLAGS)
