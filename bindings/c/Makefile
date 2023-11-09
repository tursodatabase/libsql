OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm

ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all
all: example

example: example.c
	$(CC) -o $@ $(CFLAGS) $< ../../target/debug/libsql_experimental.a $(LDFLAGS)
