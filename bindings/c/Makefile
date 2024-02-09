OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm

ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all
all: example

example: example.c
	$(CC) -o $@ $(CFLAGS) $< ../../target/release/libsql_experimental.a $(LDFLAGS)
