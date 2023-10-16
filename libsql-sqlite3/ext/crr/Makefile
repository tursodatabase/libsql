ifndef CI_GCC
CC:=gcc
else
CC:=$(CI_GCC)
endif

LOADABLE_CFLAGS=-std=c99 -fPIC -shared -Wall

ifeq ($(shell uname -s),Darwin)
CONFIG_DARWIN=y
else ifeq ($(OS),Windows_NT)
CONFIG_WINDOWS=y
else
CONFIG_LINUX=y
endif

ifdef CONFIG_DARWIN
LOADABLE_EXTENSION=dylib
endif

ifdef CONFIG_LINUX
LOADABLE_EXTENSION=so
endif

ifdef CONFIG_WINDOWS
LOADABLE_EXTENSION=dll
endif

prefix=./dist
dbg_prefix=./dbg

TARGET_LOADABLE=$(prefix)/crsqlite.$(LOADABLE_EXTENSION)
TARGET_DBG_LOADABLE=$(dbg_prefix)/crsqlite.$(LOADABLE_EXTENSION)
TARGET_SQLITE3_EXTRA_C=$(prefix)/sqlite3-extra.c
TARGET_SQLITE3=$(prefix)/sqlite3
TARGET_SQLITE3_VANILLA=$(prefix)/vanilla-sqlite3
TARGET_TEST=$(prefix)/test
TARGET_FUZZ=$(prefix)/fuzz
TARGET_TEST_ASAN=$(prefix)/test-asan


# js/browser/wa-sqlite/Makefile, deps/sqlite/GNUMakefile, core/binding.gyp, core/Makefile
ext_files=src/crsqlite.c \
	src/util.c \
	src/tableinfo.c \
	src/triggers.c \
	src/changes-vtab.c \
	src/changes-vtab-read.c \
	src/changes-vtab-common.c \
	src/changes-vtab-write.c \
	src/ext-data.c \
	src/get-table.c
ext_headers=src/crsqlite.h \
	src/util.h \
	src/tableinfo.h \
	src/triggers.h \
	src/changes-vtab.h \
	src/changes-vtab-read.h \
	src/changes-vtab-common.h \
	src/changes-vtab-write.h \
	src/ext-data.h

$(prefix):
	mkdir -p $(prefix)
$(dbg_prefix):
	mkdir -p $(dbg_prefix)

clean:
	rm -rf $(prefix)
	rm -rf $(dbg_prefix)
	cd rs/bundle && cargo clean


FORMAT_FILES=$(ext_files) $(ext_headers) ./src/core_init.c
format: $(FORMAT_FILES)
	clang-format -i $(FORMAT_FILES)

loadable: $(TARGET_LOADABLE)
loadable_dbg: $(TARGET_DBG_LOADABLE)
sqlite3: $(TARGET_SQLITE3)
vanilla: $(TARGET_SQLITE3_VANILLA)
test: $(TARGET_TEST)
	$(prefix)/test
# ASAN_OPTIONS=detect_leaks=1
asan: CC=clang
asan: $(TARGET_TEST_ASAN)
	$(TARGET_TEST_ASAN)
correctness: $(TARGET_LOADABLE) FORCE
	cd ../py/correctness && pytest
valgrind: $(TARGET_TEST)
	valgrind $(prefix)/test
analyzer:
	scan-build $(MAKE) clean loadable
ubsan: CC=clang
ubsan: LDLIBS += -lubsan
ubsan: clean $(TARGET_TEST)
	$(prefix)/test
fuzz: $(TARGET_FUZZ)
	$(prefix)/fuzz

sqlite_src = ../../
shell.c = $(sqlite_src)/shell.c
sqlite3.c = $(sqlite_src)/sqlite3.c

rs_lib_dbg_static = ./rs/bundle/target/debug/libcrsql_bundle.a
rs_lib_dbg_static_cpy = ./dbg/libcrsql_bundle-dbg-static.a

rs_lib_loadable = ./rs/bundle/target/release/libcrsql_bundle.a
rs_lib_loadable_cpy = ./dist/libcrsql_bundle-loadable.a

rs_lib_dbg_loadable = ./rs/bundle/target/debug/libcrsql_bundle.a
rs_lib_dbg_loadable_cpy = ./dbg/libcrsql_bundle-dbg-loadable.a

ifdef CI_MAYBE_TARGET
	rs_lib_dbg_static = ./rs/bundle/target/$(CI_MAYBE_TARGET)/debug/libcrsql_bundle.a
	rs_lib_loadable = ./rs/bundle/target/$(CI_MAYBE_TARGET)/release/libcrsql_bundle.a
	rs_lib_dbg_loadable = ./rs/bundle/target/$(CI_MAYBE_TARGET)/debug/libcrsql_bundle.a
	RS_TARGET = --target=$(CI_MAYBE_TARGET)
	ifndef CI_GCC
		C_TARGET = -target $(CI_MAYBE_TARGET)
	endif
endif

$(rs_lib_dbg_static_cpy): FORCE $(dbg_prefix) $(sqlite3.c)
	cd ./rs/bundle && $(rustflags_static) cargo build $(RS_TARGET) --features static,omit_load_extension $(rs_build_flags)
	cp $(rs_lib_dbg_static) $(rs_lib_dbg_static_cpy)

$(rs_lib_loadable_cpy): FORCE $(dbg_prefix) $(sqlite3.c)
	cd ./rs/bundle && $(rustflags_static) cargo build $(RS_TARGET) --release --features loadable_extension $(rs_build_flags)
	cp $(rs_lib_loadable) $(rs_lib_loadable_cpy)

# we need separate output dirs based on selected features of the build
$(rs_lib_dbg_loadable_cpy): FORCE $(dbg_prefix) $(sqlite3.c)
	cd ./rs/bundle && $(rustflags_static) cargo build $(RS_TARGET) --features loadable_extension $(rs_build_flags)
	cp $(rs_lib_dbg_loadable) $(rs_lib_dbg_loadable_cpy)

$(shell.c):
	cd $(sqlite_src) && make shell.c

$(sqlite3.c):
	cd $(sqlite_src) && make sqlite3.c

# Build the loadable extension.
$(TARGET_LOADABLE): $(prefix) $(ext_files) $(rs_lib_loadable_cpy)
	$(CC) -O2 -I./src/ -I$(sqlite_src) \
	$(LOADABLE_CFLAGS) \
	$(C_TARGET) \
	$(ext_files) $(rs_lib_loadable_cpy) -o $@

$(TARGET_DBG_LOADABLE): $(dbg_prefix) $(ext_files) $(rs_lib_dbg_loadable_cpy)
	$(CC) -g -I./src/ -I$(sqlite_src) \
	$(LOADABLE_CFLAGS) \
	$(ext_files) $(rs_lib_dbg_loadable_cpy) -o $@

# Build a SQLite CLI that pre-loads cr-sqlite.
# Useful for debugging.
$(TARGET_SQLITE3): $(prefix) $(TARGET_SQLITE3_EXTRA_C) $(rs_lib_dbg_static_cpy) $(shell.c) $(ext_files)
	$(CC) -g \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_OMIT_LOAD_EXTENSION=1 \
	-DSQLITE_EXTRA_INIT=core_init \
	-I./src/ -I$(sqlite_src) \
	$(TARGET_SQLITE3_EXTRA_C) $(shell.c) $(ext_files) $(rs_lib_dbg_static_cpy) \
	$(LDLIBS) -o $@

# Build a normal SQLite CLI that does not include cr-sqlite.
# cr-sqlite can be laoded in via the `.load` pragma.
# Useful for debugging.
$(TARGET_SQLITE3_VANILLA): $(prefix) $(shell.c) $(sqlite3.c)
	$(CC) -g \
	$(DEFINE_SQLITE_PATH) \
	-DSQLITE_THREADSAFE=0 \
	-I./src/ -I$(sqlite_src) \
	$(sqlite3.c) $(shell.c) \
	-o $@

$(TARGET_SQLITE3_EXTRA_C): $(sqlite3.c) src/core_init.c
	cat $(sqlite3.c) src/core_init.c > $@

# run tests
$(TARGET_TEST): $(prefix) $(TARGET_SQLITE3_EXTRA_C) src/tests.c src/*.test.c $(ext_files) $(rs_lib_dbg_static_cpy)
	$(CC) -g -Wall \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_OMIT_LOAD_EXTENSION=1 \
	-DSQLITE_EXTRA_INIT=core_init \
	-DUNIT_TEST=1 \
	-I./src/ -I$(sqlite_src) \
	$(TARGET_SQLITE3_EXTRA_C) src/tests.c src/*.test.c $(ext_files) $(rs_lib_dbg_static_cpy) \
	$(LDLIBS) -o $@

$(TARGET_TEST_ASAN): $(prefix) $(TARGET_SQLITE3_EXTRA_C) src/tests.c src/*.test.c $(ext_files)
	$(CC) -fsanitize=address -g -fno-omit-frame-pointer -Wall \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_OMIT_LOAD_EXTENSION=1 \
	-DSQLITE_EXTRA_INIT=core_init \
	-DUNIT_TEST=1 \
	-I./src/ -I$(sqlite_src) \
	$(TARGET_SQLITE3_EXTRA_C) src/tests.c src/*.test.c $(ext_files) $(rs_lib_dbg_static_cpy) \
	$(LDLIBS) -o $@

$(TARGET_FUZZ): $(prefix) $(TARGET_SQLITE3_EXTRA_C) src/fuzzer.cc $(ext_files)
	clang -fsanitize=fuzzer \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_OMIT_LOAD_EXTENSION=1 \
	-DSQLITE_EXTRA_INIT=core_init \
	-I./src/ -I$(sqlite_src) \
	$(TARGET_SQLITE3_EXTRA_C) src/fuzzer.cc $(ext_files) $(rs_lib_dbg_static_cpy) \
	$(LDLIBS) -o $@

.PHONY: all clean format \
	test \
	loadable \
	loadable_dbg \
	sqlite3 \
	correctness \
	valgrind \
	ubsan analyzer fuzz asan

FORCE: ;