# we need math.h for sqrt function in LIBSQL vector feature
LDLIBS += -lm

ifndef CI_GCC
CC:=gcc
else
CC:=$(CI_GCC)
endif

SHARED_CFLAGS=-DLIBSQL=1
LOADABLE_CFLAGS=-std=c99 -fPIC -shared -Wall $(SHARED_CFLAGS)
STATIC_CFLAGS=-std=c99 -fPIC -c -Wall $(SHARED_CFLAGS)
libsql_feature=,libsql

ifeq ($(shell uname -s),Darwin)
CONFIG_DARWIN=y
else ifeq ($(OS),Windows_NT)
CONFIG_WINDOWS=y
else
CONFIG_LINUX=y
endif

ifdef CONFIG_DARWIN
LOADABLE_EXTENSION=dylib
# apparently `darwin-x86_64` also works on arm macs and is the proper host arch for ndk builds.
NDK_HOSTARCH=darwin-x86_64
endif

ifdef CONFIG_LINUX
LOADABLE_EXTENSION=so
NDK_HOSTARCH=linux-x86_64
endif

ifdef CONFIG_WINDOWS
LOADABLE_EXTENSION=dll
endif

ifeq ($(CI_MAYBE_TARGET),i686-pc-windows-gnu)
C_TARGET=
LOADABLE_EXTENSION=dll
rs_build_flags = -Zbuild-std
endif
ifeq ($(CI_MAYBE_TARGET),x86_64-pc-windows-gnu)
C_TARGET=
LOADABLE_EXTENSION=dll
rs_build_flags = -Zbuild-std
endif

ifdef IOS_TARGET
CI_MAYBE_TARGET=$(IOS_TARGET)
rs_build_flags = -Zbuild-std
	ifeq ($(or $(findstring sim,$(CI_MAYBE_TARGET)),$(findstring x86_64,$(CI_MAYBE_TARGET))),)
			# todo: run the xcode command to find this
			sysroot_option = -isysroot /Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk
		else
			sysroot_option = -isysroot /Applications/Xcode.app/Contents/Developer/Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator.sdk
		endif
endif

# aarch64-linux-android
# https://github.com/marketplace/actions/setup-android-ndk
ifdef ANDROID_TARGET
CI_MAYBE_TARGET=$(ANDROID_TARGET)
NDK=$(ANDROID_NDK_HOME)
LOADABLE_EXTENSION=so
CC=$(NDK)/toolchains/llvm/prebuilt/$(NDK_HOSTARCH)/bin/clang
rs_ndk=ndk -t $(ANDROID_TARGET)
ANDROID_API_VERSION=33
rs_build_flags=-Zbuild-std
sysroot_option=--sysroot=$(NDK)/toolchains/llvm/prebuilt/$(NDK_HOSTARCH)/sysroot
endif

prefix=./dist
dbg_prefix=./dbg
bundle=bundle_static
valgrind: bundle = integration_check
test: bundle = integration_check

TARGET_LOADABLE=$(prefix)/crsqlite.$(LOADABLE_EXTENSION)
TARGET_DBG_LOADABLE=$(dbg_prefix)/crsqlite.$(LOADABLE_EXTENSION)
TARGET_SQLITE3_EXTRA_C=$(prefix)/sqlite3-extra.c
TARGET_SQLITE3=$(prefix)/sqlite3
TARGET_SQLITE3_VANILLA=$(prefix)/vanilla-sqlite3
TARGET_STATIC=$(prefix)/crsqlite.a
TARGET_TEST=$(prefix)/test
TARGET_FUZZ=$(prefix)/fuzz
TARGET_TEST_ASAN=$(prefix)/test-asan


# js/browser/wa-sqlite/Makefile, deps/sqlite/GNUMakefile, core/binding.gyp, core/Makefile
ext_files=src/crsqlite.c \
	src/changes-vtab.c \
	src/ext-data.c
ext_headers=src/crsqlite.h \
	src/util.h \
	src/changes-vtab.h \
	src/ext-data.h

$(prefix):
	mkdir -p $(prefix)
$(dbg_prefix):
	mkdir -p $(dbg_prefix)

clean:
	rm -rf $(prefix)
	rm -rf $(dbg_prefix)
	cd rs/bundle && cargo clean
	cd rs/integration_check && cargo clean

FORMAT_FILES=$(ext_files) $(ext_headers) ./src/core_init.c
format: $(FORMAT_FILES)
	clang-format -i $(FORMAT_FILES)

loadable: $(TARGET_LOADABLE)
loadable_dbg: $(TARGET_DBG_LOADABLE)
sqlite3: $(TARGET_SQLITE3)
vanilla: $(TARGET_SQLITE3_VANILLA)
static: $(TARGET_STATIC)
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

sqlite_src = src/sqlite/
shell.c = $(sqlite_src)shell.c
sqlite3.c = $(sqlite_src)sqlite3.c

rs_lib_static = ./rs/$(bundle)/target/release/libcrsql_$(bundle).a
rs_lib_static_cpy = ./dist/libcrsql_$(bundle)-static.a

rs_lib_dbg_static = ./rs/$(bundle)/target/debug/libcrsql_$(bundle).a
rs_lib_dbg_static_cpy = ./dbg/libcrsql_$(bundle)-dbg-static.a

rs_lib_loadable = ./rs/$(bundle)/target/release/libcrsql_$(bundle).a
rs_lib_loadable_cpy = ./dist/libcrsql_$(bundle)-loadable.a

rs_lib_dbg_loadable = ./rs/$(bundle)/target/debug/libcrsql_$(bundle).a
rs_lib_dbg_loadable_cpy = ./dbg/libcrsql_$(bundle)-dbg-loadable.a

ifdef CI_MAYBE_TARGET
	rs_lib_dbg_static = ./rs/$(bundle)/target/$(CI_MAYBE_TARGET)/debug/libcrsql_$(bundle).a
	rs_lib_loadable = ./rs/$(bundle)/target/$(CI_MAYBE_TARGET)/release/libcrsql_$(bundle).a
	rs_lib_dbg_loadable = ./rs/$(bundle)/target/$(CI_MAYBE_TARGET)/debug/libcrsql_$(bundle).a
	rs_lib_static = ./rs/$(bundle)/target/$(CI_MAYBE_TARGET)/release/libcrsql_$(bundle).a
	RS_TARGET = --target=$(CI_MAYBE_TARGET)
	ifndef CI_GCC
# clang has a different target triple than Rust for ios simuators
		ifeq ($(findstring sim,$(CI_MAYBE_TARGET)),)
			C_TARGET = --target=$(CI_MAYBE_TARGET)$(ANDROID_API_VERSION)
		else
			C_TARGET = --target=$(CI_MAYBE_TARGET)ulator
		endif
	endif
endif

$(shell.c):
	cd $(sqlite_src) && make shell.c

$(sqlite3.c):
	cd $(sqlite_src) && ./configure && make sqlite3.c

$(rs_lib_dbg_static_cpy): FORCE $(dbg_prefix)
	cd ./rs/$(bundle) && cargo rustc $(RS_TARGET) --features static,omit_load_extension$(libsql_feature) $(rs_build_flags)
	cp $(rs_lib_dbg_static) $(rs_lib_dbg_static_cpy)

$(rs_lib_static_cpy): FORCE $(prefix)
	cd ./rs/$(bundle) && cargo rustc $(RS_TARGET) --release --features static,omit_load_extension$(libsql_feature) $(rs_build_flags)
	cp $(rs_lib_static) $(rs_lib_static_cpy)

$(rs_lib_loadable_cpy): FORCE $(prefix)
	cd ./rs/$(bundle) && cargo $(rs_ndk) build $(RS_TARGET) --release --features loadable_extension$(libsql_feature) $(rs_build_flags)
	cp $(rs_lib_loadable) $(rs_lib_loadable_cpy)

$(rs_lib_dbg_loadable_cpy): FORCE $(dbg_prefix)
	cd ./rs/$(bundle) && cargo rustc $(RS_TARGET) --features loadable_extension$(libsql_feature) $(rs_build_flags)
	cp $(rs_lib_dbg_loadable) $(rs_lib_dbg_loadable_cpy)

# Build the loadable extension.
$(TARGET_LOADABLE): $(prefix) $(ext_files) $(sqlite3.c) $(rs_lib_loadable_cpy)
	$(CC) -O2 -I./src/ -I$(sqlite_src) \
	$(LOADABLE_CFLAGS) \
	$(C_TARGET) \
	$(sysroot_option) \
	$(ext_files) $(rs_lib_loadable_cpy) -o $@

$(TARGET_DBG_LOADABLE): $(dbg_prefix) $(ext_files) $(sqlite3.c) $(rs_lib_dbg_loadable_cpy)
	$(CC) -g -I./src/ -I$(sqlite_src) \
	$(LOADABLE_CFLAGS) \
	$(C_TARGET) \
	$(sysroot_option) \
	$(ext_files) $(rs_lib_dbg_loadable_cpy) -o $@

# Build a SQLite CLI that pre-loads cr-sqlite.
# Useful for debugging.
$(TARGET_SQLITE3): $(prefix) $(TARGET_SQLITE3_EXTRA_C) $(rs_lib_dbg_static_cpy) $(shell.c) $(ext_files)
	$(CC) -g \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_OMIT_LOAD_EXTENSION=1 \
	-DSQLITE_EXTRA_INIT=core_init \
	-DSQLITE_ENABLE_BYTECODE_VTAB \
	-I./src/ -I$(sqlite_src) \
	$(TARGET_SQLITE3_EXTRA_C) $(shell.c) $(ext_files) $(rs_lib_dbg_static_cpy) \
	$(LDLIBS) -o $@

# Build the SQLite library w/ cr-sqlite statically linked in
$(TARGET_STATIC): $(prefix) $(ext_files) $(sqlite3.c) $(rs_lib_static_cpy)
	$(CC) -g \
	-DHAVE_GETHOSTUUID=0 \
	-I./src/ -I$(sqlite_src) \
	$(STATIC_CFLAGS) \
	$(C_TARGET) \
	$(sysroot_option) \
	$(ext_files)
	mkdir -p $(prefix)/temp
	rm -f $(prefix)/temp/*
	mv *.o $(prefix)/temp
	cd $(prefix)/temp && ar -x ../libcrsql_$(bundle)-static.a && ar -rc crsqlite.a *.o && mv crsqlite.a ../crsqlite-$(CI_MAYBE_TARGET).a

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

$(TARGET_SQLITE3_EXTRA_C): $(prefix) $(sqlite3.c) src/core_init.c
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
	ubsan analyzer fuzz asan static

FORCE: ;
