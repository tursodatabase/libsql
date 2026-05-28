#!/usr/bin/make
#
# libsql-specific build targets that live alongside upstream's main.mk.
# Loaded from Makefile.in after main.mk so all of main.mk's variables
# (libsqlite3.LIB, libsqlite3.SO, install-dir.lib, T.exe, etc.) are
# defined.
#
# Provides:
#   - libsql$(T.exe)               alias for the sqlite3 shell binary
#   - liblibsql.LIB / liblibsql.SO aliases for libsqlite3
#   - libsql.pc                    pkg-config file
#   - liblibsql_wasm               cargo-built libsql wasm UDF runtime
#   - liblibsql_install            install liblibsql.{a,so} to libdir
#   - liblibsql_wasm_install       install liblibsql_wasm.{a,so} to libdir
#   - lib_install                  libsqlite3 install + liblibsql_install
#   - libsqlapi / testlibsql       cargo invocations against the libsql crates
#   - rusttest / rusttestwasm      cargo invocations against test/rust_suite

# Path to the libsql Rust workspace and the wasm-bindings crate.
APITOP = $(TOP)/crates
WBTOP  = $(TOP)/crates/wasmtime-bindings
WBSRC  = $(WBTOP)/src/lib.rs

# Wire OPT_WASM_RUNTIME (y/d/n from configure) into the legacy target
# selectors used below.
ifeq ($(OPT_WASM_RUNTIME),y)
OPT_WASM_RUNTIME_LIBRARY_TARGET = liblibsql_wasm
OPT_WASM_RUNTIME_INSTALL_TARGET = liblibsql_wasm_install
OPT_STATIC_LIBLIBSQL_WASM       = $(TOP)/.libs/liblibsql_wasm.a
else ifeq ($(OPT_WASM_RUNTIME),d)
OPT_WASM_RUNTIME_LIBRARY_TARGET = liblibsql_wasm
OPT_WASM_RUNTIME_INSTALL_TARGET = liblibsql_wasm_install
OPT_STATIC_LIBLIBSQL_WASM       =
TLIBS += -L$(WBTOP)/../target/release -llibsql_wasm
else
OPT_WASM_RUNTIME_LIBRARY_TARGET =
OPT_WASM_RUNTIME_INSTALL_TARGET =
OPT_STATIC_LIBLIBSQL_WASM       =
endif

#
# libsql shell binary: cheaply alias the upstream sqlite3 binary.
#
libsql$(T.exe): sqlite3$(T.exe)
	cp $< $@
all: libsql$(T.exe)

#
# liblibsql.a / liblibsql.so are byte-identical copies of libsqlite3.
# We copy rather than symlink so that DESTDIR-based installs (used by CI)
# do not depend on the link target resolving inside the staging tree.
#
liblibsql.LIB = liblibsql$(T.lib)
liblibsql.SO  = liblibsql$(T.dll)

$(liblibsql.LIB): $(libsqlite3.LIB)
	cp $< $@
$(liblibsql.LIB)-1: $(liblibsql.LIB)
$(liblibsql.LIB)-0 $(liblibsql.LIB)-:

$(liblibsql.SO): $(libsqlite3.SO)
	cp $< $@
$(liblibsql.SO)-1: $(liblibsql.SO)
$(liblibsql.SO)-0 $(liblibsql.SO)-:

liblibsql.la: $(liblibsql.LIB)-$(ENABLE_STATIC) $(liblibsql.SO)-$(ENABLE_SHARED)
all: liblibsql.la

#
# libsql.pc - generated from libsql.pc.in, same idea as sqlite3.pc.
#
libsql.pc: $(TOP)/libsql.pc.in Makefile
	sed -e 's,@prefix@,$(prefix),g' \
	    -e 's,@exec_prefix@,$(exec_prefix),g' \
	    -e 's,@libdir@,$(libdir),g' \
	    -e 's,@includedir@,$(includedir),g' \
	    -e 's,@PACKAGE_VERSION@,$(PACKAGE_VERSION),g' \
	    $(TOP)/libsql.pc.in > $@

#
# libsql wasm UDF runtime: built by cargo in crates/wasmtime-bindings.
# cargo's target directory depends on whether CARGO_TARGET_DIR is set in
# the environment and on which Cargo.toml above us declares the
# workspace, so query it from cargo rather than hard-coding a path.
#
liblibsql_wasm: $(WBSRC)
	cd $(WBTOP) && cargo build --release --lib
	mkdir -p $(TOP)/.libs
	cargo_target_dir=$$(cd $(WBTOP) && cargo metadata --no-deps --format-version 1 \
		| sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'); \
	cp $$cargo_target_dir/release/liblibsql_wasm.* $(TOP)/.libs/

#
# Installation.
#
liblibsql_wasm_install: liblibsql_wasm
	$(INSTALL) -d $(DESTDIR)$(libdir)
	$(INSTALL) -m 0644 $(TOP)/.libs/liblibsql_wasm.so $(DESTDIR)$(libdir)/liblibsql_wasm.so.0.0
	$(INSTALL) -m 0644 $(TOP)/.libs/liblibsql_wasm.a  $(DESTDIR)$(libdir)/liblibsql_wasm.a
	ln -fs $(DESTDIR)$(libdir)/liblibsql_wasm.so.0.0 $(DESTDIR)$(libdir)/liblibsql_wasm.so.0
	ln -fs $(DESTDIR)$(libdir)/liblibsql_wasm.so.0.0 $(DESTDIR)$(libdir)/liblibsql_wasm.so

liblibsql_install-1: $(install-dir.lib) $(liblibsql.LIB)
	$(INSTALL.noexec) $(liblibsql.LIB) $(DESTDIR)$(libdir)
liblibsql_install-0 liblibsql_install-:

liblibsql_install-so-1: $(install-dir.lib) $(liblibsql.SO)
	$(INSTALL) $(liblibsql.SO) $(DESTDIR)$(libdir)
liblibsql_install-so-0 liblibsql_install-so-:

liblibsql_install: \
		liblibsql_install-$(ENABLE_STATIC) \
		liblibsql_install-so-$(ENABLE_SHARED) \
		$(OPT_WASM_RUNTIME_INSTALL_TARGET)

lib_install: install-lib install-so liblibsql_install

#
# Rust test targets (run against the in-tree libsql/libsqlite3).
#
libsqlapi: sqlite3.h
	cd $(APITOP) && LIBSQL_SRC_DIR=$(TOP) cargo test --all --release --lib

testlibsql: sqlite3.h
	cd $(APITOP) && LIBSQL_SRC_DIR=$(TOP) \
		cargo test --all --all-targets --all-features && \
		cargo test --all --doc --all-features && \
		cargo install cargo-hack && cargo hack check --each-feature --no-dev-deps

rusttest: sqlite3.h $(liblibsql.LIB) $(liblibsql.SO)
	( cd test/rust_suite; \
		SQLITE3_STATIC=1 SQLITE3_INCLUDE_DIR=$(TOP) \
		LD_LIBRARY_PATH=$(TOP) SQLITE3_LIB_DIR=$(TOP) \
		cargo test )

rusttestwasm: sqlite3.h $(liblibsql.LIB) $(liblibsql.SO) liblibsql_wasm
	( cd test/rust_suite; \
		SQLITE3_STATIC=1 SQLITE3_INCLUDE_DIR=$(TOP) \
		LD_LIBRARY_PATH=$(TOP) SQLITE3_LIB_DIR=$(TOP) \
		cargo test --features wasm,udf )

clean: clean-libsql
clean-libsql:
	rm -f libsql$(T.exe) liblibsql$(T.lib) liblibsql$(T.dll) libsql.pc
	rm -rf $(TOP)/.libs
