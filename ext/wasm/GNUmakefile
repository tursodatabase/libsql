#######################################################################
# This GNU makefile drives the build of the sqlite3 WASM
# components. It is not part of the canonical build process.
#
# This build assumes a Linux platform and is not intended for
# general-purpose client-level use, except for creating builds with
# custom configurations. It is primarily intended for the sqlite
# project's own development of the JS/WASM components.
#
# Primary targets:
#
#  default, all = build in dev mode
#
#  o0, o1, o2, o3, os, oz = full clean/rebuild with the -Ox level indicated
#      by the target name. Rebuild is necessary for all components to get
#      the desired optimization level.
#
#  dist = create end user deliverables. Add dist.build=oX to build
#      with a specific optimization level, where oX is one of the
#      above-listed o? target names.
#
#  clean = clean up
########################################################################
SHELL := $(shell which bash 2>/dev/null)
MAKEFILE := $(lastword $(MAKEFILE_LIST))
CLEAN_FILES :=
DISTCLEAN_FILES := ./--dummy--
default: all
release: oz

# Emscripten SDK home dir and related binaries...
EMSDK_HOME ?= $(word 1,$(wildcard $(HOME)/emsdk $(HOME)/src/emsdk))
emcc.bin ?= $(word 1,$(wildcard $(EMSDK_HOME)/upstream/emscripten/emcc) $(shell which emcc))
ifeq (,$(emcc.bin))
  $(error Cannot find emcc.)
endif

wasm-strip ?= $(shell which wasm-strip 2>/dev/null)
ifeq (,$(filter clean,$(MAKECMDGOALS)))
ifeq (,$(wasm-strip))
  $(info WARNING: *******************************************************************)
  $(info WARNING: builds using -O2/-O3/-Os/-Oz will minify WASM-exported names,)
  $(info WARNING: breaking _All The Things_. The workaround for that is to build)
  $(info WARNING: with -g3 (which explodes the file size) and then strip the debug)
  $(info WARNING: info after compilation, using wasm-strip, to shrink the wasm file.)
  $(info WARNING: wasm-strip was not found in the PATH so we cannot strip those.)
  $(info WARNING: If this build uses any optimization level higher than -O1 then)
  $(info WARNING: the ***resulting JS code WILL NOT BE USABLE***.)
  $(info WARNING: wasm-strip is part of the wabt package:)
  $(info WARNING:    https://github.com/WebAssembly/wabt)
  $(info WARNING: on Ubuntu-like systems it can be installed with:)
  $(info WARNING:    sudo apt install wabt)
  $(info WARNING: *******************************************************************)
endif
endif # 'make clean' check

ifeq (,$(wasm-strip))
  maybe-wasm-strip = echo "not wasm-stripping"
else
  maybe-wasm-strip = $(wasm-strip)
endif

dir.top := ../..
# Reminder: some Emscripten flags require absolute paths but we want
# relative paths for most stuff simply to reduce noise. The
# $(abspath...) GNU make function can transform relative paths to
# absolute.
dir.wasm := $(patsubst %/,%,$(dir $(MAKEFILE)))
dir.api := api
dir.jacc := jaccwabyt
dir.common := common
dir.fiddle := fiddle
dir.tool := $(dir.top)/tool
########################################################################
# dir.dout = output dir for deliverables.
#
# MAINTENANCE REMINDER: the output .js and .wasm files of emcc must be
# in _this_ dir, rather than a subdir, or else parts of the generated
# code get confused and cannot load property. Specifically, when X.js
# loads X.wasm, whether or not X.js uses the correct path for X.wasm
# depends on how it's loaded: an HTML script tag will resolve it
# intuitively, whereas a Worker's call to importScripts() will not.
# That's a fundamental incompatibility with how URL resolution in
# JS happens between those two contexts. See:
#
# https://zzz.buzz/2017/03/14/relative-uris-in-web-development/
#
# We unfortunately have no way, from Worker-initiated code, to
# automatically resolve the path from X.js to X.wasm.
#
# We have an "only slightly unsightly" solution for our main builds
# but it does not work for the WASMFS builds, so those builds have to
# be built to _this_ directory and can only run when the client app is
# loaded from the same directory.
dir.dout := $(dir.wasm)/jswasm
# dir.tmp = output dir for intermediary build files, as opposed to
# end-user deliverables.
dir.tmp := $(dir.wasm)/bld
CLEAN_FILES += $(dir.tmp)/* $(dir.dout)/*
ifeq (,$(wildcard $(dir.dout)))
  dir._tmp := $(shell mkdir -p $(dir.dout))
endif
ifeq (,$(wildcard $(dir.tmp)))
  dir._tmp := $(shell mkdir -p $(dir.tmp))
endif

cflags.common :=  -I. -I.. -I$(dir.top)
CLEAN_FILES += *~ $(dir.jacc)/*~ $(dir.api)/*~ $(dir.common)/*~
emcc.WASM_BIGINT ?= 1
sqlite3.c := $(dir.top)/sqlite3.c
sqlite3.h := $(dir.top)/sqlite3.h
SQLITE_OPT = \
  -DSQLITE_ENABLE_FTS4 \
  -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_EXPLAIN_COMMENTS \
  -DSQLITE_ENABLE_UNKNOWN_SQL_FUNCTION \
  -DSQLITE_ENABLE_STMTVTAB \
  -DSQLITE_ENABLE_DBPAGE_VTAB \
  -DSQLITE_ENABLE_DBSTAT_VTAB \
  -DSQLITE_ENABLE_BYTECODE_VTAB \
  -DSQLITE_ENABLE_OFFSET_SQL_FUNC \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_UTF16 \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DSQLITE_OMIT_WAL \
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_TEMP_STORE=3 \
  -DSQLITE_OS_KV_OPTIONAL=1 \
  '-DSQLITE_DEFAULT_UNIX_VFS="unix-none"' \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_WASM_ENABLE_C_TESTS
# ^^^ most flags are set in sqlite3-wasm.c but we need them
# made explicit here for building speedtest1.c.

ifneq (,$(filter release,$(MAKECMDGOALS)))
emcc_opt ?= -Oz -flto
else
emcc_opt ?= -O0
# ^^^^ build times for -O levels higher than 0 are painful at
# dev-time.
endif
# When passing emcc_opt from the CLI, += and re-assignment have no
# effect, so emcc_opt+=-g3 doesn't work. So...
emcc_opt_full := $(emcc_opt) -g3
# ^^^ ALWAYS use -g3. See below for why.
#
# ^^^ -flto improves runtime speed at -O0 considerably but doubles
# build time.
#
# ^^^^ -O3, -Oz, -Os minify symbol names and there appears to be no
# way around that except to use -g3, but -g3 causes the binary file
# size to absolutely explode (approx. 5x larger). This minification
# utterly breaks the resulting module, making it unsable except as
# self-contained/self-referential-only code, as ALL of the exported
# symbols get minified names.
#
# However, we have an option for using -Oz or -Os:
#
# Build with (-Os -g3) or (-Oz -g3) then use wasm-strip, from the wabt
# tools package (https://github.com/WebAssembly/wabt), to strip the
# debugging symbols. That results in a small build with unmangled
# symbol names. -Oz gives ever-so-slightly better compression than
# -Os: not quite 1% in some completely unscientific tests. Runtime
# speed for the unit tests is all over the place either way so it's
# difficult to say whether -Os gives any speed benefit over -Oz.
#
# (Much later: -O2 consistently gives the best speeds.)
########################################################################


$(sqlite3.c) $(sqlite3.h):
	$(MAKE) -C $(dir.top) sqlite3.c

.PHONY: clean distclean
clean:
	-rm -f $(CLEAN_FILES)
distclean: clean
	-rm -f $(DISTCLEAN_FILES)

ifeq (release,$(filter release,$(MAKECMDGOALS)))
  ifeq (,$(wasm-strip))
    $(error Cannot make release-quality binary because wasm-strip is not available. \
            See notes in the warning above)
  endif
else
  $(info Development build. Use '$(MAKE) release' for a smaller release build.)
endif

bin.version-info := $(dir.wasm)/version-info
# ^^^^ NOT in $(dir.tmp) because we need it to survive the cleanup
# process for the dist build to work properly.
$(bin.version-info): $(dir.wasm)/version-info.c $(sqlite3.h) $(MAKEFILE)
	$(CC) -O0 -I$(dir.top) -o $@ $<
DISTCLEAN_FILES += $(bin.version-info)

bin.stripccomments := $(dir.tool)/stripccomments
$(bin.stripccomments): $(bin.stripccomments).c $(MAKEFILE)
	$(CC) -o $@ $<
DISTCLEAN_FILES += $(bin.stripccomments)

EXPORTED_FUNCTIONS.api.in := $(abspath $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api)
EXPORTED_FUNCTIONS.api := $(dir.tmp)/EXPORTED_FUNCTIONS.api
$(EXPORTED_FUNCTIONS.api): $(EXPORTED_FUNCTIONS.api.in) $(MAKEFILE)
	cat $(EXPORTED_FUNCTIONS.api.in) > $@

sqlite3-license-version.js := $(dir.tmp)/sqlite3-license-version.js
sqlite3-license-version-header.js := $(dir.api)/sqlite3-license-version-header.js
sqlite3-api-build-version.js := $(dir.tmp)/sqlite3-api-build-version.js
# sqlite3-api.jses = the list of JS files which make up $(sqlite3-api.js), in
# the order they need to be assembled.
sqlite3-api.jses := $(sqlite3-license-version.js)
sqlite3-api.jses += $(dir.api)/sqlite3-api-prologue.js
sqlite3-api.jses += $(dir.common)/whwasmutil.js
sqlite3-api.jses += $(dir.jacc)/jaccwabyt.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-glue.js
sqlite3-api.jses += $(sqlite3-api-build-version.js)
sqlite3-api.jses += $(dir.api)/sqlite3-api-oo1.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-worker1.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-opfs.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-cleanup.js

# "External" API files which are part of our distribution
# but not part of the sqlite3-api.js amalgamation.
SOAP.js := $(dir.api)/sqlite3-opfs-async-proxy.js
sqlite3-worker1.js := $(dir.api)/sqlite3-worker1.js
sqlite3-worker1-promiser.js := $(dir.api)/sqlite3-worker1-promiser.js
define CP_XAPI
sqlite3-api.ext.jses += $$(dir.dout)/$$(notdir $(1))
$$(dir.dout)/$$(notdir $(1)): $(1) $$(MAKEFILE)
	cp $$< $$@
endef
$(foreach X,$(SOAP.js) $(sqlite3-worker1.js) $(sqlite3-worker1-promiser.js),\
  $(eval $(call CP_XAPI,$(X))))
all: $(sqlite3-api.ext.jses)

sqlite3-api.js := $(dir.tmp)/sqlite3-api.js
$(sqlite3-api.js): $(sqlite3-api.jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(sqlite3-api.jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@

$(sqlite3-api-build-version.js): $(bin.version-info) $(MAKEFILE)
	@echo "Making $@..."
	@{ \
  echo 'self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){'; \
	echo -n '  sqlite3.version = '; \
  $(bin.version-info) --json; \
  echo ';'; \
	echo '});'; \
  } > $@

########################################################################
# --post-js and --pre-js are emcc flags we use to append/prepend JS to
# the generated emscripten module file.
pre-js.js := $(dir.api)/pre-js.js
post-js.js := $(dir.tmp)/post-js.js
post-jses := \
  $(dir.api)/post-js-header.js \
  $(sqlite3-api.js) \
  $(dir.api)/post-js-footer.js
$(post-js.js): $(post-jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(post-jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@
extern-post-js.js := $(dir.api)/extern-post-js.js
extern-pre-js.js := $(dir.api)/extern-pre-js.js
pre-post-common.flags := \
  --post-js=$(post-js.js) \
  --extern-post-js=$(extern-post-js.js) \
  --extern-pre-js=$(sqlite3-license-version.js)
pre-post-jses.deps := $(post-js.js) \
  $(extern-post-js.js) $(extern-pre-js.js) $(sqlite3-license-version.js)
$(sqlite3-license-version.js): $(sqlite3.h) $(sqlite3-license-version-header.js) $(MAKEFILE)
	@echo "Making $@..."; { \
    cat $(sqlite3-license-version-header.js); \
    echo '/*'; \
    echo '** This code was built from sqlite3 version...'; \
    echo "** "; \
    awk -e '/define SQLITE_VERSION/{$$1=""; print "**" $$0}' \
        -e '/define SQLITE_SOURCE_ID/{$$1=""; print "**" $$0}' $(sqlite3.h); \
    echo '*/'; \
   } > $@

########################################################################
# call-make-pre-js creates rules for pre-js-$(1).js. $1 = the base
# name of the JS file on whose behalf this pre-js is for.
define call-make-pre-js
pre-post-$(1).flags ?=
$$(dir.tmp)/pre-js-$(1).js: $$(pre-js.js) $$(MAKEFILE)
	cp $$(pre-js.js) $$@
	@if [ sqlite3-wasmfs = $(1) ]; then \
		echo "delete Module[xNameOfInstantiateWasm] /*for WASMFS build*/;"; \
	elif [ sqlite3 != $(1) ]; then \
		echo "Module[xNameOfInstantiateWasm].uri = '$(1).wasm';"; \
	fi >> $$@
pre-post-$(1).deps := $$(pre-post-jses.deps) $$(dir.tmp)/pre-js-$(1).js
pre-post-$(1).flags += --pre-js=$$(dir.tmp)/pre-js-$(1).js
endef
#$(error $(call call-make-pre-js,sqlite3-wasmfs))
# /post-js and pre-js
########################################################################

########################################################################
# emcc flags for .c/.o/.wasm/.js.
emcc.flags :=
#emcc.flags += -v # _very_ loud but also informative about what it's doing
# -g3 is needed to keep -O2 and higher from creating broken JS via
# minification.

########################################################################
# emcc flags for .c/.o.
emcc.cflags :=
emcc.cflags += -std=c99 -fPIC
# -------------^^^^^^^^ we currently need c99 for WASM-specific sqlite3 APIs.
emcc.cflags += -I. -I$(dir.top)

########################################################################
# emcc flags specific to building the final .js/.wasm file...
emcc.jsflags := -fPIC
emcc.jsflags += --minify 0
emcc.jsflags += --no-entry
emcc.jsflags += -sMODULARIZE
emcc.jsflags += -sSTRICT_JS
emcc.jsflags += -sDYNAMIC_EXECUTION=0
emcc.jsflags += -sNO_POLYFILL
emcc.jsflags += -sEXPORTED_FUNCTIONS=@$(EXPORTED_FUNCTIONS.api)
emcc.exportedRuntimeMethods := \
    -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory
    # FS ==> stdio/POSIX I/O proxies
    # wasmMemory ==> required by our code for use with -sIMPORTED_MEMORY
emcc.jsflags += $(emcc.exportedRuntimeMethods)
emcc.jsflags += -sUSE_CLOSURE_COMPILER=0
emcc.jsflags += -sIMPORTED_MEMORY
emcc.environment := -sENVIRONMENT=web,worker
########################################################################
# -sINITIAL_MEMORY: How much memory we need to start with is governed
# at least in part by whether -sALLOW_MEMORY_GROWTH is enabled. If so,
# we can start with less. If not, we need as much as we'll ever
# possibly use (which, of course, we can't know for sure).  Note,
# however, that speedtest1 shows that performance for even moderate
# workloads MAY suffer considerably if we start small and have to grow
# at runtime. e.g. OPFS-backed (speedtest1 --size 75) take MAY take X
# time with 16mb+ memory and 3X time when starting with 8MB. However,
# such test results are inconsistent due to browser internals which
# are opaque to us.
emcc.jsflags += -sALLOW_MEMORY_GROWTH
emcc.INITIAL_MEMORY.128 := 13107200
emcc.INITIAL_MEMORY.96  := 100663296
emcc.INITIAL_MEMORY.64  := 64225280
emcc.INITIAL_MEMORY.32  := 33554432
emcc.INITIAL_MEMORY.16  := 16777216
emcc.INITIAL_MEMORY.8   := 8388608
emcc.INITIAL_MEMORY ?= 16
ifeq (,$(emcc.INITIAL_MEMORY.$(emcc.INITIAL_MEMORY)))
$(error emcc.INITIAL_MEMORY must be one of: 8, 16, 32, 64, 96, 128 (megabytes))
endif
emcc.jsflags += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.$(emcc.INITIAL_MEMORY))
# /INITIAL_MEMORY
########################################################################

emcc.jsflags += $(emcc.environment)
#emcc.jsflags += -sTOTAL_STACK=4194304

sqlite3.js.init-func := sqlite3InitModule
# ^^^^ $(sqlite3.js.init-func) symbol name is hard-coded in
# $(extern-post-js.js) as well as in numerous docs. If changed, it
# needs to be globally modified in *.js and all related documentation.

emcc.jsflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
emcc.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
#emcc.jsflags += -sSTRICT # fails due to missing __syscall_...()
#emcc.jsflags += -sALLOW_UNIMPLEMENTED_SYSCALLS
#emcc.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#emcc.jsflags += -sABORTING_MALLOC
emcc.jsflags += -sALLOW_TABLE_GROWTH
# -sALLOW_TABLE_GROWTH is required for installing new SQL UDFs
emcc.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
#emcc.jsflags += -sSTANDALONE_WASM # causes OOM errors, not sure why
# https://lld.llvm.org/WebAssembly.html
emcc.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
emcc.jsflags += -sLLD_REPORT_UNDEFINED
#emcc.jsflags += --allow-undefined
#emcc.jsflags += --import-undefined
#emcc.jsflags += --unresolved-symbols=import-dynamic --experimental-pic
#emcc.jsflags += --experimental-pic --unresolved-symbols=ingore-all --import-undefined
#emcc.jsflags += --unresolved-symbols=ignore-all
emcc.jsflags += -sWASM_BIGINT=$(emcc.WASM_BIGINT)

########################################################################
# -sMEMORY64=1 fails to load, erroring with:
#  invalid memory limits flags 0x5
#    (enable via --experimental-wasm-memory64)
#
# ^^^^ MEMORY64=2 builds and loads but dies when we do things like:
#
#  new Uint8Array(wasm.heap8u().buffer, ptr, n)
#
# because ptr is now a BigInt, so is invalid for passing to arguments
# which have strict must-be-a-Number requirements.
########################################################################


########################################################################
# -sSINGLE_FILE:
# https://github.com/emscripten-core/emscripten/blob/main/src/settings.js#L1704
# -sSINGLE_FILE=1 would be really nice but we have to build with -g3
# for -O2 and higher to work (else minification breaks the code) and
# cannot wasm-strip the binary before it gets encoded into the JS
# file. The result is that the generated JS file is, because of the -g3
# debugging info, _huge_.
########################################################################

########################################################################
# AN EXPERIMENT: undocumented Emscripten feature: if the target file
# extension is "mjs", it defaults to ES6 module builds:
# https://github.com/emscripten-core/emscripten/issues/14383
ifeq (,$(filter esm,$(MAKECMDGOALS)))
sqlite3.js.ext := js
else
esm.deps := $(filter-out esm,$(MAKECMDGOALS))
esm: $(if $(esm.deps),$(esm.deps),all)
sqlite3.js.ext := mjs
endif
# /esm
########################################################################
sqlite3.js := $(dir.dout)/sqlite3.$(sqlite3.js.ext)
sqlite3.wasm := $(dir.dout)/sqlite3.wasm
sqlite3-wasm.c := $(dir.api)/sqlite3-wasm.c
# sqlite3-wasm.o vs sqlite3-wasm.c: building against the latter
# (predictably) results in a slightly faster binary, but we're close
# enough to the target speed requirements that the 500ms makes a
# difference. Thus we build all binaries against sqlite3-wasm.c
# instead of building a shared copy of sqlite3-wasm.o.
$(eval $(call call-make-pre-js,sqlite3))
$(sqlite3.js):
$(sqlite3.js): $(MAKEFILE) $(sqlite3.wasm.obj) \
    $(EXPORTED_FUNCTIONS.api) \
    $(pre-post-sqlite3.deps)
	@echo "Building $@ ..."
	$(emcc.bin) -o $@ $(emcc_opt_full) $(emcc.flags) \
    $(emcc.jsflags) $(pre-post-common.flags) $(pre-post-sqlite3.flags) \
    $(cflags.common) $(SQLITE_OPT) $(sqlite3-wasm.c)
	chmod -x $(sqlite3.wasm)
	$(maybe-wasm-strip) $(sqlite3.wasm)
	@ls -la $@ $(sqlite3.wasm)
$(sqlite3.wasm): $(sqlite3.js)
CLEAN_FILES += $(sqlite3.js) $(sqlite3.wasm)
all: $(sqlite3.js)
wasm: $(sqlite3.js)
# End main Emscripten-based module build
########################################################################

########################################################################
# batch-runner.js...
dir.sql := sql
speedtest1 := ../../speedtest1
speedtest1.c := ../../test/speedtest1.c
speedtest1.sql := $(dir.sql)/speedtest1.sql
speedtest1.cliflags := --size 25 --big-transactions
$(speedtest1):
	$(MAKE) -C ../.. speedtest1
$(speedtest1.sql): $(speedtest1) $(MAKEFILE)
	$(speedtest1) $(speedtest1.cliflags) --script $@
batch-runner.list: $(MAKEFILE) $(speedtest1.sql) $(dir.sql)/000-mandelbrot.sql
	bash split-speedtest1-script.sh $(dir.sql)/speedtest1.sql
	ls -1 $(dir.sql)/*.sql | grep -v speedtest1.sql | sort > $@
clean-batch:
	rm -f batch-runner.list $(dir.sql)/speedtest1*.sql
# ^^^ we don't do this along with 'clean' because we clean/rebuild on
# a regular basis with different -Ox flags and rebuilding the batch
# pieces each time is an unnecessary time sink.
batch: batch-runner.list
all: batch
# end batch-runner.js
########################################################################
# speedtest1.js...
# speedtest1-common.eflags = emcc flags used by multiple builds of speedtest1
# speedtest1.eflags = emcc flags used by main build of speedtest1
speedtest1-common.eflags := $(emcc_opt_full)
speedtest1.eflags :=
speedtest1.eflags += -sENVIRONMENT=web
speedtest1.eflags += -sALLOW_MEMORY_GROWTH
speedtest1.eflags += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.$(emcc.INITIAL_MEMORY))
speedtest1-common.eflags += -sINVOKE_RUN=0
speedtest1-common.eflags += --no-entry
#speedtest1-common.eflags += -flto
speedtest1-common.eflags += -sABORTING_MALLOC
speedtest1-common.eflags += -sSTRICT_JS
speedtest1-common.eflags += -sMODULARIZE
speedtest1-common.eflags += -Wno-limited-postlink-optimizations
EXPORTED_FUNCTIONS.speedtest1 := $(abspath $(dir.tmp)/EXPORTED_FUNCTIONS.speedtest1)
speedtest1-common.eflags += -sEXPORTED_FUNCTIONS=@$(EXPORTED_FUNCTIONS.speedtest1)
speedtest1-common.eflags += $(emcc.exportedRuntimeMethods)
speedtest1-common.eflags += -sALLOW_TABLE_GROWTH
speedtest1-common.eflags += -sDYNAMIC_EXECUTION=0
speedtest1-common.eflags += --minify 0
speedtest1-common.eflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
speedtest1-common.eflags += -sWASM_BIGINT=$(emcc.WASM_BIGINT)
speedtest1-common.eflags += $(pre-post-common.flags) 
speedtest1.exit-runtime0 := -sEXIT_RUNTIME=0
speedtest1.exit-runtime1 := -sEXIT_RUNTIME=1
# Re -sEXIT_RUNTIME=1 vs 0: if it's 1 and speedtest1 crashes, we get
# this error from emscripten:
#
# > native function `free` called after runtime exit (use
# NO_EXIT_RUNTIME to keep it alive after main() exits))
#
# If it's 0 and it crashes, we get:
#
# > stdio streams had content in them that was not flushed. you should
# set EXIT_RUNTIME to 1 (see the FAQ), or make sure to emit a newline
# when you printf etc.
#
# and pending output is not flushed because it didn't end with a
# newline (by design). The lesser of the two evils seems to be
# -sEXIT_RUNTIME=1 but we need EXIT_RUNTIME=0 for the worker-based app
# which runs speedtest1 multiple times.

$(EXPORTED_FUNCTIONS.speedtest1): $(EXPORTED_FUNCTIONS.api)
	@echo "Making $@ ..."
	@{ echo _wasm_main; cat $(EXPORTED_FUNCTIONS.api); } > $@
speedtest1.js := $(dir.dout)/speedtest1.js
speedtest1.wasm := $(subst .js,.wasm,$(speedtest1.js))
speedtest1.cflags := $(cflags.common) -DSQLITE_SPEEDTEST1_WASM
speedtest1.cses := $(speedtest1.c) $(sqlite3-wasm.c)
$(eval $(call call-make-pre-js,speedtest1))
$(speedtest1.js): $(MAKEFILE) $(speedtest1.cses) \
    $(pre-post-speedtest1.deps) \
    $(EXPORTED_FUNCTIONS.speedtest1)
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(speedtest1.eflags) $(speedtest1-common.eflags) $(speedtest1.cflags) \
        $(pre-post-speedtest1.flags) \
        $(SQLITE_OPT) \
        $(speedtest1.exit-runtime0) \
        -o $@ $(speedtest1.cses) -lm
	$(maybe-wasm-strip) $(speedtest1.wasm)
	ls -la $@ $(speedtest1.wasm)

speedtest1: $(speedtest1.js)
all: speedtest1
CLEAN_FILES += $(speedtest1.js) $(speedtest1.wasm)
# end speedtest1.js
########################################################################

########################################################################
# Convenience rules to rebuild with various -Ox levels. Much
# experimentation shows -O2 to be the clear winner in terms of speed.
# Note that build times with anything higher than -O0 are somewhat
# painful.

.PHONY: o0 o1 o2 o3 os oz
o-xtra := -flto
# ^^^^ -flto can have a considerably performance boost at -O0 but
# doubles the build time and seems to have negligible effect on
# higher optimization levels.
o0: clean
	$(MAKE) -e "emcc_opt=-O0"
o1: clean
	$(MAKE) -e "emcc_opt=-O1 $(o-xtra)"
o2: clean
	$(MAKE) -e "emcc_opt=-O2 $(o-xtra)"
o3: clean
	$(MAKE) -e "emcc_opt=-O3 $(o-xtra)"
os: clean
	@echo "WARNING: -Os can result in a build with mysteriously missing pieces!"
	$(MAKE) -e "emcc_opt=-Os $(o-xtra)"
oz: clean
	$(MAKE) -e "emcc_opt=-Oz $(o-xtra)"

########################################################################
# Sub-makes...

include fiddle.make

# Only add wasmfs if wasmfs.enable=1 or we're running (dist)clean
wasmfs.enable ?= $(if $(filter %clean,$(MAKECMDGOALS)),1,0)
ifeq (1,$(wasmfs.enable))
# wasmfs build disabled 2022-10-19 per /chat discussion.
# OPFS-over-wasmfs was initially a stopgap measure and a convenient
# point of comparison for the OPFS sqlite3_vfs's performance, but it
# currently doubles our deliverables and build maintenance burden for
# little, if any, benefit.
#
########################################################################
# Some platforms do not support the WASMFS build. Raspberry Pi OS is one
# of them. As such platforms are discovered, add their (uname -m) name
# to PLATFORMS_WITH_NO_WASMFS to exclude the wasmfs build parts.
PLATFORMS_WITH_NO_WASMFS := aarch64 # add any others here
THIS_ARCH := $(shell /usr/bin/uname -m)
ifneq (,$(filter $(THIS_ARCH),$(PLATFORMS_WITH_NO_WASMFS)))
$(info This platform does not support the WASMFS build.)
HAVE_WASMFS := 0
else
HAVE_WASMFS := 1
include wasmfs.make
endif
endif
# /wasmfs
########################################################################

########################################################################
# Create deliverables:
ifneq (,$(filter dist,$(MAKECMDGOALS)))
include dist.make
endif

########################################################################
# Push files to public wasm-testing.sqlite.org server
wasm-testing.include = $(dir.dout) *.js *.html \
    batch-runner.list $(dir.sql) $(dir.common) $(dir.fiddle) $(dir.jacc)
wasm-testing.exclude = sql/speedtest1.sql
wasm-testing.dir     = /jail/sites/wasm-testing
wasm-testing.dest   ?= wasm-testing:$(wasm-testing.dir)
# ---------------------^^^^^^^^^^^^ ssh alias
.PHONY: push-testing
push-testing:
	rsync -z -e ssh --ignore-times --chown=stephan:www-data --group -r \
    $(patsubst %,--exclude=%,$(wasm-testing.exclude)) \
    $(wasm-testing.include) $(wasm-testing.dest)
	@echo "Updating gzipped copies..."; \
		ssh wasm-testing 'cd $(wasm-testing.dir) && bash .gzip' || \
    echo "SSH failed: it's likely that stale content will be served via old gzip files."

########################################################################
# If we find a copy of the sqlite.org/wasm docs checked out, copy
# certain files over to it, noting that some need automatable edits...
WDOCS.home ?= ../../../wdoc
.PHONY: update-docs
ifneq (,$(wildcard $(WDOCS.home)/api-index.md))
WDOCS.jswasm := $(WDOCS.home)/jswasm
update-docs: $(bin.stripccomments) $(sqlite3.js) $(sqlite3.wasm)
	@echo "Copying files to the /wasm docs. Be sure to use an -Oz build for this!"
	cp $(sqlite3.wasm) $(WDOCS.jswasm)/.
	$(bin.stripccomments) -k -k < $(sqlite3.js) \
		| sed -e '/^[ \t]*$$/d' > $(WDOCS.jswasm)/sqlite3.js
	cp demo-123.js demo-123.html demo-123-worker.html $(WDOCS.home)
	sed -n -e '/EXTRACT_BEGIN/,/EXTRACT_END/p' \
		module-symbols.html > $(WDOCS.home)/module-symbols.html
else
update-docs:
	@echo "Cannot find wasm docs checkout."; \
	echo "Pass WDOCS.home=/path/to/wasm/docs/checkout or edit this makefile to suit."; \
	exit 127
endif
# end /wasm docs
########################################################################
