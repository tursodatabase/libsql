########################################################################
# This GNU makefile drives the build of the sqlite3 WASM
# components. It is not part of the canonical build process.
########################################################################
SHELL := $(shell which bash 2>/dev/null)
MAKEFILE := $(lastword $(MAKEFILE_LIST))
all:
release: all

# Emscripten SDK home dir and related binaries...
EMSDK_HOME ?= $(word 1,$(wildcard $(HOME)/src/emsdk $(HOME)/emsdk))
emcc.bin ?= $(word 1,$(wildcard $(shell which emcc) $(EMSDK_HOME)/upstream/emscripten/emcc))
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
  $(info WARNING: the ***resulting WASM binary WILL NOT BE USABLE***.)
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
# Reminder: some Emscripten flags require absolute paths
dir.wasm := $(patsubst %/,%,$(dir $(abspath $(MAKEFILE))))
dir.api := api
dir.jacc := jaccwabyt
dir.common := common
dir.fiddle := fiddle
CLEAN_FILES := *~ $(dir.jacc)/*~ $(dir.api)/*~ $(dir.common)/*~
emcc_enable_bigint ?= 1
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

clean:
	-rm -f $(CLEAN_FILES)

ifeq (release,$(filter release,$(MAKECMDGOALS)))
  ifeq (,$(wasm-strip))
    $(error Cannot make release-quality binary because wasm-strip is not available. \
            See notes in the warning above)
  endif
else
  $(info Development build. Use '$(MAKE) release' for a smaller release build.)
endif

version-info := $(dir.wasm)/version-info
$(version-info): $(dir.wasm)/version-info.c $(sqlite3.h) $(MAKEFILE)
	$(CC) -O0 -I$(dir.top) -o $@ $<
CLEAN_FILES := $(version-info)

EXPORTED_FUNCTIONS.api.in := $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api

EXPORTED_FUNCTIONS.api: $(EXPORTED_FUNCTIONS.api.in) $(MAKEFILE)
	cat $(EXPORTED_FUNCTIONS.api.in) > $@
CLEAN_FILES += EXPORTED_FUNCTIONS.api

sqlite3-license-version.js := sqlite3-license-version.js
sqlite3-license-version-header.js := $(dir.api)/sqlite3-license-version-header.js
sqlite3-api-build-version.js := $(dir.api)/sqlite3-api-build-version.js
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

sqlite3-api.js := sqlite3-api.js
CLEAN_FILES += $(sqlite3-api.js)
CLEAN_FILES += $(sqlite3-license-version.js)
CLEAN_FILES += $(sqlite3-api-build-version.js)
$(sqlite3-api.js): $(sqlite3-api.jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(sqlite3-api.jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@

$(sqlite3-api-build-version.js): $(version-info)
	@echo "Making $@..."
	@{ \
  echo 'self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){'; \
	echo -n '  sqlite3.version = '; \
  $(version-info) --json; \
  echo ';'; \
	echo '});'; \
  } > $@

########################################################################
# --post-js and --pre-js are emcc flags we use to append/prepend JS to
# the generated emscripten module file.
pre-js.js := $(dir.api)/pre-js.js
post-js.js := post-js.js
CLEAN_FILES += $(post-js.js)
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
  --extern-pre-js=$(dir.wasm)/$(sqlite3-license-version.js)
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
pre-js-$(1).js: $$(pre-js.js) $$(MAKEFILE)
	cp $$(pre-js.js) $$@
	echo "Module[xInstantiateWasm].uri = '$(1).wasm';" >> $$@
CLEAN_FILES += pre-js-$(1).js
pre-post-$(1).deps := $$(pre-post-jses.deps) pre-js-$(1).js
pre-post-$(1).flags += --pre-js=pre-js-$(1).js
endef
#$(error $(call call-make-pre-js,sqlite3-wasmfs))
# /post-js and pre-js
########################################################################

########################################################################
# emcc flags for .c/.o/.wasm/.js.
emcc.flags =
#emcc.flags += -v # _very_ loud but also informative about what it's doing
# -g is needed to keep -O2 and higher from creating broken JS via
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
emcc.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.api
emcc.exportedRuntimeMethods := \
    -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
    # FS ==> stdio/POSIX I/O proxies
    # wasmMemory ==> required by our code for use with -sIMPORTED_MEMORY
    # allocateUTF8OnStack => for kvvfs internals
emcc.jsflags += $(emcc.exportedRuntimeMethods)
emcc.jsflags += -sUSE_CLOSURE_COMPILER=0
emcc.jsflags += -sIMPORTED_MEMORY
emcc.environment := -sENVIRONMENT=web,worker
emcc.jsflags += -sALLOW_MEMORY_GROWTH
# emcc: warning: USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code
#       slowly, see https://github.com/WebAssembly/design/issues/1271
#       [-Wpthreads-mem-growth]
emcc.jsflags += -sINITIAL_MEMORY=13107200
#emcc.jsflags += -sINITIAL_MEMORY=64225280
# ^^^^ 64MB is not enough for WASMFS/OPFS test runs using batch-runner.js
emcc.jsflags += $(emcc.environment)
#emcc.jsflags += -sTOTAL_STACK=4194304

sqlite3.js.init-func := sqlite3InitModule
# ^^^^ $(sqlite3.js.init-func) symbol name is hard-coded in $(extern-post-js.js)

emcc.jsflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
emcc.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
#emcc.jsflags += -sSTRICT # fails due to missing __syscall_...()
#emcc.jsflags += -sALLOW_UNIMPLEMENTED_SYSCALLS
#emcc.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#emcc.jsflags += -sABORTING_MALLOC
emcc.jsflags += -sALLOW_TABLE_GROWTH
emcc.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
#emcc.jsflags += -sSTANDALONE_WASM # causes OOM errors, not sure why
# https://lld.llvm.org/WebAssembly.html
emcc.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
emcc.jsflags += -sLLD_REPORT_UNDEFINED
#emcc.jsflags += --allow-undefined
emcc.jsflags += --import-undefined
#emcc.jsflags += --unresolved-symbols=import-dynamic --experimental-pic
#emcc.jsflags += --experimental-pic --unresolved-symbols=ingore-all --import-undefined
#emcc.jsflags += --unresolved-symbols=ignore-all
emcc.jsflags += -sWASM_BIGINT=$(emcc_enable_bigint)
# ^^^^ MEMORY64=1 fails to load, erroring with:
#  invalid memory limits flags 0x5
#    (enable via --experimental-wasm-memory64)
#
# ^^^^ MEMORY64=2 builds and loads but dies when we do things like:
#
#  new Uint8Array(heapWrappers().HEAP8U.buffer, ptr, n)
#
# because ptr is now a BigInt, so is invalid for passing to arguments
# which have strict must-be-a-Number requirements.
########################################################################


########################################################################
# -sSINGLE_FILE:
# https://github.com/emscripten-core/emscripten/blob/main/src/settings.js#L1704
# -sSINGLE_FILE=1 would be really nice but we have to build with -g
# for -O2 and higher to work (else minification breaks the code) and
# cannot wasm-strip the binary before it gets encoded into the JS
# file. The result is that the generated JS file is, because of the -g
# debugging info, _huge_.
########################################################################

########################################################################
# Maintenance reminder: the output .js and .wasm files of emcc must be
# in _this_ dir, rather than a subdir, or else parts of the generated
# code get confused and cannot load property (namely, the
# sqlite3.worker.js generated in conjunction with -sWASMFS).
sqlite3.js := sqlite3.js
sqlite3.wasm := sqlite3.wasm
sqlite3-wasm.o := $(dir.api)/sqlite3-wasm.o
$(sqlite3-wasm.o): emcc.cflags += $(SQLITE_OPT)
$(sqlite3-wasm.o): $(dir.top)/sqlite3.c
sqlite3-wasm.c := $(dir.api)/sqlite3-wasm.c
########################################################################
# call-wasm-c-compile sets up build rules
# for $1.o. $1 must be the name of a C file (with extension).
define call-wasm-c-compile
$(1).o := $$(subst .c,.o,$(1))
sqlite3.wasm.obj += $$($(1).o)
$$($(1).o): $$(MAKEFILE) $(1)
	$$(emcc.bin) $$(emcc_opt_full) $$(emcc.flags) $$(emcc.cflags) -c $(1) -o $$@
CLEAN_FILES += $$($(1).o)
endef
$(foreach c,$(sqlite3-wasm.c),$(eval $(call call-wasm-c-compile,$(c))))
$(eval $(call call-make-pre-js,sqlite3))
$(sqlite3.js): $(MAKEFILE) $(sqlite3.wasm.obj) \
    EXPORTED_FUNCTIONS.api \
    $(pre-post-sqlite3.deps)
	@echo "Building $@ ..."
	$(emcc.bin) -o $@ $(emcc_opt_full) $(emcc.flags) \
    $(emcc.jsflags) $(pre-post-common.flags) $(pre-post-sqlite3.flags) \
    $(sqlite3.wasm.obj)
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
speedtest1-common.eflags += -sINVOKE_RUN=0
speedtest1-common.eflags += --no-entry
#speedtest1-common.eflags += -flto
speedtest1-common.eflags += -sABORTING_MALLOC
speedtest1-common.eflags += -sINITIAL_MEMORY=128450560
speedtest1-common.eflags += -sSTRICT_JS
speedtest1-common.eflags += -sMODULARIZE
speedtest1-common.eflags += -Wno-limited-postlink-optimizations
speedtest1-common.eflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.speedtest1
speedtest1-common.eflags += $(emcc.exportedRuntimeMethods)
speedtest1-common.eflags += -sALLOW_TABLE_GROWTH
speedtest1-common.eflags += -sDYNAMIC_EXECUTION=0
speedtest1-common.eflags += --minify 0
speedtest1-common.eflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
speedtest1-common.eflags += -sWASM_BIGINT=$(emcc_enable_bigint)
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

EXPORTED_FUNCTIONS.speedtest1: EXPORTED_FUNCTIONS.api
	{ echo _wasm_main; cat EXPORTED_FUNCTIONS.api; } > $@
CLEAN_FILES += EXPORTED_FUNCTIONS.speedtest1
speedtest1.js := speedtest1.js
speedtest1.wasm := $(subst .js,.wasm,$(speedtest1.js))
speedtest1.cflags := \
  -I. -I.. -I$(dir.top) \
  -DSQLITE_SPEEDTEST1_WASM
speedtest1.cs := $(speedtest1.c) $(sqlite3-wasm.c)
$(speedtest1.js): emcc.cflags+=
# speedtest1 notes re. sqlite3-wasm.o vs sqlite3-wasm.c: building against
# the latter (predictably) results in a slightly faster binary, but we're
# close enough to the target speed requirements that the 500ms makes a
# difference.
$(eval $(call call-make-pre-js,speedtest1))
$(speedtest1.js): $(MAKEFILE) $(speedtest1.cs) \
    $(pre-post-speedtest1.deps) \
    EXPORTED_FUNCTIONS.speedtest1
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(speedtest1.eflags) $(speedtest1-common.eflags) $(speedtest1.cflags) \
        $(pre-post-speedtest1.flags) \
        $(SQLITE_OPT) \
        $(speedtest1.exit-runtime0) \
        -o $@ $(speedtest1.cs) -lm
	$(maybe-wasm-strip) $(speedtest1.wasm)
	ls -la $@ $(speedtest1.wasm)

speedtest1: $(speedtest1.js)
all: speedtest1
CLEAN_FILES += $(speedtest1.js) $(speedtest1.wasm)
# end speedtest1.js
########################################################################

########################################################################
# fiddle_remote is the remote destination for the fiddle app. It
# must be a [user@]HOST:/path for rsync.
# Note that the target "should probably" contain a symlink of
# index.html -> fiddle.html.
fiddle_remote ?=
ifeq (,$(fiddle_remote))
ifneq (,$(wildcard /home/stephan))
  fiddle_remote = wh:www/wh/sqlite3/.
else ifneq (,$(wildcard /home/drh))
  #fiddle_remote = if appropriate, add that user@host:/path here
endif
endif
$(fiddle_files): default
push-fiddle: $(fiddle_files)
	@if [ x = "x$(fiddle_remote)" ]; then \
		echo "fiddle_remote must be a [user@]HOST:/path for rsync"; \
		exit 1; \
	fi
	rsync -va fiddle/ $(fiddle_remote)
# end fiddle remote push
########################################################################

########################################################################
# Convenience rules to rebuild with various -Ox levels. Much
# experimentation shows -O2 to be the clear winner in terms of speed.
# Note that build times with anything higher than -O0 are somewhat
# painful.

.PHONY: o0 o1 o2 o3 os oz
o-xtra := -g3
# ^^^ -g3 is important to keep higher -O levels from mangling (via
# minification), or outright removing, otherwise working code.

o-xtra += -flto
# ^^^^ -flto can have a considerably performance boost at -O0 but
# doubles the build time and seems to have negligible effect on
# higher optimization levels.
o0: clean
	$(MAKE) -e "emcc_opt=-O0 $(o-xtra)"
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

########################################################################
# Some platforms do not support the WASMFS build. Raspberry Pi OS is one
# of them. As such platforms are discovered, add their (uname -m) name
# to PLATFORMS_WITH_NO_WASMFS to exclude the wasmfs build parts.
PLATFORMS_WITH_NO_WASMFS := aarch64 # add any others here
THIS_ARCH := $(shell /usr/bin/uname -m)
ifneq (,$(filter $(THIS_ARCH),$(PLATFORMS_WITH_NO_WASMFS)))
$(info This platform does not support the WASMFS build.)
else
include wasmfs.make
endif

########################################################################
# Create deliverables: TODO
ifneq (,$(filter dist,$(MAKECMDGOALS)))
include dist.make
endif

########################################################################
# Push files to public wasm-testing.sqlite.org server
wasm-testing.include = *.wasm *.js *.html \
    batch-runner.list sql common fiddle jaccwabyt
wasm-testing.exclude = sql/speedtest1.sql
wasm-testing.dir     = /jail/sites/wasm-testing
wasm-testing.dest   ?= wasm-testing:$(wasm-testing.dir)
# ---------------------^^^^^^^^^^^^ ssh alias
push:
	rsync -z -e ssh --ignore-times --chown=stephan:www-data --group -r \
    $(patsubst %,--exclude=%,$(wasm-testing.exclude)) \
    $(wasm-testing.include) $(wasm-testing.dest)
	ssh wasm-testing 'cd $(wasm-testing.dir) && bash .gzip' || \
    echo "SSH failed: it's likely that stale content will be served via old gzip files." 
