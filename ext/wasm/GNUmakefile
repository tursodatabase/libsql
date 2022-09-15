# This GNU makefile exists primarily to simplify/speed up development
# of the sqlite3 WASM components. It is not part of the canonical
# build process.
#
# Maintenance notes: the fiddle build is currently performed in the
# top-level ../../Makefile.in. It may be moved into this file at some
# point, as GNU Make has been deemed acceptable for the WASM-related
# components (whereas POSIX Make is required for the more conventional
# components).
SHELL := $(shell which bash 2>/dev/null)
all:

.PHONY: fiddle
ifneq (,$(wildcard /home/stephan))
  fiddle_opt ?= -O0
else
  fiddle_opt = -Os
endif
fiddle:
	$(MAKE) -C ../.. fiddle -e emcc_opt=$(fiddle_opt)
all: fiddle

clean:
	$(MAKE) -C ../../ clean-fiddle
	-rm -f $(CLEAN_FILES)

MAKEFILE := $(lastword $(MAKEFILE_LIST))
dir.top := ../..
# Reminder: some Emscripten flags require absolute paths
dir.wasm := $(patsubst %/,%,$(dir $(abspath $(MAKEFILE))))
dir.api := api
dir.jacc := jaccwabyt
dir.common := common
CLEAN_FILES := *~ $(dir.jacc)/*~ $(dir.api)/*~ $(dir.common)/*~

sqlite3.c := $(dir.top)/sqlite3.c
$(sqlite3.c):
	$(MAKE) -C $(dir.top) sqlite3.c

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
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_TEMP_STORE=3
#SQLITE_OPT += -DSQLITE_ENABLE_MEMSYS5
# ^^^ MEMSYS5 is hypothetically useful for non-Emscripten builds but
# requires adding more infrastructure and fixing one spot in the
# sqlite3 internals which calls malloc() early on.

# SQLITE_OMIT_LOAD_EXTENSION: if this is true, sqlite3_vfs::xDlOpen
# and friends may be NULL.

emcc_opt ?= -O0
.PHONY: release
release:
	$(MAKE) 'emcc_opt=-Os -g3 -flto'
# ^^^^^ target-specific vars, e.g.:
#   release: emcc_opt=...
# apparently only work for file targets, not PHONY targets?
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
########################################################################

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
  $(info WARNING: builds using -O3/-Os/-Oz will minify WASM-exported names,)
  $(info WARNING: breaking _All The Things_. The workaround for that is to build)
  $(info WARNING: with -g3 (which explodes the file size) and then strip the debug)
  $(info WARNING: info after compilation, using wasm-strip, to shrink the wasm file.)
  $(info WARNING: wasm-strip was not found in the PATH so we cannot strip those.)
  $(info WARNING: If this build uses any optimization level higher than -O2 then)
  $(info WARNING: the ***resulting WASM binary WILL NOT BE USABLE***.)
  $(info WARNING: wasm-strip is part of the wabt package:)
  $(info WARNING:    https://github.com/WebAssembly/wabt)
  $(info WARNING: on Ubuntu-like systems it can be installed with:)
  $(info WARNING:    sudo apt install wabt)
  $(info WARNING: *******************************************************************)
endif
endif # 'make clean' check

ifeq (release,$(filter release,$(MAKECMDGOALS)))
  ifeq (,$(wasm-strip))
    $(error Cannot make release-quality binary because wasm-strip is not available. \
            See notes in the warning above)
  endif
else
  $(info Development build. Use '$(MAKE) release' for a smaller release build.)
endif

EXPORTED_FUNCTIONS.api.in := $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api \
    $(dir.jacc)/jaccwabyt_test.exports

EXPORTED_FUNCTIONS.api: $(EXPORTED_FUNCTIONS.api.in) $(MAKEFILE)
	cat $(EXPORTED_FUNCTIONS.api.in) > $@
CLEAN_FILES += EXPORTED_FUNCTIONS.api

sqlite3-api.jses := \
  $(dir.api)/sqlite3-api-prologue.js \
  $(dir.common)/whwasmutil.js \
  $(dir.jacc)/jaccwabyt.js \
  $(dir.api)/sqlite3-api-glue.js \
  $(dir.api)/sqlite3-api-oo1.js \
  $(dir.api)/sqlite3-api-worker1.js
#sqlite3-api.jses += $(dir.api)/sqlite3-api-opfs.js 
sqlite3-api.jses += $(dir.api)/sqlite3-api-cleanup.js

sqlite3-api.js := sqlite3-api.js
CLEAN_FILES += $(sqlite3-api.js)
$(sqlite3-api.js): $(sqlite3-api.jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(sqlite3-api.jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@

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


########################################################################
# emcc flags for .c/.o/.wasm/.js.
emcc.flags =
#emcc.flags += -v # _very_ loud but also informative about what it's doing
# -g is needed to keep -O2 and higher from creating broken JS via
# minification.
emcc.flags += -g

########################################################################
# emcc flags for .c/.o.
emcc.cflags :=
emcc.cflags += -std=c99 -fPIC
# -------------^^^^^^^^ we currently need c99 for WASM-specific sqlite3 APIs.
emcc.cflags += -I. -I$(dir.top) # $(SQLITE_OPT)

########################################################################
# emcc flags specific to building the final .js/.wasm file...
emcc.jsflags := -fPIC
emcc.jsflags := --minify 0
emcc.jsflags += --no-entry
emcc.jsflags += -sMODULARIZE
emcc.jsflags += -sSTRICT_JS
emcc.jsflags += -sDYNAMIC_EXECUTION=0
emcc.jsflags += -sNO_POLYFILL
emcc.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.api
emcc.jsflags += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
                # wasmMemory ==> required by our code for use with -sIMPORTED_MEMORY
                # allocateUTF8OnStack => for kvvp internals
emcc.jsflags += -sUSE_CLOSURE_COMPILER=0
emcc.jsflags += -sIMPORTED_MEMORY
emcc.environment := -sENVIRONMENT=web
emcc.jsflags += -sALLOW_MEMORY_GROWTH
# emcc: warning: USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code
#       slowly, see https://github.com/WebAssembly/design/issues/1271
#       [-Wpthreads-mem-growth]
emcc.jsflags += -sINITIAL_MEMORY=13107200
#emcc.jsflags += -sINITIAL_MEMORY=64225280
# ^^^^ 64MB is not enough for WASMFS/OPFS test runs using batch-runner.js
emcc.jsflags += $(emcc.environment)
#emcc.jsflags += -sTOTAL_STACK=4194304
emcc.jsflags += -sEXPORT_NAME=sqlite3InitModule
emcc.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
emcc.jsflags += --post-js=$(post-js.js)
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
enable_bigint ?= 1
ifneq (0,$(enable_bigint))
emcc.jsflags += -sWASM_BIGINT
endif
emcc.jsflags += -sMEMORY64=0
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
jaccwabyt_test.c := $(dir.jacc)/jaccwabyt_test.c
# ^^^ FIXME (how?): jaccwabyt_test.c is only needed for the test apps,
# so we don't really want to include it in release builds. However, we
# want to test the release builds with those apps, so we cannot simply
# elide that file in release builds. That component is critical to the
# VFS bindings so needs to be tested along with the core APIs.
define WASM_C_COMPILE
$(1).o := $$(subst .c,.o,$(1))
sqlite3.wasm.obj += $$($(1).o)
$$($(1).o): $$(MAKEFILE) $(1)
	$$(emcc.bin) $$(emcc_opt) $$(emcc.flags) $$(emcc.cflags) -c $(1) -o $$@
CLEAN_FILES += $$($(1).o)
endef
$(foreach c,$(sqlite3-wasm.c) $(jaccwabyt_test.c),$(eval $(call WASM_C_COMPILE,$(c))))
$(sqlite3.js): $(MAKEFILE) $(sqlite3.wasm.obj) \
    EXPORTED_FUNCTIONS.api \
    $(post-js.js)
	@echo "Building $@ ..."
	$(emcc.bin) -o $(sqlite3.js) $(emcc_opt) $(emcc.flags) $(emcc.jsflags) $(sqlite3.wasm.obj)
	chmod -x $(sqlite3.wasm)
ifneq (,$(wasm-strip))
	$(wasm-strip) $(sqlite3.wasm)
endif
	@ls -la $@ $(sqlite3.wasm)

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
speedtest1.cliflags := --size 100
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
emcc.speedtest1-flags := -g $(emcc_opt)
emcc.speedtest1-flags += -sINVOKE_RUN=0
#emcc.speedtest1-flags += --no-entry
emcc.speedtest1-flags += -flto
emcc.speedtest1-flags += -sABORTING_MALLOC
emcc.speedtest1-flags += -sINITIAL_MEMORY=128450560
emcc.speedtest1-flags += -sSTRICT_JS
emcc.speedtest1-flags += $(emcc.environment)
emcc.speedtest1-flags += -sMODULARIZE
emcc.speedtest1-flags += -sEXPORT_NAME=sqlite3Speedtest1InitModule
emcc.speedtest1-flags += -Wno-limited-postlink-optimizations
emcc.speedtest1-flags += -sEXPORTED_FUNCTIONS=_main,_malloc,_free,_sqlite3_wasm_vfs_unlink,_sqlite3_wasm_init_opfs
emcc.speedtest1-flags += -sDYNAMIC_EXECUTION=0
emcc.speedtest1-flags += --minify 0

speedtest1.js := speedtest1.js
speedtest1.wasm := $(subst .js,.wasm,$(speedtest1.js))
speedtest1.cflags := \
  -I. -I$(dir.top) \
  -DSQLITE_SPEEDTEST1_WASM

$(speedtest1.js): emcc.cflags+=
# speedtest1 notes re. sqlite3-wasm.o vs sqlite3-wasm.c: building against
# the latter (predictably) results in a slightly faster binary, but we're
# close enough to the target speed requirements that the 500ms makes a
# difference.
$(speedtest1.js): $(speedtest1.c) $(sqlite3-wasm.c) $(MAKEFILE) $(sqlite3.c)
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(emcc.speedtest1-flags) $(speedtest1.cflags) \
        $(SQLITE_OPT) \
        '-DSQLITE_DEFAULT_UNIX_VFS="unix-none"' \
        -o $@ $(speedtest1.c) $(sqlite3-wasm.c) -lm
ifneq (,$(wasm-strip))
	$(wasm-strip) $(speedtest1.wasm)
endif
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

include kvvfs.make
include wasmfs.make
