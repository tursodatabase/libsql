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
  -DSQLITE_THREADSAFE=0
#SQLITE_OPT += -DSQLITE_ENABLE_MEMSYS5
$(dir.top)/sqlite3.c:
	$(MAKE) -C $(dir.top) sqlite3.c

# SQLITE_OMIT_LOAD_EXTENSION: if this is true, sqlite3_vfs::xDlOpen
# and friends may be NULL.

emcc_opt ?= -O0
.PHONY: release
release:
	$(MAKE) 'emcc_opt=-Os -g3'
# ^^^^^ target-specific vars, e.g.:
#   release: emcc_opt=...
# apparently only work for file targets, not PHONY targets?
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
  $(dir.api)/sqlite3-api-worker.js \
  $(dir.api)/sqlite3-api-opfs.js \
  $(dir.api)/sqlite3-api-cleanup.js

sqlite3-api.js := $(dir.api)/sqlite3-api.js
CLEAN_FILES += $(sqlite3-api.js)
$(sqlite3-api.js): $(sqlite3-api.jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(sqlite3-api.jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@

post-js.js := $(dir.api)/post-js.js
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
# emcc flags for .c/.o/.wasm.
emcc.flags =
#emcc.flags += -v # _very_ loud but also informative about what it's doing

########################################################################
# emcc flags for .c/.o.
emcc.cflags :=
emcc.cflags += -std=c99 -fPIC
# -------------^^^^^^^^ we currently need c99 for WASM-specific sqlite3 APIs.
emcc.cflags += -I. -I$(dir.top) # $(SQLITE_OPT)

########################################################################
# emcc flags specific to building the final .js/.wasm file...
emcc.jsflags := -fPIC
emcc.jsflags += --no-entry
emcc.jsflags += -sENVIRONMENT=web
emcc.jsflags += -sMODULARIZE
emcc.jsflags += -sSTRICT_JS
emcc.jsflags += -sDYNAMIC_EXECUTION=0
emcc.jsflags += -sNO_POLYFILL
emcc.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.api
emcc.jsflags += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory # wasmMemory==>for -sIMPORTED_MEMORY
emcc.jsflags += -sUSE_CLOSURE_COMPILER=0
emcc.jsflags += -sIMPORTED_MEMORY
#emcc.jsflags += -sINITIAL_MEMORY=13107200
#emcc.jsflags += -sTOTAL_STACK=4194304
emcc.jsflags += -sEXPORT_NAME=sqlite3InitModule
emcc.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
emcc.jsflags +=--post-js=$(post-js.js)
#emcc.jsflags += -sSTRICT # fails due to missing __syscall_...()
#emcc.jsflags += -sALLOW_UNIMPLEMENTED_SYSCALLS
#emcc.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#emcc.jsflags += -sABORTING_MALLOC
emcc.jsflags += -sALLOW_MEMORY_GROWTH
emcc.jsflags += -sALLOW_TABLE_GROWTH
emcc.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
#emcc.jsflags += -sMALLOC=emmalloc
#emcc.jsflags += -sMALLOC=dlmalloc # a good 8k larger than emmalloc
#emcc.jsflags += -sSTANDALONE_WASM # causes OOM errors, not sure why
#emcc.jsflags += --import=foo_bar
#emcc.jsflags += --no-gc-sections
# https://lld.llvm.org/WebAssembly.html
emcc.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
emcc.jsflags += -sLLD_REPORT_UNDEFINED
#emcc.jsflags += --allow-undefined
emcc.jsflags += --import-undefined
#emcc.jsflags += --unresolved-symbols=import-dynamic --experimental-pic
#emcc.jsflags +=  --experimental-pic --unresolved-symbols=ingore-all --import-undefined
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
# which have strict must-be-a-number requirements.
########################################################################


sqlite3.js := $(dir.api)/sqlite3.js
sqlite3.wasm := $(dir.api)/sqlite3.wasm
$(dir.api)/sqlite3-wasm.o: emcc.cflags += $(SQLITE_OPT)
$(dir.api)/sqlite3-wasm.o: $(dir.top)/sqlite3.c
$(dir.api)/wasm_util.o: emcc.cflags += $(SQLITE_OPT)
sqlite3.wasm.c := $(dir.api)/sqlite3-wasm.c \
    $(dir.jacc)/jaccwabyt_test.c
# ^^^ FIXME (how?): jaccwabyt_test.c is only needed for the test
# apps. However, we want to test the release builds with those apps,
# so we cannot simply elide that file in release builds. That
# component is critical to the VFS bindings so needs to be tested
# along with the core APIs.
define WASM_C_COMPILE
$(1).o := $$(subst .c,.o,$(1))
sqlite3.wasm.obj += $$($(1).o)
$$($(1).o): $$(MAKEFILE) $(1)
	$$(emcc.bin) $$(emcc_opt) $$(emcc.flags) $$(emcc.cflags) -c $(1) -o $$@
CLEAN_FILES += $$($(1).o)
endef
$(foreach c,$(sqlite3.wasm.c),$(eval $(call WASM_C_COMPILE,$(c))))
$(sqlite3.js): 
$(sqlite3.js): $(MAKEFILE) $(sqlite3.wasm.obj) \
    EXPORTED_FUNCTIONS.api \
    $(post-js.js)
	$(emcc.bin) -o $@ $(emcc_opt) $(emcc.flags) $(emcc.jsflags) $(sqlite3.wasm.obj)
	chmod -x $(sqlite3.wasm)
ifneq (,$(wasm-strip))
	$(wasm-strip) $(sqlite3.wasm)
endif
	@ls -la $@ $(sqlite3.wasm)

CLEAN_FILES += $(sqlite3.js) $(sqlite3.wasm)
all: $(sqlite3.js)
# End main Emscripten-based module build
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
