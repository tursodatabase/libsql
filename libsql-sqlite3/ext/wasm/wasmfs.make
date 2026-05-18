#!/usr/bin/make
#^^^^ help emacs select makefile mode
#
# This is a sub-make for building a standalone wasmfs-based
# sqlite3.wasm.  It is intended to be "include"d from the main
# GNUMakefile.
########################################################################
MAKEFILE.wasmfs := $(lastword $(MAKEFILE_LIST))
# ensure that the following message starts on line 10 or higher for proper
# $(warning) alignment!
ifneq (1,$(MAKING_CLEAN))
  $(warning !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!)
  $(warning !! The WASMFS build is not well-supported. WASMFS is a proverbial)
  $(warning !! moving target, sometimes changing in incompatible ways between)
  $(warning !! Emscripten versions. This build is provided for adventurous folks)
  $(warning !! and is not a supported deliverable of the SQLite project.)
  $(warning !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!)
endif

sqlite3-wasmfs.js     := $(dir.wasmfs)/sqlite3-wasmfs.js
sqlite3-wasmfs.wasm   := $(dir.wasmfs)/sqlite3-wasmfs.wasm

########################################################################
# emcc flags for .c/.o.
cflags.sqlite3-wasmfs :=
cflags.sqlite3-wasmfs += -std=c99 -fPIC
cflags.sqlite3-wasmfs += -pthread
cflags.sqlite3-wasmfs += -DSQLITE_ENABLE_WASMFS

########################################################################
# emcc flags specific to building the final .js/.wasm file...
emcc.flags.sqlite3-wasmfs :=
emcc.flags.sqlite3-wasmfs += \
  -sEXPORTED_RUNTIME_METHODS=wasmMemory
                          # wasmMemory ==> for -sIMPORTED_MEMORY
# Some version of emcc between 3.1.60-ish(?) and 3.1.62 deprecated the
# use of (allocateUTF8OnStack,stringToUTF8OnStack). Earlier emcc
# versions will fail to build without those in the
# EXPORTED_RUNTIME_METHODS list.
emcc.flags.sqlite3-wasmfs += -sUSE_CLOSURE_COMPILER=0
emcc.flags.sqlite3-wasmfs += -Wno-limited-postlink-optimizations
# ^^^^^ emcc likes to warn when we have "limited optimizations" via the -g3 flag.
emcc.flags.sqlite3-wasmfs += -sMEMORY64=0
emcc.flags.sqlite3-wasmfs += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.128)
# ^^^^ 64MB is not enough for WASMFS/OPFS test runs using batch-runner.js
sqlite3-wasmfs.fsflags := -pthread -sWASMFS \
    -sPTHREAD_POOL_SIZE=1 \
    -sERROR_ON_UNDEFINED_SYMBOLS=0 -sLLD_REPORT_UNDEFINED
# ^^^^^ why undefined symbols are necessary for the wasmfs build is anyone's guess.
emcc.flags.sqlite3-wasmfs += $(sqlite3-wasmfs.fsflags)
emcc.flags.sqlite3-wasmfs += -sALLOW_MEMORY_GROWTH=0
#^^^ using ALLOW_MEMORY_GROWTH produces a warning from emcc:
#   USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code slowly,
#   see https://github.com/WebAssembly/design/issues/1271 [-Wpthreads-mem-growth]
# And, indeed, it runs slowly if memory is permitted to grow.
#emcc.flags.sqlite3-wasmfs.vanilla :=
#emcc.flags.sqlite3-wasmfs.esm := -sEXPORT_ES6 -sUSE_ES6_IMPORT_META
all: $(sqlite3-wasmfs.mjs)
$(sqlite3-wasmfs.js) $(sqlite3-wasmfs.mjs): $(MAKEFILE.wasmfs)
########################################################################
# Build quirk: we cannot build BOTH .js and .mjs with our current
# build infrastructure because the supplemental *.worker.js files get
# generated with the name of the main module file
# ($(sqlite3-wasmfs.{js,mjs})) hard-coded in them.  Thus the last one
# to get built gets the *.worker.js files mapped to it. In order to
# build both modes they would need to have distinct base names or
# output directories. "The problem" with giving them distinct base
# names is that it means that the corresponding .wasm file is also
# built/saved multiple times. It is likely that anyone wanting to use
# WASMFS will want an ES6 module, so that's what we build here.
wasmfs.build.ext := mjs
$(sqlite3-wasmfs.js) $(sqlite3-wasmfs.mjs): $(SOAP.js.bld)
ifeq (js,$(wasmfs.build.ext))
  $(sqlite3-wasmfs.wasm): $(sqlite3-wasmfs.js)
  wasmfs: $(sqlite3-wasmfs.js)
else
  $(sqlite3-wasmfs.wasm): $(sqlite3-wasmfs.mjs)
  wasmfs: $(sqlite3-wasmfs.mjs)
endif

########################################################################
# speedtest1 for wasmfs.
speedtest1-wasmfs.mjs := $(dir.wasmfs)/speedtest1-wasmfs.mjs
speedtest1-wasmfs.wasm := $(subst .mjs,.wasm,$(speedtest1-wasmfs.mjs))
emcc.flags.speedtest1-wasmfs := $(sqlite3-wasmfs.fsflags)
emcc.flags.speedtest1-wasmfs += $(SQLITE_OPT)
emcc.flags.speedtest1-wasmfs += -sALLOW_MEMORY_GROWTH=0
emcc.flags.speedtest1-wasmfs += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.128)
#$(eval $(call call-make-pre-js,speedtest1-wasmfs,ems))
$(speedtest1-wasmfs.mjs): $(speedtest1.cfiles) $(sqlite3-wasmfs.js) \
  $(MAKEFILE) $(MAKEFILE.wasmfs) \
  $(pre-post-sqlite3-wasmfs-esm.deps) \
  $(EXPORTED_FUNCTIONS.speedtest1)
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(pre-post-sqlite3-wasmfs-esm.flags) \
        $(cflags.common) \
        $(cflags.sqlite3-wasmfs) \
        $(emcc.speedtest1.common) \
        $(emcc.flags.speedtest1-vanilla) \
        $(emcc.flags.sqlite3-wasmfs) \
        $(emcc.flags.speedtest1-wasmfs) \
        -o $@ $(speedtest1.cfiles) -lm
	@$(call SQLITE3.xJS.ESM-EXPORT-DEFAULT,1,1)
	$(maybe-wasm-strip) $(speedtest1-wasmfs.wasm)
	chmod -x $(speedtest1-wasmfs.wasm)
	ls -la $@ $(speedtest1-wasmfs.wasm)

wasmfs: $(speedtest1-wasmfs.mjs)
# end speedtest1.js
########################################################################
