#!/usr/bin/make
#^^^^ help emacs select makefile mode
#
# This is a sub-make for building a standalone wasmfs-based
# sqlite3.wasm.  It is intended to be "include"d from the main
# GNUMakefile.
########################################################################
MAKEFILE.wasmfs := $(lastword $(MAKEFILE_LIST))

# Maintenance reminder: these particular files cannot be built into a
# subdirectory because loading of the auxiliary
# sqlite3-wasmfs.worker.js file it creates fails if sqlite3-wasmfs.js
# is loaded from any directory other than the one in which the
# containing HTML lives. Similarly, they cannot be loaded from a
# Worker to an Emscripten quirk regarding loading nested Workers.
dir.wasmfs := $(dir.wasm)
sqlite3-wasmfs.js     := $(dir.wasmfs)/sqlite3-wasmfs.js
sqlite3-wasmfs.wasm   := $(dir.wasmfs)/sqlite3-wasmfs.wasm

CLEAN_FILES += $(sqlite3-wasmfs.js) $(sqlite3-wasmfs.wasm) \
    $(subst .js,.worker.js,$(sqlite3-wasmfs.js))

########################################################################
# emcc flags for .c/.o.
sqlite3-wasmfs.cflags :=
sqlite3-wasmfs.cflags += -std=c99 -fPIC
sqlite3-wasmfs.cflags += -pthread
sqlite3-wasmfs.cflags += $(cflags.common)
sqlite3-wasmfs.cflags += $(SQLITE_OPT) -DSQLITE_ENABLE_WASMFS

########################################################################
# emcc flags specific to building the final .js/.wasm file...
sqlite3-wasmfs.jsflags := -fPIC
sqlite3-wasmfs.jsflags += --no-entry
sqlite3-wasmfs.jsflags += --minify 0
sqlite3-wasmfs.jsflags += -sMODULARIZE
sqlite3-wasmfs.jsflags += -sSTRICT_JS
sqlite3-wasmfs.jsflags += -sDYNAMIC_EXECUTION=0
sqlite3-wasmfs.jsflags += -sNO_POLYFILL
sqlite3-wasmfs.jsflags += -sEXPORTED_FUNCTIONS=@$(abspath $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api)
sqlite3-wasmfs.jsflags += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
                                            # wasmMemory ==> for -sIMPORTED_MEMORY
                                            # allocateUTF8OnStack ==> wasmfs internals
sqlite3-wasmfs.jsflags += -sUSE_CLOSURE_COMPILER=0
sqlite3-wasmfs.jsflags += -sIMPORTED_MEMORY
#sqlite3-wasmfs.jsflags += -sINITIAL_MEMORY=13107200
#sqlite3-wasmfs.jsflags += -sTOTAL_STACK=4194304
sqlite3-wasmfs.jsflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
sqlite3-wasmfs.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
#sqlite3-wasmfs.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#                                Perhaps the wasmfs build doesn't?
#sqlite3-wasmfs.jsflags += -sABORTING_MALLOC
sqlite3-wasmfs.jsflags += -sALLOW_TABLE_GROWTH
sqlite3-wasmfs.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
sqlite3-wasmfs.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
sqlite3-wasmfs.jsflags += -sLLD_REPORT_UNDEFINED
#sqlite3-wasmfs.jsflags += --import-undefined
sqlite3-wasmfs.jsflags += -sMEMORY64=0
sqlite3-wasmfs.jsflags += -sINITIAL_MEMORY=128450560
# ^^^^ 64MB is not enough for WASMFS/OPFS test runs using batch-runner.js
sqlite3-wasmfs.fsflags := -pthread -sWASMFS -sPTHREAD_POOL_SIZE=2 -sENVIRONMENT=web,worker
# -sPTHREAD_POOL_SIZE values of 2 or higher trigger that bug.
sqlite3-wasmfs.jsflags += $(sqlite3-wasmfs.fsflags)
#sqlite3-wasmfs.jsflags += -sALLOW_MEMORY_GROWTH
#^^^ using ALLOW_MEMORY_GROWTH produces a warning from emcc:
#   USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code slowly,
#   see https://github.com/WebAssembly/design/issues/1271 [-Wpthreads-mem-growth]
sqlite3-wasmfs.jsflags += -sWASM_BIGINT=$(emcc.WASM_BIGINT)
$(eval $(call call-make-pre-js,sqlite3-wasmfs))
sqlite3-wasmfs.jsflags += $(pre-post-common.flags) $(pre-post-sqlite3-wasmfs.flags)
$(sqlite3-wasmfs.js): $(sqlite3-wasm.c) \
    $(EXPORTED_FUNCTIONS.api) $(MAKEFILE) $(MAKEFILE.wasmfs) \
    $(pre-post-sqlite3-wasmfs.deps)
	@echo "Building $@ ..."
	$(emcc.bin) -o $@ $(emcc_opt_full) $(emcc.flags) \
      $(sqlite3-wasmfs.cflags) $(sqlite3-wasmfs.jsflags) \
     $(sqlite3-wasm.c)
	chmod -x $(sqlite3-wasmfs.wasm)
	$(maybe-wasm-strip) $(sqlite3-wasmfs.wasm)
	@ls -la $@ $(sqlite3-wasmfs.wasm)
$(sqlite3-wasmfs.wasm): $(sqlite3-wasmfs.js)
wasmfs: $(sqlite3-wasmfs.js)
all: wasmfs

########################################################################
# speedtest1 for wasmfs.
speedtest1-wasmfs.js := $(dir.wasmfs)/speedtest1-wasmfs.js
speedtest1-wasmfs.wasm := $(subst .js,.wasm,$(speedtest1-wasmfs.js))
speedtest1-wasmfs.eflags := $(sqlite3-wasmfs.fsflags)
speedtest1-wasmfs.eflags += $(SQLITE_OPT) -DSQLITE_ENABLE_WASMFS
speedtest1-wasmfs.eflags += -sALLOW_MEMORY_GROWTH=0
speedtest1-wasmfs.eflags += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.128)
$(eval $(call call-make-pre-js,speedtest1-wasmfs))
$(speedtest1-wasmfs.js): $(speedtest1.cses) $(sqlite3-wasmfs.js) \
  $(MAKEFILE) $(MAKEFILE.wasmfs) \
  $(pre-post-speedtest1-wasmfs.deps) \
  $(EXPORTED_FUNCTIONS.speedtest1)
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(speedtest1-wasmfs.eflags) $(speedtest1-common.eflags) \
        $(pre-post-speedtest1-wasmfs.flags) \
        $(speedtest1.cflags) \
        $(sqlite3-wasmfs.cflags) \
        -o $@ $(speedtest1.cses) -lm
	$(maybe-wasm-strip) $(speedtest1-wasmfs.wasm)
	ls -la $@ $(speedtest1-wasmfs.wasm)

speedtest1: $(speedtest1-wasmfs.js)
CLEAN_FILES += $(speedtest1-wasmfs.js) $(speedtest1-wasmfs.wasm) \
     $(subst .js,.worker.js,$(speedtest1-wasmfs.js))
# end speedtest1.js
########################################################################
