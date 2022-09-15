#!/usr/bin/make
#^^^^ help emacs select makefile mode
#
# This is a sub-make for building a standalone wasmfs-based
# sqlite3.wasm.  It is intended to be "include"d from the main
# GNUMakefile.
########################################################################
MAKEFILE.wasmfs := $(lastword $(MAKEFILE_LIST))

wasmfs.js     := sqlite3-wasmfs.js
wasmfs.wasm   := sqlite3-wasmfs.wasm
wasmfs.wasm.c := $(dir.api)/sqlite3-wasm.c

CLEAN_FILES += $(wasmfs.js) $(wasmfs.wasm)

########################################################################
# emcc flags for .c/.o/.wasm.
wasmfs.flags =
#wasmfs.flags += -v # _very_ loud but also informative about what it's doing

########################################################################
# emcc flags for .c/.o.
wasmfs.cflags :=
wasmfs.cflags += -std=c99 -fPIC -g
wasmfs.cflags += -pthread
wasmfs.cflags += -I. -I$(dir.top)
wasmfs.cflags += $(SQLITE_OPT) -DSQLITE_WASM_OPFS
wasmfs.cflags += '-DSQLITE_DEFAULT_UNIX_VFS="unix-none"'

wasmfs.extra.c :=
ifeq (1,1)
  # To get testing1.js to run with $(wasmfs.js) we need...
  wasmfs.extra.c += $(jaccwabyt_test.c)
endif

########################################################################
# emcc flags specific to building the final .js/.wasm file...
wasmfs.jsflags := -fPIC
wasmfs.jsflags += --no-entry
wasmfs.jsflags += --minify 0
wasmfs.jsflags += -sENVIRONMENT=web,worker
wasmfs.jsflags += -sMODULARIZE
wasmfs.jsflags += -sSTRICT_JS
wasmfs.jsflags += -sDYNAMIC_EXECUTION=0
wasmfs.jsflags += -sNO_POLYFILL
ifeq (,$(wasmfs.extra.c))
  wasmfs.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api
else
  # need more exports for jaccwabyt test code...
  wasmfs.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.api
endif
wasmfs.jsflags += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
                                            # wasmMemory ==> for -sIMPORTED_MEMORY
                                            # allocateUTF8OnStack ==> wasmfs internals
wasmfs.jsflags += -sUSE_CLOSURE_COMPILER=0
wasmfs.jsflags += -sIMPORTED_MEMORY
#wasmfs.jsflags += -sINITIAL_MEMORY=13107200
#wasmfs.jsflags += -sTOTAL_STACK=4194304
wasmfs.jsflags += -sEXPORT_NAME=sqlite3InitModule
wasmfs.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
wasmfs.jsflags += --post-js=$(post-js.js)
#wasmfs.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#                                Perhaps the wasmfs build doesn't?
#wasmfs.jsflags += -sABORTING_MALLOC
wasmfs.jsflags += -sALLOW_TABLE_GROWTH
wasmfs.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
wasmfs.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
wasmfs.jsflags += -sLLD_REPORT_UNDEFINED
#wasmfs.jsflags += --import-undefined
wasmfs.jsflags += -sMEMORY64=0
wasmfs.jsflags += -pthread -sWASMFS -sPTHREAD_POOL_SIZE=2
wasmfs.jsflags += -sINITIAL_MEMORY=128450560
#wasmfs.jsflags += -sALLOW_MEMORY_GROWTH
#^^^ using ALLOW_MEMORY_GROWTH produces a warning from emcc:
#   USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code slowly,
#   see https://github.com/WebAssembly/design/issues/1271 [-Wpthreads-mem-growth]
ifneq (0,$(enable_bigint))
wasmfs.jsflags += -sWASM_BIGINT
endif

$(wasmfs.js): $(wasmfs.wasm.c) $(sqlite3.c) $(wasmfs.extra.c) \
    EXPORTED_FUNCTIONS.api $(MAKEFILE) $(MAKEFILE.wasmfs) \
    $(post-js.js)
	@echo "Building $@ ..."
	$(emcc.bin) -o $@ $(emcc_opt) $(emcc.flags) \
  $(wasmfs.cflags) $(wasmfs.jsflags) $(wasmfs.wasm.c) $(wasmfs.extra.c)
	chmod -x $(wasmfs.wasm)
ifneq (,$(wasm-strip))
	$(wasm-strip) $(wasmfs.wasm)
endif
	@ls -la $@ $(wasmfs.wasm)

wasmfs: $(wasmfs.js)
all: wasmfs
