#!/usr/bin/make
#^^^^ help emacs select makefile mode
#
# This is a sub-make for building a standalone kvvfs-based
# sqlite3.wasm.  It is intended to be "include"d from the main
# GNUMakefile.
#
# Notable potential TODOs:
#
# - Trim down a custom sqlite3-api.js for this build. We can elimate
#   the jaccwabyt dependency, for example, because this build won't
#   make use of the VFS bits. Similarly, we can eliminate or replace
#   parts of the OO1 API, or provide a related API which manages
#   singletons of the localStorage/sessionStorage instances.
#
########################################################################
MAKEFILE.kvvfs := $(lastword $(MAKEFILE_LIST))

kvvfs.js     := sqlite3-kvvfs.js
kvvfs.wasm   := sqlite3-kvvfs.wasm
kvvfs.wasm.c := $(dir.api)/sqlite3-wasm.c

CLEAN_FILES += $(kvvfs.js) $(kvvfs.wasm)

########################################################################
# emcc flags for .c/.o/.wasm.
kvvfs.flags =
#kvvfs.flags += -v # _very_ loud but also informative about what it's doing

########################################################################
# emcc flags for .c/.o.
kvvfs.cflags :=
kvvfs.cflags += -std=c99 -fPIC -g
kvvfs.cflags += -I. -I$(dir.top)
kvvfs.cflags += -DSQLITE_OS_KV=1 $(SQLITE_OPT)

kvvfs.extra.c :=
ifeq (1,1)
  # To get testing1.js to run with $(kvvfs.js) we need...
  kvvfs.extra.c += $(jaccwabyt_test.c)
endif

########################################################################
# emcc flags specific to building the final .js/.wasm file...
kvvfs.jsflags := -fPIC
kvvfs.jsflags += --no-entry
kvvfs.jsflags += --minify 0
kvvfs.jsflags += -sENVIRONMENT=web
kvvfs.jsflags += -sMODULARIZE
kvvfs.jsflags += -sSTRICT_JS
kvvfs.jsflags += -sDYNAMIC_EXECUTION=0
kvvfs.jsflags += -sNO_POLYFILL
ifeq (,$(kvvfs.extra.c))
  kvvfs.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api
else
  # need more exports for jaccwabyt test code...
  kvvfs.jsflags += -sEXPORTED_FUNCTIONS=@$(dir.wasm)/EXPORTED_FUNCTIONS.api
endif
kvvfs.jsflags += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
                                            # wasmMemory ==> for -sIMPORTED_MEMORY
                                            # allocateUTF8OnStack ==> kvvfs internals
kvvfs.jsflags += -sUSE_CLOSURE_COMPILER=0
kvvfs.jsflags += -sIMPORTED_MEMORY
#kvvfs.jsflags += -sINITIAL_MEMORY=13107200
#kvvfs.jsflags += -sTOTAL_STACK=4194304
kvvfs.jsflags += -sEXPORT_NAME=sqlite3InitModule
kvvfs.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
kvvfs.jsflags += --post-js=$(post-js.js)
#kvvfs.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#                                Perhaps the kvvfs build doesn't?
#kvvfs.jsflags += -sABORTING_MALLOC
kvvfs.jsflags += -sALLOW_MEMORY_GROWTH
kvvfs.jsflags += -sALLOW_TABLE_GROWTH
kvvfs.jsflags += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
kvvfs.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=0
kvvfs.jsflags += -sLLD_REPORT_UNDEFINED
#kvvfs.jsflags += --import-undefined
kvvfs.jsflags += -sMEMORY64=0
ifneq (0,$(enable_bigint))
kvvfs.jsflags += -sWASM_BIGINT
endif
$(kvvfs.js): $(kvvfs.wasm.c) $(sqlite3.c) $(kvvfs.extra.c) \
    EXPORTED_FUNCTIONS.api $(MAKEFILE) $(MAKEFILE.kvvfs) \
    $(post-js.js)
	$(emcc.bin) -o $@ $(emcc_opt) $(emcc.flags) \
  $(SQLITE_OPT) \
  $(kvvfs.cflags) $(kvvfs.jsflags) $(kvvfs.wasm.c) $(kvvfs.extra.c)
	chmod -x $(kvvfs.wasm)
ifneq (,$(wasm-strip))
	$(wasm-strip) $(kvvfs.wasm)
endif
	@ls -la $@ $(kvvfs.wasm)

kvvfs: $(kvvfs.js)
all: kvvfs

########################################################################
# speedtest1-kvvfs
speedtest1-kvvfs.js := speedtest1-kvvfs.js
speedtest1-kvvfs.wasm := speedtest1-kvvfs.wasm
CLEAN_FILES += $(speedtest1-kvvfs.js) $(speedtest1-kvvfs.wasm)
$(speedtest1-kvvfs.js): $(speedtest1.c) $(sqlite3-wasm.c) $(sqlite3.c) $(MAKEFILE.kvvfs)
	$(emcc.bin) \
        $(emcc.speedtest1-flags) $(speedtest1.cflags) \
        $(SQLITE_OPT) \
        -sEXIT_RUNTIME=1 \
        $(kvvfs.cflags) \
        -o $@ $(speedtest1.c) $(sqlite3-wasm.c) -lm
ifneq (,$(wasm-strip))
	$(wasm-strip) $(speedtest1-kvvfs.wasm)
endif
	ls -la $@ $(speedtest1-kvvfs.wasm)

speedtest1: $(speedtest1-kvvfs.js)
