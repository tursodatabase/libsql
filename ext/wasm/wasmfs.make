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
sqlite3-wasmfs.mjs    := $(dir.wasmfs)/sqlite3-wasmfs.mjs
sqlite3-wasmfs.wasm   := $(dir.wasmfs)/sqlite3-wasmfs.wasm

CLEAN_FILES += $(sqlite3-wasmfs.js) $(sqlite3-wasmfs.wasm) \
    $(subst .js,.worker.js,$(sqlite3-wasmfs.js)) \
    $(sqlite3-wasmfs.mjs) \
    $(subst .mjs,.worker.mjs,$(sqlite3-wasmfs.mjs))

########################################################################
# emcc flags for .c/.o.
cflags.sqlite3-wasmfs :=
cflags.sqlite3-wasmfs += -std=c99 -fPIC
cflags.sqlite3-wasmfs += -pthread
cflags.sqlite3-wasmfs += $(cflags.speedtest1)
cflags.sqlite3-wasmfs += $(SQLITE_OPT) -DSQLITE_ENABLE_WASMFS

########################################################################
# emcc flags specific to building the final .js/.wasm file...
emcc.flags.sqlite3-wasmfs := -fPIC
emcc.flags.sqlite3-wasmfs += --no-entry
emcc.flags.sqlite3-wasmfs += --minify 0
emcc.flags.sqlite3-wasmfs += -sMODULARIZE
emcc.flags.sqlite3-wasmfs += -sEXPORT_NAME=$(sqlite3.js.init-func)
emcc.flags.sqlite3-wasmfs += -sSTRICT_JS
emcc.flags.sqlite3-wasmfs += -sDYNAMIC_EXECUTION=0
emcc.flags.sqlite3-wasmfs += -sNO_POLYFILL
emcc.flags.sqlite3-wasmfs += -sWASM_BIGINT=$(emcc.WASM_BIGINT)
emcc.flags.sqlite3-wasmfs += -sEXPORTED_FUNCTIONS=@$(abspath $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api)
emcc.flags.sqlite3-wasmfs += -sEXPORTED_RUNTIME_METHODS=FS,wasmMemory,allocateUTF8OnStack
                          # wasmMemory ==> for -sIMPORTED_MEMORY
                          # allocateUTF8OnStack ==> wasmfs internals
emcc.flags.sqlite3-wasmfs += -sUSE_CLOSURE_COMPILER=0
emcc.flags.sqlite3-wasmfs += -Wno-limited-postlink-optimizations
# ^^^^^ it likes to warn when we have "limited optimizations" via the -g3 flag.
emcc.flags.sqlite3-wasmfs += -sALLOW_TABLE_GROWTH
emcc.flags.sqlite3-wasmfs += -sSTACK_SIZE=512KB
emcc.flags.sqlite3-wasmfs += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
emcc.flags.sqlite3-wasmfs += -sMEMORY64=0
emcc.flags.sqlite3-wasmfs += -sIMPORTED_MEMORY
emcc.flags.sqlite3-wasmfs += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.128)
# ^^^^ 64MB is not enough for WASMFS/OPFS test runs using batch-runner.js
sqlite3-wasmfs.fsflags := -pthread -sWASMFS \
    -sPTHREAD_POOL_SIZE=2 -sENVIRONMENT=web,worker \
    -sERROR_ON_UNDEFINED_SYMBOLS=0 -sLLD_REPORT_UNDEFINED 
# ^^^^^ why undefined symbols are necessary for the wasmfs build is anyone's guess.
emcc.flags.sqlite3-wasmfs += $(sqlite3-wasmfs.fsflags)
#emcc.flags.sqlite3-wasmfs += -sALLOW_MEMORY_GROWTH
#^^^ using ALLOW_MEMORY_GROWTH produces a warning from emcc:
#   USE_PTHREADS + ALLOW_MEMORY_GROWTH may run non-wasm code slowly,
#   see https://github.com/WebAssembly/design/issues/1271 [-Wpthreads-mem-growth]
# And, indeed, it runs slowly if memory is permitted to grow.
emcc.flags.sqlite3-wasmfs.vanilla :=
emcc.flags.sqlite3-wasmfs.esm := -sEXPORT_ES6 -sUSE_ES6_IMPORT_META
$(eval $(call call-make-pre-js,sqlite3-wasmfs,vanilla))
$(eval $(call call-make-pre-js,sqlite3-wasmfs,esm))
Xemcc.flags.sqlite3-wasmfs.vanilla += \
  $(pre-post-common.flags.vanilla) \
  $(pre-post-sqlite3-wasmfs.flags.vanilla)
Xemcc.flags.sqlite3-wasmfs.esm += \
  $(pre-post-common.flags.esm) \
  $(pre-post-sqlite3-wasmfs.flags.esm)
$(sqlite3-wasmfs.js) $(sqlite3-wasmfs.mjs): $(sqlite3-wasm.c) \
    $(EXPORTED_FUNCTIONS.api) $(MAKEFILE) $(MAKEFILE.wasmfs)
$(sqlite3-wasmfs.js): $(pre-post-sqlite3-wasmfs.deps.vanilla)
$(sqlite3-wasmfs.mjs): $(pre-post-sqlite3-wasmfs.deps.esm)
# SQLITE3-WASMFS.xJS.RECIPE is the wasmfs-specific counterpart
# of SQLITE3.xJS.RECIPE from the main makefile.
define SQLITE3-WASMFS.xJS.RECIPE
	@echo "Building $@ ..."
	$(emcc.bin) -o $@ $(emcc_opt_full) $(emcc.flags) \
      $(cflags.sqlite3-wasmfs) \
      $(emcc.flags.sqlite3-wasmfs) $(emcc.flags.sqlite3-wasmfs.$(1)) \
      $(pre-post-sqlite3-wasmfs.flags.$(1)) \
     $(sqlite3-wasm.c)
	@$(call SQLITE3.xJS.ESM-EXPORT-DEFAULT,$(1))
	chmod -x $(sqlite3-wasmfs.wasm)
	$(maybe-wasm-strip) $(sqlite3-wasmfs.wasm)
	@ls -la $(sqlite3-wasmfs.wasm) sqlite3-wasmfs*js
endef
$(sqlite3-wasmfs.js):
	$(call SQLITE3-WASMFS.xJS.RECIPE,vanilla)
$(sqlite3-wasmfs.mjs): $(sqlite3-wasmfs.js)
	$(call SQLITE3-WASMFS.xJS.RECIPE,esm)
$(sqlite3-wasmfs.wasm): $(sqlite3-wasmfs.js)
wasmfs: $(sqlite3-wasmfs.js) $(sqlite3-wasmfs.mjs)
#all: wasmfs

########################################################################
# speedtest1 for wasmfs.
speedtest1-wasmfs.js := $(dir.wasmfs)/speedtest1-wasmfs.js
speedtest1-wasmfs.wasm := $(subst .js,.wasm,$(speedtest1-wasmfs.js))
emcc.flags.speedtest1-wasmfs := $(sqlite3-wasmfs.fsflags)
emcc.flags.speedtest1-wasmfs += $(SQLITE_OPT) -DSQLITE_ENABLE_WASMFS
emcc.flags.speedtest1-wasmfs += -sALLOW_MEMORY_GROWTH=0
emcc.flags.speedtest1-wasmfs += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.128)
#$(eval $(call call-make-pre-js,speedtest1-wasmfs,vanilla))
$(speedtest1-wasmfs.js): $(speedtest1.cses) $(sqlite3-wasmfs.js) \
  $(MAKEFILE) $(MAKEFILE.wasmfs) \
  $(pre-post-sqlite3-wasmfs.deps) \
  $(EXPORTED_FUNCTIONS.speedtest1)
	@echo "Building $@ ..."
	$(emcc.bin) \
         $(emcc.speedtest1.common) $(emcc.flags.speedtest1-wasmfs) \
        $(pre-post-sqlite3-wasmfs.flags.vanilla) \
        $(cflags.sqlite3-wasmfs) \
        -o $@ $(speedtest1.cses) -lm
	$(maybe-wasm-strip) $(speedtest1-wasmfs.wasm)
	ls -la $@ $(speedtest1-wasmfs.wasm)

#speedtest1: $(speedtest1-wasmfs.js)
wasmfs: $(speedtest1-wasmfs.js)
CLEAN_FILES += $(speedtest1-wasmfs.js) $(speedtest1-wasmfs.wasm) \
     $(subst .js,.worker.js,$(speedtest1-wasmfs.js))
# end speedtest1.js
########################################################################
