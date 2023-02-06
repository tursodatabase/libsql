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
#  quick, q = do just a minimal build (sqlite3.js/wasm, tester1) for
#      faster development-mode turnaround.
#
#  qo2, qoz = a combination of quick+o2/oz.
#
#  dist = create end user deliverables. Add dist.build=oX to build
#      with a specific optimization level, where oX is one of the
#      above-listed o? or qo? target names.
#
#  snapshot = like dist, but uses a zip file name which clearly
#      marks it as a prerelease/snapshot build.
#
#  clean = clean up
#
# Required tools beyond those needed for the canonical builds:
#
# - Emscripten SDK: https://emscripten.org/docs/getting_started/downloads.html
# - The bash shell
# - GNU make, GNU sed, GNU awk, GNU grep (all in the $PATH)
# - wasm-strip for release builds: https://github.com/WebAssembly/wabt
# - InfoZip for 'dist' zip file
########################################################################
#
# Significant TODOs for this build include, but are not necessarily
# limited to:
#
# 1) Consolidate the code generation for sqlite3*.*js into a script
#    which generates the makefile code, rather than using $(call) and
#    $(eval), or at least centralize the setup of the numerous vars
#    related to each build variant (vanilla, esm, bundler-friendly).
#
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
emcc.version := $(shell "$(emcc.bin)" --version | sed -n 1p \
                  | sed -e 's/^.* \([3-9][^ ]*\) .*$$/\1/;')
ifeq (,$(emcc.version))
  $(warning Cannot determine emcc version. This might unduly impact build flags.)
else
  $(info using emcc version [$(emcc.version)])
endif
emcc.version := $(shell "$(emcc.bin)" --version | sed -n 1p \
                  | sed -e 's/^.* \([3-9][^ ]*\) .*$$/\1/;')
ifeq (,$(emcc.version))
  $(warning Cannot determine emcc version. This might unduly impact build flags.)
else
  $(info using emcc version [$(emcc.version)])
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
CLEAN_FILES += *~ $(dir.jacc)/*~ $(dir.api)/*~ $(dir.common)/*~ $(dir.fiddle)/*~

########################################################################
# dir.dout = output dir for deliverables.
#
# MAINTENANCE REMINDER: the output .js and .wasm files of certain emcc
# buildables must be in _this_ dir, rather than a subdir, or else
# parts of the generated code get confused and cannot load
# property. Specifically, when X.js loads X.wasm, whether or not X.js
# uses the correct path for X.wasm depends on how it's loaded: an HTML
# script tag will resolve it intuitively, whereas a Worker's call to
# importScripts() will not.  That's a fundamental incompatibility with
# how URL resolution in JS happens between those two contexts. See:
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

sqlite3.c := $(dir.top)/sqlite3.c
sqlite3.h := $(dir.top)/sqlite3.h
# Most SQLITE_OPT flags are set in sqlite3-wasm.c but we need them
# made explicit here for building speedtest1.c.
SQLITE_OPT = \
  -DSQLITE_ENABLE_FTS5 \
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

# bin.version-info = binary to output various sqlite3 version info for
# embedding in the JS files and in building the distribution zip file.
# It must NOT be in $(dir.tmp) because we need it to survive the
# cleanup process for the dist build to work properly.
bin.version-info := $(dir.wasm)/version-info
$(bin.version-info): $(dir.wasm)/version-info.c $(sqlite3.h) $(MAKEFILE)
	$(CC) -O0 -I$(dir.top) -o $@ $<
DISTCLEAN_FILES += $(bin.version-info)

# bin.stripcomments is used for stripping C/C++-style comments from JS
# files. The JS files contain large chunks of documentation which we
# don't need for all builds. That app's -k flag is of particular
# importance here, as it allows us to retain the opening comment
# blocks, which contain the license header and version info.
bin.stripccomments := $(dir.tool)/stripccomments
$(bin.stripccomments): $(bin.stripccomments).c $(MAKEFILE)
	$(CC) -o $@ $<
DISTCLEAN_FILES += $(bin.stripccomments)


########################################################################
# C-PP.FILTER: a $(call)able to transform $(1) to $(2) via ./c-pp -f
# $(1) ...
#
# Historical notes:
#
# - We first attempted to use gcc and/or clang to preprocess JS files
#   in the same way we would normally do C files, but C-specific quirks
#   of each makes that untennable.
#
# - We implemented c-pp.c (the C-Minus Pre-processor) as a custom
#   generic/file-format-agnostic preprocessor to enable us to pack
#   code for different target builds into the same JS files. Most
#   notably, some ES6 module (a.k.a. ESM) features cannot legally be
#   referenced at all in non-ESM code, e.g.  the "import" and "export"
#   keywords. This preprocessing step permits us to swap out sections
#   of code where necessary for ESM and non-ESM (a.k.a. vanilla JS)
#   require different implementations. The alternative to such
#   preprocessing, would be to have separate source files for ES6
#   builds, which would have a higher maintenance burden than c-pp.c
#   seems likely to.
#
# c-pp.c was written specifically for the sqlite project's JavaScript
# builds but is maintained as a standalone project:
# https://fossil.wanderinghorse.net/r/c-pp
bin.c-pp := ./c-pp
$(bin.c-pp): c-pp.c $(sqlite3.c) $(MAKEFILE)
	$(CC) -O0 -o $@ c-pp.c $(sqlite3.c) '-DCMPP_DEFAULT_DELIM="//#"' -I$(dir.top) \
		-DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_UTF16 \
		-DSQLITE_OMIT_SHARED_CACHE -DSQLITE_OMIT_WAL -DSQLITE_THREADSAFE=0 \
		-DSQLITE_TEMP_STORE=3
define C-PP.FILTER
# Create $2 from $1 using $(bin.c-pp)
# $1 = Input file: c-pp -f $(1).js
# $2 = Output file: c-pp -o $(2).js
# $3 = optional c-pp -D... flags
$(2): $(1) $$(MAKEFILE) $$(bin.c-pp)
	$$(bin.c-pp) -f $(1) -o $$@ $(3)
CLEAN_FILES += $(2)
endef
# /end C-PP.FILTER
########################################################################

# cflags.common = C compiler flags for all builds
cflags.common :=  -I. -I.. -I$(dir.top)
# emcc.WASM_BIGINT = 1 for BigInt (C int64) support, else 0.  The API
# disables certain features if BigInt is not enabled and such builds
# _are not tested_ on any regular basis.
emcc.WASM_BIGINT ?= 1

# emcc_opt = optimization-related flags. These are primarily used by
# the various oX targets. build times for -O levels higher than 0 are
# painful at dev-time.
emcc_opt ?= -O0

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
# Much practice has demonstrated that -O2 consistently gives the best
# runtime speeds, but not by a large enough factor to rule out use of
# -Oz when small deliverable size is a priority.
########################################################################

# EXPORTED_FUNCTIONS.* = files for use with Emscripten's
# -sEXPORTED_FUNCTION flag.
EXPORTED_FUNCTIONS.api.in := $(abspath $(dir.api)/EXPORTED_FUNCTIONS.sqlite3-api)
EXPORTED_FUNCTIONS.api := $(dir.tmp)/EXPORTED_FUNCTIONS.api
$(EXPORTED_FUNCTIONS.api): $(EXPORTED_FUNCTIONS.api.in) $(MAKEFILE)
	cp $(EXPORTED_FUNCTIONS.api.in) $@

# sqlite3-license-version.js = generated JS file with the license
# header and version info.
sqlite3-license-version.js := $(dir.tmp)/sqlite3-license-version.js
# sqlite3-license-version-header.js = JS file containing only the
# license header.
sqlite3-license-version-header.js := $(dir.api)/sqlite3-license-version-header.js
# sqlite3-api-build-version.js = generated JS file which populates the
# sqlite3.version object using $(bin.version-info).
sqlite3-api-build-version.js := $(dir.tmp)/sqlite3-api-build-version.js
# sqlite3-api.jses = the list of JS files which make up
# $(sqlite3-api.js.in), in the order they need to be assembled.
sqlite3-api.jses := $(sqlite3-license-version.js)
sqlite3-api.jses += $(dir.api)/sqlite3-api-prologue.js
sqlite3-api.jses += $(dir.common)/whwasmutil.js
sqlite3-api.jses += $(dir.jacc)/jaccwabyt.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-glue.js
sqlite3-api.jses += $(sqlite3-api-build-version.js)
sqlite3-api.jses += $(dir.api)/sqlite3-api-oo1.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-worker1.js
sqlite3-api.jses += $(dir.api)/sqlite3-v-helper.js
sqlite3-api.jses += $(dir.api)/sqlite3-vfs-opfs.c-pp.js
sqlite3-api.jses += $(dir.api)/sqlite3-api-cleanup.js

# "External" API files which are part of our distribution
# but not part of the sqlite3-api.js amalgamation.
SOAP.js := $(dir.api)/sqlite3-opfs-async-proxy.js
# COPY_XAPI = a $(call)able function to copy $1 to $(dir.dout), where
# $1 must be one of the "external" JS API files.
define COPY_XAPI
sqlite3-api.ext.jses += $$(dir.dout)/$$(notdir $(1))
$$(dir.dout)/$$(notdir $(1)): $(1) $$(MAKEFILE)
	cp $$< $$@
endef
$(foreach X,$(SOAP.js),\
  $(eval $(call COPY_XAPI,$(X))))
all quick: $(sqlite3-api.ext.jses)
q: quick

########################################################################
# $(sqlite3-api*.*js) contain the core library code but not the
# Emscripten-related glue which deals with loading sqlite3.wasm. In
# theory they can be used by arbitrary build environments and WASM
# loaders, but in practice that breaks down because the WASM loader
# has to be able to provide all of the necessary "imports" to
# sqlite3.wasm, and that list of imports is unknown until sqlite3.wasm
# is compiled, at which point Emscripten sets up the imports
# appropriately. Abstractly speaking, it's impossible for other build
# environments to know exactly which imports are needed and provide
# them. Tools like wasm-objdump can be used to find the list of
# imports but it's questionable whether a non-Emscripten tool could
# realistically use that info to provide proper implementations.
# Sidebar: some of the imports are used soley by the Emscripten glue,
# which the sqlite3 JS code does not rely on.
#
# We build $(sqlite3-api*.*) "because we can" and because it might be
# a useful point of experimentation for some clients, but the
# above-described caveat may well make them unusable for real-life
# clients.
#
# sqlite3-api.js.in = the generated sqlite3-api.js before it gets
# preprocessed. It contains all of $(sqlite3-api.jses) but none of the
# Emscripten-specific headers and footers.
sqlite3-api.js.in := $(dir.tmp)/sqlite3-api.c-pp.js
$(sqlite3-api.js.in): $(sqlite3-api.jses) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(sqlite3-api.jses); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@

########################################################################
# emcc flags for .c/.o/.wasm/.js.
emcc.flags :=
ifeq (1,$(emcc.verbose))
emcc.flags += -v
# -v is _very_ loud but also informative about what it's doing
endif

########################################################################
# emcc flags for .c/.o.
emcc.cflags :=
emcc.cflags += -std=c99 -fPIC
# -------------^^^^^^^^ we need c99 for $(sqlite3-wasm.c).
emcc.cflags += -I. -I$(dir.top)
########################################################################
# emcc flags specific to building .js/.wasm files...
emcc.jsflags := -fPIC
emcc.jsflags += --minify 0
emcc.jsflags += --no-entry
emcc.jsflags += -sWASM_BIGINT=$(emcc.WASM_BIGINT)
emcc.jsflags += -sMODULARIZE
emcc.jsflags += -sDYNAMIC_EXECUTION=0
emcc.jsflags += -sNO_POLYFILL
emcc.jsflags += -sEXPORTED_FUNCTIONS=@$(EXPORTED_FUNCTIONS.api)
emcc.exportedRuntimeMethods := \
    -sEXPORTED_RUNTIME_METHODS=wasmMemory
    # wasmMemory ==> required by our code for use with -sIMPORTED_MEMORY
emcc.jsflags += $(emcc.exportedRuntimeMethods)
emcc.jsflags += -sUSE_CLOSURE_COMPILER=0
emcc.jsflags += -sIMPORTED_MEMORY
ifeq (3.1.31,$(emcc.version))
  emcc.jsflags += -sSTRICT_JS=0
  $(warning Disabling -sSTRICT_JS for emcc $(emcc.version): \
     https://github.com/emscripten-core/emscripten/issues/18610)
else
  emcc.jsflags += -sSTRICT_JS=1
endif
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
emcc.jsflags += -sSTACK_SIZE=512KB
# ^^^ ACHTUNG: emsdk 3.1.27 reduced the default stack size from 5MB to
# a mere 64KB, which leads to silent memory corruption via the kvvfs
# VFS, which requires twice that for its xRead() and xWrite() methods.
########################################################################
# $(sqlite3.js.init-func) is the name Emscripten assigns our exported
# module init/load function. This symbol name is hard-coded in
# $(extern-post-js.js) as well as in numerous docs.
#
# "sqlite3InitModule" is the symbol we document for client use, so
# that's the symbol name which must be exported, whether it comes from
# Emscripten or our own code in extern-post-js.js.
#
# That said... we can change $(sqlite3.js.init-func) as long as the
# name "sqlite3InitModule" is the one which gets exposed via the
# resulting JS files. That can be accomplished via
# extern-post-js.js. However...  using a temporary symbol name here
# and then adding sqlite3InitModule() ourselves results in 2 global
# symbols: we cannot "delete" the Emscripten-defined
# $(sqlite3.js.init-func) because it's declared with "var".
sqlite3.js.init-func := sqlite3InitModule
emcc.jsflags += -sEXPORT_NAME=$(sqlite3.js.init-func)
emcc.jsflags += -sGLOBAL_BASE=4096 # HYPOTHETICALLY keep func table indexes from overlapping w/ heap addr.
#emcc.jsflags += -sSTRICT # fails due to missing __syscall_...()
#emcc.jsflags += -sALLOW_UNIMPLEMENTED_SYSCALLS
#emcc.jsflags += -sFILESYSTEM=0 # only for experimentation. sqlite3 needs the FS API
#emcc.jsflags += -sABORTING_MALLOC # only for experimentation
emcc.jsflags += -sALLOW_TABLE_GROWTH
# ^^^^ -sALLOW_TABLE_GROWTH is required for installing new SQL UDFs
emcc.jsflags += -Wno-limited-postlink-optimizations
# ^^^^ emcc likes to warn when we have "limited optimizations" via the
# -g3 flag.
# emcc.jsflags += -sSTANDALONE_WASM # causes OOM errors, not sure why.

# Re. undefined symbol handling, see: https://lld.llvm.org/WebAssembly.html
emcc.jsflags += -sERROR_ON_UNDEFINED_SYMBOLS=1
emcc.jsflags += -sLLD_REPORT_UNDEFINED
#emcc.jsflags += --allow-undefined
#emcc.jsflags += --import-undefined
#emcc.jsflags += --unresolved-symbols=import-dynamic --experimental-pic
#emcc.jsflags += --experimental-pic --unresolved-symbols=ingore-all --import-undefined
#emcc.jsflags += --unresolved-symbols=ignore-all

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
# which have strict must-be-a-Number requirements. That aspect will
# make any eventual port to 64-bit address space extremely painful, as
# such constructs are found all over the place in the source code.
########################################################################

########################################################################
# -sSINGLE_FILE:
# https://github.com/emscripten-core/emscripten/blob/main/src/settings.js
#
# -sSINGLE_FILE=1 would be _really_ nice but we have to build with -g3
# for -O2 and higher to work (else minification breaks the code) and
# cannot wasm-strip the binary before it gets encoded into the JS
# file. The result is that the generated JS file is, because of the
# -g3 debugging info, _huge_.
########################################################################

$(sqlite3-api-build-version.js): $(bin.version-info) $(MAKEFILE)
	@echo "Making $@..."
	@{ \
  echo 'self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){'; \
	echo -n '  sqlite3.version = '; \
  $(bin.version-info) --json; \
  echo ';'; \
	echo '});'; \
  } > $@
$(sqlite3-license-version.js): $(sqlite3.h) $(sqlite3-license-version-header.js) \
  $(MAKEFILE)
	@echo "Making $@..."; { \
    cat $(sqlite3-license-version-header.js); \
    echo '/*'; \
    echo '** This code was built from sqlite3 version...'; \
    echo "**"; \
    awk -e '/define SQLITE_VERSION/{$$1=""; print "**" $$0}' \
        -e '/define SQLITE_SOURCE_ID/{$$1=""; print "**" $$0}' $(sqlite3.h); \
    echo "**"; \
    echo "** Using the Emscripten SDK version $(emcc.version)."; \
    echo '*/'; \
   } > $@

########################################################################
# --post-js and --pre-js are emcc flags we use to append/prepend JS to
# the generated emscripten module file. These rules set up the core
# pre/post files for use by the various builds.
pre-js.js.in := $(dir.api)/pre-js.c-pp.js
post-js.js.in := $(dir.tmp)/post-js.c-pp.js
post-jses.js := \
  $(dir.api)/post-js-header.js \
  $(sqlite3-api.js.in) \
  $(dir.api)/post-js-footer.js
$(post-js.js.in): $(post-jses.js) $(MAKEFILE)
	@echo "Making $@..."
	@for i in $(post-jses.js); do \
		echo "/* BEGIN FILE: $$i */"; \
		cat $$i; \
		echo "/* END FILE: $$i */"; \
	done > $@


########################################################################
# call-make-pre-post is a $(call)able which creates rules for
# pre-js-$(1).js. $1 = the base name of the JS file on whose behalf
# this pre-js is for (one of: sqlite3, sqlite3-wasmfs). $2 is the build
# mode: one of (vanilla, esm, bundler-friendly).  This sets up
# --[extern-][pre/post]-js flags in $(pre-post-$(1).flags.$(2)) and
# dependencies in $(pre-post-$(1).deps.$(2)).
define call-make-pre-post
pre-post-$(1).flags.$(2) ?=
$$(dir.tmp)/pre-js-$(1)-$(2).js: $$(pre-js.js.$(2)) $$(MAKEFILE)
	cp $$(pre-js.js.$(2)) $$@
	@if [ sqlite3-wasmfs = $(1) ]; then \
		echo "delete Module[xNameOfInstantiateWasm] /*for WASMFS build*/;"; \
	elif [ sqlite3 != $(1) ]; then \
		echo "Module[xNameOfInstantiateWasm].uri = '$(1).wasm';"; \
	fi >> $$@
pre-post-$(1).deps.$(2) := \
  $$(pre-post-jses.deps.$(2)) \
  $$(dir.tmp)/pre-js-$(1)-$(2).js
pre-post-$(1).flags.$(2) += \
  $$(pre-post-common.flags.$(2)) \
  --pre-js=$$(dir.tmp)/pre-js-$(1)-$(2).js
endef
# /post-js and pre-js
########################################################################

# Undocumented Emscripten feature: if the target file extension is
# "mjs", it defaults to ES6 module builds:
# https://github.com/emscripten-core/emscripten/issues/14383
sqlite3.wasm := $(dir.dout)/sqlite3.wasm
sqlite3-wasm.c := $(dir.api)/sqlite3-wasm.c
# sqlite3-wasm.o vs sqlite3-wasm.c: building against the latter
# (predictably) results in a slightly faster binary. We're close
# enough to the target speed requirements that the 500ms makes a
# difference, so we build all binaries against sqlite3-wasm.c instead
# of building a shared copy of sqlite3-wasm.o to link against.
########################################################################
# SQLITE3.xJS.EXPORT-DEFAULT is part of SQLITE3-WASMFS.xJS.RECIPE and
# SETUP_LIB_BUILD_MODE, factored into a separate piece to avoid code
# duplication. $1 is 1 if the build mode needs this workaround (esm,
# bundler-friendly) and 0 if not (vanilla).
#
# Reminder for ESM builds: even if we use -sEXPORT_ES6=0, emcc _still_
# adds:
#
#   export default $(sqlite3.js.init-func);
#
# when building *.mjs, which is bad because we need to export an
# overwritten version of that function and cannot "export default"
# twice. Because of this, we have to sed *.mjs to remove the _first_
# instance (only) of /^export default/.
#
# Upstream RFE:
# https://github.com/emscripten-core/emscripten/issues/18237
define SQLITE3.xJS.ESM-EXPORT-DEFAULT
if [ x1 = x$(1) ]; then \
		echo "Fragile workaround for an Emscripten annoyance. See SQLITE3.xJS.RECIPE."; \
		sed -i -e '0,/^export default/{/^export default/d;}' $@ || exit $$?; \
		if ! grep -q '^export default' $@; then \
			echo "Cannot find export default." 1>&2; \
			exit 1; \
		fi; \
fi
endef

# extern-post-js* and extern-pre-js* are files for use with
# Emscripten's --extern-pre-js and --extern-post-js flags.
extern-pre-js.js := $(dir.api)/extern-pre-js.js
extern-post-js.js.in := $(dir.api)/extern-post-js.c-pp.js
# Emscripten flags for --[extern-][pre|post]-js=... for the
# various builds.
pre-post-common.flags := \
  --extern-pre-js=$(sqlite3-license-version.js)
# pre-post-jses.deps.* = a list of dependencies for the
# --[extern-][pre/post]-js files.
pre-post-jses.deps.common := $(extern-pre-js.js) $(sqlite3-license-version.js)
########################################################################
# SETUP_LIB_BUILD_MODE is a $(call)'able which sets up numerous pieces
# for one of the build modes (vanilla, esm, bundler-friendly).
#
# $1 = build mode name
# $2 = 1 for ESM build mode, else 0
# $3 = resulting sqlite-api JS/MJS file
# $4 = resulting JS/MJS file
# $5 = -D... flags for $(bin.c-pp)
# $6 = emcc -sXYZ flags
define SETUP_LIB_BUILD_MODE
$(info Setting up build [$(1)]: $(4))
c-pp.D.$(1) := $(5)
pre-js.js.$(1) := $$(dir.api)/pre-js.$(1).js
$$(eval $$(call C-PP.FILTER,$$(pre-js.js.in),$$(pre-js.js.$(1)),$$(c-pp.D.$(1))))
post-js.js.$(1) := $$(dir.tmp)/post-js.$(1).js
$$(eval $$(call C-PP.FILTER,$$(post-js.js.in),$$(post-js.js.$(1)),$$(c-pp.D.$(1))))
extern-post-js.js.$(1) := $$(dir.tmp)/extern-post-js.$(1).js
$$(eval $$(call C-PP.FILTER,$$(extern-post-js.js.in),$$(extern-post-js.js.$(1)),$$(c-pp.D.$(1))))
pre-post-common.flags.$(1) := \
  $$(pre-post-common.flags) \
  --post-js=$$(post-js.js.$(1)) \
  --extern-post-js=$$(extern-post-js.js.$(1))
pre-post-jses.deps.$(1) := $$(pre-post-jses.deps.common) \
  $$(post-js.js.$(1)) $$(extern-post-js.js.$(1))
$$(eval $$(call call-make-pre-post,sqlite3,$(1)))
emcc.flags.sqlite3.$(1) := $(6)
$$(eval $$(call C-PP.FILTER, $$(sqlite3-api.js.in), $(3), $(5)))
$(4): $(3)
$(4): $(3) $$(MAKEFILE) $$(sqlite3-wasm.c) $$(EXPORTED_FUNCTIONS.api) $$(pre-post-sqlite3.deps.$(1))
	@echo "Building $$@ ..."
	$$(emcc.bin) -o $$@ $$(emcc_opt_full) $$(emcc.flags) \
    $$(emcc.jsflags) \
    $$(pre-post-sqlite3.flags.$(1)) $$(emcc.flags.sqlite3.$(1)) \
    $$(cflags.common) $$(SQLITE_OPT) $$(sqlite3-wasm.c)
	@$$(call SQLITE3.xJS.ESM-EXPORT-DEFAULT,$(2))
	@if [ bundler-friendly = $(1) ]; then \
		echo "Patching $(3) for sqlite3.wasm..."; \
		rm -f $$(dir.dout)/sqlite3-bundler-friendly.wasm; \
		sed -i -e 's/sqlite3-bundler-friendly.wasm/sqlite3.wasm/g' $$@ || exit $$$$?; \
	fi
	chmod -x $$(sqlite3.wasm)
	$$(maybe-wasm-strip) $$(sqlite3.wasm)
	@ls -la $@ $$(sqlite3.wasm)
all: $(4)
quick: $(4)
CLEAN_FILES += $(3) $(4)
endef
# ^^^ /SETUP_LIB_BUILD_MODE
########################################################################
sqlite3-api.js := $(dir.dout)/sqlite3-api.js
sqlite3.js := $(dir.dout)/sqlite3.js
sqlite3-api.mjs := $(dir.dout)/sqlite3-api.mjs
sqlite3.mjs := $(dir.dout)/sqlite3.mjs
sqlite3-api-bundler-friendly.mjs := $(dir.dout)/sqlite3-api-bundler-friendly.mjs
sqlite3-bundler-friendly.mjs := $(dir.dout)/sqlite3-bundler-friendly.mjs
# Maintenance reminder: careful not to introduce spaces around args $1, $2
#$(info $(call SETUP_LIB_BUILD_MODE,vanilla,0, $(sqlite3-api.js), $(sqlite3.js)))
$(eval $(call SETUP_LIB_BUILD_MODE,vanilla,0, $(sqlite3-api.js), $(sqlite3.js)))
$(eval $(call SETUP_LIB_BUILD_MODE,esm,1, $(sqlite3-api.mjs), $(sqlite3.mjs), \
       -Dtarget=es6-module, -sEXPORT_ES6 -sUSE_ES6_IMPORT_META))
$(eval $(call SETUP_LIB_BUILD_MODE,bundler-friendly,1,\
       $(sqlite3-api-bundler-friendly.mjs),$(sqlite3-bundler-friendly.mjs),\
       $(c-pp.D.esm) -Dtarget=es6-bundler-friendly, $(emcc.flags.sqlite3.esm)))
# The various -D... values used by *.c-pp.js include:
#
# -Dtarget=es6-module: for all ESM module builds
#
# -Dtarget=es6-module -Dtarget=es6-bundler-friendly: intended for
#    "bundler-friendly" ESM module build. These have some restrictions
#    on how URL() objects are constructed in some contexts: URLs which
#    refer to files which are part of this project must be references
#    as string literals so that bundlers' static-analysis tools can
#    find those files and include them in their bundles.
#
########################################################################
########################################################################
# We have to ensure that we do not build both $(sqlite3*.*js) in
# parallel because both result in the creation of $(sqlite3.wasm). We
# have no way to build just the .mjs file without also building the
# .wasm file because the generated .mjs file has to include info about
# the imports needed by the wasm file, so they have to be built
# together. i.e.  we're building $(sqlite3.wasm) multiple times, but
# that's unavoidable (and harmless, just a waste of build time).
$(sqlite3.wasm): $(sqlite3.js)
$(sqlite3.mjs): $(sqlite3.js)
$(sqlite3-bundler-friendly.mjs): $(sqlite3.mjs)
CLEAN_FILES += $(sqlite3.wasm)

########################################################################
# We need separate copies of certain supplementary JS files for the
# bundler-friendly build. Concretely, any supplemental JS files which
# themselves use importScripts() or Workers or URL() constructors
# which refer to other in-tree (m)JS files quire a bundler-friendly
# copy.
sqlite3-worker1.js.in := $(dir.api)/sqlite3-worker1.c-pp.js
sqlite3-worker1-promiser.js.in := $(dir.api)/sqlite3-worker1-promiser.c-pp.js
sqlite3-worker1.js := $(dir.dout)/sqlite3-worker1.js
sqlite3-worker1-promiser.js := $(dir.dout)/sqlite3-worker1-promiser.js
sqlite3-worker1-bundler-friendly.js := $(dir.dout)/sqlite3-worker1-bundler-friendly.js
sqlite3-worker1-promiser-bundler-friendly.js := $(dir.dout)/sqlite3-worker1-promiser-bundler-friendly.js
$(eval $(call C-PP.FILTER,$(sqlite3-worker1.js.in),$(sqlite3-worker1.js)))
$(eval $(call C-PP.FILTER,$(sqlite3-worker1.js.in),$(sqlite3-worker1-bundler-friendly.js),\
    $(c-pp.D.bundler-friendly)))
$(eval $(call C-PP.FILTER,$(sqlite3-worker1-promiser.js.in),$(sqlite3-worker1-promiser.js)))
$(eval $(call C-PP.FILTER,$(sqlite3-worker1-promiser.js.in),\
    $(sqlite3-worker1-promiser-bundler-friendly.js),\
    $(c-pp.D.bundler-friendly)))
$(sqlite3-bundler-friendly.mjs): $(sqlite3-worker1-bundler-friendly.js) \
    $(sqlite3-worker1-promiser-bundler-friendly.js)
$(sqlite3.js) $(sqlite3.mjs): $(sqlite3-worker1.js) $(sqlite3-worker1-promiser.js)

########################################################################
# batch-runner.js is part of one of the test apps which reads in SQL
# dumps generated by $(speedtest1) and executes them.
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
# Wasmified speedtest1 is our primary benchmarking tool.
#
# emcc.speedtest1.common = emcc flags used by multiple builds of speedtest1
# emcc.speedtest1 = emcc flags used by main build of speedtest1
emcc.speedtest1.common := $(emcc_opt_full)
emcc.speedtest1 :=
emcc.speedtest1 += -sENVIRONMENT=web
emcc.speedtest1 += -sALLOW_MEMORY_GROWTH
emcc.speedtest1 += -sINITIAL_MEMORY=$(emcc.INITIAL_MEMORY.$(emcc.INITIAL_MEMORY))
emcc.speedtest1.common += -sINVOKE_RUN=0
emcc.speedtest1.common += --no-entry
emcc.speedtest1.common += -sABORTING_MALLOC
emcc.speedtest1.common += -sSTRICT_JS
emcc.speedtest1.common += -sMODULARIZE
emcc.speedtest1.common += -Wno-limited-postlink-optimizations
EXPORTED_FUNCTIONS.speedtest1 := $(abspath $(dir.tmp)/EXPORTED_FUNCTIONS.speedtest1)
emcc.speedtest1.common += -sSTACK_SIZE=512KB
emcc.speedtest1.common += -sEXPORTED_FUNCTIONS=@$(EXPORTED_FUNCTIONS.speedtest1)
emcc.speedtest1.common += $(emcc.exportedRuntimeMethods)
emcc.speedtest1.common += -sALLOW_TABLE_GROWTH
emcc.speedtest1.common += -sDYNAMIC_EXECUTION=0
emcc.speedtest1.common += --minify 0
emcc.speedtest1.common += -sEXPORT_NAME=$(sqlite3.js.init-func)
emcc.speedtest1.common += -sWASM_BIGINT=$(emcc.WASM_BIGINT)
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
speedtest1.wasm := $(dir.dout)/speedtest1.wasm
cflags.speedtest1 := $(cflags.common) -DSQLITE_SPEEDTEST1_WASM
speedtest1.cses := $(speedtest1.c) $(sqlite3-wasm.c)
$(eval $(call call-make-pre-post,speedtest1,vanilla))
$(speedtest1.js): $(MAKEFILE) $(speedtest1.cses) \
    $(pre-post-speedtest1.deps.vanilla) \
    $(EXPORTED_FUNCTIONS.speedtest1)
	@echo "Building $@ ..."
	$(emcc.bin) \
        $(emcc.speedtest1) $(emcc.speedtest1.common) \
        $(cflags.speedtest1) $(pre-post-speedtest1.flags.vanilla) \
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
# tester1 is the main unit and regression test application and needs
# to be able to run in 4 separate modes to cover the primary
# client-side use cases:
#
# 1) Load sqlite3 in the main UI thread of a conventional script.
# 2) Load sqlite3 in a conventional Worker thread.
# 3) Load sqlite3 as an ES6 module (ESM) in the main thread.
# 4) Load sqlite3 as an ESM worker. (Not all browsers support this.)
#
# To that end, we require two separate builds of tester1.js:
#
#  tester1.js: cases 1 and 2
#  tester1.mjs: cases 3 and 4
#
# To create those, we filter tester1.c-pp.js with $(bin.c-pp)...
$(eval $(call C-PP.FILTER,tester1.c-pp.js,tester1.js))
$(eval $(call C-PP.FILTER,tester1.c-pp.js,tester1.mjs,$(c-pp.D.esm)))
$(eval $(call C-PP.FILTER,tester1.c-pp.html,tester1.html))
$(eval $(call C-PP.FILTER,tester1.c-pp.html,tester1-esm.html,$(c-pp.D.esm)))
tester1: tester1.js tester1.mjs tester1.html tester1-esm.html
# Note that we do not include $(sqlite3-bundler-friendly.mjs) in this
# because bundlers are client-specific.
all quick: tester1

########################################################################
# Convenience rules to rebuild with various -Ox levels. Much
# experimentation shows -O2 to be the clear winner in terms of speed.
# Note that build times with anything higher than -O0 are somewhat
# painful.

.PHONY: o0 o1 o2 o3 os oz
o-xtra :=
#o-xtra ?= -flto
# ^^^^ -flto can have a considerably performance boost at -O0 but
# doubles the build time and seems to have negligible, if any, effect
# on higher optimization levels.
o0: clean
	$(MAKE) -e "emcc_opt=-O0"
o1: clean
	$(MAKE) -e "emcc_opt=-O1 $(o-xtra)"
o2: clean
	$(MAKE) -j2 -e "emcc_opt=-O2 $(o-xtra)"
qo2: clean
	$(MAKE) -j2 -e "emcc_opt=-O2 $(o-xtra)" quick
o3: clean
	$(MAKE) -e "emcc_opt=-O3 $(o-xtra)"
os: clean
	@echo "WARNING: -Os can result in a build with mysteriously missing pieces!"
	$(MAKE) -e "emcc_opt=-Os $(o-xtra)"
oz: clean
	$(MAKE) -j2 -e "emcc_opt=-Oz $(o-xtra)"
qoz: clean
	$(MAKE) -j2 -e "emcc_opt=-Oz $(o-xtra)" quick

########################################################################
# Sub-makes...

# sqlite.org/fiddle application...
include fiddle.make

# Only add wasmfs if wasmfs.enable=1 or we're running (dist)clean
ifneq (,$(filter wasmfs,$(MAKECMDGOALS)))
wasmfs.enable ?= 1
else
wasmfs.enable ?= $(if $(filter %clean,$(MAKECMDGOALS)),1,0)
endif
ifeq (1,$(wasmfs.enable))
# wasmfs build disabled 2022-10-19 per /chat discussion.
# OPFS-over-wasmfs was initially a stopgap measure and a convenient
# point of comparison for the OPFS sqlite3_vfs's performance, but it
# currently doubles our deliverables and build maintenance burden for
# little benefit.
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
# Push files to public wasm-testing.sqlite.org server
wasm-testing.include = *.js *.mjs *.html \
  ./tests \
  batch-runner.list \
  $(dir.dout) $(dir.sql) $(dir.common) $(dir.fiddle) $(dir.jacc)
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
wasm.docs.home ?= ../../../wasm
wasm.docs.found = $(if $(wildcard $(wasm.docs.home)/api-index.md),\
  $(wildcard $(wasm.docs.home)),)
.PHONY: update-docs
ifeq (,$(wasm.docs.found))
update-docs:
	@echo "Cannot find wasm docs checkout."; \
	echo "Pass wasm.docs.home=/path/to/wasm/docs/checkout or edit this makefile to suit."; \
	exit 127
else
wasm.docs.jswasm := $(wasm.docs.home)/jswasm
update-docs: $(bin.stripccomments) $(sqlite3.js) $(sqlite3.wasm)
	@echo "Copying files to the /wasm docs. Be sure to use an -Oz build for this!"
	cp $(sqlite3.wasm) $(wasm.docs.jswasm)/.
	$(bin.stripccomments) -k -k < $(sqlite3.js) \
		| sed -e '/^[ \t]*$$/d' > $(wasm.docs.jswasm)/sqlite3.js
	cp demo-123.js demo-123.html demo-123-worker.html $(wasm.docs.home)
	sed -n -e '/EXTRACT_BEGIN/,/EXTRACT_END/p' \
		module-symbols.html > $(wasm.docs.home)/module-symbols.html
endif
# end /wasm docs
########################################################################

########################################################################
# Create main client downloadable zip file:
ifneq (,$(filter dist snapshot,$(MAKECMDGOALS)))
include dist.make
endif

# Run local web server for the test/demo pages.
httpd:
	althttpd -max-age 1 -enable-sab -page index.html
