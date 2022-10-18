#!/do/not/make
#^^^ help emacs select edit mode
#
# Intended to include'd by ./GNUmakefile.
#
# 'make dist' rules for creating a distribution archive of the WASM/JS
# pieces, noting that we only build a dist of the built files, not the
# numerous pieces required to build them.
#######################################################################
MAKEFILE.dist := $(lastword $(MAKEFILE_LIST))

ifeq (0,$(HAVE_WASMFS))
$(error The 'dist' target needs to be run on a WASMFS-capable platform.)
endif

########################################################################
# Chicken/egg situation: we need $(version-info) to get the version
# info for the archive name, but that binary may not yet be built, and
# won't be built until we expand the dependencies. We have to use a
# temporary name for the archive.
dist-name = sqlite-wasm-TEMP
dist-archive = $(dist-name).zip
.PHONY: $(dist-archive)
CLEAN_FILES += $(wildcard sqlite-wasm-*.zip)
#ifeq (0,1)
#  $(info WARNING  *******************************************************************)
#  $(info ** Be sure to create the desired build configuration before creating the)
#  $(info ** distribution archive. Use one of the following targets to do so:)
#  $(info **)
#  $(info **   o2: builds with -O2, resulting in the fastest builds)
#  $(info **   oz: builds with -Oz, resulting in the smallest builds)
#  $(info /WARNING *******************************************************************)
#endif

########################################################################
# dist-opt must be the name of a target which triggers the
# build of the files to be packed into the dist archive.  The
# intention is that it be one of (o0, o1, o2, o3, os, oz), each of
# which uses like-named -Ox optimization level flags. The o2 target
# provides the best overall runtime speeds. The oz target provides
# slightly slower speeds (roughly 10%) with significantly smaller WASM
# file sizes. Note that -O2 (the o2 target) results in faster binaries
# than both -O3 and -Os (the o3 and os targets) in all tests run to
# date.
dist-opt ?= oz

demo-123.html := $(dir.wasm)/demo-123.html
demo-123-worker.html := $(dir.wasm)/demo-123-worker.html
demo-123.js := $(dir.wasm)/demo-123.js
demo-files := $(demo-123.js) $(demo-123.html) $(demo-123-worker.html) \
              tester1.html tester1.js tester1-worker.html
README-dist := $(dir.wasm)/README-dist.txt
dist-dir-main := $(dist-name)/main
dist-dir-wasmfs := $(dist-name)/wasmfs
########################################################################
# $(dist-archive): create the end-user deliverable archive.
#
# Maintenance reminder: because $(dist-archive) depends on
# $(dist-opt), and $(dist-opt) will depend on clean, having any deps
# on $(dist-archive) which themselves may be cleaned up by the clean
# target will lead to grief in parallel builds (-j #). Thus
# $(dist-target)'s deps must be trimmed to non-generated files or
# files which are _not_ cleaned up by the clean target.
$(dist-archive): \
    $(stripccomments) $(version-info) \
    $(dist-opt) \
    $(MAKEFILE) $(MAKEFILE.dist)
	@echo "Making end-user deliverables..."
	@rm -fr $(dist-name)
	@mkdir -p $(dist-dir-main) $(dist-dir-wasmfs)
	@cp -p $(README-dist) $(dist-name)/README.txt
	@cp -p $(sqlite3.wasm) $(dist-dir-main)
	@$(stripccomments) -k -k < $(sqlite3.js) \
		> $(dist-dir-main)/$(notdir $(sqlite3.js))
	@cp -p $(demo-files) $(dist-dir-main)
	@cp -p $(sqlite3-wasmfs.wasm) sqlite3-wasmfs.worker.js $(dist-dir-wasmfs)
	@$(stripccomments) -k -k < $(sqlite3-wasmfs.js) \
		> $(dist-dir-wasmfs)/$(notdir $(sqlite3-wasmfs.js))
	@for i in $(demo-123.js) $(demo-123.html); do \
    sed -e 's/\bsqlite3\.js\b/sqlite3-wasmfs.js/' $$i \
      > $(dist-dir-wasmfs)/$${i##*/} || exit; \
  done
	@vnum=$$($(version-info) --version-number); \
	vdir=sqlite-wasm-$$vnum; \
	arc=$$vdir.zip; \
	echo "Making $$arc ..."; \
	rm -f $$arc; \
	mv $(dist-name) $$vdir; \
	zip -qr $$arc $$vdir; \
	rm -fr $$vdir; \
	ls -la $$arc; \
	unzip -lv $$arc || echo "Missing unzip app? Not fatal."

#$(shell $(version-info) --version-number)
dist: $(dist-archive)
clean-dist:
	rm -f $(dist-archive)
clean: clean-dist
