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


########################################################################
# Chicken/egg situation: we need $(version-info) to get the version
# info for the archive name, but that binary may not yet be built, so
# we have to use a temporary name for the archive.
dist-name = sqlite-wasm-TEMP
dist-archive = $(dist-name).zip

#ifeq (0,1)
#  $(info WARNING  *******************************************************************)
#  $(info ** Be sure to create the desired build configuration before creating the)
#  $(info ** distribution archive. Use one of the following targets to do so:)
#  $(info **   o2: builds with -O2, resulting in the fastest builds)
#  $(info **   oz: builds with -Oz, resulting in the smallest builds)
#  $(info /WARNING *******************************************************************)
#endif

demo-123.html := $(dir.wasm)/demo-123.html
demo-123-worker.html := $(dir.wasm)/demo-123-worker.html
demo-123.js := $(dir.wasm)/demo-123.js
demo-files := $(demo-123.js) $(demo-123.html) $(demo-123-worker.html)
README-dist := $(dir.wasm)/README-dist.txt
$(dist-archive): $(sqlite3.wasm) $(sqlite3.js) $(sqlite3-wasmfs.wasm) $(sqlite3-wasmfs.js)
#$(dist-archive): $(sqlite3.h) $(sqlite3.c) $(sqlite3-wasm.c)
$(dist-archive): $(MAKEFILE.dist) $(version-info) $(demo-files) $(README-dist)
$(dist-archive): oz
	rm -fr $(dist-name)
	mkdir -p $(dist-name)/main $(dist-name)/wasmfs
	cp -p $(README-dist) $(dist-name)/README.txt
	cp -p $(sqlite3.wasm) $(sqlite3.js) $(dist-name)/main
	cp -p $(demo-files) $(dist-name)/main
	cp -p $(sqlite3-wasmfs.wasm) $(sqlite3-wasmfs.js) $(dist-name)/wasmfs
	for i in $(demo-123.js) $(demo-123.html); do \
    sed -e 's/\bsqlite3\.js\b/sqlite3-wasmfs.js/' $$i \
      > $(dist-name)/wasmfs/$${i##*/} || exit; \
  done
	vnum=$$($(version-info) --version-number); \
	vdir=sqlite-wasm-$$vnum; \
	arc=$$vdir.zip; \
	rm -f $$arc; \
	mv $(dist-name) $$vdir; \
	zip -qr $$arc $$vdir; \
	rm -fr $$vdir; \
	ls -la $$arc; \
	unzip -l $$arc

#$(shell $(version-info) --version-number)
dist: $(dist-archive)
clean-dist:
	rm -f $(dist-archive)
clean: clean-dist
