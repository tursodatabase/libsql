#!/do/not/make
#^^^ help emacs select edit mode
#
# Intended to include'd by ./GNUmakefile.
#
# 'make dist' rules for creating a distribution archive of the WASM/JS
# pieces, noting that we only build a dist of the built files, not the
# numerous pieces required to build them.
#
# Use 'make snapshot' to create "snapshot" releases. They use a
# distinctly different zip file and top directory name to distinguish
# them from release builds.
#######################################################################
MAKEFILE.dist := $(lastword $(MAKEFILE_LIST))

########################################################################
# Chicken/egg situation: we need $(bin.version-info) to get the
# version info for the archive name, but that binary may not yet be
# built, and won't be built until we expand the dependencies. Thus we
# have to use a temporary name for the archive until we can get
# that binary built.
ifeq (,$(filter snapshot,$(MAKECMDGOALS)))
dist-name-prefix := sqlite-wasm
else
dist-name-prefix := sqlite-wasm-snapshot-$(shell /usr/bin/date +%Y%m%d)
endif
dist-name := $(dist-name-prefix)-TEMP

########################################################################
# dist.build must be the name of a target which triggers the build of
# the files to be packed into the dist archive.  The intention is that
# it be one of (o0, o1, o2, o3, os, oz), each of which uses like-named
# -Ox optimization level flags. The o2 target provides the best
# overall runtime speeds. The oz target provides slightly slower
# speeds (roughly 10%) with significantly smaller WASM file
# sizes. Note that -O2 (the o2 target) results in faster binaries than
# both -O3 and -Os (the o3 and os targets) in all tests run to
# date. Our general policy is that we want the smallest binaries for
# dist zip files, so use the oz build unless there is a compelling
# reason not to.
dist.build ?= qoz

dist-dir.top := $(dist-name)
dist-dir.jswasm := $(dist-dir.top)/$(notdir $(dir.dout))
dist-dir.common := $(dist-dir.top)/common
dist.top.extras := \
    demo-123.html demo-123-worker.html demo-123.js \
    tester1.html tester1-worker.html tester1-esm.html \
    tester1.js tester1.mjs \
    demo-jsstorage.html demo-jsstorage.js \
    demo-worker1.html demo-worker1.js \
    demo-worker1-promiser.html demo-worker1-promiser.js
dist.jswasm.extras := $(sqlite3-api.ext.jses) $(sqlite3.wasm)
dist.common.extras := \
    $(wildcard $(dir.common)/*.css) \
    $(dir.common)/SqliteTestUtil.js

.PHONY: dist snapshot
# DIST_STRIP_COMMENTS $(call)able to be used in stripping C-style
# from the dist copies of certain files.
#
#  $1 = source js file
#  $2 = flags for $(bin.stripcomments)
define DIST_STRIP_COMMENTS
$(bin.stripccomments) $(2) < $(1) > $(dist-dir.jswasm)/$(notdir $(1)) || exit;
endef
# STRIP_K1.js = list of JS files which need to be passed through
# $(bin.stripcomments) with a single -k flag.
STRIP_K1.js := $(sqlite3-worker1.js) $(sqlite3-worker1-promiser.js) \
  $(sqlite3-worker1-bundler-friendly.js) $(sqlite3-worker1-promiser-bundler-friendly.js)
# STRIP_K2.js = list of JS files which need to be passed through
# $(bin.stripcomments) with two -k flags.
STRIP_K2.js := $(sqlite3.js) $(sqlite3.mjs) \
  $(sqlite3-bundler-friendly.mjs) $(sqlite3-node.mjs)
########################################################################
# dist: create the end-user deliverable archive.
#
# Maintenance reminder: because dist depends on $(dist.build), and
# $(dist.build) will depend on clean, having any deps on
# $(dist-archive) which themselves may be cleaned up by the clean
# target will lead to grief in parallel builds (-j #). Thus
# dist's deps must be trimmed to non-generated files or
# files which are _not_ cleaned up by the clean target.
#
# Note that we require $(bin.version-info) in order to figure out the
# dist file's name, so cannot (without a recursive make) have the
# target name equal to the archive name.
dist: \
    $(bin.stripccomments) $(bin.version-info) \
    $(dist.build) $(STRIP_K1.js) $(STRIP_K2.js) \
    $(MAKEFILE) $(MAKEFILE.dist)
	@echo "Making end-user deliverables..."
	@rm -fr $(dist-dir.top)
	@mkdir -p $(dist-dir.jswasm) $(dist-dir.common)
	@cp -p $(dist.top.extras) $(dist-dir.top)
	@cp -p README-dist.txt $(dist-dir.top)/README.txt
	@cp -p index-dist.html $(dist-dir.top)/index.html
	@cp -p $(dist.jswasm.extras) $(dist-dir.jswasm)
	@$(foreach JS,$(STRIP_K1.js),$(call DIST_STRIP_COMMENTS,$(JS),-k))
	@$(foreach JS,$(STRIP_K2.js),$(call DIST_STRIP_COMMENTS,$(JS),-k -k))
	@cp -p $(dist.common.extras) $(dist-dir.common)
	@set -e; \
		vnum=$$($(bin.version-info) --download-version); \
		vdir=$(dist-name-prefix)-$$vnum; \
		arczip=$$vdir.zip; \
		echo "Making $$arczip ..."; \
		rm -fr $$arczip $$vdir; \
		mv $(dist-dir.top) $$vdir; \
		zip -qr $$arczip $$vdir; \
		rm -fr $$vdir; \
		ls -la $$arczip; \
		set +e; \
		unzip -lv $$arczip || echo "Missing unzip app? Not fatal."
ifeq (,$(wasm.docs.found))
snapshot: dist
	@echo "To upload the snapshot build to the wasm docs server:"; \
	echo "1) move $(dist-name-prefix)*.zip to the top of a wasm docs checkout."; \
  echo "2) run 'make uv-sync'"
else
snapshot: dist
	@echo "Moving snapshot to [$(wasm.docs.found)]..."; \
	mv $(dist-name-prefix)*.zip $(wasm.docs.found)/.
	@echo "Run 'make uv-sync' from $(wasm.docs.found) to upload it."
endif
# We need a separate `clean` rule to account for weirdness in
# a sub-make, where we get a copy of the $(dist-name) dir
# copied into the new $(dist-name) dir.
.PHONY: dist-clean
clean: dist-clean
dist-clean:
	rm -fr $(dist-name) $(wildcard sqlite-wasm-*.zip)
