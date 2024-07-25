# Quick-and-dirty makefile to bootstrap the sqlite3-jni project.  This
# build assumes a Linux-like system.
default: all

JAVA_HOME ?= $(HOME)/jdk/current
# e.g. /usr/lib/jvm/default-javajava-19-openjdk-amd64
JDK_HOME ?= $(JAVA_HOME)
# ^^^ JDK_HOME is not as widely used as JAVA_HOME
bin.jar     := $(JDK_HOME)/bin/jar
bin.java    := $(JDK_HOME)/bin/java
bin.javac   := $(JDK_HOME)/bin/javac
bin.javadoc := $(JDK_HOME)/bin/javadoc
ifeq (,$(wildcard $(JDK_HOME)))
$(error set JDK_HOME to the top-most dir of your JDK installation.)
endif
MAKEFILE := $(lastword $(MAKEFILE_LIST))
$(MAKEFILE):

package.jar := sqlite3-jni.jar

dir.top     := ../..
dir.tool    := ../../tool
dir.jni     := $(patsubst %/,%,$(dir $(MAKEFILE)))
dir.src     := $(dir.jni)/src
dir.src.c   := $(dir.src)/c
dir.bld     := $(dir.jni)/bld
dir.bld.c   := $(dir.bld)
dir.src.jni := $(dir.src)/org/sqlite/jni
dir.src.capi := $(dir.src.jni)/capi
dir.src.fts5 := $(dir.src.jni)/fts5
dir.tests   := $(dir.src)/tests
mkdir       ?= mkdir -p
$(dir.bld.c):
	$(mkdir) $@

javac.flags ?= -Xlint:unchecked -Xlint:deprecation
java.flags ?=
javac.flags += -encoding utf8
# -------------^^^^^^^^^^^^^^ required for Windows builds
jnicheck ?= 1
ifeq (1,$(jnicheck))
  java.flags += -Xcheck:jni
endif

classpath := $(dir.src)
CLEAN_FILES := $(package.jar)
DISTCLEAN_FILES := $(dir.jni)/*~ $(dir.src.c)/*~ $(dir.src.jni)/*~

sqlite3-jni.h := $(dir.src.c)/sqlite3-jni.h
.NOTPARALLEL: $(sqlite3-jni.h)
CApi.java := $(dir.src.capi)/CApi.java
SQLTester.java := $(dir.src.capi)/SQLTester.java
CApi.class := $(CApi.java:.java=.class)
SQLTester.class := $(SQLTester.java:.java=.class)

########################################################################
# The future of FTS5 customization in this API is as yet unclear.
# The pieces are all in place, and are all thin proxies so not much
# complexity, but some semantic changes were required in porting
# which are largely untested.
#
# Reminder: this flag influences the contents of $(sqlite3-jni.h),
# which is checked in. Please do not check in changes to that file in
# which the fts5 APIs have been stripped unless that feature is
# intended to be stripped for good.
enable.fts5 ?= 1

ifeq (,$(wildcard $(dir.tests)/*))
  enable.tester := 0
else
  enable.tester := 1
endif

# bin.version-info = binary to output various sqlite3 version info
# building the distribution zip file.
bin.version-info := $(dir.top)/version-info
.NOTPARALLEL: $(bin.version-info)
$(bin.version-info): $(dir.tool)/version-info.c $(sqlite3.h) $(dir.top)/Makefile
	$(MAKE) -C $(dir.top) version-info

# Be explicit about which Java files to compile so that we can work on
# in-progress files without requiring them to be in a compilable statae.
JAVA_FILES.main := $(patsubst %,$(dir.src.jni)/annotation/%,\
  Experimental.java \
  NotNull.java \
  Nullable.java \
) $(patsubst %,$(dir.src.capi)/%,\
  AbstractCollationCallback.java \
  AggregateFunction.java \
  AuthorizerCallback.java \
  AutoExtensionCallback.java \
  BusyHandlerCallback.java \
  CollationCallback.java \
  CollationNeededCallback.java \
  CommitHookCallback.java \
  ConfigLogCallback.java \
  ConfigSqlLogCallback.java \
  NativePointerHolder.java \
  OutputPointer.java \
  PrepareMultiCallback.java \
  PreupdateHookCallback.java \
  ProgressHandlerCallback.java \
  ResultCode.java \
  RollbackHookCallback.java \
  ScalarFunction.java \
  SQLFunction.java \
  CallbackProxy.java \
  CApi.java \
  TableColumnMetadata.java \
  TraceV2Callback.java \
  UpdateHookCallback.java \
  ValueHolder.java \
  WindowFunction.java \
  XDestroyCallback.java \
  sqlite3.java \
  sqlite3_blob.java \
  sqlite3_context.java \
  sqlite3_stmt.java \
  sqlite3_value.java \
) $(patsubst %,$(dir.src.jni)/wrapper1/%,\
  AggregateFunction.java \
  ScalarFunction.java \
  SqlFunction.java \
  Sqlite.java \
  SqliteException.java \
  ValueHolder.java \
  WindowFunction.java \
)

JAVA_FILES.unittest := $(patsubst %,$(dir.src.jni)/%,\
  capi/Tester1.java \
  wrapper1/Tester2.java \
)
ifeq (1,$(enable.fts5))
  JAVA_FILES.unittest += $(patsubst %,$(dir.src.fts5)/%,\
    TesterFts5.java \
  )
  JAVA_FILES.main += $(patsubst %,$(dir.src.fts5)/%,\
    fts5_api.java \
    fts5_extension_function.java \
    fts5_tokenizer.java \
    Fts5.java \
    Fts5Context.java \
    Fts5ExtensionApi.java \
    Fts5PhraseIter.java \
    Fts5Tokenizer.java \
    XTokenizeCallback.java \
  )
endif
JAVA_FILES.tester := $(SQLTester.java)
JAVA_FILES.package.info := \
  $(dir.src.jni)/package-info.java \
  $(dir.src.jni)/annotation/package-info.java

CLASS_FILES.main := $(JAVA_FILES.main:.java=.class)
CLASS_FILES.unittest := $(JAVA_FILES.unittest:.java=.class)
CLASS_FILES.tester := $(JAVA_FILES.tester:.java=.class)

JAVA_FILES += $(JAVA_FILES.main) $(JAVA_FILES.unittest)
ifeq (1,$(enable.tester))
  JAVA_FILES += $(JAVA_FILES.tester)
endif

CLASS_FILES :=
define CLASSFILE_DEPS
all: $(1).class
$(1).class: $(1).java
CLASS_FILES += $(1).class
endef
$(foreach B,$(basename \
  $(JAVA_FILES.main) $(JAVA_FILES.unittest) $(JAVA_FILES.tester)),\
  $(eval $(call CLASSFILE_DEPS,$(B))))
$(CLASS_FILES): $(MAKEFILE)
	$(bin.javac) $(javac.flags) -h $(dir.bld.c) -cp $(classpath) $(JAVA_FILES)

#.PHONY: classfiles

########################################################################
# Set up sqlite3.c and sqlite3.h...
#
# To build with SEE (https://sqlite.org/see), either put sqlite3-see.c
# in the top of this build tree or pass
# sqlite3.c=PATH_TO_sqlite3-see.c to the build. Note that only
# encryption modules with no 3rd-party dependencies will currently
# work here: AES256-OFB, AES128-OFB, and AES128-CCM. Not
# coincidentally, those 3 modules are included in the sqlite3-see.c
# bundle.
#
# A custom sqlite3.c must not have any spaces in its name.
# $(sqlite3.canonical.c) must point to the sqlite3.c in
# the sqlite3 canonical source tree, as that source file
# is required for certain utility and test code.
sqlite3.canonical.c := $(firstword $(wildcard $(dir.src.c)/sqlite3.c) $(dir.top)/sqlite3.c)
sqlite3.canonical.h := $(firstword $(wildcard $(dir.src.c)/sqlite3.h) $(dir.top)/sqlite3.h)
sqlite3.c := $(sqlite3.canonical.c)
sqlite3.h := $(sqlite3.canonical.h)
#ifeq (,$(shell grep sqlite3_activate_see $(sqlite3.c) 2>/dev/null))
#  SQLITE_C_IS_SEE := 0
#else
#  SQLITE_C_IS_SEE := 1
#  $(info This is an SEE build.)
#endif

.NOTPARALLEL: $(sqlite3.h)
$(sqlite3.h):
	$(MAKE) -C $(dir.top) sqlite3.c
$(sqlite3.c): $(sqlite3.h)

opt.threadsafe ?= 1
opt.fatal-oom ?= 1
opt.debug ?= 1
opt.metrics ?= 1
SQLITE_OPT = \
  -DSQLITE_THREADSAFE=$(opt.threadsafe) \
  -DSQLITE_TEMP_STORE=2 \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DSQLITE_C=$(sqlite3.c) \
  -DSQLITE_JNI_FATAL_OOM=$(opt.fatal-oom) \
  -DSQLITE_JNI_ENABLE_METRICS=$(opt.metrics)

opt.extras ?= 1
ifeq (1,$(opt.extras))
SQLITE_OPT += -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_EXPLAIN_COMMENTS \
  -DSQLITE_ENABLE_STMTVTAB \
  -DSQLITE_ENABLE_DBPAGE_VTAB \
  -DSQLITE_ENABLE_DBSTAT_VTAB \
  -DSQLITE_ENABLE_BYTECODE_VTAB \
  -DSQLITE_ENABLE_OFFSET_SQL_FUNC \
  -DSQLITE_ENABLE_PREUPDATE_HOOK \
  -DSQLITE_ENABLE_NORMALIZE \
  -DSQLITE_ENABLE_SQLLOG \
  -DSQLITE_ENABLE_COLUMN_METADATA
endif

ifeq (1,$(opt.debug))
  SQLITE_OPT += -DSQLITE_DEBUG -g -DDEBUG -UNDEBUG
else
  SQLITE_OPT += -Os
endif

ifeq (1,$(enable.fts5))
  SQLITE_OPT += -DSQLITE_ENABLE_FTS5
endif

sqlite3-jni.c := $(dir.src.c)/sqlite3-jni.c
sqlite3-jni.o := $(dir.bld.c)/sqlite3-jni.o
sqlite3-jni.h   := $(dir.src.c)/sqlite3-jni.h
package.dll := $(dir.bld.c)/libsqlite3-jni.so
# All javac-generated .h files must be listed in $(sqlite3-jni.h.in):
sqlite3-jni.h.in :=
# $(java.with.jni) lists all Java files which contain JNI decls:
java.with.jni :=
define ADD_JNI_H
sqlite3-jni.h.in += $$(dir.bld.c)/org_sqlite_jni$(3)_$(2).h
java.with.jni += $(1)/$(2).java
$$(dir.bld.c)/org_sqlite_jni$(3)_$(2).h: $(1)/$(2).java
endef
# Invoke ADD_JNI_H once for each Java file which includes JNI
# declarations:
$(eval $(call ADD_JNI_H,$(dir.src.capi),CApi,_capi))
$(eval $(call ADD_JNI_H,$(dir.src.capi),SQLTester,_capi))
ifeq (1,$(enable.fts5))
 $(eval $(call ADD_JNI_H,$(dir.src.fts5),Fts5ExtensionApi,_fts5))
 $(eval $(call ADD_JNI_H,$(dir.src.fts5),fts5_api,_fts5))
 $(eval $(call ADD_JNI_H,$(dir.src.fts5),fts5_tokenizer,_fts5))
endif
$(sqlite3-jni.h.in): $(dir.bld.c)

#package.dll.cfiles :=
package.dll.cflags = \
  -std=c99 \
  -fPIC \
  -I. \
  -I$(dir $(sqlite3.h)) \
  -I$(dir.src.c) \
  -I$(JDK_HOME)/include \
  $(patsubst %,-I%,$(patsubst %.h,,$(wildcard $(JDK_HOME)/include/*))) \
  -Wall
# The gross $(patsubst...) above is to include the platform-specific
# subdir which lives under $(JDK_HOME)/include and is a required
# include path for client-level code.
#
# Using (-Wall -Wextra) triggers an untennable number of
# gcc warnings from sqlite3.c for mundane things like
# unused parameters.
########################################################################
ifeq (1,$(enable.tester))
  package.dll.cflags += -DSQLITE_JNI_ENABLE_SQLTester
endif

$(sqlite3-jni.h): $(sqlite3-jni.h.in) $(MAKEFILE)
	@cat $(sqlite3-jni.h.in) > $@.tmp
	@if cmp $@ $@.tmp >/dev/null; then \
		rm -f $@.tmp; \
		echo "$@ not modified"; \
	else \
		mv $@.tmp $@; \
		echo "Updated $@"; \
	fi
	@if [ x1 != x$(enable.fts5) ]; then \
		echo "*** REMINDER:"; \
		echo "*** enable.fts5=0, so please do not check in changes to $@."; \
	fi

$(package.dll): $(sqlite3-jni.h) $(sqlite3.c) $(sqlite3.h)
$(package.dll): $(sqlite3-jni.c) $(MAKEFILE)
	$(CC) $(package.dll.cflags) $(SQLITE_OPT) \
			$(sqlite3-jni.c) -shared -o $@
all: $(package.dll)

.PHONY: test test-one
Tester1.flags ?=
Tester2.flags ?=
test.flags.jvm = -ea -Djava.library.path=$(dir.bld.c) \
                  $(java.flags) -cp $(classpath)
test.deps := $(CLASS_FILES) $(package.dll)
test-one: $(test.deps)
	$(bin.java) $(test.flags.jvm) org.sqlite.jni.capi.Tester1 $(Tester1.flags)
	$(bin.java) $(test.flags.jvm) org.sqlite.jni.wrapper1.Tester2 $(Tester2.flags)
test-sqllog: $(test.deps)
	@echo "Testing with -sqllog..."
	$(bin.java) $(test.flags.jvm) org.sqlite.jni.capi.Tester1 $(Tester1.flags) -sqllog
test-mt: $(test.deps)
	@echo "Testing in multi-threaded mode:";
	$(bin.java) $(test.flags.jvm) org.sqlite.jni.capi.Tester1 \
    -t 7 -r 50 -shuffle $(Tester1.flags)
	$(bin.java) $(test.flags.jvm) org.sqlite.jni.wrapper1.Tester2 \
    -t 7 -r 50 -shuffle $(Tester2.flags)

test: test-one test-mt
tests: test test-sqllog

tester.scripts := $(sort $(wildcard $(dir.src)/tests/*.test))
tester.flags ?= # --verbose
.PHONY: tester tester-local tester-ext
ifeq (1,$(enable.tester))
tester-local: $(CLASS_FILES.tester) $(package.dll)
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.capi.SQLTester $(tester.flags) $(tester.scripts)
tester: tester-local
else
tester:
	@echo "SQLTester support is disabled."
endif

tester.extdir.default := $(dir.tests)/ext
tester.extdir ?= $(tester.extdir.default)
tester.extern-scripts := $(wildcard $(tester.extdir)/*.test)
ifneq (,$(tester.extern-scripts))
tester-ext:
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.capi.SQLTester $(tester.flags) $(tester.extern-scripts)
else
tester-ext:
	@echo "******************************************************"; \
		echo "*** Include the out-of-tree test suite in the 'tester'"; \
		echo "*** target by either symlinking its directory to"; \
		echo "*** $(tester.extdir.default) or passing it to make"; \
		echo "*** as tester.extdir=/path/to/that/dir."; \
		echo "******************************************************";
endif

tester-ext: tester-local
tester: tester-ext
tests: tester
########################################################################
# Build each SQLITE_THREADMODE variant and run all tests against them.
multitest: clean
define MULTIOPT
multitest: multitest-$(1)
multitest-$(1):
	$$(MAKE) opt.debug=$$(opt.debug) $(patsubst %,opt.%,$(2)) \
		tests clean enable.fts5=1
endef

$(eval $(call MULTIOPT,01,threadsafe=0 oom=1))
$(eval $(call MULTIOPT,00,threadsafe=0 oom=0))
$(eval $(call MULTIOPT,11,threadsafe=1 oom=1))
$(eval $(call MULTIOPT,10,threadsafe=1 oom=0))
$(eval $(call MULTIOPT,21,threadsafe=2 oom=1))
$(eval $(call MULTIOPT,20,threadsafe=2 oom=0))


########################################################################
# jar bundle...
package.jar.in := $(abspath $(dir.src)/jar.in)
CLEAN_FILES += $(package.jar.in)
JAVA_FILES.jar := $(JAVA_FILES.main) $(JAVA_FILES.unittest) $(JAVA_FILES.package.info)
CLASS_FILES.jar := $(filter-out %/package-info.class,$(JAVA_FILES.jar:.java=.class))
$(package.jar.in): $(package.dll) $(MAKEFILE)
	ls -1 \
		$(dir.src.jni)/*/*.java $(dir.src.jni)/*/*.class \
	| sed -e 's,^$(dir.src)/,,' | sort > $@

$(package.jar): $(CLASS_FILES.jar) $(MAKEFILE) $(package.jar.in)
	@rm -f $(dir.src)/c/*~ $(dir.src.jni)/*~
	cd $(dir.src); $(bin.jar) -cfe ../$@ org.sqlite.jni.capi.Tester1 @$(package.jar.in)
	@ls -la $@
	@echo "To use this jar you will need the -Djava.library.path=DIR/CONTAINING/libsqlite3-jni.so flag."
	@echo "e.g. java -Djava.library.path=bld -jar $@"

jar: $(package.jar)
run-jar: $(package.jar) $(package.dll)
	$(bin.java) -Djava.library.path=$(dir.bld) -jar $(package.jar) $(run-jar.flags)

########################################################################
# javadoc...
dir.doc   := $(dir.jni)/javadoc
doc.index := $(dir.doc)/index.html
javadoc.exclude := -exclude org.sqlite.jni.fts5
# ^^^^ 2023-09-13: elide the fts5 parts from the public docs for
# the time being, as it's not clear where the Java bindings for
# those bits are going.
# javadoc.exclude += -exclude org.sqlite.jni.capi
# ^^^^ exclude the capi API only for certain builds (TBD)
$(doc.index): $(JAVA_FILES.main) $(MAKEFILE)
	@if [ -d $(dir.doc) ]; then rm -fr $(dir.doc)/*; fi
	$(bin.javadoc) -cp $(classpath) -d $(dir.doc) -quiet \
		-subpackages org.sqlite.jni $(javadoc.exclude)
	@echo "javadoc output is in $@"

.PHONY: doc javadoc docserve
.FORCE: doc
doc: $(doc.index)
javadoc: $(doc.index)
# Force rebild of docs
redoc:
	@rm -f $(doc.index)
	@$(MAKE) doc
docserve: $(doc.index)
	cd $(dir.doc) && althttpd -max-age 1 -page index.html
########################################################################
# Clean up...
CLEAN_FILES += $(dir.bld.c)/* \
  $(dir.src.jni)/*.class \
  $(dir.src.jni)/*/*.class \
  $(package.dll) \
  hs_err_pid*.log

.PHONY: clean distclean
clean:
	-rm -f $(CLEAN_FILES)
distclean: clean
	-rm -f $(DISTCLEAN_FILES)
	-rm -fr $(dir.bld.c) $(dir.doc)

########################################################################
# disttribution bundle rules...

ifeq (,$(filter snapshot,$(MAKECMDGOALS)))
dist-name-prefix := sqlite-jni
else
dist-name-prefix := sqlite-jni-snapshot-$(shell /usr/bin/date +%Y%m%d)
endif
dist-name := $(dist-name-prefix)-TEMP


dist-dir.top := $(dist-name)
dist-dir.src := $(dist-dir.top)/src
dist.top.extras := \
  README.md

.PHONY: dist snapshot

dist: \
    $(bin.version-info) $(sqlite3.canonical.c) \
    $(package.jar) $(MAKEFILE)
	@echo "Making end-user deliverables..."
	@echo "****************************************************************************"; \
	echo  "*** WARNING: be sure to build this with JDK8 (javac 1.8) for compatibility."; \
	echo  "*** reasons!"; $$($(bin.javac) -version); \
	echo  "****************************************************************************"
	@rm -fr $(dist-dir.top)
	@mkdir -p $(dist-dir.src)
	@cp -p $(dist.top.extras) $(dist-dir.top)/.
	@cp -p jar-dist.make $(dist-dir.top)/Makefile
	@cp -p $(dir.src.c)/*.[ch] $(dist-dir.src)/.
	@cp -p $(sqlite3.canonical.c) $(sqlite3.canonical.h) $(dist-dir.src)/.
	@set -e; \
		vnum=$$($(bin.version-info) --download-version); \
		vjar=$$($(bin.version-info) --version); \
		vdir=$(dist-name-prefix)-$$vnum; \
		arczip=$$vdir.zip; \
		cp -p $(package.jar) $(dist-dir.top)/sqlite3-jni-$${vjar}.jar; \
		echo "Making $$arczip ..."; \
		rm -fr $$arczip $$vdir; \
		mv $(dist-dir.top) $$vdir; \
		zip -qr $$arczip $$vdir; \
		rm -fr $$vdir; \
		ls -la $$arczip; \
		set +e; \
		unzip -lv $$arczip || echo "Missing unzip app? Not fatal."

snapshot: dist

.PHONY: dist-clean
clean: dist-clean
dist-clean:
	rm -fr $(dist-name) $(wildcard sqlite-jni-*.zip)
