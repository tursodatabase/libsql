# Quick-and-dirty makefile to bootstrap the sqlite3-jni project.  This
# build assumes a Linux-like system.
default: all

JAVA_HOME ?= $(HOME)/jdk/current
# e.g. /usr/lib/jvm/default-javajava-19-openjdk-amd64
JDK_HOME ?= $(JAVA_HOME)
# ^^^ JDK_HOME is not as widely used as JAVA_HOME
bin.javac := $(JDK_HOME)/bin/javac
bin.java  := $(JDK_HOME)/bin/java
bin.jar   := $(JDK_HOME)/bin/jar
ifeq (,$(wildcard $(JDK_HOME)))
$(error set JDK_HOME to the top-most dir of your JDK installation.)
endif
MAKEFILE := $(lastword $(MAKEFILE_LIST))
$(MAKEFILE):

package.jar := sqlite3-jni.jar

dir.top := ../..
dir.tool := ../../tool
dir.jni := $(patsubst %/,%,$(dir $(MAKEFILE)))

dir.src     := $(dir.jni)/src
dir.src.c   := $(dir.src)/c
dir.bld     := $(dir.jni)/bld
dir.bld.c   := $(dir.bld)
dir.src.jni := $(dir.src)/org/sqlite/jni
dir.src.jni.tester := $(dir.src.jni)/tester
$(dir.bld.c):
	mkdir -p $@

classpath := $(dir.src)
CLEAN_FILES := $(package.jar)
DISTCLEAN_FILES := $(dir.jni)/*~ $(dir.src.c)/*~ $(dir.src.jni)/*~

sqlite3-jni.h := $(dir.src.c)/sqlite3-jni.h
.NOTPARALLEL: $(sqlite3-jni.h)
SQLite3Jni.java := src/org/sqlite/jni/SQLite3Jni.java
SQLTester.java := src/org/sqlite/jni/tester/SQLTester.java
SQLite3Jni.class := $(SQLite3Jni.java:.java=.class)
SQLTester.class := $(SQLTester.java:.java=.class)

########################################################################
# The future of FTS5 customization in this API is as yet unclear.
# It would be a real doozy to bind to JNI.
enable.fts5 ?= 1
# If enable.tester is 0, the org/sqlite/jni/tester/* bits are elided.
enable.tester ?= 1

# bin.version-info = binary to output various sqlite3 version info
# building the distribution zip file.
bin.version-info := $(dir.top)/version-info
.NOTPARALLEL: $(bin.version-info)
$(bin.version-info): $(dir.tool)/version-info.c $(sqlite3.h) $(dir.top)/Makefile
	$(MAKE) -C $(dir.top) version-info

# Be explicit about which Java files to compile so that we can work on
# in-progress files without requiring them to be in a compilable statae.
JAVA_FILES.main := $(patsubst %,$(dir.src.jni)/%,\
  BusyHandler.java \
  Collation.java \
  CollationNeeded.java \
  CommitHook.java \
  NativePointerHolder.java \
  OutputPointer.java \
  ProgressHandler.java \
  ResultCode.java \
  RollbackHook.java \
  SQLFunction.java \
  sqlite3_context.java \
  sqlite3.java \
  SQLite3Jni.java \
  sqlite3_stmt.java \
  sqlite3_value.java \
  Tester1.java \
  Tracer.java \
  UpdateHook.java \
  ValueHolder.java \
)
ifeq (1,$(enable.fts5))
  JAVA_FILES.main += $(patsubst %,$(dir.src.jni)/%,\
    fts5_api.java \
    fts5_extension_function.java \
    fts5_tokenizer.java \
    Fts5.java \
    Fts5Context.java \
    Fts5ExtensionApi.java \
    Fts5Function.java \
    Fts5PhraseIter.java \
    Fts5Tokenizer.java \
    TesterFts5.java \
  )
endif
JAVA_FILES.tester := $(dir.src.jni.tester)/SQLTester.java

CLASS_FILES.main := $(JAVA_FILES.main:.java=.class)
CLASS_FILES.tester := $(JAVA_FILES.tester:.java=.class)

JAVA_FILES += $(JAVA_FILES.main)
ifeq (1,$(enable.tester))
  JAVA_FILES += $(JAVA_FILES.tester)
endif

CLASS_FILES :=
define DOTCLASS_DEPS
$(1).class: $(1).java $(MAKEFILE)
all: $(1).class
CLASS_FILES += $(1).class
endef
$(foreach B,$(basename $(JAVA_FILES)),$(eval $(call DOTCLASS_DEPS,$(B))))
$(CLASS_FILES.tester): $(CLASS_FILES.main)
javac.flags ?= -Xlint:unchecked -Xlint:deprecation
java.flags ?=
jnicheck ?= 1
ifeq (1,$(jnicheck))
  java.flags += -Xcheck:jni
endif
$(SQLite3Jni.class): $(JAVA_FILES)
	$(bin.javac) $(javac.flags) -h $(dir.bld.c) -cp $(classpath) $(JAVA_FILES)
all: $(SQLite3Jni.class)
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

SQLITE_OPT = \
  -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_EXPLAIN_COMMENTS \
  -DSQLITE_ENABLE_STMTVTAB \
  -DSQLITE_ENABLE_DBPAGE_VTAB \
  -DSQLITE_ENABLE_DBSTAT_VTAB \
  -DSQLITE_ENABLE_BYTECODE_VTAB \
  -DSQLITE_ENABLE_OFFSET_SQL_FUNC \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_TEMP_STORE=2 \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_C=$(sqlite3.c)
#  -DSQLITE_DEBUG
# -DSQLITE_DEBUG is just to work around a -Wall warning
# for a var which gets set in all builds but only read
# via assert().

SQLITE_OPT += -g -DDEBUG -UNDEBUG

ifeq (1,$(enable.fts5))
  SQLITE_OPT += -DSQLITE_ENABLE_FTS5
endif

sqlite3-jni.c := $(dir.src.c)/sqlite3-jni.c
sqlite3-jni.o := $(dir.bld.c)/sqlite3-jni.o
sqlite3-jni.h   := $(dir.src.c)/sqlite3-jni.h
sqlite3-jni.dll := $(dir.bld.c)/libsqlite3-jni.so
# All javac-generated .h files must be listed in $(sqlite3-jni.h.in):
sqlite3-jni.h.in :=
define ADD_JNI_H
sqlite3-jni.h.in += $$(dir.bld.c)/org_sqlite_jni_$(1).h
$$(dir.bld.c)/org_sqlite_jni_$(1).h: $$(dir.src.jni)/$(1).java
endef
$(eval $(call ADD_JNI_H,SQLite3Jni))
ifeq (1,$(enable.fts5))
  $(eval $(call ADD_JNI_H,Fts5ExtensionApi))
  $(eval $(call ADD_JNI_H,fts5_api))
  $(eval $(call ADD_JNI_H,fts5_tokenizer))
endif
ifeq (1,$(enable.tester))
  sqlite3-jni.h.in += $(dir.bld.c)/org_sqlite_jni_tester_SQLTester.h
  $(dir.bld.c)/org_sqlite_jni_tester_SQLTester.h: $(dir.src.jni.tester)/SQLTester.java
endif
#sqlite3-jni.dll.cfiles := $(dir.src.c)
sqlite3-jni.dll.cflags = \
  -fPIC \
  -I. \
  -I$(dir $(sqlite3.h)) \
  -I$(dir.src.c) \
  -I$(JDK_HOME)/include \
  $(patsubst %,-I%,$(patsubst %.h,,$(wildcard $(JDK_HOME)/include/*))) \
  -Wall
# Using (-Wall -Wextra) triggers an untennable number of
# gcc warnings from sqlite3.c for mundane things like
# unused parameters.
#
# The gross $(patsubst...) above is to include the platform-specific
# subdir which lives under $(JDK_HOME)/include and is a required
# include path for client-level code.
########################################################################
ifeq (1,$(enable.tester))
  sqlite3-jni.dll.cflags += -DS3JNI_ENABLE_SQLTester
endif
$(sqlite3-jni.h): $(sqlite3-jni.h.in) $(MAKEFILE)
	cat $(sqlite3-jni.h.in) > $@
$(sqlite3-jni.dll): $(sqlite3-jni.h) $(sqlite3.c) $(sqlite3.h)
$(sqlite3-jni.dll): $(dir.bld.c) $(sqlite3-jni.c) $(SQLite3Jni.java) $(MAKEFILE)
	$(CC) $(sqlite3-jni.dll.cflags) $(SQLITE_OPT) \
			$(sqlite3-jni.c) -shared -o $@
all: $(sqlite3-jni.dll)

.PHONY: test
test.flags ?= -v
test: $(SQLite3Jni.class) $(sqlite3-jni.dll)
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.Tester1 $(if $(test.flags),-- $(test.flags),)

tester.scripts := $(sort $(wildcard $(dir.src)/tests/*.test))
tester.flags ?= # --verbose
.PHONY: tester tester-local tester-ext
ifeq (1,$(enable.tester))
tester-local: $(CLASS_FILES.tester) $(sqlite3-jni.dll)
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.tester.SQLTester $(tester.flags) $(tester.scripts)
tester: tester-local
else
tester:
	@echo "SQLTester support is disabled. Build with enable.tester=1 to enable it."
endif

tester.extdir.default := src/tests/ext
tester.extdir ?= $(tester.extdir.default)
tester.extern-scripts := $(wildcard $(tester.extdir)/*.test)
ifneq (,$(tester.extern-scripts))
tester-ext:
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.tester.SQLTester $(tester.flags) $(tester.extern-scripts)
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
tests: test tester
package.jar.in := $(abspath $(dir.src)/jar.in)
CLEAN_FILES += $(package.jar.in)
$(package.jar.in): $(MAKEFILE) $(CLASS_FILES.main)
	cd $(dir.src); ls -1 org/sqlite/jni/*.java org/sqlite/jni/*.class > $@
	@ls -la $@
	@echo "To use this jar you will need the -Djava.library.path=DIR/WITH/libsqlite3-jni.so flag."
	@echo "e.g. java -jar $@ -Djava.library.path=bld"

$(package.jar): $(CLASS_FILES) $(MAKEFILE) $(package.jar.in)
	rm -f $(dir.src)/c/*~ $(dir.src.jni)/*~
	cd $(dir.src); $(bin.jar) -cfe ../$@ org.sqlite.jni.Tester1 @$(package.jar.in)

jar: $(package.jar)

CLEAN_FILES += $(dir.bld.c)/* \
  $(dir.src.jni)/*.class \
  $(dir.src.jni.tester)/*.class \
  $(sqlite3-jni.dll) \
  hs_err_pid*.log

.PHONY: clean distclean
clean:
	-rm -f $(CLEAN_FILES)
distclean: clean
	-rm -f $(DISTCLEAN_FILES)
	-rm -fr $(dir.bld.c)

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
