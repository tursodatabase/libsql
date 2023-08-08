# Quick-and-dirty makefile to bootstrap the sqlite3-jni project.  This
# build assumes a Linux-like system.
default: all

JDK_HOME ?= $(HOME)/jdk/current
# /usr/lib/jvm/default-javajava-19-openjdk-amd64
bin.javac := $(JDK_HOME)/bin/javac
bin.java  := $(JDK_HOME)/bin/java
bin.jar   := $(JDK_HOME)/bin/jar
ifeq (,$(wildcard $(JDK_HOME)))
$(error set JDK_HOME to the top-most dir of your JDK installation.)
endif
MAKEFILE := $(lastword $(MAKEFILE_LIST))
$(MAKEFILE):

package.version := 0.0.1
package.jar := sqlite3-jni-$(package.version).jar

dir.top := ../..
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
JAVA_FILES.tester := $(patsubst %,$(dir.src.jni.tester)/%,\
  Outer.java \
  SQLTester.java \
  TestScript.java \
)

CLASS_FILES.main := $(JAVA_FILES.main:.java=.class)
CLASS_FILES.tester := $(JAVA_FILES.tester:.java=.class)

JAVA_FILES += $(JAVA_FILES.main) $(JAVA_FILES.tester)

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
sqlite3.canonical.c := $(dir.top)/sqlite3.c
sqlite3.c ?= $(firstword $(wildcard $(dir.top)/sqlite3-see.c) $(sqlite3.canonical.c))
sqlite3.h := $(dir.top)/sqlite3.h
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

SQLITE_OPT := \
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
  -DSQLITE_C=$(sqlite3.c) \
  -DSQLITE_DEBUG
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
sqlite3-jni.h.in += $(dir.bld.c)/org_sqlite_jni_tester_SQLTester.h
$(dir.bld.c)/org_sqlite_jni_tester_SQLTester.h: $(dir.src.jni.tester)/SQLTester.java
#sqlite3-jni.dll.cfiles := $(dir.src.c)
sqlite3-jni.dll.cflags := \
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
.PHONY: tester

tester.flags ?= --verbose

tester: $(CLASS_FILES.tester) $(sqlite3-jni.dll)
	$(bin.java) -ea -Djava.library.path=$(dir.bld.c) \
		$(java.flags) -cp $(classpath) \
		org.sqlite.jni.tester.SQLTester $(tester.flags) $(tester.scripts)

$(package.jar): $(CLASS_FILES) $(MAKEFILE)
	rm -f $(dir.src)/c/*~ $(dir.src.jni)/*~
	$(bin.jar) -cfe $@ org.sqlite.Tester1 -C src org -C src c

jar: $(package.jar)

CLEAN_FILES += $(dir.bld.c)/* \
  $(dir.src.jni)/*.class \
  $(sqlite3-jni.dll) \
  hs_err_pid*.log

.PHONY: clean distclean
clean:
	-rm -f $(CLEAN_FILES)
distclean: clean
	-rm -f $(DISTCLEAN_FILES)
	-rm -fr $(dir.bld.c)
