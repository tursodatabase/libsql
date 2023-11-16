#!/this/is/make
#^^^^ help emacs out
#
# This is a POSIX-make-compatible makefile for building the sqlite3
# JNI library from "dist" zip file. It must be edited to set the
# proper top-level JDK directory and, depending on the platform, add a
# platform-specific -I directory. It should build as-is with any
# 2020s-era version of gcc or clang. It requires JDK version 8 or
# higher and that JAVA_HOME points to the top-most installation
# directory of that JDK. On Ubuntu-style systems the JDK is typically
# installed under /usr/lib/jvm/java-VERSION-PLATFORM.

default: all

JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-amd64
CFLAGS = \
  -fPIC \
  -Isrc \
  -I$(JAVA_HOME)/include \
  -I$(JAVA_HOME)/include/linux \
  -I$(JAVA_HOME)/include/apple \
  -I$(JAVA_HOME)/include/bsd \
  -Wall

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
  -DSQLITE_THREADSAFE=1 \
  -DSQLITE_TEMP_STORE=2 \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_DEBUG

sqlite3-jni.dll = libsqlite3-jni.so
$(sqlite3-jni.dll):
	@echo "************************************************************************"; \
	echo  "*** If this fails to build, be sure to edit this makefile            ***"; \
	echo  "*** to configure it for your system.                                 ***"; \
	echo  "************************************************************************"
	$(CC) $(CFLAGS) $(SQLITE_OPT) \
		src/sqlite3-jni.c -shared -o $@
	@echo "Now try running it with: make test"

test.flags = -Djava.library.path=. sqlite3-jni-*.jar
test: $(sqlite3-jni.dll)
	java -jar $(test.flags)
	java -jar $(test.flags) -t 7 -r 10 -shuffle

clean:
	-rm -f $(sqlite3-jni.dll)

all: $(sqlite3-jni.dll)
