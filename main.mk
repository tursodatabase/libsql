###############################################################################
# The following macros should be defined before this script is
# invoked:
#
# TOP              The toplevel directory of the source tree.  This is the
#                  directory that contains this "Makefile.in" and the
#                  "configure.in" script.
#
# BCC              C Compiler and options for use in building executables that
#                  will run on the platform that is doing the build.
#
# THREADLIB        Specify any extra linker options needed to make the library
#                  thread safe
#
# OPTS             Extra compiler command-line options.
#
# EXE              The suffix to add to executable files.  ".exe" for windows
#                  and "" for Unix.
#
# TCC              C Compiler and options for use in building executables that 
#                  will run on the target platform.  This is usually the same
#                  as BCC, unless you are cross-compiling.
#
# AR               Tools used to build a static library.
# RANLIB
#
# TCL_FLAGS        Extra compiler options needed for programs that use the
#                  TCL library.
#
# LIBTCL           Linker options needed to link against the TCL library.
#
# READLINE_FLAGS   Compiler options needed for programs that use the
#                  readline() library.
#
# LIBREADLINE      Linker options needed by programs using readline() must
#                  link against.
#
# NAWK             Nawk compatible awk program.  Older (obsolete?) solaris
#                  systems need this to avoid using the original AT&T AWK.
#
# Once the macros above are defined, the rest of this make script will
# build the SQLite library and testing tools.
################################################################################

# This is how we compile
#
TCCX =  $(TCC) $(OPTS) -I. -I$(TOP)/src -I$(TOP) 
TCCX += -I$(TOP)/ext/rtree -I$(TOP)/ext/icu -I$(TOP)/ext/fts3
TCCX += -I$(TOP)/ext/async

# Object files for the SQLite library.
#
LIBOBJ+= alter.o analyze.o attach.o auth.o \
         backup.o bitvec.o btmutex.o btree.o build.o \
         callback.o complete.o ctime.o date.o delete.o expr.o fault.o fkey.o \
         fts3.o fts3_expr.o fts3_hash.o fts3_icu.o fts3_porter.o \
         fts3_snippet.o fts3_tokenizer.o fts3_tokenizer1.o fts3_write.o \
         func.o global.o hash.o \
         icu.o insert.o journal.o legacy.o loadext.o \
         main.o malloc.o mem0.o mem1.o mem2.o mem3.o mem5.o \
         memjournal.o \
         mutex.o mutex_noop.o mutex_os2.o mutex_unix.o mutex_w32.o \
         notify.o opcodes.o os.o os_os2.o os_unix.o os_win.o \
         pager.o parse.o pcache.o pcache1.o pragma.o prepare.o printf.o \
         random.o resolve.o rowset.o rtree.o select.o status.o \
         table.o tokenize.o trigger.o \
         update.o util.o vacuum.o \
         vdbe.o vdbeapi.o vdbeaux.o vdbeblob.o vdbemem.o vdbetrace.o \
         wal.o walker.o where.o utf.o vtab.o



# All of the source code files.
#
SRC = \
  $(TOP)/src/alter.c \
  $(TOP)/src/analyze.c \
  $(TOP)/src/attach.c \
  $(TOP)/src/auth.c \
  $(TOP)/src/backup.c \
  $(TOP)/src/bitvec.c \
  $(TOP)/src/btmutex.c \
  $(TOP)/src/btree.c \
  $(TOP)/src/btree.h \
  $(TOP)/src/btreeInt.h \
  $(TOP)/src/build.c \
  $(TOP)/src/callback.c \
  $(TOP)/src/complete.c \
  $(TOP)/src/ctime.c \
  $(TOP)/src/date.c \
  $(TOP)/src/delete.c \
  $(TOP)/src/expr.c \
  $(TOP)/src/fault.c \
  $(TOP)/src/fkey.c \
  $(TOP)/src/func.c \
  $(TOP)/src/global.c \
  $(TOP)/src/hash.c \
  $(TOP)/src/hash.h \
  $(TOP)/src/hwtime.h \
  $(TOP)/src/insert.c \
  $(TOP)/src/journal.c \
  $(TOP)/src/legacy.c \
  $(TOP)/src/loadext.c \
  $(TOP)/src/main.c \
  $(TOP)/src/malloc.c \
  $(TOP)/src/mem0.c \
  $(TOP)/src/mem1.c \
  $(TOP)/src/mem2.c \
  $(TOP)/src/mem3.c \
  $(TOP)/src/mem5.c \
  $(TOP)/src/memjournal.c \
  $(TOP)/src/mutex.c \
  $(TOP)/src/mutex.h \
  $(TOP)/src/mutex_noop.c \
  $(TOP)/src/mutex_os2.c \
  $(TOP)/src/mutex_unix.c \
  $(TOP)/src/mutex_w32.c \
  $(TOP)/src/notify.c \
  $(TOP)/src/os.c \
  $(TOP)/src/os.h \
  $(TOP)/src/os_common.h \
  $(TOP)/src/os_os2.c \
  $(TOP)/src/os_unix.c \
  $(TOP)/src/os_win.c \
  $(TOP)/src/pager.c \
  $(TOP)/src/pager.h \
  $(TOP)/src/parse.y \
  $(TOP)/src/pcache.c \
  $(TOP)/src/pcache.h \
  $(TOP)/src/pcache1.c \
  $(TOP)/src/pragma.c \
  $(TOP)/src/prepare.c \
  $(TOP)/src/printf.c \
  $(TOP)/src/random.c \
  $(TOP)/src/resolve.c \
  $(TOP)/src/rowset.c \
  $(TOP)/src/select.c \
  $(TOP)/src/status.c \
  $(TOP)/src/shell.c \
  $(TOP)/src/sqlite.h.in \
  $(TOP)/src/sqlite3ext.h \
  $(TOP)/src/sqliteInt.h \
  $(TOP)/src/sqliteLimit.h \
  $(TOP)/src/table.c \
  $(TOP)/src/tclsqlite.c \
  $(TOP)/src/tokenize.c \
  $(TOP)/src/trigger.c \
  $(TOP)/src/utf.c \
  $(TOP)/src/update.c \
  $(TOP)/src/util.c \
  $(TOP)/src/vacuum.c \
  $(TOP)/src/vdbe.c \
  $(TOP)/src/vdbe.h \
  $(TOP)/src/vdbeapi.c \
  $(TOP)/src/vdbeaux.c \
  $(TOP)/src/vdbeblob.c \
  $(TOP)/src/vdbemem.c \
  $(TOP)/src/vdbetrace.c \
  $(TOP)/src/vdbeInt.h \
  $(TOP)/src/vtab.c \
  $(TOP)/src/wal.c \
  $(TOP)/src/wal.h \
  $(TOP)/src/walker.c \
  $(TOP)/src/where.c

# Source code for extensions
#
SRC += \
  $(TOP)/ext/fts1/fts1.c \
  $(TOP)/ext/fts1/fts1.h \
  $(TOP)/ext/fts1/fts1_hash.c \
  $(TOP)/ext/fts1/fts1_hash.h \
  $(TOP)/ext/fts1/fts1_porter.c \
  $(TOP)/ext/fts1/fts1_tokenizer.h \
  $(TOP)/ext/fts1/fts1_tokenizer1.c
SRC += \
  $(TOP)/ext/fts2/fts2.c \
  $(TOP)/ext/fts2/fts2.h \
  $(TOP)/ext/fts2/fts2_hash.c \
  $(TOP)/ext/fts2/fts2_hash.h \
  $(TOP)/ext/fts2/fts2_icu.c \
  $(TOP)/ext/fts2/fts2_porter.c \
  $(TOP)/ext/fts2/fts2_tokenizer.h \
  $(TOP)/ext/fts2/fts2_tokenizer.c \
  $(TOP)/ext/fts2/fts2_tokenizer1.c
SRC += \
  $(TOP)/ext/fts3/fts3.c \
  $(TOP)/ext/fts3/fts3.h \
  $(TOP)/ext/fts3/fts3Int.h \
  $(TOP)/ext/fts3/fts3_expr.c \
  $(TOP)/ext/fts3/fts3_hash.c \
  $(TOP)/ext/fts3/fts3_hash.h \
  $(TOP)/ext/fts3/fts3_icu.c \
  $(TOP)/ext/fts3/fts3_porter.c \
  $(TOP)/ext/fts3/fts3_snippet.c \
  $(TOP)/ext/fts3/fts3_tokenizer.h \
  $(TOP)/ext/fts3/fts3_tokenizer.c \
  $(TOP)/ext/fts3/fts3_tokenizer1.c \
  $(TOP)/ext/fts3/fts3_write.c
SRC += \
  $(TOP)/ext/icu/sqliteicu.h \
  $(TOP)/ext/icu/icu.c
SRC += \
  $(TOP)/ext/rtree/rtree.h \
  $(TOP)/ext/rtree/rtree.c


# Generated source code files
#
SRC += \
  keywordhash.h \
  opcodes.c \
  opcodes.h \
  parse.c \
  parse.h \
  sqlite3.h


# Source code to the test files.
#
TESTSRC = \
  $(TOP)/src/test1.c \
  $(TOP)/src/test2.c \
  $(TOP)/src/test3.c \
  $(TOP)/src/test4.c \
  $(TOP)/src/test5.c \
  $(TOP)/src/test6.c \
  $(TOP)/src/test7.c \
  $(TOP)/src/test8.c \
  $(TOP)/src/test9.c \
  $(TOP)/src/test_autoext.c \
  $(TOP)/src/test_async.c \
  $(TOP)/src/test_backup.c \
  $(TOP)/src/test_btree.c \
  $(TOP)/src/test_config.c \
  $(TOP)/src/test_demovfs.c \
  $(TOP)/src/test_devsym.c \
  $(TOP)/src/test_func.c \
  $(TOP)/src/test_hexio.c \
  $(TOP)/src/test_init.c \
  $(TOP)/src/test_intarray.c \
  $(TOP)/src/test_journal.c \
  $(TOP)/src/test_malloc.c \
  $(TOP)/src/test_multiplex.c \
  $(TOP)/src/test_mutex.c \
  $(TOP)/src/test_onefile.c \
  $(TOP)/src/test_osinst.c \
  $(TOP)/src/test_pcache.c \
  $(TOP)/src/test_quota.c \
  $(TOP)/src/test_rtree.c \
  $(TOP)/src/test_schema.c \
  $(TOP)/src/test_server.c \
  $(TOP)/src/test_stat.c \
  $(TOP)/src/test_superlock.c \
  $(TOP)/src/test_tclvar.c \
  $(TOP)/src/test_thread.c \
  $(TOP)/src/test_vfs.c \
  $(TOP)/src/test_wsd.c

#TESTSRC += $(TOP)/ext/fts2/fts2_tokenizer.c
#TESTSRC += $(TOP)/ext/fts3/fts3_tokenizer.c

TESTSRC2 = \
  $(TOP)/src/attach.c \
  $(TOP)/src/backup.c \
  $(TOP)/src/btree.c \
  $(TOP)/src/build.c \
  $(TOP)/src/date.c \
  $(TOP)/src/expr.c \
  $(TOP)/src/func.c \
  $(TOP)/src/insert.c \
  $(TOP)/src/wal.c \
  $(TOP)/src/mem5.c \
  $(TOP)/src/os.c \
  $(TOP)/src/os_os2.c \
  $(TOP)/src/os_unix.c \
  $(TOP)/src/os_win.c \
  $(TOP)/src/pager.c \
  $(TOP)/src/pragma.c \
  $(TOP)/src/prepare.c \
  $(TOP)/src/printf.c \
  $(TOP)/src/random.c \
  $(TOP)/src/pcache.c \
  $(TOP)/src/pcache1.c \
  $(TOP)/src/select.c \
  $(TOP)/src/tokenize.c \
  $(TOP)/src/utf.c \
  $(TOP)/src/util.c \
  $(TOP)/src/vdbeapi.c \
  $(TOP)/src/vdbeaux.c \
  $(TOP)/src/vdbe.c \
  $(TOP)/src/vdbemem.c \
  $(TOP)/src/where.c \
  parse.c \
  $(TOP)/ext/fts3/fts3.c \
  $(TOP)/ext/fts3/fts3_expr.c \
  $(TOP)/ext/fts3/fts3_tokenizer.c \
  $(TOP)/ext/fts3/fts3_write.c \
  $(TOP)/ext/async/sqlite3async.c

# Header files used by all library source files.
#
HDR = \
   $(TOP)/src/btree.h \
   $(TOP)/src/btreeInt.h \
   $(TOP)/src/hash.h \
   $(TOP)/src/hwtime.h \
   keywordhash.h \
   $(TOP)/src/mutex.h \
   opcodes.h \
   $(TOP)/src/os.h \
   $(TOP)/src/os_common.h \
   $(TOP)/src/pager.h \
   $(TOP)/src/pcache.h \
   parse.h  \
   sqlite3.h  \
   $(TOP)/src/sqlite3ext.h \
   $(TOP)/src/sqliteInt.h  \
   $(TOP)/src/sqliteLimit.h \
   $(TOP)/src/vdbe.h \
   $(TOP)/src/vdbeInt.h

# Header files used by extensions
#
EXTHDR += \
  $(TOP)/ext/fts1/fts1.h \
  $(TOP)/ext/fts1/fts1_hash.h \
  $(TOP)/ext/fts1/fts1_tokenizer.h
EXTHDR += \
  $(TOP)/ext/fts2/fts2.h \
  $(TOP)/ext/fts2/fts2_hash.h \
  $(TOP)/ext/fts2/fts2_tokenizer.h
EXTHDR += \
  $(TOP)/ext/fts3/fts3.h \
  $(TOP)/ext/fts3/fts3Int.h \
  $(TOP)/ext/fts3/fts3_hash.h \
  $(TOP)/ext/fts3/fts3_tokenizer.h
EXTHDR += \
  $(TOP)/ext/rtree/rtree.h
EXTHDR += \
  $(TOP)/ext/icu/sqliteicu.h

# This is the default Makefile target.  The objects listed here
# are what get build when you type just "make" with no arguments.
#
all:	sqlite3.h libsqlite3.a sqlite3$(EXE)

libsqlite3.a:	$(LIBOBJ)
	$(AR) libsqlite3.a $(LIBOBJ)
	$(RANLIB) libsqlite3.a

sqlite3$(EXE):	$(TOP)/src/shell.c libsqlite3.a sqlite3.h
	$(TCCX) $(READLINE_FLAGS) -o sqlite3$(EXE)                  \
		$(TOP)/src/shell.c                                  \
		libsqlite3.a $(LIBREADLINE) $(TLIBS) $(THREADLIB)

# This target creates a directory named "tsrc" and fills it with
# copies of all of the C source code and header files needed to
# build on the target system.  Some of the C source code and header
# files are automatically generated.  This target takes care of
# all that automatic generation.
#
target_source:	$(SRC) $(TOP)/tool/vdbe-compress.tcl
	rm -rf tsrc
	mkdir tsrc
	cp -f $(SRC) tsrc
	rm tsrc/sqlite.h.in tsrc/parse.y
	tclsh $(TOP)/tool/vdbe-compress.tcl <tsrc/vdbe.c >vdbe.new
	mv vdbe.new tsrc/vdbe.c
	touch target_source

sqlite3.c:	target_source $(TOP)/tool/mksqlite3c.tcl
	tclsh $(TOP)/tool/mksqlite3c.tcl
	echo '#ifndef USE_SYSTEM_SQLITE' >tclsqlite3.c
	cat sqlite3.c >>tclsqlite3.c
	echo '#endif /* USE_SYSTEM_SQLITE */' >>tclsqlite3.c
	cat $(TOP)/src/tclsqlite.c >>tclsqlite3.c

fts2amal.c:	target_source $(TOP)/ext/fts2/mkfts2amal.tcl
	tclsh $(TOP)/ext/fts2/mkfts2amal.tcl

fts3amal.c:	target_source $(TOP)/ext/fts3/mkfts3amal.tcl
	tclsh $(TOP)/ext/fts3/mkfts3amal.tcl

# Rules to build the LEMON compiler generator
#
lemon:	$(TOP)/tool/lemon.c $(TOP)/src/lempar.c
	$(BCC) -o lemon $(TOP)/tool/lemon.c
	cp $(TOP)/src/lempar.c .

# Rules to build individual *.o files from generated *.c files. This
# applies to:
#
#     parse.o
#     opcodes.o
#
%.o: %.c $(HDR)
	$(TCCX) -c $<

# Rules to build individual *.o files from files in the src directory.
#
%.o: $(TOP)/src/%.c $(HDR)
	$(TCCX) -c $<

tclsqlite.o:	$(TOP)/src/tclsqlite.c $(HDR)
	$(TCCX) $(TCL_FLAGS) -c $(TOP)/src/tclsqlite.c



# Rules to build opcodes.c and opcodes.h
#
opcodes.c:	opcodes.h $(TOP)/mkopcodec.awk
	sort -n -b -k 3 opcodes.h | $(NAWK) -f $(TOP)/mkopcodec.awk >opcodes.c

opcodes.h:	parse.h $(TOP)/src/vdbe.c $(TOP)/mkopcodeh.awk
	cat parse.h $(TOP)/src/vdbe.c | \
		$(NAWK) -f $(TOP)/mkopcodeh.awk >opcodes.h

# Rules to build parse.c and parse.h - the outputs of lemon.
#
parse.h:	parse.c

parse.c:	$(TOP)/src/parse.y lemon $(TOP)/addopcodes.awk
	cp $(TOP)/src/parse.y .
	rm -f parse.h
	./lemon $(OPTS) parse.y
	mv parse.h parse.h.temp
	$(NAWK) -f $(TOP)/addopcodes.awk parse.h.temp >parse.h

sqlite3.h:	$(TOP)/src/sqlite.h.in $(TOP)/manifest.uuid $(TOP)/VERSION
	tclsh $(TOP)/tool/mksqlite3h.tcl $(TOP) >sqlite3.h

keywordhash.h:	$(TOP)/tool/mkkeywordhash.c
	$(BCC) -o mkkeywordhash $(OPTS) $(TOP)/tool/mkkeywordhash.c
	./mkkeywordhash >keywordhash.h



# Rules to build the extension objects.
#
icu.o:	$(TOP)/ext/icu/icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/icu/icu.c

fts2.o:	$(TOP)/ext/fts2/fts2.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2.c

fts2_hash.o:	$(TOP)/ext/fts2/fts2_hash.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2_hash.c

fts2_icu.o:	$(TOP)/ext/fts2/fts2_icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2_icu.c

fts2_porter.o:	$(TOP)/ext/fts2/fts2_porter.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2_porter.c

fts2_tokenizer.o:	$(TOP)/ext/fts2/fts2_tokenizer.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2_tokenizer.c

fts2_tokenizer1.o:	$(TOP)/ext/fts2/fts2_tokenizer1.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts2/fts2_tokenizer1.c

fts3.o:	$(TOP)/ext/fts3/fts3.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3.c

fts3_expr.o:	$(TOP)/ext/fts3/fts3_expr.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_expr.c

fts3_hash.o:	$(TOP)/ext/fts3/fts3_hash.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_hash.c

fts3_icu.o:	$(TOP)/ext/fts3/fts3_icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_icu.c

fts3_snippet.o:	$(TOP)/ext/fts3/fts3_snippet.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_snippet.c

fts3_porter.o:	$(TOP)/ext/fts3/fts3_porter.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_porter.c

fts3_tokenizer.o:	$(TOP)/ext/fts3/fts3_tokenizer.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_tokenizer.c

fts3_tokenizer1.o:	$(TOP)/ext/fts3/fts3_tokenizer1.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_tokenizer1.c

fts3_write.o:	$(TOP)/ext/fts3/fts3_write.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/fts3/fts3_write.c

rtree.o:	$(TOP)/ext/rtree/rtree.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE_CORE -c $(TOP)/ext/rtree/rtree.c


# Rules for building test programs and for running tests
#
tclsqlite3:	$(TOP)/src/tclsqlite.c libsqlite3.a
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 -o tclsqlite3 \
		$(TOP)/src/tclsqlite.c libsqlite3.a $(LIBTCL) $(THREADLIB)


# Rules to build the 'testfixture' application.
#
TESTFIXTURE_FLAGS  = -DSQLITE_TEST=1 -DSQLITE_CRASH_TEST=1
TESTFIXTURE_FLAGS += -DSQLITE_SERVER=1 -DSQLITE_PRIVATE="" -DSQLITE_CORE 

testfixture$(EXE): $(TESTSRC2) libsqlite3.a $(TESTSRC) $(TOP)/src/tclsqlite.c
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
		$(TESTSRC) $(TESTSRC2) $(TOP)/src/tclsqlite.c                \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB) libsqlite3.a

amalgamation-testfixture$(EXE): sqlite3.c $(TESTSRC) $(TOP)/src/tclsqlite.c
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
		$(TESTSRC) $(TOP)/src/tclsqlite.c sqlite3.c                  \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB)

fts3-testfixture$(EXE): sqlite3.c fts3amal.c $(TESTSRC) $(TOP)/src/tclsqlite.c
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
	-DSQLITE_ENABLE_FTS3=1                                               \
		$(TESTSRC) $(TOP)/src/tclsqlite.c sqlite3.c fts3amal.c       \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB)

fulltest:	testfixture$(EXE) sqlite3$(EXE)
	./testfixture$(EXE) $(TOP)/test/all.test

soaktest:	testfixture$(EXE) sqlite3$(EXE)
	./testfixture$(EXE) $(TOP)/test/all.test -soak=1

test:	testfixture$(EXE) sqlite3$(EXE)
	./testfixture$(EXE) $(TOP)/test/veryquick.test

# The next two rules are used to support the "threadtest" target. Building
# threadtest runs a few thread-safety tests that are implemented in C. This
# target is invoked by the releasetest.tcl script.
# 
threadtest3$(EXE): sqlite3.c $(TOP)/test/threadtest3.c
	$(TCCX) -O2 sqlite3.c $(TOP)/test/threadtest3.c \
		-o threadtest3$(EXE) $(THREADLIB)

threadtest: threadtest3$(EXE)
	./threadtest3$(EXE)

sqlite3_analyzer$(EXE):	$(TOP)/src/tclsqlite.c sqlite3.c $(TESTSRC) \
			$(TOP)/tool/spaceanal.tcl
	sed \
	  -e '/^#/d' \
	  -e 's,\\,\\\\,g' \
	  -e 's,",\\",g' \
	  -e 's,^,",' \
	  -e 's,$$,\\n",' \
	  $(TOP)/tool/spaceanal.tcl >spaceanal_tcl.h
	$(TCCX) $(TCL_FLAGS) -DTCLSH=2 $(TESTFIXTURE_FLAGS)                    \
		-DSQLITE_TEST=1 -DSQLITE_PRIVATE=""                            \
		$(TESTSRC) $(TOP)/src/tclsqlite.c sqlite3.c                    \
		-o sqlite3_analyzer$(EXE)                                      \
		$(LIBTCL) $(THREADLIB)

TEST_EXTENSION = $(SHPREFIX)testloadext.$(SO)
$(TEST_EXTENSION): $(TOP)/src/test_loadext.c
	$(MKSHLIB) $(TOP)/src/test_loadext.c -o $(TEST_EXTENSION)

extensiontest: testfixture$(EXE) $(TEST_EXTENSION)
	./testfixture$(EXE) $(TOP)/test/loadext.test


# Standard install and cleanup targets
#
install:	sqlite3 libsqlite3.a sqlite3.h
	mv sqlite3 /usr/bin
	mv libsqlite3.a /usr/lib
	mv sqlite3.h /usr/include

clean:	
	rm -f *.o sqlite3 libsqlite3.a sqlite3.h opcodes.*
	rm -f lemon lempar.c parse.* sqlite*.tar.gz mkkeywordhash keywordhash.h
	rm -f $(PUBLISH)
	rm -f *.da *.bb *.bbg gmon.out
	rm -rf tsrc target_source
	rm -f testloadext.dll libtestloadext.so
	rm -f sqlite3.c fts?amal.c tclsqlite3.c
