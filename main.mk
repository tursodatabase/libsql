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
# USLEEP           If the target operating system supports the "usleep()" system
#                  call, then define the HAVE_USLEEP macro for all C modules.
#
# THREADSAFE       If you want the SQLite library to be safe for use within a 
#                  multi-threaded program, then define the following macro
#                  appropriately:
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
# ENCODING         "UTF8" or "ISO8859"
#
# Once the macros above are defined, the rest of this make script will
# build the SQLite library and testing tools.
################################################################################

# This is how we compile
#
TCCX = $(TCC) $(OPTS) $(THREADSAFE) $(USLEEP) -I. -I$(TOP)/src

# Object files for the SQLite library.
#
LIBOBJ = attach.o auth.o btree.o build.o copy.o date.o delete.o \
         expr.o func.o hash.o insert.o \
         main.o opcodes.o os_mac.o os_unix.o os_win.o \
         pager.o parse.o pragma.o printf.o random.o \
         select.o table.o tokenize.o trigger.o update.o util.o vacuum.o \
         vdbe.o vdbeapi.o vdbeaux.o vdbemem.o \
         where.o tclsqlite.o utf.o legacy.o

# All of the source code files.
#
SRC = \
  $(TOP)/src/attach.c \
  $(TOP)/src/auth.c \
  $(TOP)/src/btree.c \
  $(TOP)/src/btree.h \
  $(TOP)/src/build.c \
  $(TOP)/src/copy.c \
  $(TOP)/src/date.c \
  $(TOP)/src/delete.c \
  $(TOP)/src/encode.c \
  $(TOP)/src/expr.c \
  $(TOP)/src/func.c \
  $(TOP)/src/hash.c \
  $(TOP)/src/hash.h \
  $(TOP)/src/insert.c \
  $(TOP)/src/main.c \
  $(TOP)/src/os_mac.c \
  $(TOP)/src/os_unix.c \
  $(TOP)/src/os_win.c \
  $(TOP)/src/pager.c \
  $(TOP)/src/pager.h \
  $(TOP)/src/parse.y \
  $(TOP)/src/pragma.c \
  $(TOP)/src/printf.c \
  $(TOP)/src/random.c \
  $(TOP)/src/select.c \
  $(TOP)/src/shell.c \
  $(TOP)/src/sqlite.h.in \
  $(TOP)/src/sqliteInt.h \
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
  $(TOP)/src/vdbemem.c \
  $(TOP)/src/vdbeInt.h \
  $(TOP)/src/where.c

# Source code to the test files.
#
TESTSRC = \
  $(TOP)/src/btree.c \
  $(TOP)/src/func.c \
  $(TOP)/src/os_mac.c \
  $(TOP)/src/os_unix.c \
  $(TOP)/src/os_win.c \
  $(TOP)/src/pager.c \
  $(TOP)/src/test1.c \
  $(TOP)/src/test2.c \
  $(TOP)/src/test3.c \
  $(TOP)/src/test4.c \
  $(TOP)/src/test5.c \
  $(TOP)/src/vdbe.c \
  $(TOP)/src/md5.c

# Header files used by all library source files.
#
HDR = \
   sqlite.h  \
   $(TOP)/src/btree.h \
   config.h \
   $(TOP)/src/hash.h \
   opcodes.h \
   $(TOP)/src/os.h \
   $(TOP)/src/os_mac.h \
   $(TOP)/src/os_unix.h \
   $(TOP)/src/os_win.h \
   $(TOP)/src/sqliteInt.h  \
   $(TOP)/src/vdbe.h \
   parse.h

# Header files used by the VDBE submodule
#
VDBEHDR = \
   $(HDR) \
   $(TOP)/src/vdbeInt.h

# This is the default Makefile target.  The objects listed here
# are what get build when you type just "make" with no arguments.
#
all:	sqlite.h config.h libsqlite.a sqlite$(EXE)

# Generate the file "last_change" which contains the date of change
# of the most recently modified source code file
#
last_change:	$(SRC)
	cat $(SRC) | grep '$$Id: ' | sort +4 | tail -1 \
          | awk '{print $$5,$$6}' >last_change

libsqlite.a:	$(LIBOBJ)
	$(AR) libsqlite.a $(LIBOBJ)
	$(RANLIB) libsqlite.a

sqlite$(EXE):	$(TOP)/src/shell.c libsqlite.a sqlite.h
	$(TCCX) $(READLINE_FLAGS) -o sqlite$(EXE) $(TOP)/src/shell.c \
		libsqlite.a $(LIBREADLINE) $(THREADLIB)

objects: $(LIBOBJ_ORIG)

# This target creates a directory named "tsrc" and fills it with
# copies of all of the C source code and header files needed to
# build on the target system.  Some of the C source code and header
# files are automatically generated.  This target takes care of
# all that automatic generation.
#
target_source:	$(SRC) $(VDBEHDR) opcodes.c
	rm -rf tsrc
	mkdir tsrc
	cp $(SRC) $(VDBEHDR) tsrc
	rm tsrc/sqlite.h.in tsrc/parse.y
	cp parse.c opcodes.c tsrc

# Rules to build the LEMON compiler generator
#
lemon:	$(TOP)/tool/lemon.c $(TOP)/tool/lempar.c
	$(BCC) -o lemon $(TOP)/tool/lemon.c
	cp $(TOP)/tool/lempar.c .

# Rules to build individual files
#
attach.o:	$(TOP)/src/attach.c $(HDR)
	$(TCCX) -c $(TOP)/src/attach.c

auth.o:	$(TOP)/src/auth.c $(HDR)
	$(TCCX) -c $(TOP)/src/auth.c

btree.o:	$(TOP)/src/btree.c $(HDR) $(TOP)/src/pager.h
	$(TCCX) -c $(TOP)/src/btree.c

build.o:	$(TOP)/src/build.c $(HDR)
	$(TCCX) -c $(TOP)/src/build.c

# The config.h file will contain a single #define that tells us how
# many bytes are in a pointer.  This only works if a pointer is the
# same size on the host as it is on the target.  If you are cross-compiling
# to a target with a different pointer size, you'll need to manually
# configure the config.h file.
#
config.h:	
	echo '#include <stdio.h>' >temp.c
	echo 'int main(){printf(' >>temp.c
	echo '"#define SQLITE_PTR_SZ %d",sizeof(char*));' >>temp.c
	echo 'exit(0);}' >>temp.c
	$(BCC) -o temp temp.c
	./temp >config.h
	echo >>config.h
	rm -f temp.c temp

copy.o:	$(TOP)/src/copy.c $(HDR)
	$(TCCX) -c $(TOP)/src/copy.c

date.o:	$(TOP)/src/date.c $(HDR)
	$(TCCX) -c $(TOP)/src/date.c

delete.o:	$(TOP)/src/delete.c $(HDR)
	$(TCCX) -c $(TOP)/src/delete.c

encode.o:	$(TOP)/src/encode.c
	$(TCCX) -c $(TOP)/src/encode.c

expr.o:	$(TOP)/src/expr.c $(HDR)
	$(TCCX) -c $(TOP)/src/expr.c

func.o:	$(TOP)/src/func.c $(HDR)
	$(TCCX) -c $(TOP)/src/func.c

hash.o:	$(TOP)/src/hash.c $(HDR)
	$(TCCX) -c $(TOP)/src/hash.c

insert.o:	$(TOP)/src/insert.c $(HDR)
	$(TCCX) -c $(TOP)/src/insert.c

legacy.o:	$(TOP)/src/legacy.c $(HDR)
	$(TCCX) -c $(TOP)/src/legacy.c

main.o:	$(TOP)/src/main.c $(HDR)
	$(TCCX) -c $(TOP)/src/main.c

pager.o:	$(TOP)/src/pager.c $(HDR) $(TOP)/src/pager.h
	$(TCCX) -c $(TOP)/src/pager.c

opcodes.o:	opcodes.c
	$(TCCX) -c opcodes.c

opcodes.c:	$(TOP)/src/vdbe.c
	echo '/* Automatically generated file.  Do not edit */' >opcodes.c
	echo 'char *sqlite3OpcodeNames[] = { "???", ' >>opcodes.c
	grep '^case OP_' $(TOP)/src/vdbe.c | \
	  sed -e 's/^.*OP_/  "/' -e 's/:.*$$/", /' >>opcodes.c
	echo '};' >>opcodes.c

opcodes.h:	$(TOP)/src/vdbe.h
	echo '/* Automatically generated file.  Do not edit */' >opcodes.h
	grep '^case OP_' $(TOP)/src/vdbe.c | \
	  sed -e 's/://' | \
	  awk '{printf "#define %-30s %3d\n", $$2, ++cnt}' >>opcodes.h

os_mac.o:	$(TOP)/src/os_mac.c $(HDR)
	$(TCCX) -c $(TOP)/src/os_mac.c

os_unix.o:	$(TOP)/src/os_unix.c $(HDR)
	$(TCCX) -c $(TOP)/src/os_unix.c

os_win.o:	$(TOP)/src/os_win.c $(HDR)
	$(TCCX) -c $(TOP)/src/os_win.c

parse.o:	parse.c $(HDR)
	$(TCCX) -c parse.c

parse.h:	parse.c

parse.c:	$(TOP)/src/parse.y lemon
	cp $(TOP)/src/parse.y .
	./lemon parse.y

pragma.o:	$(TOP)/src/pragma.c $(HDR)
	$(TCCX) $(TCL_FLAGS) -c $(TOP)/src/pragma.c

printf.o:	$(TOP)/src/printf.c $(HDR)
	$(TCCX) $(TCL_FLAGS) -c $(TOP)/src/printf.c

random.o:	$(TOP)/src/random.c $(HDR)
	$(TCCX) -c $(TOP)/src/random.c

select.o:	$(TOP)/src/select.c $(HDR)
	$(TCCX) -c $(TOP)/src/select.c

sqlite.h:	$(TOP)/src/sqlite.h.in 
	sed -e s/--VERS--/`cat ${TOP}/VERSION`/ \
            -e s/--ENCODING--/$(ENCODING)/ \
                 $(TOP)/src/sqlite.h.in >sqlite.h

table.o:	$(TOP)/src/table.c $(HDR)
	$(TCCX) -c $(TOP)/src/table.c

tclsqlite.o:	$(TOP)/src/tclsqlite.c $(HDR)
	$(TCCX) $(TCL_FLAGS) -c $(TOP)/src/tclsqlite.c

tokenize.o:	$(TOP)/src/tokenize.c $(HDR)
	$(TCCX) -c $(TOP)/src/tokenize.c

trigger.o:	$(TOP)/src/trigger.c $(HDR)
	$(TCCX) -c $(TOP)/src/trigger.c

update.o:	$(TOP)/src/update.c $(HDR)
	$(TCCX) -c $(TOP)/src/update.c

utf.o:	$(TOP)/src/utf.c $(HDR)
	$(TCCX) -c $(TOP)/src/utf.c

util.o:	$(TOP)/src/util.c $(HDR)
	$(TCCX) -c $(TOP)/src/util.c

vacuum.o:	$(TOP)/src/vacuum.c $(HDR)
	$(TCCX) -c $(TOP)/src/vacuum.c

vdbe.o:	$(TOP)/src/vdbe.c $(VDBEHDR)
	$(TCCX) -c $(TOP)/src/vdbe.c

vdbeapi.o:	$(TOP)/src/vdbeapi.c $(VDBEHDR)
	$(TCCX) -c $(TOP)/src/vdbeapi.c

vdbeaux.o:	$(TOP)/src/vdbeaux.c $(VDBEHDR)
	$(TCCX) -c $(TOP)/src/vdbeaux.c

vdbemem.o:	$(TOP)/src/vdbemem.c $(VDBEHDR)
	$(TCCX) -c $(TOP)/src/vdbemem.c

where.o:	$(TOP)/src/where.c $(HDR)
	$(TCCX) -c $(TOP)/src/where.c

# Rules for building test programs and for running tests
#
tclsqlite:	$(TOP)/src/tclsqlite.c libsqlite.a
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 -o tclsqlite \
		$(TOP)/src/tclsqlite.c libsqlite.a $(LIBTCL)

testfixture$(EXE):	$(TOP)/src/tclsqlite.c libsqlite.a $(TESTSRC)
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 -DSQLITE_TEST=1 -o testfixture$(EXE) \
		$(TESTSRC) $(TOP)/src/tclsqlite.c \
		libsqlite.a $(LIBTCL) $(THREADLIB)

fulltest:	testfixture$(EXE) sqlite$(EXE)
	./testfixture$(EXE) $(TOP)/test/all.test

test:	testfixture$(EXE) sqlite$(EXE)
	./testfixture$(EXE) $(TOP)/test/quick.test

# Rules used to build documentation
#
arch.html:	$(TOP)/www/arch.tcl
	tclsh $(TOP)/www/arch.tcl >arch.html

arch.png:	$(TOP)/www/arch.png
	cp $(TOP)/www/arch.png .

c_interface.html:	$(TOP)/www/c_interface.tcl
	tclsh $(TOP)/www/c_interface.tcl >c_interface.html

changes.html:	$(TOP)/www/changes.tcl
	tclsh $(TOP)/www/changes.tcl >changes.html

conflict.html:	$(TOP)/www/conflict.tcl
	tclsh $(TOP)/www/conflict.tcl >conflict.html

datatypes.html:	$(TOP)/www/datatypes.tcl
	tclsh $(TOP)/www/datatypes.tcl >datatypes.html

download.html:	$(TOP)/www/download.tcl
	tclsh $(TOP)/www/download.tcl >download.html

faq.html:	$(TOP)/www/faq.tcl
	tclsh $(TOP)/www/faq.tcl >faq.html

fileformat.html:	$(TOP)/www/fileformat.tcl
	tclsh $(TOP)/www/fileformat.tcl >fileformat.html

formatchng.html:	$(TOP)/www/formatchng.tcl
	tclsh $(TOP)/www/formatchng.tcl >formatchng.html

index.html:	$(TOP)/www/index.tcl last_change
	tclsh $(TOP)/www/index.tcl `cat $(TOP)/VERSION` >index.html

lang.html:	$(TOP)/www/lang.tcl
	tclsh $(TOP)/www/lang.tcl >lang.html

omitted.html:	$(TOP)/www/omitted.tcl
	tclsh $(TOP)/www/omitted.tcl >omitted.html

opcode.html:	$(TOP)/www/opcode.tcl $(TOP)/src/vdbe.c
	tclsh $(TOP)/www/opcode.tcl $(TOP)/src/vdbe.c >opcode.html

mingw.html:	$(TOP)/www/mingw.tcl
	tclsh $(TOP)/www/mingw.tcl >mingw.html

nulls.html:	$(TOP)/www/nulls.tcl
	tclsh $(TOP)/www/nulls.tcl >nulls.html

quickstart.html:	$(TOP)/www/quickstart.tcl
	tclsh $(TOP)/www/quickstart.tcl >quickstart.html

speed.html:	$(TOP)/www/speed.tcl
	tclsh $(TOP)/www/speed.tcl >speed.html

sqlite.html:	$(TOP)/www/sqlite.tcl
	tclsh $(TOP)/www/sqlite.tcl >sqlite.html

tclsqlite.html:	$(TOP)/www/tclsqlite.tcl
	tclsh $(TOP)/www/tclsqlite.tcl >tclsqlite.html

vdbe.html:	$(TOP)/www/vdbe.tcl
	tclsh $(TOP)/www/vdbe.tcl >vdbe.html


# Files to be published on the website.
#
DOC = \
  arch.html \
  arch.png \
  changes.html \
  c_interface.html \
  conflict.html \
  datatypes.html \
  download.html \
  faq.html \
  fileformat.html \
  formatchng.html \
  index.html \
  lang.html \
  mingw.html \
  nulls.html \
  omitted.html \
  opcode.html \
  quickstart.html \
  speed.html \
  sqlite.html \
  tclsqlite.html \
  vdbe.html 

doc:	$(DOC)
	mkdir -p doc
	mv $(DOC) doc

# Standard install and cleanup targets
#
install:	sqlite libsqlite.a sqlite.h
	mv sqlite /usr/bin
	mv libsqlite.a /usr/lib
	mv sqlite.h /usr/include

clean:	
	rm -f *.o sqlite libsqlite.a sqlite.h opcodes.*
	rm -f lemon lempar.c parse.* sqlite*.tar.gz
	rm -f $(PUBLISH)
	rm -f *.da *.bb *.bbg gmon.out
	rm -rf tsrc
