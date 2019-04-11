
# This file contains Configuration data used by "wapptest.tcl" and
# "releasetest.tcl".
#

# Omit comments (text between # and \n) in a long multi-line string.
#
proc strip_comments {in} {
  regsub -all {#[^\n]*\n} $in {} out
  return $out
}

array set ::Configs [strip_comments {
  "Default" {
    -O2
    --disable-amalgamation --disable-shared
    --enable-session
    -DSQLITE_ENABLE_DESERIALIZE
  }
  "Sanitize" {
    CC=clang -fsanitize=undefined
    -DSQLITE_ENABLE_STAT4
    --enable-session
  }
  "Stdcall" {
    -DUSE_STDCALL=1
    -O2
  }
  "Have-Not" {
    # The "Have-Not" configuration sets all possible -UHAVE_feature options
    # in order to verify that the code works even on platforms that lack
    # these support services.
    -DHAVE_FDATASYNC=0
    -DHAVE_GMTIME_R=0
    -DHAVE_ISNAN=0
    -DHAVE_LOCALTIME_R=0
    -DHAVE_LOCALTIME_S=0
    -DHAVE_MALLOC_USABLE_SIZE=0
    -DHAVE_STRCHRNUL=0
    -DHAVE_USLEEP=0
    -DHAVE_UTIME=0
  }
  "Unlock-Notify" {
    -O2
    -DSQLITE_ENABLE_UNLOCK_NOTIFY
    -DSQLITE_THREADSAFE
    -DSQLITE_TCL_DEFAULT_FULLMUTEX=1
  }
  "User-Auth" {
    -O2
    -DSQLITE_USER_AUTHENTICATION=1
  }
  "Secure-Delete" {
    -O2
    -DSQLITE_SECURE_DELETE=1
    -DSQLITE_SOUNDEX=1
  }
  "Update-Delete-Limit" {
    -O2
    -DSQLITE_DEFAULT_FILE_FORMAT=4
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
    -DSQLITE_ENABLE_STMT_SCANSTATUS
    -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
    -DSQLITE_ENABLE_CURSOR_HINTS
    --enable-json1
  }
  "Check-Symbols" {
    -DSQLITE_MEMDEBUG=1
    -DSQLITE_ENABLE_FTS3_PARENTHESIS=1
    -DSQLITE_ENABLE_FTS3=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_MEMSYS5=1
    -DSQLITE_ENABLE_MEMSYS3=1
    -DSQLITE_ENABLE_COLUMN_METADATA=1
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
    -DSQLITE_SECURE_DELETE=1
    -DSQLITE_SOUNDEX=1
    -DSQLITE_ENABLE_ATOMIC_WRITE=1
    -DSQLITE_ENABLE_MEMORY_MANAGEMENT=1
    -DSQLITE_ENABLE_OVERSIZE_CELL_CHECK=1
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_STMT_SCANSTATUS
    --enable-json1 --enable-fts5 --enable-session
  }
  "Debug-One" {
    --disable-shared
    -O2 -funsigned-char
    -DSQLITE_DEBUG=1
    -DSQLITE_MEMDEBUG=1
    -DSQLITE_MUTEX_NOOP=1
    -DSQLITE_TCL_DEFAULT_FULLMUTEX=1
    -DSQLITE_ENABLE_FTS3=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_MEMSYS5=1
    -DSQLITE_ENABLE_COLUMN_METADATA=1
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_HIDDEN_COLUMNS
    -DSQLITE_MAX_ATTACHED=125
    -DSQLITE_MUTATION_TEST
    --enable-fts5 --enable-json1
  }
  "Fast-One" {
    -O6
    -DSQLITE_ENABLE_FTS4=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_RBU
    -DSQLITE_MAX_ATTACHED=125
    -DLONGDOUBLE_TYPE=double
    --enable-session
  }
  "Device-One" {
    -O2
    -DSQLITE_DEBUG=1
    -DSQLITE_DEFAULT_AUTOVACUUM=1
    -DSQLITE_DEFAULT_CACHE_SIZE=64
    -DSQLITE_DEFAULT_PAGE_SIZE=1024
    -DSQLITE_DEFAULT_TEMP_CACHE_SIZE=32
    -DSQLITE_DISABLE_LFS=1
    -DSQLITE_ENABLE_ATOMIC_WRITE=1
    -DSQLITE_ENABLE_IOTRACE=1
    -DSQLITE_ENABLE_MEMORY_MANAGEMENT=1
    -DSQLITE_MAX_PAGE_SIZE=4096
    -DSQLITE_OMIT_LOAD_EXTENSION=1
    -DSQLITE_OMIT_PROGRESS_CALLBACK=1
    -DSQLITE_OMIT_VIRTUALTABLE=1
    -DSQLITE_ENABLE_HIDDEN_COLUMNS
    -DSQLITE_TEMP_STORE=3
    --enable-json1
  }
  "Device-Two" {
    -DSQLITE_4_BYTE_ALIGNED_MALLOC=1
    -DSQLITE_DEFAULT_AUTOVACUUM=1
    -DSQLITE_DEFAULT_CACHE_SIZE=1000
    -DSQLITE_DEFAULT_LOCKING_MODE=0
    -DSQLITE_DEFAULT_PAGE_SIZE=1024
    -DSQLITE_DEFAULT_TEMP_CACHE_SIZE=1000
    -DSQLITE_DISABLE_LFS=1
    -DSQLITE_ENABLE_FTS3=1
    -DSQLITE_ENABLE_MEMORY_MANAGEMENT=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_MAX_COMPOUND_SELECT=50
    -DSQLITE_MAX_PAGE_SIZE=32768
    -DSQLITE_OMIT_TRACE=1
    -DSQLITE_TEMP_STORE=3
    -DSQLITE_THREADSAFE=2
    -DSQLITE_ENABLE_DESERIALIZE=1
    --enable-json1 --enable-fts5 --enable-session
  }
  "Locking-Style" {
    -O2
    -DSQLITE_ENABLE_LOCKING_STYLE=1
  }
  "Apple" {
    -Os
    -DHAVE_GMTIME_R=1
    -DHAVE_ISNAN=1
    -DHAVE_LOCALTIME_R=1
    -DHAVE_PREAD=1
    -DHAVE_PWRITE=1
    -DHAVE_USLEEP=1
    -DHAVE_USLEEP=1
    -DHAVE_UTIME=1
    -DSQLITE_DEFAULT_CACHE_SIZE=1000
    -DSQLITE_DEFAULT_CKPTFULLFSYNC=1
    -DSQLITE_DEFAULT_MEMSTATUS=1
    -DSQLITE_DEFAULT_PAGE_SIZE=1024
    -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS=1
    -DSQLITE_ENABLE_API_ARMOR=1
    -DSQLITE_ENABLE_AUTO_PROFILE=1
    -DSQLITE_ENABLE_FLOCKTIMEOUT=1
    -DSQLITE_ENABLE_FTS3=1
    -DSQLITE_ENABLE_FTS3_PARENTHESIS=1
    -DSQLITE_ENABLE_FTS3_TOKENIZER=1
    if:os=="Darwin" -DSQLITE_ENABLE_LOCKING_STYLE=1
    -DSQLITE_ENABLE_PERSIST_WAL=1
    -DSQLITE_ENABLE_PURGEABLE_PCACHE=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_SNAPSHOT=1
    # -DSQLITE_ENABLE_SQLLOG=1
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
    -DSQLITE_MAX_LENGTH=2147483645
    -DSQLITE_MAX_VARIABLE_NUMBER=500000
    # -DSQLITE_MEMDEBUG=1
    -DSQLITE_NO_SYNC=1
    -DSQLITE_OMIT_AUTORESET=1
    -DSQLITE_OMIT_LOAD_EXTENSION=1
    -DSQLITE_PREFER_PROXY_LOCKING=1
    -DSQLITE_SERIES_CONSTRAINT_VERIFY=1
    -DSQLITE_THREADSAFE=2
    -DSQLITE_USE_URI=1
    -DSQLITE_WRITE_WALFRAME_PREBUFFERED=1
    -DUSE_GUARDED_FD=1
    -DUSE_PREAD=1
    --enable-json1 --enable-fts5
  }
  "Extra-Robustness" {
    -DSQLITE_ENABLE_OVERSIZE_CELL_CHECK=1
    -DSQLITE_MAX_ATTACHED=62
  }
  "Devkit" {
    -DSQLITE_DEFAULT_FILE_FORMAT=4
    -DSQLITE_MAX_ATTACHED=30
    -DSQLITE_ENABLE_COLUMN_METADATA
    -DSQLITE_ENABLE_FTS4
    -DSQLITE_ENABLE_FTS5
    -DSQLITE_ENABLE_FTS4_PARENTHESIS
    -DSQLITE_DISABLE_FTS4_DEFERRED
    -DSQLITE_ENABLE_RTREE
    --enable-json1 --enable-fts5
  }
  "No-lookaside" {
    -DSQLITE_TEST_REALLOC_STRESS=1
    -DSQLITE_OMIT_LOOKASIDE=1
    -DHAVE_USLEEP=1
  }
  "Valgrind" {
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_FTS4
    -DSQLITE_ENABLE_RTREE
    -DSQLITE_ENABLE_HIDDEN_COLUMNS
    --enable-json1
  }

  # The next group of configurations are used only by the
  # Failure-Detection platform.  They are all the same, but we need
  # different names for them all so that they results appear in separate
  # subdirectories.
  #
  Fail0 {-O0}
  Fail2 {-O0}
  Fail3 {-O0}
  Fail4 {-O0}
  FuzzFail1 {-O0}
  FuzzFail2 {-O0}
}]

array set ::Platforms [strip_comments {
  Linux-x86_64 {
    "Check-Symbols"           checksymbols
    "Fast-One"                "fuzztest test"
    "Debug-One"               "mptest test"
    "Have-Not"                test
    "Secure-Delete"           test
    "Unlock-Notify"           "QUICKTEST_INCLUDE=notify2.test test"
    "User-Auth"               tcltest
    "Update-Delete-Limit"     test
    "Extra-Robustness"        test
    "Device-Two"              test
    "No-lookaside"            test
    "Devkit"                  test
    "Apple"                   test
    "Sanitize"                {QUICKTEST_OMIT=func4.test,nan.test test}
    "Device-One"              fulltest
    "Default"                 "threadtest fulltest"
    "Valgrind"                valgrindtest
  }
  Linux-i686 {
    "Devkit"                  test
    "Have-Not"                test
    "Unlock-Notify"           "QUICKTEST_INCLUDE=notify2.test test"
    "Device-One"              test
    "Device-Two"              test
    "Default"                 "threadtest fulltest"
  }
  Darwin-i386 {
    "Locking-Style"           "mptest test"
    "Have-Not"                test
    "Apple"                   "threadtest fulltest"
  }
  Darwin-x86_64 {
    "Locking-Style"           "mptest test"
    "Have-Not"                test
    "Apple"                   "threadtest fulltest"
  }
  "Windows NT-intel" {
    "Stdcall"                 test
    "Have-Not"                test
    "Default"                 "mptest fulltestonly"
  }
  "Windows NT-amd64" {
    "Stdcall"                 test
    "Have-Not"                test
    "Default"                 "mptest fulltestonly"
  }

  # The Failure-Detection platform runs various tests that deliberately
  # fail.  This is used as a test of this script to verify that this script
  # correctly identifies failures.
  #
  Failure-Detection {
    Fail0     "TEST_FAILURE=0 test"
    Sanitize  "TEST_FAILURE=1 test"
    Fail2     "TEST_FAILURE=2 valgrindtest"
    Fail3     "TEST_FAILURE=3 valgrindtest"
    Fail4     "TEST_FAILURE=4 test"
    FuzzFail1 "TEST_FAILURE=5 test"
    FuzzFail2 "TEST_FAILURE=5 valgrindtest"
  }
}]

proc make_test_suite {msvc withtcl name testtarget config} {

  # Tcl variable $opts is used to build up the value used to set the
  # OPTS Makefile variable. Variable $cflags holds the value for
  # CFLAGS. The makefile will pass OPTS to both gcc and lemon, but
  # CFLAGS is only passed to gcc.
  #
  set makeOpts ""
  set cflags [expr {$msvc ? "-Zi" : "-g"}]
  set opts ""
  set title ${name}($testtarget)
  set configOpts $withtcl
  set skip 0

  regsub -all {#[^\n]*\n} $config \n config
  foreach arg $config {
    if {$skip} {
      set skip 0
      continue
    }
    if {[regexp {^-[UD]} $arg]} {
      lappend opts $arg
    } elseif {[regexp {^[A-Z]+=} $arg]} {
      lappend testtarget $arg
    } elseif {[regexp {^if:([a-z]+)(.*)} $arg all key tail]} {
      # Arguments of the form 'if:os=="Linux"' will cause the subsequent
      # argument to be skipped if the $tcl_platform(os) is not "Linux", for
      # example...
      set skip [expr !(\$::tcl_platform($key)$tail)]
    } elseif {[regexp {^--(enable|disable)-} $arg]} {
      if {$msvc} {
        if {$arg eq "--disable-amalgamation"} {
          lappend makeOpts USE_AMALGAMATION=0
          continue
        }
        if {$arg eq "--disable-shared"} {
          lappend makeOpts USE_CRT_DLL=0 DYNAMIC_SHELL=0
          continue
        }
        if {$arg eq "--enable-fts5"} {
          lappend opts -DSQLITE_ENABLE_FTS5
          continue
        }
        if {$arg eq "--enable-json1"} {
          lappend opts -DSQLITE_ENABLE_JSON1
          continue
        }
        if {$arg eq "--enable-shared"} {
          lappend makeOpts USE_CRT_DLL=1 DYNAMIC_SHELL=1
          continue
        }
      }
      lappend configOpts $arg
    } else {
      if {$msvc} {
        if {$arg eq "-g"} {
          lappend cflags -Zi
          continue
        }
        if {[regexp -- {^-O(\d+)$} $arg all level]} then {
          lappend makeOpts OPTIMIZATIONS=$level
          continue
        }
      }
      lappend cflags $arg
    }
  }

  # Disable sync to make testing faster.
  #
  lappend opts -DSQLITE_NO_SYNC=1

  # Some configurations already set HAVE_USLEEP; in that case, skip it.
  #
  if {[lsearch -regexp $opts {^-DHAVE_USLEEP(?:=|$)}]==-1} {
    lappend opts -DHAVE_USLEEP=1
  }

  # Add the define for this platform.
  #
  if {$::tcl_platform(platform)=="windows"} {
    lappend opts -DSQLITE_OS_WIN=1
  } else {
    lappend opts -DSQLITE_OS_UNIX=1
  }

  # Set the sub-directory to use.
  #
  set dir [string tolower [string map {- _ " " _ "(" _ ")" _} $name]]

  # Join option lists into strings, using space as delimiter.
  #
  set makeOpts [join $makeOpts " "]
  set cflags   [join $cflags " "]
  set opts     [join $opts " "]

  return [list $title $dir $configOpts $testtarget $makeOpts $cflags $opts]
}

# Configuration verification: Check that each entry in the list of configs
# specified for each platforms exists.
#
foreach {key value} [array get ::Platforms] {
  foreach {v t} $value {
    if {0==[info exists ::Configs($v)]} {
      puts stderr "No such configuration: \"$v\""
      exit -1
    }
  }
}

