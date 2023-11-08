


namespace eval trd {
  variable tcltest
  variable extra
  variable all_configs
  variable build


  # Tcl tests to run for various builds.
  #
  set tcltest(linux.Fast-One)             veryquick
  set tcltest(linux.Debug-One)            veryquick
  set tcltest(linux.Debug-Two)            veryquick
  set tcltest(linux.Have-Not)             veryquick
  set tcltest(linux.Secure-Delete)        veryquick
  set tcltest(linux.Unlock-Notify)        veryquick
  set tcltest(linux.User-Auth)            veryquick
  set tcltest(linux.Update-Delete-Limit)  veryquick
  set tcltest(linux.Extra-Robustness)     veryquick
  set tcltest(linux.Device-Two)           veryquick
  set tcltest(linux.No-lookaside)         veryquick
  set tcltest(linux.Devkit)               veryquick
  set tcltest(linux.Apple)                veryquick
  set tcltest(linux.Sanitize)             veryquick
  set tcltest(linux.Device-One)           all
  set tcltest(linux.Default)              all_plus_autovacuum_crash
  set tcltest(linux.Valgrind)             valgrind

  set tcltest(osx.Locking-Style)          veryquick
  set tcltest(osx.Have-Not)               veryquick
  set tcltest(osx.Apple)                  all

  set tcltest(win.Stdcall)                veryquick
  set tcltest(win.Have-Not)               veryquick
  set tcltest(win.Windows-Memdebug)       veryquick
  set tcltest(win.Windows-Win32Heap)      veryquick
  set tcltest(win.Windows-Sanitize)       veryquick
  set tcltest(win.Default)                full

  # Extra [make xyz] tests that should be run for various builds.
  #
  set extra(linux.Check-Symbols)          checksymbols
  set extra(linux.Fast-One)               {fuzztest sourcetest}
  set extra(linux.Debug-One)              {fuzztest sourcetest mptest}
  set extra(linux.Debug-Two)              {fuzztest sourcetest}
  set extra(linux.Have-Not)               {fuzztest sourcetest}
  set extra(linux.Secure-Delete)          {fuzztest sourcetest}
  set extra(linux.Unlock-Notify)          {fuzztest sourcetest}
  set extra(linux.Update-Delete-Limit)    {fuzztest sourcetest}
  set extra(linux.Extra-Robustness)       {fuzztest sourcetest}
  set extra(linux.Device-Two)             {fuzztest sourcetest threadtest}
  set extra(linux.No-lookaside)           {fuzztest sourcetest}
  set extra(linux.Devkit)                 {fuzztest sourcetest}
  set extra(linux.Apple)                  {fuzztest sourcetest}
  set extra(linux.Sanitize)               {fuzztest sourcetest}
  set extra(linux.Default)                {fuzztest sourcetest threadtest}

  set extra(osx.Apple)                    {fuzztest threadtest}
  set extra(osx.Have-Not)                 {fuzztest sourcetest}
  set extra(osx.Locking-Style)            {mptest fuzztest sourcetest}

  set extra(win.Default)                  mptest
  set extra(win.Stdcall)                  {fuzztest sourcetest}
  set extra(win.Windows-Memdebug)         {fuzztest sourcetest}
  set extra(win.Windows-Win32Heap)        {fuzztest sourcetest}
  set extra(win.Windows-Sanitize)         fuzztest
  set extra(win.Have-Not)                 {fuzztest sourcetest}

  # The following mirrors the set of test suites invoked by "all.test".
  #
  set all_configs {
    full no_optimization memsubsys1 memsubsys2 singlethread 
    multithread onefile utf16 exclusive persistent_journal 
    persistent_journal_error no_journal no_journal_error
    autovacuum_ioerr no_mutex_try fullmutex journaltest 
    inmemory_journal pcache0 pcache10 pcache50 pcache90 
    pcache100 prepare mmap
  }

  #-----------------------------------------------------------------------
  # Start of build() definitions.
  #
  set build(Default) {
    -O2
    --disable-amalgamation --disable-shared
    --enable-session
    -DSQLITE_ENABLE_RBU
  }

  # These two are used by [testrunner.tcl mdevtest] (All-O0) and 
  # [testrunner.tcl sdevtest] (All-Sanitize).
  #
  set build(All-Debug) {
    --enable-debug --enable-all
  }
  set build(All-O0) {
    -O0 --enable-all
  }
  set build(All-Sanitize) { 
    -DSQLITE_OMIT_LOOKASIDE=1
    --enable-all -fsanitize=address,undefined 
  }

  set build(Sanitize) {
    CC=clang -fsanitize=address,undefined
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_OMIT_LOOKASIDE=1
    -DCONFIG_SLOWDOWN_FACTOR=5.0
    --enable-debug
    --enable-all
  }
  set build(Stdcall) {
    -DUSE_STDCALL=1
    -O2
  }

  # The "Have-Not" configuration sets all possible -UHAVE_feature options
  # in order to verify that the code works even on platforms that lack
  # these support services.
  set build(Have-Not) {
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
  set build(Unlock-Notify) {
    -O2
    -DSQLITE_ENABLE_UNLOCK_NOTIFY
    -DSQLITE_THREADSAFE
    -DSQLITE_TCL_DEFAULT_FULLMUTEX=1
  }
  set build(User-Auth) {
    -O2
    -DSQLITE_USER_AUTHENTICATION=1
  }
  set build(Secure-Delete) {
    -O2
    -DSQLITE_SECURE_DELETE=1
    -DSQLITE_SOUNDEX=1
  }
  set build(Update-Delete-Limit) {
    -O2
    -DSQLITE_DEFAULT_FILE_FORMAT=4
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
    -DSQLITE_ENABLE_STMT_SCANSTATUS
    -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
    -DSQLITE_ENABLE_CURSOR_HINTS
  }
  set build(Check-Symbols) {
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
    --enable-fts5 --enable-session
  }
  set build(Debug-One) {
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
    --enable-fts5
  }
  set build(Debug-Two) {
    -DSQLITE_DEFAULT_MEMSTATUS=0
    -DSQLITE_MAX_EXPR_DEPTH=0
    --enable-debug
  }
  set build(Fast-One) {
    -O6
    -DSQLITE_ENABLE_FTS4=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_RBU
    -DSQLITE_MAX_ATTACHED=125
    -DSQLITE_MAX_MMAP_SIZE=12884901888
    -DSQLITE_ENABLE_SORTER_MMAP=1
    -DLONGDOUBLE_TYPE=double
    --enable-session
  }
  set build(Device-One) {
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
  }
  set build(Device-Two) {
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
    --enable-fts5 --enable-session
  }
  set build(Locking-Style) {
    -O2
    -DSQLITE_ENABLE_LOCKING_STYLE=1
  }
  set build(Apple) {
    -Os
    -DHAVE_GMTIME_R=1
    -DHAVE_ISNAN=1
    -DHAVE_LOCALTIME_R=1
    -DHAVE_PREAD=1
    -DHAVE_PWRITE=1
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
    -DSQLITE_ENABLE_PERSIST_WAL=1
    -DSQLITE_ENABLE_PURGEABLE_PCACHE=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_SETLK_TIMEOUT=2
    -DSQLITE_ENABLE_SNAPSHOT=1
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
    -DSQLITE_MAX_LENGTH=2147483645
    -DSQLITE_MAX_VARIABLE_NUMBER=500000
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
    --enable-fts5
  }
  set build(Extra-Robustness) {
    -DSQLITE_ENABLE_OVERSIZE_CELL_CHECK=1
    -DSQLITE_MAX_ATTACHED=62
  }
  set build(Devkit) {
    -DSQLITE_DEFAULT_FILE_FORMAT=4
    -DSQLITE_MAX_ATTACHED=30
    -DSQLITE_ENABLE_COLUMN_METADATA
    -DSQLITE_ENABLE_FTS4
    -DSQLITE_ENABLE_FTS5
    -DSQLITE_ENABLE_FTS4_PARENTHESIS
    -DSQLITE_DISABLE_FTS4_DEFERRED
    -DSQLITE_ENABLE_RTREE
    --enable-fts5
  }
  set build(No-lookaside) {
    -DSQLITE_TEST_REALLOC_STRESS=1
    -DSQLITE_OMIT_LOOKASIDE=1
  }
  set build(Valgrind) {
    -DSQLITE_ENABLE_STAT4
    -DSQLITE_ENABLE_FTS4
    -DSQLITE_ENABLE_RTREE
    -DSQLITE_ENABLE_HIDDEN_COLUMNS
    -DLONGDOUBLE_TYPE=double
    -DCONFIG_SLOWDOWN_FACTOR=8.0
  }

  set build(Windows-Memdebug) {
    MEMDEBUG=1
    DEBUG=3
  }
  set build(Windows-Win32Heap) {
    WIN32HEAP=1
    DEBUG=4
  }
  set build(Windows-Sanitize) {
    ASAN=1
  }

}


#-------------------------------------------------------------------------
proc trd_import {} {
  uplevel {
    variable ::trd::tcltest
    variable ::trd::extra
    variable ::trd::all_configs
    variable ::trd::build
  }
}

proc trd_builds {platform} {
  trd_import

  set klist [lsort -uniq [concat \
      [array names tcltest ${platform}.*] \
      [array names extra ${platform}.*]   \
  ]]
  if {[llength $klist]==0} {
    error "no such platform: $platform"
  }

  set ret ""
  foreach k $klist {
    foreach {p c} [split $k "."] {}
    lappend ret $c
  }
  set ret
}

proc trd_configs {platform bld} {
  trd_import

  set clist [list]

  if {[info exists tcltest($platform.$bld)]} {
    set clist $tcltest($platform.$bld)
    if {$clist=="all"} {
      set clist $all_configs
    } elseif {$clist=="all_plus_autovacuum_crash"} {
      set clist [concat $all_configs autovacuum_crash]
    }
  }

  set clist
}

proc trd_extras {platform bld} {
  trd_import
  if {[info exists extra($platform.$bld)]==0} { return [list] }
  return $extra($platform.$bld)
}

# Usage: 
#
#     trd_fuzztest_data
#
# This returns data used by testrunner.tcl to run commands equivalent 
# to [make fuzztest]. The returned value is a list, which should be
# interpreted as a sequence of pairs. The first element of each pair
# is an interpreter name. The second element is a list of files.
# testrunner.tcl automatically creates one job to build each interpreter,
# and one to run each of the files with it once it has been built.
#
# In practice, the returned value looks like this:
#
# {
#   {fuzzcheck {$testdir/fuzzdata1.db $testdir/fuzzdata2.db ...}}
#   {{sessionfuzz run} $testdir/sessionfuzz-data1.db}
# }
#
# where $testdir is replaced by the full-path to the test-directory (the
# directory containing this file). "fuzzcheck" and "sessionfuzz" have .exe
# extensions on windows.
#
proc trd_fuzztest_data {} {
  set EXE ""
  set lFuzzDb    [glob [file join $::testdir fuzzdata*.db]] 
  set lSessionDb [glob [file join $::testdir sessionfuzz-data*.db]]

  if {$::tcl_platform(platform)=="windows"} {
    return [list fuzzcheck.exe $lFuzzDb]
  }

  return [list fuzzcheck $lFuzzDb {sessionfuzz run} $lSessionDb]
}


proc trd_all_configs {} {
  trd_import
  set all_configs
}

proc trimscript {text} {
  set text [string map {"\n    " "\n"} [string trim $text]]
}

proc make_sh_script {srcdir opts cflags makeOpts configOpts} {

  set tcldir [::tcl::pkgconfig get libdir,install]
  set myopts ""
  if {[info exists ::env(OPTS)]} {
    append myopts "# From environment variable:\n"
    append myopts "OPTS=$::env(OPTS)\n\n"
  }
  foreach o [lsort $opts] { 
    append myopts "OPTS=\"\$OPTS $o\"\n"
  }

  return [trimscript [subst -nocommands {
    set -e
    if [ "\$#" -ne 1 ] ; then
      echo "Usage: \$0 <target>"
      exit -1
    fi
    
    SRCDIR="$srcdir"
    TCLDIR="$tcldir"
    
    if [ ! -f Makefile ] ; then
      \$SRCDIR/configure --with-tcl=\$TCL $configOpts 
    fi
    
    $myopts
    CFLAGS="$cflags"
    
    make \$1 "CFLAGS=\$CFLAGS" "OPTS=\$OPTS" $makeOpts
  }]]
}

# Generate the text of a *.bat script.
#
proc make_bat_file {srcdir opts cflags makeOpts} {
  set srcdir [file nativename [file normalize $srcdir]]

  return [trimscript [subst -nocommands {
    set TARGET=%1
    set TMP=%CD%
    nmake /f $srcdir\\Makefile.msc TOP="$srcdir" %TARGET% "CCOPTS=$cflags" "OPTS=$opts" $makeOpts
  }]]
}


# Generate the text of a shell script.
#
proc make_script {cfg srcdir bMsvc} {
  set opts       [list]                         ;# OPTS value
  set cflags     [expr {$bMsvc ? "-Zi" : "-g"}] ;# CFLAGS value
  set makeOpts   [list]                         ;# Extra args for [make]
  set configOpts [list]                         ;# Extra args for [configure]

  # Define either SQLITE_OS_WIN or SQLITE_OS_UNIX, as appropriate.
  if {$::tcl_platform(platform)=="windows"} {
    lappend opts -DSQLITE_OS_WIN=1
  } else {
    lappend opts -DSQLITE_OS_UNIX=1
  }

  # Unless the configuration specifies -DHAVE_USLEEP=0, set -DHAVE_USLEEP=1.
  #
  if {[lsearch $cfg "-DHAVE_USLEEP=0"]<0} {
    lappend cfg -DHAVE_USLEEP=1
  }

  # Loop through the parameters of the nominated configuration, updating
  # $opts, $cflags, $makeOpts and $configOpts along the way. Rules are as
  # follows:
  #
  #   1. If the parameter begins with "-D", add it to $opts.
  #
  #   2. If the parameter begins with "--" add it to $configOpts. Unless
  #      this command is preparing a script for MSVC - then add an 
  #      equivalent to $makeOpts or $opts.
  #
  #   3. If the parameter begins with "-" add it to $cflags. If in MSVC
  #      mode and the parameter is an -O<integer> option, instead add
  #      an OPTIMIZATIONS=<integer> switch to $makeOpts.
  #
  #   4. If none of the above apply, add the parameter to $makeOpts
  #
  foreach param $cfg {

    if {[string range $param 0 1]=="-D"} {
      lappend opts $param
      continue
    }

    if {[string range $param 0 1]=="--"} {
      if {$bMsvc==0} {
        lappend configOpts $param
      } else {

        switch -- $param {
          --disable-amalgamation {
            lappend makeOpts USE_AMALGAMATION=0
          }
          --disable-shared {
            lappend makeOpts USE_CRT_DLL=0 DYNAMIC_SHELL=0
          }
          --enable-fts5 {
            lappend opts -DSQLITE_ENABLE_FTS5
          } 
          --enable-shared {
            lappend makeOpts USE_CRT_DLL=1 DYNAMIC_SHELL=1
          }
          --enable-session {
            lappend opts -DSQLITE_ENABLE_PREUPDATE_HOOK
            lappend opts -DSQLITE_ENABLE_SESSION
          }
          --enable-all {
          }
          --enable-debug {
            # lappend makeOpts OPTIMIZATIONS=0
            lappend opts -DSQLITE_DEBUG
          }
          default {
            error "Cannot translate $param for MSVC"
          }
        }
      }

      continue
    }

    if {[string range $param 0 0]=="-"} {

      if {$bMsvc} {
        if {[regexp -- {^-O(\d+)$} $param -> level]} {
          lappend makeOpts OPTIMIZATIONS=$level
          continue
        }
        if {$param eq "-fsanitize=address,undefined"} {
          lappend makeOpts ASAN=1
          continue
        }
      }

      lappend cflags $param
      continue
    }

    lappend makeOpts $param
  }

  if {$bMsvc==0} {
    set zRet [make_sh_script $srcdir $opts $cflags $makeOpts $configOpts]
  } else {
    set zRet [make_bat_file $srcdir $opts $cflags $makeOpts]
  }
}

# Usage:
#
#    trd_buildscript CONFIG SRCDIR MSVC
#
# This command returns the full text of a script (either a shell script or
# an ms-dos bat file) that may be used to build SQLite source code according
# to a nominated configuration.
#
# Parameter CONFIG must be a configuration defined above in the ::trd::build
# array. SRCDIR is the root directory of an SQLite source tree (the parent
# directory of that containing this script). MSVC is a boolean - true to
# use the MSVC compiler, false otherwise.
#
proc trd_buildscript {config srcdir bMsvc} {
  trd_import

  # Ensure that the named configuration exists.
  if {![info exists build($config)]} {
    error "No such build config: $config"
  }

  # Generate and return the script.
  return [make_script $build($config) $srcdir $bMsvc]
}

# Usage:
#
#    trd_test_script_properties PATH
#
# The argument must be a path to a Tcl test script. This function scans the
# first 100 lines of the script for lines that look like:
#
#    TESTRUNNER: <properties>
#
# where <properties> is a list of identifiers, each of which defines a 
# property of the test script. Example properties are "slow" or "superslow".
#
proc trd_test_script_properties {path} {
  # Use this global array as a cache:
  global trd_test_script_properties_cache

  if {![info exists trd_test_script_properties_cache($path)]} {
    set fd [open $path]
    set ret [list]
    for {set line 0} {$line < 100 && ![eof $fd]} {incr line} {
      set text [gets $fd]
      if {[string match -nocase *testrunner:* $text]} {
        regexp -nocase {.*testrunner:(.*)} $text -> properties
        lappend ret {*}$properties
      }
    }
    set trd_test_script_properties_cache($path) $ret
    close $fd
  }

  set trd_test_script_properties_cache($path)
}

