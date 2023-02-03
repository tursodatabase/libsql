


namespace eval trd {
  variable tcltest
  variable extra
  variable all_configs


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
}


#-------------------------------------------------------------------------
proc trd_import {} {
  uplevel {
    variable ::trd::tcltest
    variable ::trd::extra
    variable ::trd::all_configs
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

proc trd_configs {platform build} {
  trd_import

  set clist [list]

  if {[info exists tcltest($platform.$build)]} {
    set clist $tcltest($platform.$build)
    if {$clist=="all"} {
      set clist $all_configs
    } elseif {$clist=="all_plus_autovacuum_crash"} {
      set clist [concat $all_configs autovacuum_crash]
    }
  }

  set clist
}

proc trd_extras {platform build} {
  trd_import

  set elist [list]
  if {[info exists extra($platform.$build)]} {
    set elist $extra($platform.$build)
  }

  set elist
}

proc trd_all_configs {} {
  trd_import
  set all_configs
}



