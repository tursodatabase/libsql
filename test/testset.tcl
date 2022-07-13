

# Commands in this file:
#
#    testset_all
#      Return a list of all test scripts designed to be run individually.
#
#    testset_veryquick
#      The subset of [testset_all] meant to run as veryquick.test.
# 

set D(testdir) [file dir [file normalize [info script]]]

proc testset_all {} {
  global D
  set ret [list]

  # The following tests are driver scripts that themselves run lots of other
  # test scripts. They should be ignored here.
  set drivers {
    all.test        async.test         quick.test  veryquick.test
    memleak.test    permutations.test  soak.test   fts3.test
    mallocAll.test  rtree.test         full.test   extraquick.test
    session.test    rbu.test
  }

  set srcdir [file dirname $D(testdir)]
  set ret [glob -nocomplain           \
      $srcdir/test/*.test             \
      $srcdir/ext/rtree/*.test        \
      $srcdir/ext/fts5/test/*.test    \
      $srcdir/ext/expert/*.test       \
      $srcdir/ext/session/*.test      \
  ]
  set ret [ts_filter $ret $drivers]
  return $ret
}

proc testset_veryquick {} {
  set ret [testset_all]

  set ret [ts_filter $ret {
    async2.test async3.test backup_ioerr.test corrupt.test
    corruptC.test crash.test crash2.test crash3.test crash4.test crash5.test
    crash6.test crash7.test delete3.test e_fts3.test fts3rnd.test
    fkey_malloc.test fuzz.test fuzz3.test fuzz_malloc.test in2.test loadext.test
    misc7.test mutex2.test notify2.test onefile.test pagerfault2.test 
    savepoint4.test savepoint6.test select9.test 
    speed1.test speed1p.test speed2.test speed3.test speed4.test 
    speed4p.test sqllimits1.test tkt2686.test thread001.test thread002.test
    thread003.test thread004.test thread005.test trans2.test vacuum3.test 
    incrvacuum_ioerr.test autovacuum_crash.test btree8.test shared_err.test
    vtab_err.test walslow.test walcrash.test walcrash3.test
    walthread.test rtree3.test indexfault.test securedel2.test
    sort3.test sort4.test fts4growth.test fts4growth2.test
    bigsort.test walprotocol.test mmap4.test fuzzer2.test
    walcrash2.test e_fkey.test backup.test
  
    fts4merge.test fts4merge2.test fts4merge4.test fts4check.test
    fts4merge5.test
    fts3cov.test fts3snippet.test fts3corrupt2.test fts3an.test
    fts3defer.test fts4langid.test fts3sort.test fts5unicode.test
  
    rtree4.test
    sessionbig.test
  }]

  set ret [ts_filter $ret {
    *malloc* *ioerr* *fault* *bigfile* *_err* *fts5corrupt* *fts5big* *fts5aj*
  }]

  return $ret
}

proc ts_filter {input exlist} {
  foreach f $input { set a($f) 1 }
  foreach e $exlist { array unset a */$e }
  array names a
}

proc testset_patternlist {patternlist} {
  set nPat [llength $patternlist]

  if {$nPat==0} {
    set scripts [testset_veryquick]
  } else {
    set ii 0
    set p0 [lindex $patternlist 0]

    if {$p0=="veryquick"} {
      set scripts [testset_veryquick]
      incr ii
    } elseif {$p0=="all"} {
      set scripts [testset_all]
      incr ii
    } else {
      set scripts [testset_all]
    }

    if {$nPat>$ii} {
      array set S [list]
      foreach f $scripts { set a([file tail $f]) $f }

      foreach p [lrange $patternlist $ii end] {
        set nList [llength [array names a $p]]
        if {$nList==0} {
          puts stderr "Argument $p matches no scripts (typo?)"
          exit 1
        }
        foreach n [array names a $p] { set S($a($n)) 1 }
      }

      set scripts [lsort [array names S]]
    }

  }

  set scripts
}


