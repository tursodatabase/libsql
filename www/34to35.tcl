#
# Run this TCL script to generate HTML for the goals.html file.
#
set rcsid {$Id: 34to35.tcl,v 1.4 2007/10/01 13:54:11 drh Exp $}
source common.tcl
header {SQLite Changes From Version 3.4.2 To 3.5.0}

proc CODE {text} {
  puts "<blockquote><pre>"
  puts $text
  puts "</pre></blockquote>"
}
proc SYNTAX {text} {
  puts "<blockquote><pre>"
  set t2 [string map {& &amp; < &lt; > &gt;} $text]
  regsub -all "/(\[^\n/\]+)/" $t2 {</b><i>\1</i><b>} t3
  puts "<b>$t3</b>"
  puts "</pre></blockquote>"
}
proc IMAGE {name {caption {}}} {
  puts "<center><img src=\"$name\">"
  if {$caption!=""} {
    puts "<br>$caption"
  }
  puts "</center>"
}
proc PARAGRAPH {text} {
  # regsub -all "/(\[a-zA-Z0-9\]+)/" $text {<i>\1</i>} t2
  #regsub -all "\\*(\[^\n*\]+)\\*" $text {<tt><b><big>\1</big></b></tt>} t3
  regsub -all {\[([^]\n]+)\]} $text {[resolve_link \1]} t3
  puts "<p>[subst -novar -noback $t3]</p>\n"
}
proc resolve_link {args} {
  set a2 [split $args |]
  set id [string trim [lindex $a2 0]]
  if {[lindex $a2 1]==""} {
    set display [string trim [lindex $a2 0]]
  } else {
    set display [string trim [lrange $a2 1 end]]
  }
  regsub -all {[^a-zA-Z0-9_]} $id {} id
  return "<a href=\"capi3ref.html#$id\">$display</a>"
}
set level(0) 0
set level(1) 0
proc HEADING {n name {tag {}}} {
  if {$tag!=""} {
    puts "<a name=\"$tag\">"
  }
  global level
  incr level($n)
  for {set i [expr {$n+1}]} {$i<10} {incr i} {
    set level($i) 0
  }
  if {$n==0} {
    set num {}
  } elseif {$n==1} {
    set num $level(1).0
  } else {
    set num $level(1)
    for {set i 2} {$i<=$n} {incr i} {
      append num .$level($i)
    }
  }
  incr n 1
  puts "<h$n>$num $name</h$n>"
}

HEADING 0 {Moving From SQLite 3.4.2 to 3.5.0}

PARAGRAPH {
  SQLite version 3.5.0 introduces a new OS interface layer that
  is incompatible with all prior versions of SQLite.  In addition,
  a few existing interfaces have been generalized to work across all
  database connections within a process rather than just all
  connections within a thread.  The purpose of this article
  is to describe the changes to 3.5.0 in detail so that users
  of prior versions of SQLite can judge what, if any, effort will
  be required to upgrade to newer versions.
}

HEADING 1 {Overview Of Changes}

PARAGRAPH {
  A quick enumeration of the changes in SQLite version 3.5.0
  is provide here.  Subsequent sections will describe these
  changes in more detail.
}
PARAGRAPH {
  <ol>
  <li>The OS interface layer has been completely reworked:
  <ol type="a">
  <li>The undocumented <b>sqlite3_os_switch()</b> interface has
      been removed.</li>
  <li>The <b>SQLITE_ENABLE_REDEF_IO</b> compile-time flag no longer functions.
      I/O procedures are now always redefinable.</li>
  <li>Three new objects are defined for specifying I/O procedures:
      [sqlite3_vfs], [sqlite3_file], and [sqlite3_io_methods].</li>
  <li>Three new interfaces are used to create alternative OS interfaces:
      [sqlite3_vfs_register()], [sqlite3_vfs_unregister()], and
      [sqlite3_vfs_find()].</li>
  <li>A new interface has been added to provided additional control over
      the creation of new database connections: [sqlite3_open_v2()].
      The legacy interfaces of [sqlite3_open()] and
      [sqlite3_open16()] continue to be fully supported.</li>
  </ol></li>
  <li>The optional shared cache and memory management features that
      were introduced in version 3.3.0 can now be used across multiple
      threads within the same process.  Formerly, these extensions only
      applied to database connections operating within a single thread.
   <ol type="a">
   <li>The [sqlite3_enable_shared_cache()] interface now applies to all
       threads within a process, not to just the one thread in which it
       was run.</li>
   <li>The [sqlite3_soft_heap_limit()] interface now applies to all threads
       within a process, not to just the one thread in which it was run.</li>
   <li>The [sqlite3_release_memory()] interface will now attempt to reduce
       the memory usages across all database connections in all threads, not
       just connections in the thread where the interface is called.</li>
   <li>The [sqlite3_thread_cleanup()] interface has become a no-op.</li>
   </ol></li>
  <li>Restrictions on the use of the same database connection by multiple
      threads have been dropped.  It is now safe for
      multiple threads to use the same database connection at the same
      time.</li>
  <li>There is now a compile-time option that allows an application to
      define alternative malloc()/free() implementations without having
      to modify any core SQLite code.</li>
  <li>There is now a compile-time option that allows an application to
      define alternative mutex implementations without having
      to modify any core SQLite code.</li>
  </ol>
}
PARAGRAPH {
  Of these changes, only 1a and 2a through 2c are incompatibilities
  in any formal sense.
  But users who have previously made custom modifications to the
  SQLite source (for example to add a custom OS layer for embedded
  hardware) might find that these changes have a larger impact.
  On the other hand, an important goal of these changes is to make
  it much easier to customize SQLite for use on different operating
  systems. 
}

HEADING 1 {The OS Interface Layer}

PARAGRAPH {
  If your system defines a custom OS interface for SQLite or if you
  were using the undocumented <b>sqlite3_os_switch()</b>
  interface, then you will need to make modifications in order to
  upgrade to SQLite version 3.5.0.  This may seem painful at first
  glance.  But as you look more closely, you will probably discover
  that your changes are made smaller and easier to understand and manage
  by the new SQLite interface.  It is likely that your changes will
  now also work seamlessly with the SQLite amalgamation.  You will
  no longer need to make any changes to the code SQLite source code.
  All of your changes can be effected by application code and you can
  link against a standard, unmodified version of the SQLite amalgamation.
  Furthermore, the OS interface layer, which was formerly undocumented,
  is now an officially support interface for SQLite.  So you have
  some assurance that this will be a one-time change and that your
  new backend will continue to work in future versions of SQLite.
}

HEADING 2 {The Virtual File System Object}

PARAGRAPH {
  The new OS interface for SQLite is built around an object named
  [sqlite3_vfs].  The "vfs" standard for "Virtual File System".
  The sqlite3_vfs object is basically a structure containing pointers
  to functions that implement the primitive disk I/O operations that
  SQLite needs to perform in order to read and write databases.  
  In this article, we will often refer a sqlite3_vfs objects as a "VFS".
}

PARAGRAPH {
  SQLite is able to use multiple VFSes at the same time.  Each
  individual database connection is associated with just one VFS.
  But if you have multiple database connections, each connection
  can be associated with a different VFS.
}

PARAGRAPH {
  There is always a default VFS.
  The legacy interfaces [sqlite3_open()] and [sqlite3_open16()] always
  use the default VFS.
  The new interface for creating database connections,
  [sqlite3_open_v2()], allows you to specify which VFS you want to
  use by name.
}

HEADING 3 {Registering New VFS Objects}

PARAGRAPH {
  Standard builds of SQLite for unix or windows come with a single
  VFS named "unix" or "win32", as appropriate.  This one VFS is also 
  the default.  So if you are using the legacy open functions, everything
  will continue to operate as it has before.  The change is that an application
  now has the flexibility of adding new VFS modules to implement a
  customized OS layer.  The [sqlite3_vfs_register()] API can be used
  to tell SQLite about one or more application-defined VFS modules:
}

CODE {
int sqlite3_vfs_register(sqlite3_vfs*, int makeDflt);
}

PARAGRAPH {
  Applications can call sqlite3_vfs_register at any time, though of course
  a VFS needs to be registered before it can be used.  The first argument
  is a pointer to a customized VFS object that the application has prepared.
  The second argument is true to make the new VFS the default VFS so that
  it will be used by the legacy [sqlite3_open()] and [sqlite3_open16()] APIs.
  If the new VFS is not the default, then you will probably have to use
  the new [sqlite3_open_v2()] API to use it.  Note, however, that if
  a new VFS is the only VFS known to SQLite (if SQLite was compiled without
  its usual default VFS or if the pre-compiled default VFS was removed
  using [sqlite3_vfs_unregister()]) then the new VFS automatic becomes the
  default VFS regardless of the makeDflt argument to [sqlite3_vfs_register()].
}

PARAGRAPH {
  Standard builds include the default "unix" or "win32" VFSes.
  But if you use the -DOS_OTHER=1 compile-time option, then SQLite is
  built without a default VFS.  In that case, the application must
  register at least one VFS prior to calling [sqlite3_open()].
  This is the approach that embedded applications should use.
  Rather than modifying the SQLite source to to insert an alternative
  OS layer as was done in prior releases of SQLite, instead compile
  an unmodified SQLite source file (preferably the amalgamation)
  with the -DOS_OTHER=1 option, then invoke [sqlite3_vfs_register()]
  to define the interface to the underlying filesystem prior to
  creating any database connections.
}

HEADING 3 {Additional Control Over VFS Objects}

PARAGRAPH {
  The [sqlite3_vfs_unregister()] API is used to remove an existing
  VFS from the system.
}

CODE {
int sqlite3_vfs_unregister(sqlite3_vfs*);
}

PARAGRAPH {
  The [sqlite3_vfs_find()] API is used to locate a particular VFS
  by name.  Its prototype is as follows:
}

CODE {
sqlite3_vfs *sqlite3_vfs_find(const char *zVfsName);
}

PARAGRAPH {
  The argument is the symbolic name for the desired VFS.  If the
  argument is a NULL pointer, then the default VFS is returned.
  The function returns a pointer to the [sqlite3_vfs] object that
  implements the VFS.  Or it returns a NULL pointer if no object
  could be found that matched the search criteria.
}

HEADING 3 {Modifications Of Existing VFSes}

PARAGRAPH {
  Once a VFS has been registered, it should never be modified.  If
  a change in behavior is required, a new VFS should be registered.
  The application could, perhaps, use [sqlite3_vfs_find()] to locate
  the old VFS, make a copy of the old VFS into a new [sqlite3_vfs]
  object, make the desired modifications to the new VFS, unregister
  the old VFS, the register the new VFS in its place.  Existing
  database connections would continue to use the old VFS even after
  it is unregistered, but new database connections would use the
  new VFS.
}  

HEADING 3 {The VFS Object}

PARAGRAPH {
  A VFS object is an instance of the following structure:
}

CODE {
typedef struct sqlite3_vfs sqlite3_vfs;
struct sqlite3_vfs {
  int iVersion;            /* Structure version number */
  int szOsFile;            /* Size of subclassed sqlite3_file */
  int mxPathname;          /* Maximum file pathname length */
  sqlite3_vfs *pNext;      /* Next registered VFS */
  const char *zName;       /* Name of this virtual file system */
  void *pAppData;          /* Pointer to application-specific data */
  int (*xOpen)(sqlite3_vfs*, const char *zName, sqlite3_file*,
               int flags, int *pOutFlags);
  int (*xDelete)(sqlite3_vfs*, const char *zName, int syncDir);
  int (*xAccess)(sqlite3_vfs*, const char *zName, int flags);
  int (*xGetTempName)(sqlite3_vfs*, char *zOut);
  int (*xFullPathname)(sqlite3_vfs*, const char *zName, char *zOut);
  void *(*xDlOpen)(sqlite3_vfs*, const char *zFilename);
  void (*xDlError)(sqlite3_vfs*, int nByte, char *zErrMsg);
  void *(*xDlSym)(sqlite3_vfs*,void*, const char *zSymbol);
  void (*xDlClose)(sqlite3_vfs*, void*);
  int (*xRandomness)(sqlite3_vfs*, int nByte, char *zOut);
  int (*xSleep)(sqlite3_vfs*, int microseconds);
  int (*xCurrentTime)(sqlite3_vfs*, double*);
  /* New fields may be appended in figure versions.  The iVersion
  ** value will increment whenever this happens. */
};
}

PARAGRAPH {
  To create a new VFS, an application fills in an instance of this
  structure with appropriate values and then calls [sqlite3_vfs_register()].
}

PARAGRAPH {
  The iVersion field of [sqlite3_vfs] should be 1 for SQLite version 3.5.0.
  This number may increase in future versions of SQLite if we have to
  modify the VFS object in some way.  We hope that this never happens,
  but the provision is made in case it does.
}

PARAGRAPH {
  The szOsFile field is the size in bytes of the structure that defines
  an open file: the [sqlite3_file] object.  This object will be described
  more fully below.  The point here is that each VFS implementation can
  define its own [sqlite3_file] object containing whatever information
  the VFS implementation needs to store about an open file.  SQLite needs
  to know how big this object is, however, in order to preallocate enough
  space to hold it.
}

PARAGRAPH {
  The mxPathname field is the maximum length of a file pathname that
  this VFS can use.  SQLite sometimes has to preallocate buffers of
  this size, so it should be as small as reasonably possible.  Some
  filesystems permit huge pathnames, but in practice pathnames rarely
  extend beyond 100 bytes or so.  You do not have to put the longest
  pathname that the underlying filesystem can handle here.  You only
  have to put the longest pathname that you want SQLite to be able to
  handle.  A few hundred is a good value in most cases.
}

PARAGRAPH {
  The pNext field is used internally by SQLite.  Specifically, SQLite
  uses this field to form a linked list of registered VFSes.
}

PARAGRAPH {
  The zName field is the symbolic name of the VFS.  This is the name 
  that the [sqlite3_vfs_find()] compares against when it is looking for
  a VFS.
}

PARAGRAPH {
  The pAppData pointer is unused by the SQLite core.  The pointer is
  available to store auxiliary information that a VFS information might
  want to carry around.
}

PARAGRAPH {
  The remaining fields of the [sqlite3_vfs] object all store pointer
  to functions that implement primitive operations.  We call these
  "methods".  The first methods, xOpen, is used to open files on
  the underlying storage media.  The result is an [sqlite3_file]
  object.  There are additional methods, defined by the [sqlite3_file]
  object itself that are used to read and write and close the file.
  The additional methods are detailed below.  The filename is in UTF-8.
  SQLite will guarantee that the zFilename string passed to
  xOpen() is a full pathname as generated by xFullPathname() and
  that the string will be valid and unchanged until xClose() is
  called.  So the [sqlite3_file] can store a pointer to the
   filename if it needs to remember the filename for some reason.
   The flags argument to xOpen() is a copy of the flags argument
   to sqlite3_open_v2().  If sqlite3_open() or sqlite3_open16()
   is used, then flags is [SQLITE_OPEN_READWRITE] | [SQLITE_OPEN_CREATE].
   If xOpen() opens a file read-only then it sets *pOutFlags to
   include [SQLITE_OPEN_READONLY].  Other bits in *pOutFlags may be
   set.
   SQLite will also add one of the following flags to the xOpen()
   call, depending on the object being opened:
   <ul>
   <li>  [SQLITE_OPEN_MAIN_DB]
   <li>  [SQLITE_OPEN_MAIN_JOURNAL]
   <li>  [SQLITE_OPEN_TEMP_DB]
   <li>  [SQLITE_OPEN_TEMP_JOURNAL]
   <li>  [SQLITE_OPEN_TRANSIENT_DB]
   <li>  [SQLITE_OPEN_SUBJOURNAL]
   <li>  [SQLITE_OPEN_MASTER_JOURNAL]
   </ul>
   The file I/O implementation can use the object type flags to
   changes the way it deals with files.  For example, an application
   that does not care about crash recovery or rollback, might make
   the open of a journal file a no-op.  Writes to this journal are
   also a no-op.  Any attempt to read the journal returns [SQLITE_IOERR].
   Or the implementation might recognize the a database file will
   be doing page-aligned sector reads and writes in a random order
   and set up its I/O subsystem accordingly.
   SQLite might also add one of the following flags to the xOpen
   method:
   <ul>
   <li> [SQLITE_OPEN_DELETEONCLOSE]
   <li> [SQLITE_OPEN_EXCLUSIVE]
   </ul>
   The [SQLITE_OPEN_DELETEONCLOSE] flag means the file should be
   deleted when it is closed.  This will always be set for TEMP 
   databases and journals and for subjournals.  The 
   [SQLITE_OPEN_EXCLUSIVE] flag means the file should be opened
   for exclusive access.  This flag is set for all files except
   for the main database file.
   The [sqlite3_file] structure passed as the third argument to
   xOpen is allocated by the caller.  xOpen just fills it in.  The
   caller allocates a minimum of szOsFile bytes for the [sqlite3_file]
   structure.
}

PARAGRAPH {
  The differences between an [SQLITE_OPEN_TEMP_DB] database and an
  [SQLITE_OPEN_TRANSIENT_DB] database is this:  The [SQLITE_OPEN_TEMP_DB]
  is used for explicitly declared and named TEMP tables (using the
  CREATE TEMP TABLE syntax) or for named tables in a temporary database
  that is created by opening a database with a filename that is an empty
  string.  An [SQLITE_OPEN_TRANSIENT_DB] holds an database table that
  SQLite creates automatically in order to evaluate a subquery or
  ORDER BY or GROUP BY clause.  Both TEMP_DB and TRANSIENT_DB databases
  are private and are deleted automatically.  TEMP_DB databases last
  for the duration of the database connection.  TRANSIENT_DB databases
  last only for the duration of a single SQL statement.
}

PARAGRAPH {
  The xDelete method is used delete a file.  The name of the file is
  given in the second parameter.  The filename will be in UTF-8.
  The VFS must convert the filename into whatever character representation
  the underlying operating system expects.  If the syncDir parameter is
  true, then the xDelete method should not return until the change
  to the directory contents for the directory containing the
  deleted file have been synced to disk in order to insure that the
  file does not "reappear" if a power failure occurs soon after.
}

PARAGRAPH {
  The xAccess method is used to check for access permissions on a file.
  The filename will be UTF-8 encoded.  The flags argument will be
  [SQLITE_ACCESS_EXISTS] to check for the existence of the file,
  [SQLITE_ACCESS_READWRITE] to check to see if the file is both readable
  and writable, or [SQLITE_ACCESS_READ] to check to see if the file is
  at least readable.  The "file" named by the second parameter might
  be a directory or folder name.
}

PARAGRAPH {
  The xGetTempName method computes the name of a temporary file that
  SQLite can use.  The name should be written into the buffer given
  by the second parameter.  SQLite will size that buffer to hold
  at least mxPathname bytes.  The generated filename should be in UTF-8.
  To avoid security problems, the generated temporary filename should
  contain enough randomness to prevent an attacker from guessing the
  temporary filename in advance.
}

PARAGRAPH {
  The xFullPathname method is used to convert a relative pathname
  into a full pathname.  The resulting full pathname is written into
  the buffer provided by the third parameter.  SQLite will size the
  output buffer to at least mxPathname bytes.  Both the input and
  output names should be in UTF-8.
}

PARAGRAPH {
  The xDlOpen, xDlError, xDlSym, and xDlClose methods are all used for
  accessing shared libraries at run-time.  These methods may be omitted
  (and their pointers set to zero) if the library is compiled with
  SQLITE_OMIT_LOAD_EXTENSION or if the [sqlite3_enable_load_extension()]
  interface is never used to enable dynamic extension loading.  The
  xDlOpen method opens a shared library or DLL and returns a pointer to
  a handle.  NULL is returned if the open fails.  If the open fails,
  the xDlError method can be used to obtain a text error message.
  The message is written into the zErrMsg buffer of the third parameter
  which is at least nByte bytes in length.  The xDlSym returns a pointer
  to a symbol in the shared library.  The name of the symbol is given
  by the second parameter.  UTF-8 encoding is assumed.  If the symbol
  is not found a NULL pointer is returned.  The xDlClose routine closes
  the shared library.
}

PARAGRAPH {
  The xRandomness method is used exactly once to initialize the 
  pseudo-random number generator (PRNG) inside of SQLite.  Only
  the xRandomness method on the default VFS is used.  The xRandomness
  methods on other VFSes are never accessed by SQLite.
  The xRandomness routine requests that nByte bytes of randomness
  be written into zOut.  The routine returns the actual number of
  bytes of randomness obtained.  The quality of the randomness so obtained
  will determine the quality of the randomness generated by built-in 
  SQLite functions such as random() and randomblob().  SQLite also
  uses its PRNG to generate temporary file names..  On some platforms
  (ex: windows) SQLite assumes that temporary file names are unique
  without actually testing for collisions, so it is important to have
  good-quality randomness even if the random() and randomblob() 
  functions are never used.
}

PARAGRAPH {
  The xSleep method is used to suspend the calling thread for at
  least the number of microseconds given.  This method is used to
  implement the [sqlite3_sleep()] and [sqlite3_busy_timeout()] APIs.
  In the case of [sqlite3_sleep()] the xSleep method of the default
  VFS is always used.  If the underlying system does not have a
  microsecond resolution sleep capability, then the sleep time should
  be rounded up.  xSleep returns this rounded-up value.
}

PARAGRAPH {
  The xCurrentTime method finds the current time and date and writes
  the result as double-precision floating point value into pointer
  provided by the second parameter.  The time and date is in
  coordinated universal time (UTC) and is a fractional julian day number.
}

HEADING 3 {The Open File Object}

PARAGRAPH {
  The result of opening a file is an instance of an [sqlite3_file] object.
  The [sqlite3_file] object is an abstract base class defined as follows:
}

CODE {
typedef struct sqlite3_file sqlite3_file;
struct sqlite3_file {
  const struct sqlite3_io_methods *pMethods;
};
}

PARAGRAPH {
  Each VFS implementation will subclass the [sqlite3_file] by adding
  additional fields at the end to hold whatever information the VFS
  needs to know about an open file.  It does not matter what information
  is stored as long as the total size of the structure does not exceed
  the szOsFile value recorded in the [sqlite3_vfs] object.
}

PARAGRAPH {
  The [sqlite3_io_methods] object is a structure that contains pointers
  to methods for reading, writing, and otherwise dealing with files.
  This object is defined as follows:
}

CODE {
typedef struct sqlite3_io_methods sqlite3_io_methods;
struct sqlite3_io_methods {
  int iVersion;
  int (*xClose)(sqlite3_file*);
  int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
  int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);
  int (*xTruncate)(sqlite3_file*, sqlite3_int64 size);
  int (*xSync)(sqlite3_file*, int flags);
  int (*xFileSize)(sqlite3_file*, sqlite3_int64 *pSize);
  int (*xLock)(sqlite3_file*, int);
  int (*xUnlock)(sqlite3_file*, int);
  int (*xCheckReservedLock)(sqlite3_file*);
  int (*xFileControl)(sqlite3_file*, int op, void *pArg);
  int (*xSectorSize)(sqlite3_file*);
  int (*xDeviceCharacteristics)(sqlite3_file*);
  /* Additional methods may be added in future releases */
};
}

PARAGRAPH {
  The iVersion field of [sqlite3_io_methods] is provided as insurance
  against future enhancements.  The iVersion value should always be
  1 for SQLite version 3.5.
}

PARAGRAPH {
  The xClose method closes the file.  The space for the [sqlite3_file]
  structure is deallocated by the caller.  But if the [sqlite3_file]
  contains pointers to other allocated memory or resources, those
  allocations should be released by the xClose method.
}

PARAGRAPH {
  The xRead method reads iAmt bytes from the file beginning at a byte
  offset to iOfst.  The data read is stored in the pointer of the
  second parameter.  xRead returns the [SQLITE_OK] on success,
  [SQLITE_IOERR_SHORT_READ] if it was not able to read the full number
  of bytes because it reached end-of-file, or [SQLITE_IOERR_READ] for
  any other error.
}

PARAGRAPH {
  The xWrite method writes iAmt bytes of data from the second parameter
  into the file beginning at an offset of iOfst bytes.  If the size of
  the file is less than iOfst bytes prior to the write, then xWrite should
  ensure that the file is extended with zeros up to iOfst bytes prior
  to beginning its write.  xWrite continues to extends the file as
  necessary so that the size of the file is at least iAmt+iOfst bytes 
  at the conclusion of the xWrite call.  The xWrite method returns
  [SQLITE_OK] on success.  If the write cannot complete because the
  underlying storage medium is full, then [SQLITE_FULL] is returned.
  [SQLITE_IOERR_WRITE] should be returned for any other error.
}

PARAGRAPH {
  The xTruncate method truncates a file to be nByte bytes in length.
  If the file is already nByte bytes or less in length then this
  method is a no-op.  The xTruncate method returns [SQLITE_OK] on
  success and [SQLITE_IOERR_TRUNCATE] if anything goes wrong.
}

PARAGRAPH {
  The xSync method is used to force previously written data out of
  operating system cache and into non-volatile memory.  The second
  parameter is usually [SQLITE_SYNC_NORMAL].  If the second parameter
  is [SQLITE_SYNC_FULL] then the xSync method should make sure that
  data has also been flushed through the disk controllers cache.
  The [SQLITE_SYNC_FULL] parameter is the equivalent of the F_FULLSYNC
  ioctl() on Mac OS X. The xSync method returns
  [SQLITE_OK] on success and [SQLITE_IOERR_FSYNC] if anything goes wrong.
}

PARAGRAPH {
  The xFileSize() method determines the current size of the file
  in bytes and writes that value into *pSize.  It returns [SQLITE_OK]
  on success and [SQLITE_IOERR_FSTAT] if something goes wrong.
}

PARAGRAPH {
  The xLock and xUnlock methods are used to set and clear file locks.
  SQLite supports five levels of file locks, in order:
  <ul>
  <li> [SQLITE_LOCK_NONE]
  <li> [SQLITE_LOCK_SHARED]
  <li> [SQLITE_LOCK_RESERVED]
  <li> [SQLITE_LOCK_PENDING]
  <li> [SQLITE_LOCK_EXCLUSIVE]
  </ul>
  The underlying implementation can support some subset of these locking
  levels as long as it meets the other requirements of this paragraph.
  The locking level is specified as the second argument to both xLock
  and xUnlock.  The xLock method increases the locking level to the
  specified locking level or higher.  The xUnlock method decreases the
  locking level to no lower than the level specified.  
  [SQLITE_LOCK_NONE] means that the file is unlocked.  [SQLITE_LOCK_SHARED]
  gives permission to read the file.  Multiple database connections can
  hold [SQLITE_LOCK_SHARED] at the same time.
  [SQLITE_LOCK_RESERVED] is like [SQLITE_LOCK_SHARED] in that its is permission
  to read the file.  But only a single connection can hold a reserved lock
  at any point in time.  The [SQLITE_LOCK_PENDING] is also permission to
  read the file.  Other connections can continue to read the file as well,
  but no other connection is allowed to escalate a lock from none to shared.
  [SQLITE_LOCK_EXCLUSIVE] is permission to write on the file.  Only a single
  connection can hold an exclusive lock and no other connection can hold
  any lock (other than "none") while one connection is hold an exclusive
  lock.  The xLock returns [SQLITE_OK] on success, [SQLITE_BUSY] if it
  is unable to obtain the lock, or [SQLITE_IOERR_RDLOCK] if something else
  goes wrong.  The xUnlock method returns [SQLITE_OK] on success and
  [SQLITE_IOERR_UNLOCK] for problems.
}

PARAGRAPH {
  The xCheckReservedLock method checks to see if another connection or
  another process is currently holding a reserved, pending, or exclusive
  lock on the file.  It returns true or false.
}

PARAGRAPH {
  The xFileControl() method is a generic interface that allows custom
  VFS implementations to directly control an open file using the
  (new and experimental)
  [sqlite3_file_control()] interface.  The second "op" argument
  is an integer opcode.   The third
  argument is a generic pointer which is intended to be a pointer
  to a structure that may contain arguments or space in which to
  write return values.  Potential uses for xFileControl() might be
  functions to enable blocking locks with timeouts, to change the
  locking strategy (for example to use dot-file locks), to inquire
  about the status of a lock, or to break stale locks.  The SQLite
  core reserves opcodes less than 100 for its own use. 
  A [SQLITE_FCNTL_LOCKSTATE | list of opcodes] less than 100 is available.
  Applications that define a custom xFileControl method should use opcodes 
  greater than 100 to avoid conflicts.
}

PARAGRAPH {
  The xSectorSize returns the "sector size" of the underlying
  non-volatile media.  A "sector" is defined as the smallest unit of
  storage that can be written without disturbing adjacent storage.
  On a disk drive the "sector size" has until recently been 512 bytes,
  though there is a push to increase this value to 4KiB.  SQLite needs
  to know the sector size so that it can write a full sector at a
  time, and thus avoid corrupting adjacent storage space if a power
  lose occurs in the middle of a write.
}

PARAGRAPH {
  The xDeviceCharacteristics method returns an integer bit vector that
  defines any special properties that the underlying storage medium might
  have that SQLite can use to increase performance.  The allowed return
  is the bit-wise OR of the following values:
  <ul>
  <li> [SQLITE_IOCAP_ATOMIC]
  <li> [SQLITE_IOCAP_ATOMIC512]
  <li> [SQLITE_IOCAP_ATOMIC1K]
  <li> [SQLITE_IOCAP_ATOMIC2K]
  <li> [SQLITE_IOCAP_ATOMIC4K]
  <li> [SQLITE_IOCAP_ATOMIC8K]
  <li> [SQLITE_IOCAP_ATOMIC16K]
  <li> [SQLITE_IOCAP_ATOMIC32K]
  <li> [SQLITE_IOCAP_ATOMIC64K]
  <li> [SQLITE_IOCAP_SAFE_APPEND]
  <li> [SQLITE_IOCAP_SEQUENTIAL]
  </ul>
  The [SQLITE_IOCAP_ATOMIC] bit means that all writes to this device are
  atomic in the sense that either the entire write occurs or none of it
  occurs.  The other 
  [SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_ATOMIC<i>nnn</i>] values indicate that
  writes of aligned blocks of the indicated size are atomic.
  [SQLITE_IOCAP_SAFE_APPEND] means that when extending a file with new
  data, the new data is written first and then the file size is updated.
  So if a power failure occurs, there is no chance that the file might have
  been extended with randomness.  The [SQLITE_IOCAP_SEQUENTIAL] bit means
  that all writes occur in the order that they are issued and are not
  reordered by the underlying file system.
}

HEADING 3 {Checklist For Constructing A New VFS}

PARAGRAPH {
  The preceding paragraphs contain a lot of information.
  To ease the task of constructing
  a new VFS for SQLite we offer the following implementation checklist:
}

PARAGRAPH {
  <ol>
  <li> Define an appropriate subclass of the [sqlite3_file] object.
  <li> Implement the methods required by the [sqlite3_io_methods] object.
  <li> Create a static and 
       constant [sqlite3_io_methods] object containing pointers
       to the methods from the previous step.
  <li> Implement the xOpen method that opens a file and populates an
       [sqlite3_file] object, including setting pMethods to
       point to the [sqlite3_io_methods] object from the previous step.
  <li> Implement the other methods required by [sqlite3_vfs].
  <li> Define a static (but not constant) [sqlite3_vfs] structure that
       contains pointers to the xOpen method and the other methods and
       which contains the appropriate values for iVersion, szOsFile,
       mxPathname, zName, and pAppData.
  <li> Implement a procedure that calls [sqlite3_vfs_register()] and
       passes it a pointer to the [sqlite3_vfs] structure from the previous
       step.  This procedure is probably the only exported symbol in the
       source file that implements your VFS.
  </ol>
}

PARAGRAPH {
  Within your application, call the procedure implemented in the last
  step above as part of your initialization process before any
  database connections are opened.  
}

HEADING 1 {The Memory Allocation Subsystem}

PARAGRAPH {
  Beginning with version 3.5, SQLite obtains all of the heap memory it
  needs using the routines [sqlite3_malloc()], [sqlite3_free()], and
  [sqlite3_realloc()].  These routines have existed in prior versions
  of SQLite, but SQLite has previously bypassed these routines and used
  its own memory allocator.  This all changes in version 3.5.0.
}

PARAGRAPH {
  The SQLite source tree actually contains multiple versions of the
  memory allocator.  The default high-speed version found in the
  "mem1.c" source file is used for most builds.  But if the SQLITE_MEMDEBUG
  flag is enabled, a separate memory allocator the "mem2.c" source file
  is used instead.  The mem2.c allocator implements lots of hooks to
  do error checking and to simulate memory allocation failures for testing
  purposes.  Both of these allocators use the malloc()/free() implementation
  in the standard C library.
}

PARAGRAPH {
  Applications are not required to use either of these standard memory
  allocators.  If SQLite is compiled with SQLITE_OMIT_MEMORY_ALLOCATION
  then no implementation for the [sqlite3_malloc()], [sqlite3_realloc()],
  and [sqlite3_free()] functions is provided.  Instead, the application
  that links against SQLite must provide its own implementation of these
  functions.  The application provided memory allocator is not required
  to use the malloc()/free() implementation in the standard C library.
  An embedded application might provide an alternative memory allocator
  that uses memory for a fixed memory pool set aside for the exclusive
  use of SQLite, for example.
}

PARAGRAPH {
  Applications that implement their own memory allocator must provide
  implementation for the usual three allocation functions 
  [sqlite3_malloc()], [sqlite3_realloc()], and [sqlite3_free()].
  And they must also implement a fourth function:
}

CODE {
int sqlite3_memory_alarm(
  void(*xCallback)(void *pArg, sqlite3_int64 used, int N),
  void *pArg,
  sqlite3_int64 iThreshold
);
}

PARAGRAPH {
 The [sqlite3_memory_alarm] routine is used to register
 a callback on memory allocation events.
 This routine registers or clears a callbacks that fires when
 the amount of memory allocated exceeds iThreshold.  Only
 a single callback can be registered at a time.  Each call
 to [sqlite3_memory_alarm()] overwrites the previous callback.
 The callback is disabled by setting xCallback to a NULL
 pointer.
}

PARAGRAPH {
 The parameters to the callback are the pArg value, the 
 amount of memory currently in use, and the size of the
 allocation that provoked the callback.  The callback will
 presumably invoke [sqlite3_free()] to free up memory space.
 The callback may invoke [sqlite3_malloc()] or [sqlite3_realloc()]
 but if it does, no additional callbacks will be invoked by
 the recursive calls.
}

PARAGRAPH {
 The [sqlite3_soft_heap_limit()] interface works by registering
 a memory alarm at the soft heap limit and invoking 
 [sqlite3_release_memory()] in the alarm callback.  Application
 programs should not attempt to use the [sqlite3_memory_alarm()]
 interface because doing so will interfere with the
 [sqlite3_soft_heap_limit()] module.  This interface is exposed
 only so that applications can provide their own
 alternative implementation when the SQLite core is
 compiled with SQLITE_OMIT_MEMORY_ALLOCATION.
}

PARAGRAPH {
  The built-in memory allocators in SQLite also provide the following
  additional interfaces:
}

CODE {
sqlite3_int64 sqlite3_memory_used(void);
sqlite3_int64 sqlite3_memory_highwater(int resetFlag);
}

PARAGRAPH {
  These interfaces can be used by an application to monitor how
  much memory SQLite is using.  The [sqlite3_memory_used()] routine
  returns the number of bytes of memory currently in use and the
  [sqlite3_memory_highwater()] returns the maximum instantaneous
  memory usage.  Neither routine includes the overhead associated
  with the memory allocator.  These routines are provided for use
  by the application.  SQLite never invokes them itself.  So if
  the application is providing its own memory allocation subsystem,
  it can omit these interfaces if desired.
}

HEADING 1 {The Mutex Subsystem}

PARAGRAPH {
  SQLite has always been threadsafe in the sense that it is safe to
  use different SQLite database connections in different threads at the
  same time.  The constraint was that the same database connection
  could not be used in two separate threads at once.  SQLite version 3.5.0
  relaxes this constraint. 
}

PARAGRAPH {
  In order to allow multiple threads to use the same database connection
  at the same time, SQLite must make extensive use of mutexes.  And for
  this reason a new mutex subsystem as been added.  The mutex subsystem
  as the following interface:
}

CODE {
sqlite3_mutex *sqlite3_mutex_alloc(int);
void sqlite3_mutex_free(sqlite3_mutex*);
void sqlite3_mutex_enter(sqlite3_mutex*);
int sqlite3_mutex_try(sqlite3_mutex*);
void sqlite3_mutex_leave(sqlite3_mutex*);
}

PARAGRAPH {
  Though these routines exist for the use of the SQLite core, 
  application code is free to use these routines as well, if desired.
  A mutex is an [sqlite3_mutex] object.  The [sqlite3_mutex_alloc()]
  routine allocates a new mutex object and returns a pointer to it.
  The argument to [sqlite3_mutex_alloc()] should be 
  [SQLITE_MUTEX_FAST] or [SQLITE_MUTEX_RECURSIVE] for non-recursive
  and recursive mutexes, respectively.  If the underlying system does
  not provide non-recursive mutexes, then a recursive mutex can be
  substituted in that case.  The argument to [sqlite3_mutex_alloc()]
  can also be a constant designating one of several static mutexes:
  <ul>
  <li>  [SQLITE_MUTEX_STATIC_MASTER]
  <li>  [SQLITE_MUTEX_STATIC_MEM]
  <li>  [SQLITE_MUTEX_STATIC_MEM2]
  <li>  [SQLITE_MUTEX_STATIC_PRNG]
  <li>  [SQLITE_MUTEX_STATIC_LRU]
  </ul>
  These static mutexes are reserved for use internally by SQLite
  and should not be used by the application.  The static mutexes
  are all non-recursive.
}

PARAGRAPH {
  The [sqlite3_mutex_free()] routine should be used to deallocate
  a non-static mutex.  If a static mutex is passed to this routine
  then the behavior is undefined.
}

PARAGRAPH {
  The [sqlite3_mutex_enter()] attempts to enter the mutex and blocks
  if another threads is already there.  [sqlite3_mutex_try()] attempts
  to enter and returns [SQLITE_OK] on success or [SQLITE_BUSY] if another
  thread is already there.  [sqlite3_mutex_leave()] exits a mutex.
  The mutex is held until the number of exits matches the number of
  entrances.  If [sqlite3_mutex_leave()] is called on a mutex that 
  the thread is not currently holding, then the behavior is undefined.
  If any routine is called for a deallocated mutex, then the behavior
  is undefined.
}

PARAGRAPH {
  The SQLite source code provides multiple implementations of these
  APIs, suitable for varying environments.  If SQLite is compiled with
  the SQLITE_THREADSAFE=0 flag then a no-op mutex implementation that 
  is fast but does no real mutual exclusion is provided.  That 
  implementation is suitable for use in single-threaded applications
  or applications that only use SQLite in a single thread.  Other
  real mutex implementations are provided based on the underlying
  operating system.
}

PARAGRAPH {
  Embedded applications may wish to provide their own mutex implementation.
  If SQLite is compiled with the -DSQLITE_MUTEX_APPDEF=1 compile-time flag
  then the SQLite core provides no mutex subsystem and a mutex subsystem
  that matches the interface described above must be provided by the
  application that links against SQLite.
}

HEADING 1 {Other Interface Changes}

PARAGRAPH {
  Version 3.5.0 of SQLite changes the behavior of a few APIs in ways
  that are technically incompatible.  However, these APIs are seldom
  used and even when they are used it is difficult to imagine a
  scenario where the change might break something.  The changes
  actually makes these interface much more useful and powerful.
}

PARAGRAPH {
  Prior to version 3.5.0, the [sqlite3_enable_shared_cache()] API
  would enable and disable the shared cache feature for all connections
  within a single thread - the same thread from which the 
  sqlite3_enable_shared_cache() routine was called.  Database connections
  that used the shared cache were restricted to running in the same
  thread in which they were opened.  Beginning with version 3.5.0,
  the sqlite3_enable_shared_cache() applies to all database connections
  in all threads within the process.  Now database connections running
  in separate threads can share a cache.  And database connections that
  use shared cache can migrate from one thread to another.
}

PARAGRAPH {
  Prior to version 3.5.0 the [sqlite3_soft_heap_limit()] set an upper
  bound on heap memory usage for all database connections within a
  single thread.  Each thread could have its own heap limit.  Beginning
  in version 3.5.0, there is a single heap limit for the entire process.
  This seems more restrictive (one limit as opposed to many) but in
  practice it is what most users want.
}

PARAGRAPH {
  Prior to version 3.5.0 the [sqlite3_release_memory()] function would
  try to reclaim memory from all database connections in the same thread
  as the sqlite3_release_memory() call.  Beginning with version 3.5.0,
  the sqlite3_release_memory() function will attempt to reclaim memory
  from all database connections in all threads.
}

HEADING 1 {Summary}

PARAGRAPH {
  The transition from SQLite version 3.4.2 to 3.5.0 is a major change.
  Every source code file in the SQLite core had to be modified, some
  extensively.  And the change introduced some minor incompatibilities
  in the C interface.  But we feel that the benefits of the transition
  from 3.4.2 to 3.5.0 far outweigh the pain of porting.  The new
  VFS layer is now well-defined and stable and should simplify future
  customizations.  The VFS layer, and the separable memory allocator
  and mutex subsystems allow a standard SQLite source code amalgamation
  to be used in an embedded project without change, greatly simplifying
  configuration management.  And the resulting system is much more
  tolerant of highly threaded designs.
}
