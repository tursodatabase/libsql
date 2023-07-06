/**
   Dummy function stubs to get sqlite3.c compiling with
   wasi-sdk. This requires, in addition:

   -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_GETPID

   -lwasi-emulated-getpid
*/
typedef unsigned mode_t;
int fchmod(int fd, mode_t mode);
int fchmod(int fd, mode_t mode){
  return (fd && mode) ? 0 : 0;
}
typedef unsigned uid_t;
typedef uid_t gid_t;
int fchown(int fd, uid_t owner, gid_t group);
int fchown(int fd, uid_t owner, gid_t group){
  return (fd && owner && group) ? 0 : 0;
}
uid_t geteuid(void);
uid_t geteuid(void){return 0;}
#if !defined(F_WRLCK)
enum {
F_WRLCK,
F_RDLCK,
F_GETLK,
F_SETLK,
F_UNLCK
};
#endif

#undef HAVE_PREAD

#include <wasi/api.h>
#define WASM__KEEP __attribute__((used))

#if 0
/**
   wasi-sdk cannot build sqlite3's default VFS without at least the following
   functions. They are apparently syscalls which clients have to implement or
   otherwise obtain.

   https://github.com/WebAssembly/WASI/blob/main/phases/snapshot/docs.md
*/
environ_get
environ_sizes_get
clock_time_get
fd_close
fd_fdstat_get
fd_fdstat_set_flags
fd_filestat_get
fd_filestat_set_size
fd_pread
fd_prestat_get
fd_prestat_dir_name
fd_read
fd_seek
fd_sync
fd_write
path_create_directory
path_filestat_get
path_filestat_set_times
path_open
path_readlink
path_remove_directory
path_unlink_file
poll_oneoff
proc_exit
#endif
