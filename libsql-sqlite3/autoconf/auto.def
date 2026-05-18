#!/do/not/tclsh
# ^^^ help out editors which guess this file's content type.
#
# This is the main autosetup-compatible configure script for the
# "autoconf" bundle of the SQLite project.
use sqlite-config
sqlite-configure autoconf {
  sqlite-handle-debug
  sqlite-check-common-bins ;# must come before [sqlite-handle-wasi-sdk]
  sqlite-handle-wasi-sdk   ;# must run relatively early, as it changes the environment
  sqlite-check-common-system-deps
  proj-define-for-opt static-shell ENABLE_STATIC_SHELL \
    "Link library statically into the CLI shell?"
  proj-define-for-opt static-cli-shell STATIC_CLI_SHELL "Statically link CLI shell?"
  if {![opt-bool static-shell] && [opt-bool static-cli-shell]} {
    proj-fatal "--disable-static-shell and --static-cli-shell are mutualy exclusive"
  }
  if {![opt-bool shared] && ![opt-bool static-shell]} {
    proj-opt-set shared 1
    proj-indented-notice {
      NOTICE: ignoring --disable-shared because --disable-static-shell
      was specified.
    }
  }
}
