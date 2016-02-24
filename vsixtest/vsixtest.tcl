#!/usr/bin/tclsh
#
# This script is used to quickly test a VSIX (Visual Studio Extension) file
# with Visual Studio 2015 on Windows.
#
# PREREQUISITES
#
# 1. Tcl 8.4 and later are supported, earlier versions have not been tested.
#
# 2. The "sqlite-UWP-output.vsix" file is assumed to exist in the parent
#    directory of the directory containing this script.  The [optional] first
#    command line argument to this script may be used to specify an alternate
#    file.  However, currently, the file must be compatible with both Visual
#    Studio 2015 and the Universal Windows Platform.
#
# 3. The temporary directory specified in the TEMP or TMP environment variables
#    must refer to an existing directory writable by the current user.
#
# 4. The VS140COMNTOOLS environment variable must refer to the Visual Studio
#    2015 common tools directory.
#
# USAGE
#
# The first argument to this script is optional.  If specified, it must be the
# name of the VSIX file to test.
#
package require Tcl 8.4

proc fail { {error ""} {usage false} } {
  if {[string length $error] > 0} then {
    puts stdout $error
    if {!$usage} then {exit 1}
  }

  puts stdout "usage:\
[file tail [info nameofexecutable]]\
[file tail [info script]] \[vsixFile\]"

  exit 1
}

proc getEnvironmentVariable { name } {
  #
  # NOTE: Returns the value of the specified environment variable or an empty
  #       string for environment variables that do not exist in the current
  #       process environment.
  #
  return [expr {[info exists ::env($name)] ? $::env($name) : ""}]
}

proc getTemporaryPath {} {
  #
  # NOTE: Returns the normalized path to the first temporary directory found
  #       in the typical set of environment variables used for that purpose
  #       or an empty string to signal a failure to locate such a directory.
  #
  set names [list]

  foreach name [list TEMP TMP] {
    lappend names [string toupper $name] [string tolower $name] \
        [string totitle $name]
  }

  foreach name $names {
    set value [getEnvironmentVariable $name]

    if {[string length $value] > 0} then {
      return [file normalize $value]
    }
  }

  return ""
}

proc appendArgs { args } {
  #
  # NOTE: Returns all passed arguments joined together as a single string with
  #       no intervening spaces between arguments.
  #
  eval append result $args
}

#
# NOTE: This is the entry point for this script.
#
set script [file normalize [info script]]

if {[string length $script] == 0} then {
  fail "script file currently being evaluated is unknown" true
}

set path [file dirname $script]

###############################################################################

#
# NOTE: Process and verify all the command line arguments.
#
set argc [llength $argv]
if {$argc > 1} then {fail}

if {$argc == 1} then {
  set fileName [lindex $argv 0]
} else {
  set fileName [file join [file dirname $path] sqlite-UWP-output.vsix]
}

if {[string length $fileName] == 0} then {
  fail "invalid VSIX file name"
}

if {![file exists $fileName] || ![file isfile $fileName]} then {
  fail [appendArgs "VSIX file \"" $fileName "\" does not exist"]
}

set envVarName VS140COMNTOOLS
set vsDirectory [getEnvironmentVariable $envVarName]

if {[string length $vsDirectory] == 0} then {
  fail [appendArgs \
      "Visual Studio 2015 environment variable \"" $envVarName "\" missing"]
}

if {![file exists $vsDirectory] || ![file isdirectory $vsDirectory]} then {
  fail [appendArgs \
      "Visual Studio 2015 directory \"" $vsDirectory \
      "\" does not exist"]
}

set vsixInstaller [file join [file dirname $vsDirectory] IDE VSIXInstaller.exe]

if {![file exists $vsixInstaller] || ![file isfile $vsixInstaller]} then {
  fail [appendArgs \
      "Visual Studio 2015 VSIX installer \"" $vsixInstaller \
      "\" does not exist"]
}

set envVarName ProgramFiles
set programFiles [getEnvironmentVariable $envVarName]

if {[string length $programFiles] == 0} then {
  fail [appendArgs \
      "Windows environment variable \"" $envVarName "\" missing"]
}

if {![file exists $programFiles] || ![file isdirectory $programFiles]} then {
  fail [appendArgs \
      "Program Files directory \"" $programFiles "\" does not exist"]
}

set msBuild [file join $programFiles MSBuild 14.0 Bin MSBuild.exe]

if {![file exists $msBuild] || ![file isfile $msBuild]} then {
  fail [appendArgs \
      "MSBuild 14.0 executable file \"" $msBuild "\" does not exist"]
}

set temporaryDirectory [getTemporaryPath]

if {[string length $temporaryDirectory] == 0 || \
    ![file exists $temporaryDirectory] || \
    ![file isdirectory $temporaryDirectory]} then {
  fail "cannot locate a usable temporary directory"
}

set installLogFileName [appendArgs \
    [file rootname [file tail $fileName]] -install- [pid] .log]

set buildLogFileName [appendArgs \
    [file rootname [file tail $fileName]] -build- [pid] .log]

set uninstallLogFileName [appendArgs \
    [file rootname [file tail $fileName]] -uninstall- [pid] .log]

set command(1) [list exec [file nativename $vsixInstaller] /quiet /norepair]
lappend command(1) [appendArgs /logFile: $installLogFileName]
lappend command(1) [file nativename $fileName]

set command(2) [list exec [file nativename $msBuild]]
lappend command(2) [file nativename [file join $path vsixtest.sln]]
lappend command(2) /target:Rebuild /property:Configuration=Release

lappend command(2) [appendArgs \
    /logger:FileLogger,Microsoft.Build.Engine\;Logfile= \
    [file nativename [file join $temporaryDirectory $buildLogFileName]] \
    \;Verbosity=diagnostic]

set command(3) [list exec [file nativename $vsixInstaller] /quiet /norepair]
lappend command(3) [appendArgs /logFile: $uninstallLogFileName]
lappend command(3) [appendArgs /uninstall:SQLite.UWP.2015]

puts stdout [appendArgs \
    "Install log will be \"" [file nativename [file join \
    $temporaryDirectory $installLogFileName]] "\"."]

puts stdout [appendArgs \
    "Build log will be \"" [file nativename [file join \
    $temporaryDirectory $buildLogFileName]] "\"."]

puts stdout [appendArgs \
    "Uninstall log will be \"" [file nativename [file join \
    $temporaryDirectory $uninstallLogFileName]] "\"."]

puts stdout [appendArgs \
    "First command is \"" $command(1) "\"."]

puts stdout [appendArgs \
    "Second command is \"" $command(2) "\"."]

puts stdout [appendArgs \
    "Third command is \"" $command(3) "\"."]

# eval exec $command(1)
# eval exec $command(2)
# eval exec $command(3)
