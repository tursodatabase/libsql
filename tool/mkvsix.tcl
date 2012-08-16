#!/usr/bin/tclsh
#
# This script is used to generate a VSIX (Visual Studio Extension) file for
# SQLite usable by Visual Studio.

proc fail { {error ""} {usage false} } {
  if {[string length $error] > 0} then {
    puts stdout $error
    if {!$usage} then {exit 1}
  }

  puts stdout "usage:\
[file tail [info nameofexecutable]]\
[file tail [info script]] <binaryDirectory> \[sourceDirectory\]"

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

proc readFile { fileName } {
  #
  # NOTE: Reads and returns the entire contents of the specified file, which
  #       may contain binary data.
  #
  set file_id [open $fileName RDONLY]
  fconfigure $file_id -encoding binary -translation binary
  set result [read $file_id]
  close $file_id
  return $result
}

proc writeFile { fileName data } {
  #
  # NOTE: Writes the entire contents of the specified file, which may contain
  #       binary data.
  #
  set file_id [open $fileName {WRONLY CREAT TRUNC}]
  fconfigure $file_id -encoding binary -translation binary
  puts -nonewline $file_id $data
  close $file_id
  return ""
}

proc substFile { fileName } {
  #
  # NOTE: Performs all Tcl command, variable, and backslash substitutions in
  #       the specified file and then re-writes the contents of that same file
  #       with the substituted data.
  #
  return [writeFile $fileName [uplevel 1 [list subst [readFile $fileName]]]]
}

proc replacePlatform { fileName platformName } {
  #
  # NOTE: Returns the specified file name containing the platform name instead
  #       of platform placeholder tokens.
  #
  return [string map [list <platform> $platformName] $fileName]
}

set script [file normalize [info script]]

if {[string length $script] == 0} then {
  fail "script file currently being evaluated is unknown" true
}

set path [file dirname $script]
set rootName [file rootname [file tail $script]]

###############################################################################

#
# NOTE: Process and verify all the command line arguments.
#
set argc [llength $argv]
if {$argc != 1 && $argc != 2} then {fail}

set binaryDirectory [lindex $argv 0]

if {[string length $binaryDirectory] == 0} then {
  fail "invalid binary directory"
}

if {![file exists $binaryDirectory] || \
    ![file isdirectory $binaryDirectory]} then {
  fail "binary directory does not exist"
}

if {$argc == 2} then {
  set sourceDirectory [lindex $argv 1]
} else {
  #
  # NOTE: Assume that the source directory is the parent directory of the one
  #       that contains this script file.
  #
  set sourceDirectory [file dirname $path]
}

if {[string length $sourceDirectory] == 0} then {
  fail "invalid source directory"
}

if {![file exists $sourceDirectory] || \
    ![file isdirectory $sourceDirectory]} then {
  fail "source directory does not exist"
}

###############################################################################

#
# NOTE: Evaluate the user-specific customizations file, if it exists.
#
set userFile [file join $path [appendArgs \
    $rootName . $tcl_platform(user) .tcl]]

if {[file exists $userFile] && \
    [file isfile $userFile]} then {
  source $userFile
}

###############################################################################

set templateFile [file join $path win sqlite.vsix]

if {![file exists $templateFile] || \
    ![file isfile $templateFile]} then {
  fail [appendArgs "template file \"" $templateFile "\" does not exist"]
}

set currentDirectory [pwd]
set outputFile [file join $currentDirectory sqlite-output.vsix]

if {[file exists $outputFile]} then {
  fail [appendArgs "output file \"" $outputFile "\" already exists"]
}

###############################################################################

#
# NOTE: Make sure that a valid temporary directory exists.
#
set temporaryDirectory [getTemporaryPath]

if {[string length $temporaryDirectory] == 0 || \
    ![file exists $temporaryDirectory] || \
    ![file isdirectory $temporaryDirectory]} then {
  fail "cannot locate a usable temporary directory"
}

#
# NOTE: Setup the staging directory to have a unique name inside of the
#       configured temporary directory.
#
set stagingDirectory [file normalize [file join $temporaryDirectory \
    [appendArgs $rootName . [pid]]]]

###############################################################################

#
# NOTE: Configure the external zipping tool.  First, see if it has already
#       been pre-configured.  If not, try to query it from the environment.
#       Finally, fallback on the default of simply "zip", which will then
#       be assumed to exist somewhere along the PATH.
#
if {![info exists zip]} then {
  if {[info exists env(ZipTool)]} then {
    set zip $env(ZipTool)
  }
  if {![info exists zip] || ![file exists $zip]} then {
    set zip zip
  }
}

#
# NOTE: Configure the external unzipping tool.  First, see if it has already
#       been pre-configured.  If not, try to query it from the environment.
#       Finally, fallback on the default of simply "unzip", which will then
#       be assumed to exist somewhere along the PATH.
#
if {![info exists unzip]} then {
  if {[info exists env(UnZipTool)]} then {
    set unzip $env(UnZipTool)
  }
  if {![info exists unzip] || ![file exists $unzip]} then {
    set unzip unzip
  }
}

###############################################################################

#
# NOTE: Attempt to extract the SQLite version from the "sqlite3.h" header file
#       in the source directory.  This script assumes that the header file has
#       already been generated by the build process.
#
set pattern {^#define\s+SQLITE_VERSION\s+"(.*)"$}
set data [readFile [file join $sourceDirectory sqlite3.h]]

if {![regexp -line -- $pattern $data dummy version]} then {
  fail [appendArgs "cannot locate SQLITE_VERSION value in \"" \
      [file join $sourceDirectory sqlite3.h] \"]
}

###############################################################################

#
# NOTE: Setup the master file list data, including the necessary flags.
#
if {![info exists fileNames(source)]} then {
  set fileNames(source) [list "" "" "" \
      [file join $sourceDirectory sqlite3.h] \
      [file join $binaryDirectory <platform> sqlite3.lib] \
      [file join $binaryDirectory <platform> sqlite3.dll]]

  if {![info exists no(symbols)]} then {
    lappend fileNames(source) \
        [file join $binaryDirectory <platform> sqlite3.pdb]
  }
}

if {![info exists fileNames(destination)]} then {
  set fileNames(destination) [list \
      [file join $stagingDirectory extension.vsixmanifest] \
      [file join $stagingDirectory SDKManifest.xml] \
      [file join $stagingDirectory DesignTime CommonConfiguration \
          <platform> SQLite.WinRT.props] \
      [file join $stagingDirectory DesignTime CommonConfiguration \
          <platform> sqlite3.h] \
      [file join $stagingDirectory DesignTime CommonConfiguration \
          <platform> sqlite3.lib] \
      [file join $stagingDirectory Redist CommonConfiguration \
          <platform> sqlite3.dll]]

  if {![info exists no(symbols)]} then {
    lappend fileNames(destination) \
        [file join $stagingDirectory Redist Debug \
            <platform> sqlite3.pdb]
  }
}

if {![info exists fileNames(neutral)]} then {
  set fileNames(neutral) [list 1 1 1 1 0 0]

  if {![info exists no(symbols)]} then {
    lappend fileNames(neutral) 0
  }
}

if {![info exists fileNames(subst)]} then {
  set fileNames(subst) [list 1 1 1 0 0 0]

  if {![info exists no(symbols)]} then {
    lappend fileNames(subst) 0
  }
}

###############################################################################

#
# NOTE: Setup the list of platforms supported by this script.
#
if {![info exists platformNames]} then {
  set platformNames [list x86 x64 ARM]
}

###############################################################################

#
# NOTE: Make sure the staging directory exists, creating it if necessary.
#
file mkdir $stagingDirectory

#
# NOTE: Build the Tcl command used to extract the template package to the
#       staging directory.
#
set extractCommand [list exec -- $unzip $templateFile -d $stagingDirectory]

#
# NOTE: Extract the template package to the staging directory.
#
eval $extractCommand

###############################################################################

#
# NOTE: Process each file in the master file list.  There are actually four
#       parallel lists that contain the source file names, destination file
#       names, the platform-neutral flags, and the use-subst flags.  When the
#       platform-neutral flag is non-zero, the file is not platform-specific.
#       When the use-subst flag is non-zero, the file is considered to be a
#       text file that may contain Tcl variable and/or command replacements,
#       to be dynamically replaced during processing.  If the source file name
#       is an empty string, then the destination file name will be assumed to
#       already exist in the staging directory and will not be copied; however,
#       dynamic replacements may still be performed on the destination file
#       prior to the package being re-zipped.
#
foreach sourceFileName $fileNames(source) \
    destinationFileName $fileNames(destination) \
    isNeutral $fileNames(neutral) useSubst $fileNames(subst) {
  #
  # NOTE: If the current file is platform-neutral, then only one platform will
  #       be processed for it, namely "neutral"; otherwise, each supported
  #       platform will be processed for it individually.
  #
  foreach platformName [expr {$isNeutral ? [list neutral] : $platformNames}] {
    #
    # NOTE: Use the actual platform name in the destination file name.
    #
    set newDestinationFileName [replacePlatform $destinationFileName \
        $platformName]

    #
    # NOTE: Does the source file need to be copied to the destination file?
    #
    if {[string length $sourceFileName] > 0} then {
      #
      # NOTE: First, make sure the destination directory exists.
      #
      file mkdir [file dirname $newDestinationFileName]

      #
      # NOTE: Then, copy the source file to the destination file verbatim.
      #
      file copy [replacePlatform $sourceFileName $platformName] \
          $newDestinationFileName
    }

    #
    # NOTE: Does the destination file contain dynamic replacements that must
    #       be processed now?
    #
    if {$useSubst} then {
      #
      # NOTE: Perform any dynamic replacements contained in the destination
      #       file and then re-write it in-place.
      #
      substFile $newDestinationFileName
    }
  }
}

###############################################################################

#
# NOTE: Change the current directory to the staging directory so that the
#       external archive building tool can pickup the necessary files using
#       relative paths.
#
cd $stagingDirectory

#
# NOTE: Build the Tcl command used to archive the final package in the
#       output directory.
#
set archiveCommand [list exec -- $zip -r $outputFile *]

#
# NOTE: Build the final package archive in the output directory.
#
eval $archiveCommand

#
# NOTE: Change back to the previously saved current directory.
#
cd $currentDirectory

#
# NOTE: Cleanup the temporary staging directory.
#
file delete -force $stagingDirectory

###############################################################################

#
# NOTE: Success, emit the fully qualified path of the generated VSIX file.
#
puts stdout $outputFile
