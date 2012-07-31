@ECHO OFF

::
:: build-all-msvc.bat --
::
:: Multi-Platform Build Tool for MSVC
::

SETLOCAL

REM SET __ECHO=ECHO
REM SET __ECHO2=ECHO
IF NOT DEFINED _AECHO (SET _AECHO=REM)
IF NOT DEFINED _CECHO (SET _CECHO=REM)
IF NOT DEFINED _VECHO (SET _VECHO=REM)

%_AECHO% Running %0 %*

REM SET DFLAGS=/L

%_VECHO% DFlags = '%DFLAGS%'

SET FFLAGS=/V /F /G /H /I /R /Y /Z

%_VECHO% FFlags = '%FFLAGS%'

SET ROOT=%~dp0\..
SET ROOT=%ROOT:\\=\%

%_VECHO% Root = '%ROOT%'

REM
REM NOTE: The first and only argument to this batch file should be the output
REM       directory where the platform-specific binary directories should be
REM       created.
REM
SET BINARYDIRECTORY=%1

IF NOT DEFINED BINARYDIRECTORY (
  GOTO usage
)

%_VECHO% BinaryDirectory = '%BINARYDIRECTORY%'

SET DUMMY=%2

IF DEFINED DUMMY (
  GOTO usage
)

REM
REM NOTE: From this point, we need a clean error level.  Reset it now.
REM
CALL :fn_ResetErrorLevel

REM
REM NOTE: Change the current directory to the root of the source tree, saving
REM       the current directory on the directory stack.
REM
%__ECHO2% PUSHD "%ROOT%"

IF ERRORLEVEL 1 (
  ECHO Could not change directory to "%ROOT%".
  GOTO errors
)

REM
REM NOTE: This batch file requires the ComSpec environment variable to be set,
REM       typically to something like "C:\Windows\System32\cmd.exe".
REM
IF NOT DEFINED ComSpec (
  ECHO The ComSpec environment variable must be defined.
  GOTO errors
)

REM
REM NOTE: This batch file requires the VcInstallDir environment variable to be
REM       set.  Tyipcally, this means this batch file needs to be run from an
REM       MSVC command prompt.
REM
IF NOT DEFINED VCINSTALLDIR (
  ECHO The VCINSTALLDIR environment variable must be defined.
  GOTO errors
)

REM
REM NOTE: If the list of platforms is not already set, use the default list.
REM
IF NOT DEFINED PLATFORMS (
  SET PLATFORMS=x86 x86_amd64 x86_arm
)

%_VECHO% Platforms = '%PLATFORMS%'

REM
REM NOTE: Setup environment variables to translate between the MSVC platform
REM       names and the names to be used for the platform-specific binary
REM       directories.
REM
SET x86_NAME=x86
SET x86_amd64_NAME=x64
SET x86_arm_NAME=ARM

%_VECHO% x86_Name = '%x86_NAME%'
%_VECHO% x86_amd64_Name = '%x86_amd64_NAME%'
%_VECHO% x86_arm_Name = '%x86_arm_NAME%'

REM
REM NOTE: Check for the external tools needed during the build process ^(i.e.
REM       those that do not get compiled as part of the build process itself^)
REM       along the PATH.
REM
FOR %%T IN (gawk.exe tclsh85.exe) DO (
  SET %%T_PATH=%%~dp$PATH:T
)

REM
REM NOTE: Set the TOOLPATH variable to contain all the directories where the
REM       external tools were found in the search above.
REM
SET TOOLPATH=%gawk.exe_PATH%;%tclsh85.exe_PATH%

%_VECHO% ToolPath = '%TOOLPATH%'

REM
REM NOTE: Check for MSVC 2012 because the Windows SDK directory handling is
REM       slightly different for that version.
REM
IF "%VisualStudioVersion%" == "11.0" (
  SET SET_NSDKLIBPATH=1
) ELSE (
  CALL :fn_UnsetVariable SET_NSDKLIBPATH
)

REM
REM NOTE: This is the outer loop.  There should be exactly one iteration per
REM       platform.
REM
FOR %%P IN (%PLATFORMS%) DO (
  REM
  REM NOTE: Using the MSVC platform name, lookup the simpler platform name to
  REM       be used for the name of the platform-specific binary directory via
  REM       the environment variables setup earlier.
  REM
  CALL :fn_SetVariable %%P_NAME PLATFORMNAME

  REM
  REM NOTE: This is the inner loop.  There should be exactly one iteration.
  REM       This loop is necessary because the PlatformName environment
  REM       variable was set above and that value is needed by some of the
  REM       commands contained in the inner loop.  If these commands were
  REM       directly contained in the outer loop, the PlatformName environment
  REM       variable would be stuck with its initial empty value instead.
  REM
  FOR /F "tokens=2* delims==" %%D IN ('SET PLATFORMNAME') DO (
    REM
    REM NOTE: Attempt to clean the environment of all variables used by MSVC
    REM       and/or Visual Studio.  This block may need to be updated in the
    REM       future to account for additional environment variables.
    REM
    CALL :fn_UnsetVariable DevEnvDir
    CALL :fn_UnsetVariable ExtensionSdkDir
    CALL :fn_UnsetVariable Framework35Version
    CALL :fn_UnsetVariable FrameworkDir
    CALL :fn_UnsetVariable FrameworkDir32
    CALL :fn_UnsetVariable FrameworkVersion
    CALL :fn_UnsetVariable FrameworkVersion32
    CALL :fn_UnsetVariable FSHARPINSTALLDIR
    CALL :fn_UnsetVariable INCLUDE
    CALL :fn_UnsetVariable LIB
    CALL :fn_UnsetVariable LIBPATH
    CALL :fn_UnsetVariable Platform
    REM CALL :fn_UnsetVariable VCINSTALLDIR
    CALL :fn_UnsetVariable VSINSTALLDIR
    CALL :fn_UnsetVariable WindowsSdkDir
    CALL :fn_UnsetVariable WindowsSdkDir_35
    CALL :fn_UnsetVariable WindowsSdkDir_old

    REM
    REM NOTE: Reset the PATH here to the absolute bare minimum required.
    REM
    SET PATH=%TOOLPATH%;%SystemRoot%\System32;%SystemRoot%

    REM
    REM NOTE: Launch a nested command shell to perform the following steps:
    REM
    REM       1. Setup the MSVC environment for this platform using the
    REM          official batch file.
    REM
    REM       2. Make sure that no stale build output files are present.
    REM
    REM       3. Build the "sqlite3.dll" and "sqlite3.lib" binaries for this
    REM          platform.
    REM
    REM       4. Copy the "sqlite3.dll" and "sqlite3.lib" binaries for this
    REM          platform to the platform-specific directory beneath the
    REM          binary directory.
    REM
    "%ComSpec%" /C (
      REM
      REM NOTE: Attempt to setup the MSVC environment for this platform.
      REM
      %__ECHO% CALL "%VCINSTALLDIR%\vcvarsall.bat" %%P

      IF ERRORLEVEL 1 (
        ECHO Failed to call "%VCINSTALLDIR%\vcvarsall.bat" for platform %%P.
        GOTO errors
      )

      REM
      REM NOTE: If this batch file is not running in "what-if" mode, check to
      REM       be sure we were actually able to setup the MSVC environment as
      REM       current versions of their official batch file do not set the
      REM       exit code upon failure.
      REM
      IF NOT DEFINED __ECHO (
        IF NOT DEFINED WindowsSdkDir (
          ECHO Cannot build, Windows SDK not found for platform %%P.
          GOTO errors
        )
      )

      REM
      REM NOTE: When using MSVC 2012, the native SDK path cannot simply use
      REM       the "lib" sub-directory beneath the location specified in the
      REM       WindowsSdkDir environment variable because that location does
      REM       not actually contain the necessary library files for x86.
      REM       This must be done for each iteration because it relies upon
      REM       the WindowsSdkDir environment variable being set by the batch
      REM       file used to setup the MSVC environment.
      REM
      IF DEFINED SET_NSDKLIBPATH (
        CALL :fn_SetVariable WindowsSdkDir NSDKLIBPATH
        CALL :fn_AppendVariable NSDKLIBPATH \lib\win8\um\x86
      )

      REM
      REM NOTE: Unless prevented from doing so, invoke NMAKE with the MSVC
      REM       makefile to clean any stale build output from previous
      REM       iterations of this loop and/or previous runs of this batch
      REM       file, etc.
      REM
      IF NOT DEFINED NOCLEAN (
        %__ECHO% nmake -f Makefile.msc clean

        IF ERRORLEVEL 1 (
          ECHO Failed to clean for platform %%P.
          GOTO errors
        )
      ) ELSE (
        REM
        REM NOTE: Even when the cleaning step has been disabled, we still need
        REM       to remove the build output for the files we are specifically
        REM       wanting to build for each platform.
        REM
        %__ECHO% DEL /Q sqlite3.dll sqlite3.lib sqlite3.pdb
      )

      REM
      REM NOTE: Invoke NMAKE with the MSVC makefile to build the "sqlite3.dll"
      REM       binary.  The x86 compiler will be used to compile the native
      REM       command line tools needed during the build process itself.
      REM       Also, disable looking for and/or linking to the native Tcl
      REM       runtime library.
      REM
      %__ECHO% nmake -f Makefile.msc sqlite3.dll "NCC=""%VCINSTALLDIR%\bin\cl.exe""" USE_NATIVE_LIBPATHS=1 NO_TCL=1 %NMAKE_ARGS%

      IF ERRORLEVEL 1 (
        ECHO Failed to build "sqlite3.dll" for platform %%P.
        GOTO errors
      )

      REM
      REM NOTE: Copy the "sqlite3.dll" file to the platform-specific directory
      REM       beneath the binary directory.
      REM
      %__ECHO% XCOPY sqlite3.dll "%BINARYDIRECTORY%\%%D\" %FFLAGS% %DFLAGS%

      IF ERRORLEVEL 1 (
        ECHO Failed to copy "sqlite3.dll" to "%BINARYDIRECTORY%\%%D\".
        GOTO errors
      )

      REM
      REM NOTE: Copy the "sqlite3.lib" file to the platform-specific directory
      REM       beneath the binary directory.
      REM
      %__ECHO% XCOPY sqlite3.lib "%BINARYDIRECTORY%\%%D\" %FFLAGS% %DFLAGS%

      IF ERRORLEVEL 1 (
        ECHO Failed to copy "sqlite3.lib" to "%BINARYDIRECTORY%\%%D\".
        GOTO errors
      )

      REM
      REM NOTE: Copy the "sqlite3.pdb" file to the platform-specific directory
      REM       beneath the binary directory unless we are prevented from doing
      REM       so.
      REM
      IF NOT DEFINED NOSYMBOLS (
        %__ECHO% XCOPY sqlite3.pdb "%BINARYDIRECTORY%\%%D\" %FFLAGS% %DFLAGS%

        IF ERRORLEVEL 1 (
          ECHO Failed to copy "sqlite3.pdb" to "%BINARYDIRECTORY%\%%D\".
          GOTO errors
        )
      )
    )
  )

  REM
  REM NOTE: Handle any errors generated during the nested command shell.
  REM
  IF ERRORLEVEL 1 (
    GOTO errors
  )
)

REM
REM NOTE: Restore the saved current directory from the directory stack.
REM
%__ECHO2% POPD

IF ERRORLEVEL 1 (
  ECHO Could not restore directory.
  GOTO errors
)

REM
REM NOTE: If we get to this point, we have succeeded.
REM
GOTO no_errors

:fn_ResetErrorLevel
  VERIFY > NUL
  GOTO :EOF

:fn_SetErrorLevel
  VERIFY MAYBE 2> NUL
  GOTO :EOF

:fn_SetVariable
  SETLOCAL
  IF NOT DEFINED %1 GOTO :EOF
  IF "%2" == "" GOTO :EOF
  SET __ECHO_CMD=ECHO %%%1%%
  FOR /F "delims=" %%V IN ('%__ECHO_CMD%') DO (
    SET VALUE=%%V
  )
  ENDLOCAL && SET %2=%VALUE%
  GOTO :EOF

:fn_UnsetVariable
  IF NOT "%1" == "" (
    SET %1=
    CALL :fn_ResetErrorLevel
  )
  GOTO :EOF

:fn_AppendVariable
  SET __ECHO_CMD=ECHO %%%1%%
  IF DEFINED %1 (
    FOR /F "delims=" %%V IN ('%__ECHO_CMD%') DO (
      SET %1=%%V%~2
    )
  ) ELSE (
    SET %1=%~2
  )
  SET __ECHO_CMD=
  CALL :fn_ResetErrorLevel
  GOTO :EOF

:usage
  ECHO.
  ECHO Usage: %~nx0 ^<binaryDirectory^>
  ECHO.
  GOTO errors

:errors
  CALL :fn_SetErrorLevel
  ENDLOCAL
  ECHO.
  ECHO Failure, errors were encountered.
  GOTO end_of_file

:no_errors
  CALL :fn_ResetErrorLevel
  ENDLOCAL
  ECHO.
  ECHO Success, no errors were encountered.
  GOTO end_of_file

:end_of_file
%__ECHO% EXIT /B %ERRORLEVEL%
