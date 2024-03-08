cmake_minimum_required(VERSION 3.18.0.0)
project(sqlite3mc)

# Helper macro
macro(_Enable_MT _target)
  target_compile_options(${_target} PRIVATE
    $<$<CONFIG:Release>:/MT>$<$<CONFIG:Debug>:/MTd>
  )
endmacro()

set(_SQLITE3MC_REQUIRE_ZLIB OFF)

OPTION(SQLITE3MC_STATIC_RUNTIME_LINK "Link the c++ runtime statically" OFF)
OPTION(SQLITE3MC_STATIC "Build SQLITE3MC as a static library" OFF)
OPTION(SQLITE3MC_BUILD_SHELL "Builds the SQLITE3MC Shell" ON)
OPTION(SQLITE3MC_WITH_ICU "Builds SQLITE3MC with the ICU library. Will use the FindICU function to locate the library. (https://cmake.org/cmake/help/latest/module/FindICU.html)" OFF)

OPTION(SQLITE_ENABLE_DEBUG "Enable SQLite debugging" OFF)
OPTION(SQLITE_SOUNDEX "Enable the soundex() SQL function" ON)
OPTION(SQLITE_ENABLE_COLUMN_METADATA "Enable convenient access to meta-data about tables and queries" ON)
OPTION(SQLITE_SECURE_DELETE "Enable secure delete to overwrite deleted content with zeros" ON)
OPTION(SQLITE_ENABLE_FTS3 "Enable version 3 of the full-text search engine" ON)
OPTION(SQLITE_ENABLE_FTS3_PARENTHESIS "Support operators AND and NOT and nested parenthesis" ON)
OPTION(SQLITE_ENABLE_FTS4 "Enable version 4 of the full-text search engine" ON)
OPTION(SQLITE_ENABLE_FTS5 "Enable version 5 of the full-text search engine" ON)

OPTION(SQLITE_ENABLE_CARRAY "Enable extension 'carray'" ON)
OPTION(SQLITE_ENABLE_CSV "Enable extension 'csv'" ON)
OPTION(SQLITE_ENABLE_EXTFUNC "Enable extension 'extfunc'" ON)
OPTION(SQLITE_ENABLE_GEOPOLY "Enable extension 'geopoly'" ON)
OPTION(SQLITE_ENABLE_JSON1 "Enable extension 'json1'" ON)
OPTION(SQLITE_ENABLE_RTREE "Enable extension 'rtree'" ON)
OPTION(SQLITE_ENABLE_UUID "Enable extension 'uuid'" ON)
OPTION(SQLITE_USE_URI "Enable the URI filename process logic" ON)
OPTION(SQLITE_USER_AUTHENTICATION "Enable extension 'user authentication'" ON)
OPTION(SQLITE_ENABLE_PREUPDATE_HOOK "Enable preupdate hooks" OFF)
OPTION(SQLITE_ENABLE_SESSION "Enable session extension" OFF)
OPTION(SQLITE_SHELL_IS_UTF8 "Shell command line arguments in UTF-8 encoding" ON)

# Options for library only
OPTION(SQLITE_ENABLE_FILEIO "Enable extension 'fileio'" ON)
OPTION(SQLITE_ENABLE_REGEXP "Enable extension 'regexp'" ON)
OPTION(SQLITE_ENABLE_SERIES "Enable extension 'series'" ON)
OPTION(SQLITE_ENABLE_SHA3 "Enable extension 'sha3'" ON)

# Options for shell only (compatibility with official SQLite shell)
OPTION(SQLITE_ENABLE_EXPLAIN_COMMENTS "Insert comment text into the output of EXPLAIN (shell only)" ON)
OPTION(SQLITE_ENABLE_DBPAGE_VTAB "Enable the SQLITE_DBPAGE virtual table (shell only)" ON)
OPTION(SQLITE_ENABLE_DBSTAT_VTAB "Enable the dbstat virtual table (shell only)" ON)
OPTION(SQLITE_ENABLE_STMTVTAB "Enable the SQLITE_STMT virtual table (shell only)" ON)
OPTION(SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION "Suppress 'unknown function' errors when running an EXPLAIN or EXPLAIN QUERY PLAN (shell only)" ON)

# Embedded Compression
OPTION(SQLITE3MC_USE_MINIZ "Use builtin ZLIB replacement" OFF)

# Compression/Options that require ZLIB
OPTION(SQLITE_ENABLE_COMPRESS "Enable extension 'compress'" OFF)
OPTION(SQLITE_ENABLE_SQLAR "Enable extension 'sqlar'" OFF)
OPTION(SQLITE_ENABLE_ZIPFILE "Enable extension 'zipfile'" OFF)

# Legacy Encryption Extensions
OPTION(SQLITE3MC_USE_SQLEET_LEGACY "Use sqleet legacy mode as default" OFF)
OPTION(SQLITE3MC_USE_SQLCIPHER_LEGACY "Use sqlcipher legacy mode as default" OFF)

# Additional memory security (filling freed memory allocations with zeros or random data)
OPTION(SQLITE3MC_SECURE_MEMORY "Enable pragma to secure freed memory" OFF)
OPTION(SQLITE3MC_USE_RANDOM_FILL_MEMORY "Fill freed memory with random data" OFF)

# Omit AES hardware support
OPTION(SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT "Omit AES hardware support" OFF)

# Enable Wasm runtime
OPTION(LIBSQL_ENABLE_WASM_RUNTIME "Enable libSQL Wasm runtime" OFF)

# TCL
#OPTION(SQLITE_ENABLE_TCL "Enable TCL interface" OFF)

set(SQLITE_MAX_ATTACHED "10" CACHE STRING "Max number of attached files (must be in the range 0..125)")

set(SQLITE_DQS "0" CACHE STRING "Allow double-quoted string literals")
set_property(CACHE SQLITE_DQS PROPERTY STRINGS 0 1 2 3)

set(SQLITE_THREADSAFE 1 CACHE STRING "Set threading mode (0 = single-threaded, 1 = serialized, 2 = multi-threaded)")
set_property(CACHE SQLITE_THREADSAFE PROPERTY STRINGS 0 1 2)

set(CODEC_TYPE CHACHA20 CACHE STRING "Set default codec type")
set_property(CACHE CODEC_TYPE PROPERTY STRINGS AES128 AES256 CHACHA20 SQLCIPHER RC4 ASCON128)

if(SQLITE_ENABLE_COMPRESS OR SQLITE_ENABLE_SQLAR OR SQLITE_ENABLE_ZIPFILE)
  if(NOT SQLITE3MC_USE_MINIZ)
    set(_SQLITE3MC_REQUIRE_ZLIB ON)
  endif()
endif()

# Setup the options
set(SQLITE3MC_BASE_DEFINITIONS 
  $<$<BOOL:${SQLITE_SHELL_IS_UTF8}>:SQLITE_SHELL_IS_UTF8=1>
  CODEC_TYPE=CODEC_TYPE_${CODEC_TYPE}
  SQLITE_DQS=${SQLITE_DQS}
  SQLITE_MAX_ATTACHED=${SQLITE_MAX_ATTACHED}
  $<$<BOOL:${SQLITE_USER_AUTHENTICATION}>:SQLITE_USER_AUTHENTICATION=1>
  $<$<BOOL:${SQLITE_ENABLE_DEBUG}>:SQLITE_ENABLE_DEBUG=1>
  SQLITE_THREADSAFE=${SQLITE_THREADSAFE}
  $<$<BOOL:${SQLITE_SOUNDEX}>:SQLITE_SOUNDEX=1>
  $<$<BOOL:${SQLITE_ENABLE_COLUMN_METADATA}>:SQLITE_ENABLE_COLUMN_METADATA=1>
  $<$<BOOL:${SQLITE_SECURE_DELETE}>:SQLITE_SECURE_DELETE=1>
  $<$<BOOL:${SQLITE_ENABLE_FTS3}>:SQLITE_ENABLE_FTS3=1>
  $<$<BOOL:${SQLITE_ENABLE_FTS3_PARENTHESIS}>:SQLITE_ENABLE_FTS3_PARENTHESIS=1>
  $<$<BOOL:${SQLITE_ENABLE_FTS4}>:SQLITE_ENABLE_FTS4=1>
  $<$<BOOL:${SQLITE_ENABLE_FTS5}>:SQLITE_ENABLE_FTS5=1>
  $<$<BOOL:${SQLITE_ENABLE_JSON1}>:SQLITE_ENABLE_JSON1=1>
  $<$<BOOL:${SQLITE_ENABLE_RTREE}>:SQLITE_ENABLE_RTREE=1>
  $<$<BOOL:${SQLITE_ENABLE_GEOPOLY}>:SQLITE_ENABLE_GEOPOLY=1>
  $<$<BOOL:${SQLITE_ENABLE_EXTFUNC}>:SQLITE_ENABLE_EXTFUNC=1>
  $<$<BOOL:${SQLITE_ENABLE_CARRAY}>:SQLITE_ENABLE_CARRAY=1>
  $<$<BOOL:${SQLITE_ENABLE_UUID}>:SQLITE_ENABLE_UUID=1>
  SQLITE_TEMP_STORE=2
  $<$<BOOL:${SQLITE_USE_URI}>:SQLITE_USE_URI=1>
  $<$<BOOL:${SQLITE_USER_AUTHENTICATION}>:SQLITE_USER_AUTHENTICATION=1>
  $<$<BOOL:${SQLITE_ENABLE_PREUPDATE_HOOK}>:SQLITE_ENABLE_PREUPDATE_HOOK=1>
  $<$<BOOL:${SQLITE_ENABLE_SESSION}>:SQLITE_ENABLE_SESSION=1>
  $<$<BOOL:${SQLITE3MC_USE_MINIZ}>:SQLITE3MC_USE_MINIZ=1>
  $<$<BOOL:${SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT}>:SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT=1>
  $<$<BOOL:${LIBSQL_ENABLE_WASM_RUNTIME}>:LIBSQL_ENABLE_WASM_RUNTIME=1>
  LIBSQL_EXTRA_PRAGMAS=1
  LIBSQL_CUSTOM_PAGER_CODEC=1

  SQLITE_ENABLE_DBSTAT_VTAB=1
  SQLITE_ENABLE_DBPAGE_VTAB=1
  SQLITE_ENABLE_STMTVTAB=1
  SQLITE_DEFAULT_FOREIGN_KEYS=1

  HAVE_CIPHER_AES_128_CBC=0
  HAVE_CIPHER_AES_256_CBC=1
  HAVE_CIPHER_CHACHA20=0   
  HAVE_CIPHER_SQLCIPHER=0   
  HAVE_CIPHER_RC4=0   
  HAVE_CIPHER_ASCON128=0
#  $<$<BOOL:${SQLITE_USE_TCL}>:SQLITE_USE_TCL=1>
)

set(SQLITE3MC_LIBONLY_DEFINITIONS 
  $<$<BOOL:${SQLITE_ENABLE_FILEIO}>:SQLITE_ENABLE_FILEIO=1>
  $<$<BOOL:${SQLITE_ENABLE_REGEXP}>:SQLITE_ENABLE_REGEXP=1>
  $<$<BOOL:${SQLITE_ENABLE_SERIES}>:SQLITE_ENABLE_SERIES=1>
  $<$<BOOL:${SQLITE_ENABLE_SHA3}>:SQLITE_ENABLE_SHA3=1>
)

set(SQLITE3MC_ZLIB_DEFINITIONS 
)

set(SQLITE3MC_SHELL_DEFINITIONS 
  $<$<BOOL:${SQLITE_SHELL_IS_UTF8}>:SQLITE_SHELL_IS_UTF8=1>
  $<$<BOOL:${SQLITE_ENABLE_EXPLAIN_COMMENTS}>:SQLITE_ENABLE_EXPLAIN_COMMENTS=1>
  $<$<BOOL:${SQLITE_ENABLE_DBPAGE_VTAB}>:SQLITE_ENABLE_DBPAGE_VTAB=1>
  $<$<BOOL:${SQLITE_ENABLE_DBSTAT_VTAB}>:SQLITE_ENABLE_DBSTAT_VTAB=1>
  $<$<BOOL:${SQLITE_ENABLE_STMTVTAB}>:SQLITE_ENABLE_STMTVTAB=1>
  $<$<BOOL:${SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION}>:SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION=1>
)

if(SQLITE3MC_USE_SQLEET_LEGACY)
  set(SQLITE3MC_BASE_DEFINITIONS 
    ${SQLITE3MC_BASE_DEFINITIONS}
    SQLITE3MC_USE_SQLEET_LEGACY=1
  )
endif()
if(SQLITE3MC_USE_SQLCIPHER_LEGACY)
  if(NOT DEFINED SQLCIPHER_VERSION_DEFAULT)
    set(SQLCIPHER_VERSION_DEFAULT "4")
  endif()
  set(SQLITE3MC_BASE_DEFINITIONS 
    ${SQLITE3MC_BASE_DEFINITIONS}
    SQLITE3MC_USE_SQLCIPHER_LEGACY=1
    SQLCIPHER_VERSION_DEFAULT=${SQLCIPHER_VERSION_DEFAULT}
  )
endif()

if(SQLITE3MC_SECURE_MEMORY)
  set(SQLITE3MC_BASE_DEFINITIONS 
    ${SQLITE3MC_BASE_DEFINITIONS}
    SQLITE3MC_SECURE_MEMORY=1
  )
endif()
if(SQLITE3MC_USE_RANDOM_FILL_MEMORY)
  set(SQLITE3MC_BASE_DEFINITIONS 
    ${SQLITE3MC_BASE_DEFINITIONS}
    SQLITE3MC_USE_RANDOM_FILL_MEMORY=1
  )
endif()

if(SQLITE3MC_USE_MINIZ OR _SQLITE3MC_REQUIRE_ZLIB)
  if(_SQLITE3MC_REQUIRE_ZLIB)
    # Use ZLIB_ROOT to specify the location to look
    find_package(ZLIB)
    if(NOT ZLIB_FOUND)
      message( FATAL_ERROR "One of the flags specified requires zlib. Zlib is not found. Try specifying the ZLIB_ROOT environment variable.")
    endif()
  endif()
  if(SQLITE3MC_USE_MINIZ OR ZLIB_FOUND)
    set(SQLITE3MC_BASE_DEFINITIONS 
      ${SQLITE3MC_BASE_DEFINITIONS}
      $<$<BOOL:${SQLITE_ENABLE_COMPRESS}>:SQLITE_ENABLE_COMPRESS=1>
    )
    set(SQLITE3MC_ZLIB_DEFINITIONS 
      ${SQLITE3MC_ZLIB_DEFINITIONS}
      $<$<BOOL:${SQLITE_ENABLE_SQLAR}>:SQLITE_ENABLE_SQLAR=1>
      $<$<BOOL:${SQLITE_ENABLE_ZIPFILE}>:SQLITE_ENABLE_ZIPFILE=1>
    )
    set(SQLITE3MC_SHELL_DEFINITIONS 
      ${SQLITE3MC_SHELL_DEFINITIONS}
      SQLITE_HAVE_ZLIB=1
    )
  endif()
endif()

if(SQLITE3MC_WITH_ICU)
  find_package(ICU REQUIRED COMPONENTS data i18n io uc)
  if(ICU_FOUND)
    set(SQLITE3MC_ICU_DEFINITIONS
      SQLITE_ENABLE_ICU=1
    )
  else()
    message( FATAL_ERROR "ICU not found.")
  endif()
endif()

# Only generate Debug and Release configuration types.
set(CMAKE_CONFIGURATION_TYPES Debug Release)

set(SQLITE3MC_PUBLIC_HEADERS
  src/sqlite3.h
  src/sqlite3ext.h
  src/sqlite3mc.h
  src/sqlite3mc_version.h
  src/sqlite3mc_vfs.h
  src/sqlite3userauth.h
)

set(SQLITE3MC_BASE_SRCS
  src/sqlite3mc.c
  src/cipher_common.h
  src/cipher_config.h
  src/fastpbkdf2.h
  src/mystdint.h
  src/rijndael.h
  src/sha1.h
  src/sha2.h
  src/test_windirent.h
  ${SQLITE3MC_PUBLIC_HEADERS}
)
set(SQLITE3MC_DLLRES_SRCS
  src/sqlite3mc.rc
)
set(SQLITE3MC_SHELL_SRCS
  src/shell.c
  src/sqlite3mc.c
  src/sqlite3mc_shell.rc
)

set(SQLITE3MC_INCLUDEDIRS
  src
)

set(SQLITE3MC_LINK_LIBRARIES )

set(SQLITE3MC_DLL_DEFINITIONS )

set(SQLITE3MC_SHELL_SYSTEM_LINKS )

if(MSVC)
  set(SQLITE3MC_DLL_DEFINITIONS
    _USRDLL
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux"
    OR CMAKE_SYSTEM_NAME STREQUAL "Darwin")

  # Do not set `-maes -msee4.2` when we are on arm which doesn't support
  # this instruction set.
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64"
      OR CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2 -maes")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -maes")
  endif()
  
  set(SQLITE3MC_LINK_LIBRARIES
    pthread
    dl
    m
  )
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND SQLITE3MC_LINK_LIBRARIES "-framework Security")
  endif()
  set(SHARED_LIB_EXPORT_DEFINITION "__attribute__((visibility(\"default\")))")
else()
  if (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2 -maes")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -maes")
  endif()
  set(SHARED_LIB_EXPORT_DEFINITION "__declspec(dllexport)")
endif()

if (CMAKE_C_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2 -maes -Wno-error=incompatible-function-pointer-types")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -maes")
endif()

set(_LIB_DIFINITIONS
  _LIB
)

set(_DEFAULT_DEFINITIONS
  _CRT_SECURE_NO_WARNINGS
  _CRT_SECURE_NO_DEPRECATE
  _CRT_NONSTDC_NO_WARNINGS
  _CRT_NONSTDC_NO_DEPRECATE
  _UNICODE
  UNICODE
)

set(SQLITE3MC_SHELL_TARGET "sqlite3mc_shell")

if(SQLITE3MC_STATIC)
  set(SQLITE3MC_LINK "STATIC")
  set(SQLITE3MC_TARGET "sqlite3mc_static")
else()
  set(SQLITE3MC_LINK "SHARED")
  set(SQLITE3MC_TARGET "sqlite3mc")
  list(APPEND SQLITE3MC_BASE_SRCS ${SQLITE3MC_DLLRES_SRCS})
endif()

# Lib Project
add_library(${SQLITE3MC_TARGET} ${SQLITE3MC_LINK}
  ${SQLITE3MC_BASE_SRCS}
)
target_include_directories(${SQLITE3MC_TARGET} PRIVATE
  ${SQLITE3MC_INCLUDEDIRS}
)
target_compile_definitions(${SQLITE3MC_TARGET} PRIVATE
  ${_LIB_DIFINITIONS}
  ${_DEFAULT_DEFINITIONS}
  ${SQLITE3MC_BASE_DEFINITIONS}
  ${SQLITE3MC_LIBONLY_DEFINITIONS}
  ${SQLITE3MC_ZLIB_DEFINITIONS}
)
target_link_libraries(${SQLITE3MC_TARGET} PRIVATE
  ${SQLITE3MC_LINK_LIBRARIES}
)

if(NOT SQLITE3MC_STATIC)
  target_compile_definitions(${SQLITE3MC_TARGET} PRIVATE
    SQLITE_API=${SHARED_LIB_EXPORT_DEFINITION}
  )
endif()

if(ZLIB_FOUND)
  target_include_directories(${SQLITE3MC_TARGET} PRIVATE
    ${ZLIB_INCLUDE_DIRS}
  )
  target_link_libraries(${SQLITE3MC_TARGET} PRIVATE
    ${ZLIB_LIBRARIES}
  )
  message(${ZLIB_LIBRARIES})
endif()

if(SQLITE3MC_WITH_ICU)
  target_compile_definitions(${SQLITE3MC_TARGET} PRIVATE
    ${SQLITE3MC_ICU_DEFINITIONS}
  )
  target_include_directories(${SQLITE3MC_TARGET} PRIVATE 
    ${ICU_INCLUDE_DIRS}
  )
  target_link_libraries(${SQLITE3MC_TARGET} PRIVATE
    ${ICU_LIBRARIES}
  )
  message("Will build ${SQLITE3MC_TARGET} with ICU")
endif()

if(SQLITE3MC_STATIC_RUNTIME_LINK)
  message("Will build ${SQLITE3MC_TARGET} with static runtime link")
  _Enable_MT(${SQLITE3MC_TARGET})
endif()
message("Will build ${SQLITE3MC_TARGET} as ${SQLITE3MC_LINK}")

set_target_properties(${SQLITE3MC_TARGET} PROPERTIES PUBLIC_HEADER "${SQLITE3MC_PUBLIC_HEADERS}")
INSTALL(TARGETS ${SQLITE3MC_TARGET}
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION include/${CMAKE_PROJECT_NAME}
  RUNTIME DESTINATION bin
)

# Shell Executable project
if(SQLITE3MC_BUILD_SHELL)
  add_executable(${SQLITE3MC_SHELL_TARGET}
    ${SQLITE3MC_SHELL_SRCS}
  )
  target_include_directories(${SQLITE3MC_SHELL_TARGET} PRIVATE
    ${SQLITE3MC_INCLUDEDIRS}
  )
  target_compile_definitions(${SQLITE3MC_SHELL_TARGET} PRIVATE
    ${_DEFAULT_DEFINITIONS}
    ${SQLITE3MC_BASE_DEFINITIONS}
    ${SQLITE3MC_SHELL_DEFINITIONS}
  )
  target_link_libraries(${SQLITE3MC_SHELL_TARGET} PRIVATE
    ${SQLITE3MC_SHELL_SYSTEM_LINKS}
    ${SQLITE3MC_LINK_LIBRARIES}
  )
  if(ZLIB_FOUND)
    target_include_directories(${SQLITE3MC_SHELL_TARGET} PRIVATE
      ${ZLIB_INCLUDE_DIRS}
    )
    target_link_libraries(${SQLITE3MC_SHELL_TARGET} PRIVATE
      ${ZLIB_LIBRARIES}
    )
  endif()

  if(SQLITE3MC_WITH_ICU)
    target_compile_definitions(${SQLITE3MC_SHELL_TARGET} PRIVATE
      ${SQLITE3MC_ICU_DEFINITIONS}
    )
    target_include_directories(${SQLITE3MC_SHELL_TARGET} PRIVATE 
      ${ICU_INCLUDE_DIRS}
    )
    target_link_libraries(${SQLITE3MC_SHELL_TARGET} PRIVATE
      ${ICU_LIBRARIES}
    )
    message("Will build ${SQLITE3MC_SHELL_TARGET} with ICU")
  endif()

  if(SQLITE3MC_RUNTIME_LINK)
    message("Will build ${SQLITE3MC_SHELL_TARGET} with static runtime")
    _Enable_MT(${SQLIT3MC_SHELL_TARGET})
  endif()
  message("Will build ${SQLITE3MC_SHELL_TARGET}")

  INSTALL(TARGETS ${SQLITE3MC_SHELL_TARGET}
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    PUBLIC_HEADER DESTINATION include/${CMAKE_PROJECT_NAME}
    RUNTIME DESTINATION bin
  )
endif()
