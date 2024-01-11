# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.8.1] - 2023-12-02

### Changed

- Based on SQLite version 3.44.2
- Applied several modifications to improve support for SQLite3 WASM

### Fixed

- Fixed issue [#133](../../issues/133)) - missing API symbols

## [1.8.0] - 2023-11-23

### Added

- Added new cipher scheme Ascon-128

### Changed

- Based on SQLite version 3.44.1
- Updated CMake support

### Fixed

- Fixed issue [#126](../../issues/126)) - use of quadmath in VSV extension. Use of 128-bit floats for conversion purposes in the VSV extension could cause compilation problems due to the dependency on the GCC-specific quadmath library. This library will now only be used, if the preprocessor symbol `SQLITE_USE_QUADMATH` is defined. This symbol will not be defined by default.

## [1.7.4] - 2023-11-09

### Changed

- Based on SQLite version 3.44.0
- Prevent crashes due to uninitialized cipher tables

## [1.7.3] - 2023-11-05

### Changed

- Based on SQLite version 3.44.0

## [1.7.2] - 2023-10-11

### Changed

- Based on SQLite version 3.43.2

## [1.7.1] - 2023-10-09

### Added

- Added compile time option to omit AES hardware support

### Fixed

- Fixed autoconf/automake build files to be usable with msys/mingw

## [1.7.0] - 2023-10-03

### Added

- Added `PRAGMA memory_security` to allow to clear memory before it is freed. This feature can have a considerable impact on performance and is therefore disabled by default.

### Fixed

- Fixed issue [#118](../../issues/118)) - tvOS/watchOS compilation errors. On Apple platforms the function `SecRandomCopyBytes()` will now be used instead of `getentropy()`.
- Fixed issue [#119](../../issues/119)) - `PRAGMA mmap_size` conflicts with encrypted databases, a check has been added to allow this pragma for unencrypted databases.

## [1.6.5] - 2023-09-14

### Changed

- Based on SQLite version 3.43.1

## [1.6.4] - 2023-08-25

### Changed

- Based on SQLite version 3.43.0

## [1.6.3] - 2023-05-18

### Changed

- Based on SQLite version 3.42.0
- Enabled session extension

### Fixed

- Fixed incorrect patch of SQLite shell source

## [1.6.2] - 2023-03-23

### Changed

- Based on SQLite version 3.41.2

## [1.6.1] - 2023-03-14

### Changed

- Based on SQLite version 3.41.1
- Symbol `MAX_PATHNAME` (used on Unix-like platforms) has a fixed value of **512** in the original SQLite source code. This can now be configured at compile time to use a higher value (like **4096** - which is supported by most Linux variants) (see issue [#104](../../issues/104)). Use symbol `SQLITE3MC_MAX_PATHNAME` to define a higher value.

## [1.6.0] - 2023-02-23

### Changed

- Based on SQLite version 3.41.0
- Added CMake build support (thanks to [@lwttai](https://github.com/lwttai) and [@jammerxd](https://github.com/jammerxd)

### Added

- Added automatic VFS shim instantiation (see issue [#104](../../issues/104))
  To enable encryption support for a non-default VFS it is now enough to specify the name of the requested real VFS with the prefix **multipleciphers-**, either via the URI parameter `vfs` or via the 4th parameter of the SQLite API function `sqlite3_open_v2()`.

## [1.5.5] - 2022-12-29

### Changed

- Based on SQLite version 3.40.1

## [1.5.4] - 2022-11-19

### Changed

- Based on SQLite version 3.40.0

### Fixed

- Issue [#91](../../issues/91): Android NDK build error
- Issue [#92](../../issues/92): iOS build error

## [1.5.3] - 2022-09-30

### Changed

- Based on SQLite version 3.39.4

## [1.5.2] - 2022-09-08

### Changed

- Based on SQLite version 3.39.3

### Fixed

- Fixed retrieval of configuration parameter table (issue [#90](../../issues/90))

## [1.5.1] - 2022-09-08

:warning:️ **Important** :warning:️

This version and version **1.5.0** have a bug in the code for retrieval of the cipher configuration parameter table, leading to a crash on activating encryption for a database connection (see issue [#90](../../issues/90)). **Only builds that _omit_ some of the builtin cipher schemes are affected.**

### Fixed

  - Fixed a bug in shutdown code

## [1.5.0] - 2022-09-06

:warning:️ **Important** :warning:️

This version contains a bug in the shutdown code that leads to a crash on invoking `sqlite3_shutdown`.

### Changed

- Based on SQLite version 3.39.3
- Eliminated a few compile time warnings
- Improved error messages from `sqlite3_rekey`

### Added

- Added option to register cipher schemes dynamically
- Added WebAssembly target support (issues #88, #89)

## [1.4.8] - 2022-07-26

### Changed

- Based on SQLite version 3.39.2

### Fixed

- Issue [#85](../../issues/85): `PRAGMA rekey` could cause a crash

## [1.4.7] - 2022-07-21

### Changed

- Based on SQLite version 3.39.2

## [1.4.6] - 2022-07-14

### Changed

- Based on SQLite version 3.39.1

## [1.4.5] - 2022-07-02

### Changed

- Based on SQLite version 3.39.0
- Enabled preupdate hooks in build files

## [1.4.4] - 2022-05-16

### Changed

- Based on SQLite version 3.38.5

### Added

- Added optional extensions COMPRESS, SQLAR, and ZIPFILE
- Added optional TCL support (source code only)

## [1.4.3] - 2022-05-07

### Changed

- Based on SQLite version 3.38.5

## [1.4.2] - 2022-04-27

### Changed

- Based on SQLite version 3.38.3

## [1.4.1] - 2022-04-27

### Fixed

- Issue [#74](../../issues/74) (only debug builds are affected)

## [1.4.0] - 2022-04-27

### Changed

- Based on SQLite version 3.38.2

### Fixed

- Removed global VFS structure to resolve issue [#73](../../issues/73)

## [1.3.10] - 2022-03-28

### Changed

- Based on SQLite version 3.38.2

### Added

- Added pragma hexkey/hexrekey (resolving issue [#70](../../issues/70))

## [1.3.9] - 2022-03-15

### Changed

- Based on SQLite version 3.38.1

## [1.3.8] - 2022-02-24

### Changed

- Based on SQLite version 3.38.0
- Updated build files (JSON extension is now integral part of SQLite)

### Fixed

- Eliminated compile time warning (issue #66)

## [1.3.7] - 2022-01-08

### Changed

- Based on SQLite version 3.37.2

## [1.3.6] - 2022-01-01

### Changed

- Based on SQLite version 3.37.1

## [1.3.5] - 2021-11-29

### Changed

- Based on SQLite version 3.37.0
- Added build support for Visual C++ 2022
- Applied minor adjustments to ChaCha20 implementation (taken from upstream resilar/sqleet)
- The SQLite3 Multiple Ciphers version information is now exposed in the amalgamation header
- The compile-time configuration options have been moved to a separate header file

### Fixed

- Issue [#55](../../issues/55): Set pager error state on reporting decrypt error condition to avoid assertion when SQLITE_DEBUG is defined
- Issue [#54](../../issues/54): Check definition of symbol `__QNX__` to support compilation for QNX
- Issues [#50](../../issues/50) and [#51](../../issues/51): Numeric cipher ids are now handled correctly, if some of the cipher schemes are excluded from compilation

## [1.3.4] - 2021-07-24

### Changed

- Allow empty passphrase for `PRAGMA key`
- Allow to fully disable including of user authentication by defining `SQLITE_USER_AUTHENTICATION=0`

## [1.3.3] - 2021-06-19

### Changed

- Based on SQLite version 3.36.0

## [1.3.2] - 2021-05-14

:warning:️ **Important Information when operating SQLite in WAL journal mode** :warning:️

To allow concurrent use of SQLite databases in WAL journal mode with legacy encryption implementations like [System.Data.SQLite](https://system.data.sqlite.org) or [SQLCipher](https://www.zetetic.net/sqlcipher/) a new WAL journal encryption implementation was introduced in _SQLite Multiple Ciphers version **1.3.0**_. 

Unfortunately, WAL journals left behind by versions <= 1.2.5 are not compatible with this new implementation. To be able to access WAL journals created by prior versions, the configuration parameter `mc_legacy_wal` was introduced. If the parameter is set to 1, then the prior WAL journal encryption mode is used. The default of this parameter can be set at compile time by setting the symbol `SQLITE3MC_LEGACY_WAL` accordingly, but the actual value can also be set at runtime using the pragma or the URI parameter `mc_legacy_wal`.

In principle, operating generally in WAL legacy mode is possible, but it is strongly recommended to use the WAL legacy mode only to recover WAL journals left behind by prior versions without data loss.

### Added

- Added configuration parameter `mc_legacy_wal` (issue #40)

### Fixed

- Issue [#39](../../issues/39): Corrupted WAL journal due to referencing the wrong codec pointer

## [1.3.1] - 2021-04-28

:stop_sign: **Attention** :stop_sign:

As described in issue [#39](../../issues/39) using SQLite in _WAL journal mode_ is broken in this version.

### Changed

- Prevent rekey in WAL journal mode, because performing a rekeying operation (`PRAGMA rekey`) in WAL journal mode could cause database corruption.

### Fixed

- Fix issue in user authentication extension that prevented VACUUMing or rekeying

## [1.3.0] - 2021-04-23

:stop_sign: **Attention** :stop_sign:

As described in issue [#39](../../issues/39) using SQLite in _WAL journal mode_ is broken in this version.

### Changed

- Based on SQLite version 3.35.5
- Adjusted build files for MinGW
  The compile option was changed from **-march=native** to **-msse4.2 -maes**. Additionally, the MinGW variant _TDM-GCC_ is now supported by replacing the use of `RtlGenRandom` (aka `SystemFunction036`) with the use of the standard function `rand_s` (which internally calls `RtlGenRandom`). The direct call to `RtlGenRandom` can be activated by defining the compile time symbol `SQLITE3MC_USE_RAND_S=0`.

### Fixed

- Issue [#37](../../issues/37): Allow concurrent access from legacy applications by establishing WAL journal mode compatibility
  This change allows concurrent use of applications still using SQLite versions (< 3.32.0) based on the `SQLITE_HAS_CODEC` encryption API and applications using the new _SQLite3 Multiple Ciphers_ implementation in WAL journal mode.
- Issue [#36](../../issues/36): Clear pager cache after setting a new passphrase to force a reread of the database header

## [1.2.5] - 2021-04-20

### Changed

- Based on SQLite version 3.35.5

## [1.2.4] - 2021-04-02

### Changed

- Based on SQLite version 3.35.4

## [1.2.3] - 2021-03-27

### Changed

- Based on SQLite version 3.35.3

## [1.2.2] - 2021-03-22

### Changed

- Based on SQLite version 3.35.2

## [1.2.1] - 2021-03-15

### Changed

- Based on SQLite version 3.35.1

## [1.2.0] - 2021-03-13

### Changed

- Based on SQLite version 3.35.0
- Cleaned up precompiler instructions to exclude cipher schemes from build

### Added

- Enabled new SQLite Math Extension (Note: _log_ function now computes _log10_, not _ln_.)

### Fixed

- Fixed a bug in cipher selection via URI, if cipher schemes were excluded from build (issue [#26](../../issues/26))

## [1.1.4] - 2021-01-23

### Changed

- Based on SQLite version 3.34.1

## [1.1.3] - 2020-12-29

### Changed

- Added code for AES hardware support on ARM platforms
- Added GitHub Actions for CI

## [1.1.2] - 2020-12-10

### Changed

- Added SQLite3 Multple Ciphers version info to shell application

### Fixed

- Fixed a bug on cipher configuration via PRAGMA commands or URI parameters (issue #20)

## [1.1.1] - 2020-12-07

### Fixed

- Fixed a bug on removing encryption from an encrypted database (issue #19)

## [1.1.0] - 2020-12-06

### Changed

- Based on SQLite version 3.34.0
- Added code for AES hardware support on x86 platforms

### Fixed
- Fixed issues with sqlite3_key / sqlite3_rekey

## [1.0.1] - 2020-10-03

### Added

- Added VSV extension (_V_ariably _S_eparated _V_alues)

## [1.0.0] - 2020-08-15

First release of the new implementation of the SQLite3 encryption extension with support for multiple ciphers. The release is based on SQLite version 3.33.0.

The following ciphers are supported:

- AES 128 Bit CBC - No HMAC ([wxSQLite3](https://github.com/utelle/wxsqlite3))
- AES 256 Bit CBC - No HMAC ([wxSQLite3](https://github.com/utelle/wxsqlite3))
- ChaCha20 - Poly1305 HMAC ([sqleet](https://github.com/resilar/sqleet), _default_)
- AES 256 Bit CBC - SHA1/SHA256/SHA512 HMAC ([SQLCipher](https://www.zetetic.net/sqlcipher/), database versions 1, 2, 3, and 4)
- RC4 - No HMAC ([System.Data.SQLite](http://system.data.sqlite.org))

[Unreleased]: ../../compare/v1.8.0...HEAD
[1.8.0]: ../../compare/v1.7.4...v1.8.0
[1.7.4]: ../../compare/v1.7.3...v1.7.4
[1.7.3]: ../../compare/v1.7.2...v1.7.3
[1.7.2]: ../../compare/v1.7.1...v1.7.2
[1.7.1]: ../../compare/v1.7.0...v1.7.1
[1.7.0]: ../../compare/v1.6.5...v1.7.0
[1.6.5]: ../../compare/v1.6.4...v1.6.5
[1.6.4]: ../../compare/v1.6.3...v1.6.4
[1.6.3]: ../../compare/v1.6.2...v1.6.3
[1.6.2]: ../../compare/v1.6.1...v1.6.2
[1.6.1]: ../../compare/v1.6.0...v1.6.1
[1.6.0]: ../../compare/v1.5.5...v1.6.0
[1.5.5]: ../../compare/v1.5.4...v1.5.5
[1.5.4]: ../../compare/v1.5.3...v1.5.4
[1.5.3]: ../../compare/v1.5.2...v1.5.3
[1.5.2]: ../../compare/v1.5.1...v1.5.2
[1.5.1]: ../../compare/v1.5.0...v1.5.1
[1.5.0]: ../../compare/v1.4.8...v1.5.0
[1.4.8]: ../../compare/v1.4.7...v1.4.8
[1.4.7]: ../../compare/v1.4.6...v1.4.7
[1.4.6]: ../../compare/v1.4.5...v1.4.6
[1.4.5]: ../../compare/v1.4.4...v1.4.5
[1.4.4]: ../../compare/v1.4.3...v1.4.4
[1.4.3]: ../../compare/v1.4.2...v1.4.3
[1.4.2]: ../../compare/v1.4.1...v1.4.2
[1.4.1]: ../../compare/v1.4.0...v1.4.1
[1.4.0]: ../../compare/v1.3.1...v1.4.0
[1.3.10]: ../../compare/v1.3.9...v1.3.10
[1.3.9]: ../../compare/v1.3.8...v1.3.9
[1.3.8]: ../../compare/v1.3.7...v1.3.8
[1.3.7]: ../../compare/v1.3.6...v1.3.7
[1.3.6]: ../../compare/v1.3.5...v1.3.6
[1.3.5]: ../../compare/v1.3.4...v1.3.5
[1.3.4]: ../../compare/v1.3.3...v1.3.4
[1.3.3]: ../../compare/v1.3.2...v1.3.3
[1.3.2]: ../../compare/v1.3.1...v1.3.2
[1.3.1]: ../../compare/v1.3.0...v1.3.1
[1.3.0]: ../../compare/v1.2.5...v1.3.0
[1.2.5]: ../../compare/v1.2.4...v1.2.5
[1.2.4]: ../../compare/v1.2.3...v1.2.4
[1.2.3]: ../../compare/v1.2.2...v1.2.3
[1.2.2]: ../../compare/v1.2.1...v1.2.2
[1.2.1]: ../../compare/v1.2.0...v1.2.1
[1.2.0]: ../../compare/v1.1.4...v1.2.0
[1.1.4]: ../../compare/v1.1.3...v1.1.4
[1.1.3]: ../../compare/v1.1.2...v1.1.3
[1.1.2]: ../../compare/v1.1.1...v1.1.2
[1.1.1]: ../../compare/v1.1.0...v1.1.1
[1.1.0]: ../../compare/v1.0.1...v1.1.0
[1.0.1]: ../../compare/v1.0.0...v1.0.1
[1.0.0]: ../../compare/v1.0.1...v1.0.0
