# Source files of the wxSQLite3 encryption extension

The following document gives a short overview of all source files of which the **SQLite3 Multiple Ciphers** encryption extension consists.

## Kernel of the wxSQLite3 encryption extension

The following files constitute the kernel of the **SQLite3 Multiple Ciphers** encryption extension:

| Filename | Description |
| :--- | :--- |
| cipher_common.c    | Implementation of the common cipher functions |
| cipher_common.h    | Header for the common cipher functions |
| cipher_config.c    | Common cipher configuration functions |
| cipher_wxaes128.c  | Implementation of the **wxSQLite3 AES 128-bit** encryption extension |
| cipher_wxaes256.c  | Implementation of the **wxSQLite3 AES 256-bit** encryption extension |
| cipher_chacha20.c  | Implementation of the **ChaCha20-Poly1305** encryption extension |
| cipher_sqlcipher.c | Implementation of the **SQLCipher** encryption extension |
| cipher_sds_rc4.c   | Implementation of the **System.Data.SQLite RC4** encryption extension |
| codec_algos.c      | Implementation of the encryption algorithms |
| codecext.c         | Implementation of the **SQLite3** codec API |
| rekeyvacuum.c      | Adjusted VACUUM function for use on rekeying a database file |
| sqlite3mc.c        | _Amalgamation_ of the complete **SQLite3 Multiple Ciphers** encryption extension |
| sqlite3mc.h        | Header for the additional API functions of the **SQLite3 Multiple Ciphers** encryption extension |
| sqlite3mc_vfs.c    | Implementation of the Multiple Ciphers VFS |
| sqlite3mc_vfs.h    | Header for the additional API functions of the Multiple Ciphers VFS |

All files, except `rekeyvacuum.c`, are licensed under the `MIT` license.

`rekeyvacuum.c` contains a slightly modified implementation of the function `sqlite3RunVacuum` from the **SQLite3** and stays in the public domain.

## Cryptograhic algorithms

The following files contain the implementations of cryptographic algorithms used by the **wxSQLite3** encryption extension:

| Filename | Description |
| :--- | :--- |
| chacha20poly1305.c | Implementation of ChaCha20 cipher and Poly1305 message authentication |
| fastpbkdf2.c       | Implementation of PBKDF2 functions |
| fastpbkdf2.h       | Header for PBKDF2 functions |
| md5.c              | Implementation of MD5 hash functions |
| rijndael.c         | Implementation of AES block cipher |
| rijndael.h         | Header for AES block cipher |
| sha1.c             | Implementation of SHA1 hash functions |
| sha1.h             | Header for SHA1 hash functions |
| sha2.c             | Implementation of SHA2 hash functions |
| sha2.h             | Header for SHA2 hash functions |

The files `chacha20poly1305.c`, `fastpbkdf2.*`, `md5.c`, and `sha1.*` are in the public domain.

The files `rijndael.*`, are licensed under `LGPL-3.0+ WITH WxWindows-exception-3.1`.

The files `sha2.*` are licensed under `BSD-3-Clause`.

## Windows-specific files

The following files are only used under Windows platforms for creating binaries:

| Filename | Description |
| :--- | :--- |
| sqlite3mc.def      | Module definition specifying exported functions |
| sqlite3mc.rc       | Resource file specifying version information for the SQLite3MultipleCiphers library |
| sqlite3mc_shell.rc | Resource file specifying version information for the SQLite3MultipleCiphers shell |

All files are licensed under the `MIT` license.

## SQLite3 core

The following files belong to the **SQLite3** core:

| Filename | Description |
| :--- | :--- |
| shell.c          | SQLite3 shell application  |
| sqlite3.c        | SQLite3 source amalgamation  |
| sqlite3.h        | SQLite3 header  |
| sqlite3ext.h     | SQLite3 header for extensions  |
| test_windirent.c | Source for directory access under Windows used by `fileio` extension |
| test_windirent.h | Header for directory access under Windows used by `fileio` extension |

All files are in the public domain.

## SQLite3 extensions

The following files belong to **SQLite3** extensions contained in the official **SQLite3** distribution:

| Filename | Description |
| :--- | :--- |
| carray.c          | Table-valued function that returns the values in a C-language array |
| csv.c             | Virtual table for reading CSV files |
| fileio.c          | Functions `readfile` and `writefile`, and eponymous virtual table `fsdir` |
| series.c          | Table-valued-function implementing the function `generate_series` |
| shathree.c        | Functions computing SHA3 hashes |
| sqlite3userauth.h | Header for user-authentication extension feature |
| userauth.c        | User-authentication extension feature (modified password hash function) |
| uuid.c            | Functions handling RFC-4122 UUIDs |

All files are in the public domain.

## External SQLite3 extensions

The following file was posted to the SQLite mailing list:

| Filename | Description |
| :--- | :--- |
| extensionfunctions.c | The extension provides common mathematical and string functions |

The file `extensionfunctions.c` does not contain any specific license information. Since it was posted to the SQLite mailing list, it is assumed that the file is in the public domain like SQLite3 itself.
