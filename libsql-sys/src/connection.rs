use std::marker::PhantomData;
use std::path::Path;
use std::str::FromStr;

use crate::wal::{ffi::make_wal_manager, Wal, WalManager};

#[cfg(not(feature = "rusqlite"))]
type RawConnection = *mut crate::ffi::sqlite3;
#[cfg(feature = "rusqlite")]
type RawConnection = rusqlite::Connection;

#[cfg(not(feature = "rusqlite"))]
pub type OpenFlags = std::ffi::c_int;
#[cfg(feature = "rusqlite")]
pub type OpenFlags = rusqlite::OpenFlags;

#[cfg(feature = "rusqlite")]
type Error = rusqlite::Error;
#[cfg(not(feature = "rusqlite"))]
type Error = crate::Error;

#[derive(Clone, Debug, Default)]
pub enum Cipher {
    // AES 256 Bit CBC - No HMAC (wxSQLite3)
    #[default]
    Aes256Cbc,
}

impl FromStr for Cipher {
    type Err = libsql_ffi::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aes256cbc" => Ok(Cipher::Aes256Cbc),
            _ => Err(Self::Err::new(21)),
        }
    }
}

impl Cipher {
    #[cfg(feature = "encryption")]
    pub fn cipher_id(&self) -> i32 {
        let name = match self {
            Cipher::Aes256Cbc => "aes256cbc\0",
        };
        unsafe { sqlite3mc_cipher_index(name.as_ptr() as _) }
    }
}

#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    pub cipher: Cipher,
    pub encryption_key: bytes::Bytes,
}

impl EncryptionConfig {
    pub fn new(cipher: Cipher, encryption_key: bytes::Bytes) -> Self {
        Self {
            cipher,
            encryption_key,
        }
    }

    #[cfg(feature = "encryption")]
    pub fn cipher_id(&self) -> i32 {
        self.cipher.cipher_id()
    }
}

#[derive(Debug)]
pub struct Connection<W> {
    conn: RawConnection,
    _pth: PhantomData<W>,
}

#[cfg(feature = "rusqlite")]
impl<W> std::ops::Deref for Connection<W> {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

#[cfg(feature = "rusqlite")]
impl<W> std::ops::DerefMut for Connection<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

#[cfg(feature = "rusqlite")]
impl Connection<crate::wal::Sqlite3Wal> {
    /// returns a dummy, in-memory connection. For testing purposes only
    pub fn test() -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        Self {
            conn,
            _pth: PhantomData,
        }
    }
}

#[cfg(feature = "encryption")]
extern "C" {
    fn sqlite3mc_cipher_index(cipher: *const std::ffi::c_void) -> std::ffi::c_int;
    fn sqlite3mc_config(
        db: *mut libsql_ffi::sqlite3,
        cipher: *const std::ffi::c_void,
        nKey: std::ffi::c_int,
    ) -> std::ffi::c_int;
    fn sqlite3_key(
        db: *mut libsql_ffi::sqlite3,
        pKey: *const std::ffi::c_void,
        nKey: std::ffi::c_int,
    ) -> std::ffi::c_int;
    fn sqlite3_rekey(
        db: *mut libsql_ffi::sqlite3,
        pKey: *const std::ffi::c_void,
        nKey: std::ffi::c_int,
    ) -> std::ffi::c_int;

    fn libsql_leak_pager(db: *mut libsql_ffi::sqlite3) -> *mut crate::ffi::Pager;
    fn libsql_generate_initial_vector(seed: u32, iv: *mut u8);
    fn libsql_generate_aes256_key(user_password: *const u8, password_length: u32, digest: *mut u8);
}

pub fn pghdr_creator(
    data: &mut [u8; 4096],
    _db: *mut libsql_ffi::sqlite3,
) -> libsql_ffi::libsql_pghdr {
    #[cfg(feature = "encryption")]
    let pager = crate::connection::leak_pager(_db);
    #[cfg(not(feature = "encryption"))]
    let pager = std::ptr::null_mut();

    libsql_ffi::libsql_pghdr {
        pPage: std::ptr::null_mut(),
        pData: data.as_mut_ptr() as _,
        pExtra: std::ptr::null_mut(),
        pCache: std::ptr::null_mut(),
        pDirty: std::ptr::null_mut(),
        pPager: pager,
        pgno: 1,
        pageHash: 0x02, // DIRTY
        flags: 0,
        nRef: 0,
        pDirtyNext: std::ptr::null_mut(),
        pDirtyPrev: std::ptr::null_mut(),
    }
}

#[cfg(feature = "encryption")]
/// # Safety
/// db must point to a vaid sqlite database
pub unsafe fn set_encryption_cipher(db: *mut libsql_ffi::sqlite3, cipher_id: i32) -> i32 {
    unsafe { sqlite3mc_config(db, "default:cipher\0".as_ptr() as _, cipher_id) as i32 }
}

#[cfg(feature = "encryption")]
/// # Safety
/// db must point to a vaid sqlite database
pub unsafe fn set_encryption_key(db: *mut libsql_ffi::sqlite3, key: &[u8]) -> i32 {
    unsafe { sqlite3_key(db, key.as_ptr() as _, key.len() as _) as i32 }
}

#[cfg(feature = "encryption")]
/// # Safety
/// db must point to a valid sqlite database
pub fn reset_encryption_key(db: *mut libsql_ffi::sqlite3, key: &[u8]) -> i32 {
    unsafe { sqlite3_rekey(db, key.as_ptr() as _, key.len() as _) as i32 }
}

#[cfg(feature = "encryption")]
pub fn leak_pager(db: *mut libsql_ffi::sqlite3) -> *mut crate::ffi::Pager {
    unsafe { libsql_leak_pager(db) }
}

#[cfg(feature = "encryption")]
pub fn generate_initial_vector(seed: u32, iv: &mut [u8]) {
    unsafe { libsql_generate_initial_vector(seed, iv.as_mut_ptr()) }
}

#[cfg(feature = "encryption")]
pub fn generate_aes256_key(user_password: &[u8], digest: &mut [u8]) {
    unsafe {
        libsql_generate_aes256_key(
            user_password.as_ptr(),
            user_password.len() as u32,
            digest.as_mut_ptr(),
        )
    }
}

pub const NO_AUTOCHECKPOINT: u32 = 0;

impl<W: Wal> Connection<W> {
    /// Opens a database with the regular wal methods in the directory pointed to by path
    pub fn open<T>(
        path: impl AsRef<Path>,
        flags: OpenFlags,
        wal_manager: T,
        auto_checkpoint: u32,
        encryption_config: Option<EncryptionConfig>,
    ) -> Result<Self, Error>
    where
        T: WalManager<Wal = W>,
    {
        tracing::trace!(
            "Opening a connection with regular WAL at {}",
            path.as_ref().display()
        );

        #[cfg(feature = "rusqlite")]
        let conn = {
            let conn = if cfg!(feature = "unix-excl-vfs") {
                rusqlite::Connection::open_with_flags_vfs_and_wal(
                    path,
                    flags,
                    "unix-excl",
                    make_wal_manager(wal_manager),
                )
            } else {
                rusqlite::Connection::open_with_flags_and_wal(
                    path,
                    flags,
                    make_wal_manager(wal_manager),
                )
            }?;

            if !cfg!(feature = "encryption") && encryption_config.is_some() {
                return Err(Error::SqliteFailure(
                    rusqlite::ffi::Error::new(21),
                    Some("encryption feature is not enabled, the database will not be encrypted on disk"
                        .to_string()),
                ));
            }
            #[cfg(feature = "encryption")]
            if let Some(cfg) = encryption_config {
                let cipher_id = cfg.cipher_id();
                if unsafe { set_encryption_cipher(conn.handle(), cipher_id) } == -1 {
                    return Err(Error::SqliteFailure(
                        rusqlite::ffi::Error::new(21),
                        Some("failed to set encryption cipher".into()),
                    ));
                };
                if unsafe { set_encryption_key(conn.handle(), &cfg.encryption_key) }
                    != rusqlite::ffi::SQLITE_OK
                {
                    return Err(Error::SqliteFailure(
                        rusqlite::ffi::Error::new(21),
                        Some("failed to set encryption key".into()),
                    ));
                };
            }

            conn.pragma_update(None, "journal_mode", "WAL")?;
            unsafe {
                let rc =
                    rusqlite::ffi::sqlite3_wal_autocheckpoint(conn.handle(), auto_checkpoint as _);
                if rc != 0 {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rc),
                        Some("failed to set auto_checkpoint".into()),
                    ));
                }
            }

            conn.busy_timeout(std::time::Duration::from_secs(5000))?;

            conn
        };

        #[cfg(not(feature = "rusqlite"))]
        let conn = unsafe {
            use std::os::unix::ffi::OsStrExt;
            let path = std::ffi::CString::new(path.as_ref().as_os_str().as_bytes())
                .map_err(|_| crate::error::Error::Bug("invalid database path"))?;
            let mut conn: *mut crate::ffi::sqlite3 = std::ptr::null_mut();
            // We pass a pointer to the WAL methods data to the database connection. This means
            // that the reference must outlive the connection. This is guaranteed by the marker in
            // the returned connection.
            let vfs = if cfg!(feature = "unix-excl-vfs") {
                "unix-excl\0".as_ptr() as *const _
            } else {
                std::ptr::null_mut()
            };
            let mut rc = libsql_ffi::libsql_open_v3(
                path.as_ptr(),
                &mut conn as *mut _,
                flags,
                vfs,
                make_wal_manager(wal_manager),
            );

            if !cfg!(feature = "encryption") && encryption_config.is_some() {
                return Err(Error::Bug(
                    "encryption feature is not enabled, the database will not be encrypted on disk",
                ));
            }
            #[cfg(feature = "encryption")]
            if let Some(cfg) = encryption_config {
                let cipher_id = cfg.cipher_id();
                if set_encryption_cipher(conn, cipher_id) == -1 {
                    return Err(Error::Bug("failed to set encryption cipher"));
                }
                if set_encryption_key(conn, &cfg.encryption_key) != libsql_ffi::SQLITE_OK {
                    return Err(Error::Bug("failed to set encryption key"));
                }
            }

            if rc == 0 {
                rc = libsql_ffi::sqlite3_wal_autocheckpoint(conn, auto_checkpoint as _);
            }

            if rc != 0 {
                libsql_ffi::sqlite3_close(conn);
                return Err(crate::Error::from(rc));
            }

            assert!(!conn.is_null());

            crate::ffi::sqlite3_busy_timeout(conn, 5000);

            conn
        };

        Ok(Connection {
            conn,
            _pth: PhantomData,
        })
    }

    /// Returns the raw sqlite handle
    ///
    /// # Safety
    /// The caller is responsible for the returned pointer.
    pub unsafe fn handle(&self) -> *mut libsql_ffi::sqlite3 {
        #[cfg(feature = "rusqlite")]
        {
            self.conn.handle()
        }
        #[cfg(not(feature = "rusqlite"))]
        {
            self.conn
        }
    }

    pub fn db_change_counter(&self) -> Result<u32, std::ffi::c_int> {
        let mut file_ptr: *mut libsql_ffi::sqlite3_file = std::ptr::null_mut();
        let rc = unsafe {
            libsql_ffi::sqlite3_file_control(
                self.handle(),
                "main\0".as_ptr() as *const _,
                libsql_ffi::SQLITE_FCNTL_FILE_POINTER,
                &mut file_ptr as *mut _ as *mut _,
            )
        };
        if rc != libsql_ffi::SQLITE_OK {
            return Err(rc);
        }
        let counter = unsafe {
            let mut counter: u32 = 0;
            let file = &*file_ptr;
            (*file.pMethods).xRead.unwrap()(file_ptr, &mut counter as *mut _ as *mut _, 4, 24);
            u32::from_be(counter)
        };
        Ok(counter)
    }
}
// pub struct Connection<'a> {
//     pub conn: *mut crate::ffi::sqlite3,
//     _pth: PhantomData<&'a mut ()>,
// }
//
// /// The `Connection` struct is `Send` because `sqlite3` is thread-safe.
// unsafe impl<'a> Send for Connection<'a> {}
// unsafe impl<'a> Sync for Connection<'a> {}
//
// impl<'a> Connection<'a> {
//     /// returns a dummy, in-memory connection. For testing purposes only
//     pub fn test(_: &mut ()) -> Self {
//         let mut conn: *mut crate::ffi::sqlite3 = std::ptr::null_mut();
//         let rc = unsafe {
//             crate::ffi::sqlite3_open(":memory:\0".as_ptr() as *const _, &mut conn as *mut _)
//         };
//         assert_eq!(rc, 0);
//         assert!(!conn.is_null());
//         Self {
//             conn,
//             _pth: PhantomData,
//         }
//     }
//
//     /// Opens a database with the regular wal methods, given a path to the database file.
//     pub fn open<W: Wal>(
//         path: impl AsRef<std::path::Path>,
//         flags: c_int,
//         // we technically _only_ need to know about W, but requiring a static ref to the wal_hook ensures that
//         // it has been instantiated and lives for long enough
//         _wal_hook: &'static WalMethodsHook<W>,
//         hook_ctx: &'a mut W::Context,
//     ) -> Result<Self, crate::Error> {
//         let path = path.as_ref();
//         tracing::trace!(
//             "Opening a connection with regular WAL at {}",
//             path.display()
//         );
//
//         let conn_str = format!("file:{}?_journal_mode=WAL", path.display());
//         let filename = CString::new(conn_str).unwrap();
//         let mut conn: *mut crate::ffi::sqlite3 = std::ptr::null_mut();
//
//         unsafe {
//             // We pass a pointer to the WAL methods data to the database connection. This means
//             // that the reference must outlive the connection. This is guaranteed by the marker in
//             // the returned connection.
//             let rc = crate::ffi::libsql_open_v2(
//                 filename.as_ptr(),
//                 &mut conn as *mut _,
//                 flags,
//                 std::ptr::null_mut(),
//                 W::name().as_ptr(),
//                 hook_ctx as *mut _ as *mut _,
//             );
//
//             if rc != 0 {
//                 crate::ffi::sqlite3_close(conn);
//                 return Err(crate::Error::LibError(rc));
//             }
//
//             assert!(!conn.is_null());
//         };
//
//         unsafe {
//             crate::ffi::sqlite3_busy_timeout(conn, 5000);
//         }
//
//         Ok(Connection {
//             conn,
//             _pth: PhantomData,
//         })
//     }
//
//     pub fn is_autocommit(&self) -> bool {
//         unsafe { crate::ffi::sqlite3_get_autocommit(self.conn) != 0 }
//     }
// }
//
// impl Drop for Connection<'_> {
//     fn drop(&mut self) {
//         if self.conn.is_null() {
//             tracing::debug!("Trying to close a null connection");
//             return;
//         }
//         unsafe {
//             crate::ffi::sqlite3_close(self.conn as *mut _);
//         }
//     }
// }
