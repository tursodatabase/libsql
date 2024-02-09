pub mod frame;
pub mod injector;
pub mod meta;
pub mod replicator;
pub mod rpc;
pub mod snapshot;

mod error;

pub const LIBSQL_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct FrameEncryptor {
    enc: cbc::Encryptor<aes::Aes256>,
    dec: cbc::Decryptor<aes::Aes256>,
}

impl FrameEncryptor {
    pub fn new(key: bytes::Bytes) -> Self {
        #[cfg(feature = "encryption")]
        const SEED: u32 = 911;
        #[cfg(not(feature = "encryption"))]
        let _ = key;

        use aes::cipher::KeyIvInit;

        #[allow(unused_mut)]
        let mut iv: [u8; 16] = [0; 16];
        #[allow(unused_mut)]
        let mut digest: [u8; 32] = [0; 32];
        #[cfg(feature = "encryption")]
        libsql_sys::connection::generate_initial_vector(SEED, &mut iv);
        #[cfg(feature = "encryption")]
        libsql_sys::connection::generate_aes256_key(&key, &mut digest);

        let enc = cbc::Encryptor::new((&digest).into(), (&iv).into());
        let dec = cbc::Decryptor::new((&digest).into(), (&iv).into());
        Self { enc, dec }
    }

    pub fn encrypt(&self, data: &mut [u8]) -> Result<(), rusqlite::ffi::Error> {
        use aes::cipher::{block_padding::NoPadding, BlockEncryptMut};
        // NOTICE: We don't want to return padding errors, it will make the code
        // prone to CBC padding oracle attacks.
        self.enc
            .clone()
            .encrypt_padded_mut::<NoPadding>(data, data.len())
            .map_err(|_| rusqlite::ffi::Error::new(libsql_sys::ffi::SQLITE_IOERR_WRITE))?;
        Ok(())
    }

    pub fn decrypt(&self, data: &mut [u8]) -> Result<(), rusqlite::ffi::Error> {
        use aes::cipher::{block_padding::NoPadding, BlockDecryptMut};
        // NOTICE: We don't want to return padding errors, it will make the code
        // prone to CBC padding oracle attacks.
        self.dec
            .clone()
            .decrypt_padded_mut::<NoPadding>(data)
            .map_err(|_| rusqlite::ffi::Error::new(libsql_sys::ffi::SQLITE_IOERR_READ))?;
        Ok(())
    }
}
