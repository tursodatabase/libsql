use aes::Aes256;

use crate::LIBSQL_PAGE_SIZE;

#[derive(Debug)]
pub struct EncryptionConfig {
    pub decryptor: cbc::Decryptor<Aes256>,
    pub encryptor: cbc::Encryptor<Aes256>,
    pub scratch: Box<[u8; LIBSQL_PAGE_SIZE as usize]>
}
