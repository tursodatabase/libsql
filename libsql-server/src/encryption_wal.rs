use std::borrow::Cow;
use std::num::NonZeroU32;

use hmac::{Hmac, Mac};
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::{wrapper::WrapWal, Wal, PageHeaders};
use rusqlite::ffi::libsql_pghdr;
use sha2::Sha256;
use zerocopy::FromBytes;

use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use aes::cipher::block_padding::NoPadding;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

use crate::LIBSQL_PAGE_SIZE;

#[derive(Clone, Debug)]
pub struct EncryptionWrapper { 
    enc_key: Aes256CbcEnc,
    dec_key: Aes256CbcDec,
}

impl EncryptionWrapper {
    pub fn new(key: &str) -> Self {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(key.as_bytes());
        let key_h = mac.finalize().into_bytes();
        let iv = [42u8; 16];
        let enc_key = Aes256CbcEnc::new(&key_h.into(), &iv.into());
        let dec_key = Aes256CbcDec::new(&key_h.into(), &iv.into());
        Self { enc_key, dec_key }
    }
}

impl<W: Wal> WrapWal<W> for EncryptionWrapper {
    fn find_frame(
            &mut self,
            wrapped: &mut W,
            page_no: NonZeroU32,
        ) -> libsql_sys::wal::Result<Option<NonZeroU32>> {
        match wrapped.find_frame(page_no)? {
            Some(fno) => Ok(Some(fno)),
            None => {
                // we set the last bit to 1 to signify read frame that we want to read from the
                // main db file
                Ok(Some(NonZeroU32::new(page_no.get() | 1u32.rotate_right(1)).unwrap()))
            }
        }
    }

    fn read_frame(
            &mut self,
            wrapped: &mut W,
            frame_no: NonZeroU32,
            buffer: &mut [u8],
        ) -> libsql_sys::wal::Result<()> {
        let mut buff = [0; 4096];
        if frame_no.get() & 1u32.rotate_right(1) == 0 {
            wrapped.read_frame(frame_no, &mut buff)?;
        } else {
            // read from main file
            let page_no = frame_no.get() & !(1u32.rotate_right(1));
            let offset = (page_no - 1) * LIBSQL_PAGE_SIZE as u32;
            wrapped.db_file().read_at(&mut buff, offset as _)?;
            if page_no == 1 {
                let header = Sqlite3DbHeader::read_from_prefix(buffer).unwrap();
                if &header.header_str == b"SQLite format 3\0" {
                    let to_copy = buffer.len();
                    buffer.copy_from_slice(&buff[..to_copy]);
                    return Ok(())
                }
            }
        }

        self.dec_key.clone().decrypt_padded_mut::<NoPadding>(&mut buff).unwrap();
        
        let to_copy = buffer.len();
        buffer.copy_from_slice(&buff[..to_copy]);

        Ok(())
    }

    fn insert_frames(
            &mut self,
            wrapped: &mut W,
            page_size: std::ffi::c_int,
            page_headers: &mut libsql_sys::wal::PageHeaders,
            size_after: u32,
            is_commit: bool,
            sync_flags: std::ffi::c_int,
        ) -> libsql_sys::wal::Result<()> {
        let mut encrypted = Vec::new();
        for (page_no, page) in page_headers.iter() {
            let mut page = page.to_vec();
            self.enc_key.clone().encrypt_padded_mut::<NoPadding>(&mut page, 4096).unwrap();
            encrypted.push((page_no, page));
        }

        let mut headers: Vec<libsql_pghdr> = Vec::with_capacity(encrypted.len());
        
        let first = encrypted.iter().rev().fold(std::ptr::null_mut(), |next, (pno, page)| {
            headers.push(
                libsql_pghdr {
                    pPage: std::ptr::null_mut(),
                    pData: page.as_ptr() as _,
                    pExtra: std::ptr::null_mut(),
                    pCache: std::ptr::null_mut(),
                    pDirty: next,
                    pPager: std::ptr::null_mut(),
                    pgno: *pno,
                    pageHash: 0,
                    flags: 0x02, // PGHDR_DIRTY - it works without the flag, but why risk it
                    nRef: 0,
                    pDirtyNext: std::ptr::null_mut(),
                    pDirtyPrev: std::ptr::null_mut(),
                }
            );

            headers.last_mut().unwrap() as *mut _
        });

        unsafe {
            let mut headers = PageHeaders::from_raw(first);
            wrapped.insert_frames(page_size, &mut headers, size_after, is_commit, sync_flags)?;
        }

        Ok(())
    }
}
