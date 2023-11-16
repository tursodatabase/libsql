use anyhow::{anyhow, Result};
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite};

#[repr(transparent)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WalFrameHeader([u8; WalFrameHeader::SIZE]);

impl WalFrameHeader {
    pub const SIZE: usize = 24;

    /// In multi-page transactions, only the last page in the transaction contains
    /// the size_after_transaction field. If it's zero, it means it's an uncommited
    /// page.
    pub fn is_committed(&self) -> bool {
        self.size_after() != 0
    }

    /// Page number
    pub fn pgno(&self) -> u32 {
        u32::from_be_bytes([self.0[0], self.0[1], self.0[2], self.0[3]])
    }

    /// For commit records, the size of the database image in pages
    /// after the commit. For all other records, zero.
    pub fn size_after(&self) -> u32 {
        u32::from_be_bytes([self.0[4], self.0[5], self.0[6], self.0[7]])
    }

    #[allow(dead_code)]
    pub fn salt(&self) -> u64 {
        u64::from_be_bytes([
            self.0[8], self.0[9], self.0[10], self.0[11], self.0[12], self.0[13], self.0[14],
            self.0[15],
        ])
    }

    pub fn crc(&self) -> (u32, u32) {
        (
            u32::from_be_bytes([self.0[16], self.0[17], self.0[18], self.0[19]]),
            u32::from_be_bytes([self.0[20], self.0[21], self.0[22], self.0[23]]),
        )
    }

    pub fn verify(&self, init_crc: (u32, u32), page_data: &[u8]) -> Result<(u32, u32)> {
        let mut crc = init_crc;
        crc = checksum_step(crc, &self.0[0..8]);
        crc = checksum_step(crc, page_data);
        let frame_crc = self.crc();
        if crc == frame_crc {
            Ok(crc)
        } else {
            Err(anyhow!(
                "Frame checksum verification failed for page no. {}. Expected: {:X}-{:X}. Got: {:X}-{:X}",
                self.pgno(),
                frame_crc.0,
                frame_crc.1,
                crc.0,
                crc.1,
            ))
        }
    }
}

impl From<[u8; WalFrameHeader::SIZE]> for WalFrameHeader {
    fn from(value: [u8; WalFrameHeader::SIZE]) -> Self {
        WalFrameHeader(value)
    }
}

impl From<WalFrameHeader> for [u8; WalFrameHeader::SIZE] {
    fn from(h: WalFrameHeader) -> [u8; WalFrameHeader::SIZE] {
        h.0
    }
}

impl AsRef<[u8]> for WalFrameHeader {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WalHeader {
    /// Magic number. 0x377f0682 or 0x377f0683
    pub magic_no: u32,
    /// File format version. Currently 3007000
    pub version: u32,
    /// Database page size.
    pub page_size: u32,
    /// Checkpoint sequence number
    pub checkpoint_seq_no: u32,
    /// Random integer incremented with each checkpoint
    pub salt_1: u32,
    /// A different random integer changing with each checkpoint
    pub salt_2: u32,
    /// Checksum for first 24 bytes of header
    pub checksum_1: u32,
    pub checksum_2: u32,
}

impl WalHeader {
    pub const SIZE: u64 = 32;
}

impl From<[u8; WalHeader::SIZE as usize]> for WalHeader {
    fn from(v: [u8; WalHeader::SIZE as usize]) -> Self {
        WalHeader {
            magic_no: u32::from_be_bytes([v[0], v[1], v[2], v[3]]),
            version: u32::from_be_bytes([v[4], v[5], v[6], v[7]]),
            page_size: u32::from_be_bytes([v[8], v[9], v[10], v[11]]),
            checkpoint_seq_no: u32::from_be_bytes([v[12], v[13], v[14], v[15]]),
            salt_1: u32::from_be_bytes([v[16], v[17], v[18], v[19]]),
            salt_2: u32::from_be_bytes([v[20], v[21], v[22], v[23]]),
            checksum_1: u32::from_be_bytes([v[24], v[25], v[26], v[27]]),
            checksum_2: u32::from_be_bytes([v[28], v[29], v[30], v[31]]),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WalFileReader {
    file: File,
    header: WalHeader,
}

impl WalFileReader {
    pub async fn open<P: AsRef<Path>>(fpath: P) -> Result<Option<Self>> {
        let mut file = File::open(fpath).await?;
        let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        if len < WalHeader::SIZE {
            return Ok(None);
        }
        let header = {
            let mut buf = [0u8; WalHeader::SIZE as usize];
            file.read_exact(buf.as_mut()).await?;
            WalHeader::from(buf)
        };
        Ok(Some(WalFileReader { file, header }))
    }

    /// Returns page size stored in WAL file header.
    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    pub fn checksum(&self) -> (u32, u32) {
        (self.header.checksum_1, self.header.checksum_2)
    }

    pub fn frame_size(&self) -> u64 {
        (WalFrameHeader::SIZE as u64) + (self.page_size() as u64)
    }

    /// Returns an offset in a WAL file, where the data of a frame with given number starts.
    pub fn offset(&self, frame_no: u32) -> u64 {
        WalHeader::SIZE + ((frame_no - 1) as u64) * self.frame_size()
    }

    /// Returns a number of pages stored in current WAL file.
    pub async fn frame_count(&self) -> u32 {
        let len = self.file.metadata().await.map(|m| m.len()).unwrap_or(0);
        if len < WalHeader::SIZE {
            0
        } else {
            ((len - WalHeader::SIZE) / self.frame_size()) as u32
        }
    }

    /// Sets a file cursor at the beginning of a frame with given number.
    pub async fn seek_frame(&mut self, frame_no: u32) -> Result<()> {
        let offset = self.offset(frame_no);
        self.file.seek(SeekFrom::Start(offset)).await?;
        Ok(())
    }

    /// Reads a header of a WAL frame, without reading the entire page that frame is
    /// responsible for.
    ///
    /// For reading specific frame use [WalFileReader::seek_frame] before calling this method.
    pub async fn read_frame_header(&mut self) -> Result<Option<WalFrameHeader>> {
        let mut header = [0u8; WalFrameHeader::SIZE];
        match self.file.read_exact(header.as_mut()).await {
            Ok(_) => Ok(Some(WalFrameHeader::from(header))),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn copy_frames<W>(&mut self, w: &mut W, frame_count: usize) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        //TODO - specialize non-compressed file cloning:
        //   libc::copy_file_range(wal.as_mut(), wal.offset(frame), out, 0, len)
        let len = (frame_count as u64) * self.frame_size();
        let h = self.file.try_clone().await?;
        let mut range = h.take(len);
        tokio::io::copy(&mut range, w).await?;
        Ok(())
    }

    /// Reads a range of next consecutive frames, including headers, into given buffer.
    /// Returns a number of frames read this way.
    ///
    /// # Errors
    ///
    /// This function will propagate any WAL file I/O errors.
    /// It will return an error if provided `buf` length is not multiplication of an underlying
    /// WAL frame size.
    /// It will return an error if at least one frame was not fully read.
    #[allow(dead_code)]
    pub async fn read_frame_range(&mut self, buf: &mut [u8]) -> Result<usize> {
        let frame_size = self.frame_size() as usize;
        if buf.len() % frame_size != 0 {
            return Err(anyhow!("Provided buffer doesn't fit full frames"));
        }
        let read = self.file.read_exact(buf).await?;
        if read % frame_size != 0 {
            Err(anyhow!("Some of the read frames where not complete"))
        } else {
            Ok(read / frame_size)
        }
    }

    #[allow(dead_code)]
    pub async fn next_frame(&mut self, page: &mut [u8]) -> Result<Option<WalFrameHeader>> {
        debug_assert_eq!(page.len(), self.page_size() as usize);
        let header = self.read_frame_header().await?;
        if header.is_some() {
            self.file.read_exact(page).await?;
        }
        Ok(header)
    }
}

impl AsMut<File> for WalFileReader {
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

/// Generate or extend an 8 byte checksum based on the data in
///the `page` and the `init` value. `page` size must be multiple of 8.
/// FIXME: these computations are performed with host endianness,
/// which is softly assumed to be little endian for majority of devices.
/// However, the only proper way to do this is to get the endianness
/// from the WAL header, as per https://www.sqlite.org/fileformat.html#checksum_algorithm
pub fn checksum_step(init: (u32, u32), page: &[u8]) -> (u32, u32) {
    debug_assert_eq!(page.len() % 8, 0);
    let (mut s0, mut s1) = init;
    let page = unsafe { std::slice::from_raw_parts(page.as_ptr() as *const u32, page.len() / 4) };
    let mut i = 0;
    while i < page.len() {
        s0 = s0.wrapping_add(page[i]).wrapping_add(s1);
        s1 = s1.wrapping_add(page[i + 1]).wrapping_add(s0);
        i += 2;
    }
    (s0, s1)
}

#[cfg(test)]
mod test {
    use crate::wal::WalHeader;

    #[test]
    fn wal_header_mem_mapping() {
        // copied from actual SQLite WAL file
        let source = [
            55, 127, 6, 130, 0, 45, 226, 24, 0, 0, 16, 0, 0, 0, 0, 0, 190, 6, 47, 124, 39, 191, 98,
            92, 81, 22, 9, 209, 101, 96, 160, 157,
        ];
        let expected = WalHeader {
            magic_no: 0x377f0682,
            version: 3007000,
            page_size: 4096,
            checkpoint_seq_no: 0,
            salt_1: 3188076412,
            salt_2: 666853980,
            checksum_1: 1360398801,
            checksum_2: 1700831389,
        };
        let actual = WalHeader::from(source);
        assert_eq!(actual, expected);
    }
}
