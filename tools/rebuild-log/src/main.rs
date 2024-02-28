use std::collections::HashSet;
use std::{fs::File, mem::size_of, os::unix::prelude::FileExt};

use anyhow::bail;
use zerocopy::byteorder::little_endian::{
    I32 as li32, U128 as lu128, U16 as lu16, U32 as lu32, U64 as lu64,
};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const FRAME_SIZE: usize = std::mem::size_of::<FrameHeader>() + LIBSQL_PAGE_SIZE as usize;
const LIBSQL_PAGE_SIZE: u64 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let log_file = args.nth(1).expect("expected first arg for log file");
    let db_file = args.next().expect("expected 2nd db file location arg");

    let mut log_file = std::fs::File::open(log_file).unwrap();
    let db_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(db_file)
        .unwrap();

    println!("reading log header");

    let mut buf = [0; size_of::<LogFileHeader>()];
    log_file.read_exact_at(&mut buf, 0)?;
    let header =
        LogFileHeader::read_from(&buf).ok_or_else(|| anyhow::anyhow!("invalid log file header"))?;

    if header.magic.get() != WAL_MAGIC {
        bail!("invalid replication log header");
    }

    let mut current_frame_offset = header.frame_count.get();

    println!("starting to recover log");

    let mut page_seen = HashSet::new();

    loop {
        if current_frame_offset == 0 {
            break;
        }

        current_frame_offset -= 1;

        let read_byte_offset = absolute_byte_offset(current_frame_offset);
        let frame = read_frame_byte_offset_mut(&mut log_file, read_byte_offset).unwrap();

        let page_no = frame.header.page_no.get();

        if page_seen.get(&page_no).is_none() {
            page_seen.insert(page_no);

            let offset = (page_no - 1) * LIBSQL_PAGE_SIZE as u32;

            db_file.write_at(&frame.page[..], offset as u64).unwrap();
        }
    }

    println!("finished recovering {} frames", header.frame_count.get());

    Ok(())
}

fn read_frame_byte_offset_mut(file: &mut File, offset: u64) -> anyhow::Result<Box<FrameBorrowed>> {
    let mut frame = FrameBorrowed::new_zeroed();
    file.read_exact_at(frame.as_bytes_mut(), offset)?;

    Ok(frame.into())
}

/// The borrowed version of Frame
#[repr(C)]
#[derive(Copy, Clone, zerocopy::AsBytes, zerocopy::FromZeroes, zerocopy::FromBytes)]
pub struct FrameBorrowed {
    header: FrameHeader,
    page: [u8; LIBSQL_PAGE_SIZE as usize],
}

#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes)]
#[repr(C)]
pub struct LogFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    pub magic: lu64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    pub start_checksum: lu64,
    /// Uuid of the this log.
    pub log_id: lu128,
    /// Frame_no of the first frame in the log
    pub start_frame_no: lu64,
    /// entry count in file
    pub frame_count: lu64,
    /// Wal file version number, currently: 2
    pub version: lu32,
    /// page size: 4096
    pub page_size: li32,
    /// sqld version when creating this log
    pub sqld_version: [lu16; 4],
}

fn absolute_byte_offset(nth: u64) -> u64 {
    std::mem::size_of::<LogFileHeader>() as u64 + nth * FRAME_SIZE as u64
}

#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromZeroes, zerocopy::FromBytes, zerocopy::AsBytes)]
pub struct FrameHeader {
    /// Incremental frame number
    pub frame_no: lu64,
    /// Rolling checksum of all the previous frames, including this one.
    pub checksum: lu64,
    /// page number, if frame_type is FrameType::Page
    pub page_no: lu32,
    /// Size of the database (in page) after committing the transaction. This is passed from sqlite,
    /// and serves as commit transaction boundary
    pub size_after: lu32,
}
