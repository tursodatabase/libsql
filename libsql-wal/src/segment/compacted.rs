use zerocopy::little_endian::{U128 as lu128, U32 as lu32, U64 as lu64, U16 as lu16};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataHeader {
    pub(crate) magic: lu64,
    pub(crate) version: lu16,
    pub(crate) frame_count: lu32,
    pub(crate) segment_id: lu128,
    pub(crate) start_frame_no: lu64,
    pub(crate) end_frame_no: lu64,
    pub(crate) size_after: lu32,
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataFooter {
    pub(crate) checksum: lu32,
}
