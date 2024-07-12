use zerocopy::little_endian::{U128 as lu128, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataHeader {
    pub(crate) frame_count: lu64,
    pub(crate) segment_id: lu128,
    pub(crate) start_frame_no: lu64,
    pub(crate) end_frame_no: lu64,
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataFooter {
    pub(crate) checksum: lu32,
}
