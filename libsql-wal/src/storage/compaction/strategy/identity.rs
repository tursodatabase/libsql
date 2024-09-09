use crate::storage::compaction::SegmentSet;

use super::PartitionStrategy;

/// partition strategy that doesn't split the passed set
pub struct IdentityStrategy;

impl PartitionStrategy for IdentityStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet> {
        let mut out = Vec::with_capacity(1);
        out.push(segments.clone());
        out
    }
}
