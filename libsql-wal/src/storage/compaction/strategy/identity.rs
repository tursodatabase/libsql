use crate::storage::compaction::SegmentSet;

use super::CompactionStrategy;

/// partition strategy that doesn't split the passed set
pub struct IdentityStrategy;

impl CompactionStrategy for IdentityStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet> {
        let mut out = Vec::with_capacity(1);
        out.push(segments.clone());
        out
    }
}
