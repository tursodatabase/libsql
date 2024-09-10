use std::ops::Deref as _;

use crate::storage::compaction::SegmentSet;

use super::PartitionStrategy;

/// partition the SegmentSet in logarithmically reducing sets
pub struct LogReductionStrategy;

impl PartitionStrategy for LogReductionStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet> {
        let mut segs = segments.deref();
        let mut out = Vec::new();
        while !segs.is_empty() {
            let (lhs, rhs) = segs.split_at(segs.len() / 2);
            out.push(SegmentSet {
                segments: lhs.to_vec(),
                namespace: segments.namespace.clone(),
            });
            segs = rhs;
            if segs.len() == 1 {
                out.push(SegmentSet {
                    segments: rhs.to_vec(),
                    namespace: segments.namespace.clone(),
                });
                break;
            }
        }

        out
    }
}
