use super::SegmentSet;

pub mod identity;
pub mod log_strategy;

pub trait PartitionStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet>;
}
