use super::SegmentSet;

pub mod identity;
pub mod log_strategy;
pub mod tiered;

pub trait CompactionStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet>;
}
