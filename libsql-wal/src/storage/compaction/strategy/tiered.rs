//! The `LevelsStrategy` is a partial compaction strategy that compacts segments in increasingly
//! bigger sizes. Is squashes segments starting from the most recent, and working towards the
//! oldest. Segments are added to the result set as follow:
//! - if the number of segments in the set is less than the strategy's threshold, no compaction
//! occurs
//! - else, add all segments above the threshold to the result set
//! - while the span of the result set is greater than the next considered segment, add that
//! segmetn to the set too.
//!
//! In the following example, we identify segments to their span for clarity:
//!
//! initial : input_set: [10, 7, 21, 14] result_set: [], threshold: 3
//! step 1  => input_set: [10, 7] result_set: [21, 14]              | span of the result set: 21 + 14 = 35
//! step 2  => input_set: [10] result_set: [7, 21, 14]              | 35 > 7, new span: 35 + 7 = 42
//! step 3  => input_set: [] result_set: [10, 7, 21, 14]            | 42 > 10, new span: 42 + 10 = 52
//! resulting segment (after compaction) [52]
//! after more segment are added to the set:
//! [52, 5, 6, 4] -> [52, 15] (15 is less than 52)
//!
//! remarks:
//! - this compaction will always shrink the input set size, as long as it's length is greater than
//! the strategy threshold: the input set will at worst be threshold - 1 segments long
//! - segments grow toward the size of the first segment in the set. The goal is to delays having
//! to merge large segments, so that we don't need to fetch them too often, and write back large
//! segments to the storage. The idea is that we maximize the 'useful' information contained by
//! smaller segments toward the head of the set.
use crate::storage::{compaction::SegmentSet, SegmentKey};

use super::CompactionStrategy;

pub struct LevelsStrategy {
    threshold: usize,
}

impl LevelsStrategy {
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }
}

impl CompactionStrategy for LevelsStrategy {
    fn partition(&self, segments: &SegmentSet) -> Vec<SegmentSet> {
        // no need to compact, not enough segments
        if segments.len() < self.threshold {
            return Vec::new();
        }

        let overflow = segments.len() - self.threshold + 1;

        let mut out = segments
            .iter()
            .rev()
            .cloned()
            .take(overflow)
            .collect::<Vec<_>>();

        let remaining_segs = &segments[..segments.len() - overflow];

        for seg in remaining_segs.iter().rev() {
            if span(&out) >= seg.span() {
                out.push(seg.clone());
            }
        }

        // segments are in inverted order, so we need to reverse the array
        out.reverse();

        vec![SegmentSet {
            namespace: segments.namespace.clone(),
            segments: out,
        }]
    }
}

// returns the amount of frame spanned by the passed set of segments.
//
// the passed set is expected to be non-emtpy, in reverse segment order
fn span(segs: &[SegmentKey]) -> u64 {
    debug_assert!(!segs.is_empty());
    debug_assert!(segs.first().unwrap().start_frame_no >= segs.last().unwrap().start_frame_no);

    segs.first().unwrap().end_frame_no - segs.last().unwrap().start_frame_no
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use libsql_sys::name::NamespaceName;

    use super::*;

    #[test]
    fn partition_tiered() {
        let ns = NamespaceName::from_string("test".into());
        let s = LevelsStrategy { threshold: 5 };
        let mut set = SegmentSet {
            namespace: ns.clone(),
            segments: vec![
                SegmentKey {
                    start_frame_no: 1,
                    end_frame_no: 20,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 21,
                    end_frame_no: 27,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 28,
                    end_frame_no: 41,
                    timestamp: 0,
                },
            ],
        };

        assert!(s.partition(&set).is_empty());

        set.segments.push(SegmentKey {
            start_frame_no: 42,
            end_frame_no: 70,
            timestamp: 0,
        });
        set.segments.push(SegmentKey {
            start_frame_no: 71,
            end_frame_no: 81,
            timestamp: 0,
        });
        set.segments.push(SegmentKey {
            start_frame_no: 82,
            end_frame_no: 100,
            timestamp: 0,
        });

        let partition = s.partition(&set);
        assert_eq!(partition.len(), 1);
        // we should compact all segments into one
        assert_debug_snapshot!(partition.first().unwrap());

        let set = SegmentSet {
            namespace: ns.clone(),
            segments: vec![
                SegmentKey {
                    start_frame_no: 1,
                    end_frame_no: 100,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 101,
                    end_frame_no: 105,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 106,
                    end_frame_no: 110,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 111,
                    end_frame_no: 115,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 116,
                    end_frame_no: 120,
                    timestamp: 0,
                },
                SegmentKey {
                    start_frame_no: 121,
                    end_frame_no: 122,
                    timestamp: 0,
                },
            ],
        };

        let partition = s.partition(&set);
        assert_eq!(partition.len(), 1);
        assert_debug_snapshot!(partition.first().unwrap());
    }
}
