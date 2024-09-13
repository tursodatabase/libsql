use super::SegmentSwapStrategy;

/// A swap strategy that swaps if the count of frames in the wal exceed some threshold
pub struct FrameCountSwapStrategy {
    max_frames_in_wal: usize,
}

impl FrameCountSwapStrategy {
    pub fn new(max_frames_in_wal: usize) -> Self {
        Self { max_frames_in_wal }
    }
}

impl SegmentSwapStrategy for FrameCountSwapStrategy {
    #[inline(always)]
    fn should_swap(&self, frames_in_wal: usize) -> bool {
        frames_in_wal >= self.max_frames_in_wal
    }

    #[inline(always)]
    fn swapped(&self) {}
}
