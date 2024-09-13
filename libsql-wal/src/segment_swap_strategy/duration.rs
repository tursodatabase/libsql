use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::SegmentSwapStrategy;

/// A wal swap strategy that swaps the current wal if it's older that some duration
pub struct DurationSwapStrategy {
    swap_after: Duration,
    last_swapped_at: Mutex<Instant>,
}

impl DurationSwapStrategy {
    pub fn new(swap_after: Duration) -> Self {
        Self {
            swap_after,
            last_swapped_at: Mutex::new(Instant::now()),
        }
    }
}

impl SegmentSwapStrategy for DurationSwapStrategy {
    #[inline(always)]
    fn should_swap(&self, _frames_in_wal: usize) -> bool {
        let last_swapped_at = self.last_swapped_at.lock();
        last_swapped_at.elapsed() >= self.swap_after
    }

    #[inline(always)]
    fn swapped(&self) {
        *self.last_swapped_at.lock() = Instant::now();
    }
}
