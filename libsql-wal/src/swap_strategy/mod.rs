pub(crate) mod duration;
pub(crate) mod frame_count;

pub(crate) trait SwapStrategy: Sync + Send + 'static {
    fn should_swap(&self, frames_in_wal: usize) -> bool;
    fn swapped(&self);
}
