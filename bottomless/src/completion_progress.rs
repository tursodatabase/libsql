use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::watch::{channel, Receiver, Sender};
use uuid::Uuid;

#[derive(Debug)]
pub struct SavepointTracker {
    next_frame_no: Arc<AtomicU32>,
    receiver: Receiver<u32>,
    generation: Arc<ArcSwapOption<Uuid>>,
    generation_snapshot: Receiver<Result<Option<Uuid>>>,
    db_path: String,
}

impl SavepointTracker {
    pub(crate) fn new(
        generation: Arc<ArcSwapOption<Uuid>>,
        generation_snapshot: Receiver<Result<Option<Uuid>>>,
        next_frame_no: Arc<AtomicU32>,
        receiver: Receiver<u32>,
        db_path: String,
    ) -> Self {
        SavepointTracker {
            generation,
            generation_snapshot,
            next_frame_no,
            receiver,
            db_path,
        }
    }

    pub async fn confirm_snapshotted(&mut self) -> Result<Option<Uuid>> {
        if Path::new(&self.db_path).try_exists()? {
            if let Some(generation) = self.generation.load_full() {
                let res = self
                    .generation_snapshot
                    .wait_for(|gen| match gen {
                        Ok(Some(gen)) => gen == &*generation,
                        Ok(None) => false,
                        Err(_) => true,
                    })
                    .await?;
                return match &*res {
                    Ok(gen) => Ok(gen.clone()),
                    Err(e) => bail!(e.to_string()),
                };
            }
        }
        Ok(None)
    }

    /// Wait until WAL segment upload has been confirmed up until the frame which number has been
    /// snapshotted at the beginning of the call.
    pub async fn confirmed(&mut self) -> Result<BackupThreshold> {
        let last_frame_no = self.next_frame_no.load(Ordering::SeqCst) - 1;
        let frame_no = *self.receiver.wait_for(|fno| *fno >= last_frame_no).await?;
        let generation = self.confirm_snapshotted().await?;
        let t = BackupThreshold {
            generation: generation.as_ref().map(Uuid::to_string),
            frame_no,
        };
        tracing::debug!(
            "confirmed backup savepoint for generation `{:?}`, frame no.: {}",
            t.generation,
            t.frame_no
        );
        Ok(t)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupThreshold {
    pub generation: Option<String>,
    pub frame_no: u32,
}

/// Track completion progress for WAL frame segments uploaded in parallel.
#[derive(Debug)]
pub(crate) struct CompletionProgress {
    baseline: u32,
    detached_ranges: BTreeMap<u32, u32>,
    tx: Sender<u32>,
}

impl CompletionProgress {
    pub fn new(baseline: u32) -> (Self, Receiver<u32>) {
        let (tx, rx) = channel(baseline);
        let completion = CompletionProgress {
            baseline,
            detached_ranges: BTreeMap::new(),
            tx,
        };
        (completion, rx)
    }

    pub fn update(&mut self, mut start_frame: u32, mut end_frame: u32) {
        if start_frame == self.baseline + 1 {
            loop {
                self.baseline = end_frame;
                if let Some((s, e)) = self.detached_ranges.pop_first() {
                    start_frame = s;
                    end_frame = e;
                    if start_frame != self.baseline + 1 {
                        self.detached_ranges.insert(start_frame, end_frame);
                        break;
                    }
                } else {
                    break;
                }
            }
            self.tx.send_replace(self.baseline);
        } else {
            self.detached_ranges.insert(start_frame, end_frame);
        }
    }

    pub fn reset(&mut self) {
        self.baseline = 0;
        self.detached_ranges.clear();
        self.tx.send_replace(0);
    }
}

#[cfg(test)]
mod test {
    use crate::completion_progress::CompletionProgress;

    #[test]
    fn completion_progress_update() {
        let (mut p, rx) = CompletionProgress::new(0);
        p.update(1, 4);
        assert_eq!(*rx.borrow(), 4);

        p.update(5, 7);
        assert_eq!(*rx.borrow(), 7);

        p.update(9, 10); // hole: missing 8
        assert_eq!(*rx.borrow(), 7);

        p.update(13, 14); // 3 holes: missing 8, 11, 12
        assert_eq!(*rx.borrow(), 7);

        p.update(15, 20);
        assert_eq!(*rx.borrow(), 7);

        p.update(8, 8); // 2 holes: missing 11, 12
        assert_eq!(*rx.borrow(), 10);

        p.update(11, 11); // hole: missing 12
        assert_eq!(*rx.borrow(), 11);

        p.update(12, 12);
        assert_eq!(*rx.borrow(), 20);
    }
}
