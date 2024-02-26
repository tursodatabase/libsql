use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
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
    pub generation: Arc<ArcSwapOption<Uuid>>,
    pub generation_snapshot: Receiver<Result<Option<Uuid>>>,
    pub db_path: String,
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
                        Err(e) => true,
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
    pub async fn confirmed(&mut self) -> Result<u32> {
        let last_frame_no = self.next_frame_no.load(Ordering::SeqCst) - 1;
        let res = *self.receiver.wait_for(|fno| *fno >= last_frame_no).await?;
        self.confirm_snapshotted().await?;
        Ok(res)
    }
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
        if start_frame - 1 == self.baseline {
            while start_frame - 1 == self.baseline {
                self.baseline = end_frame;
                if let Some((s, e)) = self.detached_ranges.pop_first() {
                    start_frame = s;
                    end_frame = e;
                } else {
                    break;
                }
            }
            self.tx.send_replace(self.baseline);
        } else {
            self.detached_ranges.insert(start_frame, end_frame);
        }
    }

    pub(crate) fn subscribe(&mut self) -> Receiver<u32> {
        self.tx.subscribe()
    }
}
