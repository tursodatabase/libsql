use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::watch::{channel, Receiver, Sender};

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
}
