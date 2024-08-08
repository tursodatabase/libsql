use std::mem::size_of;

use libsql_wal::io::StdIO;
use libsql_wal::replication::injector::Injector;
use libsql_wal::segment::Frame as WalFrame;
use zerocopy::{AsBytes, FromZeroes};

use crate::frame::{Frame, FrameNo};

use super::error::{Error, Result};

pub struct LibsqlInjector {
    injector: Injector<StdIO>,
}

impl super::Injector for LibsqlInjector {
    async fn inject_frame(&mut self, frame: Frame) -> Result<Option<FrameNo>> {
        // this is a bit annoying be we want to read the frame, and it has to be aligned, so we
        // must copy it...
        // FIXME: optimize this.
        let mut wal_frame = WalFrame::new_box_zeroed();
        if frame.bytes().len() != size_of::<WalFrame>() {
            todo!("invalid frame");
        }
        wal_frame.as_bytes_mut().copy_from_slice(&frame.bytes()[..]);
        Ok(self
            .injector
            .insert_frame(wal_frame)
            .await
            .map_err(|e| Error::FatalInjectError(e.into()))?)
    }

    async fn rollback(&mut self) {
        self.injector.rollback();
    }

    async fn flush(&mut self) -> Result<Option<FrameNo>> {
        self.injector
            .flush(None)
            .await
            .map_err(|e| Error::FatalInjectError(e.into()))?;
        Ok(None)
    }
}
