use std::sync::Arc;
use std::task::{ready, Poll};
use std::{pin::Pin, task::Context};

use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use libsql_replication::frame::{Frame, FrameNo};

use crate::replication::{LogReadError, ReplicationLogger};
use crate::BLOCKING_RT;

/// Streams frames from the replication log starting at `current_frame_no`.
/// Only stops if the current frame is not in the log anymore.
pub struct FrameStream {
    pub(crate) current_frame_no: FrameNo,
    pub(crate) max_available_frame_no: Option<FrameNo>,
    logger: Arc<ReplicationLogger>,
    state: FrameStreamState,
    wait_for_more: bool,
    // number of frames produced in this stream
    produced_frames: usize,
    // max number of frames to produce before ending the stream
    max_frames: Option<usize>,
    /// a future that resolves when the logger was closed.
    logger_closed_fut: BoxFuture<'static, ()>,
    /// whether a stream is in-between transactions (last frame ended a transaction)
    transaction_boundary: bool,
}

impl FrameStream {
    pub fn new(
        logger: Arc<ReplicationLogger>,
        current_frameno: FrameNo,
        wait_for_more: bool,
        max_frames: Option<usize>,
    ) -> crate::Result<Self> {
        let max_available_frame_no = *logger.new_frame_notifier.subscribe().borrow();
        let mut sub = logger.closed_signal.subscribe();
        let logger_closed_fut = Box::pin(async move {
            let _ = sub.wait_for(|x| *x).await;
        });

        Ok(Self {
            current_frame_no: current_frameno,
            max_available_frame_no,
            logger,
            state: FrameStreamState::Init,
            wait_for_more,
            produced_frames: 0,
            max_frames,
            logger_closed_fut,
            transaction_boundary: false,
        })
    }

    fn transition_state_next_frame(&mut self) {
        if matches!(self.state, FrameStreamState::Closed) {
            return;
        }
        if let Some(max_frames) = self.max_frames {
            if self.produced_frames >= max_frames {
                tracing::trace!(
                    "Max number of frames reached ({} >= {max_frames})",
                    self.produced_frames
                );
                if self.transaction_boundary {
                    tracing::debug!("Closing stream");
                    self.state = FrameStreamState::Closed;
                    return;
                }
            }
        }

        let next_frameno = self.current_frame_no;
        let logger = self.logger.clone();
        let fut = async move {
            let res = BLOCKING_RT
                .spawn_blocking(move || logger.get_frame(next_frameno))
                .await;
            match res {
                Ok(Ok(frame)) => Ok(frame),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(LogReadError::Error(e.into())),
            }
        };

        self.state = FrameStreamState::WaitingFrame(Box::pin(fut));
    }
}

enum FrameStreamState {
    Init,
    /// waiting for new frames to replicate
    WaitingFrameNo(BoxFuture<'static, anyhow::Result<Option<FrameNo>>>),
    WaitingFrame(BoxFuture<'static, Result<Frame, LogReadError>>),
    Closed,
}

impl Stream for FrameStream {
    type Item = Result<Frame, LogReadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.logger_closed_fut.poll_unpin(cx).is_ready() {
            return Poll::Ready(Some(Err(LogReadError::Error(anyhow::anyhow!(
                "logger closed"
            )))));
        }

        match self.state {
            FrameStreamState::Init => {
                self.transition_state_next_frame();
                self.poll_next(cx)
            }
            FrameStreamState::WaitingFrameNo(ref mut fut) => {
                self.max_available_frame_no = match ready!(fut.as_mut().poll(cx)) {
                    Ok(frame_no) => frame_no,
                    Err(e) => {
                        self.state = FrameStreamState::Closed;
                        return Poll::Ready(Some(Err(LogReadError::Error(e))));
                    }
                };
                self.transition_state_next_frame();
                self.poll_next(cx)
            }
            FrameStreamState::WaitingFrame(ref mut fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(frame) => {
                    self.current_frame_no += 1;
                    self.produced_frames += 1;
                    self.transaction_boundary = frame.header().size_after != 0;
                    self.transition_state_next_frame();
                    tracing::trace!("sending frame_no {}", frame.header().frame_no);
                    Poll::Ready(Some(Ok(frame)))
                }

                Err(LogReadError::Ahead) => {
                    // If we don't wait to wait for more then lets end this stream
                    // without subscribing for more frames
                    if !self.wait_for_more {
                        self.state = FrameStreamState::Closed;
                        return Poll::Ready(None);
                    }

                    let mut notifier = self.logger.new_frame_notifier.subscribe();
                    let max_available_frame_no = *notifier.borrow();
                    // check in case value has already changed, otherwise we'll be notified later
                    if max_available_frame_no > self.max_available_frame_no {
                        self.max_available_frame_no = max_available_frame_no;
                        self.transition_state_next_frame();
                        self.poll_next(cx)
                    } else {
                        let fut = async move {
                            notifier.changed().await?;
                            Ok(*notifier.borrow())
                        };
                        self.state = FrameStreamState::WaitingFrameNo(Box::pin(fut));
                        self.poll_next(cx)
                    }
                }
                Err(e) => {
                    self.state = FrameStreamState::Closed;
                    Poll::Ready(Some(Err(e)))
                }
            },
            FrameStreamState::Closed => Poll::Ready(None),
        }
    }
}
