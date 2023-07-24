use std::sync::Arc;
use std::task::{ready, Poll};
use std::{pin::Pin, task::Context};

use futures::future::BoxFuture;
use futures::Stream;

use crate::replication::frame::Frame;
use crate::replication::{FrameNo, LogReadError, ReplicationLogger};

/// Streams frames from the replication log starting at `current_frame_no`.
/// Only stops if the current frame is not in the log anymore.
pub struct FrameStream {
    pub(crate) current_frame_no: FrameNo,
    pub(crate) max_available_frame_no: FrameNo,
    logger: Arc<ReplicationLogger>,
    state: FrameStreamState,
}

impl FrameStream {
    pub fn new(logger: Arc<ReplicationLogger>, current_frameno: FrameNo) -> Self {
        let max_available_frame_no = *logger.new_frame_notifier.subscribe().borrow();
        Self {
            current_frame_no: current_frameno,
            max_available_frame_no,
            logger,
            state: FrameStreamState::Init,
        }
    }

    fn transition_state_next_frame(&mut self) {
        if matches!(self.state, FrameStreamState::Closed) {
            return;
        }

        let next_frameno = self.current_frame_no;
        let logger = self.logger.clone();
        let fut = async move {
            let res = tokio::task::spawn_blocking(move || logger.get_frame(next_frameno)).await;
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
    WaitingFrameNo(BoxFuture<'static, anyhow::Result<FrameNo>>),
    WaitingFrame(BoxFuture<'static, Result<Frame, LogReadError>>),
    Closed,
}

impl Stream for FrameStream {
    type Item = Result<Frame, LogReadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
                    self.transition_state_next_frame();
                    Poll::Ready(Some(Ok(frame)))
                }

                Err(LogReadError::Ahead) => {
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
