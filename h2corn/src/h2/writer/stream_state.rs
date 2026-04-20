use std::{collections::VecDeque, mem};

use crate::bridge::PayloadBytes;
use crate::error::{ErrorExt, H2CornError, H2Error, HttpResponseError};
use crate::frame::StreamId;
use crate::h2::StreamMap;
use crate::http::pathsend::PathStreamer;
use crate::http::types::ResponseHeaders;
use crate::smallvec_deque::SmallVecDeque;

use super::ResponseCloseBatch;

#[derive(Debug)]
pub(super) struct PendingChunk {
    bytes: PayloadBytes,
    offset: usize,
    pub(super) end_stream: bool,
}

pub(super) type PendingChunks = SmallVecDeque<PendingChunk, 2>;

#[derive(Debug)]
pub(super) enum StreamBodyState {
    Idle,
    Chunks(PendingChunks),
    Path(PathStreamer),
}

#[derive(Debug)]
pub(super) enum ResponseWriteState {
    AwaitingHeaders,
    Open {
        body: StreamBodyState,
        trailers: Option<ResponseHeaders>,
    },
    Closed,
}

#[derive(Debug)]
pub(super) struct StreamWriteState {
    pub(super) send_window: i64,
    pub(super) scheduled: bool,
    response: ResponseWriteState,
}

impl StreamWriteState {
    pub(super) fn new(initial_window: i64) -> Self {
        Self {
            send_window: initial_window,
            scheduled: false,
            response: ResponseWriteState::AwaitingHeaders,
        }
    }

    pub(super) fn open_response(&mut self, end_stream: bool) -> Result<(), H2CornError> {
        if !matches!(self.response, ResponseWriteState::AwaitingHeaders) {
            return H2Error::ResponseHeadersAlreadySent.err();
        }
        self.response = if end_stream {
            ResponseWriteState::Closed
        } else {
            ResponseWriteState::Open {
                body: StreamBodyState::Idle,
                trailers: None,
            }
        };
        Ok(())
    }

    pub(super) fn is_closed(&self) -> bool {
        matches!(self.response, ResponseWriteState::Closed)
    }

    pub(super) fn has_pending_output(&self) -> bool {
        match &self.response {
            ResponseWriteState::Open { body, .. } => body.has_pending_output(),
            ResponseWriteState::AwaitingHeaders | ResponseWriteState::Closed => false,
        }
    }

    pub(super) fn schedule(
        &mut self,
        ready_streams: &mut VecDeque<u32>,
        stream_id: StreamId,
        front: bool,
    ) {
        if self.scheduled {
            return;
        }
        self.scheduled = true;
        if front {
            ready_streams.push_front(stream_id.get());
        } else {
            ready_streams.push_back(stream_id.get());
        }
    }

    pub(super) fn take_body(&mut self) -> StreamBodyState {
        match &mut self.response {
            ResponseWriteState::Open { body, .. } if body.has_pending_output() => {
                mem::replace(body, StreamBodyState::Idle)
            }
            ResponseWriteState::Open { .. }
            | ResponseWriteState::AwaitingHeaders
            | ResponseWriteState::Closed => StreamBodyState::Idle,
        }
    }

    pub(super) fn restore_body(&mut self, body: StreamBodyState) {
        match &mut self.response {
            ResponseWriteState::Open { body: current, .. } => {
                *current = body.normalized();
            }
            ResponseWriteState::AwaitingHeaders | ResponseWriteState::Closed => {}
        }
    }

    pub(super) fn take_trailers_if_body_idle(&mut self) -> Option<ResponseHeaders> {
        match &mut self.response {
            ResponseWriteState::Open { body, trailers } if body.is_idle() => trailers.take(),
            ResponseWriteState::Open { .. }
            | ResponseWriteState::AwaitingHeaders
            | ResponseWriteState::Closed => None,
        }
    }

    pub(super) fn queue_trailers(&mut self, headers: ResponseHeaders) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders | ResponseWriteState::Closed => {
                return H2Error::ResponseTrailersOnClosedOrUnopenedStream.err();
            }
            ResponseWriteState::Open { trailers, .. } => {
                if trailers.is_some() {
                    return H2Error::ResponseTrailersAlreadySent.err();
                }
                *trailers = Some(headers);
            }
        }
        Ok(())
    }

    pub(super) fn queue_data(
        &mut self,
        data: PayloadBytes,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders => {
                return H2Error::DataBeforeResponseHeaders.err();
            }
            ResponseWriteState::Closed => {
                return H2Error::DataOnClosedStream.err();
            }
            ResponseWriteState::Open { body, .. } => {
                let chunk = PendingChunk {
                    bytes: data,
                    offset: 0,
                    end_stream,
                };
                match body {
                    StreamBodyState::Idle => {
                        let mut chunks = PendingChunks::new();
                        chunks.push_back(chunk);
                        *body = StreamBodyState::Chunks(chunks);
                    }
                    StreamBodyState::Chunks(pending) => pending.push_back(chunk),
                    StreamBodyState::Path(_) => {
                        return HttpResponseError::PathsendMixedWithBody.err();
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn queue_path(&mut self, streamer: PathStreamer) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders => {
                return H2Error::PathDataBeforeResponseHeaders.err();
            }
            ResponseWriteState::Closed => {
                return H2Error::PathDataOnClosedStream.err();
            }
            ResponseWriteState::Open { body, .. } => {
                if !body.is_idle() {
                    return HttpResponseError::PathsendMixedWithBody.err();
                }
                *body = StreamBodyState::Path(streamer);
            }
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, stream_id: StreamId, response_closes: &mut ResponseCloseBatch) {
        self.response = ResponseWriteState::Closed;
        notify_response_close(response_closes, stream_id);
    }
}

impl StreamBodyState {
    pub(super) fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub(super) fn has_pending_output(&self) -> bool {
        match self {
            Self::Idle => false,
            Self::Chunks(chunks) => !chunks.is_empty(),
            Self::Path(_) => true,
        }
    }

    fn normalized(self) -> Self {
        match self {
            Self::Chunks(chunks) if chunks.is_empty() => Self::Idle,
            Self::Chunks(chunks) => Self::Chunks(chunks),
            Self::Path(streamer) if streamer.is_drained() && !streamer.end_stream => Self::Idle,
            other => other,
        }
    }
}

impl PendingChunk {
    pub(super) fn remaining(&self) -> &[u8] {
        &self.bytes.as_ref()[self.offset..]
    }

    pub(super) fn consume(&mut self, len: usize) {
        self.offset += len;
    }

    pub(super) fn is_drained(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

pub(super) fn writer_stream(
    streams: &mut StreamMap<StreamWriteState>,
    stream_id: StreamId,
    initial_stream_send_window: i64,
) -> &mut StreamWriteState {
    streams
        .entry(stream_id.get())
        .or_insert_with(|| StreamWriteState::new(initial_stream_send_window))
}

pub(super) fn notify_response_close(response_closes: &mut ResponseCloseBatch, stream_id: StreamId) {
    response_closes.push(stream_id);
}
