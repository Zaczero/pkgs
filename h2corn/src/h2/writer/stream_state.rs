use std::collections::VecDeque;
use std::mem;

use tokio::time::Instant;

use super::{PrefixedData, ResponseCloseBatch};
use crate::bridge::PayloadBytes;
use crate::error::{ErrorExt, H2CornError, H2Error, HttpResponseError};
use crate::h2::StreamMap;
use crate::h2_frame::StreamId;
use crate::http::pathsend::PathStreamer;
use crate::http::types::ResponseHeaders;
use crate::inline_fifo::InlineFifo;

#[derive(Debug)]
pub(super) struct PendingChunk {
    data: PendingChunkData,
    offset: usize,
    pub(super) end_stream: bool,
}

#[derive(Debug)]
enum PendingChunkData {
    Plain(PayloadBytes),
    Prefixed(Box<PrefixedData>),
}

pub(super) type PendingChunks = InlineFifo<PendingChunk, 2>;

#[derive(Debug)]
pub(super) struct ReadyStreamQueue {
    queue: VecDeque<StreamId>,
}

impl ReadyStreamQueue {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    pub(super) const fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(super) fn schedule(
        &mut self,
        stream: &mut StreamWriteState,
        stream_id: StreamId,
        front: bool,
    ) {
        if stream.scheduled {
            return;
        }
        stream.scheduled = true;
        if front {
            self.queue.push_front(stream_id);
        } else {
            self.queue.push_back(stream_id);
        }
    }

    pub(super) fn pop_scheduled(
        &mut self,
        streams: &mut StreamMap<StreamWriteState>,
    ) -> Option<StreamId> {
        while let Some(stream_id) = self.queue.pop_front() {
            let Some(stream) = streams.get_mut(&stream_id) else {
                continue;
            };
            debug_assert!(
                stream.scheduled,
                "ready queue contains only scheduled streams"
            );
            stream.scheduled = false;
            return Some(stream_id);
        }
        None
    }

    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.queue.iter().copied()
    }
}

#[derive(Debug)]
pub(super) enum StreamBodyState {
    Idle,
    Chunks(PendingChunks),
    // The one-buffer std-file streamer is 64 bytes, smaller than the common
    // inline chunk queue, so storing it directly removes one pathsend-only
    // allocation without growing `StreamWriteState`.
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
    pending_body_since: Option<Instant>,
    response: ResponseWriteState,
}

impl StreamWriteState {
    pub(super) const fn new(initial_window: i64) -> Self {
        Self {
            send_window: initial_window,
            scheduled: false,
            pending_body_since: None,
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

    pub(super) const fn is_closed(&self) -> bool {
        matches!(self.response, ResponseWriteState::Closed)
    }

    pub(super) fn has_pending_output(&self) -> bool {
        match &self.response {
            ResponseWriteState::Open { body, .. } => body.has_pending_output(),
            ResponseWriteState::AwaitingHeaders | ResponseWriteState::Closed => false,
        }
    }

    pub(super) fn take_body(&mut self) -> StreamBodyState {
        match &mut self.response {
            ResponseWriteState::Open { body, .. } if body.has_pending_output() => {
                mem::replace(body, StreamBodyState::Idle)
            },
            ResponseWriteState::Open { .. }
            | ResponseWriteState::AwaitingHeaders
            | ResponseWriteState::Closed => StreamBodyState::Idle,
        }
    }

    pub(super) fn restore_body(&mut self, body: StreamBodyState) {
        match &mut self.response {
            ResponseWriteState::Open { body: current, .. } => {
                *current = body.normalized();
                if current.is_idle() {
                    self.pending_body_since = None;
                } else if self.pending_body_since.is_none() {
                    self.pending_body_since = Some(Instant::now());
                }
            },
            ResponseWriteState::AwaitingHeaders | ResponseWriteState::Closed => {},
        }
    }

    pub(super) const fn pending_body_since(&self) -> Option<Instant> {
        self.pending_body_since
    }

    pub(super) fn note_body_progress(&mut self, now: Instant) {
        self.pending_body_since = self.has_pending_output().then_some(now);
    }

    pub(super) const fn take_trailers_if_body_idle(&mut self) -> Option<ResponseHeaders> {
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
            },
            ResponseWriteState::Open { trailers, .. } => {
                if trailers.is_some() {
                    return H2Error::ResponseTrailersAlreadySent.err();
                }
                *trailers = Some(headers);
            },
        }
        Ok(())
    }

    pub(super) fn queue_data(
        &mut self,
        data: PayloadBytes,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        self.queue_chunk(PendingChunk {
            data: PendingChunkData::Plain(data),
            offset: 0,
            end_stream,
        })
    }

    pub(super) fn queue_prefixed_data(
        &mut self,
        data: Box<PrefixedData>,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        self.queue_chunk(PendingChunk {
            data: PendingChunkData::Prefixed(data),
            offset: 0,
            end_stream,
        })
    }

    fn queue_chunk(&mut self, chunk: PendingChunk) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders => {
                return H2Error::DataBeforeResponseHeaders.err();
            },
            ResponseWriteState::Closed => {
                return H2Error::DataOnClosedStream.err();
            },
            ResponseWriteState::Open { body, .. } => {
                let was_idle = body.is_idle();
                match body {
                    StreamBodyState::Idle => {
                        let mut chunks = PendingChunks::new();
                        chunks.push_back(chunk);
                        *body = StreamBodyState::Chunks(chunks);
                    },
                    StreamBodyState::Chunks(pending) => pending.push_back(chunk),
                    StreamBodyState::Path(_) => {
                        return HttpResponseError::PathsendMixedWithBody.err();
                    },
                }
                if was_idle {
                    self.pending_body_since = Some(Instant::now());
                }
            },
        }
        Ok(())
    }

    pub(super) fn queue_path(&mut self, streamer: PathStreamer) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders => {
                return H2Error::PathDataBeforeResponseHeaders.err();
            },
            ResponseWriteState::Closed => {
                return H2Error::PathDataOnClosedStream.err();
            },
            ResponseWriteState::Open { body, .. } => {
                if !body.is_idle() {
                    return HttpResponseError::PathsendMixedWithBody.err();
                }
                *body = StreamBodyState::Path(streamer);
                self.pending_body_since = Some(Instant::now());
            },
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, stream_id: StreamId, response_closes: &mut ResponseCloseBatch) {
        self.response = ResponseWriteState::Closed;
        self.pending_body_since = None;
        notify_response_close(response_closes, stream_id);
    }
}

impl StreamBodyState {
    pub(super) const fn is_idle(&self) -> bool {
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
    pub(super) fn remaining_len(&self) -> usize {
        self.len() - self.offset
    }

    fn len(&self) -> usize {
        match &self.data {
            PendingChunkData::Plain(bytes) => bytes.len(),
            PendingChunkData::Prefixed(data) => data.len(),
        }
    }

    pub(super) fn remaining_slices(&self, additional_offset: usize, len: usize) -> (&[u8], &[u8]) {
        let offset = self.offset + additional_offset;
        match &self.data {
            PendingChunkData::Plain(bytes) => (&bytes.as_ref()[offset..offset + len], &[]),
            PendingChunkData::Prefixed(data) => {
                let prefix = data.prefix();
                if offset >= prefix.len() {
                    let payload_offset = offset - prefix.len();
                    return (&data.payload()[payload_offset..payload_offset + len], &[]);
                }
                let prefix_len = len.min(prefix.len() - offset);
                let payload_len = len - prefix_len;
                (
                    &prefix[offset..offset + prefix_len],
                    &data.payload()[..payload_len],
                )
            },
        }
    }

    pub(super) const fn consume(&mut self, len: usize) {
        self.offset += len;
    }
}

pub(super) fn writer_stream(
    streams: &mut StreamMap<StreamWriteState>,
    stream_id: StreamId,
    initial_stream_send_window: i64,
) -> &mut StreamWriteState {
    streams
        .entry(stream_id)
        .or_insert_with(|| StreamWriteState::new(initial_stream_send_window))
}

pub(super) fn notify_response_close(response_closes: &mut ResponseCloseBatch, stream_id: StreamId) {
    response_closes.push(stream_id);
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::mem::size_of;

    use super::{ReadyStreamQueue, StreamWriteState};
    use crate::h2::new_stream_map;
    use crate::h2_frame::StreamId;

    #[test]
    fn ready_queue_owns_schedule_membership_transitions() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let mut streams = new_stream_map(1);
        streams.insert(stream_id, StreamWriteState::new(0xFFFF));
        let mut ready = ReadyStreamQueue::new();

        ready.schedule(
            streams.get_mut(&stream_id).expect("stream exists"),
            stream_id,
            false,
        );
        ready.schedule(
            streams.get_mut(&stream_id).expect("stream exists"),
            stream_id,
            true,
        );
        assert_eq!(ready.iter().collect::<Vec<_>>(), [stream_id]);

        assert_eq!(ready.pop_scheduled(&mut streams), Some(stream_id));
        assert!(!streams.get(&stream_id).expect("stream exists").scheduled);
        assert!(ready.is_empty());
    }

    #[test]
    fn ready_queue_wrapper_has_no_layout_cost() {
        assert_eq!(
            size_of::<ReadyStreamQueue>(),
            size_of::<VecDeque<StreamId>>()
        );
    }
}
