use std::collections::VecDeque;
use std::mem;

use tokio::time::Instant;

use crate::bridge::PayloadBytes;
use crate::error::{ErrorExt as _, H2CornError, H2Error};
use crate::h2::StreamMap;
use crate::h2::writer::{ResponseClose, ResponseCloseBatch, WebSocketData};
use crate::h2_frame::StreamId;
use crate::http::pathsend::FileStreamer;
use crate::http::response::ResponseBytePermit;
use crate::http::types::ResponseTrailers;
use crate::inline_fifo::InlineFifo;

#[derive(Debug)]
pub(super) struct PendingChunk {
    data: PendingChunkData,
    credit: Option<ResponseBytePermit>,
    offset: usize,
    pub(super) end_stream: bool,
}

#[derive(Debug)]
enum PendingChunkData {
    Plain(PayloadBytes),
    WebSocket(Box<WebSocketData>),
}

/// One item of a response body, in the order the application produced it.
///
/// Buffered chunks and file segments share a queue because
/// `http.response.zerocopysend` may be sent repeatedly and interleaved with
/// `http.response.body`: order is the contract, so a body cannot be modelled as
/// "either some chunks or one file" without losing it.
///
/// Both arms are stored inline. `FileStreamer` holds only a cursor and the
/// state of one rolling read -- its buffer is behind its own pointer -- so it is
/// *smaller* than a chunk descriptor and sets nothing: the sizes below are
/// pinned so that stays true rather than being asserted in prose.
#[derive(Debug)]
pub(super) enum BodyItem {
    Chunk(PendingChunk),
    File(FileStreamer),
}

// Pinned exactly, in both directions, because this type is stored inline two
// deep in every open stream. The file arm is *not* boxed: a `FileStreamer` is
// the same size as a buffered chunk, so boxing would buy one pointer of
// discriminant back in exchange for an allocation per segment.
//
// The queued item is one word wider than a chunk, and deliberately: it carries
// the admission credit that bounds how many descriptors a flow-controlled
// client can make an application queue. Sixteen bytes per open stream is the
// price of turning an unbounded descriptor leak into backpressure.
const _: () = assert!(size_of::<FileStreamer>() == size_of::<PendingChunk>());
const _: () = assert!(size_of::<BodyItem>() == 80);

pub(super) type PendingBody = InlineFifo<BodyItem, 2>;

#[derive(Debug)]
pub(super) struct ReadyStreamQueue {
    queue: VecDeque<StreamId>,
}

impl ReadyStreamQueue {
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
    Body(PendingBody),
}

// `Open` carries everything and the other two variants carry nothing, so
// `clippy::large_enum_variant` would flag this — but its only remedy is to box
// `body`, which puts the inline chunk queue above behind a pointer and adds a
// malloc per response. The size is pinned here instead: exact, and it fails in
// both directions.
const _: () = assert!(size_of::<ResponseWriteState>() == 208);

#[expect(
    clippy::large_enum_variant,
    reason = "boxing `Open` would put the inline body queue behind a pointer and add a malloc per response; the size is pinned above instead"
)]
#[derive(Debug)]
pub(super) enum ResponseWriteState {
    AwaitingHeaders,
    Open {
        body: StreamBodyState,
        trailers: Option<ResponseTrailers>,
    },
    Closed,
}

#[derive(Debug)]
pub(super) struct StreamWriteState {
    pub(super) send_window: i32,
    pub(super) scheduled: bool,
    pending_body_since: Option<Instant>,
    response: ResponseWriteState,
}

impl StreamWriteState {
    pub(super) fn new(initial_window: i64) -> Self {
        Self {
            send_window: i32::try_from(initial_window)
                .expect("HTTP/2 stream send windows fit the signed 31-bit domain"),
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

    pub(super) const fn take_trailers_if_body_idle(&mut self) -> Option<ResponseTrailers> {
        match &mut self.response {
            ResponseWriteState::Open { body, trailers } if body.is_idle() => trailers.take(),
            ResponseWriteState::Open { .. }
            | ResponseWriteState::AwaitingHeaders
            | ResponseWriteState::Closed => None,
        }
    }

    pub(super) fn queue_trailers(&mut self, headers: ResponseTrailers) -> Result<(), H2CornError> {
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
        credit: Option<ResponseBytePermit>,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        self.queue_chunk(PendingChunk {
            data: PendingChunkData::Plain(data),
            credit,
            offset: 0,
            end_stream,
        })
    }

    pub(super) fn queue_websocket_data(
        &mut self,
        data: Box<WebSocketData>,
        credit: Option<ResponseBytePermit>,
    ) -> Result<(), H2CornError> {
        self.queue_chunk(PendingChunk {
            data: PendingChunkData::WebSocket(data),
            credit,
            offset: 0,
            // WebSocket DATA never closes the stream; the close frame and
            // session teardown own stream end separately.
            end_stream: false,
        })
    }

    fn queue_chunk(&mut self, chunk: PendingChunk) -> Result<(), H2CornError> {
        self.queue_body_item(
            BodyItem::Chunk(chunk),
            H2Error::DataBeforeResponseHeaders,
            H2Error::DataOnClosedStream,
        )
    }

    pub(super) fn queue_file(&mut self, streamer: FileStreamer) -> Result<(), H2CornError> {
        self.queue_body_item(
            BodyItem::File(streamer),
            H2Error::PathDataBeforeResponseHeaders,
            H2Error::PathDataOnClosedStream,
        )
    }

    /// Append one item to the response body, preserving application order.
    ///
    /// There is deliberately no "already has a body" rejection here. Mixing a
    /// file with buffered chunks is exactly what `http.response.zerocopysend`
    /// is for; pathsend's own terminality is a rule about the *ASGI messages*
    /// and is enforced where those are admitted
    /// (`ResponseController::handle_pathsend`), not a second time against this
    /// server's own trusted queue.
    fn queue_body_item(
        &mut self,
        item: BodyItem,
        before_headers: H2Error,
        on_closed: H2Error,
    ) -> Result<(), H2CornError> {
        match &mut self.response {
            ResponseWriteState::AwaitingHeaders => before_headers.err(),
            ResponseWriteState::Closed => on_closed.err(),
            ResponseWriteState::Open { body, .. } => {
                let was_idle = body.is_idle();
                match body {
                    StreamBodyState::Idle => {
                        let mut queue = PendingBody::new();
                        queue.push_back(item);
                        *body = StreamBodyState::Body(queue);
                    },
                    StreamBodyState::Body(queue) => queue.push_back(item),
                }
                if was_idle {
                    self.pending_body_since = Some(Instant::now());
                }
                Ok(())
            },
        }
    }

    pub(super) fn finish(&mut self, stream_id: StreamId, response_closes: &mut ResponseCloseBatch) {
        self.response = ResponseWriteState::Closed;
        self.pending_body_since = None;
        notify_response_complete(response_closes, stream_id);
    }

    pub(super) fn abort(&mut self, stream_id: StreamId, response_closes: &mut ResponseCloseBatch) {
        self.response = ResponseWriteState::Closed;
        self.pending_body_since = None;
        notify_response_abort(response_closes, stream_id);
    }
}

impl StreamBodyState {
    pub(super) const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub(super) fn has_pending_output(&self) -> bool {
        match self {
            Self::Idle => false,
            Self::Body(queue) => !queue.is_empty(),
        }
    }

    fn normalized(self) -> Self {
        match self {
            Self::Body(queue) if queue.is_empty() => Self::Idle,
            other => other,
        }
    }
}

impl BodyItem {
    /// The buffered chunk, when this item is one.
    ///
    /// The vectored frame collector walks the queue with this: a file segment
    /// has no in-memory slice to hand a `writev`, so it stops the batch rather
    /// than joining it.
    pub(super) const fn as_chunk(&self) -> Option<&PendingChunk> {
        match self {
            Self::Chunk(chunk) => Some(chunk),
            Self::File(_) => None,
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
            PendingChunkData::WebSocket(data) => data.len(),
        }
    }

    pub(super) fn remaining_slices(&self, additional_offset: usize, len: usize) -> (&[u8], &[u8]) {
        let offset = self.offset + additional_offset;
        match &self.data {
            PendingChunkData::Plain(bytes) => (&bytes.as_ref()[offset..offset + len], &[]),
            PendingChunkData::WebSocket(data) => {
                let header = data.header();
                if offset >= header.len() {
                    let payload_offset = offset - header.len();
                    return (&data.payload()[payload_offset..payload_offset + len], &[]);
                }
                let header_len = len.min(header.len() - offset);
                let payload_len = len - header_len;
                (
                    &header[offset..offset + header_len],
                    &data.payload()[..payload_len],
                )
            },
        }
    }

    pub(super) fn consume(&mut self, len: usize) {
        self.offset += len;
        if let Some(credit) = &mut self.credit {
            credit.release_written(len);
        }
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

pub(super) fn notify_response_complete(
    response_closes: &mut ResponseCloseBatch,
    stream_id: StreamId,
) {
    response_closes.push((ResponseClose::Clean, stream_id));
}

pub(super) fn notify_response_abort(response_closes: &mut ResponseCloseBatch, stream_id: StreamId) {
    response_closes.push((ResponseClose::Abort, stream_id));
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
        let mut streams = new_stream_map();
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
