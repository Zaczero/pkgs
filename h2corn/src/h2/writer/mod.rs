mod driver;
mod flush;
mod header_encode;
mod ingress;
mod stream_state;

use std::mem::size_of;

use smallvec::SmallVec;

pub(super) use self::driver::{H2WriterHandle, WindowOverflow, WriterState, init_writer};
use crate::bridge::PayloadBytes;
use crate::h2_frame::{ErrorCode, StreamId, WindowIncrement};
use crate::http::response::{FileSegment, ResponseBytePermit};
use crate::http::types::{EarlyHintLinks, HttpStatusCode, ResponseHeaders, ResponseTrailers};
use crate::inline_fifo::InlineFifo;
use crate::websocket::EncodedFrameHeader;

const WRITER_CHANNEL_CAPACITY: usize = 64;
const ENCODED_HEADER_BLOCK_CAPACITY: usize = 1024;
const FRAME_BUFFER_CAPACITY: usize = 64;
const FAIR_WRITE_QUANTUM: usize = 64 * 1024;
const H2_WRITER_BUFFER_CAPACITY: usize = 8 * 1024;
const H2_OUTBOUND_DATA_FRAME_SIZE_TARGET: usize = 64 * 1024;
/// Per-connection payload retention limit for HTTP response bodies. One
/// oversized ASGI body may occupy the full budget; all later sends wait until
/// bytes are written or the response is dropped.
const H2_OUTBOUND_RESPONSE_BYTE_CAPACITY: u32 = 2 * 1024 * 1024;

type ResponseCloseBatch = SmallVec<[(ResponseClose, StreamId); 8]>;
type ResponseDeadlineUpdateBatch = SmallVec<[StreamId; 8]>;
pub(super) type WriterCommandBatch = InlineFifo<WriterCommand, 3>;

/// A response lifecycle transition observed by the connection owner.
///
/// A clean completion has already consumed the drained command batch. An
/// abort may leave an undrained batch behind, so it still needs ingress
/// cleanup before the read side finalizes the stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponseClose {
    Clean,
    Abort,
}

/// One WebSocket frame carried as a single H2 DATA payload: frame header plus
/// application bytes. `END_STREAM` is never set for this shape.
#[derive(Debug)]
pub(super) struct WebSocketData {
    header: EncodedFrameHeader,
    payload: PayloadBytes,
}

impl WebSocketData {
    pub(super) const fn new(header: EncodedFrameHeader, payload: PayloadBytes) -> Self {
        Self { header, payload }
    }

    pub(super) fn header(&self) -> &[u8] {
        self.header.as_slice()
    }

    pub(super) fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub(super) fn len(&self) -> usize {
        self.header().len() + self.payload.len()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum WindowTarget {
    Connection,
    Stream(StreamId),
}

#[derive(Debug)]
pub(super) enum WriterCommand {
    SendHeaders {
        stream_id: StreamId,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        end_stream: bool,
    },
    SendFinal {
        stream_id: StreamId,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        data: PayloadBytes,
        credit: Option<ResponseBytePermit>,
    },
    SendTrailers {
        stream_id: StreamId,
        headers: ResponseTrailers,
    },
    /// One interim 103 HEADERS block. No status and no `end_stream` field:
    /// the status is fixed and an informational block may never end a stream.
    SendEarlyHint {
        stream_id: StreamId,
        links: EarlyHintLinks,
    },
    SendData {
        stream_id: StreamId,
        data: PayloadBytes,
        credit: Option<ResponseBytePermit>,
        end_stream: bool,
    },
    SendWebSocketData {
        stream_id: StreamId,
        data: Box<WebSocketData>,
        credit: Option<ResponseBytePermit>,
    },
    SendFile {
        stream_id: StreamId,
        segment: FileSegment,
        end_stream: bool,
    },
    FlushBufferedOutput,
    SendReset {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SendSettingsAck,
    PeerReset {
        stream_id: StreamId,
    },
    SendWindowUpdate {
        target: WindowTarget,
        increment: WindowIncrement,
    },
    PingAck([u8; 8]),
    Goaway {
        last_stream_id: Option<StreamId>,
        error_code: ErrorCode,
        debug: Vec<u8>,
        close: bool,
    },
}

impl WriterCommand {
    const fn response_stream_id(&self) -> Option<StreamId> {
        match self {
            Self::SendHeaders { stream_id, .. }
            | Self::SendFinal { stream_id, .. }
            | Self::SendTrailers { stream_id, .. }
            | Self::SendEarlyHint { stream_id, .. }
            | Self::SendData { stream_id, .. }
            | Self::SendWebSocketData { stream_id, .. }
            | Self::SendFile { stream_id, .. }
            | Self::SendReset { stream_id, .. }
            | Self::PeerReset { stream_id } => Some(*stream_id),
            Self::FlushBufferedOutput
            | Self::SendSettingsAck
            | Self::SendWindowUpdate { .. }
            | Self::PingAck(_)
            | Self::Goaway { .. } => None,
        }
    }
}

const _: () = assert!(size_of::<WriterCommand>() <= 88);
