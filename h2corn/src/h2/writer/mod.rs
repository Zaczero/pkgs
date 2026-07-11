mod driver;
mod flush;
mod header_encode;
mod ingress;
mod stream_state;

use std::fs::File;

use smallvec::SmallVec;

pub(super) use self::driver::{H2WriterHandle, WriterState, init_writer};
use crate::bridge::PayloadBytes;
use crate::h2_frame::{ErrorCode, PeerSettings, StreamId, WindowIncrement};
use crate::http::types::{HttpStatusCode, ResponseHeaders};
use crate::inline_fifo::InlineFifo;

const WRITER_CHANNEL_CAPACITY: usize = 64;
const ENCODED_HEADER_BLOCK_CAPACITY: usize = 1024;
const FRAME_BUFFER_CAPACITY: usize = 64;
const FAIR_WRITE_QUANTUM: usize = 64 * 1024;
const H2_WRITER_BUFFER_CAPACITY: usize = 8 * 1024;
const H2_OUTBOUND_DATA_FRAME_SIZE_TARGET: usize = 64 * 1024;

const INLINE_DATA_PREFIX_CAPACITY: usize = 10;
type ResponseCloseBatch = SmallVec<[StreamId; 8]>;
type ResponseDeadlineUpdateBatch = SmallVec<[StreamId; 8]>;
pub(super) type WriterCommandBatch = InlineFifo<WriterCommand, 3>;

#[derive(Debug)]
pub(super) struct PrefixedData {
    prefix: [u8; INLINE_DATA_PREFIX_CAPACITY],
    prefix_len: u8,
    payload: PayloadBytes,
}

impl PrefixedData {
    fn new(prefix: &[u8], payload: PayloadBytes) -> Self {
        assert!(prefix.len() <= INLINE_DATA_PREFIX_CAPACITY);
        let mut inline_prefix = [0; INLINE_DATA_PREFIX_CAPACITY];
        inline_prefix[..prefix.len()].copy_from_slice(prefix);
        Self {
            prefix: inline_prefix,
            prefix_len: prefix.len() as u8,
            payload,
        }
    }

    pub(super) fn prefix(&self) -> &[u8] {
        &self.prefix[..usize::from(self.prefix_len)]
    }

    pub(super) fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub(super) fn len(&self) -> usize {
        self.prefix().len() + self.payload.len()
    }
}

#[derive(Debug)]
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
    },
    SendTrailers {
        stream_id: StreamId,
        headers: ResponseHeaders,
    },
    SendData {
        stream_id: StreamId,
        data: PayloadBytes,
        end_stream: bool,
    },
    SendPrefixedData {
        stream_id: StreamId,
        data: Box<PrefixedData>,
        end_stream: bool,
    },
    SendPath {
        stream_id: StreamId,
        file: Box<File>,
        len: usize,
        end_stream: bool,
    },
    FlushBufferedOutput,
    SendReset {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SendSettingsAck,
    UpdatePeerSettings(PeerSettings),
    PeerReset {
        stream_id: StreamId,
    },
    GrantSendWindow {
        target: WindowTarget,
        increment: WindowIncrement,
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
            | Self::SendData { stream_id, .. }
            | Self::SendPrefixedData { stream_id, .. }
            | Self::SendPath { stream_id, .. }
            | Self::SendReset { stream_id, .. }
            | Self::PeerReset { stream_id } => Some(*stream_id),
            Self::FlushBufferedOutput
            | Self::SendSettingsAck
            | Self::UpdatePeerSettings(_)
            | Self::GrantSendWindow { .. }
            | Self::SendWindowUpdate { .. }
            | Self::PingAck(_)
            | Self::Goaway { .. } => None,
        }
    }
}
