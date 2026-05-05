mod cursor;
mod decode;
mod frame;
mod mask;

pub mod close_code {
    use super::WebSocketCloseCode;

    pub const NORMAL: WebSocketCloseCode = 1000;
    pub const GOING_AWAY: WebSocketCloseCode = 1001;
    pub const PROTOCOL_ERROR: WebSocketCloseCode = 1002;
    pub const NO_STATUS_RECEIVED: WebSocketCloseCode = 1005;
    pub const ABNORMAL_CLOSURE: WebSocketCloseCode = 1006;
    pub const INVALID_FRAME_PAYLOAD_DATA: WebSocketCloseCode = 1007;
    pub const MESSAGE_TOO_BIG: WebSocketCloseCode = 1009;
    pub const INTERNAL_ERROR: WebSocketCloseCode = 1011;
    pub const SERVICE_RESTART: WebSocketCloseCode = 1012;
}

mod wire {
    pub(in crate::websocket::codec) mod opcode {
        pub const CONTINUATION: u8 = 0x0;
        pub const TEXT: u8 = 0x1;
        pub const BINARY: u8 = 0x2;
        pub const CLOSE: u8 = 0x8;
        pub const PING: u8 = 0x9;
        pub const PONG: u8 = 0xA;
    }

    pub(super) const FIN: u8 = 0x80;
    pub(super) const RSV1: u8 = 0x40;
    pub(super) const RSV23: u8 = 0x30;
    pub(super) const OPCODE_MASK: u8 = 0x0F;
    pub(super) const MASK: u8 = 0x80;
    pub(super) const PAYLOAD_LEN_MASK: u8 = 0x7F;
    pub(super) const INLINE_PAYLOAD_LEN_MAX: usize = 125;
    pub(super) const PAYLOAD_LEN_U16_MARKER: u8 = 126;
    pub(super) const PAYLOAD_LEN_U64_MARKER: u8 = 127;
    pub(super) const FRAME_HEADER_MAX_LEN: usize = 10;
    pub(super) const CLIENT_MASK_LEN: usize = 4;
    pub(super) const CLIENT_FRAME_PREFIX_MAX_LEN: usize = FRAME_HEADER_MAX_LEN + CLIENT_MASK_LEN;
    pub(super) const SEGMENT_INLINE_CAPACITY: usize = 4;
    pub(super) const CLOSE_FRAME_HEADER_LEN: usize = 2;
    pub(super) const CONTROL_FRAME_PAYLOAD_MAX_LEN: usize = INLINE_PAYLOAD_LEN_MAX;
}

use std::fmt;

use bytes::Bytes;
pub use decode::WebSocketCodec;
pub use frame::{encode_close_frame_into, encode_frame_into, validate_close_code};

use crate::error::WebSocketProtocolError;
use crate::hpack::BytesStr;

pub const MAX_CLOSE_REASON_LEN: usize = 123;

pub type WebSocketCloseCode = u16;

#[derive(Debug)]
pub enum DecodedFrame {
    Text(BytesStr),
    Binary(Bytes),
    Ping(Bytes),
    Pong,
    Close {
        code: WebSocketCloseCode,
        reason: Option<BytesStr>,
    },
}

#[derive(Debug)]
pub struct WebSocketDecodeError {
    pub(crate) close_code: WebSocketCloseCode,
    pub(crate) error: WebSocketProtocolError,
}

impl WebSocketDecodeError {
    pub(crate) const fn protocol(error: WebSocketProtocolError) -> Self {
        Self {
            close_code: close_code::PROTOCOL_ERROR,
            error,
        }
    }

    pub(crate) fn invalid_utf8(detail: impl Into<Box<str>>) -> Self {
        Self {
            close_code: close_code::INVALID_FRAME_PAYLOAD_DATA,
            error: WebSocketProtocolError::invalid_utf8(detail),
        }
    }

    pub(crate) const fn message_too_large() -> Self {
        Self {
            close_code: close_code::MESSAGE_TOO_BIG,
            error: WebSocketProtocolError::MessageTooLarge,
        }
    }
}

impl fmt::Display for WebSocketDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
