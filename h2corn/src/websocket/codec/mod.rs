use std::fmt;

use bytes::Bytes;

use crate::error::WebSocketProtocolError;
use crate::hpack::BytesStr;

mod cursor;
mod decode;
mod frame;
mod mask;

pub const MAX_CLOSE_REASON_LEN: usize = 123;
const FIN_MASK: u8 = 0x80;
const RSV1_MASK: u8 = 0x40;
const RSV23_MASK: u8 = 0x30;
const OPCODE_MASK: u8 = 0x0f;
const MASK_FLAG: u8 = 0x80;
const PAYLOAD_LEN_MASK: u8 = 0x7f;
const INLINE_PAYLOAD_LEN_MAX: usize = 125;
const PAYLOAD_LEN_U16_MARKER: u8 = 126;
const PAYLOAD_LEN_U64_MARKER: u8 = 127;
const FRAME_HEADER_MAX_LEN: usize = 10;
const CLIENT_MASK_LEN: usize = 4;
const CLIENT_FRAME_PREFIX_MAX_LEN: usize = FRAME_HEADER_MAX_LEN + CLIENT_MASK_LEN;
const SEGMENT_INLINE_CAPACITY: usize = 4;
const CLOSE_FRAME_HEADER_LEN: usize = 2;
const CONTROL_FRAME_PAYLOAD_MAX_LEN: usize = INLINE_PAYLOAD_LEN_MAX;
const OPCODE_CONTINUATION: u8 = 0x0;
const OPCODE_TEXT: u8 = 0x1;
const OPCODE_BINARY: u8 = 0x2;
const OPCODE_CLOSE: u8 = 0x8;
const OPCODE_PING: u8 = 0x9;
const OPCODE_PONG: u8 = 0xA;

pub type WebSocketCloseCode = u16;

pub mod close_code {
    use super::WebSocketCloseCode;

    pub const NORMAL: WebSocketCloseCode = 1000;
    pub const GOING_AWAY: WebSocketCloseCode = 1001;
    pub const PROTOCOL_ERROR: WebSocketCloseCode = 1002;
    pub const MESSAGE_TOO_BIG: WebSocketCloseCode = 1009;
    pub const NO_STATUS_RECEIVED: WebSocketCloseCode = 1005;
    pub const ABNORMAL_CLOSURE: WebSocketCloseCode = 1006;
    pub const INVALID_FRAME_PAYLOAD_DATA: WebSocketCloseCode = 1007;
    pub const INTERNAL_ERROR: WebSocketCloseCode = 1011;
    pub const SERVICE_RESTART: WebSocketCloseCode = 1012;
}

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

pub use decode::WebSocketCodec;
pub use frame::encode_close_frame_into;
pub use frame::encode_frame_into;
pub use frame::validate_close_code;
