mod cursor;
mod decode;
mod frame;
mod mask;

pub(crate) mod close_code {
    use super::WebSocketCloseCode;

    pub(crate) const NORMAL: WebSocketCloseCode = 1000;
    pub(crate) const GOING_AWAY: WebSocketCloseCode = 1001;
    pub(crate) const PROTOCOL_ERROR: WebSocketCloseCode = 1002;
    pub(crate) const NO_STATUS_RECEIVED: WebSocketCloseCode = 1005;
    pub(crate) const ABNORMAL_CLOSURE: WebSocketCloseCode = 1006;
    pub(crate) const INVALID_FRAME_PAYLOAD_DATA: WebSocketCloseCode = 1007;
    pub(crate) const MESSAGE_TOO_BIG: WebSocketCloseCode = 1009;
    pub(crate) const INTERNAL_ERROR: WebSocketCloseCode = 1011;
    pub(crate) const SERVICE_RESTART: WebSocketCloseCode = 1012;
}

mod wire {
    pub(in crate::websocket::codec) mod opcode {
        pub(crate) const CONTINUATION: u8 = 0x0;
        pub(crate) const TEXT: u8 = 0x1;
        pub(crate) const BINARY: u8 = 0x2;
        pub(crate) const CLOSE: u8 = 0x8;
        pub(crate) const PING: u8 = 0x9;
        pub(crate) const PONG: u8 = 0xA;
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
use std::num::NonZeroU16;

use bytes::Bytes;
pub(crate) use decode::WebSocketCodec;
pub(crate) use frame::{EncodedFrameHeader, encode_frame_header};
pub(super) use frame::{encode_close_frame_into, encode_frame_into};

use crate::error::WebSocketProtocolError;
use crate::http::types::BytesStr;

pub(super) const MAX_CLOSE_REASON_LEN: usize = 123;

pub(crate) type WebSocketCloseCode = u16;

/// A close status which RFC 6455 permits on the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidCloseCode(NonZeroU16);

impl ValidCloseCode {
    pub(crate) const fn get(self) -> WebSocketCloseCode {
        self.0.get()
    }
}

impl TryFrom<WebSocketCloseCode> for ValidCloseCode {
    type Error = crate::error::H2CornError;

    fn try_from(code: WebSocketCloseCode) -> Result<Self, Self::Error> {
        frame::validate_close_code(code)?;
        Ok(Self(
            NonZeroU16::new(code).expect("valid websocket close codes are non-zero"),
        ))
    }
}

#[derive(Debug)]
pub(crate) enum DecodedPeerClose {
    Empty,
    Coded {
        code: ValidCloseCode,
        reason: Option<BytesStr>,
    },
}

#[derive(Debug)]
pub(crate) enum DecodedFrame {
    Text(BytesStr),
    Binary(Bytes),
    Ping(Bytes),
    Pong,
    Close(DecodedPeerClose),
}

#[derive(Debug)]
pub(crate) struct WebSocketDecodeError {
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
