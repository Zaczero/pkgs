mod cursor;
mod decode;
mod frame;
mod mask;

pub(crate) mod close_code {
    use crate::websocket::codec::WebSocketCloseCode;

    pub(crate) const NORMAL: WebSocketCloseCode = WebSocketCloseCode::new(1000).unwrap();
    pub(crate) const GOING_AWAY: WebSocketCloseCode = WebSocketCloseCode::new(1001).unwrap();
    pub(crate) const PROTOCOL_ERROR: WebSocketCloseCode = WebSocketCloseCode::new(1002).unwrap();
    pub(crate) const NO_STATUS_RECEIVED: WebSocketCloseCode =
        WebSocketCloseCode::new(1005).unwrap();
    pub(crate) const ABNORMAL_CLOSURE: WebSocketCloseCode = WebSocketCloseCode::new(1006).unwrap();
    pub(crate) const INVALID_FRAME_PAYLOAD_DATA: WebSocketCloseCode =
        WebSocketCloseCode::new(1007).unwrap();
    pub(crate) const MESSAGE_TOO_BIG: WebSocketCloseCode = WebSocketCloseCode::new(1009).unwrap();
    pub(crate) const INTERNAL_ERROR: WebSocketCloseCode = WebSocketCloseCode::new(1011).unwrap();
    pub(crate) const SERVICE_RESTART: WebSocketCloseCode = WebSocketCloseCode::new(1012).unwrap();
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

use std::num::NonZeroU16;

use bytes::Bytes;

use crate::http::types::BytesStr;
pub(crate) use crate::websocket::codec::decode::WebSocketCodec;
pub(crate) use crate::websocket::codec::frame::{EncodedFrameHeader, encode_frame_header};
pub(super) use crate::websocket::codec::frame::{encode_close_frame_into, encode_frame_into};

pub(super) const MAX_CLOSE_REASON_LEN: usize = 123;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WebSocketCloseCode(NonZeroU16);

impl WebSocketCloseCode {
    pub(crate) const fn new(code: u16) -> Option<Self> {
        match NonZeroU16::new(code) {
            Some(code) => Some(Self(code)),
            None => None,
        }
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }
}

/// A close status which RFC 6455 permits on the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidCloseCode(NonZeroU16);

impl ValidCloseCode {
    pub(crate) const fn get(self) -> WebSocketCloseCode {
        WebSocketCloseCode(self.0)
    }
}

impl TryFrom<WebSocketCloseCode> for ValidCloseCode {
    type Error = crate::error::H2CornError;

    fn try_from(code: WebSocketCloseCode) -> Result<Self, Self::Error> {
        frame::validate_close_code(code)?;
        Ok(Self(code.0))
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
