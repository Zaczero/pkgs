use bytes::{Bytes, BytesMut};

use crate::error::{ErrorExt as _, H2CornError, WebSocketError, WebSocketProtocolError};
use crate::http::types::BytesStr;
use crate::websocket::codec::wire::opcode;
use crate::websocket::codec::{
    DecodedFrame, DecodedPeerClose, MAX_CLOSE_REASON_LEN, ValidCloseCode, WebSocketCloseCode, wire,
};
use crate::websocket::deflate::PerMessageDeflateMode;

pub(super) struct ParsedFrameHeader {
    pub(super) fin: bool,
    pub(super) compressed: bool,
    pub(super) opcode: u8,
    pub(super) header_len: usize,
    pub(super) payload_len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodedFrameHeader {
    bytes: [u8; wire::FRAME_HEADER_MAX_LEN],
    len: u8,
}

impl EncodedFrameHeader {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

pub(super) fn parse_frame_header<M: PerMessageDeflateMode>(
    buffer: &[u8],
) -> Result<Option<ParsedFrameHeader>, WebSocketProtocolError> {
    if buffer.len() < 2 {
        return Ok(None);
    }

    let first = buffer[0];
    let second = buffer[1];
    let fin = first & wire::FIN != 0;
    let compressed = first & wire::RSV1 != 0;
    if first & wire::RSV23 != 0 {
        return Err(WebSocketProtocolError::ExtensionsNotNegotiated);
    }
    if compressed && !M::ENABLED {
        return Err(WebSocketProtocolError::ExtensionsNotNegotiated);
    }
    if second & wire::MASK == 0 {
        return Err(WebSocketProtocolError::ClientFramesMustBeMasked);
    }

    let Some((header_len, payload_len)) = parse_frame_len(buffer, second)? else {
        return Ok(None);
    };

    let opcode = first & wire::OPCODE_MASK;
    // RFC 6455 section 5.5: a control frame carries at most 125 bytes and is
    // never fragmented. Enforced here, on the header alone, because the
    // decoder buffers `payload_len` bytes before it looks at a frame again —
    // a peer declaring a 64 MiB PING would otherwise be handed that much
    // memory before the frame was ever ruled out.
    if opcode >= opcode::CLOSE && (!fin || payload_len > wire::CONTROL_FRAME_PAYLOAD_MAX_LEN) {
        return Err(WebSocketProtocolError::InvalidControlFrame);
    }

    Ok(Some(ParsedFrameHeader {
        fin,
        compressed,
        opcode,
        header_len,
        payload_len,
    }))
}

fn parse_frame_len(
    buffer: &[u8],
    second: u8,
) -> Result<Option<(usize, usize)>, WebSocketProtocolError> {
    match second & wire::PAYLOAD_LEN_MASK {
        encoded_len @ 0..=125 => Ok(Some((2, usize::from(encoded_len)))),
        wire::PAYLOAD_LEN_U16_MARKER => {
            if buffer.len() < 4 {
                return Ok(None);
            }
            let payload_len = usize::from(u16::from_be_bytes(
                *buffer[2..]
                    .first_chunk::<2>()
                    .expect("extended payload length is buffered"),
            ));
            if payload_len <= wire::INLINE_PAYLOAD_LEN_MAX {
                return Err(WebSocketProtocolError::NonCanonical16BitLengthEncoding);
            }
            Ok(Some((4, payload_len)))
        },
        // `& PAYLOAD_LEN_MASK` caps the marker at 127, so the remaining
        // pattern is exactly `PAYLOAD_LEN_U64_MARKER`.
        _ => {
            if buffer.len() < wire::FRAME_HEADER_MAX_LEN {
                return Ok(None);
            }
            let raw = u64::from_be_bytes(
                *buffer[2..]
                    .first_chunk::<8>()
                    .expect("extended payload length is buffered"),
            );
            if raw >> 63 != 0 {
                return Err(WebSocketProtocolError::ReservedHighBitIn64BitLengthEncoding);
            }
            let payload_len =
                usize::try_from(raw).map_err(|_| WebSocketProtocolError::FrameTooLarge)?;
            if payload_len < 0x0001_0000 {
                return Err(WebSocketProtocolError::NonCanonical64BitLengthEncoding);
            }
            Ok(Some((10, payload_len)))
        },
    }
}

/// Decode a control frame whose header already proved it well-formed.
///
/// `parse_frame_header` rejects a fragmented or oversized control frame
/// before its payload is buffered, so only the per-opcode payload rules are
/// left to check here.
pub(super) fn decode_control_frame(
    opcode: u8,
    payload: Bytes,
) -> Result<DecodedFrame, WebSocketProtocolError> {
    debug_assert!(payload.len() <= wire::CONTROL_FRAME_PAYLOAD_MAX_LEN);
    match opcode {
        opcode::CLOSE => {
            if payload.len() == 1 {
                return Err(WebSocketProtocolError::CloseFramePayloadTruncated);
            }
            let close = if payload.len() >= 2 {
                let code = u16::from_be_bytes(
                    *payload
                        .first_chunk::<2>()
                        .expect("close frame code is buffered"),
                );
                let code = WebSocketCloseCode::new(code)
                    .and_then(|code| ValidCloseCode::try_from(code).ok())
                    .ok_or(WebSocketProtocolError::CloseFrameInvalidCode)?;
                let reason = (payload.len() > 2)
                    .then(|| BytesStr::try_from(payload.slice(2..)))
                    .transpose()
                    .map_err(|err| WebSocketProtocolError::invalid_utf8(err.to_string()))?;
                DecodedPeerClose::Coded { code, reason }
            } else {
                DecodedPeerClose::Empty
            };
            Ok(DecodedFrame::Close(close))
        },
        opcode::PING => Ok(DecodedFrame::Ping(payload)),
        opcode::PONG => Ok(DecodedFrame::Pong),
        _ => Err(WebSocketProtocolError::UnsupportedControlOpcode),
    }
}

pub(crate) fn encode_frame_into(opcode: u8, payload: &[u8], compressed: bool, out: &mut BytesMut) {
    out.clear();
    out.reserve(payload.len() + wire::FRAME_HEADER_MAX_LEN);
    out.extend_from_slice(&[wire::FIN | opcode | if compressed { wire::RSV1 } else { 0x00 }]);
    match payload.len() {
        len if len <= wire::INLINE_PAYLOAD_LEN_MAX => out.extend_from_slice(&[len as u8]),
        len if len < 0x0001_0000 => {
            out.extend_from_slice(&[wire::PAYLOAD_LEN_U16_MARKER]);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        },
        len => {
            out.extend_from_slice(&[wire::PAYLOAD_LEN_U64_MARKER]);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        },
    }
    out.extend_from_slice(payload);
}

pub(crate) fn encode_frame_header(
    opcode: u8,
    payload_len: usize,
    compressed: bool,
) -> EncodedFrameHeader {
    let mut bytes = [0; wire::FRAME_HEADER_MAX_LEN];
    bytes[0] = wire::FIN | opcode | if compressed { wire::RSV1 } else { 0x00 };
    let len = match payload_len {
        len if len <= wire::INLINE_PAYLOAD_LEN_MAX => {
            bytes[1] = len as u8;
            2
        },
        len if len < 0x0001_0000 => {
            bytes[1] = wire::PAYLOAD_LEN_U16_MARKER;
            bytes[2..4].copy_from_slice(&(len as u16).to_be_bytes());
            4
        },
        len => {
            bytes[1] = wire::PAYLOAD_LEN_U64_MARKER;
            bytes[2..].copy_from_slice(&(len as u64).to_be_bytes());
            wire::FRAME_HEADER_MAX_LEN
        },
    };
    EncodedFrameHeader {
        bytes,
        len: len as u8,
    }
}

pub(crate) fn encode_close_frame_into(
    code: WebSocketCloseCode,
    reason: &str,
    out: &mut BytesMut,
) -> Result<(), H2CornError> {
    validate_close_code(code)?;
    if reason.len() > MAX_CLOSE_REASON_LEN {
        return WebSocketError::CloseReasonTooLong.err();
    }

    let payload_len = 2 + reason.len();
    out.clear();
    out.reserve(wire::CLOSE_FRAME_HEADER_LEN + payload_len);
    out.extend_from_slice(&[wire::FIN | opcode::CLOSE, payload_len as u8]);
    out.extend_from_slice(&code.get().to_be_bytes());
    out.extend_from_slice(reason.as_bytes());
    Ok(())
}

pub(crate) fn validate_close_code(code: WebSocketCloseCode) -> Result<(), H2CornError> {
    match code.get() {
        1000..=1003 | 1007..=1014 | 3000..=4999 => Ok(()),
        _ => WebSocketError::CloseCodeInvalid.err(),
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use bytes::{Bytes, BytesMut};

    use super::{
        DecodedFrame, DecodedPeerClose, MAX_CLOSE_REASON_LEN, decode_control_frame,
        encode_close_frame_into, encode_frame_header, opcode, wire,
    };
    use crate::websocket::close_code;

    #[test]
    fn segmented_frame_header_covers_all_wire_lengths_compactly() {
        assert_eq!(size_of::<super::EncodedFrameHeader>(), 11);
        assert_eq!(
            encode_frame_header(opcode::BINARY, 125, false).as_slice(),
            [0x82, 125]
        );
        assert_eq!(
            encode_frame_header(opcode::BINARY, 126, false).as_slice(),
            [0x82, 126, 0, 126]
        );
        assert_eq!(
            encode_frame_header(opcode::TEXT, 0x0001_0000, true).as_slice(),
            [0xC1, 127, 0, 0, 0, 0, 0, 1, 0, 0]
        );
    }

    #[test]
    fn close_frame_encodes_max_legal_reason() {
        let reason = "x".repeat(MAX_CLOSE_REASON_LEN);

        let mut frame = BytesMut::new();
        encode_close_frame_into(close_code::NORMAL, &reason, &mut frame).unwrap();

        assert_eq!(frame.len(), 4 + MAX_CLOSE_REASON_LEN);
        assert_eq!(frame[0], wire::FIN | opcode::CLOSE);
        assert_eq!(frame[1], (2 + MAX_CLOSE_REASON_LEN) as u8);
        assert_eq!(&frame[2..4], &close_code::NORMAL.get().to_be_bytes());
        assert_eq!(&frame[4..], reason.as_bytes());
    }

    #[test]
    fn empty_peer_close_does_not_manufacture_a_wire_code() {
        let close =
            decode_control_frame(opcode::CLOSE, Bytes::new()).expect("empty close is valid");
        assert!(matches!(
            close,
            DecodedFrame::Close(DecodedPeerClose::Empty)
        ));
    }
}
