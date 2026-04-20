use bytes::{Bytes, BytesMut};

use super::super::deflate::PerMessageDeflateMode;
use crate::error::{ErrorExt, H2CornError, WebSocketError, WebSocketProtocolError};
use crate::hpack::BytesStr;

use super::FRAME_HEADER_MAX_LEN;
use super::{
    CLOSE_FRAME_HEADER_LEN, CONTROL_FRAME_PAYLOAD_MAX_LEN, DecodedFrame, FIN_MASK,
    INLINE_PAYLOAD_LEN_MAX, MASK_FLAG, MAX_CLOSE_REASON_LEN, OPCODE_CLOSE, OPCODE_MASK,
    OPCODE_PING, OPCODE_PONG, PAYLOAD_LEN_MASK, PAYLOAD_LEN_U16_MARKER, PAYLOAD_LEN_U64_MARKER,
    RSV1_MASK, RSV23_MASK, WebSocketCloseCode, WebSocketDecodeError, close_code,
};

pub(super) struct ParsedFrameHeader {
    pub(super) fin: bool,
    pub(super) compressed: bool,
    pub(super) opcode: u8,
    pub(super) header_len: usize,
    pub(super) payload_len: usize,
}

pub(super) fn parse_frame_header<M: PerMessageDeflateMode>(
    buffer: &[u8],
) -> Result<Option<ParsedFrameHeader>, WebSocketDecodeError> {
    if buffer.len() < 2 {
        return Ok(None);
    }

    let first = buffer[0];
    let second = buffer[1];
    let fin = first & FIN_MASK != 0;
    let compressed = first & RSV1_MASK != 0;
    if first & RSV23_MASK != 0 {
        return Err(WebSocketDecodeError::protocol(
            WebSocketProtocolError::ExtensionsNotNegotiated,
        ));
    }
    if compressed && !M::ENABLED {
        return Err(WebSocketDecodeError::protocol(
            WebSocketProtocolError::ExtensionsNotNegotiated,
        ));
    }
    if second & MASK_FLAG == 0 {
        return Err(WebSocketDecodeError::protocol(
            WebSocketProtocolError::ClientFramesMustBeMasked,
        ));
    }

    let Some((header_len, payload_len)) = parse_frame_len(buffer, second)? else {
        return Ok(None);
    };

    Ok(Some(ParsedFrameHeader {
        fin,
        compressed,
        opcode: first & OPCODE_MASK,
        header_len,
        payload_len,
    }))
}

fn parse_frame_len(
    buffer: &[u8],
    second: u8,
) -> Result<Option<(usize, usize)>, WebSocketDecodeError> {
    match second & PAYLOAD_LEN_MASK {
        encoded_len @ 0..=125 => Ok(Some((2, usize::from(encoded_len)))),
        PAYLOAD_LEN_U16_MARKER => {
            if buffer.len() < 4 {
                return Ok(None);
            }
            let payload_len = usize::from(u16::from_be_bytes(
                *buffer[2..]
                    .first_chunk::<2>()
                    .expect("extended payload length is buffered"),
            ));
            if payload_len <= INLINE_PAYLOAD_LEN_MAX {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::NonCanonical16BitLengthEncoding,
                ));
            }
            Ok(Some((4, payload_len)))
        }
        PAYLOAD_LEN_U64_MARKER => {
            if buffer.len() < FRAME_HEADER_MAX_LEN {
                return Ok(None);
            }
            let raw = u64::from_be_bytes(
                *buffer[2..]
                    .first_chunk::<8>()
                    .expect("extended payload length is buffered"),
            );
            if raw >> 63 != 0 {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::ReservedHighBitIn64BitLengthEncoding,
                ));
            }
            let payload_len = usize::try_from(raw).map_err(|_| {
                WebSocketDecodeError::protocol(WebSocketProtocolError::FrameTooLarge)
            })?;
            if payload_len < 65_536 {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::NonCanonical64BitLengthEncoding,
                ));
            }
            Ok(Some((10, payload_len)))
        }
        _ => unreachable!("payload length marker is masked to 7 bits"),
    }
}

pub(super) fn decode_control_frame(
    opcode: u8,
    fin: bool,
    payload: Bytes,
) -> Result<DecodedFrame, WebSocketDecodeError> {
    if !fin || payload.len() > CONTROL_FRAME_PAYLOAD_MAX_LEN {
        return Err(WebSocketDecodeError::protocol(
            WebSocketProtocolError::InvalidControlFrame,
        ));
    }
    match opcode {
        OPCODE_CLOSE => {
            if payload.len() == 1 {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::CloseFramePayloadTruncated,
                ));
            }
            let (code, reason) = if payload.len() >= 2 {
                let code = u16::from_be_bytes(
                    *payload
                        .first_chunk::<2>()
                        .expect("close frame code is buffered"),
                );
                validate_close_code(code).map_err(|_| {
                    WebSocketDecodeError::protocol(WebSocketProtocolError::CloseFrameInvalidCode)
                })?;
                let reason = (payload.len() > 2)
                    .then(|| BytesStr::try_from(payload.slice(2..)))
                    .transpose()
                    .map_err(|err| WebSocketDecodeError::invalid_utf8(err.to_string()))?;
                (code, reason)
            } else {
                (close_code::NO_STATUS_RECEIVED, None)
            };
            Ok(DecodedFrame::Close { code, reason })
        }
        OPCODE_PING => Ok(DecodedFrame::Ping(payload)),
        OPCODE_PONG => Ok(DecodedFrame::Pong),
        _ => Err(WebSocketDecodeError::protocol(
            WebSocketProtocolError::UnsupportedControlOpcode,
        )),
    }
}

pub(crate) fn encode_frame_into(opcode: u8, payload: &[u8], compressed: bool, out: &mut BytesMut) {
    out.clear();
    out.reserve(payload.len() + FRAME_HEADER_MAX_LEN);
    out.extend_from_slice(&[FIN_MASK | opcode | if compressed { RSV1_MASK } else { 0x00 }]);
    match payload.len() {
        len if len <= INLINE_PAYLOAD_LEN_MAX => out.extend_from_slice(&[len as u8]),
        len if len < 65_536 => {
            out.extend_from_slice(&[PAYLOAD_LEN_U16_MARKER]);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.extend_from_slice(&[PAYLOAD_LEN_U64_MARKER]);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
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
    out.reserve(CLOSE_FRAME_HEADER_LEN + payload_len);
    out.extend_from_slice(&[FIN_MASK | OPCODE_CLOSE, payload_len as u8]);
    out.extend_from_slice(&code.to_be_bytes());
    out.extend_from_slice(reason.as_bytes());
    Ok(())
}

pub(crate) fn validate_close_code(code: WebSocketCloseCode) -> Result<(), H2CornError> {
    match code {
        close_code::NORMAL..=1003 | close_code::INVALID_FRAME_PAYLOAD_DATA..=1014 | 3000..=4999 => {
            Ok(())
        }
        _ => WebSocketError::CloseCodeInvalid.err(),
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::{
        FIN_MASK, MAX_CLOSE_REASON_LEN, OPCODE_CLOSE, close_code, encode_close_frame_into,
    };

    #[test]
    fn close_frame_encodes_max_legal_reason() {
        let reason = "x".repeat(MAX_CLOSE_REASON_LEN);

        let mut frame = BytesMut::new();
        encode_close_frame_into(close_code::NORMAL, &reason, &mut frame).unwrap();

        assert_eq!(frame.len(), 4 + MAX_CLOSE_REASON_LEN);
        assert_eq!(frame[0], FIN_MASK | OPCODE_CLOSE);
        assert_eq!(frame[1], (2 + MAX_CLOSE_REASON_LEN) as u8);
        assert_eq!(&frame[2..4], &close_code::NORMAL.to_be_bytes());
        assert_eq!(&frame[4..], reason.as_bytes());
    }
}
