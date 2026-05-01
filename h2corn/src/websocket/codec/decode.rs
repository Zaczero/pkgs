use std::{mem, num::NonZeroUsize};

use bytes::{Buf, Bytes, BytesMut};

#[cfg(test)]
use super::super::deflate::PerMessageDeflateDisabled;
use super::super::deflate::PerMessageDeflateMode;
use super::cursor::SegmentCursor;
use super::frame::{decode_control_frame, parse_frame_header};
use super::mask::apply_websocket_mask_phase;
use super::{
    CLIENT_FRAME_PREFIX_MAX_LEN, CLIENT_MASK_LEN, DecodedFrame, OPCODE_BINARY, OPCODE_CLOSE,
    OPCODE_CONTINUATION, OPCODE_TEXT, SEGMENT_INLINE_CAPACITY, WebSocketDecodeError,
};
use crate::error::WebSocketProtocolError;
use crate::hpack::BytesStr;

#[derive(Debug, Default)]
pub(crate) struct WebSocketCodec {
    pub(crate) buffer: BytesMut,
    segmented: SegmentCursor<SEGMENT_INLINE_CAPACITY>,
    fragmented: FragmentState,
    max_message_size: Option<NonZeroUsize>,
}

#[derive(Debug, Eq, PartialEq)]
enum FragmentBuffer {
    Bytes(Bytes),
    Mut(BytesMut),
}

impl FragmentBuffer {
    fn len(&self) -> usize {
        match self {
            Self::Bytes(data) => data.len(),
            Self::Mut(data) => data.len(),
        }
    }

    fn extend_from_slice(&mut self, payload: &[u8]) {
        match self {
            Self::Bytes(existing) => {
                let mut out = BytesMut::with_capacity(existing.len() + payload.len());
                out.extend_from_slice(existing.as_ref());
                out.extend_from_slice(payload);
                *self = Self::Mut(out);
            }
            Self::Mut(data) => data.extend_from_slice(payload),
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            Self::Bytes(data) => data,
            Self::Mut(data) => data.freeze(),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
enum FragmentState {
    #[default]
    Idle,
    Text {
        data: FragmentBuffer,
        compressed: bool,
        compressed_len: usize,
    },
    Binary {
        data: FragmentBuffer,
        compressed: bool,
        compressed_len: usize,
    },
}

impl WebSocketCodec {
    pub(crate) fn with_options(buffer: BytesMut, max_message_size: Option<NonZeroUsize>) -> Self {
        Self {
            buffer,
            segmented: SegmentCursor::default(),
            fragmented: FragmentState::Idle,
            max_message_size,
        }
    }

    pub(crate) fn segmented(max_message_size: Option<NonZeroUsize>) -> Self {
        Self::with_options(BytesMut::new(), max_message_size)
    }

    pub(crate) fn push_segment(&mut self, segment: Bytes) {
        debug_assert!(
            self.buffer.is_empty(),
            "segmented websocket input should not be mixed with the contiguous buffer path"
        );
        self.segmented.push(segment);
    }

    #[cfg(test)]
    pub(crate) fn decode_next(&mut self) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        let mut inflater = ();
        self.decode_next_with_inflater::<PerMessageDeflateDisabled>(&mut inflater)
    }

    pub(crate) fn decode_next_with_inflater<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        let max_message_size = self.max_message_size.map(NonZeroUsize::get);
        if self.segmented.len() != 0 {
            return self.decode_next_segmented::<M>(inflater, max_message_size);
        }

        if self.buffer.len() < 2 {
            return Ok(None);
        }

        let Some(header) = parse_frame_header::<M>(&self.buffer)? else {
            return Ok(None);
        };
        self.ensure_bufferable_frame(&header, max_message_size)?;

        let needed = header.header_len + CLIENT_MASK_LEN + header.payload_len;
        if self.buffer.len() < needed {
            return Ok(None);
        }

        let payload = self.take_buffered_payload(needed, header.header_len);

        self.decode_frame_payload::<M>(
            inflater,
            header.fin,
            header.compressed,
            header.opcode,
            payload,
            max_message_size,
        )
    }

    fn decode_next_segmented<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        max_message_size: Option<usize>,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        if self.segmented.len() < 2 {
            return Ok(None);
        }

        let prefix_len = self.segmented.len().min(CLIENT_FRAME_PREFIX_MAX_LEN);
        let mut prefix = [0_u8; CLIENT_FRAME_PREFIX_MAX_LEN];
        self.segmented
            .peek_prefix(prefix_len, &mut prefix[..prefix_len]);

        let Some(header) = parse_frame_header::<M>(&prefix[..prefix_len])? else {
            return Ok(None);
        };
        self.ensure_bufferable_frame(&header, max_message_size)?;

        let needed = header.header_len + CLIENT_MASK_LEN + header.payload_len;
        if self.segmented.len() < needed {
            return Ok(None);
        }

        let mask = *prefix[header.header_len..header.header_len + CLIENT_MASK_LEN]
            .first_chunk::<CLIENT_MASK_LEN>()
            .expect("segmented websocket mask is buffered");
        self.segmented.skip(header.header_len + CLIENT_MASK_LEN);
        let payload = self.segmented.take_masked_payload(header.payload_len, mask);

        self.decode_frame_payload::<M>(
            inflater,
            header.fin,
            header.compressed,
            header.opcode,
            payload,
            max_message_size,
        )
    }

    fn decode_frame_payload<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        fin: bool,
        compressed: bool,
        opcode: u8,
        payload: Bytes,
        max_message_size: Option<usize>,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        if opcode >= OPCODE_CLOSE {
            if compressed {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::CompressedControlFrame,
                ));
            }
            return decode_control_frame(opcode, fin, payload).map(Some);
        }

        match opcode {
            OPCODE_CONTINUATION => self.decode_continuation::<M>(
                inflater,
                fin,
                compressed,
                payload.as_ref(),
                max_message_size,
            ),
            OPCODE_TEXT | OPCODE_BINARY => self.decode_data_frame::<M>(
                inflater,
                opcode,
                fin,
                compressed,
                payload,
                max_message_size,
            ),
            _ => Err(WebSocketDecodeError::protocol(
                WebSocketProtocolError::UnsupportedOpcode,
            )),
        }
    }

    fn take_buffered_payload(&mut self, needed: usize, header_len: usize) -> Bytes {
        let mut frame = self.buffer.split_to(needed);
        frame.advance(header_len);
        // SAFETY: `needed` includes `CLIENT_MASK_LEN` bytes after the parsed
        // header, and `[u8; 4]` has byte alignment.
        let mask = unsafe { *frame.as_ptr().cast::<[u8; 4]>() };
        frame.advance(CLIENT_MASK_LEN);
        apply_websocket_mask_phase(frame.as_mut(), mask, 0);
        frame.freeze()
    }

    fn decode_payload<M: PerMessageDeflateMode>(
        inflater: &mut M::Inflater,
        opcode: u8,
        compressed: bool,
        payload: Bytes,
        max_message_size: Option<usize>,
    ) -> Result<DecodedFrame, WebSocketDecodeError> {
        let payload = if compressed {
            M::decompress(inflater, payload.as_ref(), max_message_size).map_err(
                |err| match err {
                    WebSocketProtocolError::MessageTooLarge => {
                        WebSocketDecodeError::message_too_large()
                    }
                    other => WebSocketDecodeError::protocol(other),
                },
            )?
        } else {
            Self::ensure_message_size(payload.len(), max_message_size)?;
            payload
        };

        if opcode == OPCODE_TEXT {
            let text = BytesStr::try_from(payload)
                .map_err(|err| WebSocketDecodeError::invalid_utf8(err.to_string()))?;
            Ok(DecodedFrame::Text(text))
        } else {
            Ok(DecodedFrame::Binary(payload))
        }
    }

    fn ensure_message_size(
        len: usize,
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketDecodeError> {
        if max_message_size.is_some_and(|limit| len > limit) {
            return Err(WebSocketDecodeError::message_too_large());
        }
        Ok(())
    }

    fn ensure_bufferable_frame(
        &self,
        header: &super::frame::ParsedFrameHeader,
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketDecodeError> {
        if max_message_size.is_none() || header.opcode >= OPCODE_CLOSE {
            return Ok(());
        }

        let accumulated_compressed = match (&self.fragmented, header.opcode) {
            (
                FragmentState::Text {
                    compressed: true,
                    compressed_len,
                    ..
                }
                | FragmentState::Binary {
                    compressed: true,
                    compressed_len,
                    ..
                },
                OPCODE_CONTINUATION,
            ) => compressed_len.saturating_add(header.payload_len),
            _ => header.payload_len,
        };
        Self::ensure_message_size(accumulated_compressed, max_message_size)
    }

    fn decode_data_frame<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        opcode: u8,
        fin: bool,
        compressed: bool,
        payload: Bytes,
        max_message_size: Option<usize>,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        if self.fragmented != FragmentState::Idle {
            return Err(WebSocketDecodeError::protocol(
                WebSocketProtocolError::DataBeforeFragmentCompletion,
            ));
        }
        if !fin {
            let compressed_len = payload.len();
            if !compressed {
                Self::ensure_message_size(compressed_len, max_message_size)?;
            }
            self.fragmented = if opcode == OPCODE_TEXT {
                FragmentState::Text {
                    data: FragmentBuffer::Bytes(payload),
                    compressed,
                    compressed_len,
                }
            } else {
                FragmentState::Binary {
                    data: FragmentBuffer::Bytes(payload),
                    compressed,
                    compressed_len,
                }
            };
            return Ok(None);
        }
        Self::decode_payload::<M>(inflater, opcode, compressed, payload, max_message_size).map(Some)
    }

    fn decode_continuation<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        fin: bool,
        compressed: bool,
        payload: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        if compressed {
            return Err(WebSocketDecodeError::protocol(
                WebSocketProtocolError::CompressedContinuationFrame,
            ));
        }
        match &mut self.fragmented {
            FragmentState::Idle => {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::UnexpectedContinuationFrame,
                ));
            }
            FragmentState::Text {
                data,
                compressed: is_compressed,
                compressed_len,
            }
            | FragmentState::Binary {
                data,
                compressed: is_compressed,
                compressed_len,
            } => {
                let next_len = data.len().saturating_add(payload.len());
                let next_compressed_len = compressed_len.saturating_add(payload.len());
                if !*is_compressed && max_message_size.is_some_and(|limit| next_len > limit) {
                    return Err(WebSocketDecodeError::message_too_large());
                }
                if *is_compressed
                    && max_message_size.is_some_and(|limit| next_compressed_len > limit)
                {
                    return Err(WebSocketDecodeError::message_too_large());
                }
                data.extend_from_slice(payload);
                *compressed_len = next_compressed_len;
            }
        }
        if !fin {
            return Ok(None);
        }
        match mem::take(&mut self.fragmented) {
            FragmentState::Text {
                data, compressed, ..
            } => Self::decode_payload::<M>(
                inflater,
                OPCODE_TEXT,
                compressed,
                data.freeze(),
                max_message_size,
            )
            .map(Some),
            FragmentState::Binary {
                data, compressed, ..
            } => Self::decode_payload::<M>(
                inflater,
                OPCODE_BINARY,
                compressed,
                data.freeze(),
                max_message_size,
            )
            .map(Some),
            FragmentState::Idle => Err(WebSocketDecodeError::protocol(
                WebSocketProtocolError::UnexpectedContinuationFrame,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::{Bytes, BytesMut};

    use super::{
        super::{
            super::deflate::{PerMessageDeflateEnabled, PerMessageDeflateMode},
            CLIENT_FRAME_PREFIX_MAX_LEN, FIN_MASK, INLINE_PAYLOAD_LEN_MAX, MASK_FLAG,
            OPCODE_CONTINUATION, PAYLOAD_LEN_U16_MARKER, PAYLOAD_LEN_U64_MARKER, RSV1_MASK,
            close_code,
        },
        DecodedFrame, FragmentState, OPCODE_BINARY, OPCODE_TEXT, WebSocketCodec,
    };

    fn encode_masked_client_frame(opcode: u8, payload: &[u8], fin: bool) -> Vec<u8> {
        encode_masked_client_frame_with_flags(opcode, payload, fin, 0)
    }

    fn encode_masked_client_frame_with_flags(
        opcode: u8,
        payload: &[u8],
        fin: bool,
        flags: u8,
    ) -> Vec<u8> {
        let mask = [1_u8, 2, 3, 4];
        let first = if fin { FIN_MASK } else { 0x00 } | opcode | flags;
        let mut out = Vec::with_capacity(payload.len() + CLIENT_FRAME_PREFIX_MAX_LEN);
        out.push(first);
        match payload.len() {
            len if len <= INLINE_PAYLOAD_LEN_MAX => out.push(MASK_FLAG | len as u8),
            len if len < 65_536 => {
                out.push(MASK_FLAG | PAYLOAD_LEN_U16_MARKER);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            len => {
                out.push(MASK_FLAG | PAYLOAD_LEN_U64_MARKER);
                out.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(&mask);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index & 3]),
        );
        out
    }

    #[test]
    fn websocket_codec_rejects_invalid_utf8_text() {
        let mut codec = WebSocketCodec::default();
        codec
            .buffer
            .extend_from_slice(&encode_masked_client_frame(OPCODE_TEXT, b"\xff", true));

        let err = codec.decode_next().expect_err("invalid utf-8 should fail");

        assert_eq!(err.close_code, close_code::INVALID_FRAME_PAYLOAD_DATA);
    }

    #[test]
    fn websocket_codec_rejects_non_canonical_16_bit_length() {
        let mut frame = vec![
            FIN_MASK | OPCODE_BINARY,
            MASK_FLAG | PAYLOAD_LEN_U16_MARKER,
            0x00,
            INLINE_PAYLOAD_LEN_MAX as u8,
        ];
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec = WebSocketCodec::default();
        codec.buffer.extend_from_slice(&frame);

        let err = codec
            .decode_next()
            .expect_err("non-canonical 16-bit length should fail");

        assert_eq!(err.close_code, close_code::PROTOCOL_ERROR);
    }

    #[test]
    fn websocket_codec_rejects_reserved_high_bit_in_64_bit_length() {
        let mut frame = vec![FIN_MASK | OPCODE_BINARY, MASK_FLAG | PAYLOAD_LEN_U64_MARKER];
        frame.extend_from_slice(&(1_u64 << 63).to_be_bytes());
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec = WebSocketCodec::default();
        codec.buffer.extend_from_slice(&frame);

        let err = codec
            .decode_next()
            .expect_err("reserved high bit should fail");

        assert_eq!(err.close_code, close_code::PROTOCOL_ERROR);
    }

    #[test]
    fn websocket_codec_decodes_segmented_text_frame_split_across_header_mask_and_payload() {
        let frame = encode_masked_client_frame(OPCODE_TEXT, b"hello", true);
        let mut codec = WebSocketCodec::segmented(None);

        codec.push_segment(Bytes::copy_from_slice(&frame[..1]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[1..4]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[4..7]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[7..]));
        let Some(DecodedFrame::Text(text)) = codec.decode_next().unwrap() else {
            panic!("expected a decoded text frame");
        };

        assert_eq!(AsRef::<[u8]>::as_ref(&text), b"hello");
    }

    #[test]
    fn websocket_codec_decodes_segmented_extended_length_text_frame() {
        let payload = vec![b'x'; 126];
        let frame = encode_masked_client_frame(OPCODE_TEXT, &payload, true);
        let mut codec = WebSocketCodec::segmented(None);

        codec.push_segment(Bytes::copy_from_slice(&frame[..2]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[2..5]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[5..11]));
        assert!(codec.decode_next().unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[11..]));
        let Some(DecodedFrame::Text(text)) = codec.decode_next().unwrap() else {
            panic!("expected a decoded text frame");
        };

        assert_eq!(AsRef::<[u8]>::as_ref(&text).len(), payload.len());
        assert!(
            AsRef::<[u8]>::as_ref(&text)
                .iter()
                .all(|&byte| byte == b'x')
        );
    }

    #[test]
    fn websocket_codec_rejects_oversized_frame_before_buffering_payload() {
        let mut frame = vec![FIN_MASK | OPCODE_BINARY, MASK_FLAG | PAYLOAD_LEN_U16_MARKER];
        frame.extend_from_slice(&(126_u16).to_be_bytes());
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec =
            WebSocketCodec::with_options(BytesMut::from(frame.as_slice()), NonZeroUsize::new(64));

        let err = codec
            .decode_next()
            .expect_err("oversized frame should fail before payload buffering");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
        assert_eq!(codec.buffer.len(), frame.len());
    }

    #[test]
    fn websocket_codec_rejects_fragmented_compressed_message_over_limit() {
        let mut codec = WebSocketCodec::with_options(BytesMut::new(), NonZeroUsize::new(8));
        let mut inflater = PerMessageDeflateEnabled::new_inflater();

        codec
            .buffer
            .extend_from_slice(&encode_masked_client_frame_with_flags(
                OPCODE_TEXT,
                b"abcd",
                false,
                RSV1_MASK,
            ));
        assert!(
            codec
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .unwrap()
                .is_none()
        );

        codec
            .buffer
            .extend_from_slice(&encode_masked_client_frame(0x0, b"efghi", true));
        let err = codec
            .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
            .expect_err("compressed fragments beyond the limit should fail");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
    }

    #[test]
    fn websocket_codec_rejects_fragmented_uncompressed_message_over_limit_before_copying() {
        let mut codec = WebSocketCodec::with_options(BytesMut::new(), NonZeroUsize::new(8));

        codec
            .buffer
            .extend_from_slice(&encode_masked_client_frame(OPCODE_TEXT, b"abcd", false));
        assert!(codec.decode_next().unwrap().is_none());

        codec.buffer.extend_from_slice(&encode_masked_client_frame(
            OPCODE_CONTINUATION,
            b"efghi",
            true,
        ));
        let err = codec
            .decode_next()
            .expect_err("uncompressed fragments beyond the limit should fail");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
        match &codec.fragmented {
            FragmentState::Text { data, .. } => assert_eq!(data.len(), 4),
            _ => panic!("expected fragmented text state to remain unchanged"),
        }
    }
}
