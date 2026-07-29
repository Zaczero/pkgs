use std::mem;
use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};

use super::super::deflate::PerMessageDeflateMode;
use super::cursor::SegmentCursor;
use super::frame::{decode_control_frame, parse_frame_header};
use super::mask::MaskKey;
use super::wire::opcode;
use super::{DecodedFrame, WebSocketDecodeError, wire};
use crate::error::WebSocketProtocolError;
use crate::http::types::BytesStr;

#[derive(Debug, Default)]
pub(crate) struct WebSocketCodec {
    segmented: SegmentCursor<{ wire::SEGMENT_INLINE_CAPACITY }>,
    fragmented: FragmentState,
    current: Option<CurrentFrame>,
    max_message_size: Option<NonZeroUsize>,
}

#[derive(Debug)]
struct CurrentFrame {
    fin: bool,
    compressed: bool,
    opcode: u8,
    mask: MaskKey,
    remaining: usize,
    mask_phase: usize,
    payload: BytesMut,
}

#[derive(Debug, Default, Eq, PartialEq)]
enum FragmentState {
    #[default]
    Idle,
    Message {
        /// TEXT or BINARY — the opcode of the first frame of the fragmented
        /// message, replayed when the final continuation completes it.
        opcode: u8,
        data: BytesMut,
        compressed: bool,
    },
}

impl WebSocketCodec {
    pub(crate) fn with_options(max_message_size: Option<NonZeroUsize>) -> Self {
        Self {
            segmented: SegmentCursor::default(),
            fragmented: FragmentState::Idle,
            current: None,
            max_message_size,
        }
    }

    pub(crate) fn push_segment(&mut self, segment: Bytes) {
        self.segmented.push(segment);
    }

    pub(crate) fn decode_next_with_inflater<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        let max_message_size = self.max_message_size.map(NonZeroUsize::get);
        self.start_current_frame::<M>(inflater, max_message_size)?;
        let Some(mut current) = self.current.take() else {
            return Ok(None);
        };

        while current.remaining != 0 && self.segmented.len() != 0 {
            let take = current.remaining.min(self.segmented.len());
            let (chunk, phase) =
                self.segmented
                    .take_masked_chunk(take, current.mask, current.mask_phase);
            current.mask_phase = phase;
            current.remaining -= chunk.len();

            if self.current_frame_is_compressed(&current) {
                M::push_decompress(inflater, chunk.as_ref(), max_message_size)
                    .map_err(Self::decode_deflate_error)?;
            } else {
                current.payload.unsplit(chunk);
            }
        }

        if current.remaining != 0 {
            self.current = Some(current);
            return Ok(None);
        }
        self.finish_current_frame::<M>(inflater, current, max_message_size)
    }

    fn start_current_frame<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketDecodeError> {
        if self.current.is_some() || self.segmented.len() < 2 {
            return Ok(());
        }

        let prefix_len = self.segmented.len().min(wire::CLIENT_FRAME_PREFIX_MAX_LEN);
        let mut prefix = [0_u8; wire::CLIENT_FRAME_PREFIX_MAX_LEN];
        self.segmented
            .peek_prefix(prefix_len, &mut prefix[..prefix_len]);
        let Some(header) = parse_frame_header::<M>(&prefix[..prefix_len])? else {
            return Ok(());
        };
        self.validate_frame_header_state(&header, max_message_size)?;

        let mask_end = header.header_len + wire::CLIENT_MASK_LEN;
        if self.segmented.len() < mask_end {
            return Ok(());
        }
        let mask = *prefix[header.header_len..mask_end]
            .first_chunk::<{ wire::CLIENT_MASK_LEN }>()
            .expect("segmented websocket mask is buffered");
        self.segmented.skip(mask_end);

        if header.opcode == opcode::TEXT || header.opcode == opcode::BINARY {
            if !header.fin {
                self.fragmented = FragmentState::Message {
                    opcode: header.opcode,
                    data: BytesMut::new(),
                    compressed: header.compressed,
                };
            }
            if header.compressed {
                M::begin_decompress(inflater);
            }
        }
        self.current = Some(CurrentFrame {
            fin: header.fin,
            compressed: header.compressed,
            opcode: header.opcode,
            mask: MaskKey::new(mask),
            remaining: header.payload_len,
            mask_phase: 0,
            payload: BytesMut::new(),
        });
        Ok(())
    }

    fn validate_frame_header_state(
        &self,
        header: &super::frame::ParsedFrameHeader,
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketDecodeError> {
        match header.opcode {
            opcode::TEXT | opcode::BINARY => {
                if self.fragmented != FragmentState::Idle {
                    return Err(WebSocketDecodeError::protocol(
                        WebSocketProtocolError::DataBeforeFragmentCompletion,
                    ));
                }
                if !header.compressed {
                    Self::ensure_message_size(header.payload_len, max_message_size)?;
                }
            },
            opcode::CONTINUATION => {
                if header.compressed {
                    return Err(WebSocketDecodeError::protocol(
                        WebSocketProtocolError::CompressedContinuationFrame,
                    ));
                }
                let FragmentState::Message {
                    data, compressed, ..
                } = &self.fragmented
                else {
                    return Err(WebSocketDecodeError::protocol(
                        WebSocketProtocolError::UnexpectedContinuationFrame,
                    ));
                };
                if !compressed {
                    Self::ensure_message_size(
                        data.len().saturating_add(header.payload_len),
                        max_message_size,
                    )?;
                }
            },
            opcode::CLOSE | opcode::PING | opcode::PONG => {
                if header.compressed {
                    return Err(WebSocketDecodeError::protocol(
                        WebSocketProtocolError::CompressedControlFrame,
                    ));
                }
            },
            _ => {
                return Err(WebSocketDecodeError::protocol(
                    WebSocketProtocolError::UnsupportedOpcode,
                ));
            },
        }
        Ok(())
    }

    const fn current_frame_is_compressed(&self, frame: &CurrentFrame) -> bool {
        frame.compressed
            || matches!(
                (&self.fragmented, frame.opcode),
                (
                    FragmentState::Message {
                        compressed: true,
                        ..
                    },
                    opcode::CONTINUATION
                )
            )
    }

    fn finish_current_frame<M: PerMessageDeflateMode>(
        &mut self,
        inflater: &mut M::Inflater,
        frame: CurrentFrame,
        max_message_size: Option<usize>,
    ) -> Result<Option<DecodedFrame>, WebSocketDecodeError> {
        match frame.opcode {
            opcode::CLOSE | opcode::PING | opcode::PONG => {
                decode_control_frame(frame.opcode, frame.payload.freeze()).map(Some)
            },
            opcode::TEXT | opcode::BINARY if frame.fin => {
                let payload = if frame.compressed {
                    M::finish_decompress(inflater, max_message_size)
                        .map_err(Self::decode_deflate_error)?
                } else {
                    frame.payload.freeze()
                };
                Self::decode_message(frame.opcode, payload).map(Some)
            },
            opcode::TEXT | opcode::BINARY => {
                let FragmentState::Message {
                    data, compressed, ..
                } = &mut self.fragmented
                else {
                    unreachable!("a non-final data frame starts fragment state")
                };
                debug_assert_eq!(*compressed, frame.compressed);
                if !frame.compressed {
                    data.unsplit(frame.payload);
                }
                Ok(None)
            },
            opcode::CONTINUATION => {
                let FragmentState::Message {
                    opcode,
                    data,
                    compressed,
                } = &mut self.fragmented
                else {
                    unreachable!("continuation state was validated before payload consumption")
                };
                if !*compressed {
                    data.unsplit(frame.payload);
                }
                if !frame.fin {
                    return Ok(None);
                }
                let opcode = *opcode;
                let compressed = *compressed;
                let data = mem::take(&mut self.fragmented);
                let payload = if compressed {
                    M::finish_decompress(inflater, max_message_size)
                        .map_err(Self::decode_deflate_error)?
                } else {
                    let FragmentState::Message { data, .. } = data else {
                        unreachable!("fragment state is retained until its final continuation")
                    };
                    data.freeze()
                };
                Self::decode_message(opcode, payload).map(Some)
            },
            _ => unreachable!("frame opcode was validated before payload consumption"),
        }
    }

    fn decode_message(opcode: u8, payload: Bytes) -> Result<DecodedFrame, WebSocketDecodeError> {
        if opcode == opcode::TEXT {
            let text = BytesStr::try_from(payload)
                .map_err(|err| WebSocketDecodeError::invalid_utf8(err.to_string()))?;
            Ok(DecodedFrame::Text(text))
        } else {
            Ok(DecodedFrame::Binary(payload))
        }
    }

    fn decode_deflate_error(error: WebSocketProtocolError) -> WebSocketDecodeError {
        match error {
            WebSocketProtocolError::MessageTooLarge => WebSocketDecodeError::message_too_large(),
            other => WebSocketDecodeError::protocol(other),
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::Bytes;

    use super::super::super::deflate::{
        PerMessageDeflateDisabled, PerMessageDeflateEnabled, PerMessageDeflateMode,
    };
    use super::super::wire::opcode;
    use super::super::{close_code, wire};
    use super::{DecodedFrame, FragmentState, WebSocketCodec};

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
        let first = if fin { wire::FIN } else { 0x00 } | opcode | flags;
        let mut out = Vec::with_capacity(payload.len() + wire::CLIENT_FRAME_PREFIX_MAX_LEN);
        out.push(first);
        match payload.len() {
            len if len <= wire::INLINE_PAYLOAD_LEN_MAX => out.push(wire::MASK | len as u8),
            len if len < 0x0001_0000 => {
                out.push(wire::MASK | wire::PAYLOAD_LEN_U16_MARKER);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            },
            len => {
                out.push(wire::MASK | wire::PAYLOAD_LEN_U64_MARKER);
                out.extend_from_slice(&(len as u64).to_be_bytes());
            },
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

    fn decode(
        codec: &mut WebSocketCodec,
    ) -> Result<Option<DecodedFrame>, super::WebSocketDecodeError> {
        codec.decode_next_with_inflater::<PerMessageDeflateDisabled>(&mut ())
    }

    fn masked_header_only(opcode: u8, fin: bool, payload_len: u64) -> Vec<u8> {
        let mut header = vec![if fin { wire::FIN } else { 0 } | opcode];
        match payload_len {
            len if len <= wire::INLINE_PAYLOAD_LEN_MAX as u64 => {
                header.push(wire::MASK | len as u8);
            },
            len if len < 0x0001_0000 => {
                header.push(wire::MASK | wire::PAYLOAD_LEN_U16_MARKER);
                header.extend_from_slice(&(len as u16).to_be_bytes());
            },
            len => {
                header.push(wire::MASK | wire::PAYLOAD_LEN_U64_MARKER);
                header.extend_from_slice(&len.to_be_bytes());
            },
        }
        header
    }

    fn stored_permessage_deflate(payload: &[u8]) -> Vec<u8> {
        let len = u16::try_from(payload.len()).expect("test payload fits a stored DEFLATE block");
        let mut out = Vec::with_capacity(payload.len() + 5);
        // BFINAL=0, BTYPE=00, then the byte-aligned stored-block length.
        out.push(0);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn websocket_codec_rejects_invalid_utf8_text() {
        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::TEXT,
            b"\xff",
            true,
        )));

        let err = decode(&mut codec).expect_err("invalid utf-8 should fail");

        assert_eq!(err.close_code, close_code::INVALID_FRAME_PAYLOAD_DATA);
    }

    #[test]
    fn websocket_codec_rejects_non_canonical_16_bit_length() {
        let mut frame = vec![
            wire::FIN | opcode::BINARY,
            wire::MASK | wire::PAYLOAD_LEN_U16_MARKER,
            0x00,
            wire::INLINE_PAYLOAD_LEN_MAX as u8,
        ];
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(frame));

        let err = decode(&mut codec).expect_err("non-canonical 16-bit length should fail");

        assert_eq!(err.close_code, close_code::PROTOCOL_ERROR);
    }

    #[test]
    fn websocket_codec_rejects_reserved_high_bit_in_64_bit_length() {
        let mut frame = vec![
            wire::FIN | opcode::BINARY,
            wire::MASK | wire::PAYLOAD_LEN_U64_MARKER,
        ];
        frame.extend_from_slice(&(1_u64 << 63).to_be_bytes());
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(frame));

        let err = decode(&mut codec).expect_err("reserved high bit should fail");

        assert_eq!(err.close_code, close_code::PROTOCOL_ERROR);
    }

    #[test]
    fn websocket_codec_decodes_segmented_text_frame_split_across_header_mask_and_payload() {
        let frame = encode_masked_client_frame(opcode::TEXT, b"hello", true);
        let mut codec = WebSocketCodec::with_options(None);

        codec.push_segment(Bytes::copy_from_slice(&frame[..1]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[1..4]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[4..7]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[7..]));
        let Some(DecodedFrame::Text(text)) = decode(&mut codec).unwrap() else {
            panic!("expected a decoded text frame");
        };

        assert_eq!(AsRef::<[u8]>::as_ref(&text), b"hello");
    }

    #[test]
    fn websocket_codec_decodes_segmented_extended_length_text_frame() {
        let payload = vec![b'x'; 126];
        let frame = encode_masked_client_frame(opcode::TEXT, &payload, true);
        let mut codec = WebSocketCodec::with_options(None);

        codec.push_segment(Bytes::copy_from_slice(&frame[..2]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[2..5]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[5..11]));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::copy_from_slice(&frame[11..]));
        let Some(DecodedFrame::Text(text)) = decode(&mut codec).unwrap() else {
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
    fn exact_unique_frame_payload_reaches_decoded_binary_without_copying() {
        let frame = encode_masked_client_frame(opcode::BINARY, b"payload", true);
        let segment = Bytes::from(frame);
        let payload_ptr = segment[6..].as_ptr();
        let mut codec = WebSocketCodec::default();
        codec.push_segment(segment);

        let Some(DecodedFrame::Binary(payload)) = decode(&mut codec).unwrap() else {
            panic!("expected a decoded binary frame");
        };

        assert_eq!(payload.as_ref(), b"payload");
        assert_eq!(
            payload.as_ptr(),
            payload_ptr,
            "payload allocation was copied"
        );
    }

    #[test]
    fn websocket_codec_decodes_every_two_segment_split() {
        let frame = encode_masked_client_frame(opcode::BINARY, b"payload", true);

        for split in 1..frame.len() {
            let mut codec = WebSocketCodec::default();
            codec.push_segment(Bytes::copy_from_slice(&frame[..split]));
            assert!(decode(&mut codec).unwrap().is_none(), "split {split}");
            codec.push_segment(Bytes::copy_from_slice(&frame[split..]));
            let Some(DecodedFrame::Binary(payload)) = decode(&mut codec).unwrap() else {
                panic!("split {split} did not decode a binary frame");
            };
            assert_eq!(payload.as_ref(), b"payload");
        }
    }

    #[test]
    fn websocket_codec_reassembles_fragmented_text_and_binary() {
        for opcode in [opcode::TEXT, opcode::BINARY] {
            let mut codec = WebSocketCodec::default();
            codec.push_segment(Bytes::from(encode_masked_client_frame(
                opcode, b"hel", false,
            )));
            assert!(decode(&mut codec).unwrap().is_none());
            codec.push_segment(Bytes::from(encode_masked_client_frame(
                opcode::CONTINUATION,
                b"lo",
                true,
            )));

            let decoded = decode(&mut codec).unwrap().expect("final fragment decodes");
            match decoded {
                DecodedFrame::Text(text) => assert_eq!(text.as_bytes(), b"hello"),
                DecodedFrame::Binary(payload) => assert_eq!(payload.as_ref(), b"hello"),
                _ => panic!("fragmented data must decode as its original opcode"),
            }
        }
    }

    #[test]
    fn websocket_codec_decodes_control_frame_after_segmented_input() {
        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::PING,
            b"ping",
            true,
        )));

        let Some(DecodedFrame::Ping(payload)) = decode(&mut codec).unwrap() else {
            panic!("expected a decoded ping frame");
        };
        assert_eq!(payload.as_ref(), b"ping");
    }

    #[test]
    fn reserved_opcodes_are_rejected_from_the_header_before_payload_arrives() {
        for opcode in [3, 4, 5, 6, 7, 11, 12, 13, 14, 15] {
            for payload_len in [0, 125, 126, (1_u64 << 63) - 1] {
                let mut codec = WebSocketCodec::default();
                let header = masked_header_only(opcode, true, payload_len);
                for (index, byte) in header.iter().enumerate() {
                    codec.push_segment(Bytes::copy_from_slice(std::slice::from_ref(byte)));
                    if index + 1 == header.len() {
                        let Err(err) = decode(&mut codec) else {
                            panic!(
                                "reserved opcode {opcode} with payload length {payload_len} must fail once its header is available"
                            )
                        };
                        assert_eq!(err.close_code, close_code::PROTOCOL_ERROR);
                    } else {
                        assert!(decode(&mut codec).unwrap().is_none());
                    }
                }
            }
        }
    }

    #[test]
    fn fragment_state_is_rejected_from_the_header_before_payload_arrives() {
        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::TEXT,
            b"",
            false,
        )));
        assert!(decode(&mut codec).unwrap().is_none());
        codec.push_segment(Bytes::from(masked_header_only(opcode::TEXT, true, 0)));
        assert_eq!(
            decode(&mut codec)
                .expect_err("new data during a fragment is a header error")
                .close_code,
            close_code::PROTOCOL_ERROR
        );

        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(masked_header_only(
            opcode::CONTINUATION,
            true,
            0,
        )));
        assert_eq!(
            decode(&mut codec)
                .expect_err("an idle continuation is a header error")
                .close_code,
            close_code::PROTOCOL_ERROR
        );

        let mut codec = WebSocketCodec::with_options(None);
        let mut inflater = PerMessageDeflateEnabled::new_inflater();
        codec.push_segment(Bytes::from(encode_masked_client_frame_with_flags(
            opcode::TEXT,
            b"",
            false,
            wire::RSV1,
        )));
        assert!(
            codec
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .unwrap()
                .is_none()
        );
        let mut continuation = masked_header_only(opcode::CONTINUATION, true, 0);
        continuation[0] |= wire::RSV1;
        codec.push_segment(Bytes::from(continuation));
        assert_eq!(
            codec
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .expect_err("RSV1 continuation is a header error")
                .close_code,
            close_code::PROTOCOL_ERROR
        );

        let mut codec = WebSocketCodec::default();
        codec.push_segment(Bytes::from(masked_header_only(opcode::PING, false, 0)));
        assert_eq!(
            decode(&mut codec)
                .expect_err("fragmented controls are a header error")
                .close_code,
            close_code::PROTOCOL_ERROR
        );
    }

    #[test]
    fn decoded_size_not_compressed_wire_size_enforces_the_message_limit() {
        let source = vec![b'a'; 100];
        let compressed = stored_permessage_deflate(&source);
        assert!(compressed.len() > source.len());

        let frame =
            encode_masked_client_frame_with_flags(opcode::TEXT, &compressed, true, wire::RSV1);
        let mut at_limit = WebSocketCodec::with_options(NonZeroUsize::new(source.len()));
        let mut inflater = PerMessageDeflateEnabled::new_inflater();
        at_limit.push_segment(Bytes::from(frame.clone()));
        let Some(DecodedFrame::Text(text)) = at_limit
            .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
            .expect("decoded size exactly at the limit is legal")
        else {
            panic!("expected a decoded text message");
        };
        assert_eq!(text.as_bytes(), source);

        let mut over_limit = WebSocketCodec::with_options(NonZeroUsize::new(source.len() - 1));
        let mut inflater = PerMessageDeflateEnabled::new_inflater();
        over_limit.push_segment(Bytes::from(frame));
        assert_eq!(
            over_limit
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .expect_err("decoded size one over the limit closes 1009")
                .close_code,
            close_code::MESSAGE_TOO_BIG
        );
    }

    #[test]
    fn compressed_input_is_released_one_transport_segment_at_a_time() {
        let source = vec![b'a'; 16 * 1024];
        let mut deflater = PerMessageDeflateEnabled::new_deflater();
        let compressed = PerMessageDeflateEnabled::compress(&mut deflater, &source)
            .expect("compression succeeds")
            .expect("repetitive input compresses");
        let frame =
            encode_masked_client_frame_with_flags(opcode::BINARY, &compressed, true, wire::RSV1);
        let mut codec = WebSocketCodec::with_options(None);
        let mut inflater = PerMessageDeflateEnabled::new_inflater();
        let header_len = frame.len() - compressed.len();

        codec.push_segment(Bytes::copy_from_slice(&frame[..header_len]));
        assert!(
            codec
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            codec.segmented.len(),
            0,
            "the header is consumed once complete"
        );

        for (index, byte) in frame[header_len..].iter().enumerate() {
            codec.push_segment(Bytes::copy_from_slice(std::slice::from_ref(byte)));
            let decoded = codec
                .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
                .expect("legal compressed input decodes");
            if index + 1 == compressed.len() {
                assert!(matches!(decoded, Some(DecodedFrame::Binary(_))));
            } else {
                assert!(decoded.is_none());
            }
            assert_eq!(codec.segmented.len(), 0, "consumed input is not retained");
        }
    }

    #[test]
    fn websocket_codec_rejects_oversized_frame_before_buffering_payload() {
        let mut frame = vec![
            wire::FIN | opcode::BINARY,
            wire::MASK | wire::PAYLOAD_LEN_U16_MARKER,
        ];
        frame.extend_from_slice(&(126_u16).to_be_bytes());
        frame.extend_from_slice(&[1, 2, 3, 4]);

        let mut codec = WebSocketCodec::with_options(NonZeroUsize::new(64));
        codec.push_segment(Bytes::from(frame));

        let err =
            decode(&mut codec).expect_err("oversized frame should fail before payload buffering");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
    }

    #[test]
    fn websocket_codec_rejects_fragmented_compressed_message_over_limit() {
        let mut codec = WebSocketCodec::with_options(NonZeroUsize::new(8));
        let mut inflater = PerMessageDeflateEnabled::new_inflater();
        let mut deflater = PerMessageDeflateEnabled::new_deflater();
        let compressed = PerMessageDeflateEnabled::compress(&mut deflater, &[b'a'; 64])
            .expect("compression succeeds")
            .expect("repetitive input compresses");
        let split = compressed.len() / 2;

        codec.push_segment(Bytes::from(encode_masked_client_frame_with_flags(
            opcode::TEXT,
            &compressed[..split],
            false,
            wire::RSV1,
        )));
        match codec.decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater) {
            Ok(None) => {},
            Err(err) => {
                assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
                return;
            },
            Ok(Some(_)) => panic!("a non-final compressed fragment cannot complete a message"),
        }

        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::CONTINUATION,
            &compressed[split..],
            true,
        )));
        let err = codec
            .decode_next_with_inflater::<PerMessageDeflateEnabled>(&mut inflater)
            .expect_err("compressed fragments beyond the limit should fail");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
    }

    #[test]
    fn websocket_codec_rejects_fragmented_uncompressed_message_over_limit_before_copying() {
        let mut codec = WebSocketCodec::with_options(NonZeroUsize::new(8));

        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::TEXT,
            b"abcd",
            false,
        )));
        assert!(decode(&mut codec).unwrap().is_none());

        codec.push_segment(Bytes::from(encode_masked_client_frame(
            opcode::CONTINUATION,
            b"efghi",
            true,
        )));
        let err =
            decode(&mut codec).expect_err("uncompressed fragments beyond the limit should fail");

        assert_eq!(err.close_code, close_code::MESSAGE_TOO_BIG);
        match &codec.fragmented {
            FragmentState::Message { data, .. } => assert_eq!(data.len(), 4),
            FragmentState::Idle => panic!("expected fragmented text state to remain unchanged"),
        }
    }
}
