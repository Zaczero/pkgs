use bytes::{Bytes, BytesMut};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use memchr::memchr;

use crate::error::WebSocketProtocolError;
use crate::http::header::split_commas_bytes;

const PERMESSAGE_DEFLATE: &[u8] = b"permessage-deflate";
const TAIL: &[u8; 4] = b"\x00\x00\xff\xff";
const FLATE_GROWTH_SLACK: usize = 16;

pub(crate) const PERMESSAGE_DEFLATE_RESPONSE: &[u8] =
    b"permessage-deflate; server_no_context_takeover; client_no_context_takeover";

pub(crate) trait PerMessageDeflateMode {
    type Inflater;
    type Deflater;

    const ENABLED: bool;

    fn new_inflater() -> Self::Inflater;

    fn new_deflater() -> Self::Deflater;

    fn compress(
        deflater: &mut Self::Deflater,
        payload: &[u8],
    ) -> Result<Option<Bytes>, flate2::CompressError>;

    fn decompress<const ENFORCE_MESSAGE_LIMIT: bool>(
        inflater: &mut Self::Inflater,
        payload: &[u8],
        max_message_size: usize,
    ) -> Result<Bytes, WebSocketProtocolError>;
}

#[derive(Debug)]
pub(crate) struct PerMessageDeflateEnabled;

#[derive(Debug)]
pub(crate) struct PerMessageDeflateDisabled;

#[derive(Debug)]
pub(crate) struct MessageDeflater {
    compressor: Compress,
    out: BytesMut,
}

#[derive(Debug)]
pub(crate) struct MessageInflater {
    decoder: Decompress,
    out: BytesMut,
}

pub(crate) fn requested_by_client(value: &[u8]) -> bool {
    split_commas_bytes(value).any(|extension| {
        let extension = extension.trim_ascii();
        let end = memchr(b';', extension).unwrap_or(extension.len());
        extension[..end]
            .trim_ascii()
            .eq_ignore_ascii_case(PERMESSAGE_DEFLATE)
    })
}

fn decompressed_capacity<const ENFORCE_MESSAGE_LIMIT: bool>(
    payload_len: usize,
    max_message_size: usize,
) -> usize {
    let estimated = payload_len.saturating_mul(4).max(64);
    if ENFORCE_MESSAGE_LIMIT {
        estimated.min(max_message_size)
    } else {
        estimated
    }
}

impl MessageDeflater {
    pub(crate) fn new() -> Self {
        Self {
            compressor: Compress::new(Compression::fast(), false),
            out: BytesMut::new(),
        }
    }

    pub(crate) fn compress(
        &mut self,
        payload: &[u8],
    ) -> Result<Option<Bytes>, flate2::CompressError> {
        if payload.is_empty() {
            return Ok(None);
        }

        self.compressor.reset();
        self.out.clear();
        self.out.reserve(payload.len() + FLATE_GROWTH_SLACK);

        loop {
            let before = self.compressor.total_out();
            let status = self.compressor.compress_uninit(
                &payload[self.compressor.total_in() as usize..],
                self.out.spare_capacity_mut(),
                FlushCompress::Sync,
            )?;
            let written = (self.compressor.total_out() - before) as usize;
            // SAFETY: `compress_uninit` writes exactly `written` initialized
            // bytes into the spare capacity returned by `BytesMut`.
            unsafe {
                self.out.set_len(self.out.len() + written);
            }
            if self.compressor.total_in() as usize == payload.len() && status != Status::BufError {
                break;
            }
            self.out.reserve(FLATE_GROWTH_SLACK);
        }

        if self.out.ends_with(TAIL) {
            self.out.truncate(self.out.len() - TAIL.len());
        }
        if self.out.len() >= payload.len() {
            return Ok(None);
        }
        Ok(Some(self.out.split().freeze()))
    }
}

impl Default for MessageDeflater {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageInflater {
    pub(crate) fn new() -> Self {
        Self {
            decoder: Decompress::new(false),
            out: BytesMut::new(),
        }
    }

    pub(crate) fn decompress<const ENFORCE_MESSAGE_LIMIT: bool>(
        &mut self,
        payload: &[u8],
        max_message_size: usize,
    ) -> Result<Bytes, WebSocketProtocolError> {
        self.decoder.reset(false);
        self.out.clear();
        self.out
            .reserve(decompressed_capacity::<ENFORCE_MESSAGE_LIMIT>(
                payload.len(),
                max_message_size,
            ));

        let mut input = payload;
        let mut flushing_tail = false;
        loop {
            let before = self.decoder.total_out();
            let consumed_before = self.decoder.total_in();
            let status = self
                .decoder
                .decompress_uninit(
                    input,
                    self.out.spare_capacity_mut(),
                    if flushing_tail {
                        FlushDecompress::Finish
                    } else {
                        FlushDecompress::Sync
                    },
                )
                .map_err(|_| WebSocketProtocolError::InvalidCompressedPayload)?;
            let written = (self.decoder.total_out() - before) as usize;
            // SAFETY: `decompress_uninit` writes exactly `written`
            // initialized bytes into the spare capacity returned by
            // `BytesMut`.
            unsafe {
                self.out.set_len(self.out.len() + written);
            }
            if ENFORCE_MESSAGE_LIMIT && self.out.len() > max_message_size {
                return Err(WebSocketProtocolError::MessageTooLarge);
            }
            let consumed = (self.decoder.total_in() - consumed_before) as usize;
            input = &input[consumed..];
            if status == Status::StreamEnd || (flushing_tail && input.is_empty()) {
                return Ok(self.out.split().freeze());
            }
            if input.is_empty() {
                if flushing_tail {
                    return Err(WebSocketProtocolError::InvalidCompressedPayload);
                }
                input = TAIL;
                flushing_tail = true;
            }
            self.out.reserve(FLATE_GROWTH_SLACK);
        }
    }
}

impl Default for MessageInflater {
    fn default() -> Self {
        Self::new()
    }
}

impl PerMessageDeflateMode for PerMessageDeflateEnabled {
    type Inflater = MessageInflater;
    type Deflater = MessageDeflater;

    const ENABLED: bool = true;

    fn new_inflater() -> Self::Inflater {
        MessageInflater::new()
    }

    fn new_deflater() -> Self::Deflater {
        MessageDeflater::new()
    }

    fn compress(
        deflater: &mut Self::Deflater,
        payload: &[u8],
    ) -> Result<Option<Bytes>, flate2::CompressError> {
        deflater.compress(payload)
    }

    fn decompress<const ENFORCE_MESSAGE_LIMIT: bool>(
        inflater: &mut Self::Inflater,
        payload: &[u8],
        max_message_size: usize,
    ) -> Result<Bytes, WebSocketProtocolError> {
        inflater.decompress::<ENFORCE_MESSAGE_LIMIT>(payload, max_message_size)
    }
}

impl PerMessageDeflateMode for PerMessageDeflateDisabled {
    type Inflater = ();
    type Deflater = ();

    const ENABLED: bool = false;

    fn new_inflater() -> Self::Inflater {}

    fn new_deflater() -> Self::Deflater {}

    fn compress(_: &mut Self::Deflater, _: &[u8]) -> Result<Option<Bytes>, flate2::CompressError> {
        Ok(None)
    }

    fn decompress<const ENFORCE_MESSAGE_LIMIT: bool>(
        _: &mut Self::Inflater,
        _: &[u8],
        _: usize,
    ) -> Result<Bytes, WebSocketProtocolError> {
        Err(WebSocketProtocolError::ExtensionsNotNegotiated)
    }
}
