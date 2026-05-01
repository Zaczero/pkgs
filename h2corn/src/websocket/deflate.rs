use bytes::{Bytes, BytesMut};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use memchr::memchr;

use crate::error::WebSocketProtocolError;

const PERMESSAGE_DEFLATE: &[u8] = b"permessage-deflate";
const TAIL: &[u8; 4] = b"\x00\x00\xff\xff";
const FLATE_INITIAL_GROWTH_SLACK: usize = 16;
const FLATE_MIN_GROWTH: usize = 256;

pub const PERMESSAGE_DEFLATE_RESPONSE: &[u8] =
    b"permessage-deflate; server_no_context_takeover; client_no_context_takeover";

pub trait PerMessageDeflateMode {
    type Inflater;
    type Deflater;

    const ENABLED: bool;

    fn new_inflater() -> Self::Inflater;

    fn new_deflater() -> Self::Deflater;

    fn compress(
        deflater: &mut Self::Deflater,
        payload: &[u8],
    ) -> Result<Option<Bytes>, flate2::CompressError>;

    fn decompress(
        inflater: &mut Self::Inflater,
        payload: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError>;
}

#[derive(Debug)]
pub struct PerMessageDeflateEnabled;

#[derive(Debug)]
pub struct PerMessageDeflateDisabled;

#[derive(Debug)]
pub struct MessageDeflater {
    compressor: Compress,
    out: BytesMut,
}

#[derive(Debug)]
pub struct MessageInflater {
    decoder: Decompress,
    out: BytesMut,
}

pub fn requested_by_client(value: &[u8]) -> bool {
    let mut rest = Some(value);
    while let Some(current) = rest {
        let (extension, next) = memchr(b',', current).map_or((current, None), |index| {
            (&current[..index], Some(&current[index + 1..]))
        });
        let extension = extension.trim_ascii();
        let end = memchr(b';', extension).unwrap_or(extension.len());
        if extension[..end]
            .trim_ascii()
            .eq_ignore_ascii_case(PERMESSAGE_DEFLATE)
        {
            return true;
        }
        rest = next;
    }
    false
}

fn decompressed_capacity(payload_len: usize, max_message_size: Option<usize>) -> usize {
    let estimated = payload_len.saturating_mul(4).max(64);
    max_message_size.map_or(estimated, |limit| estimated.min(limit))
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
        self.out.reserve(payload.len() + FLATE_INITIAL_GROWTH_SLACK);

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
            self.out.reserve(self.out.len().max(FLATE_MIN_GROWTH));
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

    pub(crate) fn decompress(
        &mut self,
        payload: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        self.decoder.reset(false);
        self.out.clear();
        self.out
            .reserve(decompressed_capacity(payload.len(), max_message_size));

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
            if max_message_size.is_some_and(|limit| self.out.len() > limit) {
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
            self.out.reserve(self.out.len().max(FLATE_MIN_GROWTH));
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

    fn decompress(
        inflater: &mut Self::Inflater,
        payload: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        inflater.decompress(payload, max_message_size)
    }
}

impl PerMessageDeflateMode for PerMessageDeflateDisabled {
    type Inflater = ();
    type Deflater = ();

    const ENABLED: bool = false;

    fn new_inflater() -> Self::Inflater {}

    fn new_deflater() -> Self::Deflater {}

    fn compress((): &mut Self::Deflater, _: &[u8]) -> Result<Option<Bytes>, flate2::CompressError> {
        Ok(None)
    }

    fn decompress(
        (): &mut Self::Inflater,
        _: &[u8],
        _: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        Err(WebSocketProtocolError::ExtensionsNotNegotiated)
    }
}

#[cfg(test)]
mod tests {
    use super::requested_by_client;

    #[test]
    fn requested_by_client_matches_permessage_deflate_in_any_position() {
        assert!(requested_by_client(b"permessage-deflate"));
        assert!(requested_by_client(
            b"foo, permessage-deflate; client_max_window_bits"
        ));
        assert!(requested_by_client(
            b"x-webkit-deflate-frame, permessage-deflate; server_no_context_takeover"
        ));
        assert!(!requested_by_client(b"foo, bar"));
    }
}
