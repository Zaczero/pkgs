use bytes::{Bytes, BytesMut};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

use crate::error::WebSocketProtocolError;
use crate::http::header::protocol_token_is_valid;

const PERMESSAGE_DEFLATE: &[u8] = b"permessage-deflate";
const TAIL: &[u8; 4] = b"\x00\x00\xff\xff";
const FLATE_INITIAL_GROWTH_SLACK: usize = 16;
const FLATE_MIN_GROWTH: usize = 256;

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

    fn begin_decompress(inflater: &mut Self::Inflater);

    fn push_decompress(
        inflater: &mut Self::Inflater,
        input: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketProtocolError>;

    fn finish_decompress(
        inflater: &mut Self::Inflater,
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError>;
}

#[derive(Debug)]
pub(super) struct PerMessageDeflateEnabled;

#[derive(Debug)]
pub(super) struct PerMessageDeflateDisabled;

#[derive(Debug)]
pub(super) struct MessageDeflater {
    compressor: Compress,
    out: BytesMut,
}

#[derive(Debug)]
pub(super) struct MessageInflater {
    decoder: Decompress,
    out: BytesMut,
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

    pub(crate) fn begin(&mut self) {
        self.decoder.reset(false);
        self.out.clear();
    }

    pub(crate) fn push(
        &mut self,
        mut input: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketProtocolError> {
        self.inflate(&mut input, FlushDecompress::Sync, max_message_size)?;
        Ok(())
    }

    pub(crate) fn finish(
        &mut self,
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        let mut tail = TAIL.as_slice();
        let status = self.inflate(&mut tail, FlushDecompress::Finish, max_message_size)?;
        if status != Status::StreamEnd && !tail.is_empty() {
            return Err(WebSocketProtocolError::InvalidCompressedPayload);
        }
        Ok(self.out.split().freeze())
    }

    fn inflate(
        &mut self,
        input: &mut &[u8],
        flush: FlushDecompress,
        max_message_size: Option<usize>,
    ) -> Result<Status, WebSocketProtocolError> {
        self.out
            .reserve(decompressed_capacity(input.len(), max_message_size));
        loop {
            let before = self.decoder.total_out();
            let consumed_before = self.decoder.total_in();
            let status = self
                .decoder
                .decompress_uninit(
                    input,
                    inflate_window(&mut self.out, max_message_size),
                    flush,
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
            *input = &input[consumed..];
            if input.is_empty() || status == Status::StreamEnd {
                return Ok(status);
            }
            if consumed == 0 && written == 0 {
                return Err(WebSocketProtocolError::InvalidCompressedPayload);
            }
            let growth = self.out.len().max(FLATE_MIN_GROWTH);
            self.out.reserve(match max_message_size {
                Some(limit) => growth.min(overrun_budget(self.out.len(), limit)),
                None => growth,
            });
        }
    }
}

impl Default for MessageInflater {
    fn default() -> Self {
        Self::new()
    }
}

impl PerMessageDeflateMode for PerMessageDeflateEnabled {
    type Inflater = Option<MessageInflater>;
    type Deflater = Option<MessageDeflater>;

    const ENABLED: bool = true;

    fn new_inflater() -> Self::Inflater {
        None
    }

    fn new_deflater() -> Self::Deflater {
        None
    }

    fn compress(
        deflater: &mut Self::Deflater,
        payload: &[u8],
    ) -> Result<Option<Bytes>, flate2::CompressError> {
        if payload.is_empty() {
            return Ok(None);
        }
        deflater
            .get_or_insert_with(MessageDeflater::new)
            .compress(payload)
    }

    fn begin_decompress(inflater: &mut Self::Inflater) {
        inflater.get_or_insert_with(MessageInflater::new).begin();
    }

    fn push_decompress(
        inflater: &mut Self::Inflater,
        input: &[u8],
        max_message_size: Option<usize>,
    ) -> Result<(), WebSocketProtocolError> {
        inflater
            .get_or_insert_with(MessageInflater::new)
            .push(input, max_message_size)
    }

    fn finish_decompress(
        inflater: &mut Self::Inflater,
        max_message_size: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        inflater
            .get_or_insert_with(MessageInflater::new)
            .finish(max_message_size)
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

    fn begin_decompress((): &mut Self::Inflater) {}

    fn push_decompress(
        (): &mut Self::Inflater,
        _: &[u8],
        _: Option<usize>,
    ) -> Result<(), WebSocketProtocolError> {
        Err(WebSocketProtocolError::ExtensionsNotNegotiated)
    }

    fn finish_decompress(
        (): &mut Self::Inflater,
        _: Option<usize>,
    ) -> Result<Bytes, WebSocketProtocolError> {
        Err(WebSocketProtocolError::ExtensionsNotNegotiated)
    }
}

enum ExtensionParameterValue<'a> {
    Token(&'a [u8]),
    Quoted(&'a [u8]),
}

impl ExtensionParameterValue<'_> {
    fn valid_window_bits(self) -> bool {
        match self {
            Self::Token(value) => window_bits_are_valid(value),
            Self::Quoted(value) => quoted_window_bits_are_valid(value),
        }
    }
}

/// Select only an offer the server can actually honor. Invalid offers are not
/// protocol errors: RFC 7692 lets a server decline one and select a later
/// valid offer from the same header value.
pub(crate) fn offers_compatible_permessage_deflate(value: &[u8]) -> bool {
    let mut index = 0;
    'offers: while index < value.len() {
        skip_bws(value, &mut index);
        let Some(name) = read_token(value, &mut index) else {
            if !skip_malformed_offer(value, &mut index) {
                return false;
            }
            continue;
        };
        let target = name.eq_ignore_ascii_case(PERMESSAGE_DEFLATE);
        let mut compatible = target;
        let mut seen = 0_u8;

        loop {
            skip_bws(value, &mut index);
            match value.get(index) {
                Some(b';') => index += 1,
                Some(b',') | None => break,
                _ => {
                    if !skip_malformed_offer(value, &mut index) {
                        return false;
                    }
                    continue 'offers;
                },
            }
            skip_bws(value, &mut index);
            let Some(parameter) = read_token(value, &mut index) else {
                if !skip_malformed_offer(value, &mut index) {
                    return false;
                }
                continue 'offers;
            };
            skip_bws(value, &mut index);
            let has_value = value.get(index) == Some(&b'=');
            let parameter_value = if has_value {
                index += 1;
                skip_bws(value, &mut index);
                let parsed_value = if value.get(index) == Some(&b'"') {
                    read_quoted_string(value, &mut index).map(ExtensionParameterValue::Quoted)
                } else {
                    read_token(value, &mut index).map(ExtensionParameterValue::Token)
                };
                let Some(parsed_value) = parsed_value else {
                    if !skip_malformed_offer(value, &mut index) {
                        return false;
                    }
                    continue 'offers;
                };
                Some(parsed_value)
            } else {
                None
            };

            if !target {
                continue;
            }
            let (bit, valid) = if parameter.eq_ignore_ascii_case(b"client_no_context_takeover") {
                (1, parameter_value.is_none())
            } else if parameter.eq_ignore_ascii_case(b"server_no_context_takeover") {
                (2, parameter_value.is_none())
            } else if parameter.eq_ignore_ascii_case(b"client_max_window_bits") {
                (4, parameter_value.is_none_or(valid_window_bits))
            } else if parameter.eq_ignore_ascii_case(b"server_max_window_bits") {
                (8, false)
            } else {
                (16, false)
            };
            if seen & bit != 0 || !valid {
                compatible = false;
            }
            seen |= bit;
        }
        if target && compatible {
            return true;
        }
        if value.get(index) == Some(&b',') {
            index += 1;
            continue;
        }
        return false;
    }
    false
}

fn valid_window_bits(value: ExtensionParameterValue<'_>) -> bool {
    value.valid_window_bits()
}

fn quoted_window_bits_are_valid(value: &[u8]) -> bool {
    let mut unescaped = [0; 2];
    let mut length = 0;
    let mut index = 0;
    while let Some(&byte) = value.get(index) {
        index += 1;
        let byte = if byte == b'\\' {
            let Some(&escaped) = value.get(index) else {
                return false;
            };
            index += 1;
            escaped
        } else {
            byte
        };
        let Some(slot) = unescaped.get_mut(length) else {
            return false;
        };
        *slot = byte;
        length += 1;
    }
    window_bits_are_valid(&unescaped[..length])
}

const fn window_bits_are_valid(value: &[u8]) -> bool {
    matches!(value, [b'8'..=b'9'] | [b'1', b'0'..=b'5'])
}

/// A malformed offer does not invalidate later comma-separated offers.
///
/// The enclosing header value is an extension list; once one member cannot
/// be parsed, discard that member and resume at the next list delimiter.
fn skip_malformed_offer(value: &[u8], index: &mut usize) -> bool {
    while let Some(&byte) = value.get(*index) {
        *index += 1;
        if byte == b',' {
            return true;
        }
    }
    false
}

fn skip_bws(value: &[u8], index: &mut usize) {
    while value
        .get(*index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        *index += 1;
    }
}

fn read_token<'a>(value: &'a [u8], index: &mut usize) -> Option<&'a [u8]> {
    let start = *index;
    while value
        .get(*index)
        .is_some_and(|byte| protocol_token_is_valid(&[*byte]))
    {
        *index += 1;
    }
    (*index > start).then(|| &value[start..*index])
}

fn read_quoted_string<'a>(value: &'a [u8], index: &mut usize) -> Option<&'a [u8]> {
    let start = *index;
    *index += 1;
    while let Some(&byte) = value.get(*index) {
        *index += 1;
        match byte {
            b'\"' => return Some(&value[start + 1..*index - 1]),
            b'\\' => {
                let escaped = *value.get(*index)?;
                if escaped.is_ascii_control() || escaped == 0x7F {
                    return None;
                }
                *index += 1;
            },
            byte if byte.is_ascii_control() || byte == 0x7F => return None,
            _ => {},
        }
    }
    None
}

/// One byte past the remaining budget: enough to observe an overrun, never
/// enough to materialize one.
const fn overrun_budget(written: usize, limit: usize) -> usize {
    limit.saturating_sub(written).saturating_add(1)
}

/// The spare capacity zlib may fill this round.
///
/// `BytesMut::reserve` grows geometrically, so handing zlib the whole spare
/// capacity let a message one byte over `max_message_size` decompress to
/// *twice* the limit before the length check rejected it — a hostile peer
/// bought 32 MiB of inflate work and residency at the 16 MiB default for a
/// 64 KiB compressed frame. Bounding the window makes the check the thing
/// that stops the work rather than something that reports it afterwards.
///
/// At the exact limit the window is one byte, which still lets a terminating
/// tail that produces no output be consumed.
fn inflate_window(
    out: &mut BytesMut,
    max_message_size: Option<usize>,
) -> &mut [std::mem::MaybeUninit<u8>] {
    let written = out.len();
    let spare = out.spare_capacity_mut();
    let Some(limit) = max_message_size else {
        return spare;
    };
    let window = spare.len().min(overrun_budget(written, limit));
    &mut spare[..window]
}

fn decompressed_capacity(payload_len: usize, max_message_size: Option<usize>) -> usize {
    let estimated = payload_len.saturating_mul(4).max(64);
    max_message_size.map_or(estimated, |limit| estimated.min(limit))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use flate2::{Compress, Compression, FlushCompress, Status};

    use super::{
        MessageInflater, PerMessageDeflateEnabled, PerMessageDeflateMode as _,
        offers_compatible_permessage_deflate,
    };
    use crate::error::WebSocketProtocolError;

    /// A raw DEFLATE stream that inflates to `len` bytes, as an RFC 7692
    /// message payload does.
    fn compressed_zeroes(len: usize) -> Vec<u8> {
        let mut compressor = Compress::new(Compression::default(), false);
        let source = vec![0_u8; len];
        let mut out = vec![0_u8; len / 8 + 1024];
        let status = compressor
            .compress(&source, &mut out, FlushCompress::Sync)
            .expect("zeroes compress");
        assert_ne!(status, Status::BufError);
        let produced = compressor.total_out() as usize;
        out.truncate(produced);
        out
    }

    /// A message over the limit is rejected without first materializing twice
    /// the limit. Handing zlib the whole geometrically grown spare capacity
    /// used to decompress 2x `max_message_size` before the length check ran,
    /// so a small hostile frame bought a large multiple of inflate work.
    #[test]
    fn oversized_message_does_not_inflate_past_its_limit() {
        const LIMIT: usize = 64 * 1024;

        let payload = compressed_zeroes(LIMIT * 4);
        let mut inflater = MessageInflater::new();
        inflater.begin();
        let outcome = inflater
            .push(&payload, Some(LIMIT))
            .and_then(|()| inflater.finish(Some(LIMIT)).map(|_| ()));

        assert!(matches!(
            outcome,
            Err(WebSocketProtocolError::MessageTooLarge)
        ));
        assert!(
            inflater.out.len() <= LIMIT + 1,
            "inflated {} bytes against a {LIMIT}-byte limit",
            inflater.out.len(),
        );
    }

    /// A message that exactly reaches the limit is still delivered: the
    /// one-byte overrun window must not truncate a legal message.
    #[test]
    fn message_at_exactly_the_limit_is_accepted() {
        const LIMIT: usize = 32 * 1024;

        let payload = compressed_zeroes(LIMIT);
        let mut inflater = MessageInflater::new();
        inflater.begin();
        inflater
            .push(&payload, Some(LIMIT))
            .expect("a message at the limit inflates");
        let message = inflater.finish(Some(LIMIT)).expect("and completes");

        assert_eq!(message.len(), LIMIT);
        assert!(message.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn inflate_window_is_unbounded_without_a_limit() {
        let mut out = BytesMut::with_capacity(4096);
        assert_eq!(super::inflate_window(&mut out, None).len(), out.capacity());
    }

    #[test]
    fn negotiated_codec_state_is_lazy_and_directional() {
        let inflater = PerMessageDeflateEnabled::new_inflater();
        let mut deflater = PerMessageDeflateEnabled::new_deflater();
        assert!(inflater.is_none());
        assert!(deflater.is_none());

        assert!(
            PerMessageDeflateEnabled::compress(&mut deflater, b"")
                .unwrap()
                .is_none()
        );
        assert!(deflater.is_none(), "empty sends do not need zlib state");

        let _ = PerMessageDeflateEnabled::compress(&mut deflater, b"compressible compressible")
            .unwrap();
        assert!(deflater.is_some());
        assert!(
            inflater.is_none(),
            "outbound traffic does not allocate an inflater"
        );
    }

    #[test]
    fn deflate_offer_parser_selects_a_later_compatible_offer() {
        assert!(offers_compatible_permessage_deflate(b"permessage-deflate"));
        assert!(offers_compatible_permessage_deflate(
            b"foo, permessage-deflate; client_max_window_bits"
        ));
        assert!(offers_compatible_permessage_deflate(
            b"x-webkit-deflate-frame, permessage-deflate; server_no_context_takeover"
        ));
        assert!(offers_compatible_permessage_deflate(
            b"permessage-deflate; server_max_window_bits=12, permessage-deflate"
        ));
        for offer in [
            b"permessage-deflate; client_max_window_bits=07".as_slice(),
            b"permessage-deflate; client_max_window_bits=16",
            b"permessage-deflate; client_max_window_bits=8; client_max_window_bits=9",
            b"permessage-deflate; server_max_window_bits=12",
            b"permessage-deflate; unknown",
            b"permessage-deflate; client_no_context_takeover=yes",
            b"permessage-deflate; =8",
        ] {
            assert!(!offers_compatible_permessage_deflate(offer), "{offer:?}");
        }
    }
}
