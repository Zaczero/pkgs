use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use http::Method;

use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::h2_frame::{self, ErrorCode, RawFrame, StreamId};
use crate::hpack::{DecodeBlockError, Decoder, DecoderError, HpackField};
use crate::http::header::{
    header_is_single_token, lowercase_header_name_is_valid, parse_content_length_header,
    protocol_token_is_valid, request_authority_is_valid, request_path_is_valid,
    request_scheme_is_valid, trailer_field_name_is_forbidden,
};
use crate::http::header_meta::RequestHeaderMeta;
use crate::http::header_value::header_value_is_valid;
use crate::http::types::{
    BytesStr, ConnectAuthority, HttpStatusCode, HttpVersion, KnownRequestHeaderName, Protocol,
    RequestAuthority, RequestHead, RequestHeaderName, RequestHeaderValue, RequestHeaders,
    RequestTarget, parse_request_method, status_code,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct HeaderLimits {
    pub(super) max_list_size: Option<NonZeroUsize>,
    pub(super) max_fields: Option<NonZeroUsize>,
}

impl HeaderLimits {
    pub(super) const fn new(
        max_list_size: Option<NonZeroUsize>,
        max_fields: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            max_list_size,
            max_fields,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HeaderBudget {
    limits: HeaderLimits,
    list_size: usize,
    fields: usize,
}

impl HeaderBudget {
    const fn new(limits: HeaderLimits) -> Self {
        Self {
            limits,
            list_size: 0,
            fields: 0,
        }
    }

    /// Charge decoded-list size and, for regular fields, the field count.
    ///
    /// Pseudo-headers are sized into the list budget but do not consume the
    /// regular-field count (see `limit_request_fields`).
    fn account(&mut self, field: &HpackField) -> Result<(), RequestHeadError> {
        if !field.name().starts_with(b":") {
            self.account_field()?;
        }
        self.account_bytes(field.table_size())
    }

    const fn account_bytes(&mut self, len: usize) -> Result<(), RequestHeadError> {
        if let Some(max_header_list_size) = self.limits.max_list_size {
            self.list_size = self.list_size.saturating_add(len);
            if self.list_size > max_header_list_size.get() {
                return Err(RequestHeadError::reject(
                    status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                ));
            }
        }
        Ok(())
    }

    const fn account_field(&mut self) -> Result<(), RequestHeadError> {
        if let Some(max_fields) = self.limits.max_fields {
            self.fields += 1;
            if self.fields > max_fields.get() {
                return Err(RequestHeadError::reject(
                    status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                ));
            }
        }
        Ok(())
    }
}

/// What a HEADERS/CONTINUATION sequence becomes once the compressed block is
/// complete. Classification is fixed at the first fragment so CONTINUATION
/// never reinterprets the owner.
#[derive(Clone, Copy, Debug)]
pub(super) enum HeaderBlockTarget {
    RequestHead { end_stream: bool },
    Trailers { end_stream: bool },
    RejectedNew { error_code: ErrorCode },
    RejectedTracked { error_code: ErrorCode },
}

/// One in-flight header block, owned from the first fragment through HPACK
/// consumption — request head, trailers, or a stream that will be reset after
/// the block is fully decoded.
#[derive(Debug)]
pub(super) struct PendingHeaderBlock {
    pub stream_id: StreamId,
    pub target: HeaderBlockTarget,
    pub block: BytesMut,
}

pub(super) struct HeaderBlockFragment {
    pub block: Bytes,
    pub end_headers: bool,
    pub end_stream: bool,
}

#[derive(Debug)]
pub(super) enum RequestHeadError {
    Connection {
        error_code: ErrorCode,
        error: H2Error,
    },
    Reject {
        status: HttpStatusCode,
    },
    Stream {
        error_code: ErrorCode,
    },
}

impl RequestHeadError {
    const fn compression(error: H2Error) -> Self {
        Self::Connection {
            error_code: ErrorCode::COMPRESSION_ERROR,
            error,
        }
    }

    pub(super) const fn stream_protocol() -> Self {
        Self::Stream {
            error_code: ErrorCode::PROTOCOL_ERROR,
        }
    }

    pub(super) const fn reject(status: HttpStatusCode) -> Self {
        Self::Reject { status }
    }
}

struct RequestHeadBuilder {
    method: Option<Method>,
    scheme: Option<BytesStr>,
    authority: Option<BytesStr>,
    path: Option<BytesStr>,
    protocol: Option<Protocol>,
    regular_headers: Vec<(RequestHeaderName, RequestHeaderValue)>,
    section: HeaderSection,
    host_header_index: Option<usize>,
    budget: HeaderBudget,
    header_meta: RequestHeaderMeta,
    /// When true (TLS), force `:scheme` to `https`; plaintext keeps the peer value.
    force_https: bool,
}

/// A regular HTTP/2 field after the syntax shared by request heads and
/// trailers has been accepted. Head-specific metadata is deliberately added
/// only after this boundary, so a trailer cannot accidentally affect routing
/// or request framing.
struct ValidatedH2RegularField {
    name: BytesStr,
    value: Bytes,
    known_name: Option<KnownRequestHeaderName>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum HeaderSection {
    #[default]
    Pseudo,
    Regular,
}

impl RequestHeadBuilder {
    fn new(limits: HeaderLimits, force_https: bool) -> Self {
        Self {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            protocol: None,
            regular_headers: Vec::with_capacity(16),
            section: HeaderSection::default(),
            host_header_index: None,
            budget: HeaderBudget::new(limits),
            header_meta: RequestHeaderMeta::default(),
            force_https,
        }
    }

    /// Semantic ingress for decoded HPACK fields.
    ///
    /// Budgets are charged from raw `table_size` before any HTTP validation so
    /// list accounting tracks the same octets the peer spent. Malformed fields
    /// become stream `PROTOCOL_ERROR`; configured header-budget rejection is
    /// the existing 431 response path.
    fn push(&mut self, field: HpackField) -> Result<(), RequestHeadError> {
        self.budget.account(&field)?;

        let (name, value) = field.into_parts();
        if let Some(pseudo) = name.as_ref().strip_prefix(b":") {
            return self.push_pseudo(pseudo, value);
        }

        self.section = HeaderSection::Regular;
        let field = validate_regular_h2_field(name, value)
            .map_err(|_| RequestHeadError::stream_protocol())?;
        self.push_regular_header(field)
            .map_err(|()| RequestHeadError::stream_protocol())
    }

    fn push_pseudo(&mut self, pseudo: &[u8], value: Bytes) -> Result<(), RequestHeadError> {
        if !header_value_is_valid(value.as_ref())
            || value
                .first()
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            || value
                .last()
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            return Err(RequestHeadError::stream_protocol());
        }
        match pseudo {
            b"method" => {
                let method = parse_request_method(&value)
                    .map_err(|_| RequestHeadError::stream_protocol())?;
                push_request_pseudo(&mut self.method, self.section, method)
                    .map_err(|()| RequestHeadError::stream_protocol())
            },
            b"scheme" => {
                if !request_scheme_is_valid(value.as_ref()) {
                    return Err(RequestHeadError::stream_protocol());
                }
                let value =
                    BytesStr::try_from(value).map_err(|_| RequestHeadError::stream_protocol())?;
                push_request_pseudo(&mut self.scheme, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())
            },
            b"authority" => {
                if !request_authority_is_valid(value.as_ref()) {
                    return Err(RequestHeadError::stream_protocol());
                }
                let value =
                    BytesStr::try_from(value).map_err(|_| RequestHeadError::stream_protocol())?;
                push_request_pseudo(&mut self.authority, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())
            },
            b"path" => {
                let value =
                    BytesStr::try_from(value).map_err(|_| RequestHeadError::stream_protocol())?;
                push_request_pseudo(&mut self.path, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())
            },
            b"protocol" => {
                if !protocol_token_is_valid(value.as_ref()) {
                    return Err(RequestHeadError::stream_protocol());
                }
                let value =
                    Protocol::try_from(value).map_err(|_| RequestHeadError::stream_protocol())?;
                push_request_pseudo(&mut self.protocol, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())
            },
            // `:status` and unknown pseudos are malformed on requests.
            _ => Err(RequestHeadError::stream_protocol()),
        }
    }

    fn push_regular_header(&mut self, field: ValidatedH2RegularField) -> Result<(), ()> {
        let ValidatedH2RegularField {
            name,
            value,
            known_name,
        } = field;
        let value_bytes = value.as_ref();

        match known_name {
            Some(KnownRequestHeaderName::Host) if !request_authority_is_valid(value_bytes) => {
                return Err(());
            },
            Some(KnownRequestHeaderName::Te) => {
                self.header_meta.set_accepts_trailers();
            },
            Some(KnownRequestHeaderName::ContentLength) => {
                let parsed = parse_content_length_header(value_bytes).ok_or(())?;
                if self
                    .header_meta
                    .content_length()
                    .is_some_and(|existing| existing != parsed)
                {
                    return Err(());
                }
                self.header_meta.set_content_length(Some(parsed));
            },
            _ => {},
        }
        if let Some(known_name) = known_name {
            self.header_meta
                .observe_known_header(known_name, &value, self.regular_headers.len());
        }
        if known_name == Some(KnownRequestHeaderName::Host) {
            if self.host_header_index.is_some() {
                return Err(());
            }
            self.host_header_index = Some(self.regular_headers.len());
        }
        let name = known_name.map_or(RequestHeaderName::Other(name), RequestHeaderName::Known);
        let value = RequestHeaderValue::from_h2_validated(value);
        self.regular_headers.push((name, value));
        Ok(())
    }

    fn finish(mut self) -> Result<RequestHead, ()> {
        let method = self.method.take().ok_or(())?;

        // RFC 9113 8.3.1: a Host header that disagrees with :authority makes
        // the request malformed. This runs before the target below takes the
        // authority, so CONNECT and extended CONNECT are covered too.
        if let (Some(authority), Some(host_header_index)) =
            (&self.authority, self.host_header_index)
            && !self
                .regular_headers
                .get(host_header_index)
                .expect("recorded host index must exist")
                .1
                .as_bytes()
                .eq_ignore_ascii_case(authority.as_ref())
        {
            return Err(());
        }

        let target = match (method == Method::CONNECT, self.protocol.take()) {
            (false, None) => {
                let scheme = self.scheme.take().ok_or(())?;
                let path = self.path.take().ok_or(())?;
                let scheme = self.resolve_scheme(scheme);
                if !request_scheme_is_valid(scheme.as_ref())
                    || !request_path_is_valid(&method, path.as_ref())
                {
                    return Err(());
                }
                RequestTarget::normal(scheme, path)
            },
            (false, Some(_)) => return Err(()),
            (true, None) => {
                if self.scheme.is_some() || self.path.is_some() {
                    return Err(());
                }
                let authority = self.authority.take().ok_or(())?;
                RequestTarget::connect(ConnectAuthority::try_from(authority).map_err(|_| ())?)
            },
            (true, Some(protocol)) => {
                let scheme = self.scheme.take().ok_or(())?;
                let path = self.path.take().ok_or(())?;
                let scheme = self.resolve_scheme(scheme);
                let authority = self.authority.take().ok_or(())?;
                if !request_scheme_is_valid(scheme.as_ref())
                    || !request_authority_is_valid(authority.as_ref())
                    || !request_path_is_valid(&method, path.as_ref())
                {
                    return Err(());
                }
                RequestTarget::extended_connect(
                    RequestAuthority::new(authority),
                    protocol,
                    scheme,
                    path,
                )
            },
        };

        if self.host_header_index.is_none() {
            let index = self.regular_headers.len();
            let host = self
                .authority
                .take()
                .map(|authority| RequestHeaderValue::from_h2_validated(authority.into_inner()))
                .or_else(|| {
                    target.authority().map(|authority| {
                        RequestHeaderValue::from_h2_validated(
                            authority.clone().into_bytes_str().into_inner(),
                        )
                    })
                });
            if let Some(host) = host {
                if self.regular_headers.is_empty() {
                    self.regular_headers.reserve(1);
                }
                self.regular_headers
                    .push((KnownRequestHeaderName::Host.into(), host));
                self.host_header_index = Some(index);
            }
        }

        Ok(RequestHead {
            http_version: HttpVersion::Http2,
            method,
            target,
            headers: RequestHeaders::from_h2(self.regular_headers),
            header_meta: self.header_meta,
        })
    }

    fn resolve_scheme(&self, scheme: BytesStr) -> BytesStr {
        if self.force_https {
            BytesStr::from_static("https")
        } else {
            scheme
        }
    }
}

fn validate_regular_h2_field(
    name: Bytes,
    value: Bytes,
) -> Result<ValidatedH2RegularField, H2Error> {
    // Generated names are grammar-proven lower-case HTTP tokens. Try that
    // exact classification before scanning the generic-name path; unknown
    // names, including every upper-case spelling, still require the full H2
    // lower-case validation below.
    let known_name = KnownRequestHeaderName::from_bytes(name.as_ref());
    if (known_name.is_none() && !lowercase_header_name_is_valid(name.as_ref()))
        || !header_value_is_valid(value.as_ref())
        || value
            .first()
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        || value
            .last()
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        return Err(H2Error::InvalidRequestField);
    }
    // SAFETY: a valid HTTP field name is an ASCII token.
    let name = unsafe { BytesStr::from_validated_ascii(name) };
    match known_name {
        Some(
            KnownRequestHeaderName::Connection
            | KnownRequestHeaderName::ProxyConnection
            | KnownRequestHeaderName::KeepAlive
            | KnownRequestHeaderName::TransferEncoding
            | KnownRequestHeaderName::Upgrade,
        ) => return Err(H2Error::InvalidRequestField),
        Some(KnownRequestHeaderName::Te)
            if !header_is_single_token(value.as_ref(), b"trailers") =>
        {
            return Err(H2Error::InvalidRequestField);
        },
        _ => {},
    }
    Ok(ValidatedH2RegularField {
        name,
        value,
        known_name,
    })
}

fn push_request_pseudo<T>(
    slot: &mut Option<T>,
    section: HeaderSection,
    value: T,
) -> Result<(), ()> {
    if section != HeaderSection::Pseudo || slot.is_some() {
        return Err(());
    }
    *slot = Some(value);
    Ok(())
}

pub(super) fn parse_header_block_fragment(
    frame: RawFrame,
) -> Result<HeaderBlockFragment, H2CornError> {
    let header = frame.header;
    let payload = frame.payload;
    let payload_bytes = payload.as_ref();
    let mut payload_offset = 0;
    let padded = header.flags.contains(h2_frame::FrameFlags::PADDED);
    let priority = header.flags.contains(h2_frame::FrameFlags::PRIORITY);

    if padded {
        if payload_bytes.is_empty() {
            return H2Error::HeadersPaddedMissingPadLength.err();
        }
        payload_offset += 1;
    }

    if priority {
        if payload_bytes.len() < payload_offset + 5 {
            return H2Error::HeadersPriorityTooShort.err();
        }
        payload_offset += 5;
    }

    let pad_len = if padded {
        usize::from(payload_bytes[0])
    } else {
        0
    };
    if payload_bytes.len() < payload_offset + pad_len {
        return H2Error::HeadersPaddingExceedsPayload.err();
    }
    let block_end = payload_bytes.len() - pad_len;

    Ok(HeaderBlockFragment {
        block: if payload_offset == 0 && block_end == payload.len() {
            payload
        } else {
            payload.slice(payload_offset..block_end)
        },
        end_headers: header.flags.contains(h2_frame::FrameFlags::END_HEADERS),
        end_stream: header.flags.contains(h2_frame::FrameFlags::END_STREAM),
    })
}

pub(super) fn resolve_request_head(
    end_stream: bool,
    decoded: Result<RequestHead, RequestHeadError>,
) -> Result<RequestHead, RequestHeadError> {
    let request = decoded?;
    if end_stream
        && request
            .content_length()
            .is_some_and(|content_length| content_length != 0)
    {
        return Err(RequestHeadError::Stream {
            error_code: ErrorCode::PROTOCOL_ERROR,
        });
    }
    Ok(request)
}

pub(super) fn decode_request_head(
    decoder: &mut Decoder,
    block: Bytes,
    limits: HeaderLimits,
    force_https: bool,
) -> Result<RequestHead, RequestHeadError> {
    let mut builder = RequestHeadBuilder::new(limits, force_https);
    let mut bytes = block;
    match decoder.decode_block(&mut bytes, |field| builder.push(field)) {
        Ok(()) => {},
        Err(DecodeBlockError::Compression(err)) => return Err(map_hpack_error(err)),
        Err(DecodeBlockError::Rejected(err)) => return Err(err),
    }

    builder
        .finish()
        .map_err(|()| RequestHeadError::stream_protocol())
}

pub(super) fn decode_trailer_block(
    decoder: &mut Decoder,
    block: Bytes,
    limits: HeaderLimits,
) -> Result<(), RequestHeadError> {
    let mut budget = HeaderBudget::new(limits);
    let mut bytes = block;
    match decoder.decode_block(&mut bytes, |field| {
        if field.name().starts_with(b":") {
            return Err(RequestHeadError::stream_protocol());
        }
        budget.account(&field)?;
        let (name, value) = field.into_parts();
        let field = validate_regular_h2_field(name, value)
            .map_err(|_| RequestHeadError::stream_protocol())?;
        if trailer_field_name_is_forbidden(field.name.as_ref()) {
            return Err(RequestHeadError::stream_protocol());
        }
        Ok(())
    }) {
        Ok(()) => Ok(()),
        Err(DecodeBlockError::Compression(err)) => Err(map_hpack_error(err)),
        Err(DecodeBlockError::Rejected(err)) => Err(err),
    }
}

/// Consume a block whose stream is already rejected: keep the dynamic table
/// aligned with the peer without applying request or trailer semantics.
pub(super) fn decode_discarded_header_block(
    decoder: &mut Decoder,
    block: Bytes,
) -> Result<(), DecoderError> {
    let mut bytes = block;
    match decoder.decode_block(&mut bytes, |_| Ok::<(), ()>(())) {
        Ok(()) | Err(DecodeBlockError::Rejected(())) => Ok(()),
        Err(DecodeBlockError::Compression(err)) => Err(err),
    }
}

fn map_hpack_error(err: DecoderError) -> RequestHeadError {
    let error = match err {
        DecoderError::NeedMore(_) => H2Error::IncompleteHpackHeaderBlock,
        other => H2Error::HpackDecode {
            detail: format!("{other:?}").into_boxed_str(),
        },
    };
    RequestHeadError::compression(error)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::{Bytes, BytesMut};
    use http::Method;

    use super::{HeaderLimits, RequestHeadError, decode_request_head, decode_trailer_block};
    use crate::h2_frame::ErrorCode;
    use crate::hpack::{DecodeBlockError, Decoder, DecoderError, Encoder, HpackField};
    use crate::http::types::{KnownRequestHeaderName, RequestHeaderNameRef, status_code};

    fn encode_request_block(headers: &[(&[u8], &[u8])]) -> Bytes {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        for (name, value) in headers {
            encoder.encode_field_bytes(name, value, &mut block);
        }
        block.freeze()
    }

    #[test]
    fn header_list_limit_counts_rfc_overhead() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"x0", b"1"),
            (b"x1", b"1"),
            (b"x2", b"1"),
            (b"x3", b"1"),
            (b"x4", b"1"),
            (b"x5", b"1"),
            (b"x6", b"1"),
            (b"x7", b"1"),
            (b"x8", b"1"),
            (b"x9", b"1"),
        ]);

        let mut decoder = Decoder::new(4096);
        let result = decode_request_head(
            &mut decoder,
            block,
            HeaderLimits::new(NonZeroUsize::new(90), None),
            false,
        );

        assert!(matches!(
            result,
            Err(RequestHeadError::Reject {
                status: status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
            })
        ));
    }

    #[test]
    fn synthesized_host_is_not_counted_against_received_limit() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
        ]);
        let exact_received_size = HpackField::from_static(b":method", b"GET").table_size()
            + HpackField::from_static(b":scheme", b"http").table_size()
            + HpackField::from_static(b":authority", b"example.com").table_size()
            + HpackField::from_static(b":path", b"/").table_size();

        let mut decoder = Decoder::new(4096);
        let request = decode_request_head(
            &mut decoder,
            block,
            HeaderLimits::new(NonZeroUsize::new(exact_received_size), None),
            false,
        )
        .expect("received header list size should ignore synthesized host");

        assert_eq!(request.headers.len(), 1);
        let host = request.headers.get(0).expect("synthesized host exists");
        assert_eq!(
            host.name(),
            RequestHeaderNameRef::Known(KnownRequestHeaderName::Host)
        );
        assert_eq!(host.value(), b"example.com");
    }

    #[test]
    fn secure_connection_overrides_http2_scheme() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
        ]);

        let mut decoder = Decoder::new(4096);
        let request = decode_request_head(&mut decoder, block, HeaderLimits::new(None, None), true)
            .expect("valid HTTP/2 request should decode");

        assert_eq!(request.scheme_str(), "https");
    }

    #[test]
    fn cleartext_connection_preserves_http2_scheme() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"https"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
        ]);

        let mut decoder = Decoder::new(4096);
        let request =
            decode_request_head(&mut decoder, block, HeaderLimits::new(None, None), false)
                .expect("valid HTTP/2 request should decode");

        assert_eq!(request.scheme_str(), "https");
    }

    #[test]
    fn header_field_limit_counts_regular_fields_only() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"x0", b"1"),
            (b"x1", b"1"),
        ]);

        let mut decoder = Decoder::new(4096);
        let result = decode_request_head(
            &mut decoder,
            block,
            HeaderLimits::new(None, NonZeroUsize::new(1)),
            false,
        );

        assert!(matches!(
            result,
            Err(RequestHeadError::Reject {
                status: status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
            })
        ));
    }

    /// RFC 9113 8.3.1 — the check must cover CONNECT, whose target consumes
    /// the authority. A proxy that routes on `:authority` while the
    /// application trusts `Host` must never see the two disagree.
    #[test]
    fn connect_rejects_host_disagreeing_with_authority() {
        for extra in [
            [].as_slice(),
            [(b":protocol".as_slice(), b"websocket".as_slice())].as_slice(),
        ] {
            let mut headers: Vec<(&[u8], &[u8])> = vec![
                (b":method", b"CONNECT"),
                (b":authority", b"example.com"),
                (b"host", b"attacker.example"),
            ];
            if !extra.is_empty() {
                headers.extend_from_slice(&[(b":scheme", b"https"), (b":path", b"/chat")]);
                headers.extend_from_slice(extra);
            }
            let block = encode_request_block(&headers);

            let mut decoder = Decoder::new(4096);
            let result =
                decode_request_head(&mut decoder, block, HeaderLimits::new(None, None), true);

            assert!(
                matches!(result, Err(RequestHeadError::Stream { .. })),
                "CONNECT with a mismatched Host must be a stream protocol error, \
                 got {result:?}"
            );
        }
    }

    #[test]
    fn connect_accepts_host_matching_authority() {
        let block = encode_request_block(&[
            (b":method", b"CONNECT"),
            (b":authority", b"example.com:443"),
            (b"host", b"EXAMPLE.com:443"),
        ]);

        let mut decoder = Decoder::new(4096);
        let request = decode_request_head(&mut decoder, block, HeaderLimits::new(None, None), true)
            .expect("a matching Host is a valid CONNECT request");

        assert_eq!(request.method, Method::CONNECT);
    }

    #[test]
    fn trailer_field_limit_rejects_excess_fields() {
        let block = encode_request_block(&[(b"x0", b"1"), (b"x1", b"1")]);

        let mut decoder = Decoder::new(4096);
        let result = decode_trailer_block(
            &mut decoder,
            block,
            HeaderLimits::new(None, NonZeroUsize::new(1)),
        );

        assert!(matches!(
            result,
            Err(RequestHeadError::Reject {
                status: status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
            })
        ));
    }

    #[test]
    fn uppercase_field_name_is_stream_protocol_error() {
        let block = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"X-Bad", b"1"),
        ]);

        let mut decoder = Decoder::new(4096);
        let result = decode_request_head(&mut decoder, block, HeaderLimits::new(None, None), false);

        assert!(matches!(
            result,
            Err(RequestHeadError::Stream {
                error_code: ErrorCode::PROTOCOL_ERROR,
            })
        ));
    }

    #[test]
    fn unknown_field_names_keep_h2_lowercase_token_validation() {
        let valid = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"x-extension_1", b"ok"),
        ]);
        let invalid = encode_request_block(&[
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"x invalid", b"bad"),
        ]);

        let mut decoder = Decoder::new(4096);
        decode_request_head(&mut decoder, valid, HeaderLimits::new(None, None), false)
            .expect("valid unknown field must be accepted");

        let mut decoder = Decoder::new(4096);
        let result =
            decode_request_head(&mut decoder, invalid, HeaderLimits::new(None, None), false);
        assert!(matches!(
            result,
            Err(RequestHeadError::Stream {
                error_code: ErrorCode::PROTOCOL_ERROR,
            })
        ));
    }

    /// Budget rejection then bad HPACK must still surface as compression error.
    #[test]
    fn budget_rejection_then_hpack_error_is_compression_error() {
        let mut encoder = Encoder::new();
        let mut block = BytesMut::new();
        encoder.begin_block(&mut block);
        for (name, value) in [
            (b":method".as_slice(), b"GET".as_slice()),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
        ] {
            encoder.encode_field_bytes(name, value, &mut block);
        }
        // Two regular fields with limit 1 → 431 mid-block after first.
        encoder.encode_field_bytes(b"x0", b"1", &mut block);
        encoder.encode_field_bytes(b"x1", b"1", &mut block);
        // Invalid indexed representation: index 0.
        block.extend_from_slice(&[0x80]);

        let mut decoder = Decoder::new(4096);
        let result = decode_request_head(
            &mut decoder,
            block.freeze(),
            HeaderLimits::new(None, NonZeroUsize::new(1)),
            false,
        );

        assert!(matches!(
            result,
            Err(RequestHeadError::Connection {
                error_code: ErrorCode::COMPRESSION_ERROR,
                ..
            })
        ));
    }

    #[test]
    fn rejected_indexed_field_stays_in_dynamic_table_for_next_stream() {
        // Stream-shaped: first block inserts X-Bad via indexing and rejects;
        // second block reuses the dynamic entry with a valid request around it.
        let mut encoder = Encoder::new();
        let mut bad = BytesMut::new();
        encoder.begin_block(&mut bad);
        encoder.encode_field_bytes(b":method", b"GET", &mut bad);
        encoder.encode_field_bytes(b":scheme", b"http", &mut bad);
        encoder.encode_field_bytes(b":authority", b"example.com", &mut bad);
        encoder.encode_field_bytes(b":path", b"/bad", &mut bad);
        encoder.encode_field_bytes(b"X-Bad", b"1", &mut bad);

        let mut decoder = Decoder::new(4096);
        let first = decode_request_head(
            &mut decoder,
            bad.freeze(),
            HeaderLimits::new(None, None),
            false,
        );
        assert!(matches!(
            first,
            Err(RequestHeadError::Stream {
                error_code: ErrorCode::PROTOCOL_ERROR,
            })
        ));

        // Peer reuses the dynamic table entry for X-Bad as an indexed field.
        // Build a second block: static pseudos + indexed dynamic (0xBE if sole entry).
        // After 5 incremental inserts, X-Bad is newest → index 62 → 0xBE.
        // But method/scheme/etc may also be indexed. Encode via the same encoder.
        let mut good = BytesMut::new();
        encoder.begin_block(&mut good);
        encoder.encode_field_bytes(b":method", b"GET", &mut good);
        encoder.encode_field_bytes(b":scheme", b"http", &mut good);
        encoder.encode_field_bytes(b":authority", b"example.com", &mut good);
        encoder.encode_field_bytes(b":path", b"/good", &mut good);
        // Reuse X-Bad from the dynamic table (same name/value → indexed).
        encoder.encode_field_bytes(b"X-Bad", b"1", &mut good);

        // Decode with the connection decoder that kept the insertion, but the
        // accept path will reject X-Bad again — the point is table lookup must
        // succeed (no compression error).
        let second = decode_request_head(
            &mut decoder,
            good.freeze(),
            HeaderLimits::new(None, None),
            false,
        );
        assert!(
            matches!(
                second,
                Err(RequestHeadError::Stream {
                    error_code: ErrorCode::PROTOCOL_ERROR,
                })
            ),
            "dynamic reuse must not become COMPRESSION_ERROR, got {second:?}"
        );
    }

    #[test]
    fn decode_block_error_maps_compression_and_rejection() {
        let mut decoder = Decoder::new(4096);
        let mut empty = Bytes::new();
        decoder
            .decode_block(&mut empty, |_| Ok::<(), &str>(()))
            .expect("empty block is valid");

        let mut bad_index = Bytes::from_static(&[0x80]);
        let err = decoder
            .decode_block(&mut bad_index, |_| Ok::<(), &str>(()))
            .unwrap_err();
        assert!(matches!(
            err,
            DecodeBlockError::Compression(DecoderError::InvalidTableIndex)
        ));
    }
}
