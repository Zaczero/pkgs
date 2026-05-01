use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use http::Method;
use tokio::time::Instant;

use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::ext::Protocol;
use crate::frame::{self, ErrorCode, RawFrame, StreamId};
use crate::hpack::{BytesStr, Decoder, DecoderError, Header};
use crate::http::header::{header_is_single_token, parse_content_length_header};
use crate::http::header_meta::RequestHeaderMeta;
use crate::http::types::{
    HttpStatusCode, HttpVersion, KnownRequestHeaderName, RequestAuthority, RequestHead,
    RequestHeaderName, RequestHeaderValue, RequestHeaders, RequestTarget, status_code,
};

use super::PriorityDependency;

#[derive(Debug)]
pub(super) struct PendingHeaders {
    pub stream_id: StreamId,
    pub end_stream: bool,
    pub block: BytesMut,
    pub last_fragment_at: Instant,
}

pub(super) struct HeaderBlockFragment {
    pub block: Bytes,
    pub end_headers: bool,
    pub end_stream: bool,
    pub stream_dependency: Option<PriorityDependency>,
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

    pub const fn stream_protocol() -> Self {
        Self::Stream {
            error_code: ErrorCode::PROTOCOL_ERROR,
        }
    }

    pub const fn reject(status: HttpStatusCode) -> Self {
        Self::Reject { status }
    }
}

impl From<DecoderError> for RequestHeadError {
    fn from(err: DecoderError) -> Self {
        map_hpack_error(err)
    }
}

pub(super) fn parse_header_block_fragment(
    frame: RawFrame,
) -> Result<HeaderBlockFragment, H2CornError> {
    let header = frame.header;
    let payload = frame.payload;
    let payload_bytes = payload.as_ref();
    let mut payload_offset = 0;
    let padded = header.flags.contains(frame::FrameFlags::PADDED);
    let priority = header.flags.contains(frame::FrameFlags::PRIORITY);
    let priority_offset = usize::from(padded);

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

    let stream_dependency = priority.then(|| {
        PriorityDependency::from_wire(u32::from_be_bytes(
            payload_bytes[priority_offset..priority_offset + 4]
                .try_into()
                .expect("payload length is validated"),
        ))
    });

    Ok(HeaderBlockFragment {
        block: if payload_offset == 0 && block_end == payload.len() {
            payload
        } else {
            payload.slice(payload_offset..block_end)
        },
        end_headers: header.flags.contains(frame::FrameFlags::END_HEADERS),
        end_stream: header.flags.contains(frame::FrameFlags::END_STREAM),
        stream_dependency,
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

struct RequestHeadBuilder {
    method: Option<Method>,
    scheme: Option<BytesStr>,
    authority: Option<BytesStr>,
    path: Option<BytesStr>,
    protocol: Option<Protocol>,
    regular_headers: RequestHeaders,
    section: HeaderSection,
    host_header_index: Option<usize>,
    header_list_size: usize,
    max_header_list_size: Option<NonZeroUsize>,
    header_meta: RequestHeaderMeta,
    secure: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum HeaderSection {
    #[default]
    Pseudo,
    Regular,
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

impl RequestHeadBuilder {
    fn new(max_header_list_size: Option<NonZeroUsize>, secure: bool) -> Self {
        Self {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            protocol: None,
            regular_headers: RequestHeaders::with_capacity(16),
            section: HeaderSection::default(),
            host_header_index: None,
            header_list_size: 0,
            max_header_list_size,
            header_meta: RequestHeaderMeta::default(),
            secure,
        }
    }

    const fn account_header_bytes(&mut self, len: usize) -> Result<(), RequestHeadError> {
        if let Some(max_header_list_size) = self.max_header_list_size {
            self.header_list_size = self.header_list_size.saturating_add(len);
            if self.header_list_size > max_header_list_size.get() {
                return Err(RequestHeadError::reject(
                    status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                ));
            }
        }
        Ok(())
    }

    fn push(&mut self, header: Header) -> Result<(), RequestHeadError> {
        let header_len = header.len();
        match header {
            Header::Method(value) => {
                push_request_pseudo(&mut self.method, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Scheme(value) => {
                push_request_pseudo(&mut self.scheme, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Authority(value) => {
                push_request_pseudo(&mut self.authority, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Path(value) => {
                push_request_pseudo(&mut self.path, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Protocol(value) => {
                push_request_pseudo(&mut self.protocol, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Status(_) => {
                return Err(RequestHeadError::stream_protocol());
            }
            Header::Field { name, value } => {
                self.section = HeaderSection::Regular;
                self.push_regular_header(name, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
        }
        self.account_header_bytes(header_len)?;
        Ok(())
    }

    fn push_regular_header(&mut self, name: BytesStr, value: Bytes) -> Result<(), ()> {
        let value_bytes = value.as_ref();
        let known_name = KnownRequestHeaderName::from_bytes(name.as_ref());

        if value_bytes
            .first()
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            || value_bytes
                .last()
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            return Err(());
        }

        match known_name {
            Some(
                KnownRequestHeaderName::Connection
                | KnownRequestHeaderName::ProxyConnection
                | KnownRequestHeaderName::KeepAlive
                | KnownRequestHeaderName::TransferEncoding
                | KnownRequestHeaderName::Upgrade,
            ) => {
                return Err(());
            }
            Some(KnownRequestHeaderName::Te) => {
                if !header_is_single_token(value_bytes, b"trailers") {
                    return Err(());
                }
                self.header_meta.accepts_trailers = true;
            }
            Some(KnownRequestHeaderName::ContentLength) => {
                let parsed = parse_content_length_header(value_bytes).ok_or(())?;
                if self
                    .header_meta
                    .content_length
                    .is_some_and(|existing| existing != parsed)
                {
                    return Err(());
                }
                self.header_meta.content_length = Some(parsed);
            }
            _ => {}
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
        let target = match (method == Method::CONNECT, self.protocol.take()) {
            (false, None) => {
                let scheme = self.scheme.take().ok_or(())?;
                let path = self.path.take().ok_or(())?;
                let scheme = self.resolve_scheme(scheme);
                if matches!(scheme.as_str(), "http" | "https") && path.is_empty() {
                    return Err(());
                }
                RequestTarget::normal(scheme, path)
            }
            (false, Some(_)) => return Err(()),
            (true, None) => {
                if self.scheme.is_some() || self.path.is_some() {
                    return Err(());
                }
                RequestTarget::connect(RequestAuthority::new(self.authority.take().ok_or(())?))
            }
            (true, Some(protocol)) => {
                let scheme = self.scheme.take().ok_or(())?;
                let path = self.path.take().ok_or(())?;
                let scheme = self.resolve_scheme(scheme);
                if matches!(scheme.as_str(), "http" | "https") && path.is_empty() {
                    return Err(());
                }
                RequestTarget::extended_connect(
                    RequestAuthority::new(self.authority.take().ok_or(())?),
                    protocol,
                    scheme,
                    path,
                )
            }
        };

        if let (Some(authority), Some(host_header_index)) =
            (&self.authority, self.host_header_index)
            && !self.regular_headers[host_header_index]
                .1
                .as_bytes()
                .eq_ignore_ascii_case(authority.as_ref())
        {
            return Err(());
        }

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
            headers: self.regular_headers,
            header_meta: self.header_meta,
        })
    }

    fn resolve_scheme(&self, scheme: BytesStr) -> BytesStr {
        if self.secure {
            BytesStr::from_static("https")
        } else {
            scheme
        }
    }
}

pub(super) fn decode_request_head(
    decoder: &mut Decoder,
    block: Bytes,
    max_header_list_size: Option<NonZeroUsize>,
    secure: bool,
) -> Result<RequestHead, RequestHeadError> {
    let mut builder = RequestHeadBuilder::new(max_header_list_size, secure);
    let mut bytes = block;
    decoder.decode_block(&mut bytes, |header| builder.push(header))?;

    builder
        .finish()
        .map_err(|()| RequestHeadError::stream_protocol())
}

pub(super) fn decode_trailer_block(
    decoder: &mut Decoder,
    block: Bytes,
) -> Result<(), RequestHeadError> {
    let mut bytes = block;
    decoder.decode_block(&mut bytes, |header| {
        if !matches!(header, Header::Field { .. }) {
            return Err(RequestHeadError::stream_protocol());
        }
        Ok(())
    })?;

    Ok(())
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

    use crate::hpack::{BytesStr, Decoder, Encoder, Header};
    use crate::http::types::status_code;

    use super::{RequestHeadError, decode_request_head};

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
        let result = decode_request_head(&mut decoder, block, NonZeroUsize::new(90), false);

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
        let exact_received_size = Header::Method(Method::GET).len()
            + Header::Scheme(BytesStr::from_static("http")).len()
            + Header::Authority(BytesStr::from_static("example.com")).len()
            + Header::Path(BytesStr::from_static("/")).len();

        let mut decoder = Decoder::new(4096);
        let request = decode_request_head(
            &mut decoder,
            block,
            NonZeroUsize::new(exact_received_size),
            false,
        )
        .expect("received header list size should ignore synthesized host");

        assert_eq!(request.headers.len(), 1);
        assert_eq!(request.headers[0].0.as_str(), "host");
        assert_eq!(request.headers[0].1.as_bytes(), b"example.com");
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
        let request = decode_request_head(&mut decoder, block, None, true)
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
        let request = decode_request_head(&mut decoder, block, None, false)
            .expect("valid HTTP/2 request should decode");

        assert_eq!(request.scheme_str(), "https");
    }
}
