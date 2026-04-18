use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use http::Method;

use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::ext::Protocol;
use crate::frame::{self, ErrorCode, RawFrame, StreamId};
use crate::hpack::{BytesStr, Decoder, DecoderError, Header};
use crate::http::analysis::{
    HostHeaderAnalysis, HostHeaderSource, ProxyHeaderSlots, RequestBodyFraming,
    RequestHeaderAnalysis, WebSocketRequestAnalysis,
};
use crate::http::header::{header_is_single_token, parse_content_length_header};
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
}

pub(super) struct HeaderBlockFragment {
    pub block: Bytes,
    pub end_headers: bool,
    pub end_stream: bool,
    pub stream_dependency: Option<PriorityDependency>,
}

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
    fn compression(error: H2Error) -> Self {
        Self::Connection {
            error_code: ErrorCode::COMPRESSION_ERROR,
            error,
        }
    }

    pub fn stream_protocol() -> Self {
        Self::Stream {
            error_code: ErrorCode::PROTOCOL_ERROR,
        }
    }

    pub fn reject(status: HttpStatusCode) -> Self {
        Self::Reject { status }
    }
}

impl From<DecoderError> for RequestHeadError {
    fn from(err: DecoderError) -> Self {
        map_hpack_error(err)
    }
}

pub(super) fn parse_header_block_fragment(
    frame: &RawFrame,
) -> Result<HeaderBlockFragment, H2CornError> {
    let payload = frame.payload.as_ref();
    let mut payload_offset = 0;
    let padded = frame.header.flags.contains(frame::FrameFlags::PADDED);
    let priority = frame.header.flags.contains(frame::FrameFlags::PRIORITY);
    let priority_offset = usize::from(padded);

    if padded {
        if payload.is_empty() {
            return H2Error::HeadersPaddedMissingPadLength.err();
        }
        payload_offset += 1;
    }

    if priority {
        if payload.len() < payload_offset + 5 {
            return H2Error::HeadersPriorityTooShort.err();
        }
        payload_offset += 5;
    }

    let pad_len = if padded { usize::from(payload[0]) } else { 0 };
    if payload.len() < payload_offset + pad_len {
        return H2Error::HeadersPaddingExceedsPayload.err();
    }
    let block_end = payload.len() - pad_len;

    let stream_dependency = if priority {
        Some(PriorityDependency::from_wire(u32::from_be_bytes(
            payload[priority_offset..priority_offset + 4]
                .try_into()
                .expect("payload length is validated"),
        )))
    } else {
        None
    };

    Ok(HeaderBlockFragment {
        block: frame.payload.slice(payload_offset..block_end),
        end_headers: frame.header.flags.contains(frame::FrameFlags::END_HEADERS),
        end_stream: frame.header.flags.contains(frame::FrameFlags::END_STREAM),
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

struct RequestHeadBuilder<const ENFORCE_HEADER_LIST_LIMIT: bool> {
    method: Option<Method>,
    scheme: Option<BytesStr>,
    authority: Option<BytesStr>,
    path: Option<BytesStr>,
    protocol: Option<Protocol>,
    accepts_trailers: bool,
    content_length: Option<u64>,
    regular_headers: RequestHeaders,
    section: HeaderSection,
    host_header_index: Option<usize>,
    header_list_size: usize,
    max_header_list_size: usize,
    proxy_headers: ProxyHeaderSlots,
    websocket: WebSocketRequestAnalysis,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
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

impl<const ENFORCE_HEADER_LIST_LIMIT: bool> RequestHeadBuilder<ENFORCE_HEADER_LIST_LIMIT> {
    fn new(max_header_list_size: usize) -> Self {
        Self {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            protocol: None,
            accepts_trailers: false,
            content_length: None,
            regular_headers: RequestHeaders::with_capacity(16),
            section: HeaderSection::default(),
            host_header_index: None,
            header_list_size: 0,
            max_header_list_size,
            proxy_headers: ProxyHeaderSlots::default(),
            websocket: WebSocketRequestAnalysis::default(),
        }
    }

    fn account_header_bytes(&mut self, len: usize) -> Result<(), RequestHeadError> {
        if ENFORCE_HEADER_LIST_LIMIT {
            self.header_list_size = self.header_list_size.saturating_add(len);
            if self.header_list_size > self.max_header_list_size {
                return Err(RequestHeadError::reject(
                    status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                ));
            }
        }
        Ok(())
    }

    fn push(&mut self, header: Header) -> Result<(), RequestHeadError> {
        match header {
            Header::Method(value) => {
                self.account_header_bytes(7 + value.as_str().len())?;
                push_request_pseudo(&mut self.method, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Scheme(value) => {
                self.account_header_bytes(7 + value.len())?;
                push_request_pseudo(&mut self.scheme, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Authority(value) => {
                self.account_header_bytes(10 + value.len())?;
                push_request_pseudo(&mut self.authority, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Path(value) => {
                self.account_header_bytes(5 + value.len())?;
                push_request_pseudo(&mut self.path, self.section, value)
                    .map_err(|()| RequestHeadError::stream_protocol())?;
            }
            Header::Protocol(value) => {
                self.account_header_bytes(9 + value.as_str().len())?;
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
                self.accepts_trailers = true;
            }
            Some(KnownRequestHeaderName::ContentLength) => {
                let parsed = parse_content_length_header(value_bytes).ok_or(())?;
                if self
                    .content_length
                    .is_some_and(|existing| existing != parsed)
                {
                    return Err(());
                }
                self.content_length = Some(parsed);
            }
            _ => {}
        }

        self.account_header_bytes(name.as_str().len() + value_bytes.len())
            .map_err(|_| ())?;
        if let Some(known_name) = known_name {
            self.websocket.observe(known_name, &value);
            self.proxy_headers
                .observe(known_name, self.regular_headers.len());
        }
        if known_name == Some(KnownRequestHeaderName::Host) {
            if self.host_header_index.is_some_and(|index| {
                !self.regular_headers[index]
                    .1
                    .as_bytes()
                    .eq_ignore_ascii_case(value_bytes)
            }) {
                return Err(());
            }
            if self.host_header_index.is_none() {
                self.host_header_index = Some(self.regular_headers.len());
            }
        }
        let name = RequestHeaderName::from_h2_validated(name);
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
            let synthesized_host = host.is_some();
            if let Some(host) = host {
                if self.regular_headers.is_empty() {
                    self.regular_headers.reserve(1);
                }
                self.regular_headers
                    .push((KnownRequestHeaderName::Host.into(), host));
                self.host_header_index = Some(index);
            }

            let host_header = HostHeaderAnalysis {
                index: self.host_header_index,
                source: if synthesized_host {
                    HostHeaderSource::Synthesized
                } else if self.host_header_index.is_some() {
                    HostHeaderSource::Header
                } else {
                    HostHeaderSource::None
                },
            };

            return Ok(RequestHead {
                http_version: HttpVersion::Http2,
                method,
                target,
                headers: self.regular_headers,
                header_analysis: RequestHeaderAnalysis {
                    body_framing: if self.content_length.is_some() {
                        RequestBodyFraming::ContentLength
                    } else {
                        RequestBodyFraming::None
                    },
                    accepts_trailers: self.accepts_trailers,
                    content_length: self.content_length,
                    connection_close: false,
                    websocket: self.websocket,
                    proxy_headers: self.proxy_headers,
                    host_header,
                },
            });
        }

        let host_header = HostHeaderAnalysis {
            index: self.host_header_index,
            source: if self.host_header_index.is_some() {
                HostHeaderSource::Header
            } else {
                HostHeaderSource::None
            },
        };

        Ok(RequestHead {
            http_version: HttpVersion::Http2,
            method,
            target,
            headers: self.regular_headers,
            header_analysis: RequestHeaderAnalysis {
                body_framing: if self.content_length.is_some() {
                    RequestBodyFraming::ContentLength
                } else {
                    RequestBodyFraming::None
                },
                accepts_trailers: self.accepts_trailers,
                content_length: self.content_length,
                connection_close: false,
                websocket: self.websocket,
                proxy_headers: self.proxy_headers,
                host_header,
            },
        })
    }
}

pub(super) fn decode_request_head(
    decoder: &mut Decoder,
    block: Bytes,
    max_header_list_size: Option<NonZeroUsize>,
) -> Result<RequestHead, RequestHeadError> {
    if let Some(max_header_list_size) = max_header_list_size {
        decode_request_head_with_mode::<true>(decoder, block, max_header_list_size.get())
    } else {
        decode_request_head_with_mode::<false>(decoder, block, 0)
    }
}

fn decode_request_head_with_mode<const ENFORCE_HEADER_LIST_LIMIT: bool>(
    decoder: &mut Decoder,
    block: Bytes,
    max_header_list_size: usize,
) -> Result<RequestHead, RequestHeadError> {
    let mut builder = RequestHeadBuilder::<ENFORCE_HEADER_LIST_LIMIT>::new(max_header_list_size);
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
