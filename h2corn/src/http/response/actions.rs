use std::mem;

use smallvec::SmallVec;
use tokio::fs::File;

use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::http::header::{
    ResponseHeaderScan, apply_default_response_headers_with_scan,
    canonicalize_fixed_length_response_headers_with_scan, inspect_response_default_headers,
};
use crate::http::types::{HttpStatusCode, ResponseHeaders};

#[derive(Debug)]
pub enum FinalResponseBody {
    Empty,
    Bytes(PayloadBytes),
    File { file: File, len: usize },
    Suppressed { len: usize },
}

impl FinalResponseBody {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Bytes(body) => body.len(),
            Self::File { len, .. } | Self::Suppressed { len } => *len,
        }
    }
}

#[derive(Debug)]
pub struct ResponseStart {
    status: HttpStatusCode,
    headers: ResponseHeaders,
    scan: ResponseHeaderScan,
}

impl ResponseStart {
    pub(crate) fn new(status: HttpStatusCode, headers: ResponseHeaders) -> Self {
        let scan = inspect_response_default_headers(&headers);
        Self {
            status,
            headers,
            scan,
        }
    }

    pub(crate) const fn status(&self) -> HttpStatusCode {
        self.status
    }

    pub(crate) fn content_length_hint(&mut self) -> Option<usize> {
        self.scan.ensure_content_length_scanned(&self.headers);
        self.scan.content_length()
    }

    pub(crate) fn apply_default_headers(&mut self, config: &ServerConfig) {
        apply_default_response_headers_with_scan(&mut self.headers, &mut self.scan, config);
    }

    pub(crate) fn canonicalize_known_length(&mut self, len: usize) {
        self.scan.ensure_content_length_scanned(&self.headers);
        canonicalize_fixed_length_response_headers_with_scan(
            &mut self.headers,
            &mut self.scan,
            len,
        );
    }

    pub(crate) fn take_for_action(&mut self) -> Self {
        Self {
            status: self.status,
            headers: mem::take(&mut self.headers),
            scan: self.scan,
        }
    }

    pub(crate) fn into_status_headers(self) -> (HttpStatusCode, ResponseHeaders) {
        (self.status, self.headers)
    }
}

#[derive(Debug)]
pub enum ResponseAction {
    Final {
        start: ResponseStart,
        body: FinalResponseBody,
    },
    Start {
        start: ResponseStart,
    },
    Body(PayloadBytes),
    File {
        file: File,
        len: usize,
    },
    Finish,
    FinishWithTrailers(ResponseHeaders),
    InternalError,
    AbortIncomplete,
}

pub type ResponseActions = SmallVec<[ResponseAction; 2]>;

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::ResponseStart;
    use crate::http;

    #[test]
    fn response_start_keeps_content_length_hint() {
        let mut start = ResponseStart::new(
            200,
            vec![(
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"42").into(),
            )],
        );

        assert_eq!(start.content_length_hint(), Some(42));
    }

    #[test]
    fn response_start_canonicalizes_duplicate_content_length_once() {
        let mut start = ResponseStart::new(
            200,
            vec![
                (
                    Bytes::from_static(b"content-length").into(),
                    Bytes::from_static(b"1").into(),
                ),
                (
                    Bytes::from_static(b"content-length").into(),
                    Bytes::from_static(b"1").into(),
                ),
            ],
        );

        start.canonicalize_known_length(7);
        let (_, headers) = start.into_status_headers();

        assert_eq!(
            http::header::inspect_response_headers(&headers).content_length(),
            Some(7)
        );
        assert_eq!(
            headers
                .iter()
                .filter(|(name, _)| name.as_bytes() == b"content-length")
                .count(),
            1,
        );
    }

    #[test]
    fn response_start_adds_missing_content_length() {
        let mut start = ResponseStart::new(
            200,
            vec![(
                Bytes::from_static(b"content-type").into(),
                Bytes::from_static(b"text/plain").into(),
            )],
        );

        start.canonicalize_known_length(5);
        let (_, headers) = start.into_status_headers();

        assert_eq!(
            http::header::inspect_response_headers(&headers).content_length(),
            Some(5)
        );
    }
}
