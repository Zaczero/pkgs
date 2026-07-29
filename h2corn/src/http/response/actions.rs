use std::fs::File;
use std::mem;

use smallvec::SmallVec;

use crate::bridge::PayloadBytes;
use crate::config::{ResponseHeaderConfig, ServerConfig};
use crate::http::header::{
    ApplicationResponseField, ResponseConnectionDirective, ResponseHeaderControl,
    ResponseHeaderScan, apply_default_response_headers_with_scan, inspect_response_headers,
    prepare_fixed_length_response_headers_with_scan,
    prepare_response_headers_without_content_length, prepare_streaming_response_headers_with_scan,
};
use crate::http::types::{FinalResponseStatus, HttpStatusCode, ResponseHeaders, ResponseTrailers};

#[derive(Debug)]
pub(crate) enum FinalResponseBody {
    Empty,
    Bytes(PayloadBytes),
    // File bodies are rare relative to byte/empty bodies; retain the box so
    // the handle never sets the common response-action enum layout.
    File { file: Box<File>, len: usize },
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
pub(crate) struct ResponseStart {
    status: FinalResponseStatus,
    headers: ResponseHeaders,
    scan: ResponseHeaderScan,
    control: ResponseHeaderControl,
}

impl ResponseStart {
    #[cfg(test)]
    pub(crate) fn new(status: HttpStatusCode, headers: ResponseHeaders) -> Self {
        Self::from_final(
            FinalResponseStatus::new(status)
                .expect("response actions cannot be constructed from informational statuses"),
            headers,
            ResponseHeaderControl::default(),
        )
    }

    pub(crate) fn from_final(
        status: FinalResponseStatus,
        headers: ResponseHeaders,
        control: ResponseHeaderControl,
    ) -> Self {
        let scan = inspect_response_headers(&headers);
        Self {
            status,
            headers,
            scan,
            control,
        }
    }

    pub(crate) const fn status(&self) -> HttpStatusCode {
        self.status.get()
    }

    pub(crate) const fn control(&self) -> ResponseHeaderControl {
        self.control
    }

    pub(crate) const fn declared_content_length(&self) -> Option<usize> {
        self.scan.content_length()
    }

    pub(crate) fn apply_default_headers(&mut self, config: &ServerConfig) {
        if self.status().forbids_content_length() {
            prepare_response_headers_without_content_length(
                &mut self.headers,
                &mut self.scan,
                &config.response_headers,
            );
        } else {
            apply_default_response_headers_with_scan(&mut self.headers, &mut self.scan, config);
        }
    }

    pub(crate) fn prepare_known_length(&mut self, config: &ResponseHeaderConfig, len: usize) {
        if self.status().forbids_content_length() {
            prepare_response_headers_without_content_length(
                &mut self.headers,
                &mut self.scan,
                config,
            );
            return;
        }
        prepare_fixed_length_response_headers_with_scan(
            &mut self.headers,
            &mut self.scan,
            config,
            len,
        );
    }

    pub(crate) fn prepare_streaming(&mut self, config: &ResponseHeaderConfig) -> Option<usize> {
        if self.status().forbids_content_length() {
            prepare_response_headers_without_content_length(
                &mut self.headers,
                &mut self.scan,
                config,
            );
            None
        } else {
            prepare_streaming_response_headers_with_scan(&mut self.headers, &mut self.scan, config)
        }
    }

    pub(crate) fn strip_http2_only_fields(&mut self) {
        if self.control.directive == ResponseConnectionDirective::Upgrade {
            self.control
                .strips
                .insert(ApplicationResponseField::Upgrade);
            self.control.directive = ResponseConnectionDirective::None;
        }
        self.headers
            .retain(|(name, _)| name.as_bytes() != b"upgrade");
    }

    pub(crate) fn take_for_action(&mut self) -> Self {
        Self {
            status: self.status,
            headers: mem::take(&mut self.headers),
            scan: self.scan,
            control: self.control,
        }
    }

    pub(crate) fn into_status_headers(self) -> (HttpStatusCode, ResponseHeaders) {
        (self.status(), self.headers)
    }
}

#[derive(Debug)]
pub(crate) enum ResponseAction {
    Final {
        start: ResponseStart,
        body: FinalResponseBody,
    },
    Start {
        start: ResponseStart,
    },
    Body(PayloadBytes),
    File {
        file: Box<File>,
        len: usize,
    },
    Finish,
    FinishWithTrailers(ResponseTrailers),
    InternalError,
    AbortIncomplete,
}

pub(crate) type ResponseActions = SmallVec<[ResponseAction; 2]>;

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::ResponseStart;
    use crate::config::ResponseHeaderConfig;
    use crate::http;

    #[test]
    fn response_start_canonicalizes_duplicate_content_length_once() {
        let mut start = ResponseStart::new(http::types::status_code::OK, vec![
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"1").into(),
            ),
            (
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"1").into(),
            ),
        ]);

        start.prepare_known_length(&ResponseHeaderConfig::default(), 7);
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
        let mut start = ResponseStart::new(http::types::status_code::OK, vec![(
            Bytes::from_static(b"content-type").into(),
            Bytes::from_static(b"text/plain").into(),
        )]);

        start.prepare_known_length(&ResponseHeaderConfig::default(), 5);
        let (_, headers) = start.into_status_headers();

        assert_eq!(
            http::header::inspect_response_headers(&headers).content_length(),
            Some(5)
        );
    }

    #[test]
    fn response_start_removes_content_length_when_the_status_forbids_it() {
        let mut start = ResponseStart::new(http::types::status_code::NO_CONTENT, vec![(
            Bytes::from_static(b"content-length").into(),
            Bytes::from_static(b"7").into(),
        )]);

        start.prepare_known_length(&ResponseHeaderConfig::default(), 0);
        let (_, headers) = start.into_status_headers();

        assert!(
            headers
                .iter()
                .all(|(name, _)| name.kind() != http::types::ResponseHeaderKind::ContentLength)
        );
    }

    #[test]
    fn response_header_preparation_keeps_headers_and_scan_in_lockstep() {
        type ContentLengthCase = (&'static str, &'static [&'static [u8]], Option<usize>);

        fn headers(values: &[&[u8]]) -> http::types::ResponseHeaders {
            values
                .iter()
                .map(|value| {
                    (
                        Bytes::from_static(b"content-length").into(),
                        Bytes::copy_from_slice(value).into(),
                    )
                })
                .collect()
        }

        let cases: [ContentLengthCase; 4] = [
            ("missing", &[], None),
            ("valid", &[b"5"], Some(5)),
            ("invalid", &[b"bad"], None),
            ("conflicting", &[b"5", b"6"], None),
        ];
        for (name, values, declared) in cases {
            let mut known = ResponseStart::new(http::types::status_code::OK, headers(values));
            known.prepare_known_length(&ResponseHeaderConfig::default(), 7);
            assert_eq!(known.declared_content_length(), Some(7), "{name}");
            let (_, known_headers) = known.into_status_headers();
            assert_eq!(
                http::header::inspect_response_headers(&known_headers).content_length(),
                Some(7),
                "{name}",
            );

            let mut streaming = ResponseStart::new(http::types::status_code::OK, headers(values));
            assert_eq!(
                streaming.prepare_streaming(&ResponseHeaderConfig::default()),
                declared,
                "{name}",
            );
            assert_eq!(streaming.declared_content_length(), declared, "{name}");
            let (_, streaming_headers) = streaming.into_status_headers();
            assert_eq!(
                http::header::inspect_response_headers(&streaming_headers).content_length(),
                declared,
                "{name}",
            );
        }
    }

    #[test]
    fn no_content_clears_content_length_but_not_modified_preserves_representation_metadata() {
        for streaming in [false, true] {
            let headers = vec![(
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"7").into(),
            )];
            let mut no_content = ResponseStart::new(http::types::status_code::NO_CONTENT, headers);
            if streaming {
                assert_eq!(
                    no_content.prepare_streaming(&ResponseHeaderConfig::default()),
                    None
                );
            } else {
                no_content.prepare_known_length(&ResponseHeaderConfig::default(), 7);
            }
            assert_eq!(no_content.declared_content_length(), None);
            let (_, headers) = no_content.into_status_headers();
            assert!(headers.iter().all(|(name, _)| {
                name.kind() != http::types::ResponseHeaderKind::ContentLength
            }));

            let headers = vec![(
                Bytes::from_static(b"content-length").into(),
                Bytes::from_static(b"7").into(),
            )];
            let mut not_modified =
                ResponseStart::new(http::types::status_code::NOT_MODIFIED, headers);
            if streaming {
                assert_eq!(
                    not_modified.prepare_streaming(&ResponseHeaderConfig::default()),
                    Some(7)
                );
            } else {
                not_modified.prepare_known_length(&ResponseHeaderConfig::default(), 7);
            }
            assert_eq!(not_modified.declared_content_length(), Some(7));
        }
    }

    #[test]
    fn informational_status_cannot_construct_a_response_action_start() {
        let informational = http::types::HttpStatusCode::new(103).expect("valid HTTP status");
        assert!(http::types::FinalResponseStatus::new(informational).is_none());
    }
}
