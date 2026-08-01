use std::mem;

use pyo3::exceptions::PyValueError;

use super::actions;
#[cfg(test)]
use crate::bridge;
use crate::error::{self, ErrorExt as _};
use crate::http;

#[derive(Debug)]
enum ResponseState {
    WaitingForStart,
    Started(StartedResponse),
    WaitingForTrailers {
        buffered: http::types::ResponseTrailers,
    },
    /// The response is already finished on the wire, but the application
    /// declared trailers and is entitled to send them. A response with no
    /// content has no trailer section to put them in, so they are consumed
    /// and discarded — the alternative, closing the stream first, makes a
    /// well-behaved application's own `send()` fail.
    DiscardingTrailers,
    Complete,
    Aborted,
}

impl ResponseState {
    const fn waiting_for_trailers() -> Self {
        Self::WaitingForTrailers {
            buffered: http::types::ResponseTrailers::new(),
        }
    }
}

#[derive(Debug)]
struct StartedResponse {
    start: actions::ResponseStart,
    body: StartedBody,
    declared_length: Option<DeclaredLength>,
}

#[derive(Clone, Copy, Debug)]
struct DeclaredLength {
    expected: usize,
    sent: usize,
}

#[derive(Clone, Copy, Debug)]
enum StartedBody {
    Unsent { expects_trailers: bool },
    Streaming { expects_trailers: bool },
    SuppressingHead { expects_trailers: bool, len: usize },
}

impl StartedResponse {
    fn take_start_action(&mut self) -> actions::ResponseAction {
        actions::ResponseAction::Start {
            start: self.start.take_for_action(),
            expects_trailers: self.expects_trailers(),
        }
    }

    fn take_final_action(&mut self, body: actions::FinalResponseBody) -> actions::ResponseAction {
        actions::ResponseAction::Final {
            start: self.start.take_for_action(),
            body,
        }
    }

    fn into_final_action(self, body: actions::FinalResponseBody) -> actions::ResponseAction {
        actions::ResponseAction::Final {
            start: self.start,
            body,
        }
    }

    const fn expects_trailers(&self) -> bool {
        match self.body {
            StartedBody::Unsent { expects_trailers }
            | StartedBody::Streaming { expects_trailers }
            | StartedBody::SuppressingHead {
                expects_trailers, ..
            } => expects_trailers,
        }
    }

    const fn body_started(&self) -> bool {
        !matches!(self.body, StartedBody::Unsent { .. })
    }

    fn record_streaming_body(
        &mut self,
        len: usize,
        final_chunk: bool,
    ) -> Result<(), error::H2CornError> {
        let Some(declared) = &mut self.declared_length else {
            return Ok(());
        };
        let Some(sent) = declared.sent.checked_add(len) else {
            return Err(error::H2CornError::from(PyValueError::new_err(
                "streamed response body exceeds its declared content-length",
            )));
        };
        if sent > declared.expected || (final_chunk && sent != declared.expected) {
            return Err(error::H2CornError::from(PyValueError::new_err(
                "streamed response body does not match its declared content-length",
            )));
        }
        declared.sent = sent;
        Ok(())
    }
}

/// Whether a response may carry a body, and how its length is framed.
///
/// Decided from the request method and the response status together, because
/// each forbids content for its own reason: a `HEAD` response describes a body
/// it does not send, while 204 and 205 have no content to describe at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponsePayload {
    /// Written as the application sends it.
    Send,
    /// Suppressed, but `Content-Length` still reports what the body would have
    /// been — a `HEAD` response, or 304, which names a representation it is
    /// deliberately not sending.
    Described,
    /// Suppressed with nothing to describe: 204 and 205 cannot carry a body.
    ///
    /// They differ in how that reaches the wire. 204 omits `Content-Length`
    /// entirely (RFC 9110 §8.6). 205 is framed as zero-length — it is
    /// deliberately *not* in `forbids_content_length`, so it takes the normal
    /// `Suppressed { len: 0 }` path and emits `content-length: 0`.
    Absent,
}

impl ResponsePayload {
    const fn resolve(head_only: bool, status: http::types::HttpStatusCode) -> Self {
        match status.get() {
            204 | 205 => Self::Absent,
            304 => Self::Described,
            _ if head_only => Self::Described,
            _ => Self::Send,
        }
    }

    const fn suppresses_body(self) -> bool {
        !matches!(self, Self::Send)
    }

    /// What `Content-Length` should report for a suppressed body.
    ///
    /// A `HEAD` or 304 response names the representation a `GET` or a 200
    /// would have carried, so an application that declared that length keeps
    /// it: the transmitted length is necessarily zero and is not the value
    /// being described. Without a declaration the suppressed body's own length
    /// is the best description available.
    const fn described_len(self, declared: Option<usize>, len: usize) -> usize {
        match self {
            Self::Described => match declared {
                Some(declared) => declared,
                None => len,
            },
            Self::Send | Self::Absent => 0,
        }
    }
}

pub(crate) struct ResponseController {
    head_only: bool,
    payload: ResponsePayload,
    supports_response_trailers: bool,
    state: ResponseState,
}

impl ResponseController {
    pub(crate) const fn new(head_only: bool, supports_response_trailers: bool) -> Self {
        Self {
            head_only,
            payload: ResponsePayload::Send,
            supports_response_trailers,
            state: ResponseState::WaitingForStart,
        }
    }

    pub(crate) const fn is_complete(&self) -> bool {
        matches!(self.state, ResponseState::Complete)
    }

    pub(crate) fn handle_start(
        &mut self,
        status: http::types::FinalResponseStatus,
        headers: http::types::ResponseHeaders,
        trailers: bool,
        control: http::header::ResponseConnectionDirective,
    ) -> Result<(), error::H2CornError> {
        if !matches!(self.state, ResponseState::WaitingForStart) {
            return error::HttpResponseError::StartAlreadyReceived.err();
        }
        if trailers && !self.supports_response_trailers {
            return error::HttpResponseError::TrailersNotAdvertised.err();
        }
        self.payload = ResponsePayload::resolve(self.head_only, status.get());
        let start = actions::ResponseStart::from_final(status, headers, control);
        let declared_length = matches!(self.payload, ResponsePayload::Send)
            .then(|| start.declared_content_length())
            .flatten()
            .map(|expected| DeclaredLength { expected, sent: 0 });
        self.state = ResponseState::Started(StartedResponse {
            start,
            body: StartedBody::Unsent {
                expects_trailers: trailers,
            },
            declared_length,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn handle_body(
        &mut self,
        actions: &mut actions::ResponseActions,
        body: bridge::PayloadBytes,
        more_body: bool,
    ) -> Result<(), error::H2CornError> {
        self.handle_body_with_credit(actions, actions::ResponseBody::new(body), more_body)
    }

    pub(crate) fn handle_body_with_credit(
        &mut self,
        actions: &mut actions::ResponseActions,
        body: actions::ResponseBody,
        more_body: bool,
    ) -> Result<(), error::H2CornError> {
        let mut started = self.take_started(error::HttpResponseError::BodyBeforeStart)?;
        let final_chunk = !more_body;

        if self.payload.suppresses_body() {
            let (expects_trailers, previous_len) = match started.body {
                StartedBody::Unsent { expects_trailers } => (expects_trailers, 0),
                StartedBody::SuppressingHead {
                    expects_trailers,
                    len,
                } => (expects_trailers, len),
                StartedBody::Streaming { .. } => {
                    unreachable!("a suppressed response never streams body bytes")
                },
            };
            let len = previous_len.saturating_add(body.len());
            self.state = if final_chunk {
                // The response goes out complete here rather than opening a
                // chunked body to carry trailers: HTTP/1 would write a
                // terminator and trailer lines into a response the client
                // reads as bodyless, and the next response on the connection
                // would begin mid-frame. Declared trailers are still awaited
                // — see `DiscardingTrailers` — so the application's own
                // `send()` does not fail against a closed stream.
                let expects = expects_trailers;
                let described = started.start.declared_content_length();
                let state = complete_response(
                    actions,
                    &mut started,
                    actions::FinalResponseBody::Suppressed {
                        len: self.payload.described_len(described, len),
                    },
                );
                if expects {
                    ResponseState::DiscardingTrailers
                } else {
                    state
                }
            } else {
                started.body = StartedBody::SuppressingHead {
                    expects_trailers,
                    len,
                };
                ResponseState::Started(started)
            };
            return Ok(());
        }

        if final_chunk && !started.expects_trailers() && !started.body_started() {
            let body = if body.is_empty() {
                actions::FinalResponseBody::Empty
            } else {
                actions::FinalResponseBody::Bytes(body)
            };
            self.state = complete_response(actions, &mut started, body);
            return Ok(());
        }

        if let Err(err) = started.record_streaming_body(body.len(), final_chunk) {
            actions.push(actions::ResponseAction::AbortIncomplete);
            self.state = ResponseState::Aborted;
            return Err(err);
        }

        if !started.body_started() {
            actions.push(started.take_start_action());
        }
        let expects_trailers = started.expects_trailers();
        started.body = StartedBody::Streaming { expects_trailers };
        if !body.is_empty() {
            actions.push(actions::ResponseAction::Body(body));
        }

        self.state = if final_chunk {
            if expects_trailers {
                ResponseState::waiting_for_trailers()
            } else {
                actions.push(actions::ResponseAction::Finish);
                ResponseState::Complete
            }
        } else {
            ResponseState::Started(started)
        };
        Ok(())
    }

    pub(crate) fn handle_pathsend_substitute(
        &mut self,
        actions: &mut actions::ResponseActions,
        status: http::types::HttpStatusCode,
    ) -> Result<(), error::H2CornError> {
        let started = self.take_started(error::HttpResponseError::PathsendBeforeStart)?;
        if started.body_started() {
            self.state = ResponseState::Started(started);
            return error::HttpResponseError::PathsendMixedWithBody.err();
        }
        let start = actions::ResponseStart::from_final(
            http::types::FinalResponseStatus::new(status)
                .expect("pathsend substitutions are final response statuses"),
            http::types::ResponseHeaders::new(),
            http::header::ResponseConnectionDirective::default(),
        );
        self.state = if started.expects_trailers() {
            actions.push(actions::ResponseAction::Start {
                start,
                expects_trailers: true,
            });
            ResponseState::waiting_for_trailers()
        } else {
            actions.push(actions::ResponseAction::Final {
                start,
                body: actions::FinalResponseBody::Empty,
            });
            ResponseState::Complete
        };
        Ok(())
    }

    pub(crate) fn handle_pathsend(
        &mut self,
        actions: &mut actions::ResponseActions,
        source: http::pathsend::PathSource,
        len: usize,
    ) -> Result<(), error::H2CornError> {
        let mut started = self.take_started(error::HttpResponseError::PathsendBeforeStart)?;
        if started.body_started() {
            self.state = ResponseState::Started(started);
            return error::HttpResponseError::PathsendMixedWithBody.err();
        }

        if self.payload.suppresses_body() {
            let expects_trailers = started.expects_trailers();
            let described = started.start.declared_content_length();
            let state = complete_response(
                actions,
                &mut started,
                actions::FinalResponseBody::Suppressed {
                    len: self.payload.described_len(described, len),
                },
            );
            self.state = if expects_trailers {
                ResponseState::DiscardingTrailers
            } else {
                state
            };
            return Ok(());
        }

        match source {
            // Preloaded files travel the ordinary body path (vectored
            // chunk emission downstream); the file is already closed.
            http::pathsend::PathSource::Buffered(data) => {
                if started.expects_trailers() {
                    if let Err(err) = started.record_streaming_body(len, false) {
                        actions.push(actions::ResponseAction::AbortIncomplete);
                        self.state = ResponseState::Aborted;
                        return Err(err);
                    }
                    actions.push(started.take_start_action());
                    if !data.is_empty() {
                        actions.push(actions::ResponseAction::Body(actions::ResponseBody::new(
                            data.into(),
                        )));
                    }
                    self.state = ResponseState::waiting_for_trailers();
                } else {
                    let body = if data.is_empty() {
                        actions::FinalResponseBody::Empty
                    } else {
                        actions::FinalResponseBody::Bytes(actions::ResponseBody::new(data.into()))
                    };
                    self.state = complete_response(actions, &mut started, body);
                }
            },
            http::pathsend::PathSource::File(file) => {
                if started.expects_trailers() {
                    if let Err(err) = started.record_streaming_body(len, false) {
                        actions.push(actions::ResponseAction::AbortIncomplete);
                        self.state = ResponseState::Aborted;
                        return Err(err);
                    }
                    actions.push(started.take_start_action());
                    actions.push(actions::ResponseAction::File { file, len });
                    self.state = ResponseState::waiting_for_trailers();
                } else {
                    self.state = complete_response(
                        actions,
                        &mut started,
                        actions::FinalResponseBody::File { file, len },
                    );
                }
            },
        }
        Ok(())
    }

    pub(crate) fn handle_trailers(
        &mut self,
        actions: &mut actions::ResponseActions,
        headers: http::types::ResponseTrailers,
        more_trailers: bool,
    ) -> Result<(), error::H2CornError> {
        if matches!(self.state, ResponseState::DiscardingTrailers) {
            // Consumed, never written: the response they belong to carries no
            // content, so there is no trailer section on the wire for them.
            let _ = headers;
            if !more_trailers {
                self.state = ResponseState::Complete;
            }
            return Ok(());
        }
        let ResponseState::WaitingForTrailers { buffered } = &mut self.state else {
            return error::HttpResponseError::TrailersBeforeBodyCompleted.err();
        };
        buffered.append(headers);
        if !more_trailers {
            actions.push(actions::ResponseAction::FinishWithTrailers(mem::take(
                buffered,
            )));
            self.state = ResponseState::Complete;
        }
        Ok(())
    }

    pub(crate) fn finalize(
        &mut self,
        actions: &mut actions::ResponseActions,
        app_result: Result<(), error::H2CornError>,
    ) -> Result<(), error::H2CornError> {
        let state = mem::replace(&mut self.state, ResponseState::Complete);
        match (state, app_result) {
            (ResponseState::WaitingForStart, result) => {
                actions.push(actions::ResponseAction::InternalError);
                match result {
                    Err(err) => Err(err),
                    Ok(()) => error::HttpResponseError::AppReturnedWithoutStartingResponse.err(),
                }
            },
            (ResponseState::Started(started), Ok(()))
                if matches!(started.body, StartedBody::Unsent {
                    expects_trailers: false
                }) =>
            {
                // An application that sends only `http.response.start` and
                // returns still described a representation if its status can
                // carry one: a HEAD or 304 declaring `Content-Length: 7` keeps
                // it here, exactly as it would had a body event followed.
                let action = if self.payload.suppresses_body() {
                    let described = started.start.declared_content_length();
                    actions::FinalResponseBody::Suppressed {
                        len: self.payload.described_len(described, 0),
                    }
                } else {
                    actions::FinalResponseBody::Empty
                };
                actions.push(started.into_final_action(action));
                Ok(())
            },
            (ResponseState::Started(started), Ok(()))
                if matches!(started.body, StartedBody::SuppressingHead {
                    expects_trailers: false,
                    ..
                }) =>
            {
                let StartedBody::SuppressingHead { len, .. } = started.body else {
                    unreachable!()
                };
                let described = started.start.declared_content_length();
                actions.push(
                    started.into_final_action(actions::FinalResponseBody::Suppressed {
                        len: self.payload.described_len(described, len),
                    }),
                );
                Ok(())
            },
            (ResponseState::Started(_) | ResponseState::WaitingForTrailers { .. }, Ok(())) => {
                actions.push(actions::ResponseAction::AbortIncomplete);
                error::HttpResponseError::AppReturnedWithoutCompletingResponse.err()
            },
            (ResponseState::Complete, Err(err)) => Err(err),
            (_, Err(err)) => {
                actions.push(actions::ResponseAction::AbortIncomplete);
                Err(err)
            },
            _ => Ok(()),
        }
    }

    fn take_started(
        &mut self,
        err: error::HttpResponseError,
    ) -> Result<StartedResponse, error::H2CornError> {
        match mem::replace(&mut self.state, ResponseState::Aborted) {
            ResponseState::Started(started) => Ok(started),
            state => {
                self.state = state;
                err.err()
            },
        }
    }
}

fn complete_response(
    actions: &mut actions::ResponseActions,
    started: &mut StartedResponse,
    body: actions::FinalResponseBody,
) -> ResponseState {
    actions.push(started.take_final_action(body));
    ResponseState::Complete
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{File, remove_file, write};
    use std::io::{Error, ErrorKind};
    use std::path::{Path, PathBuf};
    use std::process::id;

    use bytes::Bytes;

    use super::ResponseController;
    use crate::{bridge, error, http};

    fn final_status(status: http::types::HttpStatusCode) -> http::types::FinalResponseStatus {
        http::types::FinalResponseStatus::new(status).expect("test status is final")
    }

    fn event_requests_pathsend(
        controller: &mut ResponseController,
        actions: &mut http::response::ResponseActions,
        event: bridge::HttpOutboundEvent,
    ) -> Result<bool, error::H2CornError> {
        actions.clear();
        match event {
            bridge::HttpOutboundEvent::Start {
                status,
                headers,
                trailers,
                directive,
            } => controller
                .handle_start(status, headers, trailers, directive)
                .map(|()| false),
            bridge::HttpOutboundEvent::Body { body, more_body } => controller
                .handle_body(actions, body, more_body)
                .map(|()| false),
            bridge::HttpOutboundEvent::PathSend { .. } => Ok(true),
            bridge::HttpOutboundEvent::Trailers {
                headers,
                more_trailers,
            } => controller
                .handle_trailers(actions, headers, more_trailers)
                .map(|()| false),
        }
    }

    #[test]
    fn unary_start_and_final_body_collapse_into_one_final_action() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        assert!(
            !event_requests_pathsend(
                &mut controller,
                &mut actions,
                bridge::HttpOutboundEvent::Start {
                    status: final_status(http::types::status_code::OK),
                    headers: vec![(
                        Bytes::from_static(b"x-demo").into(),
                        Bytes::from_static(b"1").into(),
                    )],
                    trailers: false,
                    directive: http::header::ResponseConnectionDirective::default(),
                },
            )
            .expect("response start is accepted")
        );
        assert!(actions.is_empty());

        assert!(
            !event_requests_pathsend(
                &mut controller,
                &mut actions,
                bridge::HttpOutboundEvent::Body {
                    body: bridge::PayloadBytes::from(Bytes::from_static(b"hello")),
                    more_body: false,
                },
            )
            .expect("final body is accepted")
        );
        assert!(controller.is_complete());
        assert_eq!(actions.len(), 1);
        match actions.pop().expect("final action is buffered") {
            http::response::ResponseAction::Final { start, body } => {
                let (status, headers) = start.into_status_headers();
                assert_eq!(status, http::types::status_code::OK);
                assert_eq!(headers.len(), 1);
                assert_eq!(headers[0].0.as_ref(), b"x-demo");
                assert_eq!(headers[0].1.as_ref(), b"1");
                match body {
                    http::response::FinalResponseBody::Bytes(body) => {
                        assert_eq!(body.as_ref(), b"hello");
                    },
                    _ => panic!("expected byte-backed final body"),
                }
            },
            _ => panic!("expected final response action"),
        }
    }

    #[test]
    fn head_only_responses_complete_without_a_trailer_section() {
        // A response with no content has no trailer section to put trailers
        // in. This used to open a streaming body and finish it with
        // trailers, which HTTP/1 wrote as a chunked terminator and trailer
        // lines into a response the client reads as bodyless.
        let mut controller = ResponseController::new(true, true);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: true,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");
        assert!(actions.is_empty());

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"hidden")),
                more_body: false,
            },
        )
        .expect("suppressed head-only body is accepted");
        // Complete on the wire, but not yet done: the application declared
        // trailers and must be able to send them without hitting a closed
        // stream.
        assert!(!controller.is_complete());
        assert_eq!(actions.len(), 1);
        match actions.pop().expect("final action exists") {
            http::response::ResponseAction::Final { start, body } => {
                let (status, _headers) = start.into_status_headers();
                assert_eq!(status, http::types::status_code::OK);
                // The head still describes the length a GET would return.
                assert!(matches!(
                    body,
                    http::response::FinalResponseBody::Suppressed { len: 6 }
                ));
            },
            _ => panic!("expected a final action, not a streaming start"),
        }

        // Trailers the application declared are accepted and dropped: there
        // is nowhere on the wire for them to go, and refusing them would
        // fail an application that did nothing wrong.
        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Trailers {
                headers: http::types::ResponseTrailers::try_from(vec![(
                    Bytes::from_static(b"x-finished").into(),
                    Bytes::from_static(b"yes").into(),
                )])
                .expect("extension trailers are valid"),
                more_trailers: false,
            },
        )
        .expect("declared trailers are accepted");
        assert!(controller.is_complete());
        assert!(actions.is_empty(), "no trailer bytes may reach the wire");
    }

    #[test]
    fn head_pathsend_with_declared_trailers_finishes_without_a_trailer_section() {
        let mut controller = ResponseController::new(true, true);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: true,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");

        controller
            .handle_pathsend(
                &mut actions,
                http::pathsend::PathSource::Buffered(Bytes::from_static(b"hidden")),
                6,
            )
            .expect("suppressed pathsend is accepted");

        assert!(!controller.is_complete());
        assert!(matches!(
            actions.pop().expect("pathsend completes on the wire"),
            http::response::ResponseAction::Final {
                body: http::response::FinalResponseBody::Suppressed { len: 6 },
                ..
            }
        ));

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Trailers {
                headers: http::types::ResponseTrailers::try_from(vec![(
                    Bytes::from_static(b"x-finished").into(),
                    Bytes::from_static(b"yes").into(),
                )])
                .expect("extension trailers are valid"),
                more_trailers: false,
            },
        )
        .expect("declared trailers are discarded");
        assert!(controller.is_complete());
        assert!(
            actions.is_empty(),
            "suppressed trailers never reach the wire"
        );
    }

    #[test]
    fn bodyless_status_pathsend_with_declared_trailers_finishes_without_a_trailer_section() {
        for (status, expected_len) in [
            (http::types::status_code::NO_CONTENT, 0),
            (http::types::status_code::NOT_MODIFIED, 6),
        ] {
            let mut controller = ResponseController::new(false, true);
            let mut actions = http::response::ResponseActions::new();

            controller
                .handle_start(
                    final_status(status),
                    Vec::new(),
                    true,
                    http::header::ResponseConnectionDirective::default(),
                )
                .expect("response start is accepted");
            controller
                .handle_pathsend(
                    &mut actions,
                    http::pathsend::PathSource::Buffered(Bytes::from_static(b"hidden")),
                    6,
                )
                .expect("suppressed pathsend is accepted");

            assert!(matches!(
                actions.pop().expect("pathsend completes on the wire"),
                http::response::ResponseAction::Final {
                    body: http::response::FinalResponseBody::Suppressed { len },
                    ..
                } if len == expected_len
            ));
            controller
                .handle_trailers(&mut actions, http::types::ResponseTrailers::new(), false)
                .expect("declared trailers are discarded");
            assert!(controller.is_complete());
            assert!(actions.is_empty());
        }
    }

    #[test]
    fn streaming_body_transition_requires_live_stream_until_finished() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: false,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"partial")),
                more_body: true,
            },
        )
        .expect("streaming body is accepted");

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::new()),
                more_body: false,
            },
        )
        .expect("final empty body completes the stream");
        assert!(controller.is_complete());
    }

    #[test]
    fn declared_streaming_content_length_requires_exact_final_byte_count() {
        fn controller() -> ResponseController {
            let mut controller = ResponseController::new(false, false);
            controller
                .handle_start(
                    final_status(http::types::status_code::OK),
                    vec![(
                        Bytes::from_static(b"content-length").into(),
                        Bytes::from_static(b"3").into(),
                    )],
                    false,
                    http::header::ResponseConnectionDirective::default(),
                )
                .expect("start accepts a syntactically valid length");
            controller
        }

        let mut exact = controller();
        let mut actions = http::response::ResponseActions::new();
        exact
            .handle_body(&mut actions, Bytes::from_static(b"a").into(), true)
            .expect("prefix fits");
        exact
            .handle_body(&mut actions, Bytes::from_static(b"bc").into(), false)
            .expect("final byte count matches");
        assert!(exact.is_complete());

        let mut short = controller();
        short
            .handle_body(&mut actions, Bytes::from_static(b"a").into(), true)
            .expect("prefix fits");
        assert!(
            short
                .handle_body(&mut actions, Bytes::from_static(b"b").into(), false)
                .is_err()
        );
        assert!(matches!(
            actions.last(),
            Some(http::response::ResponseAction::AbortIncomplete)
        ));

        let mut long = controller();
        assert!(
            long.handle_body(&mut actions, Bytes::from_static(b"abcd").into(), true)
                .is_err()
        );
        assert!(matches!(
            actions.last(),
            Some(http::response::ResponseAction::AbortIncomplete)
        ));
    }

    #[test]
    fn finalizing_without_start_queues_internal_error_action() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        let err = controller
            .finalize(&mut actions, Ok(()))
            .expect_err("missing start is an app contract error");

        assert!(matches!(
            err.kind(),
            error::ErrorKind::HttpResponse(
                error::HttpResponseError::AppReturnedWithoutStartingResponse
            )
        ));
        assert!(matches!(actions.as_slice(), [
            http::response::ResponseAction::InternalError
        ]));
    }

    #[test]
    fn pathsend_followed_by_body_is_rejected() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: false,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");
        assert!(matches!(
            event_requests_pathsend(
                &mut controller,
                &mut actions,
                bridge::HttpOutboundEvent::PathSend {
                    path: PathBuf::from("/tmp/demo"),
                },
            ),
            Ok(true)
        ));
        let temp_path = temp_dir().join(format!("h2corn-response-test-{}", id()));
        write(&temp_path, b"demo").expect("temp file is created");
        controller
            .handle_pathsend(
                &mut actions,
                http::pathsend::PathSource::File(Box::new(
                    File::open(&temp_path).expect("temp file opens"),
                )),
                4,
            )
            .expect("pathsend is accepted");

        let Err(err) = event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"extra")),
                more_body: false,
            },
        ) else {
            panic!("body after pathsend is invalid");
        };

        assert!(matches!(
            err.kind(),
            error::ErrorKind::HttpResponse(
                error::HttpResponseError::BodyBeforeStart
                    | error::HttpResponseError::PathsendMixedWithBody
            )
        ));
        let _ = remove_file(temp_path);
    }

    fn start_unary(
        controller: &mut ResponseController,
        actions: &mut http::response::ResponseActions,
    ) {
        event_requests_pathsend(controller, actions, bridge::HttpOutboundEvent::Start {
            status: final_status(http::types::status_code::OK),
            headers: vec![(
                Bytes::from_static(b"content-type").into(),
                Bytes::from_static(b"image/png").into(),
            )],
            trailers: false,
            directive: http::header::ResponseConnectionDirective::default(),
        })
        .expect("response start is accepted");
        assert!(actions.is_empty());
    }

    fn assert_substitute_final_response(
        actions: &mut http::response::ResponseActions,
        expected_status: http::types::HttpStatusCode,
    ) {
        assert_eq!(actions.len(), 1);
        match actions.pop().expect("substitute final action is buffered") {
            http::response::ResponseAction::Final { start, body } => {
                let (status, headers) = start.into_status_headers();
                assert_eq!(status, expected_status);
                assert!(
                    headers.is_empty(),
                    "user-supplied headers must be discarded for the substitute",
                );
                assert!(matches!(body, http::response::FinalResponseBody::Empty));
            },
            _ => panic!("expected final substitute response action"),
        }
    }

    #[test]
    fn pathsend_substitute_emits_clean_404_after_start() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        start_unary(&mut controller, &mut actions);
        controller
            .handle_pathsend_substitute(&mut actions, http::types::status_code::NOT_FOUND)
            .expect("missing file substitutes 404");
        assert!(controller.is_complete());
        assert_substitute_final_response(&mut actions, http::types::status_code::NOT_FOUND);
    }

    #[test]
    fn pathsend_substitute_emits_clean_403_after_start() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        start_unary(&mut controller, &mut actions);
        controller
            .handle_pathsend_substitute(&mut actions, http::types::status_code::FORBIDDEN)
            .expect("unreadable file substitutes 403");
        assert!(controller.is_complete());
        assert_substitute_final_response(&mut actions, http::types::status_code::FORBIDDEN);
    }

    #[test]
    fn pathsend_substitute_rejects_pathsend_before_start() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        let err = controller
            .handle_pathsend_substitute(&mut actions, http::types::status_code::NOT_FOUND)
            .expect_err("pathsend before start is invalid");
        assert!(matches!(
            err.kind(),
            error::ErrorKind::HttpResponse(error::HttpResponseError::PathsendBeforeStart)
        ));
        assert!(actions.is_empty());
    }

    #[test]
    fn pathsend_substitute_rejects_after_streaming_body() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: false,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");
        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"partial")),
                more_body: true,
            },
        )
        .expect("streaming body is accepted");
        actions.clear();

        let err = controller
            .handle_pathsend_substitute(&mut actions, http::types::status_code::NOT_FOUND)
            .expect_err("substituting after body is invalid");
        assert!(matches!(
            err.kind(),
            error::ErrorKind::HttpResponse(error::HttpResponseError::PathsendMixedWithBody)
        ));
        assert!(actions.is_empty());
    }

    #[test]
    fn pathsend_error_predicates_classify_open_failures() {
        let path = Path::new("/definitely/does/not/exist/h2corn-test");
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::NotADirectory,
            ErrorKind::Other,
        ] {
            let err = error::PathsendError::open_failed(path, Error::from(kind));
            match err {
                error::PathsendError::OpenFailed { source, .. } => {
                    assert_eq!(source.kind(), kind);
                },
                error::PathsendError::NotRegularFile { .. } => {
                    panic!("open_failed must produce OpenFailed")
                },
            }
        }
        let not_regular = error::PathsendError::NotRegularFile {
            path: path.to_path_buf(),
        };
        assert!(matches!(
            not_regular,
            error::PathsendError::NotRegularFile { .. }
        ));
    }

    #[test]
    fn finalize_rejects_incomplete_streaming_responses() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: final_status(http::types::status_code::OK),
                headers: Vec::new(),
                trailers: false,
                directive: http::header::ResponseConnectionDirective::default(),
            },
        )
        .expect("response start is accepted");
        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"partial")),
                more_body: true,
            },
        )
        .expect("partial body is accepted");
        actions.clear();

        let err = controller
            .finalize(&mut actions, Ok(()))
            .expect_err("incomplete response should be rejected");
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions.pop().expect("abort action is present"),
            http::response::ResponseAction::AbortIncomplete,
        ));
        assert!(matches!(
            err.kind(),
            error::ErrorKind::HttpResponse(
                error::HttpResponseError::AppReturnedWithoutCompletingResponse
            ),
        ));
    }
}
