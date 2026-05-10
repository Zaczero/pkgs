use std::mem;

use tokio::fs::File;

use super::actions;
use crate::error::{self, ErrorExt};
use crate::{bridge, http};

#[derive(Debug)]
enum ResponseState {
    WaitingForStart,
    UnaryBufferable(StartedResponse),
    StartedStreaming(StartedResponse),
    WaitingForTrailers {
        buffered: http::types::ResponseHeaders,
    },
    Complete,
    Aborted,
}

#[derive(Debug)]
struct StartedResponse {
    start: actions::ResponseStart,
    expects_trailers: bool,
    saw_body: bool,
    suppressed_body_len: usize,
}

impl StartedResponse {
    fn take_start_action(&mut self) -> actions::ResponseAction {
        actions::ResponseAction::Start {
            start: self.start.take_for_action(),
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

    fn pathsend_len_hint(&mut self) -> Option<usize> {
        self.start.content_length_hint()
    }
}

pub struct ResponseController {
    head_only: bool,
    supports_response_trailers: bool,
    state: ResponseState,
}

impl ResponseController {
    pub(crate) const fn new(head_only: bool, supports_response_trailers: bool) -> Self {
        Self {
            head_only,
            supports_response_trailers,
            state: ResponseState::WaitingForStart,
        }
    }

    pub(crate) const fn is_complete(&self) -> bool {
        matches!(self.state, ResponseState::Complete)
    }

    pub(crate) const fn needs_live_stream(&self) -> bool {
        match &self.state {
            ResponseState::StartedStreaming(_) | ResponseState::WaitingForTrailers { .. } => true,
            ResponseState::WaitingForStart
            | ResponseState::UnaryBufferable(_)
            | ResponseState::Complete
            | ResponseState::Aborted => false,
        }
    }

    pub(crate) fn pathsend_len_hint(&mut self) -> Option<usize> {
        match &mut self.state {
            ResponseState::UnaryBufferable(started) | ResponseState::StartedStreaming(started) => {
                started.pathsend_len_hint()
            },
            ResponseState::WaitingForStart
            | ResponseState::WaitingForTrailers { .. }
            | ResponseState::Complete
            | ResponseState::Aborted => None,
        }
    }

    pub(crate) fn handle_start(
        &mut self,
        status: http::types::HttpStatusCode,
        headers: http::types::ResponseHeaders,
        trailers: bool,
    ) -> Result<(), error::H2CornError> {
        if !matches!(self.state, ResponseState::WaitingForStart) {
            return error::HttpResponseError::StartAlreadyReceived.err();
        }
        if trailers && !self.supports_response_trailers {
            return error::HttpResponseError::TrailersNotAdvertised.err();
        }
        self.state = ResponseState::UnaryBufferable(StartedResponse {
            start: actions::ResponseStart::new(status, headers),
            expects_trailers: trailers,
            saw_body: false,
            suppressed_body_len: 0,
        });
        Ok(())
    }

    pub(crate) fn handle_body(
        &mut self,
        actions: &mut actions::ResponseActions,
        body: bridge::PayloadBytes,
        more_body: bool,
    ) -> Result<(), error::H2CornError> {
        let mut started = self.take_started(error::HttpResponseError::BodyBeforeStart)?;
        let final_chunk = !more_body;

        if self.head_only {
            started.saw_body = true;
            started.suppressed_body_len += body.len();
            self.state = if final_chunk {
                let len = started.suppressed_body_len;
                complete_or_wait_for_trailers(
                    actions,
                    &mut started,
                    actions::FinalResponseBody::Suppressed { len },
                )
            } else {
                ResponseState::UnaryBufferable(started)
            };
            return Ok(());
        }

        if final_chunk && !started.expects_trailers && !started.saw_body {
            let body = if body.is_empty() {
                actions::FinalResponseBody::Empty
            } else {
                actions::FinalResponseBody::Bytes(body)
            };
            self.state = complete_response(actions, &mut started, body);
            return Ok(());
        }

        if !started.saw_body {
            actions.push(started.take_start_action());
        }
        started.saw_body = true;
        if !body.is_empty() {
            actions.push(actions::ResponseAction::Body(body));
        }

        self.state = if final_chunk {
            if started.expects_trailers {
                ResponseState::WaitingForTrailers {
                    buffered: http::types::ResponseHeaders::new(),
                }
            } else {
                actions.push(actions::ResponseAction::Finish);
                ResponseState::Complete
            }
        } else {
            ResponseState::StartedStreaming(started)
        };
        Ok(())
    }

    pub(crate) fn handle_pathsend_substitute(
        &mut self,
        actions: &mut actions::ResponseActions,
        status: http::types::HttpStatusCode,
    ) -> Result<(), error::H2CornError> {
        let started = self.take_started(error::HttpResponseError::PathsendBeforeStart)?;
        if started.saw_body {
            self.state = ResponseState::StartedStreaming(started);
            return error::HttpResponseError::PathsendMixedWithBody.err();
        }
        drop(started);
        let start = actions::ResponseStart::new(status, http::types::ResponseHeaders::new());
        actions.push(actions::ResponseAction::Final {
            start,
            body: actions::FinalResponseBody::Empty,
        });
        self.state = ResponseState::Complete;
        Ok(())
    }

    pub(crate) fn handle_pathsend(
        &mut self,
        actions: &mut actions::ResponseActions,
        file: File,
        len: usize,
    ) -> Result<(), error::H2CornError> {
        let mut started = self.take_started(error::HttpResponseError::PathsendBeforeStart)?;
        if started.saw_body {
            self.state = ResponseState::StartedStreaming(started);
            return error::HttpResponseError::PathsendMixedWithBody.err();
        }

        if self.head_only {
            self.state = complete_or_wait_for_trailers(
                actions,
                &mut started,
                actions::FinalResponseBody::Suppressed { len },
            );
            return Ok(());
        }

        if started.expects_trailers {
            actions.push(started.take_start_action());
            actions.push(actions::ResponseAction::File { file, len });
            self.state = ResponseState::WaitingForTrailers {
                buffered: http::types::ResponseHeaders::new(),
            };
        } else {
            self.state =
                complete_response(actions, &mut started, actions::FinalResponseBody::File {
                    file,
                    len,
                });
        }
        Ok(())
    }

    pub(crate) fn handle_trailers(
        &mut self,
        actions: &mut actions::ResponseActions,
        headers: http::types::ResponseHeaders,
        more_trailers: bool,
    ) -> Result<(), error::H2CornError> {
        let ResponseState::WaitingForTrailers { buffered } = &mut self.state else {
            return error::HttpResponseError::TrailersBeforeBodyCompleted.err();
        };
        buffered.extend(headers);
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
            (ResponseState::UnaryBufferable(started), Ok(()))
                if !started.expects_trailers
                    && !started.saw_body
                    && started.suppressed_body_len == 0 =>
            {
                actions.push(started.into_final_action(actions::FinalResponseBody::Empty));
                Ok(())
            },
            (ResponseState::UnaryBufferable(started), Ok(()))
                if self.head_only && !started.expects_trailers && started.saw_body =>
            {
                let suppressed_body_len = started.suppressed_body_len;
                actions.push(
                    started.into_final_action(actions::FinalResponseBody::Suppressed {
                        len: suppressed_body_len,
                    }),
                );
                Ok(())
            },
            (
                ResponseState::UnaryBufferable(_)
                | ResponseState::StartedStreaming(_)
                | ResponseState::WaitingForTrailers { .. },
                Ok(()),
            ) => {
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
            ResponseState::UnaryBufferable(started) | ResponseState::StartedStreaming(started) => {
                Ok(started)
            },
            state => {
                self.state = state;
                err.err()
            },
        }
    }
}

fn wait_for_trailers(
    actions: &mut actions::ResponseActions,
    started: &mut StartedResponse,
) -> ResponseState {
    actions.push(started.take_start_action());
    ResponseState::WaitingForTrailers {
        buffered: http::types::ResponseHeaders::new(),
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

fn complete_or_wait_for_trailers(
    actions: &mut actions::ResponseActions,
    started: &mut StartedResponse,
    body: actions::FinalResponseBody,
) -> ResponseState {
    if started.expects_trailers {
        wait_for_trailers(actions, started)
    } else {
        complete_response(actions, started, body)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;
    use tokio::fs::File;

    use super::ResponseController;
    use crate::{bridge, error, http};

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
            } => controller
                .handle_start(status, headers, trailers)
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
                    status: 200,
                    headers: vec![(
                        Bytes::from_static(b"x-demo").into(),
                        Bytes::from_static(b"1").into(),
                    )],
                    trailers: false,
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
                assert_eq!(status, 200);
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
    fn head_only_responses_wait_for_trailers_after_suppressed_body() {
        let mut controller = ResponseController::new(true, true);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: 200,
                headers: Vec::new(),
                trailers: true,
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
        assert_eq!(actions.len(), 1);
        match actions.pop().expect("streaming start action exists") {
            http::response::ResponseAction::Start { start } => {
                let (status, headers) = start.into_status_headers();
                assert_eq!(status, 200);
                assert!(headers.is_empty());
            },
            _ => panic!("expected streaming start action"),
        }
        assert!(!controller.is_complete());

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Trailers {
                headers: vec![(
                    Bytes::from_static(b"x-finished").into(),
                    Bytes::from_static(b"yes").into(),
                )],
                more_trailers: false,
            },
        )
        .expect("trailers complete the response");
        assert!(controller.is_complete());
        assert_eq!(actions.len(), 1);
        match actions.pop().expect("finish-with-trailers action exists") {
            http::response::ResponseAction::FinishWithTrailers(trailers) => {
                assert_eq!(trailers.len(), 1);
                assert_eq!(trailers[0].0.as_ref(), b"x-finished");
                assert_eq!(trailers[0].1.as_ref(), b"yes");
            },
            _ => panic!("expected finish-with-trailers action"),
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
                status: 200,
                headers: Vec::new(),
                trailers: false,
            },
        )
        .expect("response start is accepted");
        assert!(!controller.needs_live_stream());

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Body {
                body: bridge::PayloadBytes::from(Bytes::from_static(b"partial")),
                more_body: true,
            },
        )
        .expect("streaming body is accepted");
        assert!(controller.needs_live_stream());

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
        assert!(!controller.needs_live_stream());
    }

    #[test]
    fn finalizing_without_start_queues_internal_error_action() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        let err = controller
            .finalize(&mut actions, Ok(()))
            .expect_err("missing start is an app contract error");

        assert!(matches!(
            err,
            error::H2CornError::HttpResponse(
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
                status: 200,
                headers: Vec::new(),
                trailers: false,
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
        let temp_path =
            std::env::temp_dir().join(format!("h2corn-response-test-{}", std::process::id()));
        std::fs::write(&temp_path, b"demo").expect("temp file is created");
        controller
            .handle_pathsend(
                &mut actions,
                File::from_std(std::fs::File::open(&temp_path).expect("temp file opens")),
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
            err,
            error::H2CornError::HttpResponse(
                error::HttpResponseError::BodyBeforeStart
                    | error::HttpResponseError::PathsendMixedWithBody
            )
        ));
        let _ = std::fs::remove_file(temp_path);
    }

    fn start_unary(controller: &mut ResponseController, actions: &mut http::response::ResponseActions) {
        event_requests_pathsend(
            controller,
            actions,
            bridge::HttpOutboundEvent::Start {
                status: 200,
                headers: vec![(
                    Bytes::from_static(b"content-type").into(),
                    Bytes::from_static(b"image/png").into(),
                )],
                trailers: false,
            },
        )
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
        assert_substitute_final_response(&mut actions, 404);
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
        assert_substitute_final_response(&mut actions, 403);
    }

    #[test]
    fn pathsend_substitute_rejects_pathsend_before_start() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        let err = controller
            .handle_pathsend_substitute(&mut actions, http::types::status_code::NOT_FOUND)
            .expect_err("pathsend before start is invalid");
        assert!(matches!(
            err,
            error::H2CornError::HttpResponse(error::HttpResponseError::PathsendBeforeStart)
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
                status: 200,
                headers: Vec::new(),
                trailers: false,
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
            err,
            error::H2CornError::HttpResponse(error::HttpResponseError::PathsendMixedWithBody)
        ));
        assert!(actions.is_empty());
    }

    #[test]
    fn pathsend_error_predicates_classify_open_failures() {
        let path = std::path::Path::new("/definitely/does/not/exist/h2corn-test");

        let not_found = error::PathsendError::open_failed(
            path,
            std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        );
        assert!(not_found.is_not_found());
        assert!(!not_found.is_permission_denied());
        assert!(!not_found.is_not_a_directory());

        let forbidden = error::PathsendError::open_failed(
            path,
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "no access"),
        );
        assert!(!forbidden.is_not_found());
        assert!(forbidden.is_permission_denied());
        assert!(!forbidden.is_not_a_directory());

        let not_a_dir = error::PathsendError::open_failed(
            path,
            std::io::Error::new(std::io::ErrorKind::NotADirectory, "path component is a file"),
        );
        assert!(!not_a_dir.is_not_found());
        assert!(!not_a_dir.is_permission_denied());
        assert!(not_a_dir.is_not_a_directory());

        let other = error::PathsendError::open_failed(path, std::io::Error::other("weird"));
        assert!(!other.is_not_found());
        assert!(!other.is_permission_denied());
        assert!(!other.is_not_a_directory());
    }

    #[test]
    fn finalize_rejects_incomplete_streaming_responses() {
        let mut controller = ResponseController::new(false, false);
        let mut actions = http::response::ResponseActions::new();

        event_requests_pathsend(
            &mut controller,
            &mut actions,
            bridge::HttpOutboundEvent::Start {
                status: 200,
                headers: Vec::new(),
                trailers: false,
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
            err,
            error::H2CornError::HttpResponse(
                error::HttpResponseError::AppReturnedWithoutCompletingResponse
            ),
        ));
    }
}
