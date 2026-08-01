use std::io::ErrorKind;
use std::path::PathBuf;

use super::{actions, controller, transport};
use crate::log::ResponseLogState;
use crate::http::app::AdmittedHttpOutboundEvent;
use crate::http::types::{HttpStatusCode, status_code};
use crate::{bridge, error, http};

enum HttpEventEffect {
    None,
    PathSend(PathBuf),
}

pub(crate) async fn finalize_response<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    response_log: &mut ResponseLogState,
    app_result: Result<(), error::H2CornError>,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    let final_result = controller.finalize(actions, app_result);
    observe_and_apply(transport, actions, response_log).await?;
    final_result
}

pub(crate) async fn apply_http_event<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    response_log: &mut ResponseLogState,
    event: bridge::HttpOutboundEvent,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    let admitted = transport.admit_outbound_event(event).await?;
    apply_admitted_http_event(controller, transport, actions, response_log, admitted).await
}

pub(crate) async fn apply_admitted_http_event<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    response_log: &mut ResponseLogState,
    event: AdmittedHttpOutboundEvent,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    let effect = match handle_http_event_sync(controller, actions, event) {
        Ok(effect) => effect,
        Err(err) => {
            // A streaming content-length mismatch queues its protocol-specific
            // abort here. Apply it before returning the exception through the
            // application's send awaitable; otherwise H2 would leave a live
            // malformed stream and H1 would have no reason to close.
            observe_and_apply(transport, actions, response_log).await?;
            return Err(err);
        },
    };
    if let HttpEventEffect::PathSend(path) = effect {
        match http::pathsend::open_pathsend_file(path).await {
            Ok((file, len)) => controller.handle_pathsend(actions, file, len)?,
            Err(err) => match pathsend_open_substitute_status(&err) {
                Some(status) => {
                    crate::server::report_request_failure(Err(err));
                    controller.handle_pathsend_substitute(actions, status)?;
                },
                None => return Err(err),
            },
        }
    }
    observe_and_apply(transport, actions, response_log).await
}

async fn observe_and_apply<T>(
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    response_log: &mut ResponseLogState,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    for action in actions.iter() {
        response_log.observe(action);
    }
    transport.apply_response_actions(actions).await
}

fn pathsend_open_substitute_status(err: &error::H2CornError) -> Option<HttpStatusCode> {
    let error::ErrorKind::Pathsend(err) = err.kind() else {
        return None;
    };
    match err {
        error::PathsendError::NotRegularFile { .. } => Some(status_code::FORBIDDEN),
        error::PathsendError::OpenFailed { source, .. } => match source.kind() {
            ErrorKind::NotFound | ErrorKind::NotADirectory => Some(status_code::NOT_FOUND),
            ErrorKind::PermissionDenied => Some(status_code::FORBIDDEN),
            // Unix sockets refuse ordinary open with ENXIO; treat like other
            // non-file objects that must not enter the pathsend body path.
            _ if source.raw_os_error() == Some(rustix::io::Errno::NXIO.raw_os_error()) => {
                Some(status_code::FORBIDDEN)
            },
            _ => None,
        },
    }
}

fn handle_http_event_sync(
    controller: &mut controller::ResponseController,
    actions: &mut actions::ResponseActions,
    event: AdmittedHttpOutboundEvent,
) -> Result<HttpEventEffect, error::H2CornError> {
    actions.clear();
    let AdmittedHttpOutboundEvent { event, credit } = event;
    match event {
        bridge::HttpOutboundEvent::Start {
            status,
            headers,
            trailers,
            directive,
        } => controller
            .handle_start(status, headers, trailers, directive)
            .map(|()| HttpEventEffect::None),
        bridge::HttpOutboundEvent::Body { body, more_body } => controller
            .handle_body_with_credit(
                actions,
                actions::ResponseBody::with_credit(body, credit),
                more_body,
            )
            .map(|()| HttpEventEffect::None),
        bridge::HttpOutboundEvent::PathSend { path } => Ok(HttpEventEffect::PathSend(path)),
        bridge::HttpOutboundEvent::Trailers {
            headers,
            more_trailers,
        } => controller
            .handle_trailers(actions, headers, more_trailers)
            .map(|()| HttpEventEffect::None),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{apply_http_event, finalize_response};
    use crate::log::ResponseLogState;
    use crate::bridge::{HttpOutboundEvent, PayloadBytes};
    use crate::error::H2CornError;
    use crate::http::header::ResponseConnectionDirective;
    use crate::http::response::{
        FinalResponseBody, HttpResponseTransport, ResponseAction, ResponseActions,
        ResponseController,
    };
    use crate::http::types::{FinalResponseStatus, HttpStatusCode, ResponseHeaders, status_code};

    #[derive(Default)]
    struct RecordingTransport {
        actions: Vec<&'static str>,
    }

    impl HttpResponseTransport for RecordingTransport {
        async fn apply_response_action(
            &mut self,
            action: ResponseAction,
        ) -> Result<(), H2CornError> {
            self.actions.push(match action {
                ResponseAction::Final { body, .. } => match body {
                    FinalResponseBody::Empty => "final_empty",
                    FinalResponseBody::Bytes(_) => "final_bytes",
                    FinalResponseBody::File { .. } => "final_file",
                    FinalResponseBody::Suppressed { .. } => "final_suppressed",
                },
                ResponseAction::Start { .. } => "start",
                ResponseAction::Body(_) => "body",
                ResponseAction::File { .. } => "file",
                ResponseAction::Finish => "finish",
                ResponseAction::FinishWithTrailers(_) => "trailers",
                ResponseAction::InternalError => "internal_error",
                ResponseAction::AbortIncomplete => "abort",
            });
            Ok(())
        }

        async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
            self.actions.push("flush");
            Ok(())
        }
    }

    fn final_status(status: HttpStatusCode) -> FinalResponseStatus {
        FinalResponseStatus::new(status).expect("test status is final")
    }

    #[tokio::test]
    async fn driver_observes_before_transport_and_matches_action_shapes() {
        let mut controller = ResponseController::new(false, false);
        let mut transport = RecordingTransport::default();
        let mut actions = ResponseActions::new();
        let mut log = ResponseLogState::default();

        apply_http_event(
            &mut controller,
            &mut transport,
            &mut actions,
            &mut log,
            HttpOutboundEvent::Start {
                status: final_status(status_code::OK),
                headers: ResponseHeaders::new(),
                trailers: false,
                directive: ResponseConnectionDirective::default(),
            },
        )
        .await
        .unwrap();
        apply_http_event(
            &mut controller,
            &mut transport,
            &mut actions,
            &mut log,
            HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from_static(b"xy")),
                more_body: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 2);
        assert!(transport.actions.contains(&"final_bytes") || transport.actions.contains(&"body"));
    }

    #[tokio::test]
    async fn head_response_counts_zero_transmitted_body_bytes() {
        let mut controller = ResponseController::new(true, false);
        let mut transport = RecordingTransport::default();
        let mut actions = ResponseActions::new();
        let mut log = ResponseLogState::default();

        apply_http_event(
            &mut controller,
            &mut transport,
            &mut actions,
            &mut log,
            HttpOutboundEvent::Start {
                status: final_status(status_code::OK),
                headers: ResponseHeaders::new(),
                trailers: false,
                directive: ResponseConnectionDirective::default(),
            },
        )
        .await
        .unwrap();
        apply_http_event(
            &mut controller,
            &mut transport,
            &mut actions,
            &mut log,
            HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from_static(b"would-be-body")),
                more_body: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(log.status, Some(status_code::OK));
        assert_eq!(log.response_body_bytes, 0);
    }

    #[tokio::test]
    async fn finalize_internal_error_without_prior_start_records_500_once() {
        let mut controller = ResponseController::new(false, false);
        let mut transport = RecordingTransport::default();
        let mut actions = ResponseActions::new();
        let mut log = ResponseLogState::default();

        finalize_response(
            &mut controller,
            &mut transport,
            &mut actions,
            &mut log,
            Err(H2CornError::from(std::io::Error::other("boom"))),
        )
        .await
        .expect_err("app failure propagates");

        assert_eq!(log.status, Some(status_code::INTERNAL_SERVER_ERROR));
        assert_eq!(log.response_body_bytes, 0);
        assert_eq!(
            transport
                .actions
                .iter()
                .filter(|&&a| a == "internal_error")
                .count(),
            1
        );
    }
}
