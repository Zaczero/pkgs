use std::path::PathBuf;

use super::{actions, controller, transport};
use crate::http::types::{HttpStatusCode, status_code};
use crate::{bridge, error, http};

enum HttpEventEffect {
    None,
    PathSend(PathBuf),
}

pub async fn finalize_response<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    app_result: Result<(), error::H2CornError>,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    let final_result = controller.finalize(actions, app_result);
    transport.apply_response_actions(actions).await?;
    final_result
}

pub async fn apply_http_event<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    event: bridge::HttpOutboundEvent,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    if let HttpEventEffect::PathSend(path) = handle_http_event_sync(controller, actions, event)? {
        match http::pathsend::open_pathsend_file(path, controller.pathsend_len_hint()).await {
            Ok((file, len)) => controller.handle_pathsend(actions, file, len)?,
            Err(err) => match pathsend_open_substitute_status(&err) {
                Some(status) => controller.handle_pathsend_substitute(actions, status)?,
                None => return Err(err),
            },
        }
    }
    transport.apply_response_actions(actions).await
}

fn pathsend_open_substitute_status(err: &error::H2CornError) -> Option<HttpStatusCode> {
    let error::H2CornError::Pathsend(err) = err else {
        return None;
    };
    match err.io_error_kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory => {
            Some(status_code::NOT_FOUND)
        },
        std::io::ErrorKind::PermissionDenied => Some(status_code::FORBIDDEN),
        _ => None,
    }
}

fn handle_http_event_sync(
    controller: &mut controller::ResponseController,
    actions: &mut actions::ResponseActions,
    event: bridge::HttpOutboundEvent,
) -> Result<HttpEventEffect, error::H2CornError> {
    actions.clear();
    match event {
        bridge::HttpOutboundEvent::Start {
            status,
            headers,
            trailers,
        } => controller
            .handle_start(status, headers, trailers)
            .map(|()| HttpEventEffect::None),
        bridge::HttpOutboundEvent::Body { body, more_body } => controller
            .handle_body(actions, body, more_body)
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
