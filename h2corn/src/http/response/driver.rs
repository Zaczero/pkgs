use std::path::PathBuf;

use super::{actions, controller, transport};
use crate::{bridge, error, http};

pub(crate) async fn finalize_response<T>(
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

pub(crate) async fn apply_http_event<T>(
    controller: &mut controller::ResponseController,
    transport: &mut T,
    actions: &mut actions::ResponseActions,
    event: bridge::HttpOutboundEvent,
) -> Result<(), error::H2CornError>
where
    T: transport::HttpResponseTransport,
{
    if let HttpEventEffect::PathSend(path) = handle_http_event_sync(controller, actions, event)? {
        let (file, len) =
            http::pathsend::open_pathsend_file(path, controller.pathsend_len_hint()).await?;
        controller.handle_pathsend(actions, file, len)?;
    }
    transport.apply_response_actions(actions).await
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

enum HttpEventEffect {
    None,
    PathSend(PathBuf),
}
