use std::sync::Arc;

use pyo3::prelude::*;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn};

use crate::bridge::{
    ASGI_QUEUE_CAPACITY, PyWebSocketReceive, PyWebSocketSend, WebSocketInboundEvent,
    WebSocketOutboundEvent, WebSocketSendBuffer, WebSocketSendState,
};
use crate::error::H2CornError;
use crate::http::scope::build_websocket_scope;
use crate::runtime::{RequestAdmission, start_app_call};

use super::{RequestedSubprotocols, session::WebSocketContext};

pub(super) struct RunningWebSocketApp {
    pub(super) recv_tx: mpsc::Sender<WebSocketInboundEvent>,
    pub(super) requested_subprotocols: RequestedSubprotocols,
    pub(super) send_state: WebSocketSendState,
    pub(super) send_buffer: WebSocketSendBuffer,
    pub(super) send_rx: mpsc::Receiver<WebSocketOutboundEvent>,
    pub(super) app_task: JoinHandle<Result<(), H2CornError>>,
    pub(super) _admission: RequestAdmission,
}

pub(super) fn start_websocket_app(
    ctx: WebSocketContext,
) -> Result<RunningWebSocketApp, H2CornError> {
    let WebSocketContext {
        request: ctx,
        admission,
        meta,
    } = ctx;
    let (recv_tx, recv_rx) = mpsc::channel(ASGI_QUEUE_CAPACITY);
    let (send_tx, send_rx) = mpsc::channel(ASGI_QUEUE_CAPACITY);
    let (send_state, send_buffer) = WebSocketSendState::new();
    let requested_subprotocols = meta.requested_subprotocols;
    let scope_subprotocols = requested_subprotocols.clone();
    let app = Arc::clone(&ctx.connection.app);
    let task_send_state = send_state.clone();

    let app_task = spawn(start_app_call(&app, move |py, app| {
        let scope = build_websocket_scope(py, &ctx, scope_subprotocols.as_ref())?;
        let receive = Py::new(
            py,
            PyWebSocketReceive::new_stream(app.locals.clone(), recv_rx),
        )?
        .into_bound(py)
        .into_any();
        let send = Py::new(
            py,
            PyWebSocketSend::new(app.locals.clone(), task_send_state, send_tx),
        )?
        .into_bound(py)
        .into_any();
        Ok((scope.into_any(), receive, send))
    })?);

    Ok(RunningWebSocketApp {
        recv_tx,
        requested_subprotocols,
        send_state,
        send_buffer,
        send_rx,
        app_task,
        _admission: admission,
    })
}

impl RunningWebSocketApp {
    pub(super) fn close_outbound(&mut self) {
        self.send_state.close();
        self.send_rx.close();
    }
}
