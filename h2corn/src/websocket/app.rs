use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn};
use tokio::time::timeout;

use super::RequestedSubprotocols;
use super::session::WebSocketContext;
use crate::bridge::{
    ASGI_QUEUE_CAPACITY, PyWebSocketReceive, PyWebSocketSend, WebSocketInboundEvent,
    WebSocketOutboundEvent, WebSocketSendBuffer, WebSocketSendState,
};
use crate::error::{ErrorExt, H2CornError};
use crate::http::scope::build_websocket_scope;
use crate::runtime::{RequestAdmission, start_app_call};

/// The app task, encapsulated so session code can never race or misuse the
/// raw `JoinHandle`: completion is only observable through methods that
/// uphold two invariants — the handle is never polled again after
/// completing, and (via [`RunningWebSocketApp::next_handshake_step`])
/// buffered outbound events always win over task completion, because the
/// app's sends happen-before its return.
pub(super) struct AppHandle {
    task: JoinHandle<Result<(), H2CornError>>,
    /// `Some` once the task has been polled to completion; the result waits
    /// here until [`Self::settle`] (or a step API) surfaces it.
    outcome: Option<Result<(), H2CornError>>,
    /// The task has been polled to completion (even if `outcome` was taken).
    joined: bool,
}

impl AppHandle {
    pub(super) const fn new(task: JoinHandle<Result<(), H2CornError>>) -> Self {
        Self {
            task,
            outcome: None,
            joined: false,
        }
    }

    /// True once the task's completion has been observed by a join; used as
    /// a `select!` branch guard so the handle is never re-polled.
    pub(super) const fn joined(&self) -> bool {
        self.joined
    }

    /// Wait for the task and store its outcome. Callers surface the result
    /// via [`Self::take_outcome`] or [`Self::settle`] — after draining any
    /// outbound events the app sent before returning.
    pub(super) async fn join_store(&mut self) {
        debug_assert!(!self.joined, "app task joined twice");
        let result = flatten_app_result((&mut self.task).await);
        self.joined = true;
        self.outcome = Some(result);
    }

    pub(super) const fn take_outcome(&mut self) -> Option<Result<(), H2CornError>> {
        self.outcome.take()
    }

    /// Surface the app result: a stored outcome first, otherwise wait up to
    /// `grace` for completion, aborting on timeout.
    pub(super) async fn settle(&mut self, grace: Duration) -> Result<(), H2CornError> {
        if let Some(outcome) = self.outcome.take() {
            return outcome;
        }
        if self.joined {
            // The result was already surfaced through a step API.
            return Ok(());
        }
        if let Ok(result) = timeout(grace, &mut self.task).await {
            self.joined = true;
            flatten_app_result(result)
        } else {
            self.abort().await;
            Ok(())
        }
    }

    /// Abort the task and wait for it to settle. Safe in every state.
    pub(super) async fn abort(&mut self) {
        self.task.abort();
        if !self.joined {
            let _ = (&mut self.task).await;
            self.joined = true;
            self.outcome = None;
        }
    }
}

/// One arbitration step between the handshake send buffer and app
/// completion. Buffered events always win: an event the app sent just
/// before returning is visible by the time completion is observed.
pub(super) enum AppStep {
    Event(WebSocketOutboundEvent),
    /// The app finished with this result and the buffer is empty. Terminal.
    Done(Result<(), H2CornError>),
}

pub(super) struct RunningWebSocketApp {
    pub(super) recv_tx: mpsc::Sender<WebSocketInboundEvent>,
    pub(super) requested_subprotocols: RequestedSubprotocols,
    pub(super) send_state: WebSocketSendState,
    pub(super) send_buffer: WebSocketSendBuffer,
    pub(super) send_rx: mpsc::Receiver<WebSocketOutboundEvent>,
    pub(super) app: AppHandle,
    pub(super) _admission: RequestAdmission,
}

impl RunningWebSocketApp {
    pub(super) fn close_outbound(&mut self) {
        self.send_state.close();
        self.send_rx.close();
    }

    /// Next handshake-phase step; see [`AppStep`] for the ordering contract.
    pub(super) async fn next_handshake_step(&mut self) -> AppStep {
        loop {
            if let Some(event) = self.send_buffer.take_ready() {
                return AppStep::Event(event);
            }
            if self.app.joined() {
                return AppStep::Done(self.app.take_outcome().unwrap_or(Ok(())));
            }
            tokio::select! {
                () = self.send_buffer.wait_ready() => {}
                () = self.app.join_store() => {}
            }
        }
    }
}

pub(super) fn flatten_app_result(
    result: Result<Result<(), H2CornError>, tokio::task::JoinError>,
) -> Result<(), H2CornError> {
    match result {
        Ok(result) => result,
        Err(err) => err.err(),
    }
}

pub(super) fn start_websocket_app(ctx: WebSocketContext) -> RunningWebSocketApp {
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

    let app_task = spawn(start_app_call(&app, move |py, _app, shard| {
        let scope = build_websocket_scope(py, &ctx, scope_subprotocols.as_ref())?;
        let receive = Py::new(py, PyWebSocketReceive::new_stream(shard, recv_rx))?
            .into_bound(py)
            .into_any();
        let send = Py::new(py, PyWebSocketSend::new(shard, task_send_state, send_tx))?
            .into_bound(py)
            .into_any();
        Ok((scope.into_any(), receive, send))
    }));

    RunningWebSocketApp {
        recv_tx,
        requested_subprotocols,
        send_state,
        send_buffer,
        send_rx,
        app: AppHandle::new(app_task),
        _admission: admission,
    }
}
