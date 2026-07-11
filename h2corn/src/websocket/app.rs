use std::mem;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle, spawn};
use tokio::time::timeout;

use super::RequestedSubprotocols;
use super::session::WebSocketContext;
use crate::app_call::AppCallArgs;
use crate::bridge::{
    ASGI_QUEUE_CAPACITY, WebSocketInboundEvent, WebSocketOutboundEvent, WebSocketSendBuffer,
    WebSocketSendState,
};
use crate::error::{ErrorExt, H2CornError};
use crate::runtime::{RequestAdmission, start_app_call};

/// The app task, encapsulated so session code can never race or misuse the
/// raw `JoinHandle`: completion is only observable through methods that
/// uphold two invariants — the handle is never polled again after
/// completing, and (via [`RunningWebSocketApp::next_handshake_step`])
/// buffered outbound events always win over task completion, because the
/// app's sends happen-before its return.
pub(super) struct AppHandle {
    state: AppTaskState,
}

/// Exclusive ownership states for the application's Tokio task.
///
/// A raw handle and a joined outcome can no longer coexist, and a surfaced or
/// aborted task cannot accidentally be polled again. The old representation
/// encoded those constraints as a coupled `joined` flag plus an optional
/// result while retaining the handle in every state.
enum AppTaskState {
    Running(JoinHandle<Result<(), H2CornError>>),
    Joined(Result<(), H2CornError>),
    Settled,
}

impl AppHandle {
    pub(super) const fn new(task: JoinHandle<Result<(), H2CornError>>) -> Self {
        Self {
            state: AppTaskState::Running(task),
        }
    }

    /// True once the task's completion has been observed by a join; used as
    /// a `select!` branch guard so the handle is never re-polled.
    pub(super) const fn is_joined(&self) -> bool {
        !matches!(self.state, AppTaskState::Running(_))
    }

    /// Wait for the task and store its outcome. Callers surface the result
    /// via [`Self::take_outcome`] or [`Self::settle`] — after draining any
    /// outbound events the app sent before returning.
    pub(super) async fn join_store(&mut self) {
        let AppTaskState::Running(task) = &mut self.state else {
            debug_assert!(false, "app task joined twice");
            return;
        };
        let result = flatten_app_result((&mut *task).await);
        self.state = AppTaskState::Joined(result);
    }

    pub(super) fn take_outcome(&mut self) -> Option<Result<(), H2CornError>> {
        match mem::replace(&mut self.state, AppTaskState::Settled) {
            AppTaskState::Joined(outcome) => Some(outcome),
            state => {
                self.state = state;
                None
            },
        }
    }

    /// Surface the app result: a stored outcome first, otherwise wait up to
    /// `grace` for completion, aborting on timeout.
    pub(super) async fn settle(&mut self, grace: Duration) -> Result<(), H2CornError> {
        match mem::replace(&mut self.state, AppTaskState::Settled) {
            AppTaskState::Joined(outcome) => return outcome,
            AppTaskState::Settled => return Ok(()),
            AppTaskState::Running(task) => self.state = AppTaskState::Running(task),
        }

        let AppTaskState::Running(task) = &mut self.state else {
            unreachable!("running state was restored above")
        };
        if let Ok(result) = timeout(grace, &mut *task).await {
            self.state = AppTaskState::Settled;
            flatten_app_result(result)
        } else {
            self.abort().await;
            Ok(())
        }
    }

    /// Abort the task and wait for it to settle. Safe in every state.
    pub(super) async fn abort(&mut self) {
        let AppTaskState::Running(task) = &mut self.state else {
            return;
        };
        task.abort();
        let _ = (&mut *task).await;
        self.state = AppTaskState::Settled;
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
            if self.app.is_joined() {
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
    result: Result<Result<(), H2CornError>, JoinError>,
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

    let app_task = spawn(start_app_call(
        app,
        AppCallArgs::websocket(ctx, scope_subprotocols, recv_rx, task_send_state, send_tx),
    ));

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

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use tokio::spawn;

    use super::AppHandle;

    #[tokio::test]
    async fn joined_outcome_is_terminal_and_surfaced_once() {
        let mut app = AppHandle::new(spawn(async { Ok(()) }));
        assert!(!app.is_joined());
        app.join_store().await;
        assert!(app.is_joined());
        app.take_outcome()
            .expect("joined outcome is retained")
            .unwrap();
        assert!(app.take_outcome().is_none());
        app.settle(Duration::ZERO).await.unwrap();
    }

    #[tokio::test]
    async fn abort_transitions_running_task_to_terminal_state() {
        let mut app = AppHandle::new(spawn(pending()));
        app.abort().await;
        assert!(app.is_joined());
        assert!(app.take_outcome().is_none());
    }
}
