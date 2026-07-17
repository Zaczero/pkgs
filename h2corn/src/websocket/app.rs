use std::mem;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use super::RequestedSubprotocols;
use super::session::WebSocketContext;
use crate::app_call::AppCallArgs;
use crate::bridge::{
    ASGI_QUEUE_CAPACITY, WebSocketInboundEvent, WebSocketOutboundEvent, WebSocketSendBuffer,
    WebSocketSendState,
};
use crate::error::H2CornError;
use crate::pyloop::SlotFuture;
use crate::runtime::{RequestTaskGuard, start_app_call};

/// The app task, encapsulated so session code can never race or misuse the
/// raw future: completion is only observable through methods that
/// uphold two invariants — the handle is never polled again after
/// completing, and (via [`RunningWebSocketApp::next_handshake_step`])
/// buffered outbound events always win over task completion, because the
/// app's sends happen-before its return.
pub(super) struct WebSocketAppTask {
    state: WebSocketAppTaskState,
}

/// Exclusive ownership states for the application's Python task.
///
/// A raw handle and a joined outcome cannot coexist, and a surfaced or
/// aborted task cannot accidentally be polled again.
enum WebSocketAppTaskState {
    Running(SlotFuture<Result<(), H2CornError>, RequestTaskGuard>),
    Joined(Result<(), H2CornError>),
    Settled,
}

impl WebSocketAppTask {
    pub(super) const fn new(task: SlotFuture<Result<(), H2CornError>, RequestTaskGuard>) -> Self {
        Self {
            state: WebSocketAppTaskState::Running(task),
        }
    }

    /// True once the task's completion has been observed by a join; used as
    /// a `select!` branch guard so the handle is never re-polled.
    pub(super) const fn is_joined(&self) -> bool {
        match self.state {
            WebSocketAppTaskState::Running(_) => false,
            WebSocketAppTaskState::Joined(_) | WebSocketAppTaskState::Settled => true,
        }
    }

    /// Wait for the task and store its outcome. Callers surface the result
    /// via [`Self::take_outcome`] or [`Self::settle`] — after draining any
    /// outbound events the app sent before returning.
    pub(super) async fn join_store(&mut self) {
        let result = match &mut self.state {
            WebSocketAppTaskState::Running(task) => task.await,
            WebSocketAppTaskState::Joined(_) | WebSocketAppTaskState::Settled => {
                debug_assert!(false, "app task joined twice");
                return;
            },
        };
        self.state = WebSocketAppTaskState::Joined(result);
    }

    pub(super) fn take_outcome(&mut self) -> Option<Result<(), H2CornError>> {
        match mem::replace(&mut self.state, WebSocketAppTaskState::Settled) {
            WebSocketAppTaskState::Joined(outcome) => Some(outcome),
            state => {
                self.state = state;
                None
            },
        }
    }

    /// Surface the app result: a stored outcome first, otherwise wait up to
    /// `grace` for completion, aborting on timeout.
    pub(super) async fn settle(&mut self, grace: Duration) -> Result<(), H2CornError> {
        match mem::replace(&mut self.state, WebSocketAppTaskState::Settled) {
            WebSocketAppTaskState::Joined(outcome) => return outcome,
            WebSocketAppTaskState::Settled => return Ok(()),
            WebSocketAppTaskState::Running(task) => {
                self.state = WebSocketAppTaskState::Running(task);
            },
        }

        let result = match &mut self.state {
            WebSocketAppTaskState::Running(task) => timeout(grace, task).await,
            WebSocketAppTaskState::Joined(_) | WebSocketAppTaskState::Settled => {
                unreachable!("running state was restored above")
            },
        };
        self.state = WebSocketAppTaskState::Settled;
        result.unwrap_or(Ok(()))
    }

    /// Abandon the task. Dropping its slot queues cancellation on the owning
    /// Python loop; the slot's admission guard lives through the done callback.
    pub(super) fn abort(&mut self) {
        self.state = WebSocketAppTaskState::Settled;
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
    pub(super) task: WebSocketAppTask,
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
            if self.task.is_joined() {
                return AppStep::Done(self.task.take_outcome().unwrap_or(Ok(())));
            }
            tokio::select! {
                () = self.send_buffer.wait_ready() => {}
                () = self.task.join_store() => {}
            }
        }
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

    let app_task = start_app_call(
        app,
        AppCallArgs::websocket(ctx, scope_subprotocols, recv_rx, task_send_state, send_tx),
        admission,
    );

    RunningWebSocketApp {
        recv_tx,
        requested_subprotocols,
        send_state,
        send_buffer,
        send_rx,
        task: WebSocketAppTask::new(app_task),
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use pyo3::Python;

    use super::WebSocketAppTask;
    use crate::error::H2CornError;
    use crate::pyloop::{Shard, SlotFuture, TaskSlot};
    use crate::runtime::{AppRuntimeHandle, RequestTaskGuard, test_fixtures};

    fn test_app() -> AppRuntimeHandle {
        Python::initialize();
        Python::attach(test_fixtures::app_runtime)
    }

    fn completed_app_future(
        app: &AppRuntimeHandle,
        shard: &Shard,
    ) -> SlotFuture<Result<(), H2CornError>, RequestTaskGuard> {
        let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(app));
        let future = slot.wait(Arc::clone(shard));
        slot.fill(Ok(()));
        future
    }

    #[tokio::test(flavor = "current_thread")]
    async fn joined_outcome_is_terminal_and_surfaced_once() {
        let app = test_app();
        let mut task = WebSocketAppTask::new(completed_app_future(&app, &app.main_shard()));
        assert!(!task.is_joined());
        task.join_store().await;
        assert!(task.is_joined());
        task.take_outcome()
            .expect("joined outcome is retained")
            .unwrap();
        assert!(task.take_outcome().is_none());
        task.settle(Duration::ZERO).await.unwrap();
        drop(task);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_transitions_running_task_to_terminal_state() {
        let app = test_app();
        let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(&app));
        let mut task = WebSocketAppTask::new(slot.wait(app.main_shard()));
        task.abort();
        assert!(task.is_joined());
        assert!(task.take_outcome().is_none());
        drop(task);
        drop(slot);
        app.wait_for_scoped_owners().await;
        drop(app);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_running_task_aborts_instead_of_detaching() {
        let app = test_app();
        let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(&app));
        let task = WebSocketAppTask::new(slot.wait(app.main_shard()));
        drop(task);

        let mut settlement = Box::pin(app.wait_for_scoped_owners());
        let first_poll = poll_fn(|cx| Poll::Ready(settlement.as_mut().poll(cx))).await;
        assert!(
            first_poll.is_pending(),
            "dropping the owner must abandon the app task, not detach it"
        );

        drop(slot);
        settlement.await;
        drop(app);
    }
}
