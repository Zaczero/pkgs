use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::{
    Mutex as AsyncMutex,
    mpsc::{self, error::TryRecvError},
};

use crate::buffered_events::BufferedState;
use crate::error::{AsgiError, IntoPyResult, into_pyerr};

use super::{
    ReceiveStateMachine, WebSocketInboundEvent, WebSocketOutboundEvent, buffered_or_send,
    build_websocket_inbound_event, parse_websocket_outbound_event, ready_awaitable,
    receive_or_await,
};

#[derive(Clone)]
pub struct WebSocketSendState {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 2>>,
}

pub struct WebSocketSendBuffer {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 2>>,
}

pub enum WebSocketSendDisposition {
    Buffered,
    Forward(WebSocketOutboundEvent),
    Closed,
}

enum WebSocketSendMode {
    Handshake,
    AcceptedDrain,
    Streaming,
    Closed,
}

impl WebSocketSendState {
    pub(crate) fn new() -> (Self, WebSocketSendBuffer) {
        let shared = Arc::new(BufferedState::new(WebSocketSendMode::Handshake));
        (
            Self {
                shared: Arc::clone(&shared),
            },
            WebSocketSendBuffer { shared },
        )
    }

    pub(crate) fn push_or_forward(
        &self,
        event: WebSocketOutboundEvent,
    ) -> WebSocketSendDisposition {
        let mut inner = self.shared.lock();
        match &inner.state {
            WebSocketSendMode::Handshake | WebSocketSendMode::AcceptedDrain => {
                inner.queue.push_back(event);
                drop(inner);
                self.shared.notify_ready();
                WebSocketSendDisposition::Buffered
            }
            WebSocketSendMode::Streaming => WebSocketSendDisposition::Forward(event),
            WebSocketSendMode::Closed => WebSocketSendDisposition::Closed,
        }
    }

    pub(crate) fn close(&self) {
        let mut inner = self.shared.lock();
        inner.state = WebSocketSendMode::Closed;
        inner.queue.clear();
        drop(inner);
        self.shared.notify_ready();
    }
}

impl WebSocketSendBuffer {
    pub(crate) fn begin_accepted_drain(&self) {
        let mut inner = self.shared.lock();
        let WebSocketSendMode::Handshake = &inner.state else {
            return;
        };

        if inner.queue.is_empty() {
            inner.state = WebSocketSendMode::Streaming;
        } else {
            inner.state = WebSocketSendMode::AcceptedDrain;
        }
    }

    pub(crate) fn take_ready(&self) -> Option<WebSocketOutboundEvent> {
        let mut inner = self.shared.lock();
        let event = match &mut inner.state {
            WebSocketSendMode::Handshake => inner.queue.pop_front(),
            WebSocketSendMode::AcceptedDrain => {
                let event = inner.queue.pop_front()?;
                if inner.queue.is_empty() {
                    inner.state = WebSocketSendMode::Streaming;
                }
                Some(event)
            }
            WebSocketSendMode::Streaming | WebSocketSendMode::Closed => None,
        };
        drop(inner);
        event
    }

    pub(crate) async fn wait_ready(&self) {
        self.shared.wait_ready().await;
    }
}

#[derive(Debug)]
struct WebSocketReceiveState {
    rx: mpsc::Receiver<WebSocketInboundEvent>,
    phase: WebSocketReceivePhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WebSocketReceivePhase {
    PendingConnect,
    Open,
    Disconnected,
}

impl WebSocketReceiveState {
    const fn terminal_disconnect(&mut self) -> WebSocketInboundEvent {
        self.phase = WebSocketReceivePhase::Disconnected;
        WebSocketInboundEvent::Disconnect {
            code: 1005,
            reason: None,
        }
    }

    const fn finalize_event(&mut self, event: WebSocketInboundEvent) -> WebSocketInboundEvent {
        if matches!(event, WebSocketInboundEvent::Disconnect { .. }) {
            self.phase = WebSocketReceivePhase::Disconnected;
        }
        event
    }
}

impl ReceiveStateMachine for WebSocketReceiveState {
    type Event = WebSocketInboundEvent;

    fn try_next(&mut self) -> Option<Self::Event> {
        match self.phase {
            WebSocketReceivePhase::PendingConnect => {
                self.phase = WebSocketReceivePhase::Open;
                return Some(WebSocketInboundEvent::Connect);
            }
            WebSocketReceivePhase::Disconnected => {
                return Some(self.terminal_disconnect());
            }
            WebSocketReceivePhase::Open => {}
        }

        match self.rx.try_recv() {
            Ok(event) => Some(self.finalize_event(event)),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(self.terminal_disconnect()),
        }
    }

    async fn next(&mut self) -> Self::Event {
        if let Some(event) = self.try_next() {
            event
        } else {
            match self.rx.recv().await {
                Some(event) => self.finalize_event(event),
                None => self.terminal_disconnect(),
            }
        }
    }
}

#[pyclass(frozen, freelist = 256)]
pub struct PyWebSocketReceive {
    locals: TaskLocals,
    state: Arc<AsyncMutex<WebSocketReceiveState>>,
}

impl PyWebSocketReceive {
    pub(crate) fn new_stream(
        locals: TaskLocals,
        rx: mpsc::Receiver<WebSocketInboundEvent>,
    ) -> Self {
        Self {
            locals,
            state: Arc::new(AsyncMutex::new(WebSocketReceiveState {
                rx,
                phase: WebSocketReceivePhase::PendingConnect,
            })),
        }
    }
}

#[pymethods]
impl PyWebSocketReceive {
    fn __call__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        receive_or_await(py, &self.locals, &self.state, build_websocket_inbound_event)
    }
}

#[pyclass(frozen, freelist = 256)]
pub struct PyWebSocketSend {
    locals: TaskLocals,
    state: WebSocketSendState,
    tx: mpsc::Sender<WebSocketOutboundEvent>,
}

impl PyWebSocketSend {
    pub(crate) const fn new(
        locals: TaskLocals,
        state: WebSocketSendState,
        tx: mpsc::Sender<WebSocketOutboundEvent>,
    ) -> Self {
        Self { locals, state, tx }
    }
}

#[pymethods]
impl PyWebSocketSend {
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        message: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let event = parse_websocket_outbound_event(message).into_pyresult()?;
        match self.state.push_or_forward(event) {
            WebSocketSendDisposition::Buffered => ready_awaitable(py, py.None()),
            WebSocketSendDisposition::Forward(event) => {
                buffered_or_send(py, &self.locals, Some((self.tx.clone(), event)))
            }
            WebSocketSendDisposition::Closed => Err(into_pyerr(AsgiError::SendAfterClose)),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::WebSocketSendState;
    use crate::bridge::{PayloadBytes, WebSocketOutboundEvent};

    #[test]
    fn buffered_events_drain_before_streaming_after_accept() {
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::from_static(b"first")),
                more_body: true,
            }),
            super::WebSocketSendDisposition::Buffered
        ));

        send_buffer.begin_accepted_drain();

        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::SendBytes(PayloadBytes::from(
                Bytes::from_static(b"second"),
            ))),
            super::WebSocketSendDisposition::Buffered
        ));
        assert!(matches!(
            send_buffer.take_ready(),
            Some(WebSocketOutboundEvent::HttpResponseBody { .. })
        ));
        assert!(matches!(
            send_buffer.take_ready(),
            Some(WebSocketOutboundEvent::SendBytes(_))
        ));
        assert!(send_buffer.take_ready().is_none());
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::Close {
                code: 1000,
                reason: None,
            }),
            super::WebSocketSendDisposition::Forward(WebSocketOutboundEvent::Close { .. })
        ));
    }

    #[test]
    fn accept_without_buffered_events_starts_streaming_immediately() {
        let (send_state, send_buffer) = WebSocketSendState::new();
        send_buffer.begin_accepted_drain();

        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::Close {
                code: 1000,
                reason: None,
            }),
            super::WebSocketSendDisposition::Forward(WebSocketOutboundEvent::Close { .. })
        ));
    }
}
