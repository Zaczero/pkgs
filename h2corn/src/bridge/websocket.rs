use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use super::{
    EventSource, Requeueable, WebSocketInboundEvent, WebSocketOutboundEvent,
    build_websocket_inbound_event, parse_websocket_outbound_event, ready_none, receive_or_await,
};
use crate::buffered_events::BufferedState;
use crate::error::{AsgiError, IntoPyResult, into_pyerr};
use crate::pyloop::Shard;

#[derive(Clone)]
pub(crate) struct WebSocketSendState {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 2>>,
}

pub(crate) struct WebSocketSendBuffer {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 2>>,
}

pub(crate) enum WebSocketSendDisposition {
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
            },
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
            },
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

impl EventSource for WebSocketReceiveState {
    type Event = WebSocketInboundEvent;

    fn try_pull(&mut self) -> Option<Self::Event> {
        match self.phase {
            WebSocketReceivePhase::PendingConnect => {
                self.phase = WebSocketReceivePhase::Open;
                return Some(WebSocketInboundEvent::Connect);
            },
            WebSocketReceivePhase::Disconnected => {
                return Some(self.terminal_disconnect());
            },
            WebSocketReceivePhase::Open => {},
        }

        match self.rx.try_recv() {
            Ok(event) => Some(self.finalize_event(event)),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(self.terminal_disconnect()),
        }
    }

    async fn pull(&mut self) -> Self::Event {
        match self.rx.recv().await {
            Some(event) => self.finalize_event(event),
            None => self.terminal_disconnect(),
        }
    }
}

#[pyclass(frozen, name = "_WebSocketReceive")]
pub struct PyWebSocketReceive {
    shard: Shard,
    state: Arc<AsyncMutex<Requeueable<WebSocketReceiveState>>>,
}

impl PyWebSocketReceive {
    pub(crate) fn new_stream(shard: Shard, rx: mpsc::Receiver<WebSocketInboundEvent>) -> Self {
        Self {
            shard,
            state: Arc::new(AsyncMutex::new(Requeueable::new(WebSocketReceiveState {
                rx,
                phase: WebSocketReceivePhase::PendingConnect,
            }))),
        }
    }
}

#[pymethods]
impl PyWebSocketReceive {
    fn __call__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        receive_or_await(
            py,
            Arc::clone(&self.shard),
            &self.state,
            build_websocket_inbound_event,
        )
    }
}

#[pyclass(frozen, name = "_WebSocketSend")]
pub struct PyWebSocketSend {
    shard: Shard,
    state: WebSocketSendState,
    tx: mpsc::Sender<WebSocketOutboundEvent>,
}

impl PyWebSocketSend {
    pub(crate) const fn new(
        shard: Shard,
        state: WebSocketSendState,
        tx: mpsc::Sender<WebSocketOutboundEvent>,
    ) -> Self {
        Self { shard, state, tx }
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
            WebSocketSendDisposition::Buffered => Ok(ready_none(py, &self.shard)),
            WebSocketSendDisposition::Forward(event) => {
                super::try_send_or_await(py, Arc::clone(&self.shard), &self.tx, event)
            },
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
