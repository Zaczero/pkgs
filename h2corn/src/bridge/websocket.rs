use std::sync::Arc;

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
#[cfg(test)]
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc, watch};

use super::{
    EventSource, Requeueable, WebSocketInboundEvent, WebSocketOutboundEvent,
    build_websocket_inbound_event, parse_websocket_outbound_event, ready_none, receive_or_await,
};
use crate::buffered_events::BufferedState;
use crate::error::{AsgiError, IntoPyResult as _, WebSocketFrameKind, into_pyerr};
use crate::http::types::BytesStr;
use crate::pyloop::Shard;
use crate::websocket::validate_accepted_subprotocol;
use crate::websocket::{RequestedSubprotocols, WebSocketCloseCode, close_code};

/// Ordinary application-visible WebSocket data. Terminal disconnect is a
/// separate plane so a full message queue cannot block peer Close, ping
/// timeout, or transport end.
#[derive(Debug)]
pub(crate) enum WebSocketInboundMessage {
    Bytes(Bytes),
    Text(BytesStr),
}

impl WebSocketInboundMessage {
    pub(crate) const fn frame_kind(&self) -> WebSocketFrameKind {
        match self {
            Self::Bytes(_) => WebSocketFrameKind::Binary,
            Self::Text(_) => WebSocketFrameKind::Text,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Text(text) => text.len(),
        }
    }
}

#[derive(Debug)]
struct QueuedWebSocketInboundMessage {
    message: WebSocketInboundMessage,
    // The permit and payload share one owner, so queueing by bytes is exact:
    // removing the message for an ASGI receive releases its reservation.
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) enum WebSocketInboundTrySendError {
    Full(WebSocketInboundMessage),
    Closed,
}

#[derive(Clone, Debug)]
pub(crate) struct WebSocketDisconnect {
    pub code: WebSocketCloseCode,
    pub reason: BytesStr,
}

#[derive(Clone, Debug)]
pub(crate) struct WebSocketInboundSender {
    messages: mpsc::UnboundedSender<QueuedWebSocketInboundMessage>,
    byte_budget: Arc<Semaphore>,
    byte_capacity: u32,
    terminal: watch::Sender<Option<WebSocketDisconnect>>,
}

#[derive(Debug)]
pub(crate) struct WebSocketInboundReceiver {
    messages: mpsc::UnboundedReceiver<QueuedWebSocketInboundMessage>,
    terminal: watch::Receiver<Option<WebSocketDisconnect>>,
    terminal_delivered: bool,
}

impl WebSocketInboundSender {
    fn permit_cost(&self, message: &WebSocketInboundMessage) -> u32 {
        u32::try_from(message.len().max(1))
            .unwrap_or(u32::MAX)
            .min(self.byte_capacity)
    }

    fn queue(
        &self,
        message: WebSocketInboundMessage,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), WebSocketInboundMessage> {
        self.messages
            .send(QueuedWebSocketInboundMessage {
                message,
                _permit: permit,
            })
            .map_err(|queued| queued.0.message)
    }

    pub(crate) fn try_send(
        &self,
        message: WebSocketInboundMessage,
    ) -> Result<(), WebSocketInboundTrySendError> {
        if self.messages.is_closed() {
            return Err(WebSocketInboundTrySendError::Closed);
        }
        let Ok(permit) =
            Arc::clone(&self.byte_budget).try_acquire_many_owned(self.permit_cost(&message))
        else {
            return Err(WebSocketInboundTrySendError::Full(message));
        };
        self.queue(message, permit)
            .map_err(|_| WebSocketInboundTrySendError::Closed)
    }

    /// Wait for byte ownership without moving `message`. A session can select
    /// this wait against Close, EOF, ping deadlines and shutdown, retaining at
    /// most its one pending decoded message while it is stalled.
    #[cfg(test)]
    pub(crate) async fn acquire_capacity(
        &self,
        message: &WebSocketInboundMessage,
    ) -> Result<OwnedSemaphorePermit, ()> {
        self.acquire_bytes(message.len()).await
    }

    pub(crate) async fn acquire_bytes(
        &self,
        message_len: usize,
    ) -> Result<OwnedSemaphorePermit, ()> {
        if self.messages.is_closed() {
            return Err(());
        }
        Arc::clone(&self.byte_budget)
            .acquire_many_owned(
                u32::try_from(message_len.max(1))
                    .unwrap_or(u32::MAX)
                    .min(self.byte_capacity),
            )
            .await
            .map_err(|_| ())
    }

    pub(crate) fn send_reserved(
        &self,
        message: WebSocketInboundMessage,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), WebSocketInboundMessage> {
        self.queue(message, permit)
    }

    #[cfg(test)]
    pub(crate) async fn send(
        &self,
        message: WebSocketInboundMessage,
    ) -> Result<(), SendError<WebSocketInboundMessage>> {
        let Ok(permit) = self.acquire_capacity(&message).await else {
            return Err(SendError(message));
        };
        self.send_reserved(message, permit).map_err(SendError)
    }

    /// First-value-wins terminal publication. Never waits on the data queue.
    pub(crate) fn disconnect(&self, disconnect: WebSocketDisconnect) {
        self.terminal.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(disconnect);
            true
        });
    }
}

impl WebSocketInboundReceiver {
    fn take_terminal(&mut self) -> Option<WebSocketDisconnect> {
        if self.terminal_delivered {
            return None;
        }
        let disconnect = self.terminal.borrow().clone()?;
        self.terminal_delivered = true;
        Some(disconnect)
    }

    fn try_next_message(&mut self) -> Result<QueuedWebSocketInboundMessage, TryRecvError> {
        self.messages.try_recv()
    }

    fn terminal_ready(&self) -> bool {
        !self.terminal_delivered && self.terminal.borrow().is_some()
    }
}

#[derive(Clone)]
pub(crate) struct WebSocketSendState {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 1>>,
}

pub(crate) struct WebSocketSendBuffer {
    shared: Arc<BufferedState<WebSocketSendMode, WebSocketOutboundEvent, 1>>,
}

pub(crate) enum WebSocketSendDisposition {
    Buffered,
    Forward(WebSocketOutboundEvent),
    Closed,
}

enum WebSocketSendMode {
    Handshake,
    Forwarding,
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
        if matches!(inner.state, WebSocketSendMode::Handshake) {
            inner.queue.push_back(event);
            inner.state = WebSocketSendMode::Forwarding;
            drop(inner);
            self.shared.notify_ready();
            return WebSocketSendDisposition::Buffered;
        }
        match &inner.state {
            WebSocketSendMode::Handshake => unreachable!("handshake state returned above"),
            WebSocketSendMode::Forwarding => WebSocketSendDisposition::Forward(event),
            WebSocketSendMode::Closed => WebSocketSendDisposition::Closed,
        }
    }

    /// Whether the transport has already closed this stream's outbound side.
    ///
    /// Only covers a close published through the send state; the outbound
    /// channel closing is the other half, and both are consulted before an
    /// accepted value is judged. A `send()` that races either still reaches
    /// `push_or_forward` and gets the same answer.
    pub(crate) fn is_closed(&self) -> bool {
        matches!(self.shared.lock().state, WebSocketSendMode::Closed)
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
    pub(crate) fn take_ready(&self) -> Option<WebSocketOutboundEvent> {
        let mut inner = self.shared.lock();
        let event = inner.queue.pop_front();
        drop(inner);
        event
    }

    pub(crate) async fn wait_ready(&self) {
        self.shared.wait_ready().await;
    }
}

#[derive(Debug)]
struct WebSocketReceiveState {
    rx: WebSocketInboundReceiver,
    phase: WebSocketReceivePhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WebSocketReceivePhase {
    PendingConnect,
    Open,
    Disconnected,
}

impl WebSocketReceiveState {
    fn disconnect_event(disconnect: WebSocketDisconnect) -> WebSocketInboundEvent {
        WebSocketInboundEvent::Disconnect {
            code: disconnect.code,
            reason: if disconnect.reason.is_empty() {
                None
            } else {
                Some(disconnect.reason)
            },
        }
    }

    const fn terminal_disconnect(&mut self) -> WebSocketInboundEvent {
        self.phase = WebSocketReceivePhase::Disconnected;
        WebSocketInboundEvent::Disconnect {
            code: close_code::NO_STATUS_RECEIVED,
            reason: None,
        }
    }

    fn finalize_disconnect(&mut self, disconnect: WebSocketDisconnect) -> WebSocketInboundEvent {
        self.phase = WebSocketReceivePhase::Disconnected;
        Self::disconnect_event(disconnect)
    }

    fn message_event(message: QueuedWebSocketInboundMessage) -> WebSocketInboundEvent {
        match message.message {
            WebSocketInboundMessage::Bytes(body) => WebSocketInboundEvent::ReceiveBytes(body),
            WebSocketInboundMessage::Text(text) => WebSocketInboundEvent::ReceiveText(text),
        }
    }

    /// Prefer ordinary messages over terminal state when both are ready.
    fn try_open_event(&mut self) -> Option<WebSocketInboundEvent> {
        match self.rx.try_next_message() {
            Ok(message) => Some(Self::message_event(message)),
            Err(TryRecvError::Empty) => {
                if self.rx.terminal_ready() {
                    let disconnect = self.rx.take_terminal()?;
                    Some(self.finalize_disconnect(disconnect))
                } else {
                    None
                }
            },
            Err(TryRecvError::Disconnected) => {
                if let Some(disconnect) = self.rx.take_terminal() {
                    Some(self.finalize_disconnect(disconnect))
                } else {
                    Some(self.terminal_disconnect())
                }
            },
        }
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

        self.try_open_event()
    }

    async fn pull(&mut self) -> Self::Event {
        loop {
            if let Some(event) = self.try_open_event() {
                return event;
            }

            // Distinct-field borrows so both arms can poll without aliasing.
            tokio::select! {
                message = self.rx.messages.recv() => {
                    if let Some(message) = message {
                        return Self::message_event(message);
                    }
                    if let Some(disconnect) = self.rx.take_terminal() {
                        return self.finalize_disconnect(disconnect);
                    }
                    return self.terminal_disconnect();
                },
                result = self.rx.terminal.changed() => {
                    // Re-check messages first so a concurrent data item wins
                    // over the just-published terminal.
                    if result.is_err() {
                        if let Some(event) = self.try_open_event() {
                            return event;
                        }
                        return self.terminal_disconnect();
                    }
                },
            }
        }
    }
}

#[pyclass(frozen, name = "_WebSocketReceive")]
pub struct PyWebSocketReceive {
    shard: Shard,
    state: Arc<AsyncMutex<Requeueable<WebSocketReceiveState>>>,
}

impl PyWebSocketReceive {
    pub(crate) fn new_stream(shard: Shard, rx: WebSocketInboundReceiver) -> Self {
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
            &self.shard,
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
    /// Held here so `websocket.accept` can be checked against what the client
    /// offered while the application is still inside `await send(...)`.
    requested_subprotocols: RequestedSubprotocols,
}

impl PyWebSocketSend {
    pub(crate) const fn new(
        shard: Shard,
        state: WebSocketSendState,
        tx: mpsc::Sender<WebSocketOutboundEvent>,
        requested_subprotocols: RequestedSubprotocols,
    ) -> Self {
        Self {
            shard,
            state,
            tx,
            requested_subprotocols,
        }
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
        // Validate the accepted subprotocol here rather than in the session.
        // Deferring it let `await send(...)` return successfully and the
        // request fail afterwards, so the application could neither catch the
        // error nor choose a different subprotocol.
        //
        // Both checks sit inside this arm deliberately: an accept happens once
        // per connection, while `websocket.send` is the hot path and must not
        // pay for a lock the disposition below takes anyway. A closed stream
        // outranks a bad value — reporting `ValueError` for a subprotocol on a
        // connection that had already gone away would name the wrong problem,
        // when every other `send()` on that stream reports `OSError`.
        if let WebSocketOutboundEvent::Accept { subprotocol, .. } = &event {
            if self.state.is_closed() || self.tx.is_closed() {
                return Err(into_pyerr(AsgiError::SendAfterClose));
            }
            validate_accepted_subprotocol(
                &self.requested_subprotocols,
                subprotocol.as_ref().map(AsRef::as_ref),
            )
            .into_pyresult()?;
        }
        match self.state.push_or_forward(event) {
            WebSocketSendDisposition::Buffered => Ok(ready_none(py, &self.shard)),
            WebSocketSendDisposition::Forward(event) => {
                super::try_send_or_await(py, &self.shard, &self.tx, event)
            },
            WebSocketSendDisposition::Closed => Err(into_pyerr(AsgiError::SendAfterClose)),
        }
    }
}

pub(crate) fn websocket_inbound_channel(
    byte_capacity: usize,
) -> (WebSocketInboundSender, WebSocketInboundReceiver) {
    let byte_capacity = u32::try_from(byte_capacity.max(1)).unwrap_or(u32::MAX);
    let (messages_tx, messages_rx) = mpsc::unbounded_channel();
    let (terminal_tx, terminal_rx) = watch::channel(None);
    (
        WebSocketInboundSender {
            messages: messages_tx,
            byte_budget: Arc::new(Semaphore::new(byte_capacity as usize)),
            byte_capacity,
            terminal: terminal_tx,
        },
        WebSocketInboundReceiver {
            messages: messages_rx,
            terminal: terminal_rx,
            terminal_delivered: false,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::timeout;

    use super::{
        WebSocketDisconnect, WebSocketInboundMessage, WebSocketSendState, websocket_inbound_channel,
    };
    use crate::bridge::{PayloadBytes, WebSocketOutboundEvent};
    use crate::error::WebSocketFrameKind;
    use crate::http::types::BytesStr;
    use crate::websocket::close_code;

    #[test]
    fn dropped_text_message_is_reported_as_text() {
        let message = WebSocketInboundMessage::Text(BytesStr::from("text"));
        let error = crate::error::WebSocketError::receive_channel_closed(message.frame_kind());

        assert_eq!(
            error.to_string(),
            "application stopped receiving before the connection closed; a text frame was dropped"
        );
        assert_eq!(
            WebSocketInboundMessage::Bytes(Bytes::from_static(b"binary")).frame_kind(),
            WebSocketFrameKind::Binary
        );
    }

    #[test]
    fn one_inline_handshake_event_then_forwarding() {
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::from_static(b"first")),
                more_body: true,
            }),
            super::WebSocketSendDisposition::Buffered
        ));

        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::SendBytes(PayloadBytes::from(
                Bytes::from_static(b"second"),
            ))),
            super::WebSocketSendDisposition::Forward(_)
        ));
        assert!(matches!(
            send_buffer.take_ready(),
            Some(WebSocketOutboundEvent::HttpResponseBody { .. })
        ));
        assert!(send_buffer.take_ready().is_none());
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::Close {
                code: close_code::NORMAL,
                reason: None,
            }),
            super::WebSocketSendDisposition::Forward(WebSocketOutboundEvent::Close { .. })
        ));
    }

    #[test]
    fn forwarding_begins_with_the_first_handshake_event() {
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::new()),
                more_body: true,
            }),
            super::WebSocketSendDisposition::Buffered
        ));
        assert!(send_buffer.take_ready().is_some());

        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::Close {
                code: close_code::NORMAL,
                reason: None,
            }),
            super::WebSocketSendDisposition::Forward(WebSocketOutboundEvent::Close { .. })
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn disconnect_completes_while_data_queue_is_full() {
        let (tx, mut rx) = websocket_inbound_channel(2);
        tx.send(WebSocketInboundMessage::Bytes(Bytes::from_static(b"a")))
            .await
            .expect("slot 1");
        tx.send(WebSocketInboundMessage::Bytes(Bytes::from_static(b"b")))
            .await
            .expect("slot 2");

        // A full data plane must not stall terminal publication.
        timeout(Duration::from_millis(50), async {
            tx.disconnect(WebSocketDisconnect {
                code: close_code::NORMAL,
                reason: BytesStr::from("done"),
            });
        })
        .await
        .expect("disconnect must not await queue capacity");

        rx.try_next_message().expect("first full-queue message");
        rx.try_next_message().expect("second full-queue message");
        let disconnect = rx.take_terminal().expect("terminal after drained messages");
        assert_eq!(disconnect.code, close_code::NORMAL);
        assert_eq!(disconnect.reason.as_str(), "done");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn disconnect_is_first_value_wins() {
        let (tx, mut rx) = websocket_inbound_channel(1);
        tx.disconnect(WebSocketDisconnect {
            code: close_code::NORMAL,
            reason: BytesStr::from("first"),
        });
        tx.disconnect(WebSocketDisconnect {
            code: close_code::GOING_AWAY,
            reason: BytesStr::from("second"),
        });
        let disconnect = rx.take_terminal().expect("terminal");
        assert_eq!(disconnect.code, close_code::NORMAL);
        assert_eq!(disconnect.reason.as_str(), "first");
        assert!(rx.take_terminal().is_none());
    }
}
