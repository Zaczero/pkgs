use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use super::{
    EventSource, HttpInboundEvent, Requeueable, build_http_inbound_event,
    parse_http_outbound_event, ready_awaitable, receive_or_await,
};
use crate::error::{AsgiError, IntoPyResult, into_pyerr};
use crate::http::app::{HttpSendDisposition, HttpSendState};
use crate::pyloop::Shard;
use crate::runtime::StreamInput;

#[derive(Debug)]
struct HttpReceiveState {
    input: HttpReceiveInput,
    disconnect: Arc<RequestInputShared>,
}

#[derive(Debug)]
enum HttpReceiveInput {
    Open(mpsc::Receiver<StreamInput>),
    Disconnected,
}

#[derive(Debug, Default)]
pub(crate) struct RequestInputShared {
    app_started: OnceLock<()>,
    started: AtomicU64,
    queued: AtomicU64,
    notify: Notify,
}

/// Optional request-body accounting capability. The allocation exists only
/// when access logging consumes it, keeping the default request signal small.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestBodyCounter {
    bytes: Arc<AtomicU64>,
}

impl RequestInputShared {
    pub(crate) fn mark_app_started(&self) {
        let _ = self.app_started.set(());
        self.notify.notify_one();
    }

    pub(crate) async fn wait_app_started(&self) {
        loop {
            let notified = self.notify.notified();
            if self.app_started.get().is_some() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn pending_resolution(self: &Arc<Self>) -> Option<PendingHttpDisconnect> {
        let started = self.started.load(Ordering::Acquire);
        (self.queued.load(Ordering::Acquire) < started).then(|| PendingHttpDisconnect {
            signal: Arc::clone(self),
            generation: started,
        })
    }

    pub(super) fn begin_wait(self: &Arc<Self>) -> HttpReceiveWaitGuard {
        let ticket = self.started.fetch_add(1, Ordering::AcqRel) + 1;
        HttpReceiveWaitGuard {
            signal: Arc::clone(self),
            ticket,
        }
    }
}

impl RequestBodyCounter {
    pub(crate) fn add(&self, len: u64) {
        self.bytes.fetch_add(len, Ordering::Relaxed);
    }

    pub(crate) fn load(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Single-owner proof that a receive resolution was pending when input
/// ownership closed. It cannot be mixed with another request's signal or
/// reused after cancellation has been ordered.
pub(crate) struct PendingHttpDisconnect {
    signal: Arc<RequestInputShared>,
    generation: u64,
}

impl PendingHttpDisconnect {
    pub(crate) async fn wait(self) {
        loop {
            let notified = self.signal.notify.notified();
            if self.signal.queued.load(Ordering::Acquire) >= self.generation {
                return;
            }
            notified.await;
        }
    }
}

pub(super) struct HttpReceiveWaitGuard {
    signal: Arc<RequestInputShared>,
    ticket: u64,
}

impl Drop for HttpReceiveWaitGuard {
    fn drop(&mut self) {
        self.signal.queued.fetch_max(self.ticket, Ordering::Release);
        // `notify_one` retains a permit if the owner observes the generation
        // before its Notified future is first polled. `notify_waiters` would
        // lose that narrow check-to-poll race.
        self.signal.notify.notify_one();
    }
}

#[derive(Debug)]
enum HttpReceiveKind {
    NoBody(AtomicBool),
    Single(Mutex<Option<Bytes>>),
    Stream(Arc<AsyncMutex<Requeueable<HttpReceiveState>>>),
}

impl HttpReceiveState {
    fn map_input(&mut self, input: Option<StreamInput>) -> HttpInboundEvent {
        match input {
            Some(StreamInput::Data { body, credit }) => HttpInboundEvent::Request {
                body,
                more_body: true,
                credit,
            },
            Some(StreamInput::BufferedData { body, credit }) => HttpInboundEvent::Request {
                body: body.freeze(),
                more_body: true,
                credit,
            },
            Some(StreamInput::DataBatch {
                bodies,
                body_bytes,
                credit,
            }) => HttpInboundEvent::RequestBatch {
                bodies,
                body_bytes,
                credit,
            },
            Some(StreamInput::EndStream) => HttpInboundEvent::Request {
                body: Bytes::new(),
                more_body: false,
                credit: None,
            },
            Some(StreamInput::Reset(_)) | None => {
                // Dropping the receiver here releases queued body owners and
                // their HTTP/2 credits immediately. A separate boolean left
                // an unreachable "disconnected but still retaining input"
                // state until the Python receive object itself was dropped.
                self.input = HttpReceiveInput::Disconnected;
                HttpInboundEvent::HttpDisconnect
            },
        }
    }
}

impl EventSource for HttpReceiveState {
    type Event = HttpInboundEvent;

    fn try_pull(&mut self) -> Option<Self::Event> {
        let input = match &mut self.input {
            HttpReceiveInput::Disconnected => return Some(HttpInboundEvent::HttpDisconnect),
            HttpReceiveInput::Open(rx) => match rx.try_recv() {
                Ok(input) => Some(input),
                Err(TryRecvError::Empty) => return None,
                Err(TryRecvError::Disconnected) => None,
            },
        };
        Some(self.map_input(input))
    }

    async fn pull(&mut self) -> Self::Event {
        let input = match &mut self.input {
            HttpReceiveInput::Disconnected => return HttpInboundEvent::HttpDisconnect,
            HttpReceiveInput::Open(rx) => rx.recv().await,
        };
        self.map_input(input)
    }

    fn wait_signal(&self) -> Option<Arc<RequestInputShared>> {
        Some(Arc::clone(&self.disconnect))
    }
}

#[pyclass(frozen, name = "_HttpReceive")]
pub struct PyHttpReceive {
    shard: Shard,
    kind: HttpReceiveKind,
}

impl PyHttpReceive {
    pub(crate) const fn new_no_body(shard: Shard) -> Self {
        Self {
            shard,
            kind: HttpReceiveKind::NoBody(AtomicBool::new(false)),
        }
    }

    pub(crate) fn new_stream(
        shard: Shard,
        rx: mpsc::Receiver<StreamInput>,
        disconnect: Arc<RequestInputShared>,
    ) -> Self {
        Self {
            shard,
            kind: HttpReceiveKind::Stream(Arc::new(AsyncMutex::new(Requeueable::new(
                HttpReceiveState {
                    input: HttpReceiveInput::Open(rx),
                    disconnect,
                },
            )))),
        }
    }

    pub(crate) const fn new_single(shard: Shard, body: Bytes) -> Self {
        Self {
            shard,
            kind: HttpReceiveKind::Single(Mutex::new(Some(body))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use bytes::{Bytes, BytesMut};
    use tokio::sync::mpsc;

    use super::{
        EventSource, HttpInboundEvent, HttpReceiveInput, HttpReceiveState, RequestInputShared,
    };
    use crate::h2_frame::{ErrorCode, StreamId};
    use crate::runtime::{H2InputCreditQueue, StreamInput};

    #[test]
    fn buffered_h2_input_maps_without_releasing_credit_early() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(StreamInput::BufferedData {
            body: BytesMut::from(&b"coalesced-body"[..]),
            credit: Some(flow.credit(stream_id, NonZeroU32::new(14).unwrap())),
        })
        .unwrap();
        let mut state = HttpReceiveState {
            input: HttpReceiveInput::Open(rx),
            disconnect: Arc::new(RequestInputShared::default()),
        };

        let Some(HttpInboundEvent::Request {
            body,
            more_body: true,
            credit,
        }) = state.try_pull()
        else {
            panic!("buffered data must map to one ordinary request event");
        };
        assert_eq!(body.as_ref(), b"coalesced-body");
        assert!(!flow.has_pending(), "mapping retains the receive credit");
        drop(credit);

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 14);
    }

    #[test]
    fn batched_h2_input_preserves_body_length_and_credit() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(StreamInput::DataBatch {
            bodies: vec![
                Bytes::from_static(b"segmented-"),
                Bytes::from_static(b"body"),
            ],
            body_bytes: b"segmented-body".len(),
            credit: Some(flow.credit(stream_id, NonZeroU32::new(14).unwrap())),
        })
        .unwrap();
        let mut state = HttpReceiveState {
            input: HttpReceiveInput::Open(rx),
            disconnect: Arc::new(RequestInputShared::default()),
        };

        let Some(HttpInboundEvent::RequestBatch {
            bodies,
            body_bytes,
            credit,
        }) = state.try_pull()
        else {
            panic!("batched data must remain one batched request event");
        };
        assert_eq!(body_bytes, b"segmented-body".len());
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0].as_ref(), b"segmented-");
        assert_eq!(bodies[1].as_ref(), b"body");
        assert!(!flow.has_pending(), "mapping retains the receive credit");
        drop(credit);

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 14);
    }

    #[test]
    fn terminal_input_drops_the_receiver_and_queued_owners_immediately() {
        let (tx, rx) = mpsc::channel(2);
        tx.try_send(StreamInput::Reset(ErrorCode::CANCEL)).unwrap();
        tx.try_send(StreamInput::data(bytes::Bytes::from_static(b"queued")))
            .unwrap();
        let mut state = HttpReceiveState {
            input: HttpReceiveInput::Open(rx),
            disconnect: Arc::new(RequestInputShared::default()),
        };

        assert!(matches!(
            state.try_pull(),
            Some(HttpInboundEvent::HttpDisconnect)
        ));
        assert!(
            tx.is_closed(),
            "terminal input must drop the receiver immediately"
        );
        assert!(matches!(state.input, HttpReceiveInput::Disconnected));
    }
}

#[pymethods]
impl PyHttpReceive {
    fn __call__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let event = match &self.kind {
            HttpReceiveKind::NoBody(state) => {
                if state.swap(true, Ordering::Relaxed) {
                    HttpInboundEvent::HttpDisconnect
                } else {
                    HttpInboundEvent::Request {
                        body: Bytes::new(),
                        more_body: false,
                        credit: None,
                    }
                }
            },
            HttpReceiveKind::Single(body) => body.lock().take().map_or_else(
                || HttpInboundEvent::HttpDisconnect,
                |body| HttpInboundEvent::Request {
                    body,
                    more_body: false,
                    credit: None,
                },
            ),
            HttpReceiveKind::Stream(state) => {
                return receive_or_await(
                    py,
                    Arc::clone(&self.shard),
                    state,
                    build_http_inbound_event,
                );
            },
        };
        ready_awaitable(py, build_http_inbound_event(py, event)?)
    }
}

#[pyclass(frozen, name = "_HttpSend")]
pub struct PyHttpSend {
    shard: Shard,
    state: HttpSendState,
}

impl PyHttpSend {
    pub(crate) const fn new(shard: Shard, state: HttpSendState) -> Self {
        Self { shard, state }
    }
}

#[pymethods]
impl PyHttpSend {
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        message: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let event = parse_http_outbound_event(message).into_pyresult()?;
        match self.state.push_or_forward(event) {
            HttpSendDisposition::Buffered | HttpSendDisposition::Sent => {
                Ok(super::ready_none(py, &self.shard))
            },
            HttpSendDisposition::Backpressured { tx, event } => {
                super::send_after_full(py, Arc::clone(&self.shard), tx, event)
            },
            HttpSendDisposition::Closed => Err(into_pyerr(AsgiError::SendAfterClose)),
        }
    }
}
