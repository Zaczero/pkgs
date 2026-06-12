use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use super::{
    EventSource, HttpInboundEvent, Requeueable, buffered_or_send, build_http_inbound_event,
    parse_http_outbound_event, ready_awaitable, receive_or_await,
};
use crate::error::IntoPyResult;
use crate::http::app::HttpSendState;
use crate::pyloop::Shard;
use crate::runtime::StreamInput;

#[derive(Debug)]
struct HttpReceiveState {
    rx: mpsc::Receiver<StreamInput>,
    disconnected: bool,
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
            Some(StreamInput::Data(body)) => HttpInboundEvent::Request {
                body,
                more_body: true,
            },
            Some(StreamInput::EndStream) => HttpInboundEvent::Request {
                body: Bytes::new(),
                more_body: false,
            },
            Some(StreamInput::Reset(_)) | None => {
                self.disconnected = true;
                HttpInboundEvent::HttpDisconnect
            },
        }
    }
}

impl EventSource for HttpReceiveState {
    type Event = HttpInboundEvent;

    fn try_pull(&mut self) -> Option<Self::Event> {
        if self.disconnected {
            return Some(HttpInboundEvent::HttpDisconnect);
        }
        match self.rx.try_recv() {
            Ok(input) => Some(self.map_input(Some(input))),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(self.map_input(None)),
        }
    }

    async fn pull(&mut self) -> Self::Event {
        let input = self.rx.recv().await;
        self.map_input(input)
    }
}

#[pyclass(frozen)]
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

    pub(crate) fn new_stream(shard: Shard, rx: mpsc::Receiver<StreamInput>) -> Self {
        Self {
            shard,
            kind: HttpReceiveKind::Stream(Arc::new(AsyncMutex::new(Requeueable::new(
                HttpReceiveState {
                    rx,
                    disconnected: false,
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
                    }
                }
            },
            HttpReceiveKind::Single(body) => body.lock().take().map_or_else(
                || HttpInboundEvent::HttpDisconnect,
                |body| HttpInboundEvent::Request {
                    body,
                    more_body: false,
                },
            ),
            HttpReceiveKind::Stream(state) => {
                return receive_or_await(py, self.shard, state, build_http_inbound_event);
            },
        };
        ready_awaitable(py, build_http_inbound_event(py, event)?)
    }
}

#[pyclass(frozen)]
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
        buffered_or_send(py, self.shard, self.state.push_or_forward(event))
    }
}
