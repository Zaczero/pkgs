use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use super::{
    HttpInboundEvent, ReceiveStateMachine, buffered_or_send, build_http_inbound_event,
    parse_http_outbound_event, ready_awaitable, receive_or_await,
};
use crate::error::IntoPyResult;
use crate::http::app::HttpSendState;
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
    Stream(Arc<AsyncMutex<HttpReceiveState>>),
}

impl ReceiveStateMachine for HttpReceiveState {
    type Event = HttpInboundEvent;

    fn try_next(&mut self) -> Option<Self::Event> {
        if self.disconnected {
            return Some(HttpInboundEvent::HttpDisconnect);
        }

        match self.rx.try_recv() {
            Ok(StreamInput::Data(body)) => Some(HttpInboundEvent::Request {
                body,
                more_body: true,
            }),
            Ok(StreamInput::EndStream) => Some(HttpInboundEvent::Request {
                body: Bytes::new(),
                more_body: false,
            }),
            Err(TryRecvError::Empty) => None,
            Ok(StreamInput::Reset(_)) | Err(TryRecvError::Disconnected) => {
                self.disconnected = true;
                Some(HttpInboundEvent::HttpDisconnect)
            },
        }
    }

    async fn next(&mut self) -> Self::Event {
        if let Some(event) = self.try_next() {
            event
        } else {
            match self.rx.recv().await {
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
}

#[pyclass(frozen, freelist = 256)]
pub struct PyHttpReceive {
    locals: TaskLocals,
    kind: HttpReceiveKind,
}

impl PyHttpReceive {
    pub(crate) const fn new_no_body(locals: TaskLocals) -> Self {
        Self {
            locals,
            kind: HttpReceiveKind::NoBody(AtomicBool::new(false)),
        }
    }

    pub(crate) fn new_stream(locals: TaskLocals, rx: mpsc::Receiver<StreamInput>) -> Self {
        Self {
            locals,
            kind: HttpReceiveKind::Stream(Arc::new(AsyncMutex::new(HttpReceiveState {
                rx,
                disconnected: false,
            }))),
        }
    }

    pub(crate) const fn new_single(locals: TaskLocals, body: Bytes) -> Self {
        Self {
            locals,
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
                return receive_or_await(py, &self.locals, state, build_http_inbound_event);
            },
        };
        ready_awaitable(py, build_http_inbound_event(py, event)?)
    }
}

#[pyclass(frozen, freelist = 256)]
pub struct PyHttpSend {
    locals: TaskLocals,
    state: HttpSendState,
}

impl PyHttpSend {
    pub(crate) const fn new(locals: TaskLocals, state: HttpSendState) -> Self {
        Self { locals, state }
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
        buffered_or_send(py, &self.locals, self.state.push_or_forward(event))
    }
}
