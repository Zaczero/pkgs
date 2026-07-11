//! Closed ownership states for one ASGI application invocation.
//!
//! HTTP and WebSocket calls move different Rust resources onto the Python
//! event-loop thread. The public constructors are the only way to create a
//! call state, so protocol resources cannot be crossed accidentally. The
//! enum is boxed once at the queue boundary: the old erased closure allocated
//! once as well, but also carried a fat pointer and duplicated monomorphized
//! builder code.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::bridge::{
    PyHttpReceive, PyHttpSend, PyWebSocketReceive, PyWebSocketSend, WebSocketInboundEvent,
    WebSocketOutboundEvent, WebSocketSendState,
};
use crate::http::app::{HttpRequestBody, HttpSendState};
use crate::http::scope::{build_http_scope, build_websocket_scope};
use crate::pyloop::Shard;
use crate::runtime::RequestContext;
use crate::websocket::RequestedSubprotocols;

pub(crate) struct BuiltAppCall<'py> {
    pub(crate) scope: Bound<'py, PyDict>,
    pub(crate) receive: Bound<'py, PyAny>,
    pub(crate) send: Bound<'py, PyAny>,
}

pub(crate) struct HttpAppCall {
    ctx: Box<RequestContext>,
    body: HttpRequestBody,
    send_state: HttpSendState,
}

pub(crate) struct WebSocketAppCall {
    ctx: Box<RequestContext>,
    subprotocols: RequestedSubprotocols,
    receive: mpsc::Receiver<WebSocketInboundEvent>,
    send_state: WebSocketSendState,
    send: mpsc::Sender<WebSocketOutboundEvent>,
}

/// The exhaustive Rust-owned argument shapes accepted by the app pump.
/// Boxing keeps the queue record pointer-sized while the enum prevents a
/// mismatched protocol state from entering the invocation path.
pub(crate) enum AppCallArgs {
    Http(HttpAppCall),
    WebSocket(WebSocketAppCall),
}

impl AppCallArgs {
    pub(crate) fn http(
        ctx: Box<RequestContext>,
        body: HttpRequestBody,
        send_state: HttpSendState,
    ) -> Box<Self> {
        Box::new(Self::Http(HttpAppCall {
            ctx,
            body,
            send_state,
        }))
    }

    pub(crate) fn websocket(
        ctx: Box<RequestContext>,
        subprotocols: RequestedSubprotocols,
        receive: mpsc::Receiver<WebSocketInboundEvent>,
        send_state: WebSocketSendState,
        send: mpsc::Sender<WebSocketOutboundEvent>,
    ) -> Box<Self> {
        Box::new(Self::WebSocket(WebSocketAppCall {
            ctx,
            subprotocols,
            receive,
            send_state,
            send,
        }))
    }

    pub(crate) fn build(self, py: Python<'_>, shard: Shard) -> PyResult<BuiltAppCall<'_>> {
        match self {
            Self::Http(call) => call.build(py, shard),
            Self::WebSocket(call) => call.build(py, shard),
        }
    }
}

impl HttpAppCall {
    fn build(self, py: Python<'_>, shard: Shard) -> PyResult<BuiltAppCall<'_>> {
        if let HttpRequestBody::Stream { disconnect, .. } = &self.body {
            // Connection/body ownership may need to cancel this app before it
            // reaches receive(). Publish pump ownership before constructing
            // Python objects so cancellation remains deterministically
            // ordered behind eager-start side effects.
            disconnect.mark_app_started();
        }
        let scope = build_http_scope(py, &self.ctx)?;
        let receive = match self.body {
            HttpRequestBody::NoBody => PyHttpReceive::new_no_body(Arc::clone(&shard)),
            HttpRequestBody::Single(body) => PyHttpReceive::new_single(Arc::clone(&shard), body),
            HttpRequestBody::Stream { rx, disconnect } => {
                PyHttpReceive::new_stream(Arc::clone(&shard), rx, disconnect)
            },
        };
        let receive = Py::new(py, receive)?.into_bound(py).into_any();
        let send = Py::new(py, PyHttpSend::new(shard, self.send_state))?
            .into_bound(py)
            .into_any();
        Ok(BuiltAppCall {
            scope,
            receive,
            send,
        })
    }
}

impl WebSocketAppCall {
    fn build(self, py: Python<'_>, shard: Shard) -> PyResult<BuiltAppCall<'_>> {
        let scope = build_websocket_scope(py, &self.ctx, self.subprotocols.as_ref())?;
        let receive = Py::new(
            py,
            PyWebSocketReceive::new_stream(Arc::clone(&shard), self.receive),
        )?
        .into_bound(py)
        .into_any();
        let send = Py::new(py, PyWebSocketSend::new(shard, self.send_state, self.send))?
            .into_bound(py)
            .into_any();
        Ok(BuiltAppCall {
            scope,
            receive,
            send,
        })
    }
}
