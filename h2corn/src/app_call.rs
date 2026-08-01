//! Closed ownership states for one ASGI application invocation.
//!
//! HTTP and WebSocket calls move different Rust resources onto the Python
//! event-loop thread. The public constructors are the only way to create a
//! call state, so protocol resources cannot be crossed accidentally. The
//! enum is boxed once at the queue boundary, keeping the record pointer-sized
//! without a fat pointer or duplicated monomorphized builder code.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::bridge::{
    PyHttpReceive, PyHttpSend, PyWebSocketReceive, PyWebSocketSend, WebSocketInboundReceiver,
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
    receive: WebSocketInboundReceiver,
    send_state: WebSocketSendState,
    send: mpsc::Sender<WebSocketOutboundEvent>,
}

/// The exhaustive Rust-owned argument shapes accepted by the app pump.
/// Boxing keeps the queue record pointer-sized while the enum prevents a
/// mismatched protocol state from entering the invocation path.
///
/// Measured: removing this outer box is a wash (+0.05 % instructions per
/// request, inside a 0.2 % spread) — the malloc/free it saves is paid back
/// copying the enum through the queue twice — and it would double
/// `PumpEvent` to 64 bytes for every event, including the small ones.
///
/// The WebSocket state is boxed separately: it is three times the size of an
/// HTTP call, and an unboxed variant would make every HTTP request allocate
/// for state only a WebSocket upgrade uses. A WebSocket pays one extra
/// allocation, once per connection.
pub(crate) enum AppCallArgs {
    Http(HttpAppCall),
    WebSocket(Box<WebSocketAppCall>),
}

impl AppCallArgs {
    #[expect(
        clippy::unnecessary_box_returns,
        reason = "the box is the queue record: it keeps PumpEvent at 32 bytes, and \
                  removing it measured as a wash (see the type comment)"
    )]
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

    #[expect(
        clippy::unnecessary_box_returns,
        reason = "the box is the queue record: it keeps PumpEvent at 32 bytes, and \
                  removing it measured as a wash (see the type comment)"
    )]
    pub(crate) fn websocket(
        ctx: Box<RequestContext>,
        subprotocols: RequestedSubprotocols,
        receive: WebSocketInboundReceiver,
        send_state: WebSocketSendState,
        send: mpsc::Sender<WebSocketOutboundEvent>,
    ) -> Box<Self> {
        Box::new(Self::WebSocket(Box::new(WebSocketAppCall {
            ctx,
            subprotocols,
            receive,
            send_state,
            send,
        })))
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
            HttpRequestBody::NoBody => {
                PyHttpReceive::new_no_body(Arc::clone(&shard), self.send_state.clone())
            },
            HttpRequestBody::Single(body) => {
                PyHttpReceive::new_single(Arc::clone(&shard), body, self.send_state.clone())
            },
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
        let send = Py::new(
            py,
            PyWebSocketSend::new(shard, self.send_state, self.send, self.subprotocols),
        )?
        .into_bound(py)
        .into_any();
        Ok(BuiltAppCall {
            scope,
            receive,
            send,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AppCallArgs, HttpAppCall};

    /// Every HTTP request allocates one `AppCallArgs`, and an enum is as large
    /// as its largest variant — so a WebSocket-only field growing unboxed
    /// would silently enlarge every HTTP request's allocation.
    #[test]
    fn app_call_args_stay_sized_for_http() {
        assert!(
            size_of::<AppCallArgs>() <= size_of::<HttpAppCall>() + size_of::<usize>(),
            "AppCallArgs is {} bytes for a {}-byte HTTP call; box the larger variant",
            size_of::<AppCallArgs>(),
            size_of::<HttpAppCall>(),
        );
    }
}
