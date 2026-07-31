// Retained for the WebSocket masking SIMD fast path in
// `websocket::codec::mask`.
// Derives the response-header variant names in `http::types`.
#![feature(macro_metavar_expr_concat)]
#![feature(portable_simd)]

mod access_log;
mod app_call;
mod ascii;
mod async_util;
mod base64;
mod bridge;
mod buffered_events;
mod config;
mod error;
mod h1;
mod h2;
mod h2_frame;
mod hpack;
mod http;
mod inline_fifo;
mod proxy_protocol;
mod pyapi;
mod pyloop;
mod python;
mod runtime;
mod sendfile;
mod server;
mod tls;
mod websocket;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_lib")]
fn init_lib_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<pyapi::LifespanHandoff>()?;
    m.add_class::<pyapi::PreparedTls>()?;
    m.add_function(wrap_pyfunction!(pyapi::prepare_tls, m)?)?;
    m.add_function(wrap_pyfunction!(pyapi::emit_banner, m)?)?;
    m.add_function(wrap_pyfunction!(pyapi::serve_fds, m)?)?;
    Ok(())
}
