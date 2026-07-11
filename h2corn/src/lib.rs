// Retained for the WebSocket masking SIMD fast path in
// `websocket::codec::mask`.
#![feature(portable_simd)]

mod access_log;
mod app_call;
mod ascii;
mod async_util;
mod bridge;
mod buffered_events;
mod config;
mod error;
mod h1;
mod h2;
mod h2_frame;
mod hpack;
mod http;
mod proxy_protocol;
mod pyapi;
mod pyloop;
mod python;
mod runtime;
mod sendfile;
mod server;
mod smallvec_deque;
mod tls;
mod websocket;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_lib")]
fn h2corn_lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(pyapi::emit_banner, m)?)?;
    m.add_function(wrap_pyfunction!(pyapi::validate_config, m)?)?;
    m.add_function(wrap_pyfunction!(pyapi::serve_fds, m)?)?;
    Ok(())
}
