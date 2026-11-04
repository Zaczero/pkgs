//! Shared CPython primitives built on PyO3's types and ownership model.
//!
//! Dictionary construction and cached literals avoid repeated allocation;
//! iterator and buffer admission preserve provider errors while owning the
//! native boundary. Callers supply their domain validation and conversion.
//!
//! Runtime Python objects retain their normal interpreter lifetimes. The static
//! literal helpers follow PyO3's single-interpreter module support; dynamic
//! request data belongs to an explicit caller-owned lifetime.

pub mod buffer;
pub mod dict;
pub mod iter;
pub mod literal;
pub mod protocol;
pub mod types;

// Exported macros must resolve the same PyO3 dependency as their implementation.
#[doc(hidden)]
pub use pyo3;
