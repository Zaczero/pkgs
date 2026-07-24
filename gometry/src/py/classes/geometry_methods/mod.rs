#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The `Geometry` Python method surface (`#[pymethods] impl PyGeometry`).
//!
//! Extracted from the crate root as one block (PyO3 allows a single
//! `#[pymethods]` impl per type). Reaches the crate-root geometry model,
//! helpers, and sibling types via `use super::*`; the methods auto-register
//! with the `PyGeometry` class wherever the impl is compiled.
//! Unary method dedup lives in `crate::py::methods` (constructive,
//! transform, linref, ordinate, io serialization).

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyTuple};

use crate::py::errors::{CRSError, InvalidGeometryError};
use crate::*;

mod model;
pub(crate) use model::*;

mod methods_core;
mod methods_frame;
mod methods_io;
mod ops;
