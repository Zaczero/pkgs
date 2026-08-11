//! `#[pymethods]` for the typed leaves `Point` and `Polygon`.
//!
//! `PyPoint` ordinate accessors / point CRS-aware ops and `PyPolygon`
//! exterior/interiors; both reach the crate-root model and helpers via
//! `use super::*`, auto-registering with their classes where compiled.

use pyo3::prelude::*;

use crate::{
    EmptyKind, PyGeometry, PyGeometryCollection, PyLineString, PyMultiLineString, PyMultiPoint,
    PyMultiPolygon, PyPoint, PyPolygon, Shape, ShapeData, Typed,
};

mod construct;
mod leaf_accessors;
