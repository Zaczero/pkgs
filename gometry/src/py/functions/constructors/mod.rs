#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::*;

mod builders;
mod pyfuncs;

pub(crate) use builders::*;
use pyfuncs::*;

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(
        m;
        box_,
        boxes,
        points,
        line_strings,
        polygons,
        multi_points,
        multi_line_strings,
        multi_polygons
    );
    Ok(())
}
