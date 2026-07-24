#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The GeohashCell class and parsing helpers.

use pyo3::pymethods;
use pyo3::types::PyAny;

use super::super::*;
use crate::Typed;
use crate::grid::cell::GridCell;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::py::cells::cell_ops::{
    cell_boundary, cell_center, cell_children_array, cell_contains, cell_descendant_count,
    cell_hash, cell_intersects, cell_neighbors_array, cell_parent, cell_reduce, cell_richcmp,
};
use crate::py::cells::{GridKind, construct_geohash_cell};
use crate::py::errors::tag_parse_format;

/// One geohash cell: a base-32 character prefix addressing a lon/lat
/// rectangle.
///
/// Wraps the packed cell with typed accessors (``cell.precision``,
/// ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy moves
/// (``parent``/``children``/``neighbors``). Geohash tokens are the public
/// identity — text, not integers. Convert via ``GeohashCell(...)``.
#[pyclass(name = "GeohashCell", module = "gometry", frozen, skip_from_py_object)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyGeohashCell {
    pub(crate) cell: Geohash,
}

crate::heapless!(PyGeohashCell);
#[pyfunction]
pub(super) fn _unpickle_geohash_cell(token: &str) -> PyResult<PyGeohashCell> {
    Ok(PyGeohashCell {
        cell: parse_geohash_token(token)?,
    })
}

pub(crate) fn parse_geohash_token(token: &str) -> PyResult<Geohash> {
    Geohash::parse(token).map_err(|message| {
        tag_parse_format(
            crate::py::errors::ParseError::new_err(message),
            crate::py::errors::ParseFormat::Geohash,
        )
    })
}

/// Parse a cell from an existing GeohashCell or a token string.
pub(crate) fn geohash_cell_arg(cell: &Bound<'_, PyAny>) -> PyResult<Geohash> {
    if let Ok(cell) = cell.cast_exact::<PyGeohashCell>() {
        return Ok(cell.get().cell);
    }
    let text = crate::py_text_borrow(cell, "geohash cell must be a GeohashCell or string token")?;
    parse_geohash_token(text.as_ref())
}

/// Shared i64 → precision conversion (`1..=12`).
pub(crate) fn parse_geohash_precision_value(value: i64) -> PyResult<u8> {
    super::super::checked_depth(
        value,
        "geohash precision",
        "precision",
        1,
        i64::from(GEOHASH_MAX_PRECISION),
    )
}

/// Boundary parser for geohash precision: `1..=12`.
pub(crate) fn parse_geohash_precision(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    parse_geohash_precision_value(crate::py_i64_required("precision", value)?)
}

pub(super) fn geohash_floor(min_precision: i64) -> PyResult<u8> {
    super::super::checked_depth(
        min_precision,
        "geohash min_precision",
        "min_precision",
        1,
        i64::from(GEOHASH_MAX_PRECISION),
    )
}

#[pymethods]
impl PyGeohashCell {
    /// One geohash cell from a token, lon/lat pair, or point geometry.
    ///
    /// Parameters
    /// ----------
    /// lon : GeohashCell, str, float, or Point
    ///     A cell token, the longitude of a ``lon, lat`` pair, or a point
    ///     geometry.
    ///
    /// lat : float, optional
    ///     Latitude when ``lon`` is a scalar longitude.
    ///
    /// precision : int, optional
    ///     Geohash precision (``1``-``12``); required for coordinate
    ///     construction.
    ///
    /// Returns
    /// -------
    /// GeohashCell
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid geohash token.
    /// GeometryError
    ///     If ``precision`` is out of range.
    /// InvalidGeometryError
    ///     If a scalar coordinate is non-finite or out of range.
    #[new]
    #[pyo3(
        signature = (value, /, lat = None, *, precision = None),
        text_signature = "(value, /, lat=None, *, precision=None)"
    )]
    fn new(
        value: &Bound<'_, PyAny>,
        lat: Option<&Bound<'_, PyAny>>,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        construct_geohash_cell(value, lat, precision)
    }

    /// Geohash precision of this cell (``1``-``12`` characters).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn precision(&self) -> u8 {
        self.cell.depth()
    }

    /// The base-32 geohash string (lowercase).
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn token(&self) -> String {
        self.cell.token()
    }
}

grid_cell_common_pymethods! {
    impl PyGeohashCell {
        kind: GridKind::GeohashCell,
        class_name: "GeohashCell",
        depth: precision,
        depth_name: "precision",
        parse_depth: parse_geohash_precision,
        parse_cell: geohash_cell_arg,
        unpickle: "_unpickle_geohash_cell",
        nbytes: std::mem::size_of::<u64>(),
        parent_text_signature: "($self, precision=None)",
        children_text_signature: "($self, precision=None)",
        neighbors_doc: "The surrounding cells at this precision (8, fewer at the poles), row-major from the north-west; east-west wraps the antimeridian.",
        candidate_doc: "other : GeohashCell or str",
        example_parent: r"
>>> import gometry as gm
>>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
>>> cell.parent().token
'9q8yy'
",
        example_children: r"
>>> import gometry as gm
>>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
>>> len(cell.children())
32
",
        example_children_count: r"
>>> import gometry as gm
>>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
>>> cell.children_count()
32
",
        example_contains: r"
>>> import gometry as gm
>>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
>>> cell.contains(cell)
True
",
        example_intersects: r"
>>> import gometry as gm
>>> cell = gm.geohash_cover(gm.Point(-122.4194, 37.7749, crs=4326), precision=6).cells[0]
>>> cell.intersects(cell.parent())
True
",
        match_arg: "token",
        repr: geohash,
    }
}
