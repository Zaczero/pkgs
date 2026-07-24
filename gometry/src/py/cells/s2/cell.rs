#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::pymethods;
use pyo3::types::{PyBool, PyInt};

use super::*;
use crate::Typed;
use crate::boundary::geographic::validate_lonlat_xy;
use crate::grid::cell::GridCell;
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::cellid::CellId;
use crate::py::cells::cell_ops::{
    cell_boundary, cell_center, cell_children_array, cell_contains, cell_descendant_count,
    cell_hash, cell_intersects, cell_neighbors_array, cell_parent, cell_reduce, cell_richcmp,
};
use crate::py::cells::coverage_ops::CoverageCells;
use crate::py::cells::{GridKind, PyCellArray, construct_s2_cell};
use crate::py::errors::{ParseFormat, parse_error};

pub(crate) fn s2_cell_array(cells: impl IntoIterator<Item = CellId>) -> PyCellArray {
    PyCellArray::from_trusted_ids(
        GridKind::S2Cell,
        cells.into_iter().map(CellId::raw).collect(),
    )
}

pub(crate) fn py_s2_cell_array(cells: &CoverageCells<PyS2Cell>) -> PyCellArray {
    cells.cell_array(GridKind::S2Cell)
}

#[pymethods]
impl PyS2Cell {
    /// One S2 cell from an id, token, lon/lat pair, or point geometry.
    ///
    /// Parameters
    /// ----------
    /// lon : S2Cell, int, str, float, or Point
    ///     A cell id/token, the longitude of a ``lon, lat`` pair, or a point
    ///     geometry.
    ///
    /// lat : float, optional
    ///     Latitude when ``lon`` is a scalar longitude.
    ///
    /// level : int, optional
    ///     S2 level (``0``-``30``); required for coordinate construction.
    ///
    /// Returns
    /// -------
    /// S2Cell
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid S2 cell id or token.
    /// GeometryError
    ///     If ``level`` is out of range.
    /// InvalidGeometryError
    ///     If a scalar coordinate is non-finite or out of range.
    #[new]
    #[pyo3(
        signature = (value, /, lat = None, *, level = None),
        text_signature = "(value, /, lat=None, *, level=None)"
    )]
    fn new(
        value: &Bound<'_, PyAny>,
        lat: Option<&Bound<'_, PyAny>>,
        level: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        construct_s2_cell(value, lat, level)
    }

    /// S2 level of this cell (``0``-``30``).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn level(&self) -> u8 {
        self.cell.depth()
    }

    /// The 64-bit S2 cell id.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn id(&self) -> u64 {
        self.cell.raw()
    }

    /// Compact lowercase hexadecimal S2 cell token.
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
    impl PyS2Cell {
        kind: GridKind::S2Cell,
        class_name: "S2Cell",
        depth: level,
        depth_name: "level",
        parse_depth: parse_s2_level,
        parse_cell: s2_cell_id,
        unpickle: "_unpickle_s2_cell",
        nbytes: std::mem::size_of::<u64>(),
        parent_text_signature: "($self, level=None)",
        children_text_signature: "($self, level=None)",
        neighbors_doc: "The four edge-adjacent cells at this cell's level.",
        candidate_doc: "other : S2Cell, int, or str",
        example_parent: r"
>>> import gometry as gm
>>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
>>> cell.parent(10).token
'808581'
",
        example_children: r"
>>> import gometry as gm
>>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
>>> len(cell.children(13))
4
",
        example_children_count: r"
>>> import gometry as gm
>>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
>>> cell.children_count(13)
4
",
        example_contains: r"
>>> import gometry as gm
>>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
>>> cell.contains(cell.children(13)[0])
True
",
        example_intersects: r"
>>> import gometry as gm
>>> cell = gm.s2_cover(gm.Point(-122.4194, 37.7749, crs=4326), level=12).cells[0]
>>> cell.intersects(cell.parent(10))
True
",
        repr: s2,
        cell_int: |cell| cell.cell.raw(),
    }
}

pub(crate) fn s2_cell_from_xy(lon: f64, lat: f64, level: u8) -> PyResult<PyS2Cell> {
    validate_lonlat_xy(lon, lat)?;
    Ok(PyS2Cell {
        cell: CellId::from_lonlat(lon, lat)
            .parent(level)
            .expect("level validated"),
    })
}

pub(crate) fn s2_cell_id(cell: &Bound<'_, PyAny>) -> PyResult<CellId> {
    if let Ok(cell) = cell.cast_exact::<PyS2Cell>() {
        let cell = cell.get();
        return Ok(cell.cell);
    }
    if cell.cast_exact::<PyBool>().is_err() && cell.cast::<PyInt>().is_ok() {
        let id = cell.extract::<u64>().map_err(|_| {
            parse_error(
                "S2 cell id must be a non-negative 64-bit integer",
                ParseFormat::S2,
            )
        })?;
        return CellId::from_raw(id)
            .ok_or_else(|| parse_error(format!("invalid S2 cell id {id}"), ParseFormat::S2));
    }
    let text = crate::py_text_borrow(
        cell,
        "S2 cell must be an S2Cell, integer id, or string token",
    )?;
    CellId::from_token(text.as_ref())
        .ok_or_else(|| parse_error(format!("invalid S2 cell token {text:?}"), ParseFormat::S2))
}

/// Which pole a cell's lon/lat boundary touches (`Some(true)` north,
/// `Some(false)` south, `None` away from both) — drives forced antimeridian
/// closure so a polar cell dissolves over the pole it actually contains.
pub(crate) fn cell_pole_side(boundary: &Shape) -> Option<bool> {
    const EPS: f64 = 1e-6;
    let bounds = boundary.bounds()?;
    if bounds.maxy() >= 90.0 - EPS {
        Some(true)
    } else if bounds.miny() <= -90.0 + EPS {
        Some(false)
    } else {
        None
    }
}

pub(crate) fn s2_boundary_geometry(cell: CellId) -> PyGeometry {
    PyGeometry::wgs84(S2GeomCell::from_id(cell).boundary_shape())
}
