#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::{PyIndexError, PyTypeError};

use crate::geometry::LineSeq;
use crate::py::cells::{
    Bound, GridKind, Point, Py, PyAny, PyAnyMethods as _, PyCellArray, PyErr, PyResult,
    PySliceMethods as _, Python, Shape, lonlat_shape,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;
use crate::{
    PointRows, PyGeometry, PyGeometryArray, Typed, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, py_bool, validate_lonlat_shape,
};

pub(super) fn uncompact_budget_err(err: crate::grid::UncompactBudgetExceeded) -> PyErr {
    GeometryError::new_err(err.to_string())
}

pub(super) fn cell_limit_err(err: crate::grid::CellLimitExceeded) -> PyErr {
    GeometryError::new_err(err.to_string())
}

pub(super) fn coverage_explain(
    system: &str,
    depth: &str,
    rule: super::CellRule,
    visible_len: usize,
    member_noun: &str,
    interior_len: usize,
    outer_len: usize,
) -> Vec<String> {
    vec![
        format!(
            "{system} coverage: {depth}, cell_rule {}, {visible_len} {member_noun}",
            rule.token()
        ),
        format!(
            "coverage partition: {interior_len} interior {member_noun}, {} boundary {member_noun}",
            outer_len - interior_len
        ),
        "membership predicates: exact source-geometry predicates; cell partitions are retained for inspection and derived coverages".to_owned(),
    ]
}

/// Collect one cell identity or any iterable of identities.
///
/// Strings are atomic grid tokens, and non-iterable cell objects / integer
/// ids are singleton sets. This keeps the set algebra pleasant for the common
/// one-cell case without weakening per-grid validation in the caller.
pub(super) fn cell_items<'py>(cells: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if cells.is_instance_of::<pyo3::types::PyBytes>()
        || cells.is_instance_of::<pyo3::types::PyByteArray>()
        || (cells.is_instance_of::<pyo3::types::PyMemoryView>()
            && matches!(
                cells.getattr("format")?.extract::<String>()?.as_str(),
                "B" | "b" | "c"
            ))
    {
        return Err(PyTypeError::new_err(
            "byte payloads are not cell collections; use payload.decode() for one textual token or a list/uint64 array for numeric ids",
        ));
    }
    if cells.cast::<pyo3::types::PyString>().is_ok() {
        return Ok(vec![cells.clone()]);
    }
    // Fallible growth (D10): `h3_union(itertools.repeat(cell))` etc. must
    // MemoryError rather than hang / capacity-overflow abort.
    match crate::collect_py_iter(cells, Ok) {
        Ok(items) => Ok(items),
        Err(error) if error.is_instance_of::<PyTypeError>(cells.py()) => Ok(vec![cells.clone()]),
        Err(error) => Err(error),
    }
}

/// Resolve a bounding-cell query into validated lon/lat bounds: a Geometry
/// or `GeometryArray` (grid input policy — non-WGS84 frames reproject), or a
/// bare ``(minx, miny, maxx, maxy)`` lon/lat iterable.
pub(super) fn bounding_query_bounds(value: &Bound<'_, PyAny>) -> PyResult<crate::geometry::Bounds> {
    use crate::geometry::Bounds;
    if let Some(geometry) = exact_geometry(value) {
        let shape = lonlat_shape(geometry)?;
        validate_lonlat_shape(&shape)?;
        return shape.bounds().ok_or_else(|| {
            InvalidGeometryError::new_err("bounding cell requires a non-empty geometry")
        });
    }
    if let Some(array) = exact_geometry_array(value) {
        let crs = array.crs_str();
        let mut folded: Option<Bounds> = None;
        for (row, shape) in array.storage().iter_shapes().enumerate() {
            if array.is_row_missing(row) {
                continue;
            }
            let shape = crate::lonlat_shape_under(&shape, crs)?;
            validate_lonlat_shape(&shape)?;
            if let Some(bounds) = shape.bounds() {
                folded = Some(folded.map_or(bounds, |mut acc| {
                    acc.include_bounds(bounds);
                    acc
                }));
            }
        }
        return folded.ok_or_else(|| {
            InvalidGeometryError::new_err("bounding cell requires a non-empty geometry")
        });
    }
    if let Ok(values) = crate::coordinate_values(value.py(), value, "bounds")
        && let [minx, miny, maxx, maxy] = values.as_slice()
    {
        if !(minx <= maxx && miny <= maxy) {
            return Err(GeometryError::new_err(
                "bounds must be ordered (minx <= maxx, miny <= maxy)",
            ));
        }
        crate::boundary::geographic::validate_lonlat_xy(*minx, *miny)?;
        crate::boundary::geographic::validate_lonlat_xy(*maxx, *maxy)?;
        return Ok(Bounds::new_unchecked(*minx, *miny, *maxx, *maxy));
    }
    Err(PyTypeError::new_err(
        "expected a Geometry, GeometryArray, or (minx, miny, maxx, maxy) bounds",
    ))
}

/// Apply an exact membership `test` to a Geometry or `GeometryArray` of
/// candidates, normalized by the grid input policy (WGS84 and CRS-free
/// lon/lat pass through; any other CRS is reprojected).
pub(super) fn coverage_members(
    values: &Bound<'_, PyAny>,
    test: impl Fn(&Shape) -> PyResult<bool>,
    test_point: impl Fn(Point) -> PyResult<bool>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        if let Some(geometry) = exact_geometry(values) {
            let candidate = lonlat_shape(geometry)?;
            return Ok(py_bool(py, test(&candidate)?));
        }
        if let Some(array) = exact_geometry_array(values) {
            // Lon/lat point arrays skip per-row wrapper materialization and
            // shape cloning: straight from the packed columns to the point
            // test (range validity is checked inside the point kernels).
            let lonlat_frame = array.crs_str().is_none_or(crate::crs::is_wgs84_lonlat);
            if lonlat_frame && let Some(points) = array.storage().point_rows() {
                let result = points
                    .iter()
                    .enumerate()
                    .map(|(row, point)| {
                        if array.is_row_missing(row) {
                            return Ok(false);
                        }
                        test_point(point)
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                return bool_array(py, result);
            }
            let crs = array.crs_str();
            let result = array
                .storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| {
                    if array.is_row_missing(row) {
                        return Ok(false);
                    }
                    crate::lonlat_shape_under(&shape, crs).and_then(|candidate| test(&candidate))
                })
                .collect::<PyResult<Vec<_>>>()?;
            return bool_array(py, result);
        }
        Err(expected_geometry_or_array())
    })
}

pub(crate) enum GridPointRows<'a> {
    Source(PointRows<'a>),
    Columns { xs: Vec<f64>, ys: Vec<f64> },
}

impl GridPointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Source(points) => points.len(),
            Self::Columns { xs, .. } => xs.len(),
        }
    }

    pub(crate) fn get(&self, row: usize) -> Point {
        match self {
            Self::Source(points) => points.get(row),
            Self::Columns { xs, ys } => Point::new_unchecked_xy(xs[row], ys[row]),
        }
    }
}

/// Resolve every row of `array` to a validated lon/lat `Point`. The frame is
/// classified once; a non-lonlat frame reprojects all rows in ONE batched
/// transform (instead of one PROJ round trip per row), and packed point
/// arrays gather straight off their shared column.
pub(crate) fn grid_lonlat_points(array: &PyGeometryArray) -> PyResult<GridPointRows<'_>> {
    let points = array
        .storage()
        .point_rows()
        .ok_or_else(|| crate::py::errors::geometry_type_err("expected Point geometry"))?;
    let lonlat_frame = array.crs_str().is_none_or(crate::crs::is_wgs84_lonlat);
    if lonlat_frame || points.is_empty() {
        // Skip lon/lat validation on missing rows (NaN placeholders) — factories
        // carry them as a missing mask instead of rejecting the whole batch.
        for (row, point) in points.iter().enumerate() {
            if array.is_row_missing(row) {
                continue;
            }
            crate::boundary::geographic::validate_lonlat_point(point)?;
        }
        return Ok(GridPointRows::Source(points));
    }
    let crs = array.crs_str().expect("non-lonlat frame carries a CRS");
    // Grid cells are 2-D (lon/lat), so keep independent X/Y columns through
    // the batched transform. This avoids the previous AoS -> CoordSeq -> Shape
    // round-trip; only the final `PointRows` interchange materializes Points.
    let mut xs = Vec::with_capacity(points.len());
    let mut ys = Vec::with_capacity(points.len());
    for point in points.iter() {
        xs.push(point.x);
        ys.push(point.y);
    }
    // Transform present rows only: missing placeholders may be non-finite and
    // must not trip domain validation on the identity/real path.
    let present: Vec<usize> = (0..points.len())
        .filter(|&row| !array.is_row_missing(row))
        .collect();
    if !present.is_empty() {
        let mut px: Vec<f64> = present.iter().map(|&r| xs[r]).collect();
        let mut py: Vec<f64> = present.iter().map(|&r| ys[r]).collect();
        crate::crs::Transformer::new(crs, "EPSG:4326").transform_coordinates(
            &mut px,
            &mut py,
            crate::Zt::None,
        )?;
        for (i, &row) in present.iter().enumerate() {
            crate::boundary::geographic::validate_lonlat_xy(px[i], py[i])?;
            xs[row] = px[i];
            ys[row] = py[i];
        }
    }
    Ok(GridPointRows::Columns { xs, ys })
}

/// Scalar/array coverage dispatch shared by all four grid families.
///
/// A scalar geometry keeps the rich coverage object. An array returns one
/// ragged cell row per input geometry; missing rows become empty groups so
/// source-row association is preserved.
pub(crate) fn grid_cover_dispatch<'py, C>(
    py: Python<'py>,
    geom: &Bound<'py, PyAny>,
    kind: GridKind,
    max_cells: Option<usize>,
    build: impl Fn(&PyGeometry) -> PyResult<C>,
    cell_ids: impl Fn(&C) -> Vec<u64>,
) -> PyResult<Py<PyAny>>
where
    C: pyo3::IntoPyObjectExt<'py>,
{
    if let Some(geometry) = exact_geometry(geom) {
        return build(geometry)?.into_py_any(py);
    }
    let Some(array) = exact_geometry_array(geom) else {
        return Err(expected_geometry_or_array());
    };
    let crs = array.crs_str();
    let mut produced = 0_usize;
    let rows = array
        .masked_shape_rows()
        .map(|(missing, shape)| {
            if missing {
                return Ok(Vec::new());
            }
            let shape = crate::lonlat_shape_under(&shape, crs)?.into_owned();
            let coverage = build(&PyGeometry::wgs84(shape))?;
            let ids = cell_ids(&coverage);
            produced = checked_cover_batch_len(produced, ids.len(), max_cells)?;
            Ok(ids)
        })
        .collect::<PyResult<Vec<_>>>()?;
    crate::py::vectors::Groups::from_cell_rows(kind, rows)?.into_py_any(py)
}

fn checked_cover_batch_len(
    produced: usize,
    row_len: usize,
    max_cells: Option<usize>,
) -> PyResult<usize> {
    let produced = produced.saturating_add(row_len);
    crate::grid::ensure_cover_budget(produced, max_cells)
        .map_err(crate::py::cells::coverage_ops::cover_budget_err)?;
    Ok(produced)
}

pub(super) fn lonlat_point_geometry(point: Point) -> PyGeometry {
    PyGeometry::wgs84(Shape::Point(point))
}

// --- Shared coverage dunder engines (S2/H3 mirror one cell-list shape) ---

/// Python-negative-index lookup over a coverage's cell list, with the
/// coverage's own out-of-range message.
/// Coverage ``__getitem__``: an ``int`` returns one cell, a ``slice`` returns a
/// `CellArray` of cells (the coverages are finite Python sequences).
pub(super) fn coverage_getitem<'py, C>(
    py: Python<'py>,
    cells: &crate::py::cells::coverage_ops::CoverageCells<C>,
    index: &Bound<'py, PyAny>,
    message: &'static str,
    kind: GridKind,
) -> PyResult<Py<PyAny>>
where
    C: crate::py::cells::coverage_ops::CoverageCell + pyo3::IntoPyObjectExt<'py>,
{
    let len = cells.len() as isize;
    if let Ok(slice) = index.cast::<pyo3::types::PySlice>() {
        let indices = slice.indices(len)?;
        let mut ids = Vec::new();
        let mut i = indices.start;
        while (indices.step > 0 && i < indices.stop) || (indices.step < 0 && i > indices.stop) {
            ids.push(crate::py::cells::coverage_ops::CoverageCell::coverage_id(
                cells.get(i as usize).expect("slice index is in bounds"),
            ));
            i += indices.step;
        }
        return PyCellArray::from_trusted_ids(kind, ids).into_py_any(py);
    }
    let index: isize = index.extract()?;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        return Err(PyIndexError::new_err(message));
    }
    cells
        .get(normalized as usize)
        .expect("normalized index is in bounds")
        .into_py_any(py)
}

/// Dissolve a coverage's per-cell boundary polygons into one outline geometry
/// (shared cell edges removed), tagged OGC:CRS84. Single-part results collapse
/// to a plain `Polygon` (the buffer output-seam convention).
pub(super) fn coverage_to_polygon(shapes: &[Shape]) -> PyResult<Typed> {
    if shapes.is_empty() {
        return Err(GeometryError::new_err(
            "a coverage needs at least one cell to dissolve into a polygon",
        ));
    }
    let dissolved = Shape::union_all(shapes, crate::geometry::Strictness::Lenient)?;
    let shape = match dissolved {
        Shape::MultiPolygon(mut polygons) if polygons.len() == 1 => {
            Shape::Polygon(polygons.pop().expect("one polygon"))
        },
        shape => shape,
    };
    Ok(PyGeometry::typed_wgs84(shape))
}

/// Topology-driven dissolve for a uniform-depth rect-grid coverage (geohash /
/// tiles): an edge is interior iff the cell across it is in the set, so only
/// outline edges are emitted and fully-interior cells cost no geometry. Returns
/// `None` to defer to the union fallback — for a mixed-depth set (where
/// same-depth adjacency can't cancel) or a degenerate outline. `cells` is
/// sorted by `C`'s `Ord` (every coverage constructor sorts), so membership is a
/// binary search. The antimeridian and geohash pole rows are emitted as
/// boundary edges (`edge_neighbors` returns `None` there), so the planar
/// polygonize splits the outline at ±180 / closes the lat=±90 edge without a
/// special case.
pub(super) fn rect_dissolve<C: crate::grid::coverer::RectCell>(
    cells: &[C],
) -> PyResult<Option<Typed>> {
    let Some(first) = cells.first() else {
        return Ok(None);
    };
    let depth = first.depth();
    if !cells.iter().all(|cell| cell.depth() == depth) {
        return Ok(None);
    }
    let mut outline: Vec<crate::geometry::CoordSeq> = Vec::with_capacity(cells.len() * 4);
    for cell in cells {
        let bounds = cell.bounds();
        // CCW corners: (minx,miny) -> (maxx,miny) -> (maxx,maxy) -> (minx,maxy).
        let corners = [
            Point::new_unchecked_xy(bounds.minx(), bounds.miny()),
            Point::new_unchecked_xy(bounds.maxx(), bounds.miny()),
            Point::new_unchecked_xy(bounds.maxx(), bounds.maxy()),
            Point::new_unchecked_xy(bounds.minx(), bounds.maxy()),
        ];
        // edge_neighbors() is [south, east, north, west], matching side `k` =
        // corners[k] -> corners[k+1].
        for (side, neighbor) in cell.edge_neighbors().into_iter().enumerate() {
            let interior = neighbor.is_some_and(|across| cells.binary_search(&across).is_ok());
            if !interior {
                outline.push(crate::geometry::CoordSeq::from_points(&[
                    corners[side],
                    corners[(side + 1) % 4],
                ]));
            }
        }
    }
    let polygons: Vec<crate::geometry::Polygon> = Shape::build_area_all(
        &[&Shape::MultiLineString(
            outline.into_iter().map(LineSeq::from_trusted).collect(),
        )],
        true,
    )?
    .into_iter()
    .filter_map(|shape| match shape {
        Shape::Polygon(polygon) => Some(polygon),
        _ => None,
    })
    .collect();
    if polygons.is_empty() {
        return Ok(None);
    }
    let shape = if polygons.len() == 1 {
        Shape::Polygon(polygons.into_iter().next().expect("one polygon"))
    } else {
        Shape::MultiPolygon(polygons)
    };
    Ok(Some(PyGeometry::typed_wgs84(shape)))
}

/// Dissolve an arbitrary slice of rect-grid cells (geohash / tiles) into one
/// outline geometry — the free-function twin of the coverage `to_polygon`,
/// shared by `geohash_to_polygon` / `tiles_to_polygon`. Free-function input is
/// arbitrary, so it is sorted and deduped first (the topology fast path needs
/// sorted membership and one entry per cell); then [`rect_dissolve`] runs the
/// uniform-depth edge-cancel pass, falling back to the antimeridian-aware
/// boundary union for mixed-depth or degenerate sets. Empty input flows to
/// `coverage_to_polygon`, which raises the documented `GeometryError`.
pub(super) fn rect_cells_to_polygon<C: crate::grid::coverer::RectCell>(
    mut cells: Vec<C>,
) -> PyResult<Typed> {
    cells.sort_unstable();
    cells.dedup();
    if let Some(typed) = rect_dissolve(&cells)? {
        return Ok(typed);
    }
    let shapes: Vec<Shape> = cells
        .iter()
        .map(|cell| {
            let shape = crate::geometry::bounds_to_shape(cell.bounds());
            shape.split_antimeridian()
        })
        .collect::<crate::error::Result<_>>()?;
    coverage_to_polygon(&shapes)
}

/// Whether two coverages share the same source geometry frame and shape.
pub(super) fn coverage_geometry_eq(left: &PyGeometry, right: &PyGeometry) -> bool {
    left.crs_ref() == right.crs_ref() && left.epoch() == right.epoch() && left.shape == right.shape
}

/// Hash key for a coverage's source geometry (CRS, epoch, shape).
pub(super) fn coverage_geometry_hash(geometry: &PyGeometry) -> u64 {
    crate::collections::python_hash(&(
        geometry.crs_ref(),
        geometry.epoch().map(f64::to_bits),
        &geometry.shape,
    ))
}
use pyo3::IntoPyObjectExt as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_cover_budget_is_cumulative_and_overflow_safe() {
        // `PyErr` rendering touches the interpreter even in a pure-Rust test.
        pyo3::Python::initialize();
        let limit = crate::grid::GRID_MAX_CELLS;
        assert_eq!(
            checked_cover_batch_len(limit - 1, 1, Some(limit)).expect("at budget"),
            limit
        );
        let over = checked_cover_batch_len(limit, 1, Some(limit))
            .expect_err("combined rows exceed budget");
        assert!(over.to_string().contains("max_cells"));
        // Unlimited: no rejection even for a huge cumulative count.
        assert_eq!(
            checked_cover_batch_len(usize::MAX - 1, 1, None).expect("unlimited"),
            usize::MAX
        );
    }
}
