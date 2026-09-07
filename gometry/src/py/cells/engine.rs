#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::array::PackedStorage;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyTypeError;

use crate::py::cells::{
    Bound, GridKind, Point, Py, PyAny, PyAnyMethods as _, PyCellArray, PyErr, PyResult, Python,
    Shape, lonlat_shape,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::{
    PointRows, PyGeometry, PyGeometryArray, Typed, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, validate_lonlat_shape,
};

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
    // MemoryError rather than hang / capacity-overflow abort. A scalar is
    // recognized from the absence of both iterable protocols; an exception
    // raised by a supplied protocol belongs to that provider.
    let iter = match cells.try_iter() {
        Ok(iter) => iter,
        Err(error)
            if error.is_instance_of::<PyTypeError>(cells.py())
                && cells
                    .getattr_opt(pyo3::intern!(cells.py(), "__iter__"))?
                    .is_none()
                && cells
                    .getattr_opt(pyo3::intern!(cells.py(), "__getitem__"))?
                    .is_none() =>
        {
            return Ok(vec![cells.clone()]);
        },
        Err(error) => return Err(error),
    };
    crate::collect_py_iter_from_iter(cells, iter, Ok)
}

/// Resolve a bounding-cell query into validated lon/lat bounds: a Geometry
/// or `GeometryArray` (grid input policy — non-WGS84 frames reproject), or a
/// bare ``(minx, miny, maxx, maxy)`` lon/lat iterable.
pub(super) fn bounding_query_bounds(value: &Bound<'_, PyAny>) -> PyResult<crate::geometry::Bounds> {
    Ok(bounding_query_region(value)?.bounds)
}

/// The complete geographic extent used by a single-cell query.
///
/// A west-of-east extent is a narrow antimeridian window, not the 358 degree
/// planar envelope.  Keeping that fact beside the bounds prevents each grid
/// family from silently choosing a different seam interpretation.
#[derive(Clone, Copy, Debug)]
pub(super) struct BoundingQueryRegion {
    pub(super) bounds: crate::geometry::Bounds,
    pub(super) crosses_antimeridian: bool,
}

pub(super) fn bounding_query_region(value: &Bound<'_, PyAny>) -> PyResult<BoundingQueryRegion> {
    use crate::geometry::Bounds;
    if let Some(geometry) = exact_geometry(value) {
        let shape = lonlat_shape(geometry)?;
        validate_lonlat_shape(&shape)?;
        let bounds = if shape.crosses_antimeridian() {
            crate::geometry::geographic_crossing_bounds(&shape)
        } else {
            shape.bounds()
        };
        return bounds.map_or_else(|| Err(InvalidGeometryError::new_err(
                "bounding cell requires a non-empty geometry",
            )), |bounds| Ok(BoundingQueryRegion {
                bounds,
                crosses_antimeridian: shape.crosses_antimeridian(),
            }));
    }
    if let Some(array) = exact_geometry_array(value) {
        let crs = array.crs_str();
        let mut shapes = Vec::new();
        for (_, shape) in array.present_shape_rows() {
            let shape = crate::lonlat_shape_under(&shape, crs)?;
            validate_lonlat_shape(&shape)?;
            if !shape.is_empty() {
                crate::try_push(&mut shapes, shape.into_owned())?;
            }
        }
        let shape = match shapes.len() {
            0 => {
                return Err(InvalidGeometryError::new_err(
                    "bounding cell requires a non-empty geometry",
                ));
            },
            1 => shapes.pop().expect("one shape"),
            _ => Shape::geometry_collection(shapes)?,
        };
        let bounds = if shape.crosses_antimeridian() {
            crate::geometry::geographic_crossing_bounds(&shape)
        } else {
            shape.bounds()
        };
        return bounds.map_or_else(|| Err(InvalidGeometryError::new_err(
                "bounding cell requires a non-empty geometry",
            )), |bounds| Ok(BoundingQueryRegion {
                bounds,
                crosses_antimeridian: shape.crosses_antimeridian(),
            }));
    }
    let values = match crate::parameter_values_exact(value.py(), value, "bounds", 4, |got| {
        GeometryError::new_err(format!("bounds must contain exactly 4 values, got {got}"))
    }) {
        Ok(values) => values,
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__iter__"))?
                    .is_none()
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__getitem__"))?
                    .is_none() =>
        {
            return Err(PyTypeError::new_err(
                "expected a Geometry, GeometryArray, or (minx, miny, maxx, maxy) bounds",
            ));
        },
        Err(error) => return Err(error),
    };
    if let [minx, miny, maxx, maxy] = values.as_slice() {
        if !(minx <= maxx && miny <= maxy) {
            return Err(GeometryError::new_err(
                "bounds must be ordered (minx <= maxx, miny <= maxy)",
            ));
        }
        crate::boundary::geographic::validate_lonlat_xy(*minx, *miny)?;
        crate::boundary::geographic::validate_lonlat_xy(*maxx, *maxy)?;
        return Ok(BoundingQueryRegion {
            bounds: Bounds::new_raw(*minx, *miny, *maxx, *maxy),
            crosses_antimeridian: false,
        });
    }
    Err(PyTypeError::new_err(
        "expected a Geometry, GeometryArray, or (minx, miny, maxx, maxy) bounds",
    ))
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
            Self::Columns { xs, ys } => Point::new_raw_xy(xs[row], ys[row]),
        }
    }
}

/// Resolve every row of `array` to a validated lon/lat `Point`. The frame is
/// classified once; a non-lonlat frame reprojects all rows in ONE batched
/// transform (instead of one PROJ round trip per row), and packed point
/// arrays gather straight off their shared column.
pub(crate) fn grid_lonlat_points(array: &PyGeometryArray) -> PyResult<GridPointRows<'_>> {
    if array.is_compact_nullable() {
        if !matches!(array.storage(), crate::GeometryArrayStorage::Packed(PackedStorage::Points { .. })) {
            return Err(crate::py::errors::geometry_type_err(
                "expected Point geometry",
            ));
        }
        // A frame is lon/lat when it has no CRS or a WGS 84 one; any other CRS
        // is the one to reproject from.
        let projected_crs = array.crs_str().filter(|crs| !crate::crs::is_wgs84_lonlat(crs));
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        let mut present = Vec::new();
        crate::try_reserve_pair(&mut xs, &mut ys, array.len()).map_err(PyErr::from)?;
        crate::try_reserve_vec(&mut present, array.len()).map_err(PyErr::from)?;
        for logical_row in array
            .row_operation()
            .rows_with_missing(|| (f64::NAN, f64::NAN))
        {
            match logical_row {
                crate::array::ArrayRow::Missing { value, .. } => {
                    crate::try_push_pair(&mut xs, &mut ys, value.0, value.1)
                        .map_err(PyErr::from)?;
                },
                crate::array::ArrayRow::Present(row) => {
                    let logical = row.logical();
                    let crate::array::ShapeRow::Point(point) = row.row() else {
                        unreachable!("compact point storage yields point rows")
                    };
                    crate::try_push_pair(&mut xs, &mut ys, point.x, point.y)
                        .map_err(PyErr::from)?;
                    crate::try_push(&mut present, logical)?;
                },
            }
        }
        let Some(crs) = projected_crs else {
            for &logical in &present {
                crate::boundary::geographic::validate_lonlat_xy(xs[logical], ys[logical])?;
            }
            return Ok(GridPointRows::Columns { xs, ys });
        };
        let mut px = Vec::new();
        let mut py = Vec::new();
        crate::try_reserve_pair(&mut px, &mut py, present.len()).map_err(PyErr::from)?;
        for &logical in &present {
            crate::try_push_pair(&mut px, &mut py, xs[logical], ys[logical])
                .map_err(PyErr::from)?;
        }
        crate::crs::TransformPlan::new(crs, "EPSG:4326")?.transform_coordinates(
            &mut px,
            &mut py,
            crate::Zt::None,
        )?;
        for (index, &logical) in present.iter().enumerate() {
            crate::boundary::geographic::validate_lonlat_xy(px[index], py[index])?;
            xs[logical] = px[index];
            ys[logical] = py[index];
        }
        return Ok(GridPointRows::Columns { xs, ys });
    }
    let points = array
        .storage()
        .point_rows()
        .ok_or_else(|| crate::py::errors::geometry_type_err("expected Point geometry"))?;
    let projected_crs = array.crs_str().filter(|crs| !crate::crs::is_wgs84_lonlat(crs));
    let crs = match projected_crs {
        Some(crs) if !points.is_empty() => crs,
        _ => {
            // Skip lon/lat validation on missing rows (NaN placeholders) —
            // factories carry them as a missing mask instead of rejecting the
            // whole batch.
            for (row, point) in points.iter().enumerate() {
                if array.is_row_missing(row) {
                    continue;
                }
                crate::boundary::geographic::validate_lonlat_point(point)?;
            }
            return Ok(GridPointRows::Source(points));
        },
    };
    // Grid cells are 2-D (lon/lat), so keep independent X/Y columns through
    // the batched transform. This avoids the previous AoS -> CoordSeq -> Shape
    // round-trip; only the final `PointRows` interchange materializes Points.
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, points.len()).map_err(PyErr::from)?;
    for point in points.iter() {
        crate::try_push_pair(&mut xs, &mut ys, point.x, point.y).map_err(PyErr::from)?;
    }
    // Transform present rows only: missing placeholders may be non-finite and
    // must not trip domain validation on the identity/real path.
    let mut present = crate::try_vec_with_capacity(points.len())?;
    for row in 0..points.len() {
        if !array.is_row_missing(row) {
            crate::try_push(&mut present, row)?;
        }
    }
    if !present.is_empty() {
        let mut px = Vec::new();
        let mut py = Vec::new();
        crate::try_reserve_pair(&mut px, &mut py, present.len()).map_err(PyErr::from)?;
        for &row in &present {
            crate::try_push_pair(&mut px, &mut py, xs[row], ys[row]).map_err(PyErr::from)?;
        }
        crate::crs::TransformPlan::new(crs, "EPSG:4326")?.transform_coordinates(
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
/// A scalar returns a flat cell array. An array returns one ragged cell row per
/// input geometry; missing rows become empty groups so source-row association
/// is preserved.
pub(crate) fn grid_cover_dispatch<'py>(
    py: Python<'py>,
    geom: &Bound<'py, PyAny>,
    kind: GridKind,
    max_cells: Option<usize>,
    build: impl Fn(&PyGeometry, Option<usize>) -> PyResult<Vec<u64>> + Send,
) -> PyResult<Py<PyAny>> {
    if let Some(geometry) = exact_geometry(geom) {
        let geometry = geometry.clone();
        let coverage = py.detach(move || build(&geometry, max_cells))?;
        return PyCellArray::from_ids(kind, coverage).into_py_any(py);
    }
    let Some(array) = exact_geometry_array(geom) else {
        return Err(expected_geometry_or_array());
    };
    let array = array.clone();
    let rows = py.detach(move || {
        let crs = array.crs_str().map(str::to_owned);
        let mut produced = 0_usize;
        crate::try_collect_results(
            array
                .row_operation()
                .rows_with_missing(Vec::<u64>::new)
                .map(|row| {
                    let crate::array::ArrayRow::Present(row) = row else {
                        return Ok(Vec::new());
                    };
                    row.row().with_shape(|shape| {
                        let shape = crate::lonlat_shape_under(shape, crs.as_deref())?.into_owned();
                        // Scalar factories reject empty sources, but an array must retain row
                        // alignment. Represent a present empty exactly like a missing row in
                        // the ragged cell result: an empty group, not a batch error.
                        if shape.is_empty() {
                            return Ok(Vec::new());
                        }
                        let remaining = remaining_cover_budget(produced, max_cells)?;
                        if remaining == Some(0)
                            && !matches!(&shape, crate::geometry::Shape::Empty(..))
                        {
                            return Err(crate::py::cells::coverage_ops::cover_budget_err(
                                crate::grid::CoverBudgetExceeded::new(
                                    max_cells.expect("finite budget"),
                                ),
                            ));
                        }
                        let ids = build(&PyGeometry::wgs84(shape), remaining)?;
                        produced = checked_cover_batch_len(produced, ids.len(), max_cells)?;
                        Ok(ids)
                    })
                }),
        )
    })?;
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

fn remaining_cover_budget(produced: usize, max_cells: Option<usize>) -> PyResult<Option<usize>> {
    max_cells
        .map(|limit| {
            limit.checked_sub(produced).ok_or_else(|| {
                crate::py::cells::coverage_ops::cover_budget_err(
                    crate::grid::CoverBudgetExceeded::new(limit),
                )
            })
        })
        .transpose()
}

pub(super) fn lonlat_point_geometry(point: Point) -> PyGeometry {
    PyGeometry::wgs84(Shape::Point(point))
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
        Shape::MultiPolygon(polygons) if polygons.len() == 1 && !polygons.is_empty() => {
            Shape::Polygon(polygons.into_iter().next().expect("one polygon"))
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
    cells: &crate::py::cells::coverage_ops::SortedCells<C>,
) -> PyResult<Option<Typed>> {
    let Some(first) = cells.as_slice().first() else {
        return Ok(None);
    };
    let depth = first.depth();
    if !cells.as_slice().iter().all(|cell| cell.depth() == depth) {
        return Ok(None);
    }
    let capacity = cells
        .as_slice()
        .len()
        .checked_mul(4)
        .ok_or_else(|| {
            crate::py::errors::native_overflow_error("coverage outline size overflows usize")
        })?;
    let mut outline = crate::try_vec_with_capacity(capacity)?;
    for cell in cells.as_slice() {
        let bounds = cell.bounds();
        // CCW corners: (minx,miny) -> (maxx,miny) -> (maxx,maxy) -> (minx,maxy).
        let corners = [
            Point::new_raw_xy(bounds.minx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.maxy()),
            Point::new_raw_xy(bounds.minx(), bounds.maxy()),
        ];
        // edge_neighbors() is [south, east, north, west], matching side `k` =
        // corners[k] -> corners[k+1].
        for (side, neighbor) in cell.edge_neighbors().into_iter().enumerate() {
            let interior =
                neighbor.is_some_and(|across| cells.as_slice().binary_search(&across).is_ok());
            if !interior {
                crate::try_push(
                    &mut outline,
                    crate::geometry::CoordSeq::try_from_points(&[
                        corners[side],
                        corners[(side + 1) % 4],
                    ])
                    .map_err(PyErr::from)?,
                )?;
            }
        }
    }
    super::coverage_ops::polygonize_outline(outline)
}

/// Dissolve present geohash or tile cells from `CellArray.to_polygon`.
///
/// The caller already skipped missing rows and may pass an arbitrary logical
/// selection. Sort and dedup for topology membership, then [`rect_dissolve`]
/// runs the uniform-depth edge-cancel pass. Mixed-depth, degenerate, or
/// empty-outline sets fall back to the antimeridian-aware boundary union.
/// Empty input reaches `coverage_to_polygon`, which raises the documented
/// `GeometryError`.
pub(super) fn rect_cells_to_polygon<C: crate::grid::coverer::RectCell>(
    cells: Vec<C>,
) -> PyResult<Typed> {
    let cells = crate::py::cells::coverage_ops::SortedCells::new(cells);
    if let Some(typed) = rect_dissolve(&cells)? {
        return Ok(typed);
    }
    let shapes = crate::try_collect_results(cells.as_slice().iter().map(|cell| {
        let shape = crate::geometry::bounds_to_shape(cell.bounds());
        shape.split_antimeridian().map_err(PyErr::from)
    }))?;
    coverage_to_polygon(&shapes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_cover_budget_is_cumulative_and_overflow_safe() {
        // `PyErr` rendering touches the interpreter even in a pure-Rust test.
        crate::test_support::initialize_python();
        let limit = usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap();
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

    #[test]
    fn remaining_row_budget_shrinks_without_capping_unlimited_batches() {
        assert_eq!(remaining_cover_budget(3, Some(5)).unwrap(), Some(2));
        assert_eq!(remaining_cover_budget(5, Some(5)).unwrap(), Some(0));
        assert_eq!(remaining_cover_budget(usize::MAX, None).unwrap(), None);
    }
}
