#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyTypeError;

use crate::grid::CoverBudgetExceeded;
use crate::py::cells::coverage_ops::GridCoverError;
use crate::py::cells::{
    Bound, GridKind, Point, Py, PyAny, PyAnyMethods as _, PyCellArray, PyErr, PyResult, Python,
    Shape, lonlat_shape,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::{PyGeometry, Typed, exact_geometry, exact_geometry_array, expected_geometry_or_array};

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
        let bounds = if shape.crosses_antimeridian() {
            crate::geometry::geographic_crossing_bounds(&shape)
        } else {
            shape.bounds()
        };
        return bounds.map_or_else(
            || {
                Err(InvalidGeometryError::new_err(
                    "bounding cell requires a non-empty geometry",
                ))
            },
            |bounds| {
                Ok(BoundingQueryRegion {
                    bounds,
                    crosses_antimeridian: shape.crosses_antimeridian(),
                })
            },
        );
    }
    if let Some(array) = exact_geometry_array(value) {
        let mut shapes = crate::lonlat_nonempty_shapes(array)?;
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
        return bounds.map_or_else(
            || {
                Err(InvalidGeometryError::new_err(
                    "bounding cell requires a non-empty geometry",
                ))
            },
            |bounds| {
                Ok(BoundingQueryRegion {
                    bounds,
                    crosses_antimeridian: shape.crosses_antimeridian(),
                })
            },
        );
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
    build: impl Fn(&PyGeometry, Option<usize>) -> Result<Vec<u64>, GridCoverError> + Send,
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
    let rows = py.detach(move || cover_array_rows(&array, max_cells, build))?;
    crate::py::vectors::Groups::from_cell_rows(kind, rows)?.into_py_any(py)
}

fn cover_array_rows(
    array: &crate::PyGeometryArray,
    max_cells: Option<usize>,
    build: impl Fn(&PyGeometry, Option<usize>) -> Result<Vec<u64>, GridCoverError>,
) -> Result<Vec<Vec<u64>>, GridCoverError> {
    let crs = array.crs_str();
    let mut produced = 0_usize;
    let mut out = Vec::new();
    for row in array.row_operation().rows_with_missing(Vec::<u64>::new) {
        let ids = match row {
            crate::array::ArrayRow::Missing { .. } => Vec::new(),
            crate::array::ArrayRow::Present(row) => row.row().try_with_shape(|shape| {
                let shape = crate::lonlat_shape_under(shape, crs)?.into_owned();
                // Scalar factories reject empty sources, but an array must retain row
                // alignment. Represent a present empty exactly like a missing row in
                // the ragged cell result: an empty group, not a batch error.
                if shape.is_empty() {
                    return Ok(Vec::new());
                }
                let remaining = remaining_cover_budget(produced, max_cells)?;
                if remaining == Some(0) {
                    return Err(GridCoverError::Budget(CoverBudgetExceeded::new(
                        max_cells.expect("finite budget"),
                    )));
                }
                let geometry = PyGeometry::wgs84(shape)?;
                let ids = build(&geometry, remaining)?;
                drop(geometry);
                produced = checked_cover_batch_len(produced, ids.len(), max_cells)?;
                Ok(ids)
            })?,
        };
        crate::try_push_vec(&mut out, ids)?;
    }
    Ok(out)
}

fn checked_cover_batch_len(
    produced: usize,
    row_len: usize,
    max_cells: Option<usize>,
) -> Result<usize, CoverBudgetExceeded> {
    let produced = produced.saturating_add(row_len);
    crate::grid::ensure_cover_budget(produced, max_cells)?;
    Ok(produced)
}

fn remaining_cover_budget(
    produced: usize,
    max_cells: Option<usize>,
) -> Result<Option<usize>, CoverBudgetExceeded> {
    max_cells
        .map(|limit| {
            limit
                .checked_sub(produced)
                .ok_or_else(|| CoverBudgetExceeded::new(limit))
        })
        .transpose()
}

pub(super) fn lonlat_point_geometry(point: Point) -> crate::error::Result<PyGeometry> {
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
    Ok(PyGeometry::typed_wgs84(shape)?)
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
    let capacity = cells.as_slice().len().checked_mul(4).ok_or_else(|| {
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
        let shape = crate::geometry::bounds_to_shape(cell.bounds()).map_err(PyErr::from)?;
        shape.split_antimeridian().map_err(PyErr::from)
    }))?;
    coverage_to_polygon(&shapes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_cover_budget_is_cumulative_and_overflow_safe() {
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
        let over = remaining_cover_budget(6, Some(5)).expect_err("produced past the cap");
        assert!(over.to_string().contains("max_cells"));
    }

    fn point_array(points: Vec<(f64, f64)>) -> crate::PyGeometryArray {
        crate::PyGeometryArray::packed_points(
            crate::geometry::CoordSeq::try_from_vecs(
                points.iter().map(|point| point.0).collect(),
                points.iter().map(|point| point.1).collect(),
                None,
                None,
            )
            .unwrap(),
            crate::boundary::Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn first_row_budget_beats_later_invalid_latitude() {
        let array = point_array(vec![(13.4, 52.5), (0.0, 95.0)]);
        let error = cover_array_rows(&array, Some(1), |_, _| Ok(vec![0, 1]))
            .expect_err("row 0 must exhaust max_cells before row 1 lonlat");
        let GridCoverError::Budget(budget) = error else {
            panic!("expected budget error, got {error:?}");
        };
        assert!(budget.to_string().contains("max_cells"), "{budget}");
    }

    #[test]
    fn later_row_domain_after_successful_row_is_native() {
        let array = point_array(vec![(13.4, 52.5), (0.0, 95.0)]);
        let error =
            cover_array_rows(&array, None, |_, _| Ok(vec![0])).expect_err("row 1 invalid latitude");
        let GridCoverError::Native(error) = error else {
            panic!("expected native lonlat error, got {error:?}");
        };
        let message = error.to_string();
        assert!(
            message.contains("invalid longitude/latitude (0, 95)"),
            "{message}"
        );
    }

    #[test]
    fn missing_and_empty_present_rows_are_empty_groups() {
        let present = crate::PyGeometryArray::from_shapes(
            vec![
                crate::geometry::Shape::empty_polygon(),
                crate::geometry::Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 2.0)),
            ],
            crate::boundary::Frame::None,
        )
        .unwrap();
        let mask = crate::array::ValidityMask::from_vec(3, vec![false, true, false])
            .unwrap()
            .expect("mask");
        let array = crate::PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let rows = cover_array_rows(&array, None, |_, _| Ok(vec![7]))
            .expect("empty and missing are groups");
        assert_eq!(rows, vec![Vec::new(), Vec::new(), vec![7]]);
    }

    #[test]
    fn allocation_failure_after_fixture_is_native_resource() {
        let array = crate::PyGeometryArray::from_shapes(
            vec![
                crate::geometry::Shape::empty_polygon(),
                crate::geometry::Shape::empty_polygon(),
            ],
            crate::boundary::Frame::None,
        )
        .unwrap();
        let error = crate::test_support::fail_allocations_after(0, || {
            cover_array_rows(&array, None, |_, _| unreachable!("empty rows skip build"))
                .expect_err("collector allocation must fail")
        });
        assert!(
            matches!(error, GridCoverError::Native(ref inner) if matches!(
                inner.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
            )),
            "{error:?}"
        );
    }
}
