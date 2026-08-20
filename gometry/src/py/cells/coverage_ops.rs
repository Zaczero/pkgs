//! Grid factory and rectangular-grid cell-array helpers.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::geometry::{CoordSeq, LineSeq, Polygon, Shape, is_geographic_frame};
use crate::grid::affine_source::normalize_grid_source;
use crate::grid::cell::RectGridCell;
use crate::grid::coverer;
use crate::py::cells::cell_ops::cell_array_from_keys;
use crate::py::cells::{CellRule, GridKind, PyCellArray, coverage_to_polygon};
use crate::py::errors::{GeometryError, InvalidGeometryError, integer_parameter_error};
use crate::{PyGeometry, Typed, lonlat_shape, validate_lonlat_shape};

/// Cell ids in the order required by dissolve membership searches.
///
/// This is deliberately separate from cell-set normalization: dissolve needs
/// `Ord` order and strict uniqueness, while hierarchical set operations use
/// each grid's descendant range-key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SortedCells<C>(Vec<C>);

impl<C: Ord> SortedCells<C> {
    pub(crate) fn new(mut cells: Vec<C>) -> Self {
        if !cells.windows(2).all(|pair| pair[0] < pair[1]) {
            cells.sort_unstable();
            cells.dedup();
        }
        Self(cells)
    }

    pub(crate) fn from_sorted(cells: Vec<C>) -> Self {
        // Producers use this fast path only after proving strict order, but
        // keep the boundary total so a future producer cannot ship an invalid
        // SortedCells value in release builds.
        if !cells.windows(2).all(|pair| pair[0] < pair[1]) {
            return Self::new(cells);
        }
        Self(cells)
    }

    pub(crate) fn into_vec(self) -> Vec<C> {
        self.0
    }

    pub(crate) fn as_slice(&self) -> &[C] {
        &self.0
    }
}

pub(crate) trait GridDissolver {
    type Cell: Copy + Ord;

    fn fast_path_cells(
        cells: &SortedCells<Self::Cell>,
    ) -> PyResult<Option<SortedCells<Self::Cell>>>;
    /// Cheap neighbor ids for adjacency pre-scan (no edge geometry).
    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell>;
    /// Exterior edge segments only (`neighbor` not in the prepared set).
    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> Vec<CoordSeq>;
    fn crosses_seam(segment: &CoordSeq) -> bool;
    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape>;
}

pub(crate) fn dissolve_grid_cells<D>(cells: SortedCells<D::Cell>) -> PyResult<Typed>
where
    D: GridDissolver,
{
    let fallback_cells = if let Some(prepared) = D::fast_path_cells(&cells)? {
        // Pre-scan adjacency without materializing edge geometry. Zero shared
        // edges → MultiPolygon of per-cell polygons (no polygonize).
        let any_adjacent = prepared.as_slice().iter().any(|&cell| {
            D::adjacency_neighbors(cell)
                .any(|neighbor| prepared.as_slice().binary_search(&neighbor).is_ok())
        });
        if !any_adjacent {
            // No shared edges: assemble a MultiPolygon of the per-cell
            // polygons directly — never pay union/polygonize.
            return multipolygon_from_cell_shapes(
                prepared
                    .as_slice()
                    .iter()
                    .copied()
                    .map(D::fallback_shape)
                    .collect::<crate::error::Result<Vec<_>>>()?,
            );
        }
        let is_member = |neighbor: D::Cell| prepared.as_slice().binary_search(&neighbor).is_ok();
        let mut outline: Vec<CoordSeq> = Vec::with_capacity(prepared.as_slice().len() * 2);
        let mut crosses_seam = false;
        'scan: for &cell in prepared.as_slice() {
            for segment in D::exterior_edge_segments(cell, &is_member) {
                if D::crosses_seam(&segment) {
                    crosses_seam = true;
                    break 'scan;
                }
                outline.push(segment);
            }
        }
        if !crosses_seam && let Some(typed) = polygonize_outline(outline)? {
            return Ok(typed);
        }
        prepared
    } else {
        cells
    };
    let shapes: Vec<Shape> = fallback_cells
        .as_slice()
        .iter()
        .copied()
        .map(D::fallback_shape)
        .collect::<crate::error::Result<_>>()?;
    coverage_to_polygon(&shapes)
}

/// Assemble one MultiPolygon (or single Polygon) from already-disjoint cell
/// shapes — used when the dissolve pre-scan finds zero shared edges.
fn multipolygon_from_cell_shapes(shapes: Vec<Shape>) -> PyResult<Typed> {
    if shapes.is_empty() {
        return Err(GeometryError::new_err(
            "a coverage needs at least one cell to dissolve into a polygon",
        ));
    }
    let mut polygons = Vec::with_capacity(shapes.len());
    for shape in shapes {
        match shape {
            Shape::Polygon(polygon) => polygons.push(polygon),
            Shape::MultiPolygon(parts) => polygons.extend(parts),
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    match part {
                        Shape::Polygon(polygon) => polygons.push(polygon),
                        Shape::MultiPolygon(multi) => polygons.extend(multi),
                        _ => {},
                    }
                }
            },
            _ => {},
        }
    }
    if polygons.is_empty() {
        return Err(GeometryError::new_err(
            "a coverage needs at least one cell to dissolve into a polygon",
        ));
    }
    let shape = if polygons.len() == 1 {
        Shape::Polygon(polygons.into_iter().next().expect("one polygon"))
    } else {
        Shape::MultiPolygon(polygons)
    };
    Ok(PyGeometry::typed_wgs84(shape))
}

fn polygonize_outline(outline: Vec<CoordSeq>) -> PyResult<Option<Typed>> {
    let polygons: Vec<Polygon> = Shape::build_area_all(
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

pub(crate) fn coordseq_crosses_lon_seam(segment: &CoordSeq) -> bool {
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for point in segment.points() {
        lo = lo.min(point.x);
        hi = hi.max(point.x);
    }
    hi - lo > 180.0
}

pub(crate) trait RectCoverSpec {
    type Cell: RectGridCell + crate::grid::cell_set::HierarchicalId;

    const KIND: GridKind;

    fn roots() -> Vec<Self::Cell>;
    fn id(cell: &Self::Cell) -> u64;
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn coverage_label() -> &'static str;
}

/// Build the public rectangular-grid result directly as canonical cell ids.
pub(crate) fn build_rect_coverage_ids<S>(
    geometry: &PyGeometry,
    depth: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<Vec<u64>>
where
    S: RectCoverSpec,
{
    let depth = S::parse_depth(depth)?;
    let (_, cover_shape, _) = coverage_factory_shapes(geometry, S::coverage_label())?;
    let ids: Vec<u64> = match cell_rule {
        CellRule::Overlap | CellRule::Bbox => {
            coverer::cover(&cover_shape, S::roots(), depth, max_cells)
                .map_err(cover_budget_err)?
                .into_iter()
                .map(|(cell, _)| S::id(&cell))
                .collect()
        },
        CellRule::Within => coverer::cover(&cover_shape, S::roots(), depth, max_cells)
            .map_err(cover_budget_err)?
            .into_iter()
            .filter_map(|(cell, interior)| interior.then_some(S::id(&cell)))
            .collect(),
        CellRule::Center => coverer::cover_center(&cover_shape, S::roots(), depth, max_cells)
            .map_err(cover_budget_err)?
            .into_iter()
            .map(|cell| S::id(&cell))
            .collect(),
    };
    Ok(SortedCells::from_sorted(ids).into_vec())
}

/// Shared error and source-normalization helpers for grid factories.
pub(crate) fn empty_coverage_err(label: &str) -> PyErr {
    InvalidGeometryError::new_err(format!("{label} coverage requires a non-empty geometry"))
}

/// Convert a covering cell-budget overflow into the Python `GeometryError`
/// (the shared boundary mapping for every grid's `cover(...)` factory).
pub(crate) fn cover_budget_err(err: crate::grid::CoverBudgetExceeded) -> PyErr {
    integer_parameter_error(err.to_string(), "max_cells", err.limit as i64)
}

/// Parse the shared cover-factory `max_cells` parameter.
///
/// - omitted default is applied by the pyfunction signature (`1_000_000`)
/// - `None` (Python `None`) → unlimited
/// - positive int → that budget
/// - `<= 0` → typed value error naming `max_cells`
pub(crate) fn parse_max_cells(max_cells: Option<i64>) -> PyResult<Option<usize>> {
    match max_cells {
        None => Ok(None),
        Some(n) if n <= 0 => Err(integer_parameter_error(
            format!("max_cells must be greater than zero, got {n}"),
            "max_cells",
            n,
        )),
        Some(n) => usize::try_from(n)
            .map(Some)
            .map_err(|_| integer_parameter_error("max_cells is too large", "max_cells", n)),
    }
}

pub(crate) fn cover_working_shape(
    frame: &crate::boundary::metadata::Frame,
    lonlat: &Shape,
) -> PyResult<(Shape, bool)> {
    if is_geographic_frame(frame) && lonlat.crosses_antimeridian() {
        Ok((lonlat.split_antimeridian()?, true))
    } else {
        Ok((lonlat.clone(), false))
    }
}

/// Normalize caller-owned source geometry to canonical WGS84 lon/lat
/// membership geometry. New certified coverers consume this exact source
/// directly; only the historical planar tilers need a split working image.
pub(crate) fn coverage_factory_geometry(
    geometry: &PyGeometry,
    label: &str,
) -> PyResult<PyGeometry> {
    let shape = lonlat_shape(geometry)?;
    validate_lonlat_shape(&shape)?;
    // This is the sole public grid-source normalization point.  Every grid
    // lane receives the same physical-pole spelling before it can derive a
    // working image, a rectangle certificate, or aggregate components.
    let shape = normalize_grid_source(&shape);
    if shape.is_empty() {
        return Err(empty_coverage_err(label));
    }
    Ok(PyGeometry::wgs84(shape))
}

/// Prepare the historical planar cover lanes with caller-owned WGS84
/// membership geometry (unsplit) plus their split-normalized working shape.
///
/// Returns `(membership_geometry, cover_shape, cover_is_split)`.
pub(crate) fn coverage_factory_shapes(
    geometry: &PyGeometry,
    label: &str,
) -> PyResult<(PyGeometry, Shape, bool)> {
    let membership_geometry = coverage_factory_geometry(geometry, label)?;
    let shape = membership_geometry.shape.as_ref();
    let (cover_shape, cover_is_split) = cover_working_shape(&geometry.frame, shape)?;
    Ok((membership_geometry, cover_shape, cover_is_split))
}

/// Build a `CellArray` from source cell ids through the public factory path.
pub(crate) fn rect_cell_array_for<S: RectCoverSpec>(
    cells: impl IntoIterator<Item = S::Cell>,
) -> PyCellArray {
    cell_array_from_keys(S::KIND, cells)
}

#[cfg(test)]
mod tests {
    use super::SortedCells;

    #[test]
    fn canonicalizes_shuffled_duplicates() {
        let cells = SortedCells::new(vec![3, 1, 3, 2, 1]);
        assert_eq!(cells.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn preserves_already_canonical_input() {
        let cells = SortedCells::new(vec![1, 2, 3]);
        assert_eq!(cells.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn from_sorted_canonicalizes_an_invalid_producer() {
        let cells = SortedCells::from_sorted(vec![3, 1, 3, 2]);
        assert_eq!(cells.as_slice(), &[1, 2, 3]);
    }
}
