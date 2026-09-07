//! Grid factory and rectangular-grid cell-array helpers.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::prelude::*;

use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::{CoordSeq, LineSeq, Shape, TopologyDomain};
use crate::grid::affine_source::normalize_grid_source;
use crate::grid::cell::{HierarchicalCell, RectGridCell};
use crate::grid::coverer::{self, RectCoverRule};
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
    ) -> PyResult<Vec<CoordSeq>>;
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
            let shapes = crate::try_collect_results(
                prepared
                    .as_slice()
                    .iter()
                    .copied()
                    .map(|cell| D::fallback_shape(cell).map_err(PyErr::from)),
            )?;
            return multipolygon_from_cell_shapes(shapes);
        }
        let Some(capacity) = prepared.as_slice().len().checked_mul(2) else {
            drop(prepared);
            // A failed capacity calculation is a size error, not an
            // allocation failure; retain its public overflow class.
            return Err(crate::py::errors::native_overflow_error(
                "coverage outline size overflows usize",
            ));
        };
        let is_member = |neighbor: D::Cell| prepared.as_slice().binary_search(&neighbor).is_ok();
        let mut outline = crate::try_vec_with_capacity(capacity)?;
        let mut crosses_seam = false;
        'scan: for &cell in prepared.as_slice() {
            for segment in D::exterior_edge_segments(cell, &is_member)? {
                if D::crosses_seam(&segment) {
                    crosses_seam = true;
                    break 'scan;
                }
                crate::try_push(&mut outline, segment)?;
            }
        }
        if !crosses_seam && let Some(typed) = polygonize_outline(outline)? {
            return Ok(typed);
        }
        prepared
    } else {
        cells
    };
    let shapes = crate::try_collect_results(
        fallback_cells
            .as_slice()
            .iter()
            .copied()
            .map(|cell| D::fallback_shape(cell).map_err(PyErr::from)),
    )?;
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
    let mut polygons = crate::try_vec_with_capacity(shapes.len())?;
    for shape in shapes {
        match shape {
            Shape::Polygon(polygon) => crate::try_push(&mut polygons, polygon)?,
            Shape::MultiPolygon(parts) => {
                for polygon in parts {
                    crate::try_push(&mut polygons, polygon)?;
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    match part {
                        Shape::Polygon(polygon) => crate::try_push(&mut polygons, polygon)?,
                        Shape::MultiPolygon(multi) => {
                            for polygon in multi {
                                crate::try_push(&mut polygons, polygon)?;
                            }
                        },
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
    let shape = Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons);
    Ok(PyGeometry::typed_wgs84(shape))
}

fn polygonize_outline(outline: Vec<CoordSeq>) -> PyResult<Option<Typed>> {
    if outline.is_empty() {
        return Ok(None);
    }
    let polygons = crate::try_collect_results(
        Shape::build_area_all(
            &[&Shape::multi_line_string(
                crate::geometry::CoordinateAxes::XY,
                crate::try_collect_results(
                    outline
                        .into_iter()
                        .map(|line| Ok::<_, PyErr>(LineSeq::from_proven(line))),
                )?,
            )],
            true,
        )?
        .into_iter()
        .filter_map(|shape| match shape {
            Shape::Polygon(polygon) => Some(Ok(polygon)),
            _ => None,
        }),
    )?;
    if polygons.is_empty() {
        return Ok(None);
    }
    let shape = Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons);
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
    type Cell: RectGridCell + HierarchicalCell;

    const KIND: GridKind;

    fn roots() -> Vec<Self::Cell>;
    fn id(cell: &Self::Cell) -> u64;
}

/// Build canonical cell ids from an admitted membership source and its working
/// shape. Callers own source admission so tile-specific gates run before this.
pub(crate) fn build_rect_coverage_ids<S>(
    membership: &PyGeometry,
    cover_shape: &Shape,
    depth: u8,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<Vec<u64>>
where
    S: RectCoverSpec,
{
    let authority = membership.shape.as_ref();
    let authority_plan = membership.plan_or_transient();
    let authority_cache = membership.frame_cache();
    let TopologyDomain::Geographic(frame) = crate::geometry::topology_domain(&membership.frame)?
    else {
        unreachable!("normalized coverage geometry has a geographic frame")
    };
    let rule = match cell_rule {
        CellRule::Overlap | CellRule::Bbox => RectCoverRule::Overlap,
        CellRule::Within => RectCoverRule::Within,
        CellRule::Center => RectCoverRule::Center,
    };
    let ids = crate::try_collect_results(
        coverer::cover(
            authority,
            cover_shape,
            &authority_plan,
            &authority_cache,
            frame,
            rule,
            S::roots(),
            depth,
            max_cells,
        )
        .map_err(cover_error)?
        .into_iter()
        .map(|cell| Ok::<_, PyErr>(S::id(&cell))),
    )?;
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

pub(crate) fn cover_error(err: crate::grid::CoverError) -> PyErr {
    match err {
        crate::grid::CoverError::Preparation(error) => error.into(),
        crate::grid::CoverError::Budget(error) => cover_budget_err(error),
    }
}

/// Parse the shared cover-factory `max_cells` parameter.
///
/// - omitted default is applied by the pyfunction signature (`GRID_MAX_CELLS`)
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
) -> PyResult<Shape> {
    let domain = crate::geometry::topology_domain(frame)?;
    if let TopologyDomain::Geographic(frame) = domain
        && crate::geometry::crosses_antimeridian_in_frame(lonlat, frame)
    {
        Ok(lonlat.split_antimeridian_in_frame(frame)?)
    } else {
        Ok(lonlat.clone())
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
    ScalarGeographicView::try_new(GeographicFrame::crs84(), &shape)
        .expect("the normalized result of public CRS84 admission is a valid geographic view");
    if shape.is_empty() {
        return Err(empty_coverage_err(label));
    }
    Ok(PyGeometry::wgs84(shape))
}

/// Prepare the historical planar cover lanes with caller-owned WGS84
/// membership geometry (unsplit) plus their split-normalized working shape.
///
/// Returns the admitted membership geometry and its split working shape.
pub(crate) fn coverage_factory_shapes(
    geometry: &PyGeometry,
    label: &str,
) -> PyResult<(PyGeometry, Shape)> {
    let membership_geometry = coverage_factory_geometry(geometry, label)?;
    let shape = membership_geometry.shape.as_ref();
    // `membership_geometry` is the admitted WGS84 source. Derive the working
    // image from that frame, never from the caller's pre-reprojection frame:
    // a projected source may become antimeridian-crossing only after this
    // conversion.
    let cover_shape = cover_working_shape(&membership_geometry.frame, shape)?;
    Ok((membership_geometry, cover_shape))
}

/// Build a `CellArray` from source cell ids through the public factory path.
pub(crate) fn rect_cell_array_for<S: RectCoverSpec>(
    cells: impl IntoIterator<Item = S::Cell>,
) -> PyResult<PyCellArray> {
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
