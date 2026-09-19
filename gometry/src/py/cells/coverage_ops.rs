//! Grid factory and rectangular-grid cell-array helpers.
use pyo3::prelude::*;

use crate::boundary::geographic;
use crate::error::{Error, ResourceError};
use crate::geographic::{
    GeographicDiagnostic, GeographicFrame, GeographicViewError, ScalarGeographicView,
};
use crate::geometry::{CoordSeq, LineSeq, Shape, TopologyDomain};
use crate::grid::affine_source::{normalize_grid_source, visit_grid_cover_components};
use crate::grid::cell::{HierarchicalCell, RectGridCell};
use crate::grid::coverer::{self, RectCoverRule};
use crate::grid::{CoverBudgetExceeded, CoverError};
use crate::py::cells::cell_ops::cell_array_from_keys;
use crate::py::cells::{CellRule, GridKind, PyCellArray, coverage_to_polygon};
use crate::py::errors;
use crate::py::errors::{GeometryError, InvalidGeometryError, integer_parameter_error};
use crate::{
    Frame, GeometryTransformFrame, Point, PyGeometry, PyGeometryArray, Typed, crs, crs_arc,
};

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
            return Err(errors::native_overflow_error(
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
    let shape = Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons)
        .map_err(PyErr::from)?;
    Ok(PyGeometry::typed_wgs84(shape)?)
}

pub(super) fn polygonize_outline(outline: Vec<CoordSeq>) -> PyResult<Option<Typed>> {
    if outline.is_empty() {
        return Ok(None);
    }
    let outline = Shape::multi_line_string(
        crate::geometry::CoordinateAxes::XY,
        crate::try_collect_results(
            outline
                .into_iter()
                .map(|line| Ok::<_, PyErr>(LineSeq::from_proven(line))),
        )?,
    )
    .map_err(PyErr::from)?;
    let polygons = crate::try_collect_results(
        Shape::build_area_all(&[&outline], true)?
            .into_iter()
            .filter_map(|shape| match shape {
                Shape::Polygon(polygon) => Some(Ok(polygon)),
                _ => None,
            }),
    )?;
    if polygons.is_empty() {
        return Ok(None);
    }
    let shape = Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons)
        .map_err(PyErr::from)?;
    Ok(Some(PyGeometry::typed_wgs84(shape)?))
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

    fn roots() -> crate::error::Result<Vec<Self::Cell>>;
    fn id(cell: &Self::Cell) -> u64;
}

/// Native cover-factory error. Converted to `PyErr` only after the detach
/// closure drops membership geometry, working shapes, and prior cell rows.
#[derive(Debug)]
pub(crate) enum GridCoverError {
    Native(Error),
    NativeRow { row: usize, error: Error },
    Row { row: usize, error: Box<Self> },
    Budget(CoverBudgetExceeded),
    Empty(&'static str),
    Type,
    TileLatitude(f64),
    Geographic(Box<GeographicDiagnostic>),
    H3Capacity,
}

impl From<Error> for GridCoverError {
    fn from(error: Error) -> Self {
        Self::Native(error)
    }
}

impl From<ResourceError> for GridCoverError {
    fn from(error: ResourceError) -> Self {
        Self::Native(error.into())
    }
}

impl From<CoverError> for GridCoverError {
    fn from(error: CoverError) -> Self {
        match error {
            CoverError::Preparation(error) => Self::Native(error),
            CoverError::Budget(error) => Self::Budget(error),
        }
    }
}

impl From<CoverBudgetExceeded> for GridCoverError {
    fn from(error: CoverBudgetExceeded) -> Self {
        Self::Budget(error)
    }
}

impl From<GeographicViewError> for GridCoverError {
    fn from(error: GeographicViewError) -> Self {
        match error {
            GeographicViewError::Resource => Self::Native(ResourceError::Allocation.into()),
            GeographicViewError::Diagnostic(diagnostic) => Self::Geographic(diagnostic),
        }
    }
}

impl From<GridCoverError> for PyErr {
    fn from(error: GridCoverError) -> Self {
        grid_cover_error_py(error)
    }
}

fn grid_cover_error_py(error: GridCoverError) -> PyErr {
    match error {
        GridCoverError::Native(error) => error.into(),
        GridCoverError::NativeRow { row, error } => crate::note_array_row(error.into(), row),
        GridCoverError::Row { row, error } => {
            crate::note_array_row(grid_cover_error_py(*error), row)
        },
        GridCoverError::Budget(error) => cover_budget_err(error),
        GridCoverError::Empty(label) => empty_coverage_err(label),
        GridCoverError::Type => errors::geometry_type_err("expected Point geometry"),
        GridCoverError::TileLatitude(lat) => InvalidGeometryError::new_err(format!(
            "latitude {lat} is outside the square Web Mercator tile grid ±85.05112878 degrees"
        )),
        GridCoverError::Geographic(diagnostic) => {
            let message = diagnostic.to_string();
            drop(diagnostic);
            InvalidGeometryError::new_err(message)
        },
        GridCoverError::H3Capacity => {
            GeometryError::new_err("H3 coverage traversal exceeded its representable capacity")
        },
    }
}

impl GridCoverError {
    pub(crate) fn at_row(self, row: usize) -> Self {
        Self::Row {
            row,
            error: Box::new(self),
        }
    }
}

/// One grid operation's source admission plan.
///
/// CRS-free coordinates are already admitted WGS84 lon/lat and therefore have
/// no invented frame or transform plan. Tagged coordinates use the complete
/// source [`Frame`] to build one plan to EPSG:4326; the plan retains the
/// runtime snapshot and source epoch for the whole scalar or array operation.
pub(crate) struct GridSourceAdmission {
    transformer: Option<crs::TransformPlan>,
}

impl GridSourceAdmission {
    pub(crate) fn new(frame: &Frame) -> crate::error::Result<Self> {
        let Some(_) = frame.crs_ref() else {
            return Ok(Self { transformer: None });
        };
        let resolved = GeometryTransformFrame::new(
            frame,
            crs_arc("EPSG:4326"),
            None,
            crs::TransformOptions::default(),
        )?;
        let transformer = crs::TransformPlan::new_with_options_at(
            resolved.source.as_str(),
            resolved.target.as_str(),
            resolved.options,
            resolved.state,
        )?;
        Ok(Self {
            transformer: Some(transformer),
        })
    }

    pub(crate) fn new_for_array(array: &PyGeometryArray) -> crate::error::Result<Self> {
        let has_present = array.len() != 0
            && array
                .missing()
                .is_none_or(|missing| missing.present_count() != 0);
        if has_present {
            Self::new(&array.frame)
        } else {
            Ok(Self { transformer: None })
        }
    }

    fn transform_shapes(
        &self,
        shapes: &[&Shape],
        logical_rows: Option<&[usize]>,
    ) -> Result<Vec<Shape>, GridCoverError> {
        if let Some(transformer) = &self.transformer {
            return transformer
                .transform_shapes_indexed(shapes)
                .map_err(|failure| {
                    let row = failure
                        .coordinate
                        .and_then(|rank| logical_rows.and_then(|rows| rows.get(rank).copied()));
                    let error = failure.error;
                    match row {
                        Some(row) => GridCoverError::NativeRow { row, error },
                        None => GridCoverError::Native(error),
                    }
                });
        }
        Ok(shapes.iter().map(|shape| (**shape).clone()).collect())
    }

    fn normalize_shape(shape: Shape) -> Result<Shape, GridCoverError> {
        crate::validate_lonlat_shape(&shape)?;
        let normalized = normalize_grid_source(&shape);
        ScalarGeographicView::try_new(GeographicFrame::crs84(), &normalized)?;
        Ok(normalized)
    }

    pub(crate) fn admit_geometry(
        self,
        geometry: &PyGeometry,
    ) -> Result<PyGeometry, GridCoverError> {
        let shape = if let Some(transformer) = &self.transformer {
            transformer.transform_shape(geometry.shape.shape())?
        } else {
            geometry.shape.shape().clone()
        };
        Ok(PyGeometry::wgs84(Self::normalize_shape(shape)?)?)
    }

    pub(crate) fn admit_array_shapes(
        self,
        array: &PyGeometryArray,
    ) -> Result<Vec<Result<Option<Shape>, GridCoverError>>, GridCoverError> {
        let mut source_rows = Vec::new();
        for row in array.row_operation().present_rows() {
            let logical = row.logical();
            let shape =
                row.row()
                    .with_shape(Clone::clone)
                    .map_err(|error| GridCoverError::NativeRow {
                        row: logical,
                        error,
                    })?;
            crate::try_push_vec(&mut source_rows, (logical, shape))?;
        }
        let logical_rows: Vec<_> = source_rows.iter().map(|(logical, _)| *logical).collect();
        let source_shapes: Vec<_> = source_rows.iter().map(|(_, shape)| shape).collect();
        let mut output = Vec::new();
        crate::try_reserve_vec(&mut output, array.len())?;
        for _ in 0..array.len() {
            crate::try_push_vec(&mut output, Ok(None))?;
        }
        let transformed = match self.transform_shapes(&source_shapes, Some(&logical_rows)) {
            Ok(transformed) => transformed,
            Err(GridCoverError::NativeRow { row, error }) => {
                let rank = logical_rows
                    .iter()
                    .position(|&logical| logical == row)
                    .expect("transform failure row came from the source batch");
                let prefix =
                    self.transform_shapes(&source_shapes[..rank], Some(&logical_rows[..rank]))?;
                for ((logical, _), shape) in source_rows[..rank].iter().zip(prefix) {
                    output[*logical] = Self::normalize_shape(shape)
                        .map(Some)
                        .map_err(|error| error.at_row(*logical));
                }
                output[row] = Err(GridCoverError::NativeRow { row, error });
                return Ok(output);
            },
            Err(error) => return Err(error),
        };
        for ((logical, _), shape) in source_rows.into_iter().zip(transformed) {
            output[logical] = Self::normalize_shape(shape)
                .map(Some)
                .map_err(|error| error.at_row(logical));
        }
        Ok(output)
    }

    pub(crate) fn admit_points(
        self,
        array: &PyGeometryArray,
    ) -> Result<Vec<Option<Point>>, GridCoverError> {
        let mut output = Vec::new();
        crate::try_reserve_vec(&mut output, array.len())?;
        for _ in 0..array.len() {
            crate::try_push_vec(&mut output, None)?;
        }
        let mut logical_rows = Vec::new();
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        crate::try_reserve_vec(&mut logical_rows, array.len())?;
        crate::try_reserve_pair(&mut xs, &mut ys, array.len())?;

        if array.is_compact_nullable() {
            if !matches!(
                array.storage(),
                crate::array::GeometryArrayStorage::Packed(
                    crate::array::PackedStorage::Points { .. }
                )
            ) {
                return Err(GridCoverError::Type);
            }
            for row in array.row_operation().rows_with_missing(|| ()) {
                let crate::array::ArrayRow::Present(row) = row else {
                    continue;
                };
                let logical = row.logical();
                let crate::array::ShapeRow::Point(point) = row.row() else {
                    unreachable!("compact point storage yields point rows")
                };
                crate::try_push_vec(&mut logical_rows, logical)?;
                crate::try_push_pair(&mut xs, &mut ys, point.x, point.y)?;
            }
        } else {
            let Some(points) = array.storage().point_rows() else {
                return Err(GridCoverError::Type);
            };
            for logical in 0..array.len() {
                if array.is_row_missing(logical) {
                    continue;
                }
                let point = points.get(logical);
                crate::try_push_vec(&mut logical_rows, logical)?;
                crate::try_push_pair(&mut xs, &mut ys, point.x, point.y)?;
            }
        }

        if let Some(transformer) = &self.transformer {
            transformer
                .transform_coordinates_indexed(&mut xs, &mut ys, crate::Zt::None)
                .map_err(|failure| {
                    let row = failure
                        .coordinate
                        .and_then(|rank| logical_rows.get(rank).copied());
                    let error = failure.error;
                    match row {
                        Some(row) => GridCoverError::NativeRow { row, error },
                        None => GridCoverError::Native(error),
                    }
                })?;
        }
        for (rank, &logical) in logical_rows.iter().enumerate() {
            geographic::validate_lonlat_xy(xs[rank], ys[rank]).map_err(|error| {
                GridCoverError::NativeRow {
                    row: logical,
                    error,
                }
            })?;
            output[logical] = Some(Point::new_raw_xy(xs[rank], ys[rank]));
        }
        Ok(output)
    }
}

impl Drop for GridSourceAdmission {
    fn drop(&mut self) {
        // Internal grid operations own this reprojection. Consume its
        // execution diagnostic when the admission operation ends, including
        // native errors and early returns; it is never a public warning.
        if let Some(transformer) = &self.transformer {
            let _ = transformer.execution().take_diagnostic();
        }
    }
}

/// Admit one scalar grid source at its internal operation boundary.
pub(crate) fn admit_grid_geometry(geometry: &PyGeometry) -> PyResult<PyGeometry> {
    let admission = GridSourceAdmission::new(&geometry.frame).map_err(PyErr::from)?;
    admission.admit_geometry(geometry).map_err(PyErr::from)
}

/// Fallible identity collect of cover cell ids. Prior ids drop before the
/// allocation error leaves the collector.
pub(crate) fn try_collect_cover_ids(
    values: impl IntoIterator<Item = u64>,
) -> Result<Vec<u64>, GridCoverError> {
    let mut out = Vec::new();
    for value in values {
        crate::try_push_vec(&mut out, value)?;
    }
    Ok(out)
}

/// Build canonical cell ids from an admitted membership source and its working
/// shape. Callers own source admission so tile-specific gates run before this.
pub(crate) fn build_rect_coverage_ids<S>(
    membership: &PyGeometry,
    cover_shape: &Shape,
    depth: u8,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> Result<Vec<u64>, GridCoverError>
where
    S: RectCoverSpec,
{
    if cell_rule == CellRule::Bbox {
        let windows = envelope_windows(cover_shape)?;
        let covered = coverer::cover_windows(&windows, S::roots()?, depth, max_cells)?;
        return try_collect_cover_ids(covered.into_iter().map(|cell| S::id(&cell)))
            .map(SortedCells::from_sorted)
            .map(SortedCells::into_vec);
    }
    let authority = membership.shape.as_ref();
    let authority_plan = membership.plan_or_transient()?;
    let authority_cache = membership.frame_cache()?;
    let TopologyDomain::Geographic(frame) = crate::geometry::topology_domain(&membership.frame)?
    else {
        unreachable!("normalized coverage geometry has a geographic frame")
    };
    let rule = match cell_rule {
        CellRule::Overlap => RectCoverRule::Overlap,
        CellRule::Within => RectCoverRule::Within,
        CellRule::Center => RectCoverRule::Center,
        CellRule::Bbox => unreachable!("bbox covers its envelope windows under overlap"),
    };
    let covered = coverer::cover(
        authority,
        cover_shape,
        &authority_plan,
        &authority_cache,
        frame,
        rule,
        S::roots()?,
        depth,
        max_cells,
    )?;
    try_collect_cover_ids(covered.into_iter().map(|cell| S::id(&cell)))
        .map(SortedCells::from_sorted)
        .map(SortedCells::into_vec)
}

/// Shared error and source-normalization helpers for grid factories.
pub(crate) fn empty_coverage_err(label: &str) -> PyErr {
    InvalidGeometryError::new_err(format!("{label} coverage requires a non-empty geometry"))
}

/// Convert a covering cell-budget overflow into the Python `GeometryError`
/// (the shared boundary mapping for every grid's `cover(...)` factory).
pub(crate) fn cover_budget_err(err: CoverBudgetExceeded) -> PyErr {
    integer_parameter_error(err.to_string(), "max_cells", err.limit as i64)
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

pub(crate) fn cover_working_shape(frame: &Frame, lonlat: &Shape) -> Result<Shape, GridCoverError> {
    let domain = crate::geometry::topology_domain(frame)?;
    if let TopologyDomain::Geographic(frame) = domain
        && crate::geometry::crosses_antimeridian_in_frame(lonlat, frame)
    {
        Ok(lonlat.split_antimeridian_in_frame(frame)?)
    } else {
        Ok(lonlat.clone())
    }
}

/// Derive the historical planar cover lanes' split working shape from an
/// already admitted WGS84 membership geometry.
pub(crate) fn coverage_factory_shapes(
    geometry: &PyGeometry,
    label: &'static str,
) -> Result<Shape, GridCoverError> {
    let shape = geometry.shape.as_ref();
    if shape.is_empty() {
        return Err(GridCoverError::Empty(label));
    }
    // `membership_geometry` is the admitted WGS84 source. Derive the working
    // image from that frame, never from the caller's pre-reprojection frame:
    // a projected source may become antimeridian-crossing only after this
    // conversion.
    cover_working_shape(&geometry.frame, shape)
}

/// The lon/lat windows `cell_rule='bbox'` covers: the envelope of every
/// atomic component of the admitted working shape, split in two where it
/// wraps the antimeridian. One merged envelope would flood the ground between
/// distant parts, and the band behind a seam split, with unrelated cells.
pub(crate) fn envelope_windows(
    cover_shape: &Shape,
) -> Result<Vec<crate::geometry::Bounds>, GridCoverError> {
    let mut windows = Vec::new();
    visit_grid_cover_components(cover_shape, &mut |component| {
        let Some(bounds) = component.bounds() else {
            return Ok(());
        };
        let (west, south, east, north) =
            (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
        if west > east {
            crate::try_push_vec(
                &mut windows,
                crate::geometry::Bounds::new_raw(west, south, 180.0, north),
            )?;
            crate::try_push_vec(
                &mut windows,
                crate::geometry::Bounds::new_raw(-180.0, south, east, north),
            )?;
        } else {
            crate::try_push_vec(&mut windows, bounds)?;
        }
        Ok::<_, GridCoverError>(())
    })?;
    Ok(windows)
}

/// [`envelope_windows`] as query shapes, for grids whose cells are not lon/lat
/// rectangles and so cover the windows under `'overlap'`.
pub(crate) fn envelope_window_shapes(cover_shape: &Shape) -> Result<Vec<Shape>, GridCoverError> {
    let windows = envelope_windows(cover_shape)?;
    let mut shapes = Vec::new();
    crate::try_reserve_vec(&mut shapes, windows.len())?;
    for window in windows {
        shapes.push(bounds_query_shape(window)?);
    }
    Ok(shapes)
}

fn bounds_query_shape(bounds: crate::geometry::Bounds) -> crate::error::Result<Shape> {
    // Exact identity on purpose: only literally-degenerate bounds collapse
    // to lower-dimensional query shapes; near-degenerate boxes stay boxes.
    #[expect(clippy::float_cmp, reason = "exact degenerate-bounds dispatch")]
    if bounds.minx() == bounds.maxx() && bounds.miny() == bounds.maxy() {
        Ok(Shape::Point(Point::new_raw_xy(
            bounds.minx(),
            bounds.miny(),
        )))
    } else if bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy() {
        Ok(Shape::LineString(LineSeq::segment(
            Point::new_raw_xy(bounds.minx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.maxy()),
        )?))
    } else {
        Ok(Shape::Polygon(crate::box_polygon(
            bounds.minx(),
            bounds.miny(),
            bounds.maxx(),
            bounds.maxy(),
        )?))
    }
}

/// Build a `CellArray` from source cell ids through the public factory path.
pub(crate) fn rect_cell_array_for<S: RectCoverSpec>(
    cells: impl IntoIterator<Item = S::Cell>,
) -> PyResult<PyCellArray> {
    cell_array_from_keys(S::KIND, cells)
}

#[cfg(test)]
mod tests {
    use super::{GridCoverError, GridSourceAdmission, SortedCells, try_collect_cover_ids};
    use crate::Frame;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geographic::{GeographicFrame, ScalarGeographicView};
    use crate::geometry::{GeometryErrorKind, Point, Shape};
    use crate::grid::CoverError;

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

    #[test]
    fn scalar_empty_is_empty_coverage() {
        let geometry = crate::PyGeometry::wgs84(Shape::empty_polygon()).unwrap();
        let admission = GridSourceAdmission::new(&geometry.frame).unwrap();
        let admitted = admission.admit_geometry(&geometry).unwrap();
        let error = super::coverage_factory_shapes(&admitted, "H3").expect_err("empty scalar");
        assert!(matches!(error, GridCoverError::Empty("H3")), "{error:?}");
    }

    #[test]
    fn invalid_lonlat_is_native_geometry() {
        let geometry =
            crate::PyGeometry::wgs84(Shape::Point(Point::new_raw_xy(0.0, 95.0))).unwrap();
        let admission = GridSourceAdmission::new(&geometry.frame).unwrap();
        let error = admission
            .admit_geometry(&geometry)
            .expect_err("out of domain");
        let GridCoverError::Native(error) = error else {
            panic!("expected native lonlat error, got {error:?}");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        assert!(
            error
                .to_string()
                .contains("invalid longitude/latitude (0, 95)"),
            "{error}"
        );
    }

    #[test]
    fn internal_admission_drop_consumes_live_transform_diagnostic() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let geometry = crate::PyGeometry::with_frame(
            Shape::Point(Point::new_raw_xy(-104.0, 40.0)),
            Frame::Crs(crate::boundary::crs_arc_static("EPSG:4267")),
        )
        .unwrap();
        let admission = GridSourceAdmission::new(&geometry.frame).unwrap();
        let execution = admission
            .transformer
            .as_ref()
            .expect("tagged source has a transform")
            .execution();

        admission
            .transformer
            .as_ref()
            .expect("tagged source has a transform")
            .transform_shape(geometry.shape.shape())
            .unwrap();
        assert!(execution.diagnostic().is_some());

        drop(admission);
        assert!(execution.diagnostic().is_none());
    }

    #[test]
    fn geographic_view_keeps_boxed_diagnostic() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 91.0));
        let error = ScalarGeographicView::try_new(GeographicFrame::crs84(), &shape)
            .expect_err("latitude is outside the physical axis");
        let mapped = GridCoverError::from(error);
        let GridCoverError::Geographic(diagnostic) = mapped else {
            panic!("expected geographic diagnostic, got {mapped:?}");
        };
        let message = diagnostic.to_string();
        assert!(
            message.contains("geographic shape") && message.contains("latitude 91"),
            "{message}"
        );
    }

    #[test]
    fn cover_error_preparation_is_native() {
        let error = GridCoverError::from(CoverError::Preparation(
            crate::geometry::GeometryErrorKind::message("prepared"),
        ));
        assert!(matches!(error, GridCoverError::Native(_)), "{error:?}");
    }

    #[test]
    fn identity_id_collect_allocation_is_native_resource() {
        let error = crate::test_support::fail_allocations_after(0, || {
            try_collect_cover_ids([1_u64, 2, 3]).expect_err("id collect allocation must fail")
        });
        assert!(
            matches!(&error, GridCoverError::Native(inner) if matches!(
                inner.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            )),
            "{error:?}"
        );
    }
}
