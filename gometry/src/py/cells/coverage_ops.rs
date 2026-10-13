//! Grid factory and rectangular-grid cell-array helpers.
use pyo3::prelude::*;

use crate::error::{Error, ResourceError};
use crate::geographic::{
    GeographicDiagnostic, GeographicFrame, GeographicViewError, ScalarGeographicView,
    lonlat as geographic,
};
use crate::geometry::{CoordSeq, Shape, TopologyDomain};
use crate::grid::affine_source::normalize_grid_source;
use crate::grid::cell::{HierarchicalCell, RectGridCell};
use crate::grid::coverer::{self, RectCoverRule};
use crate::grid::{CoverBudgetExceeded, CoverError};
use crate::py::cells::cell_ops::cell_array_from_keys;
use crate::py::cells::{CellRule, GridKind, PyCellArray, coverage_to_polygon};
use crate::py::errors;
use crate::py::errors::{GeometryError, InvalidGeometryError, integer_parameter_error};
use crate::{Frame, GeometryTransformFrame, PyGeometry, PyGeometryArray, Typed, crs, crs_arc};

/// Cell ids in the order required by dissolve membership searches.
///
/// This is deliberately separate from cell-set normalization: dissolve needs
/// `Ord` order and strict uniqueness, while hierarchical set operations use
/// each grid's descendant range-key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SortedCells<C>(Vec<C>);

impl<C: Ord> SortedCells<C> {
    pub(crate) fn new(mut cells: Vec<C>) -> Self {
        if !cells.array_windows::<2>().all(|[a, b]| a < b) {
            cells.sort_unstable();
            cells.dedup();
        }
        Self(cells)
    }

    pub(crate) fn from_sorted(cells: Vec<C>) -> Self {
        // Producers use this fast path only after proving strict order, but
        // keep the boundary total so a future producer cannot ship an invalid
        // SortedCells value in release builds.
        if !cells.array_windows::<2>().all(|[a, b]| a < b) {
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
            let shapes = prepared
                .as_slice()
                .iter()
                .copied()
                .map(|cell| D::fallback_shape(cell).map_err(PyErr::from))
                .collect::<PyResult<Vec<_>>>()?;
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
        let mut outline = Vec::with_capacity(capacity);
        let mut crosses_seam = false;
        'scan: for &cell in prepared.as_slice() {
            for segment in D::exterior_edge_segments(cell, &is_member)? {
                if D::crosses_seam(&segment) {
                    crosses_seam = true;
                    break 'scan;
                }
                outline.push(segment);
            }
        }
        if !crosses_seam && let Some(typed) = super::grid_outline::polygonize_outline(outline)? {
            return Ok(typed);
        }
        prepared
    } else {
        cells
    };
    let shapes = fallback_cells
        .as_slice()
        .iter()
        .copied()
        .map(|cell| D::fallback_shape(cell).map_err(PyErr::from))
        .collect::<PyResult<Vec<_>>>()?;
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
            Shape::MultiPolygon(parts) => {
                polygons.reserve(parts.len());
                polygons.extend(parts);
            },
            // A cell boundary split at the antimeridian is polygonal; nothing
            // else reaches here, and an empty result is the error below.
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => {},
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

pub(crate) trait RectCoverSpec {
    type Cell: RectGridCell + HierarchicalCell;

    const KIND: GridKind;

    fn roots() -> Vec<Self::Cell>;
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
        Self::Geographic(error)
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
            let error = geographic::geographic_admission_error(&diagnostic);
            drop(diagnostic);
            error.into()
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
        let transformer = crs::TransformPlan::from_geometry_frame(resolved)?;
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
        transformer: &crs::TransformPlan,
        source_rows: &[(usize, Shape)],
    ) -> Result<Vec<Shape>, GridCoverError> {
        let mut shapes = Vec::with_capacity(source_rows.len());
        shapes.extend(source_rows.iter().map(|(_, shape)| shape));
        transformer
            .transform_shapes_with_computed_edges_indexed(&shapes)
            .map_err(|failure| {
                let row = failure
                    .coordinate
                    .and_then(|rank| source_rows.get(rank).map(|(logical, _)| *logical));
                let error = failure.error;
                match row {
                    Some(row) => GridCoverError::NativeRow { row, error },
                    None => GridCoverError::Native(error),
                }
            })
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
            transformer.transform_shape_with_computed_edges(geometry.shape.shape())?
        } else {
            geometry.shape.shape().clone()
        };
        Ok(PyGeometry::wgs84(Self::normalize_shape(shape)?))
    }

    pub(crate) fn admit_array_shapes(
        self,
        array: &PyGeometryArray,
    ) -> Result<Vec<Result<Option<Shape>, GridCoverError>>, GridCoverError> {
        let present_count = array
            .missing()
            .map_or_else(|| array.len(), |mask| mask.present_count());
        let mut source_rows = Vec::with_capacity(present_count);
        for row in array.row_operation().present_rows() {
            let logical = row.logical();
            let shape =
                row.row()
                    .with_shape(Clone::clone)
                    .map_err(|error| GridCoverError::NativeRow {
                        row: logical,
                        error,
                    })?;
            source_rows.push((logical, shape));
        }
        let mut output = Vec::with_capacity(array.len());
        output.resize_with(array.len(), || Ok(None));
        let mut transformed = if let Some(transformer) = &self.transformer {
            match Self::transform_shapes(transformer, &source_rows) {
                Ok(transformed) => Some(transformed.into_iter()),
                Err(GridCoverError::NativeRow { row, error }) => {
                    let rank = source_rows
                        .iter()
                        .position(|(logical, _)| *logical == row)
                        .expect("transform failure row came from the source batch");
                    let source_prefix = source_rows
                        .get(..rank)
                        .expect("transform failure rank belongs to source rows");
                    let prefix = Self::transform_shapes(transformer, source_prefix)?;
                    for ((logical, _), shape) in source_prefix.iter().zip(prefix) {
                        *output
                            .get_mut(*logical)
                            .expect("source logical row fits admission output") =
                            Self::normalize_shape(shape)
                                .map(Some)
                                .map_err(|error| error.at_row(*logical));
                    }
                    *output
                        .get_mut(row)
                        .expect("transform failure row fits admission output") =
                        Err(GridCoverError::NativeRow { row, error });
                    return Ok(output);
                },
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        for (logical, source) in source_rows {
            let shape = transformed.as_mut().map_or(source, |shapes| {
                shapes.next().expect("one transformed shape per source row")
            });
            *output
                .get_mut(logical)
                .expect("source logical row fits admission output") = Self::normalize_shape(shape)
                .map(Some)
                .map_err(|error| error.at_row(logical));
        }
        Ok(output)
    }

    pub(crate) fn admit_bounding_collection(
        self,
        array: &PyGeometryArray,
    ) -> Result<Shape, GridCoverError> {
        let mut parts = Vec::new();
        for shape in self.admit_array_shapes(array)? {
            if let Some(shape) = shape?
                && !shape.is_empty()
            {
                parts.push(shape);
            }
        }
        match parts.len() {
            0 => Err(crate::geometry::GeometryErrorKind::message(
                "bounding cell requires a non-empty geometry",
            )
            .into()),
            1 => Ok(parts.pop().expect("one shape")),
            _ => Ok(Shape::geometry_collection(parts)?),
        }
    }

    pub(crate) fn admit_point_columns(
        array: &PyGeometryArray,
    ) -> Result<(Vec<f64>, Vec<f64>), GridCoverError> {
        let points = array.storage().point_rows().ok_or(GridCoverError::Type)?;
        let admission = Self::new_for_array(array)?;
        let present_count = array
            .missing()
            .map_or_else(|| array.len(), |mask| mask.present_count());
        let mut xs = Vec::with_capacity(present_count);
        let mut ys = Vec::with_capacity(present_count);
        if admission.transformer.is_none() {
            for logical in 0..array.len() {
                if array.is_row_missing(logical) {
                    continue;
                }
                let row = if array.is_compact_nullable() {
                    xs.len()
                } else {
                    logical
                };
                let point = points.get(row);
                geographic::validate_lonlat_xy(point.x, point.y).map_err(|error| {
                    GridCoverError::NativeRow {
                        row: logical,
                        error,
                    }
                })?;
                xs.push(point.x);
                ys.push(point.y);
            }
            return Ok((xs, ys));
        }
        for logical in 0..array.len() {
            if array.is_row_missing(logical) {
                continue;
            }
            let row = if array.is_compact_nullable() {
                xs.len()
            } else {
                logical
            };
            let point = points.get(row);
            xs.push(point.x);
            ys.push(point.y);
        }

        if let Some(transformer) = &admission.transformer {
            transformer
                .transform_coordinates_indexed(&mut xs, &mut ys, crate::Zt::None)
                .map_err(|failure| {
                    let row = failure.coordinate.and_then(|rank| {
                        array.missing().map_or_else(
                            || (rank < array.len()).then_some(rank),
                            |mask| {
                                mask.cursor().find_map(|(logical, present)| {
                                    (present == Some(rank)).then_some(logical)
                                })
                            },
                        )
                    });
                    let error = failure.error;
                    match row {
                        Some(row) => GridCoverError::NativeRow { row, error },
                        None => GridCoverError::Native(error),
                    }
                })?;
        }
        let mut coordinates = xs.iter().zip(&ys);
        for logical in 0..array.len() {
            if array.is_row_missing(logical) {
                continue;
            }
            let (&x, &y) = coordinates.next().expect("one coordinate per present row");
            geographic::validate_lonlat_xy(x, y).map_err(|error| GridCoverError::NativeRow {
                row: logical,
                error,
            })?;
        }
        Ok((xs, ys))
    }
}

impl Drop for GridSourceAdmission {
    fn drop(&mut self) {
        // Internal grid operations own this reprojection. Consume its
        // execution diagnostic when the admission operation ends, including
        // native errors and early returns; it is never a public warning.
        if let Some(transformer) = &self.transformer {
            drop(transformer.execution().take_diagnostic());
        }
    }
}

/// Admit one scalar grid source at its internal operation boundary.
pub(crate) fn admit_grid_geometry(geometry: &PyGeometry) -> PyResult<PyGeometry> {
    let admission = GridSourceAdmission::new(&geometry.frame).map_err(PyErr::from)?;
    admission.admit_geometry(geometry).map_err(PyErr::from)
}

pub(crate) fn collect_cover_ids(values: impl IntoIterator<Item = u64>) -> Vec<u64> {
    values.into_iter().collect()
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
        let windows = envelope_windows(cover_shape);
        let covered = coverer::cover_windows(&windows, S::roots(), depth, max_cells)?;
        return Ok(SortedCells::from_sorted(collect_cover_ids(
            covered.into_iter().map(|cell| S::id(&cell)),
        ))
        .into_vec());
    }
    let authority = membership.shape.as_ref();
    let authority_plan = membership.prepared_plan();
    let authority_cache = membership.frame_cache();
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
        S::roots(),
        depth,
        max_cells,
    )?;
    Ok(SortedCells::from_sorted(collect_cover_ids(
        covered.into_iter().map(|cell| S::id(&cell)),
    ))
    .into_vec())
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
    // Cell membership observes XY only; this temporary working image never
    // publishes source ordinates.
    let lonlat = lonlat.force_2d()?;
    if let TopologyDomain::Geographic(frame) = domain
        && crate::geometry::crosses_antimeridian_in_frame(&lonlat, frame)
    {
        Ok(lonlat.split_antimeridian_in_frame(frame)?)
    } else {
        Ok(lonlat)
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
pub(crate) fn envelope_windows(cover_shape: &Shape) -> Vec<crate::geometry::Bounds> {
    let mut windows = Vec::new();
    cover_shape
        .try_for_each_first_component(&mut |component| {
            let Some(bounds) = component.to_shape().bounds() else {
                return Ok(());
            };
            let (west, south, east, north) =
                (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
            if west > east {
                windows.push(crate::geometry::Bounds::new_raw(west, south, 180.0, north));
                windows.push(crate::geometry::Bounds::new_raw(-180.0, south, east, north));
            } else {
                windows.push(bounds);
            }
            Ok::<_, std::convert::Infallible>(())
        })
        .expect("component windows cannot fail");
    windows
}

/// [`envelope_windows`] as query shapes, for grids whose cells are not lon/lat
/// rectangles and so cover the windows under `'overlap'`.
pub(crate) fn envelope_window_shapes(cover_shape: &Shape) -> Result<Vec<Shape>, GridCoverError> {
    let windows = envelope_windows(cover_shape);
    let mut shapes = Vec::with_capacity(windows.len());
    for window in windows {
        shapes.push(bounds_query_shape(window)?);
    }
    Ok(shapes)
}

fn bounds_query_shape(bounds: crate::geometry::Bounds) -> crate::error::Result<Shape> {
    crate::geometry::geographic_bounds_chart(bounds, crate::geographic::GeographicFrame::crs84())
}

/// Build a `CellArray` from source cell ids through the public factory path.
pub(crate) fn rect_cell_array_for<S: RectCoverSpec>(
    cells: impl IntoIterator<Item = S::Cell>,
) -> PyCellArray {
    cell_array_from_keys(S::KIND, cells)
}

#[cfg(test)]
mod tests {
    use super::{GridCoverError, GridSourceAdmission, SortedCells};
    use crate::Frame;
    use crate::crs::frame;
    use crate::error::ErrorKind;
    use crate::geographic::{GeographicFrame, ScalarGeographicView};
    use crate::geometry::{CoordSeq, GeometryErrorKind, LineSeq, Point, Shape};
    use crate::grid::CoverError;

    #[test]
    fn envelope_windows_preserve_distinct_occurrences_and_reduce_shared_owners() {
        fn line(west: f64, east: f64) -> Shape {
            Shape::LineString(
                LineSeq::segment(Point::new_raw_xy(west, 0.0), Point::new_raw_xy(east, 0.0))
                    .expect("valid test line"),
            )
        }

        let source = Shape::geometry_collection(vec![line(-5.0, -4.0), line(4.0, 5.0)])
            .expect("distinct envelope components");
        let mut expanded = Vec::new();
        source.for_each_component(&mut |component| {
            if let Some(bounds) = component.to_shape().bounds() {
                expanded.push(bounds);
            }
        });
        assert_eq!(super::envelope_windows(&source), expanded);

        let mut shared = line(-5.0, -4.0);
        for _ in 0..9 {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection node");
        }
        let mut occurrences = 0;
        shared.for_each_component(&mut |_| occurrences += 1);
        assert_eq!(occurrences, 1 << 9);
        let mut first_owner_components = 0;
        shared
            .try_for_each_first_component(&mut |_| {
                first_owner_components += 1;
                Ok::<_, std::convert::Infallible>(())
            })
            .expect("component traversal cannot fail");
        assert_eq!(first_owner_components, 2);
        assert_eq!(super::envelope_windows(&shared).len(), 2);
    }

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
        let geometry = crate::PyGeometry::wgs84(Shape::empty_polygon());
        let admission = GridSourceAdmission::new(&geometry.frame).unwrap();
        let admitted = admission.admit_geometry(&geometry).unwrap();
        let error = super::coverage_factory_shapes(&admitted, "H3").expect_err("empty scalar");
        assert!(matches!(error, GridCoverError::Empty("H3")), "{error:?}");
    }

    #[test]
    fn invalid_lonlat_is_native_geometry() {
        let geometry = crate::PyGeometry::wgs84(Shape::Point(Point::new_raw_xy(0.0, 95.0)));
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
    fn source_error_prefix_uses_the_paired_transform_publisher() {
        let line = |points: &[Point]| {
            Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).unwrap())
        };
        let array = crate::PyGeometryArray::from_shapes(
            vec![
                line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(360.0, 0.0)]),
                line(&[Point::new_raw_xy(0.0, 91.0), Point::new_raw_xy(10.0, 91.0)]),
            ],
            Frame::Crs(frame::crs_arc_static("EPSG:4267")),
        )
        .unwrap();
        let admission = GridSourceAdmission::new_for_array(&array).unwrap();
        let rows = admission.admit_array_shapes(&array).unwrap();

        let Some(prefix) = rows[0].as_ref().unwrap() else {
            panic!("the valid prefix row is present");
        };
        let Shape::LineString(prefix) = prefix else {
            panic!("the valid prefix retains its line type");
        };
        assert_eq!(prefix.len(), 5);
        // PROJ maps both endpoints to 0°; the source pair retains the turn.
        assert_eq!(prefix.iter().collect::<Vec<_>>(), [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(90.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(-90.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
        assert!(matches!(
            rows[1].as_ref(),
            Err(GridCoverError::NativeRow { row: 1, .. })
        ));
    }

    #[test]
    fn internal_admission_drop_consumes_live_transform_diagnostic() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::crs::clear_cache();
        let geometry = crate::PyGeometry::with_frame(
            Shape::Point(Point::new_raw_xy(-104.0, 40.0)),
            Frame::Crs(frame::crs_arc_static("EPSG:4267")),
        );
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
}
