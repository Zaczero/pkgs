//! Planar set-overlay free functions and the `OverlayOp` dispatch enum.
//!
//! One `OverlayOp` (Intersection/Union/Difference/SymmetricDifference) carries
//! `#[pymethods]` (in the crate root), the array methods, and these free
//! functions all share a single overlay contract. Reached from the crate root
//! via `pub use overlay::*`.

use pyo3::prelude::*;
use pyo3::types::PyAny;

pub(crate) use crate::geometry::OverlayOp;
use crate::{
    Frame, GeometryErrorKind, PyGeometry, PyGeometryArray, Shape, SimplifyMethod, Typed,
    exact_geometry, exact_geometry_array, geometry,
};

fn pooled_shapes(values: &Bound<'_, PyAny>) -> PyResult<(Frame, Vec<Shape>)> {
    if let Some(array) = exact_geometry_array(values) {
        return Ok((array.frame.clone(), array.present_shapes()));
    }
    if let Some(geometry) = exact_geometry(values) {
        return Ok((geometry.frame.clone(), vec![geometry.shape.shape().clone()]));
    }
    let array = PyGeometryArray::new(values, None, None)?;
    Ok((array.frame.clone(), array.present_shapes()))
}

/// The shared tail of the n-ary reductions (`union_all` general path,
/// `intersection_all`, `symmetric_difference_all`): collect the input shapes
/// under one shared frame and fold them with `kernel` (GIL released). The four
/// reductions differ only in that kernel. Aggregates are always lenient
/// (missing rows skipped; SQL/pandas semantics). Geographic frames pass
/// `geographic=true` so antimeridian-crossing operands are split first —
/// the same gate binary overlay uses.
fn reduce_all(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    kernel: impl FnOnce(&[Shape], bool, crate::geometry::Strictness) -> crate::error::Result<Shape>
    + Send,
) -> PyResult<Typed> {
    let (metadata, shapes) = pooled_shapes(values)?;
    let geographic = crate::geometry::is_geographic_frame(&metadata);
    let shape =
        py.detach(move || kernel(&shapes, geographic, crate::geometry::Strictness::Lenient))?;
    Ok(PyGeometry::typed_with_epoch(
        shape,
        metadata.crs_owned(),
        metadata.epoch(),
    ))
}

/// The `&`/`|`/`-`/`^` operator entry shared by the `Geometry` and
/// `GeometryArray` dunders: the overlay broadcast with the fixed ordinate
/// behavior, returning `NotImplemented` for foreign operands so Python's
/// binary-operator protocol stays intact.
pub(crate) fn overlay_operator(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op: OverlayOp,
) -> PyResult<Py<PyAny>> {
    if exact_geometry(right).is_none() && exact_geometry_array(right).is_none() {
        return Ok(py.NotImplemented());
    }
    crate::dispatch::dispatch_overlay(py, left, right, op)
}

/// Compute the set-theoretic intersection of ``left`` and ``right``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the overlay cannot be constructed.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> left, right = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
/// >>> gm.intersection(left, right).to_wkt()
/// 'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'
#[pyfunction]
pub(crate) fn intersection(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_overlay(py, left, right, OverlayOp::Intersection)
}

/// Compute the set-theoretic union of ``left`` and ``right``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the overlay cannot be constructed.
///
/// See Also
/// --------
/// union_all : Union of many geometries at once.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.union(gm.box(0, 0, 1, 2), gm.box(1, 0, 2, 2)).to_wkt()
/// 'POLYGON ((0 0, 1 0, 2 0, 2 2, 1 2, 0 2, 0 0))'
#[pyfunction]
pub(crate) fn union(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_overlay(py, left, right, OverlayOp::Union)
}

/// Compute the set-theoretic difference ``left - right``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Geometry to subtract from (``left``) and geometry subtracted
///     (``right``). Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the overlay cannot be constructed.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> left, right = gm.box(0, 0, 2, 1), gm.box(1, 0, 2, 1)
/// >>> gm.difference(left, right).to_wkt()
/// 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
#[pyfunction]
pub(crate) fn difference(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_overlay(py, left, right, OverlayOp::Difference)
}

/// Compute the set-theoretic symmetric difference of ``left`` and
/// ``right``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the overlay cannot be constructed.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> left, right = gm.box(0, 0, 2, 1), gm.box(1, 0, 3, 1)
/// >>> gm.symmetric_difference(left, right).area
/// 2.0
#[pyfunction]
pub(crate) fn symmetric_difference(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_overlay(py, left, right, OverlayOp::SymmetricDifference)
}

/// Union of many geometries into one (planar overlay).
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One geometry or geometries to union. Missing array rows are skipped.
///
/// Returns
/// -------
/// Geometry
///     A single geometry that is the union of all inputs.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If ``values`` is empty or the overlay cannot be constructed.
///
/// See Also
/// --------
/// coverage_union : Faster dissolve for a valid polygonal coverage.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> panes = [gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
/// >>> gm.union_all(panes).to_wkt()
/// 'POLYGON ((0 0, 2 0, 2 1, 3 1, 3 3, 1 3, 1 2, 0 2, 0 0))'
#[pyfunction]
pub(crate) fn union_all(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Typed> {
    reduce_all(py, values, Shape::union_all_topo)
}

/// Union a polygonal *coverage* into one geometry by dissolving shared edges.
///
/// A fast specialization of ``union_all`` for a **valid
/// coverage** — polygons that tile a region edge-to-edge with no overlaps and
/// no T-junctions (see ``coverage_is_valid``). It
/// cancels the interior edges shared between adjacent polygons rather than
/// noding and classifying every intersection, so it is dramatically faster on
/// tilings (grids, parcels, administrative boundaries). On overlapping or
/// badly-noded input raises before the specialized dissolve runs; use
/// `union_all` there.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One polygonal row or multiple rows of a valid coverage. Missing array
///     rows are skipped.
///
/// Returns
/// -------
/// Geometry
///     A single `Polygon`/`MultiPolygon` covering the merged area.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If ``values`` is empty or the rows are not a valid coverage.
///
/// See Also
/// --------
/// union_all : General multi-geometry union (handles overlaps).
/// coverage_is_valid : Whether the rows form a valid polygonal coverage.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> tiles = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
/// >>> gm.coverage_union(tiles).normalize().to_wkt()
/// 'POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))'
#[pyfunction]
pub(crate) fn coverage_union(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Typed> {
    let (shapes, metadata, _) = coverage_values(values)?;
    if shapes.is_empty() {
        // Reductions require an operand, consistent with `union_all`.
        return Err(
            crate::error::Error::from(GeometryErrorKind::EmptyGeometrySequence {
                operation: "coverage_union",
            })
            .into(),
        );
    }
    let geographic = crate::geometry::is_geographic_frame(&metadata);
    let shape = py.detach(move || crate::geometry::coverage_union_topo(&shapes, geographic))?;
    Ok(PyGeometry::typed_with_epoch(
        shape,
        metadata.crs_owned(),
        metadata.epoch(),
    ))
}

/// Build polygons from the pooled linework of many geometries.
///
/// ALL inputs' edges are noded into ONE planar graph, so a ring can close from
/// edges spread across different inputs — the canonical "reconstruct polygons
/// from this pile of noded lines" call. Returns every enclosed face as a
/// `GeometryArray` of polygons.
///
/// This is the whole-collection aggregate. To polygonize each geometry's own
/// linework independently (one polygon set per input), use the per-element
/// method `GeometryArray.polygonize` (which returns `Groups`) or
/// `Geometry.polygonize` on a single geometry.
///
/// Parameters
/// ----------
/// values : Geometry or iterable of Geometry
///     One geometry or linework values to node and polygonize together.
///
/// Returns
/// -------
/// GeometryArray
///     The polygons enclosed by the pooled linework.
///
/// See Also
/// --------
/// polygonize_full : The same pooled aggregate plus dangles, cut edges, and invalid-ring diagnostics.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.polygonize([
/// ...     gm.LineString([(0, 0), (1, 0)]),
/// ...     gm.LineString([(1, 0), (1, 1)]),
/// ...     gm.LineString([(1, 1), (0, 0)]),
/// ... ]).to_wkt()
/// ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
pub(crate) fn polygonize(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<PyGeometryArray> {
    let (metadata, shapes) = pooled_shapes(values)?;
    let polygons = py.detach(move || {
        let refs: Vec<&Shape> = shapes.iter().collect();
        Shape::polygonize_all(&refs, false)
    })?;
    Ok(PyGeometryArray::from_shapes(polygons, metadata))
}

/// Polygonize pooled linework and return full diagnostics.
///
/// Like ``polygonize``, all inputs form ONE planar graph,
/// including rings whose edges span multiple input geometries. In addition to
/// polygons, returns cut edges, dangles, and invalid rings. Passing a
/// `GeometryArray` pools its present rows; `array.polygonize()` remains the
/// independent row-wise spelling.
///
/// Parameters
/// ----------
/// values : Geometry or iterable of Geometry
///     One geometry or linework values to node and polygonize together.
///
/// Returns
/// -------
/// PolygonizeResult
///     Pooled polygons, cut edges, dangles, and invalid rings.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> result = gm.polygonize_full([
/// ...     gm.LineString([(0, 0), (1, 0)]),
/// ...     gm.LineString([(1, 0), (1, 1)]),
/// ...     gm.LineString([(1, 1), (0, 0)]),
/// ... ])
/// >>> result.polygons.to_wkt()
/// ['POLYGON ((0 0, 1 0, 1 1, 0 0))']
pub(crate) fn polygonize_full(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let (metadata, shapes) = pooled_shapes(values)?;
    let full = py.detach(move || {
        let refs: Vec<&Shape> = shapes.iter().collect();
        Shape::polygonize_full_all(&refs, false)
    })?;
    crate::py_polygonize_full(py, full, metadata.crs_ref(), metadata.epoch())
}

/// Common intersection of many geometries (planar overlay).
///
/// The region inside EVERY input — `g0 ∩ g1 ∩ … ∩ gn`. Reduces the sequence
/// with the pairwise ``intersection``, so mixed
/// dimensions narrow to the lowest shared dimension and an empty result keeps
/// that dimension's typed empty.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One geometry or geometries to intersect. Missing array rows are skipped.
///
/// Returns
/// -------
/// Geometry
///     A single geometry that is the intersection of all inputs.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If ``values`` is empty or the overlay cannot be constructed.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> panes = [gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5)]
/// >>> gm.intersection_all(panes).to_wkt()
/// 'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
#[pyfunction]
pub(crate) fn intersection_all(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Typed> {
    reduce_all(py, values, Shape::intersection_all_topo)
}

/// Symmetric difference of many geometries (planar overlay).
///
/// The region covered by an ODD number of inputs — `g0 ▵ g1 ▵ … ▵ gn`. Reduces
/// the sequence with the pairwise
/// ``symmetric_difference``; the fold is
/// order-independent (`▵` is associative and commutative).
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One geometry or geometries to combine. Missing array rows are skipped.
///
/// Returns
/// -------
/// Geometry
///     A single geometry covered by an odd number of inputs.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If ``values`` is empty or the overlay cannot be constructed.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> panes = [gm.box(0, 0, 2, 2), gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]
/// >>> gm.symmetric_difference_all(panes).to_wkt()  # the duplicate cancels
/// 'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))'
#[pyfunction]
pub(crate) fn symmetric_difference_all(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
) -> PyResult<Typed> {
    reduce_all(py, values, Shape::symmetric_difference_all_topo)
}

/// Test whether geometries form a valid polygonal coverage.
///
/// A polygonal coverage (parcels, admin boundaries) is a set of polygons
/// with no interior overlaps whose shared
/// boundaries use vector-identical linework. Distinct from the DGGS cell
/// `Coverage` classes.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
///
/// gap_width : float, default 0.0
///     Gap width in coordinate units; 0 disables gap detection.
///
/// Returns
/// -------
/// bool
///     ``True`` when no row has invalid coverage edges.
///
/// Raises
/// ------
/// GeometryTypeError
///     If a row is not a `Polygon` or `MultiPolygon`.
/// GeometryError
///     If ``gap_width`` is negative or non-finite.
/// CRSMismatchError
///     If the rows' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// coverage_invalid_edges : The offending linework itself.
/// coverage_clean : Rebuild an exact coverage from a near-coverage.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> grid = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
/// >>> gm.coverage_is_valid(grid)
/// True
/// >>> gm.coverage_is_valid([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)])
/// False
#[pyfunction]
#[pyo3(signature = (values, *, gap_width = 0.0))]
pub(crate) fn coverage_is_valid(values: &Bound<'_, PyAny>, gap_width: f64) -> PyResult<bool> {
    let (shapes, ..) = coverage_values(values)?;
    Ok(geometry::coverage_is_valid(&shapes, gap_width)?)
}

/// Per-row invalid coverage boundary linework.
///
/// Each output row merges the row's boundary segments that violate the
/// coverage contract (overlaps, T-joins, slivers, and — with ``gap_width``
/// — narrow gaps); rows that are clean come back as ``LINESTRING EMPTY``.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
///
/// gap_width : float, default 0.0
///     Gap width in coordinate units; 0 disables gap detection.
///
/// Returns
/// -------
/// GeometryArray
///     One `LineString`/`MultiLineString` per input row.
///
/// Raises
/// ------
/// GeometryTypeError
///     If a row is not a `Polygon` or `MultiPolygon`.
/// GeometryError
///     If ``gap_width`` is negative or non-finite.
/// CRSMismatchError
///     If the rows' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// coverage_is_valid : The boolean verdict.
#[pyfunction]
#[pyo3(signature = (values, *, gap_width = 0.0))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.coverage_invalid_edges(
/// ...     [gm.box(0, 0, 1, 1), gm.box(0.5, 0, 1.5, 1)]))
/// 2
pub(crate) fn coverage_invalid_edges(
    values: &Bound<'_, PyAny>,
    gap_width: f64,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let edges = geometry::coverage_invalid_edges(&shapes, gap_width)?;
    let result = PyGeometryArray::from_shapes(edges, metadata);
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result),
    })
}

/// Simplify a VALID polygonal coverage's boundaries, preserving its
/// topology.
///
/// Shared boundaries are simplified once and spliced into both neighbors,
/// so the result stays an exact coverage (vector-identical interfaces, no
/// new crossings — a conservative topology guard rejects any vertex removal
/// that would intersect other linework or swallow a neighbor). Input must
/// be a valid coverage; invalid rows raise with repair guidance.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
/// tolerance : float
///     Distance-scale simplification tolerance, in coordinate units;
///     non-negative finite. Read on the same scale by both methods (the
///     Visvalingam-Whyatt effective area is ``tolerance**2 / 2``).
/// method : {'vw', 'dp'}, default 'vw'
///     Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
///     ``'dp'`` is distance-based (Douglas-Peucker). Both run under the same
///     topology guard.
/// simplify_boundary : bool, default True
///     Also simplify exterior (unshared) boundaries; ``False`` pins them
///     and simplifies only the shared interfaces.
///
/// Returns
/// -------
/// GeometryArray
///     One simplified `Polygon`/`MultiPolygon` per input row.
///
/// Raises
/// ------
/// GeometryTypeError
///     If a row is not a `Polygon` or `MultiPolygon`.
/// GeometryError
///     If ``tolerance`` is negative or non-finite.
/// CRSMismatchError
///     If the rows' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the rows are not a valid polygonal coverage.
///
/// See Also
/// --------
/// Geometry.simplify : Per-geometry simplify (not coverage-topology-preserving).
/// coverage_is_valid : Whether the rows form a valid polygonal coverage.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> left = gm.Polygon([(0, 0), (1, 0), (1.05, 0.5), (1, 1), (0, 1)])
/// >>> right = gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.05, 0.5)])
/// >>> out = gm.coverage_simplify([left, right], 0.5)
/// >>> out.to_wkt()[0]
/// 'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))'
#[pyfunction]
#[pyo3(
    signature = (values, tolerance, *, method = SimplifyMethod::Vw, simplify_boundary = true),
    text_signature = "(values, tolerance, *, method='vw', simplify_boundary=True)"
)]
pub(crate) fn coverage_simplify(
    values: &Bound<'_, PyAny>,
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let rows = geometry::coverage_simplify(&shapes, tolerance, method, simplify_boundary)?;
    let result = PyGeometryArray::from_shapes(rows, metadata);
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result),
    })
}

/// Clean a near-coverage into an exact polygonal coverage.
///
/// Optionally snaps vertices, nodes every boundary, then assigns each face
/// of the arrangement to exactly one row: overlaps go to the row chosen by
/// ``overlap_rule``, and enclosed gaps narrower than ``gap_width`` merge into
/// the neighbor with the longest shared border. Both sides of every
/// interface come from the same noded linework, so the result is an exact
/// coverage by construction.
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of Geometry
///     One coverage row or multiple rows (`Polygon` or `MultiPolygon` each).
/// grid_size : float, default 0.0
///     Vertex snapping grid in coordinate units; ``0`` preserves input
///     coordinates and disables snapping.
///
/// gap_width : float, default 0.0
///     Merge enclosed gaps narrower than this coordinate-space width (0 keeps
///     gaps).
/// overlap_rule : str, default 'longest_border'
///     Which row keeps a region covered more than once:
///     ``'longest_border'``, ``'max_area'``, ``'min_area'``, or
///     ``'min_index'``.
///     Cleaning rebuilds faces and returns their natural 2D geometry.
///
/// Returns
/// -------
/// GeometryArray
///     One cleaned `Polygon`/`MultiPolygon` per input row.
///
/// Raises
/// ------
/// GeometryTypeError
///     If a row is not a `Polygon` or `MultiPolygon`.
/// GeometryError
///     If ``grid_size`` or ``gap_width`` is negative or non-finite.
/// InvalidGeometryError
///     If ``grid_size > 0`` and snap-repair cannot converge on a valid
///     grid-aligned result.
/// CRSMismatchError
///     If the rows' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// coverage_is_valid : Test whether a polygonal coverage is already valid.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> rows = [gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)]
/// >>> gm.coverage_is_valid(rows)
/// False
/// >>> cleaned = gm.coverage_clean(rows)
/// >>> cleaned.coverage_is_valid()
/// True
#[pyfunction]
#[pyo3(
    signature = (values, *, grid_size = 0.0, gap_width = 0.0, overlap_rule = geometry::CoverageOverlapRule::LongestBorder),
    text_signature = "(values, *, grid_size=0.0, gap_width=0.0, overlap_rule='longest_border')"
)]
pub(crate) fn coverage_clean(
    values: &Bound<'_, PyAny>,
    grid_size: f64,
    gap_width: f64,
    overlap_rule: geometry::CoverageOverlapRule,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let rows = geometry::coverage_clean(&shapes, grid_size, gap_width, overlap_rule)?;
    let result = PyGeometryArray::from_shapes(rows, metadata);
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result),
    })
}

/// Resolve a coverage `values` argument: the rows' shapes plus their strict
/// shared frame metadata.
fn coverage_values(
    values: &Bound<'_, PyAny>,
) -> PyResult<(Vec<Shape>, Frame, Option<PyGeometryArray>)> {
    if let Some(array) = exact_geometry_array(values) {
        return Ok((
            array.present_shapes(),
            array.frame.clone(),
            Some(array.clone()),
        ));
    }
    if let Some(geometry) = exact_geometry(values) {
        return Ok((
            vec![geometry.shape.shape().clone()],
            geometry.frame.clone(),
            None,
        ));
    }
    let array = PyGeometryArray::new(values, None, None)?;
    Ok((array.present_shapes(), array.frame.clone(), Some(array)))
}
