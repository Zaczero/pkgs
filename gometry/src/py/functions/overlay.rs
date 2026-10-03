//! Planar set-overlay free functions and the `OverlayOp` dispatch enum.
//!
//! One `OverlayOp` (Intersection/Union/Difference/SymmetricDifference) carries
//! `#[pymethods]` (in the crate root), the array methods, and these free
//! functions all share a single overlay contract. Reached from the crate root
//! via `pub use overlay::*`.

use pyo3::prelude::*;

pub(crate) use crate::geometry::OverlayOp;
use crate::{
    CoordinateAxes, Frame, GeometryErrorKind, GeometryKind, PyGeometry, PyGeometryArray, Shape,
    SimplifyMethod, Typed, exact_geometry, exact_geometry_array, geometry,
};

fn pooled_shapes(values: &Bound<'_, PyAny>) -> PyResult<(Frame, Vec<Shape>)> {
    if let Some(array) = exact_geometry_array(values) {
        return Ok((array.frame.clone(), array.present_shapes()?));
    }
    if let Some(geometry) = exact_geometry(values) {
        return Ok((geometry.frame.clone(), vec![geometry.shape.shape().clone()]));
    }
    let array = PyGeometryArray::new(values, None, None)?;
    Ok((array.frame.clone(), array.present_shapes()?))
}

fn pooled_shapes_with_rows(values: &Bound<'_, PyAny>) -> PyResult<(Frame, Vec<Shape>)> {
    let array = if let Some(array) = exact_geometry_array(values) {
        array.clone()
    } else if exact_geometry(values).is_some() {
        let geometry = exact_geometry(values).expect("checked scalar geometry");
        return Ok((geometry.frame.clone(), vec![geometry.shape.shape().clone()]));
    } else {
        PyGeometryArray::new(values, None, None)?
    };
    let frame = array.frame.clone();
    let shapes = array
        .row_operation()
        .rows_with_missing(|| {
            Shape::typed_empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
        })
        .map(|row| match row {
            crate::array::ArrayRow::Missing { value, .. } => Ok(value),
            crate::array::ArrayRow::Present(row) => row.row().with_shape(Clone::clone),
        })
        .collect::<crate::error::Result<Vec<_>>>()?;
    Ok((frame, shapes))
}

/// The shared tail of the all-input overlays (`union_all`, `intersection_all`,
/// `symmetric_difference_all`): collect the input shapes under one shared frame
/// and assemble them with `kernel` (GIL released). Aggregates are always lenient
/// (missing rows skipped; SQL/pandas semantics). Geographic frames pass their
/// native topology frame so seam crossing and pole equivalence match binary
/// overlay.
fn apply_overlay_all(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    kernel: impl FnOnce(
        &[Shape],
        crate::geometry::TopologyDomain,
        crate::geometry::Strictness,
    ) -> crate::error::Result<Shape>
    + Send,
) -> PyResult<Typed> {
    let (metadata, shapes) = pooled_shapes(values)?;
    let domain = crate::geometry::topology_domain(&metadata)?;
    let shape = py.detach(move || kernel(&shapes, domain, crate::geometry::Strictness::Lenient))?;
    Ok(Typed(PyGeometry::with_frame(shape, metadata)))
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
/// Evaluated in the coordinate plane. In a geographic CRS, topology resolves
/// longitude aliases, seam spellings, and pole longitudes in the CRS-native
/// frame; seam-crossing inputs are split into chart pieces before construction.
/// A closed axis-aligned rectangle containing the other operand's raw bounds
/// retains that operand verbatim when its bounds are sound in the operation's frame.
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
///     If either operand has coordinates outside its geographic CRS domain.
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
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
/// Evaluated in the coordinate plane. In a geographic CRS, topology resolves
/// longitude aliases, seam spellings, and pole longitudes in the CRS-native
/// frame; seam-crossing inputs are split into chart pieces before construction.
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
///     If either operand has coordinates outside its geographic CRS domain.
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
///
/// See Also
/// --------
/// union_all : Union of many geometries at once.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.union(gm.box(0, 0, 1, 2), gm.box(1, 0, 2, 2)).to_wkt()
/// 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
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
/// Evaluated in the coordinate plane. In a geographic CRS, topology resolves
/// longitude aliases, seam spellings, and pole longitudes in the CRS-native
/// frame; seam-crossing inputs are split into chart pieces before construction.
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
///     If either operand has coordinates outside its geographic CRS domain.
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
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
/// Evaluated in the coordinate plane. In a geographic CRS, topology resolves
/// longitude aliases, seam spellings, and pole longitudes in the CRS-native
/// frame; seam-crossing inputs are split into chart pieces before construction.
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
///     If either operand has coordinates outside its geographic CRS domain.
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
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

/// Union of many geometries into one.
///
/// Strictly disjoint inputs are assembled independently; inputs that can meet
/// use one joint arrangement. In a geographic CRS, topology resolves longitude
/// aliases and seam and pole equivalence in the native frame.
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
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
/// InvalidGeometryError
///     If ``values`` is empty or contains coordinates outside its geographic CRS
///     domain.
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
    // Packed points union to their multipoint without assembling a graph.
    if let Some(array) = exact_geometry_array(values)
        && let Some(multipoint) = array.packed_points_as_multipoint()?
    {
        let domain = crate::geometry::topology_domain(&array.frame)?;
        let shape = py.detach(move || {
            Shape::union_all_topo(&[multipoint], domain, crate::geometry::Strictness::Lenient)
        })?;
        return Ok(Typed(PyGeometry::with_frame(shape, array.frame.clone())));
    }
    apply_overlay_all(py, values, Shape::union_all_topo)
}

/// Union a polygonal *coverage* into one geometry by dissolving shared edges.
///
/// A fast specialization of ``union_all`` for a **valid
/// coverage** — polygons that tile a region edge-to-edge with no overlaps and
/// no T-junctions (see ``coverage_is_valid``). It
/// cancels the interior edges shared between adjacent polygons rather than
/// general noding and classification. On overlapping or badly-noded input it
/// raises before the specialized dissolve runs; use
/// `union_all` there.
///
/// In a geographic CRS, rows whose raw bounds are unsound at the periodic seam,
/// a pole, or a non-canonical longitude are split into chart-safe pieces before
/// dissolve. Canonical seam-free and pole-free rows retain the direct path. The
/// result preserves the shared CRS and coordinate epoch.
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
///     If ``values`` is empty, the rows are not a valid coverage, or coordinates
///     are outside their geographic CRS domain.
///
/// See Also
/// --------
/// union_all : General multi-geometry union (handles overlaps).
/// coverage_is_valid : Whether the rows form a valid polygonal coverage.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> tiles = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
/// >>> merged = gm.coverage_union(tiles)
/// >>> (merged.geometry_type, round(merged.area, 6))
/// ('Polygon', 2.0)
#[pyfunction]
pub(crate) fn coverage_union(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Typed> {
    let (metadata, shapes) = pooled_shapes(values)?;
    if shapes.is_empty() {
        // A coverage union requires an operand, consistent with `union_all`.
        return Err(
            crate::error::Error::from(GeometryErrorKind::EmptyGeometrySequence {
                operation: "coverage_union",
            })
            .into(),
        );
    }
    let domain = crate::geometry::topology_domain(&metadata)?;
    let shape = py.detach(move || crate::geometry::coverage_union_topo(&shapes, domain))?;
    Ok(Typed(PyGeometry::with_frame(shape, metadata)))
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
/// values : Geometry, GeometryArray, or iterable of Geometry-like values
///     One geometry or linework values to node and polygonize together. Array
///     and iterable inputs may contain ``None`` rows; they are omitted from the
///     pooled linework.
///
/// Returns
/// -------
/// GeometryArray
///     The polygons enclosed by the pooled linework.
///
/// Raises
/// ------
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact polygonized
///     output, or the bounded embedding search is exhausted.
///
/// See Also
/// --------
/// polygonize_full : The same pooled aggregate plus dangles, cut edges, and invalid-ring diagnostics.
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
#[pyfunction]
pub(crate) fn polygonize(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<PyGeometryArray> {
    let (metadata, shapes) = pooled_shapes(values)?;
    let polygons = py.detach(move || Shape::polygonize_all(&shapes, false))?;
    Ok(PyGeometryArray::from_shapes(polygons, metadata)?)
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
/// values : Geometry, GeometryArray, or iterable of Geometry-like values
///     One geometry or linework values to node and polygonize together. Array
///     and iterable inputs may contain ``None`` rows; they are omitted from the
///     pooled linework, while diagnostic groups retain their source-row
///     positions.
///
/// Returns
/// -------
/// PolygonizeResult
///     Pooled polygons plus source-row ``Groups`` for cut edges, dangles, and
///     invalid rings.
///
/// Raises
/// ------
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact polygonized
///     output, or the bounded embedding search is exhausted.
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
#[pyfunction]
pub(crate) fn polygonize_full(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let (metadata, shapes) = pooled_shapes_with_rows(values)?;
    let full = py.detach(move || Shape::polygonize_full_all(&shapes, false))?;
    crate::py_polygonize_full(py, full, metadata.crs_ref(), metadata.epoch())
}

/// Common intersection of many geometries.
///
/// The region inside every input. When sound coordinate bounds prove operands
/// disjoint, returns the typed empty directly; in a geographic CRS this requires
/// every operand to be seam-free, pole-free, and canonically spelled. Otherwise,
/// the result is assembled from one joint arrangement. Mixed dimensions narrow
/// to the lowest shared dimension and an empty result keeps that dimension's
/// typed empty.
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
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
/// InvalidGeometryError
///     If ``values`` is empty or contains coordinates outside its geographic CRS
///     domain.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> panes = [gm.box(0, 0, 3, 3), gm.box(1, 1, 4, 4), gm.box(2, 2, 5, 5)]
/// >>> gm.intersection_all(panes).to_wkt()
/// 'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
#[pyfunction]
pub(crate) fn intersection_all(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Typed> {
    apply_overlay_all(py, values, Shape::intersection_all_topo)
}

/// Symmetric difference of many geometries.
///
/// The region covered by an odd number of inputs. Strictly disjoint inputs are
/// assembled independently; inputs that can meet use one joint arrangement. In
/// a geographic CRS, topology resolves longitude aliases and seam and pole
/// equivalence in the native frame. The result is order-independent (`▵` is
/// associative and commutative).
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
/// GeometryError
///     If no adjacent binary64 assignment preserves the exact result topology,
///     or the bounded embedding search is exhausted.
/// InvalidGeometryError
///     If ``values`` is empty or contains coordinates outside its geographic CRS
///     domain.
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
    apply_overlay_all(py, values, Shape::symmetric_difference_all_topo)
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
///     Finite non-negative coordinate-space gap width; 0 disables gap
///     detection. Geographic CRSs use native angular units, projected CRSs
///     use native linear units, and CRS-free inputs use raw coordinate units.
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
/// >>> grid = gm.GeometryArray([
/// ...     gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
/// ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
/// ... ])
/// >>> gm.coverage_is_valid(grid)
/// True
/// >>> gm.coverage_is_valid(gm.GeometryArray([
/// ...     gm.Polygon([(0, 0), (1.1, 0), (1.1, 1), (0, 1)]),
/// ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
/// ... ]))
/// False
#[pyfunction]
#[pyo3(signature = (values, *, gap_width = 0.0))]
pub(crate) fn coverage_is_valid(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    gap_width: f64,
) -> PyResult<bool> {
    let (_, shapes) = pooled_shapes(values)?;
    Ok(py.detach(move || geometry::coverage_is_valid(&shapes, gap_width))?)
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
///     Finite non-negative coordinate-space gap width; 0 disables gap
///     detection. Geographic CRSs use native angular units, projected CRSs
///     use native linear units, and CRS-free inputs use raw coordinate units.
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.coverage_invalid_edges(
/// ...     [gm.box(0, 0, 1, 1), gm.box(0.5, 0, 1.5, 1)]))
/// 2
#[pyfunction]
#[pyo3(signature = (values, *, gap_width = 0.0))]
pub(crate) fn coverage_invalid_edges(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    gap_width: f64,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let edges = py.detach(move || geometry::coverage_invalid_edges(&shapes, gap_width))?;
    let result = PyGeometryArray::from_shapes(edges, metadata)?;
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result)?,
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
///     Finite non-negative coordinate-space distance-scale tolerance:
///     geographic CRSs use native angular units, projected CRSs use native
///     linear units, and CRS-free inputs use raw coordinate units. Read on the
///     same scale by both methods (the
///     Visvalingam-Whyatt effective area is ``tolerance**2 / 2``).
/// method : {'vw', 'dp'}, default 'vw'
///     Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
///     ``'dp'`` is distance-based (Douglas-Peucker). Both run under the same
///     topology guard. For coverage simplification, ``'dp'`` is a local
///     importance criterion; it does not provide scalar Douglas-Peucker's
///     original-span error contract.
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
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let rows = py.detach(move || {
        geometry::coverage_simplify(&shapes, tolerance, method, simplify_boundary)
    })?;
    let result = PyGeometryArray::from_shapes(rows, metadata)?;
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result)?,
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
///     Finite non-negative vertex snapping grid in coordinate space:
///     geographic CRSs use native angular units, projected CRSs use native
///     linear units, and CRS-free inputs use raw coordinate units. ``0``
///     preserves input coordinates and disables snapping. Unlike
///     ``snap_to_grid(grid_size=...)``, this operation accepts zero.
///
/// gap_width : float, default 0.0
///     Finite non-negative coordinate-space width of gaps to merge (0 keeps
///     gaps).
/// overlap_rule : str, default 'longest_border'
///     Which row keeps a region covered more than once:
///     ``'longest_border'``, ``'max_area'``, ``'min_area'``, or
///     ``'min_index'``.
///     Cleaning rebuilds faces from one exact partition. Source Z/M ordinates
///     are retained for rows whose emitted nodes have one consistent source
///     value; an unavailable or conflicting axis is dropped for that row.
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
///     If ``grid_size`` or ``gap_width`` is negative or non-finite, or no
///     adjacent binary64 assignment preserves the exact cleaned coverage or
///     the bounded embedding search is exhausted.
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
/// >>> gm.coverage_is_valid(cleaned)
/// True
#[pyfunction]
#[pyo3(
    signature = (values, *, grid_size = 0.0, gap_width = 0.0, overlap_rule = geometry::CoverageOverlapRule::LongestBorder),
    text_signature = "(values, *, grid_size=0.0, gap_width=0.0, overlap_rule='longest_border')"
)]
pub(crate) fn coverage_clean(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    grid_size: f64,
    gap_width: f64,
    overlap_rule: geometry::CoverageOverlapRule,
) -> PyResult<PyGeometryArray> {
    let (shapes, metadata, source) = coverage_values(values)?;
    let rows =
        py.detach(move || geometry::coverage_clean(&shapes, grid_size, gap_width, overlap_rule))?;
    let result = PyGeometryArray::from_shapes(rows, metadata)?;
    Ok(match source {
        None => result,
        Some(array) => array.scatter_present_result(result)?,
    })
}

/// Resolve a coverage `values` argument for scatter-producing operations: the
/// rows' shapes, strict shared frame metadata, and the admitted source array
/// needed to restore missing logical rows after native work.
fn coverage_values(
    values: &Bound<'_, PyAny>,
) -> PyResult<(Vec<Shape>, Frame, Option<PyGeometryArray>)> {
    if let Some(array) = exact_geometry_array(values) {
        return Ok((
            array.present_shapes()?,
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
    Ok((array.present_shapes()?, array.frame.clone(), Some(array)))
}
