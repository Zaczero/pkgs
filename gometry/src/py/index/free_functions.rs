#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;

use crate::py::index::{
    Bound, DistanceUnit, IndexPredicate, NonNegative, PreparedIndexQuery, Py, PyAny, PyResult,
    PySpatialIndex, Python, build_spatial_index, exact_geometry_array, geometry_items, pyfunction,
    query_distance,
};
#[pyfunction]
#[pyo3(
    signature = (values, geom, *, k = 1, max_distance = None, return_distance = false, unit = None, exclusive = false, ties = false),
    text_signature = "(values, geom, *, k=1, max_distance=None, return_distance=False, unit=None, exclusive=False, ties=False)"
)]
/// Find the nearest of `values` to each query geometry (builds an index).
///
/// Parameters
/// ----------
/// values : sequence of Geometry or GeometryArray
///     Candidate geometries to index and search.
/// geom : Geometry or GeometryArray
///     Query geometry (or array of queries).
/// k : int, default 1
///     Number of nearest neighbors to return per query.
/// max_distance : float, optional
///     Ignore candidates farther than this from the query.
/// return_distance : bool, default False
///     If ``True``, return distances alongside handles — ``(indices,
///     distances)`` for a scalar query, ``(matches, distances)`` for an
///     array query.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
/// exclusive : bool, default False
///     If ``True``, skip candidates structurally equal to the query geometry
///     (same exact coordinates) — "the nearest *other* feature".
/// ties : bool, default False
///     Also return every candidate TYING the k-th nearest distance (exact
///     comparison) — results can then exceed ``k``.
///
/// Returns
/// -------
/// int64 numpy.ndarray, Groups, or tuple
///     Indices into `values` of the nearest geometries — an ``int64`` ndarray
///     for a scalar query, CSR ``Groups`` for an array query. With
///     ``return_distance=True``, plain tuple field order is ``(indices,
///     distances)`` for a scalar query or ``(matches, distances)`` for an
///     array query (distances parallel to ``matches.values``).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> sites = [gm.Point(0, 0), gm.Point(5, 5)]
/// >>> gm.nearest(sites, gm.Point(4, 4))
/// array([1])
pub(crate) fn nearest(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    geom: &Bound<'_, PyAny>,
    k: i64,
    max_distance: Option<f64>,
    return_distance: bool,
    unit: Option<DistanceUnit>,
    exclusive: bool,
    ties: bool,
) -> PyResult<Py<PyAny>> {
    build_spatial_index(values)?.nearest(
        py,
        geom,
        k,
        max_distance,
        return_distance,
        unit,
        exclusive,
        ties,
    )
}

/// Perform a spatial join between two geometry collections via an internal index.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or sequence of Geometry
///     The two geometry collections to join. Missing rows produce no pairs;
///     their original row positions are preserved in every returned id.
/// predicate : str, default 'intersects'
///     Spatial predicate each returned pair must satisfy — one of
///     ``'intersects'``, ``'contains'``, ``'contains_properly'``,
///     ``'covers'``, ``'within'``, ``'covered_by'``, ``'equals'``,
///     ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
/// distance : float, optional
///     Required when ``predicate='dwithin'``: the maximum separation, in
///     CRS-natural units — geodesic meters on a geographic CRS, native
///     units on a projected CRS, coordinate units when CRS-free.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// tuple of numpy.ndarray
///     ``(left, right)`` parallel int64 row-id columns satisfying the
///     predicate.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``predicate`` is unknown, ``distance`` is missing or invalid for
///     ``predicate='dwithin'``, or ``unit='meters'`` is requested for a
///     CRS-free geometry.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> stops = gm.GeometryArray([gm.Point(0.5, 0.5), gm.Point(9, 9)])
/// >>> zones = gm.GeometryArray([gm.box(0, 0, 1, 1)])
/// >>> left, right = gm.join(stops, zones, predicate='within')
/// >>> (left, right)
/// (array([0]), array([0]))
#[pyfunction]
#[pyo3(signature = (left, right, *, predicate = "intersects", distance = None, unit = None))]
pub(crate) fn join(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    predicate: &str,
    distance: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let predicate_op = IndexPredicate::parse(predicate)?;
    let distance = query_distance(Some(predicate_op), distance)?;
    let index = build_spatial_index(right)?;
    join_indexed(py, left, &index, predicate_op, distance, unit)
}

/// Format a directed join against an already-built index. Shared by the free-
/// function ``join`` workflow and ``SpatialIndex.join`` so pair ordering and
/// exact refinement cannot drift between the two surfaces.
pub(crate) fn join_indexed(
    py: Python<'_>,
    queries: &Bound<'_, PyAny>,
    index: &PySpatialIndex,
    predicate_op: IndexPredicate,
    distance: Option<NonNegative>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let (left, right) = join_pair_columns(queries, index, predicate_op, distance, unit)?;
    (
        crate::py::numpy::int64_array(py, left)?,
        crate::py::numpy::int64_array(py, right)?,
    )
        .into_py_any(py)
}

/// Parallel `(left, right)` int64 join columns plus the left collection length.
///
/// Writes the final pair columns during the scan — no intermediate
/// `Vec<(usize, usize)>` that would double peak memory on dense joins.
/// Packed point arrays drive the join column-direct: one frame check, then
/// per-point envelope lookups refined with the predicate's point kernel — no
/// per-row `PyGeometry` materialization.
pub(crate) fn join_pair_columns(
    left: &Bound<'_, PyAny>,
    index: &PySpatialIndex,
    predicate: IndexPredicate,
    distance: Option<NonNegative>,
    unit: Option<DistanceUnit>,
) -> PyResult<(Vec<i64>, Vec<i64>)> {
    if let Some(array) = exact_geometry_array(left)
        && let Some(points) = array.storage().point_rows()
    {
        let matches = index.point_rows_matches(
            array,
            &points,
            array.missing().map(crate::array::MissingMask::as_slice),
            Some(predicate),
            distance,
            unit,
        )?;
        let capacity = matches.ids.len();
        let mut left_ids = Vec::with_capacity(capacity);
        let mut right_ids = Vec::with_capacity(capacity);
        for (left_idx, row) in matches.rows().enumerate() {
            let left = left_idx as i64;
            for &idx in row {
                left_ids.push(left);
                right_ids.push(idx as i64);
            }
        }
        return Ok((left_ids, right_ids));
    }
    if let Some(array) = exact_geometry_array(left) {
        // Row lane for line/mixed left arrays: one frame check (and one
        // metric resolution for dwithin); rows feed the shared cores with
        // no Vec<PyGeometry> staging.
        let plan = PreparedIndexQuery::for_array(index, array, Some(predicate), distance, unit)?;
        let mut left_ids = Vec::new();
        let mut right_ids = Vec::new();
        if let IndexPredicate::Topological(topological) = predicate
            && distance.is_none()
        {
            for (left_idx, (missing, row)) in array.masked_storage_rows().enumerate() {
                if missing {
                    continue;
                }
                let left = left_idx as i64;
                let shape = array.prepared_row(left_idx, row);
                for right_idx in index.topological_matches(&shape, topological) {
                    left_ids.push(left);
                    right_ids.push(right_idx as i64);
                }
            }
            return Ok((left_ids, right_ids));
        }
        let mut scratch = Vec::new();
        for (left_idx, (missing, row)) in array.masked_storage_rows().enumerate() {
            if missing {
                continue;
            }
            let seeded = array.row_bounds_seed(left_idx);
            index.dwithin_query_row_matches(
                row,
                &array.row_frame_cache(left_idx),
                &plan,
                &mut scratch,
                seeded,
            )?;
            let left = left_idx as i64;
            for &right_idx in &scratch {
                left_ids.push(left);
                right_ids.push(right_idx as i64);
            }
        }
        return Ok((left_ids, right_ids));
    }
    let left_items = geometry_items(left)?;
    let mut left_ids = Vec::new();
    let mut right_ids = Vec::new();
    for (left_idx, geometry) in left_items.iter().enumerate() {
        let left = left_idx as i64;
        for right_idx in index.query_exact(geometry, predicate, distance, unit)? {
            left_ids.push(left);
            right_ids.push(right_idx as i64);
        }
    }
    Ok((left_ids, right_ids))
}
