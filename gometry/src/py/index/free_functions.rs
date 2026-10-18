use pyo3::IntoPyObjectExt as _;

use crate::py::index::{
    Bound, DistanceUnit, DwithinDistance, IndexPredicate, PreparedIndexQuery, Py, PyAny, PyResult,
    PySpatialIndex, Python, build_spatial_index, exact_geometry, exact_geometry_array, pyfunction,
    query_distance,
};
use crate::py::numpy::int64_array;
#[pyfunction]
#[pyo3(
    signature = (values, geom, *, k = 1, max_distance = None, return_distance = false, unit = None, exclusive = false, ties = false),
    text_signature = "(values, geom, *, k=1, max_distance=None, return_distance=False, unit=None, exclusive=False, ties=False)"
)]
/// Find the nearest of `values` to each query geometry (builds an index).
///
/// Parameters
/// ----------
/// values : Geometry, GeometryArray, or iterable of geometry-like values or None
///     Candidate geometries to index and search.
/// geom : Geometry, GeometryArray, or iterable of Geometry or None
///     Query geometry or one query per sequence row. Missing rows produce
///     empty result groups and retain their logical positions.
/// k : int, default 1
///     Number of nearest neighbors to return per query; ``0`` returns no
///     neighbors.
/// max_distance : float, optional
///     Ignore candidates farther than this from the query. Must be finite and
///     non-negative.
/// return_distance : bool
///     If ``True``, return distances alongside handles — ``(indices,
///     distances)`` for a scalar query, ``(matches, distances)`` for an
///     array query. The distance-returning form requires
///     ``return_distance=True``.
#[doc = unit_parameter_doc!()]
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
///     for a scalar query, CSR ``Groups`` for an array or iterable query. With
///     ``return_distance=True``, plain tuple field order is ``(indices,
///     distances)`` for a scalar query or ``(matches, distances)`` for an
///     array or iterable query (distances parallel to ``matches.values``).
///
/// Raises
/// ------
/// GeometryError
///     If ``k`` is negative, ``max_distance`` is negative or non-finite, or
///     ``unit='meters'`` is requested for a CRS-free geometry.
/// CRSMismatchError
///     If query and indexed geometries have different CRS or coordinate-epoch
///     metadata.
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
    #[pyo3(from_py_with = crate::strict_i64)] k: i64,
    max_distance: Option<f64>,
    return_distance: bool,
    unit: Option<DistanceUnit>,
    exclusive: bool,
    ties: bool,
) -> PyResult<Py<PyAny>> {
    build_spatial_index(py, values)?.nearest(
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
/// left, right : Geometry, GeometryArray, or iterable of geometry-like values or None
///     The two geometry collections to join. Missing rows produce no pairs;
///     their original row positions are preserved in every returned id.
/// predicate : str
///     Spatial predicate each returned pair must satisfy — one of
///     ``'intersects'``, ``'contains'``, ``'contains_properly'``,
///     ``'covers'``, ``'within'``, ``'covered_by'``, ``'equals'``,
///     ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
///     Topological forms omit ``predicate`` to use ``'intersects'``;
///     ``'dwithin'`` requires it explicitly.
/// distance : float, optional
///     Required when ``predicate='dwithin'``: the maximum separation, in
///     CRS-natural units — geodesic meters on a geographic CRS, native
///     linear units on a projected CRS, raw coordinate units when CRS-free. Finite
///     non-negative values and positive infinity are accepted; missing or
///     empty rows produce no pairs.
#[doc = unit_parameter_doc!()]
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
/// CRSError
///     If geographic ``dwithin`` refinement cannot prove a result within the
///     point-edge evaluation budget.
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
    let index = build_spatial_index(py, right)?;
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
    distance: Option<DwithinDistance>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let (left, right) = join_pair_columns(py, queries, index, predicate_op, distance, unit)?;
    (int64_array(py, left)?, int64_array(py, right)?).into_py_any(py)
}

/// Parallel `(left, right)` int64 join columns plus the left collection length.
///
/// Writes the final pair columns during the scan — no intermediate
/// `Vec<(usize, usize)>` that would double peak memory on dense joins.
/// Packed point arrays drive the join column-direct: one frame check, then
/// per-point envelope lookups refined with the shared relation matrix — no
/// per-row `PyGeometry` materialization.
pub(crate) fn join_pair_columns(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    index: &PySpatialIndex,
    predicate: IndexPredicate,
    distance: Option<DwithinDistance>,
    unit: Option<DistanceUnit>,
) -> PyResult<(Vec<i64>, Vec<i64>)> {
    // Matching is the native work `SpatialIndex.query` already detaches; only
    // pulling the operands out of Python needs the interpreter.
    if let Some(geometry) = exact_geometry(left) {
        return Ok(py.detach(|| {
            join_item_pairs(
                std::slice::from_ref(geometry),
                index,
                predicate,
                distance,
                unit,
            )
        })?);
    }
    if let Some(array) = exact_geometry_array(left) {
        return Ok(py.detach(|| join_array_pairs(array, index, predicate, distance, unit))?);
    }
    let array = crate::PyGeometryArray::new(left, None, None)?;
    Ok(py.detach(|| join_array_pairs(&array, index, predicate, distance, unit))?)
}

pub(super) fn append_join_matches(
    left_ids: &mut Vec<i64>,
    right_ids: &mut Vec<i64>,
    left: usize,
    matches: &[usize],
) {
    left_ids.reserve(matches.len());
    right_ids.reserve(matches.len());
    left_ids.resize(left_ids.len().wrapping_add(matches.len()), left as i64);
    right_ids.extend(matches.iter().map(|&right| right as i64));
}

/// Join an extracted geometry array against the index.
fn join_array_pairs(
    array: &crate::PyGeometryArray,
    index: &PySpatialIndex,
    predicate: IndexPredicate,
    distance: Option<DwithinDistance>,
    unit: Option<DistanceUnit>,
) -> crate::error::BatchResult<(Vec<i64>, Vec<i64>)> {
    if !array.is_compact_nullable()
        && let Some(points) = array.storage().point_rows()
    {
        let matches = index.point_rows_matches(
            array,
            &points,
            array.missing(),
            Some(predicate),
            distance,
            unit,
        )?;
        let mut left_ids = Vec::with_capacity(matches.ids.len());
        let mut right_ids = Vec::with_capacity(matches.ids.len());
        for (left_idx, row) in matches.rows().enumerate() {
            append_join_matches(&mut left_ids, &mut right_ids, left_idx, row);
        }
        return Ok((left_ids, right_ids));
    }
    {
        // Row lane for line/mixed left arrays: one frame check (and one
        // metric resolution for dwithin); rows feed the shared cores with
        // no Vec<PyGeometry> staging.
        let plan = PreparedIndexQuery::for_array(index, array, Some(predicate), distance, unit)?;
        let mut left_ids = Vec::new();
        let mut right_ids = Vec::new();
        if let IndexPredicate::Topological(topological) = predicate
            && distance.is_none()
        {
            let mut rows = array.row_operation();
            for logical_row in rows.rows_with_missing(|| ()) {
                let crate::array::ArrayRow::Present(row) = logical_row else {
                    continue;
                };
                let logical = row.logical();
                let shape = rows.prepared(row)?;
                let matches = index.topological_matches(
                    &shape,
                    &rows.frame_cache(logical),
                    topological,
                    plan.query_domain(),
                    Some(logical),
                )?;
                append_join_matches(&mut left_ids, &mut right_ids, logical, &matches);
            }
            return Ok((left_ids, right_ids));
        }
        let mut scratch = Vec::new();
        let PreparedIndexQuery::Dwithin { resolved, .. } = &plan else {
            unreachable!("dwithin join constructs a dwithin plan")
        };
        match resolved {
            crate::crs::ResolvedMetric::Planar { .. } => {
                let rows = array.row_operation();
                for logical_row in rows.rows_with_missing(|| ()) {
                    let crate::array::ArrayRow::Present(row) = logical_row else {
                        continue;
                    };
                    let logical = row.logical();
                    let row = row.row();
                    index.planar_dwithin_query_row_matches(
                        row,
                        &plan,
                        &mut scratch,
                        array.row_bounds_seed(logical),
                        Some(logical),
                    )?;
                    append_join_matches(&mut left_ids, &mut right_ids, logical, &scratch);
                }
            },
            crate::crs::ResolvedMetric::Geodesic(metric) => {
                let mut rows = array.row_operation();
                for logical_row in rows.rows_with_missing(|| ()) {
                    let crate::array::ArrayRow::Present(row) = logical_row else {
                        continue;
                    };
                    let logical = row.logical();
                    let row = row.row();
                    scratch.clear();
                    let mut query_cache = || rows.frame_cache(logical);
                    index.geodesic_dwithin_query_row_matches_append(
                        row,
                        metric,
                        &mut query_cache,
                        &plan,
                        &mut scratch,
                        array.row_bounds_seed(logical),
                        Some(logical),
                    )?;
                    append_join_matches(&mut left_ids, &mut right_ids, logical, &scratch);
                }
            },
        }
        Ok((left_ids, right_ids))
    }
}

/// Join a sequence of extracted geometries against the index.
fn join_item_pairs(
    left_items: &[crate::PyGeometry],
    index: &PySpatialIndex,
    predicate: IndexPredicate,
    distance: Option<DwithinDistance>,
    unit: Option<DistanceUnit>,
) -> crate::error::BatchResult<(Vec<i64>, Vec<i64>)> {
    let mut left_ids = Vec::new();
    let mut right_ids = Vec::new();
    for (left_idx, geometry) in left_items.iter().enumerate() {
        let matches = index.query_exact(geometry, predicate, distance, unit)?;
        append_join_matches(&mut left_ids, &mut right_ids, left_idx, &matches);
    }
    Ok((left_ids, right_ids))
}
