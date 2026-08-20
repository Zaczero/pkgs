#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::types::PyBool;

use crate::collections::sort_row_ids;
use crate::parse_spatial_index_handle;
use crate::py::index::{
    BinaryHeap, Bound, DistanceUnit, GeometryError, Groups, HeapSize as _, IndexEntry,
    IndexEnvelope, IndexPredicate, IntoPyObject as _, NearestOptions, PreparedIndexQuery, Py,
    PyAny, PyAnyMethods as _, PyGeometry, PyResult, PySpatialIndex, Python, Shape,
    build_spatial_index, exact_geometry, exact_geometry_array, format_nearest, format_nearest_rows,
    geodesic_prunable_point, geometry_items, index_envelope, join_indexed, parse_max_distance,
    pyfunction, pymethods, query_distance, resolve_metric, restore_spatial_index, spatial_index,
    usize_array, validate_nearest_k,
};

type SpatialIndexReduce = (Py<PyAny>, (crate::PyGeometryArray, Vec<usize>));

#[path = "iter.rs"]
mod iter;
pub(crate) use iter::PySpatialIndexIter;

fn mapping_view(slf: Bound<'_, PySpatialIndex>, class_name: &str) -> PyResult<Py<PyAny>> {
    let py = slf.py();
    Ok(py
        .import("collections.abc")?
        .getattr(class_name)?
        .call1((slf,))?
        .unbind())
}

#[pyfunction]
pub(crate) fn _unpickle_spatial_index(
    values: &crate::PyGeometryArray,
    live_handles: &Bound<'_, PyAny>,
) -> PyResult<PySpatialIndex> {
    // Never trust a lying `__len__` for live-handle capacity.
    let live_handles = crate::collect_usize_sequence(live_handles, "spatial index live handles")?;
    // Metadata presence is re-derived from the row storage — a forged
    // has_metadata=false flag must not disable the CRS gate.
    restore_spatial_index(values, &live_handles)
}

fn parse_handle_like(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.cast_exact::<PyBool>().is_ok() {
        return Err(PyTypeError::new_err(
            "spatial index handle must be an integer",
        ));
    }
    if let Ok(handle) = parse_spatial_index_handle(value) {
        return Ok(handle);
    }
    let indexed = value
        .call_method0(pyo3::intern!(value.py(), "__index__"))
        .map_err(|_| PyTypeError::new_err("spatial index handle must be an integer"))?;
    let handle = indexed
        .extract::<i64>()
        .map_err(|_| GeometryError::new_err("spatial index handle is too large"))?;
    if handle < 0 {
        return Err(GeometryError::new_err(
            "spatial index handle must be non-negative",
        ));
    }
    usize::try_from(handle).map_err(|_| GeometryError::new_err("spatial index handle is too large"))
}

fn handle_like_or_none(value: &Bound<'_, PyAny>) -> Option<usize> {
    parse_handle_like(value).ok()
}

#[pymethods]
impl PySpatialIndex {
    /// Build a spatial index (STR-tree) over present geometries.
    ///
    /// Parameters
    /// ----------
    /// values : GeometryArray, iterable of Geometry or None, default None
    ///     Geometries to index. ``None`` builds an empty mutable index for
    ///     later ``insert`` calls. Every indexed geometry must share one CRS
    ///     and coordinate epoch. Missing rows are skipped but retain their
    ///     original positions as stable, non-live handles, so query and join
    ///     results always refer to the input row ids.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If items carry conflicting CRS or coordinate-epoch metadata.
    ///
    /// See Also
    /// --------
    /// join : High-level spatial join built on the index.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)])
    /// >>> [int(i) for i in idx.query(gm.Point(0.5, 0.5), predicate='intersects')]
    /// [0]
    #[new]
    #[pyo3(signature = (values = None), text_signature = "(values=None)")]
    fn new(values: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        values.map_or_else(|| spatial_index(Vec::new()), build_spatial_index)
    }

    /// CRS shared by the indexed geometries, or ``None`` for an unframed index.
    ///
    /// Returns
    /// -------
    /// CRS or None
    #[getter]
    fn crs(&self) -> Option<crate::py::crs::PyCrs> {
        self.metadata
            .as_ref()
            .and_then(crate::Frame::crs_ref)
            .cloned()
            .map(crate::py::crs::PyCrs::from_canonical)
    }

    /// Coordinate epoch shared by the indexed geometries, if set.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    fn epoch(&self) -> Option<f64> {
        self.metadata.as_ref().and_then(crate::Frame::epoch)
    }

    /// Number of live (non-removed) geometries in the index.
    ///
    /// Returns
    /// -------
    /// int
    pub(crate) fn __len__(&self) -> usize {
        self.bulk.live() + self.overflow.size()
    }

    /// Pickle as the full sparse row table plus live handles.
    ///
    /// Handles are the public identity returned by ``query``/``nearest`` and
    /// consumed by ``remove``/``__getitem__``; round-tripping must therefore
    /// preserve tombstones instead of compactly renumbering live rows.
    pub(crate) fn __reduce__(&self, py: Python<'_>) -> PyResult<SpatialIndexReduce> {
        let shapes: Vec<crate::geometry::Shape> = (0..self.rows.len())
            .map(|handle| self.rows.row(handle).with_shape(std::clone::Clone::clone))
            .collect();
        let frame = self.metadata.clone().unwrap_or_default();
        let array = crate::PyGeometryArray::from_shapes(shapes, frame);
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_spatial_index"))?
            .unbind();
        Ok((callable, (array, self.live_handles_sorted())))
    }

    pub(crate) fn __copy__(&self, py: Python<'_>) -> PyResult<Py<Self>> {
        Py::new(py, self.clone())
    }

    #[pyo3(signature = (memo))]
    pub(crate) fn __deepcopy__(
        &self,
        py: Python<'_>,
        memo: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        let _ = memo;
        Py::new(py, self.clone())
    }

    /// Return the geometry at a live handle.
    ///
    /// Raises ``KeyError`` when the handle is unknown or has been removed.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A typed leaf (`Point`, `Polygon`, …) matching the stored kind —
    ///     never the bare base `Geometry` class.
    pub(crate) fn __getitem__(&self, handle: &Bound<'_, PyAny>) -> PyResult<crate::Typed> {
        let handle = parse_handle_like(handle)?;
        if !self.is_live_handle(handle) {
            return Err(PyKeyError::new_err(handle));
        }
        Ok(self.geometry_at_handle(handle))
    }

    /// Whether ``handle`` is a live geometry handle.
    ///
    /// Non-integer probes return ``False`` instead of raising, matching
    /// Python's container protocol.
    ///
    /// Returns
    /// -------
    /// bool
    pub(crate) fn __contains__(&self, handle: &Bound<'_, PyAny>) -> bool {
        handle_like_or_none(handle).is_some_and(|handle| self.is_live_handle(handle))
    }

    /// Iterate live handles lazily in ascending handle order.
    ///
    /// Returns
    /// -------
    /// iterator of int
    pub(crate) fn __iter__(slf: Bound<'_, Self>) -> PyResult<Py<PySpatialIndexIter>> {
        let py = slf.py();
        let (remaining, generation) = {
            let index = slf.borrow();
            (index.__len__(), index.mutation_gen)
        };
        Py::new(py, PySpatialIndexIter {
            source: slf.unbind(),
            next_handle: 0,
            remaining,
            generation,
        })
    }

    /// Return a dynamic view of the live handles.
    ///
    /// Returns
    /// -------
    /// KeysView
    ///     The live handles, in ascending order.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]).keys())
    /// [0, 1]
    pub(crate) fn keys(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "KeysView")
    }

    /// Return a dynamic view of the geometries at live handles.
    ///
    /// Returns
    /// -------
    /// ValuesView
    ///     One geometry per live handle, in handle order.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1)]).values())[0].to_wkt()
    /// 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    pub(crate) fn values(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "ValuesView")
    }

    /// Return a dynamic view of `(handle, geometry)` pairs.
    ///
    /// Returns
    /// -------
    /// ItemsView
    ///     ``(handle, geometry)`` pairs for the live handles.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> list(gm.SpatialIndex([gm.box(0, 0, 1, 1)]).items())[0][0]
    /// 0
    pub(crate) fn items(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "ItemsView")
    }

    /// Return the geometry at handle, or default when it is not live.
    ///
    /// Parameters
    /// ----------
    /// handle : int
    ///     A row handle (positional-only; handles are integers, so a
    ///     non-integer probe raises ``TypeError`` like ``Mapping.get``).
    ///
    /// default : object, optional
    ///     Value returned when the handle is not live.
    ///
    /// Returns
    /// -------
    /// Geometry or object
    ///     The live geometry, else ``default``.
    #[pyo3(signature = (handle, /, default = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    /// >>> g = idx.get(0)
    /// >>> g is not None and g.to_wkt().startswith('POLYGON')
    /// True
    pub(crate) fn get(
        &self,
        py: Python<'_>,
        handle: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let handle = parse_handle_like(handle)?;
        if self.is_live_handle(handle) {
            return Ok(self.geometry_at_handle(handle).into_pyobject(py)?.unbind());
        }
        Ok(default.unwrap_or_else(|| py.None()))
    }

    pub(crate) fn __repr__(&self) -> String {
        let len = self.bulk.live() + self.overflow.size();
        self.metadata
            .as_ref()
            .and_then(|frame| frame.crs_str())
            .map_or_else(
                || format!("<SpatialIndex len={len}>"),
                |crs| format!("<SpatialIndex len={len} crs={crs}>"),
            )
    }

    /// `sys.getsizeof` support: the wrapper plus the retained Rust-side
    /// index payload — packed or boxed row geometry coordinates, the immutable
    /// STR tree, overflow R-tree entries, frame metadata, and any built
    /// geodesic cap cache. Shared buffers are reported as this index's
    /// logical retained footprint.
    pub(crate) fn __sizeof__(&self) -> usize {
        self.total_size()
    }

    /// Return exact predicate-refined matches for a query geometry or array.
    ///
    /// Candidates are refined with predicate (`'intersects'` by
    /// default); distance is accepted only with `'dwithin'`. Use
    /// candidates for a bounding-box-only prefilter. A single geometry
    /// returns an int64 ndarray; a GeometryArray returns
    /// Groups — one id row per row, CSR-grouped.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     The query geometry, or one query per array element.
    /// predicate : str, default 'intersects'
    ///     Spatial relation each match must satisfy (``'dwithin'`` requires
    ///     ``distance``).
    /// distance : float, optional
    ///     ``'dwithin'`` distance threshold, in ``unit``.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
    ///     geographic CRS — only for deliberate coordinate-space math);
    ///     ``'meters'`` forces the CRS metric and raises without a CRS.
    ///
    /// Returns
    /// -------
    /// int64 numpy.ndarray or Groups
    ///     Matching index handles (row ids). A scalar query returns a read-only
    ///     ``int64`` ndarray of handles; an array query returns ``Groups`` of
    ///     handles, one group per query row.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If a query parameter is invalid, or ``unit='meters'`` is requested
    ///     for a CRS-free geometry.
    #[pyo3(signature = (geom, *, predicate = "intersects", distance = None, unit = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.query(gm.Point(0.5, 0.5)).tolist()
    /// [0]
    pub(crate) fn query(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        predicate: &str,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let predicate = IndexPredicate::parse(predicate)?;
        let distance = query_distance(Some(predicate), distance)?;
        if let Some(geometry) = exact_geometry(geom) {
            let matches = self.query_exact(geometry, predicate, distance, unit)?;
            return usize_array(py, matches);
        }
        if let Some(array) = exact_geometry_array(geom) {
            if let Some(points) = array.storage().point_rows() {
                let results = self.point_rows_matches(
                    array,
                    &points,
                    array.missing().map(crate::array::MissingMask::as_slice),
                    Some(predicate),
                    distance,
                    unit,
                )?;
                return Groups::from_int64_csr(results.ids, results.offsets)?.into_py_any(py);
            }
            if let IndexPredicate::Topological(topological) = predicate
                && distance.is_none()
            {
                // One frame check for the whole array; rows feed the
                // shape-level core with no per-row wrapper. Matches append
                // straight into the CSR values column (one pass).
                self.ensure_frame_compatible(
                    array.crs_ref(),
                    array.epoch(),
                    "spatial index query",
                )?;
                let row_count = array.storage().len();
                let mut ids = Vec::new();
                let mut offsets = Vec::with_capacity(row_count + 1);
                offsets.push(0);
                for (row_index, (missing, row)) in array.masked_storage_rows().enumerate() {
                    if !missing {
                        let shape = array.prepared_row(row_index, row);
                        self.topological_matches_append(&shape, topological, &mut ids);
                    }
                    offsets.push(ids.len());
                }
                return Groups::from_int64_csr(ids, offsets)?.into_py_any(py);
            }
            // dwithin array queries (the only distance-carrying lane left):
            // one frame check + one metric resolution; rows run the shared
            // candidate core and the pair kernel on stack handles — no
            // per-row PyGeometry staging.
            let plan = PreparedIndexQuery::for_array(self, array, Some(predicate), distance, unit)?;
            let row_count = array.storage().len();
            let mut ids = Vec::new();
            let mut offsets = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            for (row_index, (missing, row)) in array.masked_storage_rows().enumerate() {
                if !missing {
                    let seeded = array.row_bounds_seed(row_index);
                    self.dwithin_query_row_matches_append(
                        row,
                        &array.row_frame_cache(row_index),
                        &plan,
                        &mut ids,
                        seeded,
                    )?;
                }
                offsets.push(ids.len());
            }
            return Groups::from_int64_csr(ids, offsets)?.into_py_any(py);
        }
        Err(PyTypeError::new_err("expected Geometry or GeometryArray"))
    }

    /// Join query rows against this prebuilt index.
    ///
    /// Reuses the index instead of rebuilding the right side on every call.
    /// Predicate orientation is ``predicate(query, indexed_geometry)``, exactly
    /// matching free-function ``join(queries, indexed_values, ...)``. Missing query rows
    /// produce no pairs; missing rows skipped while building the index retain
    /// their original handles.
    ///
    /// Parameters
    /// ----------
    /// queries : Geometry, GeometryArray, or iterable of Geometry
    ///     Left-side geometries to join against the indexed rows.
    /// predicate : str, default 'intersects'
    ///     Spatial predicate each returned pair must satisfy.
    /// distance : float, optional
    ///     Required when ``predicate='dwithin'``: the maximum separation in
    ///     CRS-natural units.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units; ``'meters'`` forces the
    ///     CRS metric and raises without a CRS.
    ///
    /// Returns
    /// -------
    /// tuple of numpy.ndarray
    ///     ``(query_ids, handles)`` parallel read-only int64 columns.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query and index CRS/coordinate-epoch frames differ.
    /// GeometryError
    ///     If a predicate or distance option is invalid.
    #[pyo3(signature = (queries, *, predicate = "intersects", distance = None, unit = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> left, right = idx.join(gm.GeometryArray([gm.Point(0.5, 0.5)]))
    /// >>> (left.tolist(), right.tolist())
    /// ([0], [0])
    pub(crate) fn join(
        &self,
        py: Python<'_>,
        queries: &Bound<'_, PyAny>,
        predicate: &str,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let predicate = IndexPredicate::parse(predicate)?;
        let distance = query_distance(Some(predicate), distance)?;
        join_indexed(py, queries, self, predicate, distance, unit)
    }

    /// All index pairs ``(i, j)`` with ``i < j`` whose geometries satisfy a
    /// symmetric ``predicate`` — a self-join over the index.
    ///
    /// Parameters
    /// ----------
    /// predicate : str, default 'intersects'
    ///     A symmetric relation: ``'intersects'``, ``'equals'``,
    ///     ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
    ///     Directional predicates (``'contains'``, ``'within'``, ...) are
    ///     rejected — unordered pairs would drop the reverse direction; use
    ///     ``join(...)`` for directed relations.
    /// distance : float, optional
    ///     ``'dwithin'`` distance threshold, in ``unit``.
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
    ///     ``(left, right)`` parallel int64 row-id columns.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If indexed items carry conflicting CRS/epoch frames.
    /// GeometryError
    ///     If ``predicate`` is unknown or directional, ``distance`` is missing
    ///     or invalid for ``predicate='dwithin'``, or ``unit='meters'`` is
    ///     requested for a CRS-free geometry.
    #[pyo3(signature = (*, predicate = "intersects", distance = None, unit = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> left, right = idx.self_join()
    /// >>> (left.tolist(), right.tolist())
    /// ([], [])
    pub(crate) fn self_join(
        &self,
        py: Python<'_>,
        predicate: &str,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let predicate = IndexPredicate::parse(predicate)?;
        if !predicate.is_symmetric() {
            return Err(GeometryError::new_err(format!(
                "self_join requires a symmetric predicate, but \"{}\" is directional; \
                 use join(...) for directed relations",
                predicate.label(),
            )));
        }
        let distance = query_distance(Some(predicate), distance)?;
        let mut left = Vec::new();
        let mut right = Vec::new();
        self.for_each_symmetric_pair(predicate, distance, unit, |i, j| {
            left.push(i as i64);
            right.push(j as i64);
        })?;
        Ok((
            crate::py::numpy::int64_array(py, left)?,
            crate::py::numpy::int64_array(py, right)?,
        ))
    }

    /// Bounding-box candidate matches for a query geometry or array (not
    /// exact).
    ///
    /// A single geometry returns an `int64` ndarray; a `GeometryArray`
    /// returns `Groups` — one candidate row per row.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     The query geometry, or one query per array element.
    /// distance : float, optional
    ///     Expand the query envelope by this much, in ``unit``.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
    ///     geographic CRS — only for deliberate coordinate-space math);
    ///     ``'meters'`` forces the CRS metric and raises without a CRS.
    ///
    /// Returns
    /// -------
    /// int64 numpy.ndarray or Groups
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If a query parameter is invalid, or ``unit='meters'`` is requested
    ///     for a CRS-free geometry.
    #[pyo3(signature = (geom, *, distance = None, unit = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.candidates(gm.box(0, 0, 1, 1)).tolist()
    /// [0]
    pub(crate) fn candidates(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let distance = query_distance(None, distance)?;
        if let Some(geometry) = exact_geometry(geom) {
            let matches = self.candidate_ids_sorted(geometry, distance, unit)?;
            return usize_array(py, matches);
        }
        if let Some(array) = exact_geometry_array(geom) {
            if let Some(points) = array.storage().point_rows() {
                let results = self.point_rows_matches(
                    array,
                    &points,
                    array.missing().map(crate::array::MissingMask::as_slice),
                    None,
                    distance,
                    unit,
                )?;
                return Groups::from_int64_csr(results.ids, results.offsets)?.into_py_any(py);
            }
            // One frame check + one metric resolution for the whole array;
            // rows answer from their window bounds and append into one CSR
            // values column — no per-row PyGeometry, ShapeData, or second
            // id-buffer copy. Packed element bounds are hoisted once so each
            // row reuses the cache instead of re-scanning shells.
            let plan = PreparedIndexQuery::for_array(self, array, None, distance, unit)?;
            // Pure envelope candidates (no distance, not geographic): never
            // enter row prep / with_shape — bounds cache is the whole answer.
            let envelope_only =
                matches!(plan, PreparedIndexQuery::Candidates) && !self.geographic();
            let row_count = array.storage().len();
            let mut ids = Vec::new();
            let mut offsets = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            for (row_index, (missing, row)) in array.masked_storage_rows().enumerate() {
                if !missing {
                    let row_start = ids.len();
                    let seeded = array.row_bounds_seed(row_index);
                    let (predicate, distance, metric) = plan.candidate_parts();
                    if envelope_only {
                        let bounds = match seeded {
                            crate::array::BoundsSeed::Unset => row.quick_bounds(),
                            crate::array::BoundsSeed::Value(bounds) => bounds,
                        };
                        self.candidate_ids_core_append(
                            bounds, None, None, None, None, None, &mut ids,
                        );
                    } else {
                        let prepared = plan.row(self, row, seeded);
                        self.candidate_ids_core_append(
                            prepared.bounds,
                            prepared.pruner_point,
                            prepared.cap,
                            predicate,
                            distance,
                            metric,
                            &mut ids,
                        );
                    }
                    sort_row_ids(&mut ids[row_start..], self.rows.len());
                }
                offsets.push(ids.len());
            }
            return Groups::from_int64_csr(ids, offsets)?.into_py_any(py);
        }
        Err(PyTypeError::new_err("expected Geometry or GeometryArray"))
    }

    /// Describe the query plan steps.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry, optional
    ///     The query to plan for; omitted, the index itself is described.
    /// predicate : str, optional
    ///     Spatial relation the plan would refine with; omitted, the plan
    ///     stops at the candidate filter.
    /// distance : float, optional
    ///     ``'dwithin'`` distance threshold (or envelope expansion), in ``unit``.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
    ///     geographic CRS — only for deliberate coordinate-space math);
    ///     ``'meters'`` forces the CRS metric and raises without a CRS.
    ///
    /// Returns
    /// -------
    /// list of str
    ///     One line per plan step.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If ``predicate`` is unknown, ``distance`` is invalid, or
    ///     ``unit='meters'`` is requested for a CRS-free geometry.
    #[pyo3(signature = (geom = None, *, predicate = None, distance = None, unit = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]).explain()[0]
    /// 'loaded 2 geometries'
    /// >>> gm.SpatialIndex([gm.box(0, 0, 1, 1)]).explain()[0]
    /// 'loaded 1 geometry'
    pub(crate) fn explain(
        &self,
        geom: Option<&PyGeometry>,
        predicate: Option<&str>,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<String>> {
        let predicate = IndexPredicate::parse_opt(predicate)?;
        let distance = query_distance(predicate, distance)?;
        // Live geometries only — allocated slots include tombstones / missing
        // rows that were never indexed.
        let n = self.__len__();
        let loaded = if n == 1 {
            format!("loaded {n} geometry")
        } else {
            format!("loaded {n} geometries")
        };
        let mut steps = vec![loaded, "bulk-loaded packed STR envelope index".to_owned()];
        let Some(geom) = geom else {
            // No query geometry: describe the index itself, not a plan for a
            // query that does not exist.
            return Ok(steps);
        };
        let plan = PreparedIndexQuery::for_geometry(self, geom, predicate, distance, unit)?;
        // No predicate = the `candidates(...)` plan: the envelope filter
        // (optionally distance-expanded) with no exact refine step.
        match &plan {
            PreparedIndexQuery::Candidates | PreparedIndexQuery::CandidatesDwithin { .. } => {
                steps.push(plan.candidate_step(self, Some(geom.shape.shape())));
                return Ok(steps);
            },
            PreparedIndexQuery::Dwithin {
                distance, metric, ..
            } => {
                steps.push("predicate operands: predicate(query_geom, indexed_row)".to_owned());
                steps.push(plan.candidate_step(self, Some(geom.shape.shape())));
                steps.push(format!(
                    "exact {} distance refine within {}",
                    metric.explain_label(),
                    distance.get()
                ));
            },
            PreparedIndexQuery::Topological { predicate } => {
                steps.push("predicate operands: predicate(query_geom, indexed_row)".to_owned());
                let envelope = predicate
                    .spec()
                    .index_envelope
                    .expect("IndexPredicate::parse rejects non-indexable predicates");
                steps.push(match envelope {
                    IndexEnvelope::ContainedInQuery => {
                        "bounds envelope containment filter".to_owned()
                    },
                    IndexEnvelope::Intersecting => "bounds envelope candidate filter".to_owned(),
                });
                steps.push(format!("exact {} predicate refine", predicate.token()));
            },
        }
        Ok(steps)
    }

    /// Nearest indexed geometries to the query.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     The query geometry, or one query per array element.
    /// k : int, default 1
    ///     How many nearest neighbors to return.
    /// max_distance : float, optional
    ///     Ignore matches farther than this, in ``unit``.
    /// return_distance : bool, default False
    ///     Return distances alongside handles — ``(indices, distances)`` for
    ///     a scalar query, ``(matches, distances)`` for an array query.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
    ///     geographic CRS — only for deliberate coordinate-space math);
    ///     ``'meters'`` forces the CRS metric and raises without a CRS.
    /// exclusive : bool, default False
    ///     Skip an indexed geometry equal to the query (self-matches in
    ///     joins over the indexed set itself).
    /// ties : bool, default False
    ///     Also return every geometry TYING the k-th nearest distance
    ///     (exact comparison) — results can then exceed ``k``.
    ///
    /// Returns
    /// -------
    /// int64 numpy.ndarray, Groups, or tuple
    ///     The nearest handles — an `int64` ndarray for a scalar query,
    ///     CSR `Groups` for an array query. With ``return_distance=True``,
    ///     plain tuple field order is ``(indices, distances)`` for a scalar
    ///     query or ``(matches, distances)`` for an array query (distances
    ///     parallel to ``matches.values``).
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If ``k`` or ``max_distance`` is invalid, or ``unit='meters'`` is
    ///     requested for a CRS-free geometry.
    #[pyo3(
        signature = (geom, *, k = 1, max_distance = None, return_distance = false, unit = None, exclusive = false, ties = false),
        text_signature = "($self, geom, *, k=1, max_distance=None, return_distance=False, unit=None, exclusive=False, ties=False)"
    )]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.nearest(gm.Point(4, 4)).tolist()
    /// [1]
    pub(crate) fn nearest(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        k: i64,
        max_distance: Option<f64>,
        return_distance: bool,
        unit: Option<DistanceUnit>,
        exclusive: bool,
        ties: bool,
    ) -> PyResult<Py<PyAny>> {
        let k = validate_nearest_k(k)?;
        let max_distance = parse_max_distance(max_distance)?;
        if let Some(geometry) = exact_geometry(geom) {
            let pairs = self.nearest_one(geometry, k, unit, max_distance, NearestOptions {
                exclude_equal: exclusive,
                include_ties: ties,
            })?;
            return format_nearest(py, pairs, return_distance);
        }
        if let Some(array) = exact_geometry_array(geom) {
            // One frame check and metric resolution for the whole array;
            // every storage's rows drive `nearest_core` on stack handles —
            // no per-row `PyGeometry` materialization anywhere.
            // Batch-local R-tree frontier heap reused across rows (clear+push;
            // free-threading: not a receiver cache / lock).
            self.ensure_frame_compatible(array.crs_ref(), array.epoch(), "spatial index nearest")?;
            let metric = resolve_metric(self.metric_crs_str(array.crs_str()), unit, "nearest")?;
            let row_count = array.storage().len();
            let row_capacity = k.min(self.__len__());
            let capacity = row_count.checked_mul(row_capacity).unwrap_or(0);
            let mut ids = Vec::with_capacity(capacity);
            let mut distances = Vec::with_capacity(if return_distance { capacity } else { 0 });
            let mut offsets = Vec::with_capacity(row_count + 1);
            offsets.push(0);
            let mut frontier = BinaryHeap::new();
            for (row_index, (missing, row)) in array.masked_storage_rows().enumerate() {
                if !missing {
                    let query = array.prepared_row(row_index, row);
                    let candidates = self.nearest_core_ties(
                        &metric,
                        &query,
                        &array.row_frame_cache(row_index),
                        k,
                        max_distance,
                        NearestOptions {
                            exclude_equal: exclusive,
                            include_ties: ties,
                        },
                        &mut frontier,
                    )?;
                    for candidate in candidates {
                        ids.push(candidate.idx);
                        if return_distance {
                            distances.push(candidate.distance);
                        }
                    }
                }
                offsets.push(ids.len());
            }
            return format_nearest_rows(py, ids, offsets, distances, return_distance);
        }
        Err(PyTypeError::new_err("expected Geometry or GeometryArray"))
    }

    /// Insert one geometry or many geometries and return their stable handles.
    ///
    /// A single `Geometry` returns one ``int`` handle. A `GeometryArray` or
    /// generic iterable of geometries returns a read-only `int64` ndarray of
    /// handles in input order. Batch inserts follow the same frame and envelope rules as scalar
    /// insert: the first inserted row fixes an empty index's CRS/epoch frame,
    /// later inserts must match it, and geographic antimeridian-crossing rows use
    /// the wrapped-band envelope required by ``self_join``.
    ///
    /// Parameters
    /// ----------
    /// values : Geometry or GeometryArray or iterable of Geometry
    ///     Values to append to the index; all must share the
    ///     index's CRS/epoch frame. Empty geometries cannot be inserted.
    ///
    /// Returns
    /// -------
    /// int or numpy.ndarray
    ///     Stable handle for a scalar insert, or stable handles assigned to a
    ///     batch insert in input order as a read-only int64 ndarray.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the geometry or geometries do not share the index's CRS/epoch
    ///     frame.
    /// GeometryError
    ///     If any inserted geometry is empty.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([])
    /// >>> idx.insert(gm.Point(1, 1))
    /// 0
    pub(crate) fn insert(&mut self, values: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = values.py();
        if let Some(geometry) = exact_geometry(values) {
            return self.insert_one(geometry)?.into_py_any(py);
        }
        // Batch handles return a read-only int64 ndarray, matching every other
        // index id lane (query/candidates/nearest/join/self_join).
        let handles = if let Some(array) = exact_geometry_array(values) {
            self.insert_array(array)?
        } else {
            self.insert_items(geometry_items(values)?)?
        };
        crate::py::numpy::int64_array(py, handles.into_iter().map(|h| h as i64).collect())
    }

    /// Remove a geometry by its handle. Returns ``True`` if a live geometry
    /// was removed, ``False`` if the handle is unknown or was already removed.
    /// Removed handles are not reused, so surviving handles stay stable.
    ///
    /// Parameters
    /// ----------
    /// handle : int
    ///     The handle returned by ``insert`` (or a position from building).
    ///
    /// Returns
    /// -------
    /// bool
    ///     Whether a live geometry was removed.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([])
    /// >>> handle = idx.insert(gm.Point(1, 1))
    /// >>> idx.remove(handle)
    /// True
    pub(crate) fn remove(&mut self, handle: &Bound<'_, PyAny>) -> PyResult<bool> {
        let handle = parse_handle_like(handle)?;
        let Some(row) = self.rows.get(handle) else {
            return Ok(false);
        };
        let non_prunable = row.with_shape(|shape| !geodesic_prunable_point(shape));
        let removed = if handle < self.bulk.initial_len() {
            self.bulk.tombstone(handle)
        } else {
            let Some(bounds) = row.with_shape(Shape::bounds) else {
                return Ok(false);
            };
            // Mirror insert's envelope exactly (crossing rows and physical
            // poles get the full-longitude band) so the overflow R-tree can
            // locate the entry to remove.
            let envelope = row.with_shape(|shape| index_envelope(shape, bounds, self.geographic()));
            let entry = IndexEntry {
                idx: handle,
                envelope,
            };
            self.overflow.remove(&entry).is_some()
        };
        if removed {
            self.rows.mark_removed(handle);
            if non_prunable {
                debug_assert!(self.non_prunable_live > 0);
                self.non_prunable_live -= 1;
            }
            self.mutation_gen = self.mutation_gen.wrapping_add(1);
        }
        Ok(removed)
    }
}
