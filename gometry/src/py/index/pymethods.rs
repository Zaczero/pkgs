use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::types::{PyBool, PyDict, PyMapping, PyTuple, PyTupleMethods as _};
use pyo3::{IntoPyObjectExt as _, PyErr};

use crate::parse_spatial_index_handle;
use crate::py::crs::PyCrs;
use crate::py::index::{
    Bound, DistanceUnit, GeometryError, HeapSize as _, IndexPredicate, IntoPyObject as _,
    NearestOptions, Py, PyAny, PyAnyMethods as _, PyResult, PySpatialIndex, Python,
    build_spatial_index, exact_geometry, exact_geometry_array, format_nearest, geometry_items,
    index_ids, join_indexed, nearest_columns, parse_max_distance, pyfunction, pymethods,
    query_distance, restore_spatial_index, spatial_index, validate_nearest_k,
};
use crate::py::numpy::int64_array;
use crate::py::sizeof::python_sizeof;

type SpatialIndexReduce = (
    Py<PyAny>,
    (crate::PyGeometryArray, Py<PyTuple>, usize, bool),
);

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

fn lookup_geometry(
    py: Python<'_>,
    index: &PySpatialIndex,
    handle: usize,
) -> PyResult<crate::Typed> {
    let geometry = if index.geometry_lookup_is_local(handle)? {
        index.geometry_at_handle(handle)?
    } else {
        py.detach(|| index.geometry_at_handle(handle))?
    };
    Ok(crate::Typed(geometry))
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores this name in its stream"
)]
#[pyfunction]
pub(crate) fn _unpickle_spatial_index_v1(
    values: &crate::PyGeometryArray,
    handles: &Bound<'_, PyTuple>,
    next_handle: &Bound<'_, PyAny>,
    has_metadata: bool,
) -> PyResult<PySpatialIndex> {
    let handle_count = handles.len();
    if handle_count != values.len() {
        return Err(GeometryError::new_err(
            "spatial index pickle handle count does not match value count",
        ));
    }
    let next_handle = parse_index_next_handle(next_handle)?;
    let mut parsed_handles = Vec::with_capacity(handle_count);
    for value in handles.iter() {
        if value.cast_exact::<PyBool>().is_ok() {
            return Err(PyTypeError::new_err(
                "spatial index pickle handles must be integers, not bool",
            ));
        }
        let handle = parse_handle_like(&value)?;
        if handle >= next_handle {
            return Err(GeometryError::new_err(
                "spatial index pickle handle must be less than next_handle",
            ));
        }
        if parsed_handles
            .last()
            .is_some_and(|&previous| previous >= handle)
        {
            return Err(GeometryError::new_err(
                "spatial index pickle handles must be strictly increasing",
            ));
        }
        parsed_handles.push(handle);
    }
    Ok(handles
        .py()
        .detach(|| restore_spatial_index(values, &parsed_handles, next_handle, has_metadata))?)
}

fn parse_index_next_handle(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.cast_exact::<PyBool>().is_ok() {
        return Err(PyTypeError::new_err(
            "spatial index next_handle must be an integer, not bool",
        ));
    }
    let handle = crate::py_i64_required("spatial index next_handle", value)?;
    if handle < 0 {
        return Err(GeometryError::new_err(
            "spatial index next_handle must be non-negative",
        ));
    }
    let handle = usize::try_from(handle)
        .map_err(|_| GeometryError::new_err("spatial index next_handle is too large"))?;
    super::checked_index_handle_end(0, handle).map_err(PyErr::from)?;
    Ok(handle)
}

fn parse_handle_like(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    parse_spatial_index_handle(value)
}

fn handle_like_or_none(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    if value.cast_exact::<PyBool>().is_ok() {
        return Ok(None);
    }
    if !crate::py_index_check(value) {
        return Ok(None);
    }
    let integer = crate::py_index_object("spatial index handle", value)?;
    Ok(integer.extract::<usize>().ok())
}

#[pymethods]
impl PySpatialIndex {
    /// Build a spatial index (STR-tree) over present geometries.
    ///
    /// Parameters
    /// ----------
    /// values : Geometry, GeometryArray, iterable, or None, default None
    ///     Geometries to index. Iterable rows may be native geometries, WKB
    ///     buffers, GeoJSON mappings, or objects exposing
    ///     ``__geo_interface__``. ``None`` builds an empty mutable index for
    ///     later ``insert`` calls. Every indexed geometry must share one CRS
    ///     and coordinate epoch. Missing rows are skipped but retain their
    ///     original positions as stable, non-live handles, so query and join
    ///     results always refer to the input row ids. Packed arrays retain
    ///     their immutable column storage; empty rows likewise have no live
    ///     tree entry.
    ///
    /// Returns
    /// -------
    /// SpatialIndex
    ///     A mutable STR-tree whose handles refer to the original input rows.
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
    fn new(py: Python<'_>, values: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        values.map_or_else(
            || Ok(spatial_index(Vec::new())?),
            |values| build_spatial_index(py, values),
        )
    }

    /// CRS shared by the indexed geometries, or ``None`` for an unframed index.
    ///
    /// Returns
    /// -------
    /// CRS or None
    #[getter]
    fn crs(&self) -> Option<PyCrs> {
        self.metadata
            .as_ref()
            .and_then(crate::Frame::crs_ref)
            .cloned()
            .map(PyCrs::from_definition)
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
    pub(crate) const fn __len__(&self) -> usize {
        self.rows.live_handles.len()
    }

    /// Pickle live geometries with their stable handles and next identity.
    ///
    /// Handles are the public identity returned by ``query``/``nearest`` and
    /// consumed by ``remove``/``__getitem__``. Values are stored compactly in
    /// handle order; removed and missing rows do not occupy pickle rows.
    pub(crate) fn __reduce__(&self, py: Python<'_>) -> PyResult<SpatialIndexReduce> {
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_spatial_index_v1"))?
            .unbind();
        let frame = self.metadata.clone().unwrap_or_default();
        let (array, handles) = py.detach(|| self.rows.pickle_live_rows(frame))?;
        let handles = PyTuple::new(py, handles)?.unbind();
        Ok((
            callable,
            (
                array,
                handles,
                self.rows.next_handle,
                self.metadata.is_some(),
            ),
        ))
    }

    pub(crate) fn __copy__(&self, py: Python<'_>) -> PyResult<Py<Self>> {
        Py::new(py, py.detach(|| self.clone()))
    }

    #[pyo3(signature = (memo))]
    pub(crate) fn __deepcopy__(
        &self,
        py: Python<'_>,
        memo: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        let _ = memo;
        Py::new(py, py.detach(|| self.clone()))
    }

    /// Return the geometry at a live handle.
    ///
    /// The handle must be a non-negative integer accepted by ``__index__``;
    /// ``bool`` is not accepted. Raises ``KeyError`` when a valid handle is
    /// unknown or has been removed.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A typed leaf (`Point`, `Polygon`, …) matching the stored kind —
    ///     never the bare base `Geometry` class.
    pub(crate) fn __getitem__(&self, handle: &Bound<'_, PyAny>) -> PyResult<crate::Typed> {
        let py = handle.py();
        let handle = parse_handle_like(handle)?;
        if !self.is_live_handle(handle) {
            return Err(PyKeyError::new_err(handle));
        }
        lookup_geometry(py, self, handle)
    }

    /// Compare live handles and geometries using mapping value semantics.
    pub(crate) fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if !other.is_instance(&py.get_type::<PyMapping>())? {
            return Ok(false);
        }
        if other.len()? != self.__len__() {
            return Ok(false);
        }
        let values = PyDict::new(py);
        let geometries = py.detach(|| self.live_geometries())?;
        for (handle, geometry) in geometries {
            values.set_item(handle, crate::Typed(geometry).into_pyobject(py)?.unbind())?;
        }
        values.eq(other)
    }

    /// Whether ``handle`` is a live geometry handle.
    ///
    /// Non-integer probes return ``False`` instead of raising, matching
    /// Python's container protocol.
    ///
    /// Returns
    /// -------
    /// bool
    pub(crate) fn __contains__(&self, handle: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(handle_like_or_none(handle)?.is_some_and(|handle| self.is_live_handle(handle)))
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
    /// handle : SupportsIndex
    ///     A non-negative integer/index handle (positional-only; ``bool`` is
    ///     rejected). Invalid or negative handles raise instead of returning
    ///     ``default``.
    ///
    /// default : object, optional
    ///     Value returned when the handle is not live.
    ///
    /// Returns
    /// -------
    /// Geometry or object
    ///     The live geometry, else ``default``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    /// >>> g = idx.get(0)
    /// >>> g is not None and g.to_wkt().startswith('POLYGON')
    /// True
    #[pyo3(signature = (handle, /, default = None))]
    pub(crate) fn get(
        &self,
        py: Python<'_>,
        handle: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let handle = parse_handle_like(handle)?;
        if self.is_live_handle(handle) {
            return Ok(lookup_geometry(py, self, handle)?
                .into_pyobject(py)?
                .unbind());
        }
        Ok(default.unwrap_or_else(|| py.None()))
    }

    pub(crate) fn __repr__(&self) -> String {
        let len = self.rows.live_handles.len();
        self.metadata
            .as_ref()
            .and_then(|frame| frame.crs_str())
            .map_or_else(
                || format!("<SpatialIndex len={len}>"),
                |crs| format!("<SpatialIndex len={len} crs={crs}>"),
            )
    }

    /// `sys.getsizeof` support: Python's allocation base plus the retained Rust-side
    /// index payload — packed or boxed row geometry coordinates, the immutable
    /// STR tree, overflow STR runs, and frame metadata. Shared buffers are
    /// reported as this index's logical retained footprint.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    pub(crate) fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        let index = slf.borrow();
        let native = &*index;
        let bytes = slf.py().detach(|| native.heap_bytes());
        python_sizeof(slf.py(), slf.as_any(), bytes)
    }

    /// Return exact predicate-refined matches for a query geometry, array, or
    /// iterable of geometry rows.
    ///
    /// Topological predicates use `'intersects'` when `predicate` is omitted;
    /// `'dwithin'` requires an explicit predicate and distance. Use
    /// candidates for a bounding-box-only prefilter. A single geometry
    /// returns an int64 ndarray; arrays and iterables return Groups, with one
    /// id row per input row.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry, GeometryArray, or iterable of Geometry or None
    ///     One query geometry or one query per sequence row. Missing rows
    ///     produce missing result groups (`None`); present rows with no matches
    ///     produce empty groups.
    /// predicate : str, default varies by call form
    ///     Spatial relation each match must satisfy, evaluated as
    ///     ``predicate(query_geometry, indexed_geometry)``. Topological forms
    ///     omit ``predicate`` to use ``'intersects'``; ``'dwithin'`` forms
    ///     require an explicit predicate and distance.
    /// distance : float, optional
    ///     ``'dwithin'`` distance threshold, in ``unit``. A positive infinite
    ///     threshold is accepted; other values must be finite and
    ///     non-negative. Missing query rows remain missing result groups;
    ///     present empty query rows produce empty result groups.
    #[doc = unit_parameter_doc!()]
    ///
    /// Returns
    /// -------
    /// int64 numpy.ndarray or Groups
    ///     Matching index handles (row ids). A scalar query returns a read-only
    ///     ``int64`` ndarray of handles; an array query returns ``Groups`` of
    ///     handles, one row per query row. Missing queries yield ``None`` rows,
    ///     while present queries with no matches yield empty groups. Each result
    ///     is in ascending source-row order.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If a query parameter is invalid, or ``unit='meters'`` is requested
    ///     for a CRS-free geometry.
    /// CRSError
    ///     If geographic ``dwithin`` refinement cannot prove a result within
    ///     the point-edge evaluation budget.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.query(gm.Point(0.5, 0.5)).tolist()
    /// [0]
    #[pyo3(signature = (geom, *, predicate = "intersects", distance = None, unit = None))]
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
        let geom = crate::normalize_geometry_batch(py, geom)?;
        if let Some(geometry) = exact_geometry(&geom) {
            let geometry = geometry.clone();
            let matches = py.detach(move || {
                self.query_exact(&geometry, predicate, distance, unit)
                    .map(index_ids)
            })?;
            return int64_array(py, matches);
        }
        if let Some(array) = exact_geometry_array(&geom) {
            return py
                .detach(|| self.query_array(array, predicate, distance, unit))?
                .into_py_any(py);
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
    /// queries : Geometry, GeometryArray, or iterable of Geometry or None
    ///     Left-side geometries to join against the indexed rows.
    /// predicate : str, default varies by call form
    ///     Spatial predicate each returned pair must satisfy. Topological forms
    ///     omit ``predicate`` to use ``'intersects'``; ``'dwithin'`` forms
    ///     require an explicit predicate and distance.
    /// distance : float, optional
    ///     Required when ``predicate='dwithin'``: the maximum separation in
    ///     CRS-natural units. Finite non-negative values and positive infinity
    ///     are accepted; missing query rows produce no pairs.
    #[doc = unit_parameter_doc!()]
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
    /// CRSError
    ///     If geographic ``dwithin`` refinement cannot prove a result within
    ///     the point-edge evaluation budget.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> left, right = idx.join(gm.GeometryArray([gm.Point(0.5, 0.5)]))
    /// >>> (left.tolist(), right.tolist())
    /// ([0], [0])
    #[pyo3(signature = (queries, *, predicate = "intersects", distance = None, unit = None))]
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
    /// predicate : str
    ///     A symmetric relation: ``'intersects'``, ``'equals'``,
    ///     ``'dwithin'``, ``'touches'``, ``'crosses'``, or ``'overlaps'``.
    ///     Directional predicates (``'contains'``, ``'within'``, ...) are
    ///     rejected — unordered pairs would drop the reverse direction; use
    ///     ``join(...)`` for directed relations.
    /// distance : float, optional
    ///     ``'dwithin'`` distance threshold, in ``unit``. Positive infinity is
    ///     accepted; other values must be finite and non-negative. Empty
    ///     geometries never produce a pair.
    #[doc = unit_parameter_doc!()]
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
    /// CRSError
    ///     If geographic ``dwithin`` refinement cannot prove a result within
    ///     the point-edge evaluation budget.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> left, right = idx.self_join()
    /// >>> (left.tolist(), right.tolist())
    /// ([], [])
    #[pyo3(signature = (*, predicate = "intersects", distance = None, unit = None))]
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
        let (left, right) = py.detach(|| self.self_join_pairs(predicate, distance, unit))?;
        Ok((int64_array(py, left)?, int64_array(py, right)?))
    }

    /// Bounding-box candidate matches for a query geometry, array, or iterable (not
    /// exact). If you refine these candidates with a directional predicate,
    /// evaluate it as ``predicate(query_geometry, indexed_geometry)``.
    ///
    /// A single geometry returns an `int64` ndarray; arrays and iterables
    /// return `Groups` — one candidate row per input row.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry, GeometryArray, or iterable of Geometry or None
    ///     One query geometry or one query per sequence row. Missing rows
    ///     produce ``None`` groups; present rows with no candidates produce
    ///     empty groups.
    /// distance : float, optional
    ///     Expand the query envelope by this much, in ``unit``. Finite
    ///     non-negative expansions and positive infinity are accepted;
    ///     infinity admits every live non-empty row for a query with bounds.
    #[doc = unit_parameter_doc!()]
    ///
    /// Returns
    /// -------
    /// int64 numpy.ndarray or Groups
    ///     Source-row handles in ascending order.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the query does not share the index's CRS/epoch frame.
    /// GeometryError
    ///     If a query parameter is invalid, or ``unit='meters'`` is requested
    ///     for a CRS-free geometry.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.candidates(gm.box(0, 0, 1, 1)).tolist()
    /// [0]
    #[pyo3(signature = (geom, *, distance = None, unit = None))]
    pub(crate) fn candidates(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        distance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let distance = query_distance(None, distance)?;
        let geom = crate::normalize_geometry_batch(py, geom)?;
        if let Some(geometry) = exact_geometry(&geom) {
            let matches = py.detach(|| {
                self.candidate_ids_sorted(geometry, distance, unit)
                    .map(index_ids)
            })?;
            return int64_array(py, matches);
        }
        if let Some(array) = exact_geometry_array(&geom) {
            let groups = py.detach(|| self.candidate_array(array, distance, unit))?;
            return groups.into_py_any(py);
        }
        Err(PyTypeError::new_err("expected Geometry or GeometryArray"))
    }

    /// Nearest indexed geometries to the query.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry, GeometryArray, or iterable of Geometry or None
    ///     One query geometry or one query per sequence row. Missing rows
    ///     produce ``None`` groups and retain their logical positions.
    /// k : int, default 1
    ///     How many nearest neighbors to return; ``0`` returns no neighbors.
    /// max_distance : float, optional
    ///     Ignore matches farther than this, in ``unit``. Must be finite and
    ///     non-negative.
    /// return_distance : bool
    ///     Return distances alongside handles — ``(indices, distances)`` for
    ///     a scalar query, ``(matches, distances)`` for an array query. The
    ///     distance-returning form requires ``return_distance=True``.
    #[doc = unit_parameter_doc!()]
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    /// >>> idx.nearest(gm.Point(4, 4)).tolist()
    /// [1]
    #[pyo3(
        signature = (geom, *, k = 1, max_distance = None, return_distance = false, unit = None, exclusive = false, ties = false),
        text_signature = "($self, geom, *, k=1, max_distance=None, return_distance=False, unit=None, exclusive=False, ties=False)"
    )]
    pub(crate) fn nearest(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] k: i64,
        max_distance: Option<f64>,
        return_distance: bool,
        unit: Option<DistanceUnit>,
        exclusive: bool,
        ties: bool,
    ) -> PyResult<Py<PyAny>> {
        let k = validate_nearest_k(k)?;
        let max_distance = parse_max_distance(max_distance)?;
        let geom = crate::normalize_geometry_batch(py, geom)?;
        if let Some(geometry) = exact_geometry(&geom) {
            let geometry = geometry.clone();
            let (ids, distances) = py.detach(move || {
                self.nearest_one(&geometry, k, unit, max_distance, NearestOptions {
                    exclude_equal: exclusive,
                    include_ties: ties,
                })
                .map(|pairs| nearest_columns(pairs, return_distance))
            })?;
            return format_nearest(py, int64_array(py, ids)?, distances);
        }
        if let Some(array) = exact_geometry_array(&geom) {
            let (matches, distances) = py.detach(|| {
                self.nearest_array(
                    array,
                    k,
                    max_distance,
                    return_distance,
                    unit,
                    NearestOptions {
                        exclude_equal: exclusive,
                        include_ties: ties,
                    },
                )
            })?;
            return format_nearest(py, matches.into_py_any(py)?, distances);
        }
        Err(PyTypeError::new_err("expected Geometry or GeometryArray"))
    }

    /// Insert one geometry or many geometries and return their stable handles.
    ///
    /// A single `Geometry` returns one ``int`` handle. A `GeometryArray` or
    /// generic iterable of geometries returns a read-only `int64` ndarray of
    /// handles in input order. Batch inserts follow the same frame rules as
    /// scalar insert: the first inserted row fixes an empty index's CRS/epoch
    /// frame, and later inserts must match it. A GeometryArray containing a
    /// missing row is rejected. All input-validation failures leave the index
    /// unchanged; this is not a transaction guarantee for allocation or
    /// publication failures.
    ///
    /// Parameters
    /// ----------
    /// values : Geometry or GeometryArray or iterable of Geometry
    ///     Values to append to the index; all must share the index's
    ///     CRS/epoch frame. Empty geometries and missing GeometryArray rows
    ///     cannot be inserted.
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
    ///     If any inserted geometry is empty, or a GeometryArray contains a
    ///     missing row.
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
            let handle = if self.scalar_insert_is_local(geometry) {
                self.insert_one(geometry)?
            } else {
                py.detach(|| self.insert_one(geometry))?
            };
            return handle.into_py_any(py);
        }
        // Batch handles return a read-only int64 ndarray, matching every other
        // index id lane (query/candidates/nearest/join/self_join).
        let handles = if let Some(array) = exact_geometry_array(values) {
            py.detach(|| self.insert_array(array).map(index_ids))?
        } else {
            let items = geometry_items(values)?;
            py.detach(|| self.insert_items(items).map(index_ids))?
        };
        int64_array(py, handles)
    }

    /// Remove a geometry by its handle. Returns ``True`` if a live geometry
    /// was removed, ``False`` if the handle is unknown or was already removed.
    /// Removed handles are not reused, so surviving handles stay stable.
    ///
    /// Parameters
    /// ----------
    /// handle : SupportsIndex
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
        let py = handle.py();
        let handle = parse_handle_like(handle)?;
        if !self.is_live_handle(handle) {
            return Ok(false);
        }
        if !self.removal_is_local(handle) {
            return Ok(py.detach(|| self.remove_handle(handle))?);
        }
        Ok(self.remove_handle(handle)?)
    }
}
