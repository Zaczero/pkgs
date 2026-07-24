#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::super::*;
use crate::HeapSize;
use crate::broadcast::bool_array_mask_missing;
// --- PreparedGeometry #[pymethods] (moved from crate root) ---

/// Rebuild a pickled `PreparedGeometry` by re-preparing its geometry
/// (internal; see ``PreparedGeometry.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_prepared(geometry: &Bound<'_, PyAny>) -> PyResult<PyPreparedGeometry> {
    let geometry = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    Ok(PyPreparedGeometry { geometry })
}

/// A geometry with a prebuilt edge index for repeated predicate tests.
///
/// Returned by ``geom.prepare()``: the full predicate surface
/// (``contains``/``intersects``/...) against one fixed geometry whose spatial
/// structure is indexed once and reused; each call accepts a scalar or array
/// of probes. Prefer it when the same geometry is tested across many separate
/// calls — the array-broadcast surfaces already auto-prepare internally.
#[pyclass(
    name = "PreparedGeometry",
    module = "gometry",
    frozen,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyPreparedGeometry {
    pub geometry: PyGeometry,
}

// PreparedGeometry is shared across threads under free-threaded CPython: the
// geometry is Arc-backed immutable state and lazy caches live in Sync
// OnceLock/Mutex slots on ShapeData.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PyPreparedGeometry>();
};

frozen_pymethods! {
impl PyPreparedGeometry {
    /// Source geometry retained by this prepared handle.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     The original typed geometry, sharing its immutable coordinate payload.
    #[getter]
    fn geometry(&self) -> Typed {
        Typed(self.geometry.clone())
    }

    /// ``sys.getsizeof`` support: the wrapper plus the source geometry's
    /// retained coordinate payload and any prepared caches already built on
    /// that shared geometry handle. Calling this does not build new caches.
    fn __sizeof__(&self) -> usize {
        self.total_size()
    }

    /// Pickles as the source geometry plus a re-`prepare()` on load: the
    /// cached indexes are transient state, rebuilt cheaply on first use in
    /// the new process (`multiprocessing`/`dask` round-trips just work).
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Typed,))> {
        Ok((
            crate::gometry_lib_module(py)?
                .getattr(pyo3::intern!(py, "_unpickle_prepared"))?
                .unbind(),
            (Typed(self.geometry.clone()),),
        ))
    }

    /// Two prepared handles are equal when their source geometries are equal.
    fn __eq__(&self, other: &Self) -> bool {
        let (left, right) = (&self.geometry, &other.geometry);
        left.crs_ref() == right.crs_ref()
            && left.epoch() == right.epoch()
            && left.shape == right.shape
    }

    /// Hash consistent with `__eq__` (the wrapped geometry only).
    fn __hash__(&self) -> u64 {
        crate::collections::python_hash(&(
            self.geometry.crs_ref(),
            self.geometry.epoch().map(f64::to_bits),
            &self.geometry.shape,
        ))
    }

    /// Test whether this prepared geometry contains ``geom``.
    ///
    /// Same definition as ``contains``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// contains : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().contains(gm.Point(1, 1))
    /// True
pub fn contains(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Contains)
    }

    /// Test whether this prepared geometry contains ``geom`` properly.
    ///
    /// Same definition as ``contains_properly`` (no boundary contact).
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// contains_properly : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().contains_properly(gm.Point(1, 1))
    /// True
pub fn contains_properly(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::ContainsProperly)
    }

    /// Test whether this prepared geometry intersects ``geom``.
    ///
    /// Same definition as ``intersects``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// intersects : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().intersects(gm.Point(1, 1))
    /// True
pub fn intersects(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Intersects)
    }

    /// Test whether this prepared geometry lies within ``geom``.
    ///
    /// Same definition as ``within``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// within : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().within(gm.box(-1, -1, 3, 3))
    /// True
pub fn within(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Within)
    }

    /// Test whether this prepared geometry covers ``geom``.
    ///
    /// Same definition as ``covers``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// covers : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().covers(gm.Point(0, 0))
    /// True
pub fn covers(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Covers)
    }

    /// Test whether this prepared geometry is covered by ``geom``.
    ///
    /// Same definition as ``covered_by``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// covered_by : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().covered_by(gm.box(-1, -1, 3, 3))
    /// True
pub fn covered_by(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::CoveredBy)
    }

    /// Test whether this prepared geometry is disjoint from ``geom``.
    ///
    /// Same definition as ``disjoint``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// disjoint : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().disjoint(gm.Point(5, 5))
    /// True
pub fn disjoint(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Disjoint)
    }

    /// Test whether this prepared geometry touches ``geom``.
    ///
    /// Same definition as ``touches``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// touches : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().touches(gm.Point(0, 1))
    /// True
pub fn touches(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Touches)
    }

    /// Test whether this prepared geometry crosses ``geom``.
    ///
    /// Same definition as ``crosses``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// crosses : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().crosses(gm.LineString([(-1, 1), (3, 1)]))
    /// True
pub fn crosses(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Crosses)
    }

    /// Test whether this prepared geometry overlaps ``geom``.
    ///
    /// Same definition as ``overlaps``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// overlaps : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().overlaps(gm.box(1, 1, 3, 3))
    /// True
pub fn overlaps(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Overlaps)
    }

    /// Test whether this prepared geometry is topologically equal to ``geom``.
    ///
    /// Same definition as ``equals``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Probe geometry or array of probes; must share the prepared
    ///     geometry's CRS. A scalar gives one ``bool``; an array gives one
    ///     result per row.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     Whether the relation holds; one result per input.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    ///
    /// See Also
    /// --------
    /// equals : Free-function form of the same predicate.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().equals(gm.box(0, 0, 2, 2))
    /// True
pub fn equals(&self, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.eval_predicate(geom, Predicate::Equals)
    }

    /// Test whether this prepared geometry is within ``distance`` of ``geom``.
    ///
    /// Same definition as ``dwithin``.
    ///
    /// Parameters
    /// ----------
    /// geom : Geometry or GeometryArray
    ///     Geometry (or array) to test; must share this geometry's CRS.
    /// distance : float
    ///     Non-negative threshold.
    /// unit : {'planar', 'meters'}, default None
    ///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
    ///     units on a projected one, coordinate units without a CRS.
    ///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
    ///     geographic CRS — only for deliberate coordinate-space math);
    ///     ``'meters'`` forces the CRS metric and raises without a CRS.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     One result per input geometry.
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If the operands' CRS or coordinate-epoch metadata differ.
    /// GeometryError
    ///     If ``distance`` is negative or non-finite, or ``unit='meters'`` is
    ///     requested for a CRS-free geometry.
    #[pyo3(signature = (geom, distance, *, unit = None))]
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().dwithin(gm.Point(3, 0), 1.0)
    /// True
pub fn dwithin(
        &self,
        geom: &Bound<'_, PyAny>,
        distance: &Bound<'_, PyAny>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let distance = validate_distance_arg(distance)?.get();
        self.dwithin_eval(geom, distance, unit)
    }

    /// Test whether this prepared geometry contains each ``(x, y)`` point.
    ///
    /// Parameters
    /// ----------
    /// x, y : float or sequence of float
    ///     Finite coordinates in the prepared geometry's CRS. Geographic
    ///     antimeridian seams and poles use full point-predicate topology.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     A single bool for scalar ``x, y``, or one result per coordinate.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If ``x``/``y`` are non-finite or differ in length.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().contains_xy(1, 1)
    /// True
pub fn contains_xy(
        &self,
        py: Python<'_>,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        xy_predicate_geometry(py, &self.geometry, x, y, false)
    }

    /// Test whether this prepared geometry intersects each ``(x, y)`` point.
    ///
    /// Boundary-inclusive (unlike ``contains_xy``).
    ///
    /// Parameters
    /// ----------
    /// x, y : float or sequence of float
    ///     Finite coordinates in the prepared geometry's CRS. Geographic
    ///     antimeridian seams and poles use full point-predicate topology.
    ///
    /// Returns
    /// -------
    /// bool or numpy.ndarray
    ///     A single bool for scalar ``x, y``, or one result per coordinate.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If ``x``/``y`` are non-finite or differ in length.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().intersects_xy(3, 3)
    /// False
pub fn intersects_xy(
        &self,
        py: Python<'_>,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        xy_predicate_geometry(py, &self.geometry, x, y, true)
    }

    /// Describe the prepared-predicate plan.
    ///
    /// Returns
    /// -------
    /// list of str
    ///     One line per plan step.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.box(0, 0, 2, 2).prepare().explain()[0]
    /// 'prepared geometry: Polygon'
pub fn explain(&self) -> Vec<String> {
        let mut plan = vec![
            format!("prepared geometry: {}", self.geometry.shape.geometry_type()),
            "predicate kernel: native cached facet-tree engines".to_owned(),
            "scalar geometry inputs: bounds gate, then the cached pair kernel".to_owned(),
            "array geometry inputs: shared batch engine (point/line gates, cached scalar state)"
                .to_owned(),
            "packed point arrays: direct Rust point-in-geometry kernel".to_owned(),
        ];
        if let Some(crs) = self.geometry.crs_ref() {
            plan.push(format!("CRS metadata: {crs}"));
        }
        plan
    }

    pub fn __repr__(&self) -> String {
        format!(
            "<PreparedGeometry geometry_type={}>",
            self.geometry.shape.geometry_type()
        )
    }
}
}

impl HeapSize for PyPreparedGeometry {
    fn heap_bytes(&self) -> usize {
        self.geometry.shape.retained_heap_bytes()
    }
}

impl PyPreparedGeometry {
    fn eval_predicate(
        &self,
        values: &Bound<'_, PyAny>,
        predicate: Predicate,
    ) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            let spec = predicate.spec();
            // The prepared geometry's frame is the single source of truth for
            // antimeridian split-normalization (matched against every queried
            // operand by the frame checks below); never assume planar.
            let geographic = crate::geometry::is_geographic_frame(&self.geometry.frame);
            if let Some(geometry) = exact_geometry(values) {
                self.geometry
                    .frame
                    .compatible(&geometry.frame, spec.token)?;
                let held =
                    topology_scalar_pair(&spec, &self.geometry.shape, &geometry.shape, geographic);
                return Ok(py_bool(py, held));
            }
            if let Some(array) = exact_geometry_array(values) {
                Frame::compatible_parts(
                    self.geometry.crs_ref(),
                    self.geometry.epoch(),
                    array.crs_ref(),
                    array.epoch(),
                    spec.token,
                )?;
                let held_crosses = geographic && self.geometry.shape.shape().crosses_antimeridian();
                if spec.right_point.is_some()
                    && !held_crosses
                    && let Some(points) = array.storage().point_rows()
                {
                    let shape = self.geometry.shape.clone();
                    let result = py.detach(move || {
                        crate::py::functions::predicate::point_batch(&spec, &shape, &points, true)
                            .expect("right_point checked above")
                    });
                    // Missing rows carry a `POINT (NaN NaN)` placeholder that
                    // must not be evaluated: force them to the predicate's
                    // missing sentinel (`false`), matching the free-function
                    // path (`bool_array_mask_missing`, broadcast/predicates.rs).
                    return bool_array_mask_missing(py, result, array.missing());
                }
                let result: Vec<bool> = if array.storage().len() >= PREPARED_PREDICATE_MIN {
                    let array = array.clone();
                    let shape = self.geometry.shape.clone();
                    py.detach(move || {
                        scalar_vs_shapes(
                            &spec,
                            &shape,
                            array.storage().iter_rows().enumerate(),
                            true,
                            Some(&array),
                            geographic,
                        )
                    })
                } else {
                    array
                        .storage()
                        .iter_rows()
                        .enumerate()
                        .map(|(index, row)| {
                            array.with_row_data(index, row, |element| {
                                topology_scalar_pair(
                                    &spec,
                                    &self.geometry.shape,
                                    element,
                                    geographic,
                                )
                            })
                        })
                        .collect()
                };
                // Force missing rows (evaluated on their placeholder) to the
                // predicate's missing sentinel, matching the free path.
                return bool_array_mask_missing(py, result, array.missing());
            }
            Err(expected_geometry_or_array())
        })
    }

    pub(crate) fn dwithin_eval(
        &self,
        values: &Bound<'_, PyAny>,
        distance: f64,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            if let Some(geometry) = exact_geometry(values) {
                return Ok(py_bool(
                    py,
                    crs_aware_dwithin(&self.geometry, geometry, distance, "dwithin", unit)?,
                ));
            }
            if let Some(array) = exact_geometry_array(values) {
                let result =
                    array_crs_dwithin_scalar(py, array, &self.geometry, distance, "dwithin", unit)?;
                return Ok(result.into_pyobject(py)?.into_any().unbind());
            }
            Err(expected_geometry_or_array())
        })
    }
}
