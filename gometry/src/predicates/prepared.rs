use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{HeapSize, PyGeometry, Typed, exact_geometry, expected_geometry_or_array};
// --- PreparedGeometry #[pymethods] (moved from crate root) ---

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `PreparedGeometry` by re-preparing its geometry
/// (internal; see ``PreparedGeometry.__reduce__``).
#[pyfunction]
pub(crate) fn _unpickle_prepared(geometry: &Bound<'_, PyAny>) -> PyResult<PyPreparedGeometry> {
    let geometry = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    geometry.prepared_plan()?;
    Ok(PyPreparedGeometry { geometry })
}

/// A geometry handle that opts repeated predicate tests into prepared kernels.
///
/// Returned by ``geom.prepare()``. The handle is usable on either side of
/// free predicate functions. The relevant spatial product is built lazily
/// on first use. The handle shares ownership of the source geometry.
#[pyclass(
    name = "PreparedGeometry",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyPreparedGeometry {
    pub geometry: PyGeometry,
}

// PreparedGeometry is shared across threads under free-threaded CPython: the
// geometry is Arc-backed immutable state and lazy caches live in Sync
// OnceLock/Mutex slots on PreparedPlan and its frame sidecar.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PyPreparedGeometry>();
};

impl PyPreparedGeometry {
    pub(crate) fn as_geometry(&self) -> PyGeometry {
        self.geometry.clone()
    }
}

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
        Typed(self.as_geometry())
    }

    /// False when the source geometry is empty; True otherwise.
    fn __bool__(&self) -> bool {
        !self.geometry.shape.is_empty()
    }

    /// ``sys.getsizeof`` support: the wrapper plus the source geometry's
    /// retained native cost (the source ``ShapeData`` Arc, prepared-plan
    /// products, and frame caches already built on this handle). Calling
    /// this does not build new caches.
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
        self.geometry.heap_bytes()
    }
}
