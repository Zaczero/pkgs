#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{HeapSize, PyGeometry, Typed, exact_geometry, expected_geometry_or_array};
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

/// A geometry handle that opts repeated predicate tests into prepared kernels.
///
/// Returned by ``geom.prepare()``: the full predicate surface
/// The relevant spatial product is built lazily on first use. Pass this handle
/// on either side of free predicate functions.
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
    /// retained native cost (``ShapeData`` Arc, shape payload, and any
    /// prepared/frame caches already built on that shared handle). Calling
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
        // Same retained model as `Geometry.__sizeof__`: the shared
        // `ShapeData` Arc block + nested heap + frame-cache sidecar.
        // Does not force uninitialized lazy products.
        std::mem::size_of_val(self.geometry.shape.as_ref())
            + self.geometry.shape.retained_heap_bytes()
            + std::mem::size_of_val(self.geometry.frame_cache.as_ref())
            + self.geometry.frame_cache.heap_bytes()
    }
}
