use std::sync::{Arc, Mutex};

use pyo3::types::PyDict;

use crate::HeapSize;
use crate::py::crs::{
    Bound, Crs, IntoPyObject as _, Py, PyAny, PyResult, Python, crs, crs_parse_required, pyclass,
    pymethods,
};
/// Coordinate reference system.
///
/// A PROJ-backed CRS object: introspect it (``crs.is_geographic``,
/// ``crs.ellipsoid``), serialize it (``crs.to_wkt()``, ``crs.to_epsg()``),
/// compute on it (``crs.factors(...)``, ``crs.geodesic(...)``), and attach it
/// to geometries via ``to_crs``. Accepts an authority string, EPSG code,
/// authority tuple, PROJJSON/CF mapping, WKT/PROJ string, or another ``CRS``.
///
/// Equality is structural — a ``CRS`` compares equal only to the same
/// canonical stored label (``crs == 4326`` and ``crs == 'EPSG:4326'``).
/// Operational equivalence, including axis-order-only differences, is the
/// explicit ``same_as(..., mode='ignore_axis_order')`` query. It is therefore
/// unhashable; key mappings by ``crs.canonical`` instead.
#[pyclass(name = "CRS", frozen, immutable_type, weakref, module = "gometry")]
pub struct PyCrs {
    pub(crate) canonical: Crs,
    /// Shared with the process-wide CRS info cache — store the `Arc`, never
    /// deep-clone the heavy `CrsInfo` into this cell. Stamped with
    /// [`crs::runtime_config_generation`] so ``crs_clear_cache`` /
    /// ``crs_configure`` re-resolve instead of returning permanently stale
    /// metadata from a prior PROJ database.
    info: Mutex<Option<(u64, Arc<crs::CrsInfo>)>>,
    /// Receiver-local frozen Python dict for ``CRS.info``, stamped with the
    /// CRS runtime-config generation so ``crs_clear_cache`` invalidates it.
    /// Kept separate from the free ``crs_info()`` bounded global LRU — held
    /// receivers must not thrash that cache.
    info_py: Mutex<Option<(u64, Py<PyDict>)>>,
}

impl PyCrs {
    pub(crate) const fn from_canonical(canonical: Crs) -> Self {
        Self {
            canonical,
            info: Mutex::new(None),
            info_py: Mutex::new(None),
        }
    }

    pub(crate) fn resolve(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from_canonical(crs_parse_required(value)?))
    }

    /// Lazily resolved, generation-stamped PROJ description of this CRS.
    ///
    /// Returns an `Arc` so the receiver cell can be re-resolved after a
    /// runtime-config generation bump without dangling references.
    pub(crate) fn cached_info(&self) -> PyResult<Arc<crs::CrsInfo>> {
        let generation = crs::runtime_config_generation();
        {
            let guard = self
                .info
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((cached_gen, info)) = guard.as_ref()
                && *cached_gen == generation
            {
                return Ok(Arc::clone(info));
            }
        }
        // Resolve outside the lock (PROJ may take its own locks).
        let resolved = crs::info(&self.canonical)?;
        {
            let mut guard = self
                .info
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Another thread may have won the race; prefer a matching entry.
            if let Some((cached_gen, info)) = guard.as_ref()
                && *cached_gen == generation
            {
                return Ok(Arc::clone(info));
            }
            *guard = Some((generation, Arc::clone(&resolved)));
        }
        Ok(resolved)
    }

    /// Isolation-safe Python dict for ``CRS.info``, cached on this receiver.
    pub(crate) fn cached_info_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let generation = crs::runtime_config_generation();
        {
            let guard = self
                .info_py
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((cached_gen, dict)) = guard.as_ref()
                && *cached_gen == generation
            {
                return super::list_cache::isolation_copy_dict(py, dict);
            }
        }
        // Build outside the lock (IntoPyObject may take the GIL / call Python).
        let info = (*self.cached_info()?).clone();
        let built = info.into_pyobject(py)?;
        let owned = super::list_cache::freeze_top_dict(py, built)?;
        let out = super::list_cache::isolation_copy_dict(py, &owned)?;
        {
            let mut guard = self
                .info_py
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Another thread may have won the race; prefer the first stored entry
            // if its generation still matches.
            if let Some((cached_gen, dict)) = guard.as_ref()
                && *cached_gen == generation
            {
                return super::list_cache::isolation_copy_dict(py, dict);
            }
            *guard = Some((generation, owned));
        }
        Ok(out)
    }

    fn retained_heap_bytes(&self) -> usize {
        self.canonical.heap_bytes() + self.info.heap_bytes()
    }
}

#[pymethods]
impl PyCrs {
    /// ``sys.getsizeof`` support: the wrapper plus owned Rust-side CRS text
    /// retained by this object. This includes the canonical CRS string when it
    /// spills out of `SmolStr` inline storage and any heap strings/vectors in
    /// the lazily cached `CrsInfo`. Opaque PROJ process-global caches are not
    /// owned by the Python object and are not counted.
    fn __sizeof__(&self) -> usize {
        self.total_size()
    }
}

impl HeapSize for PyCrs {
    fn heap_bytes(&self) -> usize {
        self.retained_heap_bytes()
    }
}

impl HeapSize for crs::AreaOfUse {
    fn heap_bytes(&self) -> usize {
        let Self {
            west: _west,
            south: _south,
            east: _east,
            north: _north,
            name,
        } = self;
        name.heap_bytes()
    }
}

impl HeapSize for crs::AuthorityObjectInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            crs,
            authority,
            code,
            name,
            kind: _kind,
            deprecated: _deprecated,
            area_of_use,
        } = self;
        crs.heap_bytes()
            + authority.heap_bytes()
            + code.heap_bytes()
            + name.heap_bytes()
            + area_of_use.heap_bytes()
    }
}

impl HeapSize for crs::AxisInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            abbreviation,
            direction,
            unit_name,
            unit_conversion_factor: _unit_conversion_factor,
        } = self;
        name.heap_bytes()
            + abbreviation.heap_bytes()
            + direction.heap_bytes()
            + unit_name.heap_bytes()
    }
}

impl HeapSize for crs::DomainInfo {
    fn heap_bytes(&self) -> usize {
        let Self { scope, area_of_use } = self;
        scope.heap_bytes() + area_of_use.heap_bytes()
    }
}

impl HeapSize for crs::DatumInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            authority,
            code,
            kind: _kind,
            frame_reference_epoch: _frame_reference_epoch,
            ensemble_accuracy: _ensemble_accuracy,
            ensemble_members,
        } = self;
        name.heap_bytes()
            + authority.heap_bytes()
            + code.heap_bytes()
            + ensemble_members.heap_bytes()
    }
}

impl HeapSize for crs::EllipsoidInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            semi_major_metre: _semi_major_metre,
            semi_minor_metre: _semi_minor_metre,
            inverse_flattening: _inverse_flattening,
            is_semi_minor_computed: _is_semi_minor_computed,
        } = self;
        name.heap_bytes()
    }
}

impl HeapSize for crs::PrimeMeridianInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            longitude: _longitude,
            unit_name,
            unit_conversion_factor: _unit_conversion_factor,
        } = self;
        name.heap_bytes() + unit_name.heap_bytes()
    }
}

impl HeapSize for crs::OperationParameterInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            authority,
            code,
            value,
            value_string,
            unit_conversion_factor: _unit_conversion_factor,
            unit_name,
            unit_authority,
            unit_code,
            unit_category,
        } = self;
        name.heap_bytes()
            + authority.heap_bytes()
            + code.heap_bytes()
            + value.heap_bytes()
            + value_string.heap_bytes()
            + unit_name.heap_bytes()
            + unit_authority.heap_bytes()
            + unit_code.heap_bytes()
            + unit_category.heap_bytes()
    }
}

impl HeapSize for crs::GridInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            short_name,
            full_name,
            package_name,
            available: _available,
        } = self;
        short_name.heap_bytes() + full_name.heap_bytes() + package_name.heap_bytes()
    }
}

impl HeapSize for crs::MethodInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            authority,
            code,
        } = self;
        name.heap_bytes() + authority.heap_bytes() + code.heap_bytes()
    }
}

impl HeapSize for crs::CrsCoordinateOperationInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            name,
            definition,
            description,
            accuracy: _accuracy,
            has_inverse: _has_inverse,
            method,
            parameters,
            grids,
            steps,
            area_of_use,
            has_ballpark_transformation: _has_ballpark_transformation,
            requires_coordinate_epoch: _requires_coordinate_epoch,
            instantiable: _instantiable,
        } = self;
        name.heap_bytes()
            + definition.heap_bytes()
            + description.heap_bytes()
            + method.heap_bytes()
            + parameters.heap_bytes()
            + grids.heap_bytes()
            + steps.heap_bytes()
            + area_of_use.heap_bytes()
    }
}

impl HeapSize for crs::CrsInfo {
    fn heap_bytes(&self) -> usize {
        let Self {
            crs,
            name,
            authority,
            code,
            kind: _kind,
            is_derived: _is_derived,
            deprecated: _deprecated,
            remarks,
            scope,
            coordinate_system: _coordinate_system,
            axis_order,
            celestial_body,
            has_point_motion_operation: _has_point_motion_operation,
            area_of_use,
            axes,
            domains,
            sub_crs,
            source_crs,
            target_crs,
            coordinate_operation,
            geodetic_crs,
            horizontal_datum,
            datum,
            ellipsoid,
            prime_meridian,
        } = self;
        crs.heap_bytes()
            + name.heap_bytes()
            + authority.heap_bytes()
            + code.heap_bytes()
            + remarks.heap_bytes()
            + scope.heap_bytes()
            + axis_order.heap_bytes()
            + celestial_body.heap_bytes()
            + area_of_use.heap_bytes()
            + axes.heap_bytes()
            + domains.heap_bytes()
            + sub_crs.heap_bytes()
            + source_crs.heap_bytes()
            + target_crs.heap_bytes()
            + coordinate_operation.heap_bytes()
            + geodetic_crs.heap_bytes()
            + horizontal_datum.heap_bytes()
            + datum.heap_bytes()
            + ellipsoid.heap_bytes()
            + prime_meridian.heap_bytes()
    }
}
