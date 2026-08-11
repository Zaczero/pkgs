//! Binary-op operand classification: a Python argument resolved once into a
//! scalar `Geometry` or a `GeometryArray`, plus the native-type extractors.

use crate::boundary::metadata::Frame;
use crate::broadcast::{
    Arc, Bound, GeoJsonDecodeContext, GeometryArrayStorage, PyAny, PyAnyMethods as _, PyErr,
    PyGeometry, PyGeometryArray, PyResult, PyTypeError, PyTypeMethods as _,
    coerce_geojson_geometry_value, crs_arc, ensure_same_len, is_mapping_like, is_one_byte_buffer,
    parse_geojson_geometry_value,
};

pub(crate) fn parse_wkb_geometry(value: &[u8]) -> PyResult<PyGeometry> {
    let geometry = crate::io::parse_wkb(value)?;
    let crs = crate::io::crs_from_optional_srid(geometry.srid)?;
    Ok(PyGeometry::with_frame(
        geometry.shape,
        Frame::new(crs.map(crs_arc), None)?,
    ))
}

/// The canonical "wrong argument type" error for the dispatch helpers, owned in
/// one place instead of repeating the literal at every operand check.
pub(crate) fn expected_geometry_or_array() -> PyErr {
    PyTypeError::new_err("expected Geometry or GeometryArray")
}

/// The value-aware form: a foreign geometry object (shapely, geojson, ...)
/// exposing ``__geo_interface__`` gets a conversion hint instead of a bare
/// type error — the ecosystem's duck type is the #1 mixed-object mistake.
pub(crate) fn expected_geometry_or_array_for(value: &Bound<'_, PyAny>) -> PyErr {
    if value
        .getattr_opt(pyo3::intern!(value.py(), "__geo_interface__"))
        .unwrap_or(None)
        .is_some()
    {
        return PyTypeError::new_err(
            "expected Geometry or GeometryArray, got a foreign geometry object; \
             convert it with gometry.from_geojson(obj) (reads __geo_interface__)",
        );
    }
    expected_geometry_or_array()
}

/// `classify_input` with the canonical error baked in — the one spelling for
/// "this operand must be a Geometry or GeometryArray".
pub(crate) fn classify_required<'a>(value: &'a Bound<'_, PyAny>) -> PyResult<GeometryInput<'a>> {
    classify_input(value).ok_or_else(|| expected_geometry_or_array_for(value))
}

/// One operand of a binary op, resolved once: a scalar `Geometry` or a
/// `GeometryArray`. The classifier behind every binary broadcast.
#[derive(Clone, Copy)]
pub(crate) enum GeometryInput<'a> {
    One(&'a PyGeometry),
    Many(&'a PyGeometryArray),
}

pub(crate) fn classify_input<'a>(value: &'a Bound<'_, PyAny>) -> Option<GeometryInput<'a>> {
    exact_geometry(value)
        .map(GeometryInput::One)
        .or_else(|| exact_geometry_array(value).map(GeometryInput::Many))
}

pub(crate) fn exact_geometry<'a>(value: &'a Bound<'_, PyAny>) -> Option<&'a PyGeometry> {
    // `cast` (not `cast_exact`) so the typed leaf subclasses (Point, Polygon, …)
    // are accepted as geometries — every returned geometry is now a subclass.
    value.cast::<PyGeometry>().ok().map(pyo3::Bound::get)
}

pub(crate) fn exact_geometry_array<'a>(value: &'a Bound<'_, PyAny>) -> Option<&'a PyGeometryArray> {
    value
        .cast_exact::<PyGeometryArray>()
        .ok()
        .map(pyo3::Bound::get)
}

/// The ONLY way to obtain both operand storages of a Many×Many broadcast:
/// runs strict broadcasting (LENGTH FIRST so the length error wins, then frame
/// compatibility) and returns both storage Arcs cloned. Making this the sole
/// producer of the zipped pair means the strict-broadcast check is unskippable
/// and silent zip-truncation of unequal-length arrays cannot recur.
pub(crate) fn paired_arrays(
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    operation: &str,
) -> PyResult<(Arc<GeometryArrayStorage>, Arc<GeometryArrayStorage>)> {
    ensure_same_len(left.storage().len(), right.storage().len())?;
    Frame::compatible_parts(
        left.crs_ref(),
        left.epoch(),
        right.crs_ref(),
        right.epoch(),
        operation,
    )?;
    Ok((
        Arc::clone(left.storage_arc()),
        Arc::clone(right.storage_arc()),
    ))
}

/// `paired_arrays` for geometry-RETURNING lanes that resolve the OUTPUT frame
/// separately via `Frame::compatible_parts` and must NOT also run the
/// operand-equality frame check — length gate only.
pub(crate) fn paired_arrays_len(
    left: &PyGeometryArray,
    right: &PyGeometryArray,
) -> PyResult<(Arc<GeometryArrayStorage>, Arc<GeometryArrayStorage>)> {
    ensure_same_len(left.storage().len(), right.storage().len())?;
    Ok((
        Arc::clone(left.storage_arc()),
        Arc::clone(right.storage_arc()),
    ))
}

/// Resolve one array element: a native `Geometry`, a GeoJSON mapping, or any
/// object exposing ``__geo_interface__`` (Shapely and other adapters).
///
/// Mapping / interface rows use the single shared GeoJSON Shape+Frame coercer
/// with the caller's [`LegacyGeoJsonCrsPolicy`] so legacy ``crs`` is reconciled
/// semantically (``IgnoreAxisOrder``) before generic frame equality.
pub(crate) fn coerce_geometry(
    item: &Bound<'_, PyAny>,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<PyGeometry> {
    if let Some(geometry) = exact_geometry(item) {
        return Ok(geometry.clone());
    }
    if is_one_byte_buffer(item) {
        return parse_wkb_geometry_from_payload(item);
    }
    // Mapping or ``__geo_interface__``: classify without enumerating keys
    // (one-shot keys() streams), then parse once via the keystone.
    if is_mapping_like(item)?
        || item
            .getattr_opt(pyo3::intern!(item.py(), "__geo_interface__"))?
            .is_some()
    {
        let value = parse_geojson_geometry_value(item)?;
        let Some((shape, frame)) = coerce_geojson_geometry_value(
            &value,
            GeoJsonDecodeContext::GeometryLike,
            policy,
            None,
        )?
        else {
            return Err(crate::io::IoError::geojson(
                "Feature has null geometry (an unlocated feature); parse the \
                 FeatureCollection with from_geojson/from_features, where null \
                 geometries become missing rows",
            )
            .into());
        };
        return Ok(PyGeometry::with_frame(shape, frame));
    }
    if item.cast::<pyo3::types::PyString>().is_ok() {
        return Err(PyTypeError::new_err(
            "expected Geometry, got a str; strings parse via from_wkt/from_geojson",
        ));
    }
    Err(PyTypeError::new_err(format!(
        "expected Geometry, got {}",
        item.get_type()
            .name()
            .map_or_else(|_| "<unknown>".to_owned(), |name| name.to_string())
    )))
}

pub(crate) fn parse_wkb_geometry_from_payload(item: &Bound<'_, PyAny>) -> PyResult<PyGeometry> {
    let geometry = crate::parse_wkb_payload(item)?;
    let crs = crate::io::crs_from_optional_srid(geometry.srid)?;
    Ok(PyGeometry::with_frame(
        geometry.shape,
        Frame::new(crs.map(crs_arc), None)?,
    ))
}
