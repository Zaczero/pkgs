#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::{
    GeometryError, InvalidGeometryError, PyTypeError, Shape, exact_geometry, exact_geometry_array,
    geometry_type_err, mapping_as_dict, require_geojson_crs,
};

fn properties_value<'py>(
    py: Python<'py>,
    value: Option<&Bound<'py, PyAny>>,
    preserve_explicit_null: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let Some(value) = value else {
        return Ok(PyDict::new(py).into_any());
    };
    if value.is_none() {
        return if preserve_explicit_null {
            Ok(py.None().into_bound(py))
        } else {
            Ok(PyDict::new(py).into_any())
        };
    }
    // Shared keys()+seen copier (N4): accepts keys()-only ducks and rejects
    // repeated-key streams immediately — never dict.update object-iteration.
    let Some(out) = mapping_as_dict(value)? else {
        return Err(GeometryError::new_err(
            "feature properties must be a mapping or None",
        ));
    };
    for key in out.keys().iter() {
        if !key.is_instance_of::<PyString>() {
            return Err(GeometryError::new_err(
                "feature properties keys must be strings",
            ));
        }
    }
    Ok(out.into_any())
}

fn set_feature_id(feature: &Bound<'_, PyDict>, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(());
    };
    let valid = !value.is_instance_of::<PyBool>()
        && (value.is_instance_of::<PyString>()
            || value.is_instance_of::<PyInt>()
            || value
                .cast::<PyFloat>()
                .is_ok_and(|number| number.value().is_finite()));
    if !valid {
        return Err(GeometryError::new_err(
            "feature id must be a string or finite number",
        ));
    }
    feature.set_item("id", value)
}

fn require_geojson_shape(shape: &Shape, crs: Option<&str>) -> PyResult<()> {
    require_geojson_crs(crs)?;
    if shape.has_m() {
        return Err(InvalidGeometryError::new_err(
            "GeoJSON has no M ordinate; use WKT/GeoArrow to preserve M",
        ));
    }
    Ok(())
}

/// Whether a Feature encoder should apply RFC 7946 geographic preparation
/// (domain check + antimeridian split). CRS-tagged WGS84-family frames opt in;
/// CRS-free geometry stays planar (same rule as `to_geojson`).
const fn feature_geographic(crs: Option<&str>) -> bool {
    crs.is_some()
}

fn feature_dict<'py>(
    py: Python<'py>,
    geometry: Option<&Shape>,
    crs: Option<&str>,
    properties: Option<&Bound<'py, PyAny>>,
    id: Option<&Bound<'py, PyAny>>,
    preserve_explicit_null: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let geometry = if let Some(shape) = geometry {
        // Share `to_geojson`'s RFC 7946 preparation (domain + seam split).
        Some(crate::boundary::convert::geojson_dict_prepared(
            py,
            shape,
            feature_geographic(crs),
        )?)
    } else {
        None
    };
    let properties = properties_value(py, properties, preserve_explicit_null)?;
    let feature = PyDict::new(py);
    feature.set_item("type", "Feature")?;
    if let Some(geometry) = geometry {
        feature.set_item("geometry", geometry)?;
    } else {
        feature.set_item("geometry", py.None())?;
    }
    feature.set_item("properties", properties)?;
    set_feature_id(&feature, id)?;
    Ok(feature)
}

/// Build a GeoJSON Feature mapping from a geometry and side data.
///
/// Parameters
/// ----------
/// geom : Geometry, optional
///     Geometry to encode. ``None`` emits a null GeoJSON geometry. A geometry
///     with a CRS must use EPSG:4326 longitude/latitude coordinates.
/// properties : Mapping[str, Any], optional
///     Feature properties. The mapping is copied and all keys must be strings.
/// id : str or finite number, optional
///     Feature identifier.
///
/// Returns
/// -------
/// GeoJsonFeature
///     A new ``{"type": "Feature", ...}`` mapping.
///
/// Raises
/// ------
/// TypeError
///     If ``geom`` is not a Geometry or ``None``.
/// GeometryError
///     If properties are not a string-keyed mapping, or the id is invalid.
/// InvalidGeometryError
///     If a WGS84-tagged geometry has coordinates outside the lon/lat domain.
/// CRSError
///     If a CRS-tagged geometry is not in the WGS84 lon/lat family.
///
/// Notes
/// -----
/// Coordinate epoch metadata is not representable in GeoJSON Feature mappings
/// and is **silently dropped** (same contract as ``to_geojson(..., drop_epoch=True)``).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> feature = gm.to_feature(gm.Point(1, 2), properties={"name": "A"})
/// >>> feature.get("properties")
/// {'name': 'A'}
#[pyfunction]
#[pyo3(signature = (geom, *, properties = None, id = None))]
pub(crate) fn to_feature(
    py: Python<'_>,
    geom: Option<&Bound<'_, PyAny>>,
    properties: Option<&Bound<'_, PyAny>>,
    id: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyDict>> {
    let geometry = match geom.filter(|value| !value.is_none()) {
        None => None,
        Some(value) => {
            Some(exact_geometry(value).ok_or_else(|| geometry_type_err("expected Geometry"))?)
        },
    };
    let crs = geometry.as_ref().and_then(|geometry| geometry.crs_str());
    if let Some(geometry) = geometry {
        require_geojson_shape(geometry.shape.shape(), geometry.crs_str())?;
    }
    Ok(feature_dict(
        py,
        geometry.map(|geometry| geometry.shape.shape()),
        crs,
        properties,
        id,
        false,
    )?
    .unbind())
}

fn aligned_rows<'py>(
    value: Option<&Bound<'py, PyAny>>,
    len: usize,
    name: &str,
) -> PyResult<Vec<Option<Bound<'py, PyAny>>>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(std::iter::repeat_n(None, len).collect());
    };
    if value.is_instance_of::<PyString>()
        || value.is_instance_of::<pyo3::types::PyBytes>()
        || value.is_instance_of::<pyo3::types::PyByteArray>()
    {
        return Err(GeometryError::new_err(format!(
            "{name} must be an aligned iterable, not a scalar string or bytes"
        )));
    }
    // Fixed-count alignment via keystone: fallible exact reserve + stop after
    // expected+1 so unbounded side-data iterators cannot grow forever.
    let rows = crate::collect_py_iter_exact(value, len, Ok, |got| {
        GeometryError::new_err(format!(
            "{name} length {} does not match values length {len}",
            if got > len {
                format!(">{len}")
            } else {
                got.to_string()
            }
        ))
    })?;
    Ok(rows.into_iter().map(Some).collect())
}

/// Build a GeoJSON FeatureCollection from geometries and aligned side data.
///
/// Parameters
/// ----------
/// values : Features, Geometry, None, GeometryArray, or iterable of Geometry or None
///     A `Features` record reuses its aligned geometries, properties, and ids.
///     Otherwise, one geometry or geometry rows to encode. CRS-tagged rows must
///     use EPSG:4326 longitude/latitude coordinates.
/// properties : Mapping or iterable of Mapping or None, optional
///     One mapping broadcasts to every geometry. An iterable supplies one
///     mapping or explicit ``None`` per row. Omit for independent empty mappings.
/// ids : iterable of str, finite number, or None, optional
///     One optional feature identifier per geometry.
///
/// Returns
/// -------
/// GeoJsonFeatureCollection
///     A new ``{"type": "FeatureCollection", "features": [...]}`` mapping.
///
/// Raises
/// ------
/// TypeError
///     If a geometry row is not a Geometry or ``None``.
/// GeometryError
///     If properties or ids are invalid or are not aligned with geometries.
/// CRSError
///     If a CRS-tagged geometry is not EPSG:4326 longitude/latitude.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> fc = gm.to_feature_collection(gm.GeometryArray([gm.Point(1, 2)]))
/// >>> fc["type"], len(fc["features"])
/// ('FeatureCollection', 1)
#[pyfunction]
#[pyo3(signature = (values, *, properties = None, ids = None))]
pub(crate) fn to_feature_collection(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    properties: Option<&Bound<'_, PyAny>>,
    ids: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyDict>> {
    if values.is_instance(crate::features_type(py)?)? {
        if properties.is_some() || ids.is_some() {
            return Err(PyTypeError::new_err(
                "properties and ids must be omitted when values is a Features record",
            ));
        }
        // Normalize Features once and call the private impl — do not re-enter
        // the public pyfunction.
        let feature_geometries = values.getattr("geometries")?;
        let feature_properties = values.getattr("properties")?;
        let feature_ids = values.getattr("ids")?;
        return to_feature_collection_impl(
            py,
            &feature_geometries,
            Some(&feature_properties),
            Some(&feature_ids),
        );
    }
    to_feature_collection_impl(py, values, properties, ids)
}

fn to_feature_collection_impl(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    properties: Option<&Bound<'_, PyAny>>,
    ids: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyDict>> {
    // F1: copy a broadcast mapping ONCE via the keystone, then shallow-copy
    // that retained dict per feature (identity isolation). Never re-enumerate
    // a one-shot keys() stream (which would silently yield empty properties).
    let broadcast_properties: Option<Bound<'_, PyDict>> = match properties {
        Some(value) if !value.is_none() => mapping_as_dict(value)?,
        _ => None,
    };
    let features = PyList::empty(py);
    if let Some(array) = exact_geometry_array(values) {
        require_geojson_crs(array.crs_str())?;
        if array.has_m() {
            return Err(InvalidGeometryError::new_err(
                "GeoJSON has no M ordinate; use WKT/GeoArrow to preserve M",
            ));
        }
        let array_crs = array.crs_str();
        let properties = if let Some(props) = &broadcast_properties {
            std::iter::repeat_with(|| Ok(Some(props.copy()?.into_any())))
                .take(array.storage().len())
                .collect::<PyResult<Vec<_>>>()?
        } else {
            aligned_rows(properties, array.storage().len(), "properties")?
        };
        let ids = aligned_rows(ids, array.storage().len(), "ids")?;
        for (((missing, shape), properties), id) in array
            .masked_shape_rows()
            .zip(properties.iter())
            .zip(ids.iter())
        {
            features.append(feature_dict(
                py,
                (!missing).then_some(shape.as_ref()),
                array_crs,
                properties.as_ref(),
                id.as_ref(),
                true,
            )?)?;
        }
    } else {
        let geometry_rows = if exact_geometry(values).is_some() || values.is_none() {
            vec![values.clone()]
        } else {
            crate::collect_py_iter(values, Ok)?
        };
        for geometry in &geometry_rows {
            if geometry.is_none() {
                continue;
            }
            let geometry =
                exact_geometry(geometry).ok_or_else(|| geometry_type_err("expected Geometry"))?;
            require_geojson_shape(geometry.shape.shape(), geometry.crs_str())?;
        }
        let properties = if let Some(props) = &broadcast_properties {
            std::iter::repeat_with(|| Ok(Some(props.copy()?.into_any())))
                .take(geometry_rows.len())
                .collect::<PyResult<Vec<_>>>()?
        } else {
            aligned_rows(properties, geometry_rows.len(), "properties")?
        };
        let ids = aligned_rows(ids, geometry_rows.len(), "ids")?;
        for ((geometry, properties), id) in
            geometry_rows.iter().zip(properties.iter()).zip(ids.iter())
        {
            let (shape, row_crs) = if geometry.is_none() {
                (None, None)
            } else {
                let geometry = exact_geometry(geometry)
                    .expect("geometry rows were validated before aligned side data");
                (Some(geometry.shape.shape()), geometry.crs_str())
            };
            features.append(feature_dict(
                py,
                shape,
                row_crs,
                properties.as_ref(),
                id.as_ref(),
                true,
            )?)?;
        }
    }
    let collection = PyDict::new(py);
    collection.set_item("type", "FeatureCollection")?;
    collection.set_item("features", features)?;
    Ok(collection.unbind())
}
