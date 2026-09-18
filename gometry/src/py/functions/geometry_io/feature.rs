use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::py::errors;
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

fn feature_dict<'py>(
    py: Python<'py>,
    geometry: Option<&Bound<'py, PyAny>>,
    properties: Option<&Bound<'py, PyAny>>,
    id: Option<&Bound<'py, PyAny>>,
    preserve_explicit_null: bool,
) -> PyResult<Bound<'py, PyDict>> {
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

fn geometry_from_json_text(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    crate::boundary::json_text_to_py(py, text)
}

fn serialize_feature_shapes(
    py: Python<'_>,
    shapes: Vec<Option<(Shape, Option<String>)>>,
) -> PyResult<Vec<Option<String>>> {
    py.detach(move || {
        shapes
            .into_iter()
            .map(|shape| {
                shape
                    .map(|(shape, crs)| {
                        let domain = crate::io::geojson_domain(crs.as_deref())?;
                        crate::io::to_geojson_string::<true>(&shape, domain)
                    })
                    .transpose()
            })
            .collect::<crate::error::Result<Vec<_>>>()
    })
    .map_err(PyErr::from)
}

fn serialize_array_feature_shapes(
    py: Python<'_>,
    array: &crate::PyGeometryArray,
    crs: Option<String>,
) -> PyResult<Vec<Option<String>>> {
    let array = array.clone();
    py.detach(move || {
        let domain = crate::io::geojson_domain(crs.as_deref())?;
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, array.len())?;
        for row in array.row_operation().rows_with_missing(|| ()) {
            let text = match row {
                crate::array::ArrayRow::Missing { .. } => None,
                crate::array::ArrayRow::Present(row) => {
                    Some(row.row().try_with_shape(|shape| {
                        crate::io::to_geojson_string::<true>(shape, domain)
                    })?)
                },
            };
            crate::boundary::try_push_vec(&mut output, text)?;
        }
        Ok::<_, crate::error::Error>(output)
    })
    .map_err(PyErr::from)
}

fn owned_rows(rows: Vec<Option<Bound<'_, PyAny>>>) -> Vec<Option<Py<PyAny>>> {
    rows.into_iter().map(|row| row.map(Bound::unbind)).collect()
}

fn side_rows<'py>(
    value: Option<&Bound<'py, PyAny>>,
    len: usize,
    name: &str,
    broadcast: Option<&Bound<'py, PyDict>>,
) -> PyResult<Vec<Option<Py<PyAny>>>> {
    if value.is_none_or(|value| value.is_none()) {
        let mut rows = crate::try_vec_with_capacity(len)?;
        for _ in 0..len {
            rows.push(None);
        }
        return Ok(rows);
    }
    if let Some(broadcast) = broadcast {
        let mut rows = crate::try_vec_with_capacity(len)?;
        for _ in 0..len {
            rows.push(Some(broadcast.copy()?.into_any().unbind()));
        }
        return Ok(rows);
    }
    Ok(owned_rows(aligned_rows(value, len, name)?))
}

fn materialize_feature<'py>(
    py: Python<'py>,
    geometry: Option<&Py<PyAny>>,
    properties: Option<&Py<PyAny>>,
    id: Option<&Py<PyAny>>,
    preserve_explicit_null: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let geometry = geometry.map(|text| text.bind(py));
    let properties = properties.map(|value| value.bind(py));
    let id = id.map(|value| value.bind(py));
    feature_dict(py, geometry, properties, id, preserve_explicit_null)
}

enum FeatureGeometryRows {
    Array(crate::PyGeometryArray),
    Shapes(Vec<Option<(Shape, Option<String>)>>),
}

/// Build a GeoJSON Feature mapping from a geometry and side data.
///
/// Parameters
/// ----------
/// geom : Geometry, optional
///     Geometry to encode. ``None`` emits a null GeoJSON geometry. A geometry
///     with a CRS must use WGS84 longitude/latitude coordinates. A CRS-free
///     geometry is explicitly assumed to already use those coordinates too.
/// properties : Mapping[str, Any], optional
///     Feature properties. The mapping is copied and all keys must be strings.
/// id : str or finite number, optional
///     Feature identifier.
/// drop_epoch : bool, default False
///     GeoJSON Feature mappings cannot encode coordinate epoch metadata. If
///     ``False``, an epoch-bearing geometry raises ``GeometryError``; pass
///     ``True`` to acknowledge and explicitly lose the epoch metadata.
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
///     If properties are not a string-keyed mapping, the id is invalid, or
///     coordinate epoch metadata is present without ``drop_epoch=True``.
/// InvalidGeometryError
///     If the geometry has an M ordinate or coordinates outside the WGS84
///     longitude/latitude domain. GeoJSON Feature mappings have no
///     representation for M; use WKT/GeoArrow to preserve it.
/// CRSError
///     If a CRS-tagged geometry is not in the WGS84 lon/lat family.
///
/// Notes
/// -----
/// Coordinate epoch metadata is not representable in GeoJSON Feature mappings.
/// It is rejected unless ``drop_epoch=True``; that option explicitly
/// acknowledges and loses the epoch metadata.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> feature = gm.to_feature(gm.Point(1, 2), properties={"name": "A"})
/// >>> feature.get("properties")
/// {'name': 'A'}
#[pyfunction]
#[pyo3(signature = (geom, *, properties = None, id = None, drop_epoch = false))]
pub(crate) fn to_feature(
    py: Python<'_>,
    geom: Option<&Bound<'_, PyAny>>,
    properties: Option<&Bound<'_, PyAny>>,
    id: Option<&Bound<'_, PyAny>>,
    drop_epoch: bool,
) -> PyResult<Py<PyDict>> {
    let geometry = match geom.filter(|value| !value.is_none()) {
        None => None,
        Some(value) => {
            Some(exact_geometry(value).ok_or_else(|| geometry_type_err("expected Geometry"))?)
        },
    };
    let crs = geometry
        .as_ref()
        .and_then(|geometry| geometry.crs_str())
        .map(str::to_owned);
    let geometry = if let Some(geometry) = geometry {
        errors::require_epoch_drop(geometry.epoch(), drop_epoch, "to_feature")?;
        require_geojson_shape(geometry.shape.shape(), geometry.crs_str())?;
        let shape = geometry.shape.shape().clone();
        Some(
            py.detach(move || {
                let domain = crate::io::geojson_domain(crs.as_deref())?;
                crate::io::to_geojson_string::<true>(&shape, domain)
            })
            .map_err(PyErr::from)?,
        )
    } else {
        None
    };
    let geometry = geometry
        .as_deref()
        .map(|text| geometry_from_json_text(py, text))
        .transpose()?;
    let geometry = geometry.as_ref().map(|text| text.bind(py));
    Ok(feature_dict(py, geometry, properties, id, false)?.unbind())
}

fn aligned_rows<'py>(
    value: Option<&Bound<'py, PyAny>>,
    len: usize,
    name: &str,
) -> PyResult<Vec<Option<Bound<'py, PyAny>>>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        let mut rows = crate::try_vec_with_capacity(len)?;
        for _ in 0..len {
            rows.push(None);
        }
        return Ok(rows);
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
    let mut aligned = crate::try_vec_with_capacity(rows.len())?;
    for row in rows {
        aligned.push(Some(row));
    }
    Ok(aligned)
}

/// Build a GeoJSON FeatureCollection from geometries and aligned side data.
///
/// Parameters
/// ----------
/// values : Features, Geometry, None, GeometryArray, or iterable of Geometry or None
///     A `Features` record reuses its aligned geometries, properties, and ids.
///     Features already owns aligned properties and ids, so properties and ids
///     must be omitted; supplying them raises ``TypeError``.
///     Otherwise, one geometry or geometry rows to encode. CRS-tagged rows must
///     use WGS84 longitude/latitude coordinates; CRS-free rows are explicitly
///     assumed to already use those coordinates.
/// properties : Mapping or iterable of Mapping or None, optional
///     One mapping broadcasts to every geometry. An iterable supplies one
///     mapping or explicit ``None`` per row. Omit for independent empty mappings.
/// ids : iterable of str, finite number, or None, optional
///     One optional feature identifier per geometry.
/// drop_epoch : bool, default False
///     GeoJSON Feature mappings cannot encode coordinate epoch metadata. If
///     ``False``, an epoch-bearing geometry raises ``GeometryError``; pass
///     ``True`` to acknowledge and explicitly lose the epoch metadata.
///
/// Returns
/// -------
/// GeoJsonFeatureCollection
///     A new ``{"type": "FeatureCollection", "features": [...]}`` mapping.
///
/// Raises
/// ------
/// TypeError
///     If a geometry row is not a Geometry or ``None``, or if properties or ids
///     are supplied when values is a Features record.
/// GeometryError
///     If properties or ids are invalid or are not aligned with geometries, or
///     coordinate epoch metadata is present without ``drop_epoch=True``.
/// InvalidGeometryError
///     If a geometry has an M ordinate or coordinates outside the WGS84
///     longitude/latitude domain. GeoJSON Feature mappings have no
///     representation for M; use WKT/GeoArrow to preserve it.
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
#[pyo3(signature = (values, *, properties = None, ids = None, drop_epoch = false))]
pub(crate) fn to_feature_collection(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    properties: Option<&Bound<'_, PyAny>>,
    ids: Option<&Bound<'_, PyAny>>,
    drop_epoch: bool,
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
            drop_epoch,
        );
    }
    to_feature_collection_impl(py, values, properties, ids, drop_epoch)
}

fn to_feature_collection_impl(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    properties: Option<&Bound<'_, PyAny>>,
    ids: Option<&Bound<'_, PyAny>>,
    drop_epoch: bool,
) -> PyResult<Py<PyDict>> {
    // F1: copy a broadcast mapping ONCE via the keystone, then shallow-copy
    // that retained dict per feature (identity isolation). Never re-enumerate
    // a one-shot keys() stream (which would silently yield empty properties).
    let broadcast_properties: Option<Bound<'_, PyDict>> = match properties {
        Some(value) if !value.is_none() => mapping_as_dict(value)?,
        _ => None,
    };
    let (geometry_rows, row_count) = if let Some(array) = exact_geometry_array(values) {
        errors::require_epoch_drop(array.epoch(), drop_epoch, "to_feature_collection")?;
        require_geojson_crs(array.crs_str())?;
        if array.has_m() {
            return Err(InvalidGeometryError::new_err(
                "GeoJSON has no M ordinate; use WKT/GeoArrow to preserve M",
            ));
        }
        (FeatureGeometryRows::Array(array.clone()), array.len())
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
            errors::require_epoch_drop(geometry.epoch(), drop_epoch, "to_feature_collection")?;
            require_geojson_shape(geometry.shape.shape(), geometry.crs_str())?;
        }
        let mut shapes = crate::try_vec_with_capacity(geometry_rows.len())?;
        for geometry in geometry_rows {
            let row = if geometry.is_none() {
                None
            } else {
                let geometry = exact_geometry(&geometry)
                    .expect("geometry rows were validated before detached serialization");
                Some((
                    geometry.shape.shape().clone(),
                    geometry.crs_str().map(str::to_owned),
                ))
            };
            crate::boundary::try_push_vec(&mut shapes, row)?;
        }
        let row_count = shapes.len();
        (FeatureGeometryRows::Shapes(shapes), row_count)
    };

    let properties = side_rows(
        properties,
        row_count,
        "properties",
        broadcast_properties.as_ref(),
    )?;
    let ids = side_rows(ids, row_count, "ids", None)?;
    let geometry_texts = match geometry_rows {
        FeatureGeometryRows::Array(array) => {
            serialize_array_feature_shapes(py, &array, array.crs_str().map(str::to_owned))?
        },
        FeatureGeometryRows::Shapes(shapes) => serialize_feature_shapes(py, shapes)?,
    };
    let features = PyList::empty(py);
    for index in 0..row_count {
        let geometry = geometry_texts[index]
            .as_deref()
            .map(|text| geometry_from_json_text(py, text))
            .transpose()?;
        let feature = materialize_feature(
            py,
            geometry.as_ref(),
            properties[index].as_ref(),
            ids[index].as_ref(),
            true,
        )?;
        features.append(feature)?;
    }
    let collection = PyDict::new(py);
    collection.set_item("type", "FeatureCollection")?;
    collection.set_item("features", features)?;
    Ok(collection.unbind())
}
