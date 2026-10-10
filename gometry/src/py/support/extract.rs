use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyBytes;

use crate::boundary::FloatInput;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{MOrdinate, Point, Shape, ZOrdinate};
use crate::py::errors;
use crate::py::errors::GeometryError;
use crate::py::numpy::reject_masked_array;
use crate::py::support::{
    Bound, Frame, GeoJsonDecodeContext, GeometryInput, InvalidGeometryError, Py, PyAny,
    PyAnyMethods as _, PyErr, PyGeometry, PyGeometryArray, PyResult, PyTypeError, Python, Value,
    classify_input, coerce_geometry, exact_geometry, float_values_at_most, float_values_exact,
    is_mapping_like, is_py_bytes_or_bytearray, note_array_row, parse_geojson_geometry_value,
};

pub(crate) fn point_xy(shape: &Shape) -> PyResult<(f64, f64)> {
    let Shape::Point(point) = shape else {
        return Err(errors::geometry_type_err("expected Point geometry"));
    };
    Ok((point.x, point.y))
}

pub(crate) enum OwnedBytes {
    Backed(PyBackedBytes),
    Copied(Vec<u8>),
}

impl OwnedBytes {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Backed(bytes) => bytes.as_ref(),
            Self::Copied(bytes) => bytes,
        }
    }
}

pub(crate) fn exact_backed_bytes(value: &Bound<'_, PyAny>) -> Option<PyBackedBytes> {
    value
        .cast::<PyBytes>()
        .ok()
        .map(|bytes| bytes.clone().into())
}

/// One canonical boundary representation for APIs that reduce a geometry,
/// ``GeometryArray``, or general iterable to present geometry items.
///
/// Borrow native scalar/array inputs until an owning collection is actually
/// required; only the foreign/general iterable lane is coerced eagerly. This
/// keeps aggregate and index-like operations from rediscovering the same
/// three-way classification.
enum GeometryValues<'a> {
    One(&'a PyGeometry),
    Array(&'a PyGeometryArray),
    Collected(Vec<PyGeometry>),
}

impl<'a> GeometryValues<'a> {
    fn parse(values: &'a Bound<'_, PyAny>) -> PyResult<Self> {
        match classify_input(values) {
            Some(GeometryInput::One(geometry)) => Ok(Self::One(geometry)),
            Some(GeometryInput::Many(array)) => Ok(Self::Array(array)),
            None => {
                let iter = values.try_iter()?;
                Ok(Self::Collected(coerce_geometry_iter(
                    values.py(),
                    iter,
                    true,
                    crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
                )?))
            },
        }
    }

    fn into_items(self) -> PyResult<Vec<PyGeometry>> {
        match self {
            Self::One(geometry) => Ok(vec![geometry.clone()]),
            Self::Array(array) if array.has_missing() => Err(missing_geometry_items_error()),
            Self::Array(array) => array.items(),
            Self::Collected(items) => Ok(items),
        }
    }
}

pub(crate) fn geometry_items(values: &Bound<'_, PyAny>) -> PyResult<Vec<PyGeometry>> {
    GeometryValues::parse(values)?.into_items()
}

/// Normalize a scalar, native array, or iterable of native geometry rows to
/// the representation used by pairwise operations. Multipart geometries are
/// classified before iteration because they implement Python iteration too.
pub(crate) fn normalize_geometry_batch<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if classify_input(value).is_some() {
        return Ok(value.clone());
    }
    let capacity = value
        .get_type()
        .getattr_opt(pyo3::intern!(py, "__len__"))?
        .map(|_| value.len())
        .transpose()?
        .unwrap_or_default();
    let iterator = value
        .try_iter()
        .map_err(|_| crate::broadcast::expected_geometry_or_array_for(value))?;
    let mut rows = Vec::with_capacity(capacity);
    for (logical_row, item) in iterator.enumerate() {
        let item = item?;
        if item.is_none() {
            rows.push(None);
            continue;
        }
        let Some(geometry) = exact_geometry(&item) else {
            return Err(note_array_row(
                PyTypeError::new_err("iterable rows must be Geometry or None"),
                logical_row,
            ));
        };
        rows.push(Some(geometry.clone()));
    }

    let first = rows.iter().flatten().next();
    let frame = first.map_or(Frame::None, |geometry| geometry.frame.clone());
    if let Some(first) = first {
        for (logical_row, geometry) in rows
            .iter()
            .enumerate()
            .filter_map(|(row, value)| value.as_ref().map(|geometry| (row, geometry)))
        {
            Frame::compatible_parts(
                first.crs_ref(),
                first.epoch(),
                geometry.crs_ref(),
                geometry.epoch(),
                "GeometryArray",
            )
            .map_err(PyErr::from)
            .map_err(|error| note_array_row(error, logical_row))?;
        }
    }

    let fallback = rows.iter().flatten().next().map_or_else(
        || Shape::Point(Point::new(0.0, 0.0).expect("zero coordinates are finite")),
        |geometry| geometry.shape.shape().clone(),
    );
    let mut missing = Vec::with_capacity(rows.len());
    let mut shapes = Vec::with_capacity(rows.len());
    for geometry in rows {
        missing.push(geometry.is_none());
        shapes.push(geometry.map_or_else(
            || fallback.clone(),
            |geometry| geometry.shape.shape().clone(),
        ));
    }
    let row_count = shapes.len();
    let array = PyGeometryArray::from_shapes(shapes, frame)?
        .with_missing_mask(ValidityMask::from_vec(row_count, missing))?;
    Ok(Py::new(py, array)?.into_any().bind(py).clone())
}

pub(crate) fn missing_geometry_items_error() -> PyErr {
    GeometryError::new_err(
        "values contains missing geometries; call drop_missing() or \
         fill_missing(...) first (aggregates and spatial indexes skip \
         them, but collections need every row present)",
    )
}

pub(crate) enum OwnedGeometryItem {
    Native(PyGeometry),
    Wkb(OwnedBytes),
    GeoJson(Value),
}

pub(crate) fn own_geometry_item(
    item: &Bound<'_, PyAny>,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<OwnedGeometryItem> {
    if let Some(geometry) = exact_geometry(item) {
        return Ok(OwnedGeometryItem::Native(geometry.clone()));
    }
    if is_py_bytes_or_bytearray(item)? {
        return Ok(OwnedGeometryItem::Wkb(
            if let Some(bytes) = exact_backed_bytes(item) {
                OwnedBytes::Backed(bytes)
            } else {
                OwnedBytes::Copied(copy_wkb_payload(item)?)
            },
        ));
    }
    if is_mapping_like(item)?
        || item
            .getattr_opt(pyo3::intern!(item.py(), "__geo_interface__"))?
            .is_some()
    {
        return Ok(OwnedGeometryItem::GeoJson(parse_geojson_geometry_value(
            item,
        )?));
    }
    // Preserve the existing type error for strings and unsupported values.
    coerce_geometry(item, policy).map(OwnedGeometryItem::Native)
}

pub(crate) fn parse_owned_geometry_item(
    item: OwnedGeometryItem,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> crate::error::Result<PyGeometry> {
    match item {
        OwnedGeometryItem::Native(geometry) => Ok(geometry),
        OwnedGeometryItem::Wkb(bytes) => {
            let parsed = crate::io::parse_wkb(bytes.as_slice())?;
            let crs = crate::io::crs_from_optional_srid(parsed.srid)?.map(crate::crs_arc);
            Ok(PyGeometry::with_frame(parsed.shape, Frame::new(crs, None)?))
        },
        OwnedGeometryItem::GeoJson(value) => {
            let Some((shape, frame)) = crate::coerce_geojson_geometry_value(
                &value,
                GeoJsonDecodeContext::GeometryLike,
                policy,
                None,
            )?
            else {
                return Err(crate::io::IoError::geojson(
                    "Feature has null geometry (an unlocated feature); parse the FeatureCollection with from_geojson, where null geometries become missing rows, or use from_geojson(..., features=True) to retain its properties and ID",
                ));
            };
            Ok(PyGeometry::with_frame(shape, frame))
        },
    }
}

fn coerce_geometry_iter<'py>(
    py: pyo3::Python<'py>,
    iter: Bound<'py, pyo3::types::PyIterator>,
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<Vec<PyGeometry>> {
    let mut output = Vec::new();
    for (row, item) in iter.enumerate() {
        let item = item?;
        crate::check_python_signals(py, row)?;
        if item.is_none() {
            return Err(missing_geometry_items_error());
        }
        let owned = match own_geometry_item(&item, policy) {
            Ok(owned) => owned,
            Err(error) => {
                return Err(if note_rows {
                    crate::note_array_row(error, row)
                } else {
                    error
                });
            },
        };
        let parsed = py.detach(move || parse_owned_geometry_item(owned, policy));
        let geometry = match parsed {
            Ok(geometry) => geometry,
            Err(error) => {
                return Err(if note_rows {
                    crate::note_array_row(error.into(), row)
                } else {
                    error.into()
                });
            },
        };
        output.push(geometry);
    }
    Ok(output)
}

pub(crate) fn coerce_present_geometry_items<'a, 'py>(
    items: impl Iterator<Item = &'a Bound<'py, PyAny>> + Clone,
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<Vec<PyGeometry>>
where
    'py: 'a,
{
    let Some(first) = items.clone().next() else {
        return Ok(Vec::new());
    };
    let py = first.py();
    // Callers replay collected objects, not Python providers, when counting
    // the present rows for this reservation.
    let mut owned = Vec::with_capacity(items.clone().count());
    for (row, item) in items.enumerate() {
        match own_geometry_item(item, policy) {
            Ok(item) => owned.push(item),
            Err(error) => {
                return Err(if note_rows {
                    crate::note_array_row(error, row)
                } else {
                    error
                });
            },
        }
    }
    let parsed = py.detach(move || {
        let mut output = Vec::with_capacity(owned.len());
        for (row, item) in owned.into_iter().enumerate() {
            let geometry = parse_owned_geometry_item(item, policy).map_err(|error| (row, error))?;
            output.push(geometry);
        }
        Ok::<_, (usize, crate::error::Error)>(output)
    });
    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err((row, error)) => {
            return Err(if note_rows {
                crate::note_array_row(error.into(), row)
            } else {
                error.into()
            });
        },
    };
    Ok(parsed)
}

pub(crate) fn extract_points(
    value: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<Point>> {
    reject_masked_array(value)?;
    let coordinates = crate::collect_py_iter(value, |item| {
        float_values_at_most(
            item.py(),
            &item,
            "coordinate",
            FloatInput::Coordinate,
            4,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "coordinate has too many ordinates (got {got})"
                ))
            },
        )
    })?;
    let z = optional_coordinates(z, coordinates.len(), "z")?;
    let m = optional_coordinates(m, coordinates.len(), "m")?;
    let mut points = Vec::with_capacity(coordinates.len());
    for (idx, coordinate) in coordinates.into_iter().enumerate() {
        let [x, y, rest @ ..] = coordinate.as_slice() else {
            return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
        };
        if !rest.is_empty() && (z.is_some() || m.is_some()) {
            return Err(InvalidGeometryError::new_err(
                "inline Z/M coordinates cannot be combined with z/m arrays",
            ));
        }
        if coordinate.iter().any(|value| !value.is_finite()) {
            return Err(InvalidGeometryError::new_err("coordinates must be finite"));
        }
        points.push(Point::new_raw_axes(
            *x,
            *y,
            ZOrdinate(
                z.as_ref()
                    .and_then(|values| values.get(idx).copied())
                    .or_else(|| rest.first().copied()),
            ),
            MOrdinate(
                m.as_ref()
                    .and_then(|values| values.get(idx).copied())
                    .or_else(|| rest.get(1).copied()),
            ),
        ));
    }
    ensure_homogeneous_axes(&points)?;
    Ok(points)
}

pub(crate) fn extract_coordinate(value: &Bound<'_, PyAny>) -> PyResult<Point> {
    extract_coordinate_or_empty(value)?
        .ok_or_else(|| InvalidGeometryError::new_err("coordinate requires x and y"))
}

/// Admit one raw coordinate once, retaining an empty member for multipart
/// constructors that use an explicit empty point.
pub(crate) fn extract_coordinate_or_empty(value: &Bound<'_, PyAny>) -> PyResult<Option<Point>> {
    reject_masked_array(value)?;
    let coordinate = float_values_at_most(
        value.py(),
        value,
        "coordinate",
        FloatInput::Coordinate,
        4,
        |got| {
            InvalidGeometryError::new_err(format!("coordinate has too many ordinates (got {got})"))
        },
    )?;
    if coordinate.is_empty() {
        return Ok(None);
    }
    let [x, y, rest @ ..] = coordinate.as_slice() else {
        return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
    };
    if coordinate.iter().any(|value| !value.is_finite()) {
        return Err(InvalidGeometryError::new_err("coordinates must be finite"));
    }
    Ok(Some(Point::new_raw_axes(
        *x,
        *y,
        ZOrdinate(rest.first().copied()),
        MOrdinate(rest.get(1).copied()),
    )))
}

/// Reject a vertex sequence whose points do not all share one coordinate-axis
/// layout (all XY, all XYZ, all XYM, or all XYZM). A mixed layout would make
/// `coordinate_axes` over-report and serialize missing ordinates as `NaN`.
pub(crate) fn ensure_homogeneous_axes(points: &[Point]) -> PyResult<()> {
    let Some(first) = points.first() else {
        return Ok(());
    };
    let (has_z, has_m) = (first.z().is_some(), first.m().is_some());
    if points
        .iter()
        .any(|point| point.z().is_some() != has_z || point.m().is_some() != has_m)
    {
        return Err(InvalidGeometryError::new_err(
            "all coordinates of a geometry must share one axis layout (XY, XYZ, XYM, or XYZM); mixed Z/M presence is not allowed",
        ));
    }
    Ok(())
}

pub(crate) fn optional_coordinates(
    value: Option<&Bound<'_, PyAny>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Vec<f64>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let values = float_values_exact(
        value.py(),
        value,
        name,
        FloatInput::Coordinate,
        expected_len,
        |got| {
            InvalidGeometryError::new_err(format!(
                "{name} must have the same length as x/y coordinates (got length {got})"
            ))
        },
    )?;
    Ok(Some(values))
}

pub(crate) fn copy_wkb_payload(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    crate::copy_one_byte_buffer(value).map_err(|error| {
        if error.is_instance_of::<PyTypeError>(value.py()) {
            PyTypeError::new_err("expected WKB bytes or iterable of WKB bytes")
        } else {
            error
        }
    })
}
