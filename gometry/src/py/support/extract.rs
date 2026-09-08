#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyByteArray, PyBytes};

use crate::geometry::{MOrdinate, ZOrdinate};
use crate::py::support::{
    Bound, Frame, GeoJsonDecodeContext, GeometryInput, InvalidGeometryError, Point, Polygon, Py,
    PyAny, PyAnyMethods as _, PyErr, PyGeometry, PyGeometryArray, PyResult, PyTypeError, Ring,
    Shape, Value, classify_input, coerce_geometry, coordinate_values_at_most,
    coordinate_values_exact, exact_geometry, is_mapping_like, is_py_bytes_or_bytearray,
    parse_geojson_geometry_value, push_ring_shapes, pyfunction,
};

pub(crate) enum CoercedCollectedGeometryItems {
    Items(Vec<PyGeometry>),
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

pub(crate) fn exact_backed_bytes(value: &Bound<'_, PyAny>) -> PyResult<Option<PyBackedBytes>> {
    if value.cast::<PyBytes>().is_ok() || value.cast::<PyByteArray>().is_ok() {
        return value.extract().map(Some).map_err(Into::into);
    }
    Ok(None)
}

impl CoercedCollectedGeometryItems {
    pub(crate) fn into_items(self) -> Vec<PyGeometry> {
        let Self::Items(items) = self;
        items
    }
}

/// One canonical boundary representation for APIs that accept a geometry,
/// ``GeometryArray``, or general iterable of geometry-like values.
///
/// Borrow native scalar/array inputs until an owning collection is actually
/// required; only the foreign/general iterable lane is coerced eagerly. This
/// keeps the pleasant scalar-or-collection API without making each aggregate,
/// index, or collection function rediscover the same three-way classification.
pub(crate) enum GeometryValues<'a> {
    One(&'a PyGeometry),
    Array(&'a PyGeometryArray),
    Collected(CoercedCollectedGeometryItems),
}

impl<'a> GeometryValues<'a> {
    pub(crate) fn parse(values: &'a Bound<'_, PyAny>) -> PyResult<Self> {
        match classify_input(values) {
            Some(GeometryInput::One(geometry)) => Ok(Self::One(geometry)),
            Some(GeometryInput::Many(array)) => Ok(Self::Array(array)),
            None => {
                let iter = values.try_iter()?;
                Ok(Self::Collected(CoercedCollectedGeometryItems::Items(
                    coerce_geometry_iter(
                        values.py(),
                        iter,
                        true,
                        crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
                    )?,
                )))
            },
        }
    }

    pub(crate) fn into_items(self) -> PyResult<Vec<PyGeometry>> {
        match self {
            Self::One(geometry) => Ok(vec![geometry.clone()]),
            Self::Array(array) if array.has_missing() => Err(missing_geometry_items_error()),
            Self::Array(array) => Ok(array.items()?.into_owned()),
            Self::Collected(items) => Ok(items.into_items()),
        }
    }
}

pub(crate) fn geometry_items(values: &Bound<'_, PyAny>) -> PyResult<Vec<PyGeometry>> {
    GeometryValues::parse(values)?.into_items()
}

pub(crate) fn missing_geometry_items_error() -> PyErr {
    crate::py::errors::GeometryError::new_err(
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
    if is_py_bytes_or_bytearray(item) {
        return Ok(OwnedGeometryItem::Wkb(
            if let Some(bytes) = exact_backed_bytes(item)? {
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
                    "Feature has null geometry (an unlocated feature); parse the FeatureCollection with from_geojson/from_features, where null geometries become missing rows",
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
        crate::try_push(&mut output, geometry)?;
    }
    Ok(output)
}

pub(crate) fn coerce_collected_geometry_items(
    items: &[Bound<'_, PyAny>],
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<CoercedCollectedGeometryItems> {
    let Some(first) = items.first() else {
        return Ok(CoercedCollectedGeometryItems::Items(Vec::new()));
    };
    if items.iter().any(|item| item.is_none()) {
        return Err(missing_geometry_items_error());
    }
    let py = first.py();
    let mut owned = crate::try_vec_with_capacity(items.len())?;
    for (row, item) in items.iter().enumerate() {
        match own_geometry_item(item, policy) {
            Ok(item) => crate::try_push(&mut owned, item)?,
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
        let mut output = Vec::new();
        for (row, item) in owned.into_iter().enumerate() {
            let geometry = parse_owned_geometry_item(item, policy).map_err(|error| (row, error))?;
            crate::try_push_vec(&mut output, geometry).map_err(|error| (row, error))?;
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
    Ok(CoercedCollectedGeometryItems::Items(parsed))
}

/// Component geometries of a multipart geometry.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values
///     The geometry, array, or iterable materialized as an array.
///
/// Returns
/// -------
/// GeometryArray or Groups
///     A scalar or raw iterable returns a flat `GeometryArray`; a
///     `GeometryArray` input returns one geometry group per source row.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> multi = gm.from_wkt('MULTIPOINT ((0 0), (1 1))')
/// >>> for part in gm.parts(multi):
/// ...     assert part is not None
/// ...     print(part.to_wkt())
/// POINT (0 0)
/// POINT (1 1)
#[pyfunction]
pub(crate) fn parts(py: pyo3::Python<'_>, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    match crate::GeometryValues::parse(geom)? {
        crate::GeometryValues::One(geometry) => PyGeometryArray::from_shapes(
            part_shapes(geometry.shape.shape()).map_err(pyo3::PyErr::from)?,
            geometry.frame.clone(),
        )?
        .into_py_any(py),
        crate::GeometryValues::Array(array) => array
            .flat_map_shapes_groups(py, part_shapes)
            .and_then(|groups| groups.into_py_any(py)),
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items();
            let frame = crate::Frame::resolve_items(
                &mut items,
                crate::FrameAdoption {
                    crs: None,
                    epoch: None,
                },
                "GeometryArray",
            )?;
            let array = PyGeometryArray::pack_or_mixed(items, frame)?;
            let mut parts = Vec::new();
            for row in array.present_shape_rows() {
                let (_, shape) = row.map_err(PyErr::from)?;
                for part in part_shapes(&shape).map_err(pyo3::PyErr::from)? {
                    crate::try_push_vec(&mut parts, part).map_err(pyo3::PyErr::from)?;
                }
            }
            PyGeometryArray::from_shapes(parts, array.frame)?.into_py_any(py)
        },
    }
}

fn part_shapes(shape: &Shape) -> crate::Result<Vec<Shape>> {
    let mut parts = Vec::new();
    for part in shape.parts() {
        crate::try_push_vec(&mut parts, part)?;
    }
    Ok(parts)
}

/// Return the rings (exterior + interiors) of a polygonal geometry.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values
///     The geometry, array, or iterable materialized as an array.
///
/// Returns
/// -------
/// GeometryArray or Groups
///     A scalar or raw iterable returns a flat `GeometryArray`; a
///     `GeometryArray` input returns one ring group per source row.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> hole = [(1, 1), (2, 1), (2, 2), (1, 2)]
/// >>> shell = [(0, 0), (4, 0), (4, 4), (0, 4)]
/// >>> donut = gm.Polygon(shell, holes=[hole])
/// >>> len(gm.rings(donut))
/// 2
#[pyfunction]
pub(crate) fn rings(py: pyo3::Python<'_>, geom: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    match crate::GeometryValues::parse(geom)? {
        crate::GeometryValues::One(geometry) => {
            let mut rings = Vec::new();
            push_ring_shapes(geometry.shape.shape(), &mut rings)?;
            PyGeometryArray::from_shapes(rings, geometry.frame.clone())?.into_py_any(py)
        },
        crate::GeometryValues::Array(array) => {
            validate_ring_shapes(array)?;
            array
                .flat_map_shapes_groups(py, ring_shapes)
                .and_then(|groups| groups.into_py_any(py))
        },
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items();
            let frame = crate::Frame::resolve_items(
                &mut items,
                crate::FrameAdoption {
                    crs: None,
                    epoch: None,
                },
                "GeometryArray",
            )?;
            let array = PyGeometryArray::pack_or_mixed(items, frame)?;
            let mut rings = Vec::new();
            for row in array.present_shape_rows() {
                let (_, shape) = row.map_err(PyErr::from)?;
                push_ring_shapes(&shape, &mut rings)?;
            }
            PyGeometryArray::from_shapes(rings, array.frame.clone())?.into_py_any(py)
        },
    }
}

fn validate_ring_shapes(array: &PyGeometryArray) -> PyResult<()> {
    for row in array.present_shape_rows() {
        let (_, shape) = row.map_err(PyErr::from)?;
        if !matches!(
            *shape,
            Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::Empty(
                    crate::geometry::GeometryKind::Polygon
                        | crate::geometry::GeometryKind::MultiPolygon,
                    _,
                )
        ) {
            return Err(crate::py::errors::geometry_type_err(
                "rings requires Polygon or MultiPolygon geometries",
            ));
        }
    }
    Ok(())
}

fn ring_shapes(shape: &Shape) -> crate::Result<Vec<Shape>> {
    let mut rings = Vec::new();
    let mut failure = None;
    shape.for_each_ring(|ring| {
        if failure.is_none()
            && let Err(error) = crate::try_push_vec(&mut rings, Shape::LineString(ring.as_line()))
        {
            failure = Some(error);
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(rings)
}

pub(crate) fn extract_points(
    value: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<Point>> {
    crate::py::numpy::reject_masked_array(value)?;
    let coordinates = crate::collect_py_iter(value, |item| {
        coordinate_values_at_most(item.py(), &item, "coordinate", 4, |got| {
            InvalidGeometryError::new_err(format!("coordinate has too many ordinates (got {got})"))
        })
    })?;
    let z = optional_coordinates(z, coordinates.len(), "z")?;
    let m = optional_coordinates(m, coordinates.len(), "m")?;
    let mut points = crate::try_vec_with_capacity(coordinates.len())?;
    for (idx, coordinate) in coordinates.into_iter().enumerate() {
        if coordinate.len() < 2 {
            return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
        }
        if coordinate.len() > 4 {
            return Err(InvalidGeometryError::new_err(
                "coordinate has too many ordinates",
            ));
        }
        if coordinate.len() > 2 && (z.is_some() || m.is_some()) {
            return Err(InvalidGeometryError::new_err(
                "inline Z/M coordinates cannot be combined with z/m arrays",
            ));
        }
        if coordinate.iter().any(|value| !value.is_finite()) {
            return Err(InvalidGeometryError::new_err("coordinates must be finite"));
        }
        crate::try_push(
            &mut points,
            Point::new_raw_axes(
                coordinate[0],
                coordinate[1],
                ZOrdinate(
                    z.as_ref()
                        .map(|values| values[idx])
                        .or_else(|| coordinate.get(2).copied()),
                ),
                MOrdinate(
                    m.as_ref()
                        .map(|values| values[idx])
                        .or_else(|| coordinate.get(3).copied()),
                ),
            ),
        )?;
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
    crate::py::numpy::reject_masked_array(value)?;
    let coordinate = coordinate_values_at_most(value.py(), value, "coordinate", 4, |got| {
        InvalidGeometryError::new_err(format!("coordinate has too many ordinates (got {got})"))
    })?;
    if coordinate.is_empty() {
        return Ok(None);
    }
    if coordinate.len() < 2 {
        return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
    }
    if coordinate.len() > 4 {
        return Err(InvalidGeometryError::new_err(
            "coordinate has too many ordinates",
        ));
    }
    if coordinate.iter().any(|value| !value.is_finite()) {
        return Err(InvalidGeometryError::new_err("coordinates must be finite"));
    }
    Ok(Some(Point::new_raw_axes(
        coordinate[0],
        coordinate[1],
        ZOrdinate(coordinate.get(2).copied()),
        MOrdinate(coordinate.get(3).copied()),
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

pub(crate) fn extract_lines(value: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<Point>>> {
    crate::collect_py_iter(value, |item| extract_points(&item, None, None))
}

pub(crate) fn extract_polygons(value: &Bound<'_, PyAny>) -> PyResult<Vec<Polygon>> {
    crate::collect_py_iter(value, |item| {
        if let Some(geometry) = exact_geometry(&item) {
            if let Shape::Polygon(polygon) = geometry.shape.shape() {
                return Ok(polygon.clone());
            }
            return Err(crate::py::errors::geometry_type_err(
                "multi_polygon geometry members must be Polygon geometries",
            ));
        }
        let rings = crate::collect_py_iter(&item, Ok)?;
        let Some(shell) = rings.first() else {
            return Err(InvalidGeometryError::new_err(
                "multi_polygon member requires a shell",
            ));
        };
        let mut holes = crate::try_vec_with_capacity(rings.len().saturating_sub(1))?;
        for ring in &rings[1..] {
            crate::try_push(&mut holes, Ring::closed(extract_points(ring, None, None)?)?)?;
        }
        Ok(Polygon::new(
            Ring::closed(extract_points(shell, None, None)?)?,
            holes,
        )?)
    })
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
    let values = coordinate_values_exact(value.py(), value, name, expected_len, |got| {
        InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        ))
    })?;
    if values.len() != expected_len {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates"
        )));
    }
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
