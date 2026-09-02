#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;

use crate::geometry::{MOrdinate, ZOrdinate};
use crate::py::support::{
    Bound, GeometryInput, InvalidGeometryError, Point, Polygon, Py, PyAny, PyAnyMethods as _,
    PyErr, PyGeometry, PyGeometryArray, PyResult, PyTypeError, Ring, Shape, classify_input,
    coerce_geometry, coordinate_values_at_most, coordinate_values_exact, exact_geometry,
    is_mapping_like, is_py_bytes_or_bytearray, push_ring_shapes, pyfunction, with_one_byte_buffer,
};

pub(crate) enum CoercedCollectedGeometryItems {
    Items(Vec<PyGeometry>),
    FramelessShapes(Vec<Shape>),
}

impl CoercedCollectedGeometryItems {
    pub(crate) fn into_items(self) -> PyResult<Vec<PyGeometry>> {
        match self {
            Self::Items(items) => Ok(items),
            Self::FramelessShapes(shapes) => {
                let mut items = Vec::new();
                for shape in shapes {
                    crate::try_push(&mut items, PyGeometry::with_epoch(shape, None, None)?)?;
                }
                Ok(items)
            },
        }
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
                let items = crate::collect_py_iter(values, Ok)?;
                Ok(Self::Collected(coerce_collected_geometry_items(
                    &items,
                    true,
                    crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
                )?))
            },
        }
    }

    pub(crate) fn into_items(self) -> PyResult<Vec<PyGeometry>> {
        match self {
            Self::One(geometry) => Ok(vec![geometry.clone()]),
            Self::Array(array) if array.has_missing() => Err(missing_geometry_items_error()),
            Self::Array(array) => Ok(array.items()?.into_owned()),
            Self::Collected(items) => items.into_items(),
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

fn coerce_generic_geometry_items(
    items: &[Bound<'_, PyAny>],
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<Vec<PyGeometry>> {
    crate::try_collect_results(items.iter().enumerate().map(|(row, item)| {
        coerce_geometry(item, policy).map_err(|error| {
            if note_rows {
                crate::note_array_row(error, row)
            } else {
                error
            }
        })
    }))
}

/// Pure-bytes GeometryArray ingest: parse WKB/EWKB rows with one SRID
/// canonicalize per distinct code (same cache as bulk `from_wkb`).
fn coerce_collected_wkb_items(
    items: &[Bound<'_, PyAny>],
    note_rows: bool,
) -> PyResult<CoercedCollectedGeometryItems> {
    let mut rows = crate::try_vec_with_capacity(items.len())?;
    let mut saw_embedded = false;
    let mut srid_cache = crate::io::SridCrsCache::default();
    for (row, item) in items.iter().enumerate() {
        let note = |err: PyErr| {
            if note_rows {
                crate::note_array_row(err, row)
            } else {
                err
            }
        };
        let parsed = parse_wkb_payload(item).map_err(note)?;
        let embedded = srid_cache
            .resolve(parsed.srid)
            .map_err(|err| note(err.into()))?;
        saw_embedded |= embedded.is_some();
        crate::try_push(&mut rows, (parsed.shape, embedded.map(crate::crs_arc)))?;
    }
    if !saw_embedded {
        return Ok(CoercedCollectedGeometryItems::FramelessShapes(
            crate::try_collect_results(rows.into_iter().map(|(shape, _)| Ok(shape)))?,
        ));
    }
    Ok(CoercedCollectedGeometryItems::Items(
        crate::try_collect_results(
            rows.into_iter()
                .map(|(shape, crs)| PyGeometry::with_epoch(shape, crs, None).map_err(Into::into)),
        )?,
    ))
}

pub(crate) fn coerce_collected_geometry_items(
    items: &[Bound<'_, PyAny>],
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<CoercedCollectedGeometryItems> {
    // Dispatch pure-batch lanes from the first non-None item so an all-bytes
    // batch never runs the doomed geometry pass (and its speculative Vec).
    // Mixed batches still fall through to the geo-interface / coerce lanes.
    let first = items.iter().find(|item| !item.is_none());
    match first {
        None if items.is_empty() => {
            return Ok(CoercedCollectedGeometryItems::Items(Vec::new()));
        },
        None => {
            // Every item is None — same missing-geometry error as a pure-geometry
            // pass that collected zero present rows.
            return Err(missing_geometry_items_error());
        },
        Some(first) if exact_geometry(first).is_some() => {
            let mut native_items = crate::try_vec_with_capacity(items.len())?;
            let mut missing_items = 0_usize;
            for item in items {
                if item.is_none() {
                    missing_items += 1;
                    continue;
                }
                let Some(geometry) = exact_geometry(item) else {
                    native_items.clear();
                    break;
                };
                crate::try_push(&mut native_items, geometry.clone())?;
            }
            if missing_items > 0
                && native_items.len().checked_add(missing_items) == Some(items.len())
            {
                return Err(missing_geometry_items_error());
            }
            if native_items.len() == items.len() {
                return Ok(CoercedCollectedGeometryItems::Items(native_items));
            }
            // Mixed geometry + other kinds: fall through below.
        },
        Some(first) if is_py_bytes_or_bytearray(first) => {
            // Pure-bytes candidate: skip the geometry pass entirely.
        },
        Some(_) => {
            // Neither pure-geometry nor pure-bytes lead item: skip both pure
            // speculative passes and fall through to geo-interface / coerce.
        },
    }

    if items.iter().all(is_py_bytes_or_bytearray) {
        return coerce_collected_wkb_items(items, note_rows);
    }
    let mut all_mapping_or_interface = true;
    for item in items {
        if exact_geometry(item).is_some() {
            all_mapping_or_interface = false;
            break;
        }
        // Propagate protocol errors from mapping / attribute probes — never
        // swallow into a false negative that demotes the pure-mapping lane.
        let mapping = is_mapping_like(item)?;
        let interface = item
            .getattr_opt(pyo3::intern!(item.py(), "__geo_interface__"))?
            .is_some();
        if !(mapping || interface) {
            all_mapping_or_interface = false;
            break;
        }
    }
    if all_mapping_or_interface {
        // Pure Mapping / __geo_interface__ batch: one shared GeoJSON coercer
        // with the caller's legacy-CRS policy (Adopt for GeometryArray /
        // require; Fixed for document decoders that pass through elsewhere).
        return Ok(CoercedCollectedGeometryItems::Items(
            crate::try_collect_results(items.iter().enumerate().map(|(row, item)| {
                coerce_geometry(item, policy).map_err(|err| {
                    if note_rows {
                        crate::note_array_row(err, row)
                    } else {
                        err
                    }
                })
            }))?,
        ));
    }
    Ok(CoercedCollectedGeometryItems::Items(
        coerce_generic_geometry_items(items, note_rows, policy)?,
    ))
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
        )
        .into_py_any(py),
        crate::GeometryValues::Array(array) => array
            .flat_map_shapes_groups(py, part_shapes)
            .and_then(|groups| groups.into_py_any(py)),
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items()?;
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
            for (_, shape) in array.present_shape_rows() {
                for part in part_shapes(&shape).map_err(pyo3::PyErr::from)? {
                    crate::try_push_vec(&mut parts, part).map_err(pyo3::PyErr::from)?;
                }
            }
            PyGeometryArray::from_shapes(parts, array.frame.clone()).into_py_any(py)
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
            PyGeometryArray::from_shapes(rings, geometry.frame.clone()).into_py_any(py)
        },
        crate::GeometryValues::Array(array) => {
            validate_ring_shapes(array)?;
            array
                .flat_map_shapes_groups(py, ring_shapes)
                .and_then(|groups| groups.into_py_any(py))
        },
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items()?;
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
            for (_, shape) in array.present_shape_rows() {
                push_ring_shapes(&shape, &mut rings)?;
            }
            PyGeometryArray::from_shapes(rings, array.frame.clone()).into_py_any(py)
        },
    }
}

fn validate_ring_shapes(array: &PyGeometryArray) -> PyResult<()> {
    for (_, shape) in array.present_shape_rows() {
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
        if failure.is_none() {
            if let Err(error) = crate::try_push_vec(
                &mut rings,
                Shape::LineString(
                    crate::geometry::LineSeq::try_new(ring.clone())
                        .expect("ring has at least two vertices"),
                ),
            ) {
                failure = Some(error);
            }
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
        ))
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

pub(crate) fn parse_wkb_payload(value: &Bound<'_, PyAny>) -> PyResult<crate::io::WkbGeometry> {
    parse_wkb_payload_bytes(value, |wkb| Ok(crate::io::parse_wkb(wkb)?))
}

pub(crate) fn parse_wkb_payload_bytes<T>(
    value: &Bound<'_, PyAny>,
    parse: impl FnOnce(&[u8]) -> PyResult<T>,
) -> PyResult<T> {
    // One shared extractor: signed and unsigned one-byte buffers (bytes,
    // bytearray, memoryview, array.array('B'|'b'), …).
    with_one_byte_buffer(value, parse).map_err(|err| {
        if err.is_instance_of::<PyTypeError>(value.py()) {
            PyTypeError::new_err("expected WKB bytes or iterable of WKB bytes")
        } else {
            err
        }
    })
}
