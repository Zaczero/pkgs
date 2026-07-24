#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::{LineSeq, MOrdinate, ShapePart, ZOrdinate};

pub(crate) enum CoercedCollectedGeometryItems {
    Items(Vec<PyGeometry>),
    FramelessShapes(Vec<Shape>),
}

impl CoercedCollectedGeometryItems {
    pub(crate) fn into_items(self) -> Vec<PyGeometry> {
        match self {
            Self::Items(items) => items,
            Self::FramelessShapes(shapes) => shapes
                .into_iter()
                .map(|shape| PyGeometry::with_epoch(shape, None, None))
                .collect(),
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
            Self::Array(array) => Ok(array.items().into_owned()),
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

fn coerce_generic_geometry_items(
    items: &[Bound<'_, PyAny>],
    note_rows: bool,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
) -> PyResult<Vec<PyGeometry>> {
    items
        .iter()
        .enumerate()
        .map(|(row, item)| {
            coerce_geometry(item, policy).map_err(|error| {
                if note_rows {
                    crate::note_array_row(error, row)
                } else {
                    error
                }
            })
        })
        .collect()
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
            let mut native_items = Vec::with_capacity(items.len());
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
                native_items.push(geometry.clone());
            }
            if missing_items > 0 && native_items.len() + missing_items == items.len() {
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
        let mut rows = Vec::with_capacity(items.len());
        let mut saw_embedded = false;
        for (row, item) in items.iter().enumerate() {
            let parsed = parse_wkb_payload(item).map_err(|err| {
                if note_rows {
                    crate::note_array_row(err, row)
                } else {
                    err
                }
            })?;
            saw_embedded |= parsed.crs.is_some();
            rows.push((parsed.shape, parsed.crs.map(crate::crs_arc)));
        }
        if !saw_embedded {
            return Ok(CoercedCollectedGeometryItems::FramelessShapes(
                rows.into_iter().map(|(shape, _)| shape).collect(),
            ));
        }
        return Ok(CoercedCollectedGeometryItems::Items(
            rows.into_iter()
                .map(|(shape, crs)| PyGeometry::with_epoch(shape, crs, None))
                .collect(),
        ));
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
            items
                .iter()
                .enumerate()
                .map(|(row, item)| {
                    coerce_geometry(item, policy).map_err(|err| {
                        if note_rows {
                            crate::note_array_row(err, row)
                        } else {
                            err
                        }
                    })
                })
                .collect::<PyResult<Vec<_>>>()?,
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
/// geom : Geometry or GeometryArray
///     The geometry (or array of geometries) to operate on.
///
/// Returns
/// -------
/// GeometryArray
///     The flattened component geometries, carrying the input CRS/epoch.
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
pub(crate) fn parts(geom: &Bound<'_, PyAny>) -> PyResult<PyGeometryArray> {
    crate::dispatch::dispatch_unary_simple_same(
        geom,
        |geometry| {
            Ok(PyGeometryArray::mixed(
                geometry
                    .shape
                    .parts()
                    .map(|shape| PyGeometry::with_frame(shape, geometry.frame.clone()))
                    .collect(),
                geometry.frame.clone(),
            ))
        },
        |array| {
            let mut parts = Vec::new();
            let frame = array.frame.clone();
            for (_, shape) in array.present_shape_rows() {
                shape.any_part(|part| {
                    let part = match part {
                        ShapePart::Point(point) => Shape::Point(point),
                        ShapePart::LineString(line) => Shape::LineString(
                            LineSeq::try_new(line.clone())
                                .expect("shape part line is empty or >=2 vertices"),
                        ),
                        ShapePart::Polygon(polygon) => Shape::Polygon(polygon.clone()),
                        ShapePart::Nested(nested) => nested.clone(),
                    };
                    parts.push(PyGeometry::with_frame(part, frame.clone()));
                    false
                });
            }
            Ok(PyGeometryArray::mixed(parts, frame))
        },
    )
}

/// Return the rings (exterior + interiors) of a polygonal geometry.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     The geometry (or array of geometries) to operate on.
///
/// Returns
/// -------
/// GeometryArray
///     The flattened rings as `LineString` geometries, carrying the input
///     CRS/epoch.
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
pub(crate) fn rings(geom: &Bound<'_, PyAny>) -> PyResult<PyGeometryArray> {
    crate::dispatch::dispatch_unary_simple_same(
        geom,
        |geometry| {
            let mut rings = Vec::new();
            push_rings(geometry, &mut rings)?;
            Ok(PyGeometryArray::mixed(rings, geometry.frame.clone()))
        },
        |array| {
            let mut rings = Vec::new();
            for (_, shape) in array.present_shape_rows() {
                let geometry = PyGeometry::with_frame(shape.into_owned(), array.frame.clone());
                push_rings(&geometry, &mut rings)?;
            }
            Ok(PyGeometryArray::mixed(rings, array.frame.clone()))
        },
    )
}

pub(crate) fn extract_points(
    value: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<Point>> {
    let coordinates = crate::collect_py_iter(value, |item| {
        coordinate_values(item.py(), &item, "coordinate")
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
            Point::new_axes(
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
            )?,
        )?;
    }
    ensure_homogeneous_axes(&points)?;
    Ok(points)
}

pub(crate) fn extract_coordinate(value: &Bound<'_, PyAny>) -> PyResult<Point> {
    let coordinate = coordinate_values(value.py(), value, "coordinate")?;
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
    Ok(Point::new_axes(
        coordinate[0],
        coordinate[1],
        ZOrdinate(coordinate.get(2).copied()),
        MOrdinate(coordinate.get(3).copied()),
    )?)
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
            return Err(PyTypeError::new_err(
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
    let values = coordinate_values(value.py(), value, name)?;
    if values.len() != expected_len {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates"
        )));
    }
    Ok(Some(values))
}
