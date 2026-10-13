use super::*;

/// The canonical `LINESTRING EMPTY`.
pub(super) const fn empty_line_string() -> Shape {
    Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XY)
}

/// The canonical `MULTIPOINT EMPTY` — an `XY` multipoint with no members.
pub(super) const fn empty_multi_point() -> Shape {
    Shape::typed_empty(GeometryKind::MultiPoint, CoordinateAxes::XY)
}

/// The canonical `MULTILINESTRING EMPTY` — a multilinestring with no members.
pub(super) const fn empty_multi_line_string() -> Shape {
    Shape::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XY)
}

/// The canonical `MULTIPOLYGON EMPTY` — a multipolygon with no members.
pub(super) const fn empty_multi_polygon() -> Shape {
    Shape::typed_empty(GeometryKind::MultiPolygon, CoordinateAxes::XY)
}

/// The canonical `GEOMETRYCOLLECTION EMPTY` — a collection with no members.
pub(super) const fn empty_geometry_collection() -> Shape {
    Shape::typed_empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
}

/// Build a ``Point`` geometry — ``XY``, ``XYZ``, ``XYM``, or ``XYZM``.
pub(crate) fn build_point(
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    let mut row_frame = Frame::None;
    let shape = match (x, y) {
        (Some(x), Some(y)) => {
            let x = finite_coordinate_required("x", x)?;
            let y = finite_coordinate_required("y", y)?;
            let z = z.map(|z| finite_coordinate_required("z", z)).transpose()?;
            let m = m.map(|m| finite_coordinate_required("m", m)).transpose()?;
            Shape::Point(Point::new_raw_axes(x, y, ZOrdinate(z), MOrdinate(m)))
        },
        (Some(value), None) => {
            let point = if let Some(geometry) = exact_geometry(value) {
                match geometry.shape.shape() {
                    Shape::Point(point) => {
                        row_frame = geometry.frame.clone();
                        *point
                    },
                    Shape::Empty(GeometryKind::Point, axes) => {
                        if z.is_some() || m.is_some() {
                            return Err(InvalidGeometryError::new_err(
                                "empty point takes no z/m; pass x and y for a non-empty point",
                            ));
                        }
                        row_frame = geometry.frame.clone();
                        return geometry_with_frame(
                            Shape::typed_empty(GeometryKind::Point, *axes),
                            row_frame,
                            crs,
                            epoch,
                        );
                    },
                    shape @ (Shape::MultiPoint(_)
                    | Shape::LineString(_)
                    | Shape::MultiLineString(_)
                    | Shape::Polygon(_)
                    | Shape::MultiPolygon(_)
                    | Shape::GeometryCollection(_)
                    | Shape::Empty(..)) => {
                        return Err(geometry_type_err(format!(
                            "Point coordinate input must be a Point, got {}",
                            shape.geometry_type()
                        )));
                    },
                }
            } else {
                extract_coordinate(value)?
            };
            let z = z.map(|z| finite_coordinate_required("z", z)).transpose()?;
            let m = m.map(|m| finite_coordinate_required("m", m)).transpose()?;
            Shape::Point(point_with_ordinates(point, z, m)?)
        },
        // No coordinates → `POINT EMPTY` (Shapely-compatible `Point()`); Z/M
        // are meaningless without X/Y.
        (None, None) if z.is_none() && m.is_none() => Shape::empty_point(),
        (None, None) => {
            return Err(InvalidGeometryError::new_err(
                "empty point takes no z/m; pass x and y for a non-empty point",
            ));
        },
        _ => {
            return Err(InvalidGeometryError::new_err(
                "point requires both x and y, or neither",
            ));
        },
    };
    if matches!(row_frame, Frame::None) {
        geometry_with_crs_epoch(shape, crs, epoch)
    } else {
        geometry_with_frame(shape, row_frame, crs, epoch)
    }
}

#[expect(
    clippy::float_cmp,
    reason = "embedded and supplied ordinates must be exactly equal; a tolerance would accept a different coordinate"
)]
fn point_with_ordinates(point: Point, z: Option<f64>, m: Option<f64>) -> PyResult<Point> {
    if z.is_some_and(|value| point.z().is_some_and(|embedded| embedded != value)) {
        return Err(InvalidGeometryError::new_err(
            "z= conflicts with the coordinate's embedded Z ordinate",
        ));
    }
    if m.is_some_and(|value| point.m().is_some_and(|embedded| embedded != value)) {
        return Err(InvalidGeometryError::new_err(
            "m= conflicts with the coordinate's embedded M ordinate",
        ));
    }
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(point.z().or(z)),
        MOrdinate(point.m().or(m)),
    ))
}

/// Build a ``LineString`` from an ordered coordinate sequence.
pub(crate) fn build_line_string(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No coordinates → `LINESTRING EMPTY` (Shapely-compatible `LineString()`).
    if coordinates.is_none() && x.is_none() && y.is_none() && z.is_none() && m.is_none() {
        return geometry_with_crs_epoch(empty_line_string(), crs, epoch);
    }
    let vertices = coordinate_vertices(coordinates, x, y, z, m)?;
    if vertices.is_empty() {
        return geometry_with_crs_epoch(
            Shape::typed_empty(GeometryKind::LineString, vertices.axes()),
            crs,
            epoch,
        );
    }
    geometry_with_crs_epoch(
        Shape::LineString(LineSeq::try_new(vertices).map_err(PyErr::from)?),
        crs,
        epoch,
    )
}

/// Resolve a line/multipoint's vertices from EITHER a coordinate sequence
/// (``[(x, y), …]`` / ``(N, 2)`` array) OR parallel ``x``/``y`` columns (with
/// optional ``z``/``m`` columns) — the columnar inverse of ``coords.x``/``.y``.
pub(super) fn coordinate_vertices(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<CoordSeq> {
    for value in [coordinates, x, y, z, m].into_iter().flatten() {
        numpy::reject_masked_array(value)?;
    }
    match (coordinates, x, y) {
        // Contiguous / strided N×D buffer → final SoA Arcs (no Vec<Point>).
        // Nested lists / iterators keep the extract_points path.
        (Some(coordinates), None, None) => {
            if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
                coordinates,
                z,
                m,
                crate::CoordSeqBufferMode::Geometry(crate::CoordSeqStructure::Sequence),
            )? {
                return Ok(buffer.coordinates);
            }
            let points = extract_points(coordinates, z, m)?;
            if points.is_empty() {
                let axes = if z.is_some() || m.is_some() {
                    CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some()))
                } else {
                    coordinates
                        .getattr_opt(pyo3::intern!(coordinates.py(), "shape"))?
                        .and_then(|shape| shape.extract::<Vec<usize>>().ok())
                        .and_then(|shape| match shape.as_slice() {
                            [0, 2] => Some(CoordinateAxes::XY),
                            [0, 3] => Some(CoordinateAxes::XYZ),
                            [0, 4] => Some(CoordinateAxes::XYZM),
                            _ => None,
                        })
                        .unwrap_or(CoordinateAxes::XY)
                };
                return Ok(CoordSeq::empty(axes));
            }
            Ok(CoordSeq::try_from_points(&points)?)
        },
        // Column input is already SoA — build the `CoordSeq` straight from the
        // `x`/`y`/`z`/`m` columns, with no `Vec<Point>` bounce.
        (None, Some(x), Some(y)) => {
            // Establish a sibling length before draining either bare column;
            // exact admission then stops an infinite paired source at the
            // first excess item.
            let x_hint = coordinate_sequence_len_hint(x, "x")?;
            let y_hint = coordinate_sequence_len_hint(y, "y")?;
            let z_hint = z
                .map(|value| coordinate_sequence_len_hint(value, "z"))
                .transpose()?
                .flatten();
            let m_hint = m
                .map(|value| coordinate_sequence_len_hint(value, "m"))
                .transpose()?
                .flatten();
            let established = x_hint.or(y_hint).or(z_hint).or(m_hint);
            let xs = if x_hint.is_none()
                && let Some(expected) = established
            {
                coordinate_column_values_exact(x.py(), x, "x", expected, |got| {
                    InvalidGeometryError::new_err(format!(
                        "x and y must have the same length, got {got} and {expected}"
                    ))
                })?
            } else {
                coordinate_column_values(x.py(), x, "x")?
            };
            let ys = if y_hint.is_none() && established.is_some() {
                let expected = xs.len();
                coordinate_column_values_exact(y.py(), y, "y", expected, |got| {
                    InvalidGeometryError::new_err(format!(
                        "x and y must have the same length, got {expected} and {got}"
                    ))
                })?
            } else {
                coordinate_column_values(y.py(), y, "y")?
            };
            if xs.len() != ys.len() {
                return Err(InvalidGeometryError::new_err(format!(
                    "x and y must have the same length, got {} and {}",
                    xs.len(),
                    ys.len(),
                )));
            }
            let expected = xs.len();
            let z = optional_coordinate_column_values(x.py(), z, expected, "z")?;
            let m = optional_coordinate_column_values(x.py(), m, expected, "m")?;
            Ok(CoordSeq::try_from_vecs(xs, ys, z, m)?)
        },
        (None, None, None) => Err(InvalidGeometryError::new_err(
            "provide a coordinate sequence or both x= and y= columns",
        )),
        (None, ..) => Err(InvalidGeometryError::new_err(
            "x= and y= columns must be provided together",
        )),
        (Some(_), ..) => Err(InvalidGeometryError::new_err(
            "provide either a coordinate sequence or x=/y= columns, not both",
        )),
    }
}

/// Build a ``Polygon`` from an exterior ring and optional holes.
pub(crate) fn build_polygon(
    shell: Option<&Bound<'_, PyAny>>,
    holes: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No shell (or an empty one) yields `POLYGON EMPTY` (Shapely-compatible
    // `Polygon()`). A bare empty shell with holes is contradictory.
    let shell_coords = match (shell, x, y) {
        (Some(shell), None, None) => coordinate_vertices(Some(shell), None, None, z, m)?,
        (None, Some(x), Some(y)) => coordinate_vertices(None, Some(x), Some(y), z, m)?,
        (None, None, None) => {
            let z = optional_coordinates(z, 0, "z")?;
            let m = optional_coordinates(m, 0, "m")?;
            CoordSeq::empty(CoordinateAxes::new(HasZ(z.is_some()), HasM(m.is_some())))
        },
        (None, ..) => {
            return Err(InvalidGeometryError::new_err(
                "x= and y= columns must be provided together",
            ));
        },
        (Some(_), ..) => {
            return Err(InvalidGeometryError::new_err(
                "provide either a shell coordinate sequence or x=/y= columns, not both",
            ));
        },
    };
    if shell_coords.is_empty() {
        // `holes` must still be a (possibly empty) iterable: a wrong-kind value
        // is a TypeError, never silently ignored.
        if let Some(holes) = holes {
            let mut holes = crate::py_iter_or_error(holes, || {
                PyTypeError::new_err("polygon holes must be an iterable")
            })?;
            match holes.next() {
                None => {},
                Some(Err(error)) => return Err(error),
                Some(Ok(_)) => {
                    return Err(InvalidGeometryError::new_err(
                        "an empty polygon cannot have holes",
                    ));
                },
            }
        }
        return geometry_with_crs_epoch(
            Shape::typed_empty(GeometryKind::Polygon, shell_coords.axes()),
            crs,
            epoch,
        );
    }
    let shell = Ring::closed_coordseq(shell_coords)?;
    // Rings must share one axes layout at construction (same rule as WKT/WKB
    // writers via `require_serializable_axes`) — never admit mixed XY/XYZ that
    // would invent Z/M on serialize. Promote with force_3d/with_coordinates(m=...) first.
    let holes = match holes {
        Some(value) => crate::collect_py_iter(value, |item| {
            Ok(Ring::closed_coordseq(coordinate_vertices(
                Some(&item),
                None,
                None,
                None,
                None,
            )?)?)
        })?,
        None => Vec::new(),
    };
    let shape = Shape::Polygon(Polygon::new(shell, holes));
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    geometry_with_crs_epoch(shape, crs, epoch)
}

pub(in crate::py::functions::constructors) fn build_box_shape(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    wrap: Option<&str>,
    crs: Option<&Crs>,
    ccw: bool,
) -> PyResult<Shape> {
    if miny > maxy {
        return Err(InvalidGeometryError::new_err(format!(
            "box miny ({miny}) must not exceed maxy ({maxy})"
        )));
    }
    let shape = match wrap {
        None => {
            if minx > maxx {
                return Err(InvalidGeometryError::new_err(format!(
                    "box minx ({minx}) must not exceed maxx ({maxx}) unless wrap='split'"
                )));
            }
            if let Some(crs) = crs
                && let Some(frame) = crs::info(crs).map_err(PyErr::from)?.geographic_frame()
                && let Ok(bounds) = Bounds::new_geographic(frame, minx, miny, maxx, maxy)
                && minx < maxx
                && miny < maxy
            {
                geographic_bounds_to_shape(bounds, frame).map_err(PyErr::from)
            } else {
                Ok(Shape::Polygon(box_polygon(minx, miny, maxx, maxy)?))
            }
        },
        Some("split") => {
            let Some(crs) = crs else {
                return Err(CRSError::new_err(
                    "box wrap='split' requires a geographic CRS",
                ));
            };
            let Some(frame) = crs::info(crs).map_err(PyErr::from)?.geographic_frame() else {
                return Err(CRSError::new_err(
                    "box wrap='split' requires a geographic CRS",
                ));
            };
            let bounds = if maxx - minx >= frame.longitude().period() {
                Bounds::full_geographic_longitude(frame, miny, maxy)?
            } else {
                Bounds::new_geographic(frame, minx, miny, maxx, maxy)?
            };
            if let Some([west, east]) = bounds.split_geographic(frame) {
                Ok(Shape::multi_polygon(
                    crate::geometry::CoordinateAxes::XY,
                    vec![
                        box_polygon(west.minx(), west.miny(), west.maxx(), west.maxy())?,
                        box_polygon(east.minx(), east.miny(), east.maxx(), east.maxy())?,
                    ],
                ))
            } else if maxx - minx >= frame.longitude().period() {
                geographic_bounds_to_shape(bounds, frame).map_err(PyErr::from)
            } else {
                Ok(Shape::Polygon(box_polygon(minx, miny, maxx, maxy)?))
            }
        },
        Some(value) => Err(parameter_error(
            crate::tokens::unknown_token_message("box wrap", value, &["split"]),
            "wrap",
        )),
    }?;
    // Box construction already orders its chart sides counterclockwise.
    // Planar orientation after canonical seam publication can reverse a band.
    Ok(if ccw { shape } else { shape.reverse()? })
}
