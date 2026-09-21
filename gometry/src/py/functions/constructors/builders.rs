#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::boundary::FloatInput;
use crate::geometry::{
    Bounds, HasM, HasZ, MOrdinate, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    ZOrdinate, geographic_bounds_to_shape,
};
use crate::py::errors::parameter_error;
use crate::py::functions::constructors::{
    Bound, CRSError, CoordSeq, CoordinateAxes, Crs, Frame, FrameAdoption, GeometryKind,
    InvalidGeometryError, LineSeq, Point, Polygon, PyAny, PyAnyMethods as _, PyErr, PyGeometry,
    PyGeometryArray, PyResult, PyTypeError, Ring, Shape, box_polygon, coordinate_column_values,
    coordinate_column_values_exact, coordinate_epoch_option, coordinate_sequence_len_hint, crs,
    ensure_homogeneous_axes, exact_geometry, extract_coordinate, extract_coordinate_or_empty,
    extract_lines, extract_points, extract_polygons, finite_coordinate_required,
    optional_coordinate_column_values, optional_coordinates, parse_crs, parse_crs_epoch,
    wrapped_box,
};
use crate::py::numpy;

pub(super) fn geometry_with_crs_epoch(
    shape: impl crate::geometry::IntoSharedShape,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    Ok(PyGeometry::with_frame(shape, parse_crs_epoch(crs, epoch)?))
}

fn multipart_members(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    context: &str,
    expected: &str,
    exact: impl Fn(&Shape) -> Option<Shape>,
    raw: impl Fn(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<(Vec<Shape>, Frame)> {
    numpy::reject_masked_array(values)?;
    // Fallible growth (D10) via collect_py_iter — no artificial count ceiling.
    let mut row = 0_usize;
    let mut members = crate::collect_py_iter(values, |item| {
        let this_row = row;
        row += 1;
        if let Some(geometry) = exact_geometry(&item) {
            let shape = exact(geometry.shape.shape()).ok_or_else(|| {
                PyTypeError::new_err(format!(
                    "{context} expected {expected} at element {this_row}"
                ))
            })?;
            Ok(PyGeometry::with_frame(shape, geometry.frame.clone()))
        } else {
            Ok(PyGeometry::with_frame(raw(&item)?, Frame::None))
        }
    })?;
    let frame = Frame::resolve_items(
        &mut members,
        FrameAdoption {
            crs: parse_crs(crs)?,
            epoch: coordinate_epoch_option("epoch", epoch)?,
        },
        context,
    )?;
    let member_count = members.len();
    let mut shapes = Vec::new();
    if let Err(error) = crate::try_reserve_vec(&mut shapes, member_count) {
        drop(members);
        return Err(error.into());
    }
    for geometry in members {
        shapes.push(match std::sync::Arc::try_unwrap(geometry.shape) {
            Ok(data) => data.into_shape(),
            Err(shared) => shared.shape().clone(),
        });
    }
    Ok((shapes, frame))
}

fn homogeneous_multipart_axes(members: &[Shape], kind: &str) -> PyResult<CoordinateAxes> {
    let axes = members.first().map_or(CoordinateAxes::XY, Shape::axes);
    if members.iter().any(|member| member.axes() != axes) {
        return Err(InvalidGeometryError::new_err(format!(
            "{kind} members must share one coordinate axes layout (XY, XYZ, XYM, or XYZM)"
        )));
    }
    Ok(axes)
}

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
    let shape = match (x, y) {
        (Some(x), Some(y)) => {
            let x = finite_coordinate_required("x", x)?;
            let y = finite_coordinate_required("y", y)?;
            let z = z.map(|z| finite_coordinate_required("z", z)).transpose()?;
            let m = m.map(|m| finite_coordinate_required("m", m)).transpose()?;
            Shape::Point(Point::new_raw_axes(x, y, ZOrdinate(z), MOrdinate(m)))
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
    geometry_with_crs_epoch(shape, crs, epoch)
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
            if let Some(seq) = crate::try_coordseq_from_nd_buffer(coordinates, z, m)? {
                return Ok(seq);
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
    // would invent Z/M on serialize. Promote with force_3d/set_m first.
    let holes = match holes {
        // Fallible growth (D10): `holes=itertools.repeat(...)` → MemoryError.
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

/// Build a ``MultiPoint`` geometry from a coordinate sequence.
pub(crate) fn build_multi_point(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    if let Some(coordinates) = coordinates {
        numpy::reject_masked_array(coordinates)?;
    }
    // No coordinates → `MULTIPOINT EMPTY` (Shapely-compatible `MultiPoint()`).
    if coordinates.is_none() && x.is_none() && y.is_none() && z.is_none() && m.is_none() {
        return geometry_with_crs_epoch(empty_multi_point(), crs, epoch);
    }
    if x.is_some() || y.is_some() || z.is_some() || m.is_some() {
        let points = coordinate_vertices(coordinates, x, y, z, m)?;
        return geometry_with_crs_epoch(Shape::multi_point(points.axes(), points), crs, epoch);
    }
    let coordinates = coordinates.expect("non-empty multipart input checked above");
    // N×D buffer is a flat multipoint, not a sequence of Point members.
    if let Some(seq) = crate::try_coordseq_from_nd_buffer(coordinates, None, None)? {
        return geometry_with_crs_epoch(Shape::multi_point(seq.axes(), seq), crs, epoch);
    }
    let (members, frame) = multipart_members(
        coordinates,
        crs,
        epoch,
        "MultiPoint",
        "Point or coordinate",
        |shape| match shape {
            Shape::Point(point) => Some(Shape::Point(*point)),
            Shape::Empty(GeometryKind::Point, _) => Some(shape.clone()),
            _ => None,
        },
        |item| {
            Ok(extract_coordinate_or_empty(item)?.map_or_else(
                || Shape::typed_empty(GeometryKind::Point, CoordinateAxes::XY),
                Shape::Point,
            ))
        },
    )?;
    if members.is_empty() {
        return Ok(PyGeometry::with_frame(empty_multi_point(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiPoint")?;
    if members.iter().all(|shape| matches!(shape, Shape::Point(_))) {
        let mut points = crate::try_vec_with_capacity(members.len())?;
        for shape in members {
            let Shape::Point(point) = shape else {
                unreachable!("dense MultiPoint members are points");
            };
            crate::try_push(&mut points, point)?;
        }
        return Ok(PyGeometry::with_frame(
            Shape::multi_point(
                axes,
                CoordSeq::try_from_points(&points).map_err(PyErr::from)?,
            ),
            frame,
        ));
    }
    let mut point_members = crate::try_vec_with_capacity(members.len())?;
    for shape in members {
        crate::try_push(&mut point_members, match shape {
            Shape::Point(point) => MultiMember::Value(point),
            Shape::Empty(GeometryKind::Point, _) => MultiMember::Empty,
            _ => unreachable!("multipart member type checked above"),
        })?;
    }
    if point_members.is_empty() {
        return Ok(PyGeometry::with_frame(empty_multi_point(), frame));
    }
    Ok(PyGeometry::with_frame(
        Shape::MultiPoint(MultiPoint::try_new(axes, point_members).map_err(PyErr::from)?),
        frame,
    ))
}

/// Build a ``MultiLineString`` from a sequence of line coordinate sequences.
pub(crate) fn build_multi_line_string(
    lines: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `MULTILINESTRING EMPTY` (Shapely-compatible `MultiLineString()`).
    let Some(lines) = lines else {
        return geometry_with_crs_epoch(empty_multi_line_string(), crs, epoch);
    };
    let (members, frame) = multipart_members(
        lines,
        crs,
        epoch,
        "MultiLineString",
        "LineString or coordinate sequence",
        |shape| match shape {
            Shape::LineString(line) => Some(Shape::LineString(line.clone())),
            Shape::Empty(GeometryKind::LineString, _) => Some(shape.clone()),
            _ => None,
        },
        |item| {
            let seq = if let Some(seq) = crate::try_coordseq_from_nd_buffer(item, None, None)? {
                seq
            } else {
                CoordSeq::try_from_points(&extract_points(item, None, None)?)
                    .map_err(PyErr::from)?
            };
            if seq.is_empty() {
                Ok(Shape::typed_empty(GeometryKind::LineString, seq.axes()))
            } else {
                Ok(Shape::LineString(
                    LineSeq::try_new(seq).map_err(PyErr::from)?,
                ))
            }
        },
    )?;
    if members.is_empty() {
        return Ok(PyGeometry::with_frame(empty_multi_line_string(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiLineString")?;
    if members
        .iter()
        .all(|shape| matches!(shape, Shape::LineString(_)))
    {
        let mut lines = crate::try_vec_with_capacity(members.len())?;
        for shape in members {
            let Shape::LineString(line) = shape else {
                unreachable!("dense MultiLineString members are lines");
            };
            crate::try_push(&mut lines, line)?;
        }
        let shape = Shape::multi_line_string(axes, lines).map_err(PyErr::from)?;
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        return Ok(PyGeometry::with_frame(shape, frame));
    }
    let member_count = members.len();
    let mut line_members = Vec::new();
    if let Err(error) = crate::try_reserve_vec(&mut line_members, member_count) {
        drop(members);
        return Err(error.into());
    }
    for shape in members {
        crate::try_push_vec(&mut line_members, match shape {
            Shape::LineString(line) => MultiMember::Value(line),
            Shape::Empty(GeometryKind::LineString, _) => MultiMember::Empty,
            _ => unreachable!("multipart member type checked above"),
        })
        .map_err(PyErr::from)?;
    }
    let shape =
        Shape::MultiLineString(MultiLineString::try_new(axes, line_members).map_err(PyErr::from)?);
    // Match writers (`require_serializable_axes`): reject heterogeneous XY/XYZ
    // members at construction rather than admit a shape that cannot serialize
    // without inventing Z/M.
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(PyGeometry::with_frame(shape, frame))
}

/// Build a ``MultiPolygon`` from a sequence of polygons.
pub(crate) fn build_multi_polygon(
    polygons: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `MULTIPOLYGON EMPTY` (Shapely-compatible `MultiPolygon()`).
    let Some(polygons) = polygons else {
        return geometry_with_crs_epoch(empty_multi_polygon(), crs, epoch);
    };
    let (members, frame) = multipart_members(
        polygons,
        crs,
        epoch,
        "MultiPolygon",
        "Polygon or polygon coordinates",
        |shape| match shape {
            Shape::Polygon(polygon) => Some(Shape::Polygon(polygon.clone())),
            Shape::Empty(GeometryKind::Polygon, _) => Some(shape.clone()),
            _ => None,
        },
        polygon_from_coordinates_item,
    )?;
    if members.is_empty() {
        return Ok(PyGeometry::with_frame(empty_multi_polygon(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiPolygon")?;
    if members
        .iter()
        .all(|shape| matches!(shape, Shape::Polygon(_)))
    {
        let mut polygons = crate::try_vec_with_capacity(members.len())?;
        for shape in members {
            let Shape::Polygon(polygon) = shape else {
                unreachable!("dense MultiPolygon members are polygons");
            };
            crate::try_push(&mut polygons, polygon)?;
        }
        let shape = Shape::multi_polygon(axes, polygons).map_err(PyErr::from)?;
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        return Ok(PyGeometry::with_frame(shape, frame));
    }
    let member_count = members.len();
    let mut polygon_members = Vec::new();
    if let Err(error) = crate::try_reserve_vec(&mut polygon_members, member_count) {
        drop(members);
        return Err(error.into());
    }
    for shape in members {
        crate::try_push_vec(&mut polygon_members, match shape {
            Shape::Polygon(polygon) => MultiMember::Value(polygon),
            Shape::Empty(GeometryKind::Polygon, _) => MultiMember::Empty,
            _ => unreachable!("multipart member type checked above"),
        })
        .map_err(PyErr::from)?;
    }
    let shape =
        Shape::MultiPolygon(MultiPolygon::try_new(axes, polygon_members).map_err(PyErr::from)?);
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(PyGeometry::with_frame(shape, frame))
}

pub(super) fn build_box_shape(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    wrap: Option<&str>,
    crs: Option<&Crs>,
) -> PyResult<Shape> {
    if miny > maxy {
        return Err(InvalidGeometryError::new_err(format!(
            "box miny ({miny}) must not exceed maxy ({maxy})"
        )));
    }
    match wrap {
        None => {
            if minx > maxx {
                return Err(InvalidGeometryError::new_err(format!(
                    "box minx ({minx}) must not exceed maxx ({maxx}) unless wrap='split'"
                )));
            }
            // `box` names a lat/lon rectangle, so its horizontal edges are
            // parallels rather than one long geodesic between corners. Add
            // the minimum number of equal-longitude chords at the documented
            // angular limit.
            if let Some(crs) = crs
                && let Some(frame) = crs::info(crs).map_err(PyErr::from)?.geographic_frame()
                && (-frame.longitude().half_period()..=frame.longitude().half_period())
                    .contains(&minx)
                && (-frame.longitude().half_period()..=frame.longitude().half_period())
                    .contains(&maxx)
                && (-frame.pole()..=frame.pole()).contains(&miny)
                && (-frame.pole()..=frame.pole()).contains(&maxy)
                && minx < maxx
                && miny < maxy
            {
                return geographic_bounds_to_shape(Bounds::new(minx, miny, maxx, maxy)?, frame)
                    .map_err(PyErr::from);
            }
            Ok(Shape::Polygon(box_polygon(minx, miny, maxx, maxy)?))
        },
        Some("split") => {
            match crs {
                Some(crs) if crs::is_wgs84_lonlat(crs) => {},
                _ => {
                    return Err(CRSError::new_err(
                        "box wrap='split' requires crs=4326 (EPSG:4326 longitude/latitude)",
                    ));
                },
            }
            if maxx - minx >= 360.0 {
                // A full turn is a geographic latitude band, not a pair of
                // geodesic edges between the antimeridian endpoints.
                if miny < maxy {
                    let frame = crs::info("EPSG:4326")
                        .map_err(PyErr::from)?
                        .geographic_frame()
                        .expect("EPSG:4326 has a geographic frame");
                    return geographic_bounds_to_shape(
                        Bounds::new(-180.0, miny, 180.0, maxy)?,
                        frame,
                    )
                    .map_err(PyErr::from);
                }
            }
            Ok(wrapped_box(minx, miny, maxx, maxy)?)
        },
        Some(value) => Err(parameter_error(
            crate::tokens::unknown_token_message("box wrap", value, &["split"]),
            "wrap",
        )),
    }
}

/// A `LineString` array member from raw coordinates.
pub(super) fn line_string_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "LineString", |item| {
        let vertices = coordinate_vertices(Some(item), None, None, None, None)?;
        if vertices.is_empty() {
            Ok(Shape::typed_empty(
                GeometryKind::LineString,
                vertices.axes(),
            ))
        } else {
            Ok(Shape::LineString(
                LineSeq::try_new(vertices).map_err(PyErr::from)?,
            ))
        }
    })
}

/// A `MultiPoint` array member from raw coordinates.
pub(super) fn multi_point_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiPoint", |item| {
        let points = coordinate_vertices(Some(item), None, None, None, None)?;
        Ok(Shape::multi_point(points.axes(), points))
    })
}

/// A `MultiLineString` array member from raw line coordinate sequences.
pub(super) fn multi_line_string_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiLineString", |item| {
        let lines = extract_lines(item)?;
        if lines.is_empty() {
            return Ok(Shape::typed_empty(
                GeometryKind::MultiLineString,
                CoordinateAxes::XY,
            ));
        }
        let mut axes = None;
        let members = lines
            .into_iter()
            .map(|line| {
                let line = CoordSeq::try_from_points(&line).map_err(PyErr::from)?;
                if line.is_empty() {
                    Ok(MultiMember::Empty)
                } else {
                    let line_axes = line.axes();
                    if axes.is_some_and(|expected| expected != line_axes) {
                        return Err(InvalidGeometryError::new_err(
                            "MultiLineString members must share one coordinate axes layout (XY, XYZ, XYM, or XYZM)",
                        ));
                    }
                    axes = Some(line_axes);
                    Ok(MultiMember::Value(
                        LineSeq::try_new(line).map_err(PyErr::from)?,
                    ))
                }
            })
            .collect::<PyResult<Vec<_>>>()?;
        let shape = Shape::MultiLineString(
            MultiLineString::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
                .map_err(PyErr::from)?,
        );
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        Ok(shape)
    })
}

/// A `MultiPolygon` array member from raw polygon coordinate sequences.
pub(super) fn multi_polygon_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "MultiPolygon", |item| {
        let polygons = extract_polygons(item)?;
        if polygons.is_empty() {
            return Ok(Shape::typed_empty(
                GeometryKind::MultiPolygon,
                CoordinateAxes::XY,
            ));
        }
        let axes = polygons.first().map_or(CoordinateAxes::XY, Polygon::axes);
        let members = polygons.into_iter().map(MultiMember::Value).collect();
        let shape = Shape::MultiPolygon(MultiPolygon::try_new(axes, members).map_err(|error| {
            if matches!(error.kind(), crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::CoordinateAxesMismatch { .. }
            )) {
                InvalidGeometryError::new_err(
                    "MultiPolygon members must share one coordinate axes layout (XY, XYZ, XYM, or XYZM)",
                )
            } else {
                PyErr::from(error)
            }
        })?);
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        Ok(shape)
    })
}

pub(super) fn polygon_from_data_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    geometry_from_data_item(item, "Polygon", polygon_from_coordinates_item)
}

fn geometry_from_data_item(
    item: &Bound<'_, PyAny>,
    expected: &'static str,
    build: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<Shape> {
    if exact_geometry(item).is_some() {
        return Err(PyTypeError::new_err(format!(
            "{expected} array factories accept raw coordinate inputs only; use GeometryArray(values) for built geometries"
        )));
    }
    build(item)
}

fn polygon_from_coordinates_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    numpy::reject_masked_array(item)?;
    // Materialize once, then classify. Speculative `extract_points` on the
    // parent would consume the first ring of a one-shot iterator and leave
    // only holes for the ring-list path (shell silently dropped).
    let members = crate::collect_py_iter(item, Ok)?;
    let shape = polygon_from_coordinate_members(&members)?;
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(shape)
}

enum PolygonMember<'py> {
    Coordinate(Point),
    Nested(Vec<Bound<'py, PyAny>>),
}

fn materialize_polygon_member<'py>(member: &Bound<'py, PyAny>) -> PyResult<PolygonMember<'py>> {
    numpy::reject_masked_array(member)?;
    let mut iter = crate::py_iter_or_error(member, || {
        PyTypeError::new_err("polygon member must be a coordinate sequence")
    })?;
    let Some(first_item) = iter.next() else {
        return Ok(PolygonMember::Nested(Vec::new()));
    };
    let first_item = first_item?;
    let Some(first) = crate::f64_scalar_named("coordinate", &first_item)? else {
        // Ring cardinality is variable; only the coordinate-member branch is
        // bounded by the four-ordinate contract.
        let mut values = crate::try_vec_with_capacity_hint(1)?;
        crate::try_push(&mut values, first_item)?;
        let rest = crate::collect_py_iter_from_iter(member, iter, Ok::<_, PyErr>)?;
        for item in rest {
            crate::try_push(&mut values, item)?;
        }
        return Ok(PolygonMember::Nested(values));
    };
    if !first.is_finite() {
        return Err(InvalidGeometryError::new_err("x must be finite"));
    }
    let rest = crate::collect_py_iter_at_most_from_iter(
        member,
        iter,
        3,
        |item| FloatInput::Coordinate.item("coordinate", &item),
        |got| {
            InvalidGeometryError::new_err(format!(
                "coordinate has too many ordinates (got {})",
                got.saturating_add(1)
            ))
        },
    )?;
    let mut values = crate::try_vec_with_capacity(1 + rest.len())?;
    crate::try_push(&mut values, first)?;
    for value in rest {
        crate::try_push(&mut values, value)?;
    }
    if values.len() < 2 {
        return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
    }
    let x = values[0];
    let y = values[1];
    let z = values.get(2).copied();
    let m = values.get(3).copied();
    Ok(PolygonMember::Coordinate(Point::new_raw_axes(
        x,
        y,
        ZOrdinate(z),
        MOrdinate(m),
    )))
}

/// Classify a materialized top-level polygon item as a bare shell or ring list.
fn polygon_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Shape> {
    if members.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    let first = materialize_polygon_member(&members[0])?;
    match first {
        PolygonMember::Coordinate(first) => {
            // Bare shell `[(x, y), ...]`: each top-level member is admitted
            // once as one coordinate.
            let mut points = crate::try_vec_with_capacity(members.len())?;
            crate::try_push(&mut points, first)?;
            for member in &members[1..] {
                let PolygonMember::Coordinate(point) = materialize_polygon_member(member)? else {
                    return Err(PyTypeError::new_err(
                        "polygon shell must contain only coordinate values",
                    ));
                };
                crate::try_push(&mut points, point)?;
            }
            ensure_homogeneous_axes(&points)?;
            Ok(Shape::Polygon(Polygon::new(
                Ring::closed(points)?,
                Vec::new(),
            )))
        },
        PolygonMember::Nested(first) => {
            // Ring list: each ring source is materialized once before the
            // shell/holes distinction, so one-shot ring iterators survive.
            let shell = Ring::closed(points_from_coordinate_members(&first)?)?;
            let mut holes = crate::try_vec_with_capacity(members.len().saturating_sub(1))?;
            for member in &members[1..] {
                let PolygonMember::Nested(ring) = materialize_polygon_member(member)? else {
                    return Err(PyTypeError::new_err(
                        "polygon holes must contain coordinate sequences",
                    ));
                };
                crate::try_push(
                    &mut holes,
                    Ring::closed(points_from_coordinate_members(&ring)?)?,
                )?;
            }
            Ok(Shape::Polygon(Polygon::new(shell, holes)))
        },
    }
}

/// Parse every top-level member as a coordinate (bare-shell form).
fn points_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Vec<Point>> {
    let mut points = crate::try_vec_with_capacity(members.len())?;
    for member in members {
        crate::try_push(&mut points, extract_coordinate(member)?)?;
    }
    ensure_homogeneous_axes(&points)?;
    Ok(points)
}

pub(super) fn build_geometry_array(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    item: impl Fn(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<PyGeometryArray> {
    numpy::reject_masked_array(values)?;
    // Fallible growth (D10): linestrings/polygons factories and similar
    // `itertools.repeat` inputs terminate with MemoryError, never abort.
    // Stream directly into StreamingShapes — no intermediate Vec<Shape>.
    let frame = parse_crs_epoch(crs, epoch)?;
    let mut rows = crate::array::StreamingShapes::new();
    for (row, item_value) in values.try_iter()?.enumerate() {
        let item_value = item_value?;
        crate::check_python_signals(item_value.py(), row)?;
        rows.try_push(item(&item_value)?)?;
    }
    Ok(rows.finish(frame)?)
}

/// Build a ``GeometryCollection`` from a sequence of geometries.
pub(crate) fn build_geometry_collection(
    geometries: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `GEOMETRYCOLLECTION EMPTY` (Shapely-compatible
    // `GeometryCollection()`), carrying any explicit crs/epoch.
    let Some(geometries) = geometries else {
        return geometry_with_crs_epoch(empty_geometry_collection(), crs, epoch);
    };
    // Fallible growth (D10): `GeometryCollection(itertools.repeat(Point))`
    // must MemoryError on real fallible OOM, never hang/abort.
    // Fail-fast on the first missing member: GeometryCollection cannot carry
    // None (unlike GeometryArray rows).
    let mut row = 0_usize;
    let mut members = crate::collect_py_iter(geometries, |item| {
        let this_row = row;
        row += 1;
        if item.is_none() {
            return Err(crate::missing_geometry_items_error());
        }
        let Some(geometry) = exact_geometry(&item) else {
            return Err(PyTypeError::new_err(format!(
                "expected Geometry at element {this_row}; strings parse via from_wkt/from_wkb/from_geojson"
            )));
        };
        Ok(geometry.clone())
    })?;
    let explicit_crs = parse_crs(crs)?;
    let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
    let frame = Frame::resolve_items(
        &mut members,
        FrameAdoption {
            crs: explicit_crs,
            epoch: explicit_epoch,
        },
        "GeometryCollection",
    )?;
    let items = crate::try_collect_results(members.into_iter().map(|geometry| {
        Ok::<_, PyErr>(match std::sync::Arc::try_unwrap(geometry.shape) {
            Ok(data) => data.into_shape(),
            Err(shared) => shared.shape().clone(),
        })
    }))?;
    Ok(PyGeometry::with_epoch(
        Shape::geometry_collection(items)?,
        frame.crs_owned(),
        frame.epoch(),
    )?)
}
