use super::*;

/// A `LineString` array member from raw coordinates.
pub(in crate::py::functions::constructors) enum GeometryArrayItem<'py> {
    Python(Bound<'py, PyAny>),
    Buffer(crate::CoordSeqBuffer),
}

pub(in crate::py::functions::constructors) fn line_string_from_data_item(
    item: GeometryArrayItem<'_>,
) -> PyResult<(Shape, Frame)> {
    match item {
        GeometryArrayItem::Buffer(buffer) => Ok((
            line_string_from_coordinates(buffer.coordinates)?,
            Frame::None,
        )),
        GeometryArrayItem::Python(item) => geometry_from_data_item(
            &item,
            "LineString",
            |shape| {
                matches!(
                    shape,
                    Shape::LineString(_) | Shape::Empty(GeometryKind::LineString, _)
                )
            },
            |item| {
                let vertices = coordinate_vertices(Some(item), None, None, None, None)?;
                Ok((line_string_from_coordinates(vertices)?, Frame::None))
            },
        ),
    }
}

fn line_string_from_coordinates(vertices: CoordSeq) -> PyResult<Shape> {
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
}

/// A `MultiPoint` array member from raw coordinates.
pub(in crate::py::functions::constructors) fn multi_point_from_data_item(
    item: GeometryArrayItem<'_>,
) -> PyResult<(Shape, Frame)> {
    match item {
        GeometryArrayItem::Buffer(buffer) => {
            let points = buffer.coordinates;
            Ok((Shape::multi_point(points.axes(), points), Frame::None))
        },
        GeometryArrayItem::Python(item) => geometry_from_data_item(
            &item,
            "MultiPoint",
            |shape| {
                matches!(
                    shape,
                    Shape::MultiPoint(_) | Shape::Empty(GeometryKind::MultiPoint, _)
                )
            },
            |item| build_multi_point_parts(Some(item), None, None, None, None, None, None),
        ),
    }
}

/// A `MultiLineString` array member from raw line coordinate sequences.
pub(in crate::py::functions::constructors) fn multi_line_string_from_data_item(
    item: GeometryArrayItem<'_>,
) -> PyResult<(Shape, Frame)> {
    match item {
        GeometryArrayItem::Buffer(buffer) => {
            Ok((multi_line_string_from_buffer(buffer)?, Frame::None))
        },
        GeometryArrayItem::Python(item) => geometry_from_data_item(
            &item,
            "MultiLineString",
            |shape| {
                matches!(
                    shape,
                    Shape::MultiLineString(_) | Shape::Empty(GeometryKind::MultiLineString, _)
                )
            },
            |item| build_multi_line_string_parts(Some(item), None, None),
        ),
    }
}

fn multi_line_string_from_buffer(buffer: crate::CoordSeqBuffer) -> PyResult<Shape> {
    let [line_count, _] = buffer.shape.as_slice() else {
        unreachable!("MultiLineString buffers have line and coordinate dimensions");
    };
    if *line_count == 0 {
        return Ok(Shape::typed_empty(
            GeometryKind::MultiLineString,
            CoordinateAxes::XY,
        ));
    }
    let lines = (0..*line_count)
        .map(|index| buffer.row(index).map(|line| line.coordinates))
        .collect::<crate::error::Result<Vec<_>>>()?;
    // Admit every coordinate before checking line length or cross-line axes,
    // preserving the scalar constructor's error precedence.
    if lines.iter().any(|line| {
        line.points().any(|point| {
            !point.x.is_finite()
                || !point.y.is_finite()
                || point.z().is_some_and(|value| !value.is_finite())
                || point.m().is_some_and(|value| !value.is_finite())
        })
    }) {
        return Err(InvalidGeometryError::new_err("coordinates must be finite"));
    }
    multi_line_string_from_sequences(lines)
}

fn multi_line_string_from_sequences(lines: Vec<CoordSeq>) -> PyResult<Shape> {
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
}

/// A `MultiPolygon` array member from raw polygon coordinate sequences.
pub(in crate::py::functions::constructors) fn multi_polygon_from_data_item(
    item: GeometryArrayItem<'_>,
) -> PyResult<(Shape, Frame)> {
    match item {
        GeometryArrayItem::Buffer(buffer) => Ok((multi_polygon_from_buffer(buffer)?, Frame::None)),
        GeometryArrayItem::Python(item) => geometry_from_data_item(
            &item,
            "MultiPolygon",
            |shape| {
                matches!(
                    shape,
                    Shape::MultiPolygon(_) | Shape::Empty(GeometryKind::MultiPolygon, _)
                )
            },
            |item| build_multi_polygon_parts(Some(item), None, None),
        ),
    }
}

fn multi_polygon_from_buffer(buffer: crate::CoordSeqBuffer) -> PyResult<Shape> {
    let [polygon_count, _, _] = buffer.shape.as_slice() else {
        unreachable!("MultiPolygon buffers have polygon, ring, and coordinate dimensions");
    };
    if *polygon_count == 0 {
        return Ok(Shape::typed_empty(
            GeometryKind::MultiPolygon,
            CoordinateAxes::XY,
        ));
    }
    let mut polygons = Vec::with_capacity(*polygon_count);
    for index in 0..*polygon_count {
        let polygon = buffer.row(index)?;
        let Some(polygon) = polygon_from_buffer(polygon, PolygonBufferContext::MultiPolygon)?
        else {
            unreachable!("a MultiPolygon member must contain its shell");
        };
        polygons.push(polygon);
    }
    multi_polygon_from_polygons(polygons)
}

fn multi_polygon_from_polygons(polygons: Vec<Polygon>) -> PyResult<Shape> {
    if polygons.is_empty() {
        return Ok(Shape::typed_empty(
            GeometryKind::MultiPolygon,
            CoordinateAxes::XY,
        ));
    }
    let axes = polygons.first().map_or(CoordinateAxes::XY, Polygon::axes);
    let members = polygons.into_iter().map(MultiMember::Value).collect();
    let shape = Shape::MultiPolygon(MultiPolygon::try_new(axes, members).map_err(|error| {
        if matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::CoordinateAxesMismatch { .. }
            )
        ) {
            InvalidGeometryError::new_err(
                "MultiPolygon members must share one coordinate axes layout (XY, XYZ, XYM, or XYZM)",
            )
        } else {
            PyErr::from(error)
        }
    })?);
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(shape)
}

pub(in crate::py::functions::constructors) fn polygon_from_data_item(
    item: GeometryArrayItem<'_>,
) -> PyResult<(Shape, Frame)> {
    match item {
        GeometryArrayItem::Buffer(buffer) => Ok((
            polygon_shape_from_buffer(buffer, PolygonBufferContext::Polygon)?,
            Frame::None,
        )),
        GeometryArrayItem::Python(item) => geometry_from_data_item(
            &item,
            "Polygon",
            |shape| {
                matches!(
                    shape,
                    Shape::Polygon(_) | Shape::Empty(GeometryKind::Polygon, _)
                )
            },
            |item| Ok((polygon_from_coordinates_item(item)?, Frame::None)),
        ),
    }
}

fn geometry_from_data_item(
    item: &Bound<'_, PyAny>,
    expected: &'static str,
    matches_kind: impl FnOnce(&Shape) -> bool,
    build: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<(Shape, Frame)>,
) -> PyResult<(Shape, Frame)> {
    if let Some(geometry) = exact_geometry(item) {
        let shape = geometry.shape.shape();
        if !matches_kind(shape) {
            return Err(geometry_type_err(format!(
                "{expected} array factories require {expected} rows, got {}",
                shape.geometry_type()
            )));
        }
        return Ok((shape.clone(), geometry.frame.clone()));
    }
    build(item)
}

pub(super) fn polygon_from_coordinates_item(item: &Bound<'_, PyAny>) -> PyResult<Shape> {
    if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
        item,
        None,
        None,
        crate::CoordSeqBufferMode::Geometry(crate::CoordSeqStructure::Polygon),
    )? {
        return polygon_shape_from_buffer(buffer, PolygonBufferContext::Polygon);
    }
    // Materialize once, then classify. Speculative `extract_points` on the
    // parent would consume the first ring of a one-shot iterator and leave
    // only holes for the ring-list path (shell silently dropped).
    let members = crate::collect_py_iter(item, Ok)?;
    let shape = polygon_from_coordinate_members(&members)?;
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(shape)
}

#[derive(Clone, Copy)]
enum PolygonBufferContext {
    Polygon,
    MultiPolygon,
}

fn polygon_shape_from_buffer(
    buffer: crate::CoordSeqBuffer,
    context: PolygonBufferContext,
) -> PyResult<Shape> {
    let shape =
        polygon_from_buffer(buffer, context)?.map_or_else(Shape::empty_polygon, Shape::Polygon);
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok(shape)
}

fn polygon_from_buffer(
    buffer: crate::CoordSeqBuffer,
    context: PolygonBufferContext,
) -> PyResult<Option<Polygon>> {
    match buffer.shape.as_slice() {
        [_] => {
            if buffer.coordinates.is_empty() {
                return Ok(None);
            }
            let shell = ring_from_buffer_coordinates(buffer.coordinates, context)?;
            Ok(Some(Polygon::new(shell, Vec::new())))
        },
        [ring_count, _] => {
            if *ring_count == 0 {
                return match context {
                    PolygonBufferContext::Polygon => Ok(None),
                    PolygonBufferContext::MultiPolygon => Err(InvalidGeometryError::new_err(
                        "multi_polygon member requires a shell",
                    )),
                };
            }
            let mut shell = None;
            let mut holes = Vec::with_capacity(ring_count.saturating_sub(1));
            match context {
                PolygonBufferContext::Polygon => {
                    for ring_index in 0..*ring_count {
                        append_buffer_ring(&buffer, ring_index, context, &mut shell, &mut holes)?;
                    }
                },
                PolygonBufferContext::MultiPolygon => {
                    for ring_index in 1..*ring_count {
                        append_buffer_ring(&buffer, ring_index, context, &mut shell, &mut holes)?;
                    }
                    append_buffer_ring(&buffer, 0, context, &mut shell, &mut holes)?;
                },
            }
            Ok(Some(Polygon::new(
                shell.expect("every non-empty ring list has a shell"),
                holes,
            )))
        },
        _ => unreachable!("Polygon buffers contain vertices or rings and vertices"),
    }
}

fn append_buffer_ring(
    buffer: &crate::CoordSeqBuffer,
    ring_index: usize,
    context: PolygonBufferContext,
    shell: &mut Option<Ring>,
    holes: &mut Vec<Ring>,
) -> PyResult<()> {
    let coordinates = buffer.row(ring_index)?.coordinates;
    let ring = ring_from_buffer_coordinates(coordinates, context)?;
    if ring_index == 0 {
        *shell = Some(ring);
    } else {
        holes.push(ring);
    }
    Ok(())
}

fn ring_from_buffer_coordinates(
    coordinates: CoordSeq,
    context: PolygonBufferContext,
) -> PyResult<Ring> {
    match context {
        PolygonBufferContext::Polygon => {
            for point in coordinates.points() {
                if !point.x.is_finite() {
                    return Err(InvalidGeometryError::new_err("x must be finite"));
                }
                if !point.y.is_finite()
                    || point.z().is_some_and(|value| !value.is_finite())
                    || point.m().is_some_and(|value| !value.is_finite())
                {
                    return Err(InvalidGeometryError::new_err("coordinate must be finite"));
                }
            }
        },
        PolygonBufferContext::MultiPolygon => {
            if coordinates.points().any(|point| {
                !point.x.is_finite()
                    || !point.y.is_finite()
                    || point.z().is_some_and(|value| !value.is_finite())
                    || point.m().is_some_and(|value| !value.is_finite())
            }) {
                return Err(InvalidGeometryError::new_err("coordinates must be finite"));
            }
        },
    }
    Ring::closed_coordseq(coordinates).map_err(PyErr::from)
}

enum PolygonMember<'py> {
    Coordinate(Point),
    Nested(Vec<Bound<'py, PyAny>>),
    Buffer(CoordSeq),
}

fn materialize_polygon_member<'py>(member: &Bound<'py, PyAny>) -> PyResult<PolygonMember<'py>> {
    if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
        member,
        None,
        None,
        crate::CoordSeqBufferMode::Geometry(crate::CoordSeqStructure::Sequence),
    )? {
        return Ok(PolygonMember::Buffer(buffer.coordinates));
    }
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
        let mut values = Vec::with_capacity(1);
        values.push(first_item);
        let rest = crate::collect_py_iter_from_iter(member, iter, Ok::<_, PyErr>)?;
        values.reserve(rest.len());
        values.extend(rest);
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
    let mut values = Vec::with_capacity(rest.len().saturating_add(1));
    values.push(first);
    values.extend(rest);
    if values.len() < 2 {
        return Err(InvalidGeometryError::new_err("coordinate requires x and y"));
    }
    let [x, y, optional @ ..] = values.as_slice() else {
        unreachable!("coordinate length checked above");
    };
    let z = optional.first().copied();
    let m = optional.get(1).copied();
    Ok(PolygonMember::Coordinate(Point::new_raw_axes(
        *x,
        *y,
        ZOrdinate(z),
        MOrdinate(m),
    )))
}

/// Classify a materialized top-level polygon item as a bare shell or ring list.
fn polygon_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Shape> {
    let Some((first_member, remaining_members)) = members.split_first() else {
        return Ok(Shape::empty_polygon());
    };
    let first = materialize_polygon_member(first_member)?;
    match first {
        PolygonMember::Coordinate(first) => {
            // Bare shell `[(x, y), ...]`: each top-level member is admitted
            // once as one coordinate.
            let mut points = Vec::with_capacity(members.len());
            points.push(first);
            for member in remaining_members {
                let PolygonMember::Coordinate(point) = materialize_polygon_member(member)? else {
                    return Err(PyTypeError::new_err(
                        "polygon shell must contain only coordinate values",
                    ));
                };
                points.push(point);
            }
            ensure_homogeneous_axes(&points)?;
            Ok(Shape::Polygon(Polygon::new(
                Ring::closed(points)?,
                Vec::new(),
            )))
        },
        PolygonMember::Buffer(shell) => {
            let shell = ring_from_buffer_coordinates(shell, PolygonBufferContext::Polygon)?;
            let mut holes = Vec::with_capacity(remaining_members.len());
            for member in remaining_members {
                let ring = materialize_polygon_member(member)?;
                holes.push(polygon_ring_from_member(ring)?);
            }
            Ok(Shape::Polygon(Polygon::new(shell, holes)))
        },
        PolygonMember::Nested(first) => {
            // Ring list: each ring source is materialized once before the
            // shell/holes distinction, so one-shot ring iterators survive.
            let shell = Ring::closed(points_from_coordinate_members(&first)?)?;
            let mut holes = Vec::with_capacity(remaining_members.len());
            for member in remaining_members {
                let ring = materialize_polygon_member(member)?;
                holes.push(polygon_ring_from_member(ring)?);
            }
            Ok(Shape::Polygon(Polygon::new(shell, holes)))
        },
    }
}

fn polygon_ring_from_member(member: PolygonMember<'_>) -> PyResult<Ring> {
    match member {
        PolygonMember::Buffer(coordinates) => {
            ring_from_buffer_coordinates(coordinates, PolygonBufferContext::Polygon)
        },
        PolygonMember::Nested(coordinates) => {
            Ring::closed(points_from_coordinate_members(&coordinates)?).map_err(PyErr::from)
        },
        PolygonMember::Coordinate(_) => Err(PyTypeError::new_err(
            "polygon holes must contain coordinate sequences",
        )),
    }
}

/// Parse every top-level member as a coordinate (bare-shell form).
fn points_from_coordinate_members(members: &[Bound<'_, PyAny>]) -> PyResult<Vec<Point>> {
    let mut points = Vec::with_capacity(members.len());
    for member in members {
        points.push(extract_coordinate(member)?);
    }
    ensure_homogeneous_axes(&points)?;
    Ok(points)
}
