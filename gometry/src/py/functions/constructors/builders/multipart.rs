use super::*;

pub(super) fn multipart_members(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    context: &str,
    expected: &str,
    exact: impl Fn(&Shape) -> Option<Shape>,
    raw: impl Fn(&Bound<'_, PyAny>) -> PyResult<Shape>,
) -> PyResult<(Vec<Shape>, Frame)> {
    numpy::reject_masked_array(values)?;
    // Materialize every member before frame reconciliation so a later member
    // error keeps precedence over a frame conflict.
    let iter = values.try_iter()?;
    let capacity = crate::optional_len(values)?.unwrap_or_default();
    let mut members = Vec::with_capacity(capacity);
    let mut frames = Vec::with_capacity(capacity);
    for (row, item) in iter.enumerate() {
        let item = item?;
        crate::check_python_signals(values.py(), row)?;
        if let Some(geometry) = exact_geometry(&item) {
            let shape = exact(geometry.shape.shape()).ok_or_else(|| {
                geometry_type_err(format!("{context} expected {expected} at element {row}"))
            })?;
            members.push(shape);
            frames.push(geometry.frame.clone());
        } else {
            members.push(raw(&item)?);
            frames.push(Frame::None);
        }
    }
    let explicit = FrameAdoption {
        crs: parse_crs(crs)?,
        epoch: coordinate_epoch_option("epoch", epoch)?,
    };
    let frame = metadata::resolve_frame_rows(&mut frames, explicit, context)?;
    Ok((members, frame))
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
    let (shape, frame) = build_multi_point_parts(coordinates, x, y, z, m, crs, epoch)?;
    Ok(PyGeometry::with_frame(shape, frame))
}

pub(super) fn build_multi_point_parts(
    coordinates: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Shape, Frame)> {
    if let Some(coordinates) = coordinates {
        numpy::reject_masked_array(coordinates)?;
    }
    // No coordinates → `MULTIPOINT EMPTY` (Shapely-compatible `MultiPoint()`).
    if coordinates.is_none() && x.is_none() && y.is_none() && z.is_none() && m.is_none() {
        return Ok((empty_multi_point(), parse_crs_epoch(crs, epoch)?));
    }
    if x.is_some() || y.is_some() || z.is_some() || m.is_some() {
        let points = coordinate_vertices(coordinates, x, y, z, m)?;
        return Ok((
            Shape::multi_point(points.axes(), points),
            parse_crs_epoch(crs, epoch)?,
        ));
    }
    let coordinates = coordinates.expect("non-empty multipart input checked above");
    // N×D buffer is a flat multipoint, not a sequence of Point members.
    if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
        coordinates,
        None,
        None,
        crate::CoordSeqBufferMode::Geometry(crate::CoordSeqStructure::Sequence),
    )? {
        let seq = buffer.coordinates;
        return Ok((
            Shape::multi_point(seq.axes(), seq),
            parse_crs_epoch(crs, epoch)?,
        ));
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
            Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        },
        |item| {
            Ok(extract_coordinate_or_empty(item)?.map_or_else(
                || Shape::typed_empty(GeometryKind::Point, CoordinateAxes::XY),
                Shape::Point,
            ))
        },
    )?;
    if members.is_empty() {
        return Ok((empty_multi_point(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiPoint")?;
    if members.iter().all(|shape| matches!(shape, Shape::Point(_))) {
        let coordinates = dense_multi_point_coordinates(members, axes)?;
        return Ok((Shape::multi_point(axes, coordinates), frame));
    }
    let mut point_members = Vec::with_capacity(members.len());
    for shape in members {
        point_members.push(match shape {
            Shape::Point(point) => MultiMember::Value(point),
            Shape::Empty(GeometryKind::Point, _) => MultiMember::Empty,
            Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => unreachable!("multipart member type checked above"),
        });
    }
    if point_members.is_empty() {
        return Ok((empty_multi_point(), frame));
    }
    Ok((
        Shape::MultiPoint(MultiPoint::try_new(axes, point_members).map_err(PyErr::from)?),
        frame,
    ))
}

pub(super) fn dense_multi_point_coordinates(
    members: Vec<Shape>,
    axes: CoordinateAxes,
) -> PyResult<CoordSeq> {
    let mut coordinates = CoordSeqBuilder::with_capacity(axes, members.len());
    for member in members {
        let Shape::Point(point) = member else {
            unreachable!("dense MultiPoint members are points");
        };
        coordinates.push(point);
    }
    coordinates.finish().map_err(PyErr::from)
}

/// Build a ``MultiLineString`` from a sequence of line coordinate sequences.
pub(crate) fn build_multi_line_string(
    lines: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    let (shape, frame) = build_multi_line_string_parts(lines, crs, epoch)?;
    Ok(PyGeometry::with_frame(shape, frame))
}

pub(super) fn build_multi_line_string_parts(
    lines: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Shape, Frame)> {
    // No members → `MULTILINESTRING EMPTY` (Shapely-compatible `MultiLineString()`).
    let Some(lines) = lines else {
        return Ok((empty_multi_line_string(), parse_crs_epoch(crs, epoch)?));
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
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        },
        |item| {
            let seq = if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
                item,
                None,
                None,
                crate::CoordSeqBufferMode::Geometry(crate::CoordSeqStructure::Sequence),
            )? {
                buffer.coordinates
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
        return Ok((empty_multi_line_string(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiLineString")?;
    if members
        .iter()
        .all(|shape| matches!(shape, Shape::LineString(_)))
    {
        let mut lines = Vec::with_capacity(members.len());
        for shape in members {
            let Shape::LineString(line) = shape else {
                unreachable!("dense MultiLineString members are lines");
            };
            lines.push(line);
        }
        let shape = Shape::multi_line_string(axes, lines);
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        return Ok((shape, frame));
    }
    let member_count = members.len();
    let mut line_members = Vec::with_capacity(member_count);
    for shape in members {
        line_members.push(match shape {
            Shape::LineString(line) => MultiMember::Value(line),
            Shape::Empty(GeometryKind::LineString, _) => MultiMember::Empty,
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => unreachable!("multipart member type checked above"),
        });
    }
    let shape =
        Shape::MultiLineString(MultiLineString::try_new(axes, line_members).map_err(PyErr::from)?);
    // Match writers (`require_serializable_axes`): reject heterogeneous XY/XYZ
    // members at construction rather than admit a shape that cannot serialize
    // without inventing Z/M.
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok((shape, frame))
}

/// Build a ``MultiPolygon`` from a sequence of polygons.
pub(crate) fn build_multi_polygon(
    polygons: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    let (shape, frame) = build_multi_polygon_parts(polygons, crs, epoch)?;
    Ok(PyGeometry::with_frame(shape, frame))
}

pub(super) fn build_multi_polygon_parts(
    polygons: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Shape, Frame)> {
    // No members → `MULTIPOLYGON EMPTY` (Shapely-compatible `MultiPolygon()`).
    let Some(polygons) = polygons else {
        return Ok((empty_multi_polygon(), parse_crs_epoch(crs, epoch)?));
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
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        },
        polygon_from_coordinates_item,
    )?;
    if members.is_empty() {
        return Ok((empty_multi_polygon(), frame));
    }
    let axes = homogeneous_multipart_axes(&members, "MultiPolygon")?;
    if members
        .iter()
        .all(|shape| matches!(shape, Shape::Polygon(_)))
    {
        let mut polygons = Vec::with_capacity(members.len());
        for shape in members {
            let Shape::Polygon(polygon) = shape else {
                unreachable!("dense MultiPolygon members are polygons");
            };
            polygons.push(polygon);
        }
        let shape = Shape::multi_polygon(axes, polygons);
        crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
        return Ok((shape, frame));
    }
    let member_count = members.len();
    let mut polygon_members = Vec::with_capacity(member_count);
    for shape in members {
        polygon_members.push(match shape {
            Shape::Polygon(polygon) => MultiMember::Value(polygon),
            Shape::Empty(GeometryKind::Polygon, _) => MultiMember::Empty,
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => unreachable!("multipart member type checked above"),
        });
    }
    let shape =
        Shape::MultiPolygon(MultiPolygon::try_new(axes, polygon_members).map_err(PyErr::from)?);
    crate::io::require_serializable_axes(&shape).map_err(PyErr::from)?;
    Ok((shape, frame))
}
