//! Decode one fully-owned Arrow batch without Python-shaped array objects.
//!
//! Admission in `native.rs` owns every selected buffer before this module is
//! entered.  The decoder therefore works only with `NativeNode` accessors and
//! can run entirely inside `Python::detach`.

use std::cell::OnceCell;
use std::sync::Arc;

use super::{ArrowFormat, NativeArrowBatch, NativeNode};
use crate::geometry::{
    CoordSeq, CoordWindow, CoordinateAxes, GeometryKind, HasM, HasZ, LineSeq, MOrdinate,
    MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Shape, ZOrdinate,
    column_all_finite,
};
use crate::io;
use crate::py::geoarrow::GeometryEncoding;

/// Errors that can be produced after the Arrow producer has been released.
/// Arrow structure errors retain the GeoArrow parse boundary; geometry and WKB
/// errors retain the crate's ordinary domain classification.
#[derive(Debug)]
pub(crate) enum NativeArrowDecodeError {
    GeoArrow(String),
    Core(crate::error::Error),
}

impl From<crate::error::Error> for NativeArrowDecodeError {
    fn from(error: crate::error::Error) -> Self {
        Self::Core(error)
    }
}

impl NativeArrowDecodeError {
    pub(crate) fn geoarrow(message: impl Into<String>) -> Self {
        Self::GeoArrow(message.into())
    }

    pub(crate) fn into_py_err(self) -> pyo3::PyErr {
        match self {
            Self::GeoArrow(message) => crate::py::arrow::geoarrow_parse_error(message),
            Self::Core(error) => error.into(),
        }
    }
}

pub(crate) struct NativeDecodedBatch {
    pub(crate) length: usize,
    pub(crate) shapes: Vec<Shape>,
    /// One entry per present shape, in logical row order.
    pub(crate) srids: Vec<Option<u32>>,
    /// Local logical row indexes whose outer geometry value is null.
    pub(crate) missing: Vec<usize>,
}

type DecodeResult<T> = Result<T, NativeArrowDecodeError>;

pub(crate) fn decode_batch(batch: NativeArrowBatch) -> DecodeResult<NativeDecodedBatch> {
    let NativeArrowBatch {
        node,
        classification,
    } = batch;
    let mut decoded = match classification.encoding {
        GeometryEncoding::Point => decode_points(&node)?,
        GeometryEncoding::MultiPoint => decode_multipoints(&node)?,
        GeometryEncoding::LineString => decode_linestrings(&node)?,
        GeometryEncoding::MultiLineString => decode_multilinestrings(&node)?,
        GeometryEncoding::Polygon => decode_polygons(&node)?,
        GeometryEncoding::MultiPolygon => decode_multipolygons(&node)?,
        GeometryEncoding::Wkb => match classification.wkb_offset_width {
            crate::py::arrow::WkbOffsetWidth::View => decode_binary_view_wkb(&node)?,
            crate::py::arrow::WkbOffsetWidth::Int32 | crate::py::arrow::WkbOffsetWidth::Int64 => {
                decode_offset_wkb(&node)?
            },
        },
    };
    if !matches!(classification.encoding, GeometryEncoding::Wkb) {
        decoded.srids.clear();
        reserve(&mut decoded.srids, decoded.shapes.len())?;
        decoded.srids.resize(decoded.shapes.len(), None);
    }
    Ok(decoded)
}

fn decode_points(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let coordinates = NativeCoordinates::new(node)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        push(&mut shapes, coordinates.point_shape(row, row)?)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids: Vec::new(),
        missing,
    })
}

fn decode_multipoints(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    decode_coordseq_rows(node, GeometryEncoding::MultiPoint)
}

fn decode_linestrings(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    decode_coordseq_rows(node, GeometryEncoding::LineString)
}

fn decode_coordseq_rows(
    node: &NativeNode,
    encoding: GeometryEncoding,
) -> DecodeResult<NativeDecodedBatch> {
    let values = node
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow geometry list child is missing"))?;
    let coordinates = NativeCoordinates::new(values)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let range = list_range(node, row)?;
        let coords = coordinates.coordseq(range.start, range.end, row)?;
        let shape = if coords.is_empty() {
            Shape::typed_empty(
                match encoding {
                    GeometryEncoding::MultiPoint => GeometryKind::MultiPoint,
                    GeometryEncoding::LineString => GeometryKind::LineString,
                    _ => unreachable!("coordinate sequence decoder only handles two encodings"),
                },
                coordinates.axes(),
            )
        } else {
            match encoding {
                GeometryEncoding::MultiPoint => Shape::multi_point(coords.axes(), coords),
                GeometryEncoding::LineString => Shape::LineString(
                    LineSeq::try_new(coords).map_err(NativeArrowDecodeError::from)?,
                ),
                _ => unreachable!("coordinate sequence decoder only handles two encodings"),
            }
        };
        push(&mut shapes, shape)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids: Vec::new(),
        missing,
    })
}

fn decode_multilinestrings(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let lines = node.child(0).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow multilinestring child is missing")
    })?;
    let values = lines.child(0).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow line coordinate child is missing")
    })?;
    let coordinates = NativeCoordinates::new(values)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let line_range = list_range(node, row)?;
        let mut members = Vec::new();
        reserve(&mut members, line_range.len())?;
        for line_index in line_range {
            if lines.missing_at(line_index) {
                return Err(NativeArrowDecodeError::geoarrow(format!(
                    "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {line_index}"
                )));
            }
            let vertex_range = list_range(lines, line_index)?;
            if vertex_range.is_empty() {
                push(&mut members, MultiMember::Empty)?;
            } else {
                let coords = coordinates.coordseq(vertex_range.start, vertex_range.end, row)?;
                push(
                    &mut members,
                    MultiMember::Value(
                        LineSeq::try_new(coords).map_err(NativeArrowDecodeError::from)?,
                    ),
                )?;
            }
        }
        let shape = if members.is_empty() {
            Shape::typed_empty(GeometryKind::MultiLineString, coordinates.axes())
        } else {
            Shape::MultiLineString(
                MultiLineString::try_new(coordinates.axes(), members)
                    .map_err(NativeArrowDecodeError::from)?,
            )
        };
        push(&mut shapes, shape)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids: Vec::new(),
        missing,
    })
}

fn decode_polygons(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let rings = node
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow polygon ring child is missing"))?;
    let values = rings.child(0).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow ring coordinate child is missing")
    })?;
    let coordinates = NativeCoordinates::new(values)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let range = list_range(node, row)?;
        let shape = polygon_shape(&coordinates, rings, range, row)?;
        push(&mut shapes, shape)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids: Vec::new(),
        missing,
    })
}

fn decode_multipolygons(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let polygons = node
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow multipolygon child is missing"))?;
    let rings = polygons
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow polygon ring child is missing"))?;
    let values = rings.child(0).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow ring coordinate child is missing")
    })?;
    let coordinates = NativeCoordinates::new(values)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let polygon_range = list_range(node, row)?;
        let mut items = Vec::new();
        reserve(&mut items, polygon_range.len())?;
        for polygon_index in polygon_range {
            if polygons.missing_at(polygon_index) {
                return Err(NativeArrowDecodeError::geoarrow(format!(
                    "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {polygon_index}"
                )));
            }
            let ring_range = list_range(polygons, polygon_index)?;
            if ring_range.is_empty() {
                push(&mut items, MultiMember::Empty)?;
                continue;
            }
            for ring_index in ring_range.clone() {
                if rings.missing_at(ring_index) {
                    return Err(NativeArrowDecodeError::geoarrow(format!(
                        "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {ring_index}"
                    )));
                }
            }
            match polygon_shape(&coordinates, rings, ring_range, row)? {
                Shape::Polygon(polygon) => push(&mut items, MultiMember::Value(polygon))?,
                Shape::Empty(GeometryKind::Polygon, _) => push(&mut items, MultiMember::Empty)?,
                _ => {
                    return Err(NativeArrowDecodeError::geoarrow(
                        "Arrow multipolygon member decoded as an unexpected shape",
                    ));
                },
            }
        }
        let shape = if items.is_empty() {
            Shape::typed_empty(GeometryKind::MultiPolygon, coordinates.axes())
        } else {
            Shape::MultiPolygon(
                MultiPolygon::try_new(coordinates.axes(), items)
                    .map_err(NativeArrowDecodeError::from)?,
            )
        };
        push(&mut shapes, shape)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids: Vec::new(),
        missing,
    })
}

fn polygon_shape(
    coordinates: &NativeCoordinates,
    rings: &NativeNode,
    range: std::ops::Range<usize>,
    row: usize,
) -> DecodeResult<Shape> {
    if range.is_empty() {
        return Ok(Shape::typed_empty(
            GeometryKind::Polygon,
            coordinates.axes(),
        ));
    }
    let mut shell_and_holes = Vec::new();
    reserve(&mut shell_and_holes, range.len())?;
    for ring_index in range {
        if rings.missing_at(ring_index) {
            return Err(NativeArrowDecodeError::geoarrow(format!(
                "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {ring_index}"
            )));
        }
        let vertex_range = list_range(rings, ring_index)?;
        let coords = coordinates.coordseq(vertex_range.start, vertex_range.end, row)?;
        let ring = io::admit_closed_ring(coords).map_err(|error| {
            let message = error.to_string();
            if let crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::RingTooShort(n),
            ) = error.kind()
            {
                NativeArrowDecodeError::geoarrow(format!(
                    "Arrow polygon rings require at least three coordinates, got {n}"
                ))
            } else if message.contains("closed on all active ordinates") {
                NativeArrowDecodeError::geoarrow(
                    "Arrow polygon ring must be closed on all active ordinates",
                )
            } else {
                NativeArrowDecodeError::geoarrow(format!("Arrow polygon ring: {message}"))
            }
        })?;
        push(&mut shell_and_holes, ring)?;
    }
    let mut rings = shell_and_holes.into_iter();
    let shell = rings.next().expect("non-empty polygon range has a shell");
    let mut holes = Vec::new();
    reserve(&mut holes, rings.len())?;
    for ring in rings {
        push(&mut holes, ring)?;
    }
    Ok(Shape::Polygon(Polygon::new(shell, holes)?))
}

fn decode_offset_wkb(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let data = node
        .bytes_at(2)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow WKB data buffer is missing"))?;
    let mut shapes = Vec::new();
    let mut srids = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut srids, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let start = offset(node, row)?;
        let end = offset(node, row + 1)?;
        let bytes = data.get(start..end).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow WKB offsets are out of bounds")
        })?;
        let parsed = io::parse_wkb(bytes).map_err(NativeArrowDecodeError::from)?;
        push(&mut shapes, parsed.shape)?;
        push(&mut srids, parsed.srid)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids,
        missing,
    })
}

fn decode_binary_view_wkb(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let views = node
        .bytes_at(1)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow binary-view buffer is missing"))?;
    let mut shapes = Vec::new();
    let mut srids = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut srids, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            push(&mut missing, row)?;
            continue;
        }
        let start = row
            .checked_mul(16)
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow binary-view row overflows"))?;
        let view = views.get(start..start + 16).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow binary-view buffer is shorter than declared")
        })?;
        let length = i32::from_le_bytes(view[0..4].try_into().expect("descriptor length"));
        let length = usize::try_from(length).map_err(|_| {
            NativeArrowDecodeError::geoarrow("Arrow binary-view length is negative")
        })?;
        let bytes = if length <= 12 {
            view.get(4..4 + length).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view inline value is truncated")
            })?
        } else {
            let buffer_index = i32::from_le_bytes(view[8..12].try_into().expect("buffer index"));
            let buffer_index = usize::try_from(buffer_index).map_err(|_| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view buffer index is negative")
            })?;
            let byte_offset = i32::from_le_bytes(view[12..16].try_into().expect("byte offset"));
            let byte_offset = usize::try_from(byte_offset).map_err(|_| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view byte offset is negative")
            })?;
            let buffer_slot = buffer_index.checked_add(2).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view buffer index overflows")
            })?;
            let data = node.bytes_at(buffer_slot).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view data buffer is missing")
            })?;
            let end = byte_offset.checked_add(length).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view byte range overflows")
            })?;
            data.get(byte_offset..end).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view byte range is out of bounds")
            })?
        };
        let parsed = io::parse_wkb(bytes).map_err(NativeArrowDecodeError::from)?;
        push(&mut shapes, parsed.shape)?;
        push(&mut srids, parsed.srid)?;
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        shapes,
        srids,
        missing,
    })
}

fn offset(node: &NativeNode, index: usize) -> DecodeResult<usize> {
    node.offset_at(index).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow offsets buffer is shorter than declared")
    })
}

fn list_range(node: &NativeNode, index: usize) -> DecodeResult<std::ops::Range<usize>> {
    node.list_range(index).ok_or_else(|| {
        NativeArrowDecodeError::geoarrow("Arrow offsets are malformed or exceed the selected child")
    })
}

struct NativeCoordinates {
    x: Arc<Vec<f64>>,
    y: Arc<Vec<f64>>,
    z: Option<Arc<Vec<f64>>>,
    m: Option<Arc<Vec<f64>>>,
    validity: Option<Arc<Vec<bool>>>,
    axes: CoordinateAxes,
    full: OnceCell<CoordSeq>,
}

impl NativeCoordinates {
    fn new(node: &NativeNode) -> DecodeResult<Self> {
        match node.format {
            ArrowFormat::Struct => Self::separated(node),
            ArrowFormat::FixedSizeList(size) => Self::interleaved(node, size),
            _ => Err(NativeArrowDecodeError::geoarrow(
                "Arrow geometry coordinates must be a struct or fixed-size list",
            )),
        }
    }

    fn separated(node: &NativeNode) -> DecodeResult<Self> {
        let x_index = node
            .field_names
            .iter()
            .position(|name| name == "x")
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow point coordinates require x"))?;
        let y_index = node
            .field_names
            .iter()
            .position(|name| name == "y")
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow point coordinates require y"))?;
        let x_node = node.child(x_index).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow x coordinate child is missing")
        })?;
        let y_node = node.child(y_index).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow y coordinate child is missing")
        })?;
        let x = x_node
            .f64_values()
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow x values buffer is missing"))?;
        let y = y_node
            .f64_values()
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow y values buffer is missing"))?;
        if x.len() != node.length || y.len() != node.length {
            return Err(NativeArrowDecodeError::geoarrow(
                "Arrow coordinate columns have mismatched lengths",
            ));
        }
        let z_index = node.field_names.iter().position(|name| name == "z");
        let m_index = node.field_names.iter().position(|name| name == "m");
        let z = z_index
            .map(|index| {
                node.child(index)
                    .and_then(NativeNode::f64_values)
                    .ok_or_else(|| {
                        NativeArrowDecodeError::geoarrow("Arrow z values buffer is missing")
                    })
            })
            .transpose()?;
        let m = m_index
            .map(|index| {
                node.child(index)
                    .and_then(NativeNode::f64_values)
                    .ok_or_else(|| {
                        NativeArrowDecodeError::geoarrow("Arrow m values buffer is missing")
                    })
            })
            .transpose()?;
        if z.as_ref().is_some_and(|values| values.len() != node.length)
            || m.as_ref().is_some_and(|values| values.len() != node.length)
        {
            return Err(NativeArrowDecodeError::geoarrow(
                "Arrow coordinate columns have mismatched lengths",
            ));
        }
        let validity = combined_validity(node.length, |row| {
            node.missing_at(row)
                || x_node.missing_at(row)
                || y_node.missing_at(row)
                || z_index.is_some_and(|index| {
                    node.child(index).is_none_or(|child| child.missing_at(row))
                })
                || m_index.is_some_and(|index| {
                    node.child(index).is_none_or(|child| child.missing_at(row))
                })
        })?;
        Ok(Self {
            x: Arc::clone(x_node.f64_values().expect("checked above")),
            y: Arc::clone(y_node.f64_values().expect("checked above")),
            z: z.map(Arc::clone),
            m: m.map(Arc::clone),
            validity,
            axes: CoordinateAxes::new(HasZ(z_index.is_some()), HasM(m_index.is_some())),
            full: OnceCell::new(),
        })
    }

    fn interleaved(node: &NativeNode, size: usize) -> DecodeResult<Self> {
        if !(2..=4).contains(&size) {
            return Err(NativeArrowDecodeError::geoarrow(
                "geoarrow interleaved coordinates require fixed_size_list of length 2, 3, or 4",
            ));
        }
        let values_node = node.child(0).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow interleaved values child is missing")
        })?;
        let values = values_node.f64_values().ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow interleaved values buffer is missing")
        })?;
        let expected = node.length.checked_mul(size).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow interleaved coordinate span overflows")
        })?;
        if values.len() != expected {
            return Err(NativeArrowDecodeError::geoarrow(
                "Arrow interleaved coordinate buffer is shorter than declared",
            ));
        }
        let name = values_node.schema_name.as_str();
        let (has_z, has_m) = match (size, name) {
            (2, _) => (false, false),
            (3, "xym") => (false, true),
            (3, _) => (true, false),
            (4, _) => (true, true),
            _ => unreachable!("size is checked above"),
        };
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        let mut zs = has_z.then(Vec::new);
        let mut ms = has_m.then(Vec::new);
        reserve(&mut xs, node.length)?;
        reserve(&mut ys, node.length)?;
        if let Some(values) = zs.as_mut() {
            reserve(values, node.length)?;
        }
        if let Some(values) = ms.as_mut() {
            reserve(values, node.length)?;
        }
        for row in 0..node.length {
            let start = row * size;
            push(&mut xs, values[start])?;
            push(&mut ys, values[start + 1])?;
            if let Some(zs) = zs.as_mut() {
                push(zs, values[start + 2])?;
            }
            if let Some(ms) = ms.as_mut() {
                push(ms, values[start + if has_z { 3 } else { 2 }])?;
            }
        }
        let validity = combined_validity(node.length, |row| {
            node.missing_at(row)
                || (0..size).any(|offset| values_node.missing_at(row * size + offset))
        })?;
        Ok(Self {
            x: crate::py::arrow_c::try_arc_from_vec_native(xs)?,
            y: crate::py::arrow_c::try_arc_from_vec_native(ys)?,
            z: zs
                .map(crate::py::arrow_c::try_arc_from_vec_native)
                .transpose()?,
            m: ms
                .map(crate::py::arrow_c::try_arc_from_vec_native)
                .transpose()?,
            validity,
            axes: CoordinateAxes::new(HasZ(has_z), HasM(has_m)),
            full: OnceCell::new(),
        })
    }

    const fn axes(&self) -> CoordinateAxes {
        self.axes
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_ref()
            .and_then(|validity| validity.get(index))
            .copied()
            .unwrap_or(true)
    }

    fn point_shape(&self, index: usize, row: usize) -> DecodeResult<Shape> {
        if index >= self.x.len() || index >= self.y.len() {
            return Err(NativeArrowDecodeError::geoarrow(
                "Arrow coordinate buffer is shorter than declared array length",
            ));
        }
        if !self.is_valid(index) {
            return Err(NativeArrowDecodeError::geoarrow(format!(
                "Arrow geometry arrays require non-null geometry values; found null at index {row}"
            )));
        }
        if self.empty_point_sentinel(index) {
            return Ok(Shape::typed_empty(GeometryKind::Point, self.axes));
        }
        self.ensure_range_finite(index, index + 1)?;
        Ok(Shape::Point(
            Point::new_axes(
                self.x[index],
                self.y[index],
                ZOrdinate(self.z.as_ref().map(|values| values[index])),
                MOrdinate(self.m.as_ref().map(|values| values[index])),
            )
            .map_err(NativeArrowDecodeError::from)?,
        ))
    }

    fn empty_point_sentinel(&self, index: usize) -> bool {
        self.x[index].is_nan()
            && self.y[index].is_nan()
            && self.z.as_ref().is_none_or(|values| values[index].is_nan())
            && self.m.as_ref().is_none_or(|values| values[index].is_nan())
    }

    fn coordseq(&self, start: usize, end: usize, row: usize) -> DecodeResult<CoordSeq> {
        if end < start
            || end > self.x.len()
            || end > self.y.len()
            || self.z.as_ref().is_some_and(|values| end > values.len())
            || self.m.as_ref().is_some_and(|values| end > values.len())
        {
            return Err(NativeArrowDecodeError::geoarrow(
                "Arrow coordinate buffer is shorter than declared array length",
            ));
        }
        for index in start..end {
            if !self.is_valid(index) {
                return Err(NativeArrowDecodeError::geoarrow(format!(
                    "Arrow geometry arrays require non-null geometry values; found null at index {row}"
                )));
            }
        }
        self.ensure_range_finite(start, end)?;
        if self.full.get().is_none() {
            let x = crate::geometry::try_copy_slice(&self.x)?;
            let y = crate::geometry::try_copy_slice(&self.y)?;
            let z = self
                .z
                .as_ref()
                .map(|values| crate::geometry::try_copy_slice(values))
                .transpose()?;
            let m = self
                .m
                .as_ref()
                .map(|values| crate::geometry::try_copy_slice(values))
                .transpose()?;
            let full = CoordSeq::try_from_vecs(x, y, z, m).map_err(NativeArrowDecodeError::from)?;
            let _ = self.full.set(full);
        }
        let full = self.full.get().expect("coordinate sequence initialized");
        Ok(full.view(CoordWindow::from_range(start..end, full.len())))
    }

    fn ensure_range_finite(&self, start: usize, end: usize) -> DecodeResult<()> {
        let finite = column_all_finite(&self.x[start..end])
            && column_all_finite(&self.y[start..end])
            && self
                .z
                .as_ref()
                .is_none_or(|values| column_all_finite(&values[start..end]))
            && self
                .m
                .as_ref()
                .is_none_or(|values| column_all_finite(&values[start..end]));
        if finite {
            Ok(())
        } else {
            Err(NativeArrowDecodeError::geoarrow(
                "Arrow coordinates must be finite",
            ))
        }
    }
}

fn combined_validity(
    length: usize,
    missing: impl Fn(usize) -> bool,
) -> DecodeResult<Option<Arc<Vec<bool>>>> {
    let any = (0..length).any(&missing);
    if !any {
        return Ok(None);
    }
    let mut values = Vec::new();
    reserve(&mut values, length)?;
    for row in 0..length {
        push(&mut values, !missing(row))?;
    }
    Ok(Some(crate::py::arrow_c::try_arc_from_vec_native(values)?))
}

fn reserve<T>(values: &mut Vec<T>, capacity: usize) -> DecodeResult<()> {
    crate::boundary::try_reserve_vec(values, capacity).map_err(NativeArrowDecodeError::from)
}

fn push<T>(values: &mut Vec<T>, value: T) -> DecodeResult<()> {
    crate::boundary::try_push_vec(values, value).map_err(NativeArrowDecodeError::from)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pyo3::exceptions::PyMemoryError;
    use pyo3::prelude::Python;

    use super::super::{OwnedBuffer, OwnedBufferSlots};
    use super::{ArrowFormat, NativeCoordinates, NativeNode};

    #[test]
    fn coordinate_materialization_allocation_failure_maps_to_memory_error() {
        crate::test_support::initialize_python();
        let leaf = |values| NativeNode {
            length: 2,
            validity_offset: 0,
            n_buffers: 2,
            format: ArrowFormat::Float64,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![
                Some(OwnedBuffer::Bytes(Arc::new(Vec::new()))),
                Some(OwnedBuffer::F64(Arc::new(values))),
            ])),
            children: Arc::new(Vec::new()),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(Vec::new()),
            effective_missing: None,
        };
        let node = NativeNode {
            length: 2,
            validity_offset: 0,
            n_buffers: 1,
            format: ArrowFormat::Struct,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![Some(OwnedBuffer::Bytes(Arc::new(
                Vec::new(),
            )))])),
            children: Arc::new(vec![leaf(vec![1.0, 2.0]), leaf(vec![3.0, 4.0])]),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(vec![String::from("x"), String::from("y")]),
            effective_missing: None,
        };
        let coordinates = NativeCoordinates::new(&node).expect("struct coordinates");

        let result =
            crate::test_support::fail_allocations_after(0, || coordinates.coordseq(0, 2, 0));
        let error = match result {
            Ok(_) => panic!("coordinate copy allocation should fail"),
            Err(error) => error,
        };
        let error = error.into_py_err();
        Python::attach(|py| assert!(error.is_instance_of::<PyMemoryError>(py)));
    }
}
