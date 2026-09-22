//! Decode one fully-owned Arrow batch without Python-shaped array objects.
//!
//! Admission in `native.rs` owns every selected buffer before this module is
//! entered.  The decoder therefore works only with `NativeNode` accessors and
//! can run entirely inside `Python::detach`.

use std::sync::Arc;

use super::{ArrowFormat, NativeArrowBatch, NativeNode};
use crate::array::{GeometryArrayStorage, PackedColumnBuilder, PackedStorage, RowSelection};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordWindow, CoordinateAxes, CsrOffsetBuilder, GeometryKind,
    LineSeq, MColumn, MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring,
    Shape, SharedSlice, ZOrdinate, column_all_finite,
};
use crate::io;
use crate::py::arrow;
use crate::py::arrow::WkbOffsetWidth;
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

impl From<crate::error::ResourceError> for NativeArrowDecodeError {
    fn from(error: crate::error::ResourceError) -> Self {
        Self::Core(error.into())
    }
}

impl NativeArrowDecodeError {
    pub(crate) fn geoarrow(message: impl Into<String>) -> Self {
        Self::GeoArrow(message.into())
    }

    pub(crate) fn into_py_err(self) -> pyo3::PyErr {
        match self {
            Self::GeoArrow(message) => arrow::geoarrow_parse_error(message),
            Self::Core(error) => error.into(),
        }
    }
}

/// Native batch payload after Arrow admission. Packed payloads already use
/// canonical columnar storage; mixed payloads are retained only for geometry
/// encodings that the array storage cannot represent without changing their
/// semantics (multipart and WKB rows, or primitive rows that are not packable).
pub(crate) enum NativeDecodedStorage {
    Packed(GeometryArrayStorage),
    Mixed(Vec<Shape>),
}

impl NativeDecodedStorage {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Packed(storage) => storage.len(),
            Self::Mixed(shapes) => shapes.len(),
        }
    }

    /// Materialize a packed payload only when an explicit mixed fallback is
    /// required. Packed rows are already canonical and normally never take
    /// this route.
    pub(crate) fn into_shapes(self) -> DecodeResult<Vec<Shape>> {
        match self {
            Self::Mixed(shapes) => Ok(shapes),
            Self::Packed(storage) => {
                let mut shapes = Vec::new();
                reserve(&mut shapes, storage.len())?;
                for shape in storage.iter_shapes() {
                    shapes.push(shape?.into_owned());
                }
                Ok(shapes)
            },
        }
    }
}

pub(crate) struct NativeDecodedBatch {
    pub(crate) length: usize,
    pub(crate) storage: NativeDecodedStorage,
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
    let decoded = match classification.encoding {
        GeometryEncoding::Point => decode_points(&node)?,
        GeometryEncoding::MultiPoint => decode_multipoints(&node)?,
        GeometryEncoding::LineString => decode_linestrings(&node)?,
        GeometryEncoding::MultiLineString => decode_multilinestrings(&node)?,
        GeometryEncoding::Polygon => decode_polygons(&node)?,
        GeometryEncoding::MultiPolygon => decode_multipolygons(&node)?,
        GeometryEncoding::Wkb => match classification.wkb_offset_width {
            WkbOffsetWidth::View => decode_binary_view_wkb(&node)?,
            WkbOffsetWidth::Int32 | WkbOffsetWidth::Int64 => decode_offset_wkb(&node)?,
        },
    };
    Ok(decoded)
}

fn mixed_batch(length: usize, shapes: Vec<Shape>, missing: Vec<usize>) -> NativeDecodedBatch {
    NativeDecodedBatch {
        length,
        storage: NativeDecodedStorage::Mixed(shapes),
        srids: Vec::new(),
        missing,
    }
}

fn missing_rows(node: &NativeNode) -> DecodeResult<Vec<usize>> {
    if node.effective_missing.is_none() {
        return Ok(Vec::new());
    }
    let mut missing = Vec::new();
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            missing.push(row);
        }
    }
    Ok(missing)
}

fn decode_points(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let coordinates = NativeCoordinates::new(node)?;
    let missing = missing_rows(node)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            continue;
        }
        if !coordinates.point_packable(row, row)? {
            return decode_points_mixed(node);
        }
    }
    let coords = if missing.is_empty() {
        Arc::new(coordinates.coords().clone())
    } else {
        let mut builder = CoordSeqBuilder::try_with_capacity(
            coordinates.axes(),
            node.length.saturating_sub(missing.len()),
        )?;
        for row in 0..node.length {
            if !node.missing_at(row) {
                builder.push_at(coordinates.coords(), row)?;
            }
        }
        Arc::new(builder.finish()?)
    };
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Packed(GeometryArrayStorage::Packed(
            PackedStorage::Points {
                coords,
                row_map: RowSelection::Identity,
            },
        )),
        srids: Vec::new(),
        missing,
    })
}

fn decode_points_mixed(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let coordinates = NativeCoordinates::new(node)?;
    let mut shapes = Vec::new();
    let mut missing = Vec::new();
    reserve(&mut shapes, node.length)?;
    reserve(&mut missing, node.length)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            missing.push(row);
            continue;
        }
        shapes.push(coordinates.point_shape(row, row)?);
    }
    Ok(mixed_batch(node.length, shapes, missing))
}

fn decode_multipoints(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    decode_coordseq_rows(node, GeometryEncoding::MultiPoint)
}

fn decode_linestrings(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
    let values = node
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow geometry list child is missing"))?;
    let coordinates = NativeCoordinates::new(values)?;
    let missing = missing_rows(node)?;
    let mut total_vertices = 0_usize;
    for row in 0..node.length {
        if node.missing_at(row) {
            continue;
        }
        let range = list_range(node, row)?;
        if range.is_empty() {
            return decode_coordseq_rows(node, GeometryEncoding::LineString);
        }
        if range.len() < 2 {
            // Let the established shape decoder retain the public geometry
            // error for a one-coordinate line rather than turning a
            // structural non-packability decision into a different error.
            return decode_coordseq_rows(node, GeometryEncoding::LineString);
        }
        coordinates.ensure_range_valid(range.start, range.end, row)?;
        total_vertices = total_vertices
            .checked_add(range.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    if total_vertices > i32::MAX as usize {
        return decode_coordseq_rows(node, GeometryEncoding::LineString);
    }
    let storage = packed_lines_storage(node, &coordinates, &missing, total_vertices)?;
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Packed(storage),
        srids: Vec::new(),
        missing,
    })
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
            missing.push(row);
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
        shapes.push(shape);
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Mixed(shapes),
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
            missing.push(row);
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
                members.push(MultiMember::Empty);
            } else {
                let coords = coordinates.coordseq(vertex_range.start, vertex_range.end, row)?;
                members.push(MultiMember::Value(
                    LineSeq::try_new(coords).map_err(NativeArrowDecodeError::from)?,
                ));
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
        shapes.push(shape);
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Mixed(shapes),
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
    let missing = missing_rows(node)?;
    let mut needs_rebuild = !missing.is_empty();
    let mut total_vertices = 0_usize;
    let mut ring_count = 0_usize;
    for row in 0..node.length {
        if node.missing_at(row) {
            continue;
        }
        let range = list_range(node, row)?;
        if range.is_empty() {
            return decode_polygons_mixed(node);
        }
        for ring_index in range {
            if rings.missing_at(ring_index) {
                return Err(NativeArrowDecodeError::geoarrow(format!(
                    "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {ring_index}"
                )));
            }
            let vertex_range = list_range(rings, ring_index)?;
            let open = coordinates.ring_admission(vertex_range.clone(), row)?;
            total_vertices = total_vertices
                .checked_add(vertex_range.len())
                .and_then(|total| total.checked_add(usize::from(open)))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ring_count = ring_count
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            needs_rebuild |= open;
        }
    }
    // Packed CSR stores both vertex and ring ends as i32.  This is a
    // structural packed-layout limit; preserve the established mixed fallback
    // instead of treating it as an allocation failure.
    if total_vertices > i32::MAX as usize || ring_count > i32::MAX as usize {
        return decode_polygons_mixed(node);
    }
    let storage = packed_polygon_storage(
        node,
        &coordinates,
        &missing,
        needs_rebuild,
        total_vertices,
        ring_count,
    )?;
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Packed(storage),
        srids: Vec::new(),
        missing,
    })
}

fn decode_polygons_mixed(node: &NativeNode) -> DecodeResult<NativeDecodedBatch> {
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
            missing.push(row);
            continue;
        }
        let range = list_range(node, row)?;
        let shape = polygon_shape(&coordinates, rings, range, row)?;
        shapes.push(shape);
    }
    Ok(mixed_batch(node.length, shapes, missing))
}

fn packed_lines_storage(
    node: &NativeNode,
    coordinates: &NativeCoordinates,
    missing: &[usize],
    total_vertices: usize,
) -> DecodeResult<GeometryArrayStorage> {
    if missing.is_empty() {
        let mut offsets = CsrOffsetBuilder::with_capacity(node.length)?;
        for row in 0..node.length {
            let range = list_range(node, row)?;
            offsets.push_end(range.end, i32::MAX as usize)?;
        }
        return Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::new(coordinates.coords().clone()),
            offsets: offsets.finish(coordinates.coords().len())?,
            row_map: RowSelection::Identity,
        }));
    }

    let mut builder = PackedColumnBuilder::try_like(coordinates.coords(), total_vertices)?;
    for row in 0..node.length {
        if node.missing_at(row) {
            continue;
        }
        let range = list_range(node, row)?;
        builder.push_window(coordinates.coords(), range)?;
    }
    let (coords, offsets) = builder.finish(total_vertices)?;
    Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::new(coords),
        offsets,
        row_map: RowSelection::Identity,
    }))
}

fn packed_polygon_storage(
    node: &NativeNode,
    coordinates: &NativeCoordinates,
    missing: &[usize],
    needs_rebuild: bool,
    total_vertices: usize,
    ring_count: usize,
) -> DecodeResult<GeometryArrayStorage> {
    let present_rows = node.length - missing.len();
    if missing.is_empty() && !needs_rebuild {
        let mut ring_offsets = CsrOffsetBuilder::with_capacity(ring_count)?;
        let mut polygon_offsets = CsrOffsetBuilder::with_capacity(present_rows)?;
        let mut rings = 0_usize;
        for row in 0..node.length {
            let range = list_range(node, row)?;
            for ring_index in range {
                let vertex_range = list_range(
                    node.child(0).expect("polygon ring child was checked"),
                    ring_index,
                )?;
                ring_offsets.push_end(vertex_range.end, i32::MAX as usize)?;
                rings += 1;
            }
            polygon_offsets.push_end(rings, i32::MAX as usize)?;
        }
        return Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::new(coordinates.coords().clone()),
            ring_offsets: ring_offsets
                .finish(coordinates.coords().len())?
                .cast_level(),
            polygon_offsets: polygon_offsets.finish(rings)?.cast_level(),
            row_map: RowSelection::Identity,
        }));
    }

    let rings_node = node
        .child(0)
        .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow polygon ring child is missing"))?;
    let mut builder = CoordSeqBuilder::try_with_capacity(coordinates.axes(), total_vertices)?;
    let mut ring_offsets = CsrOffsetBuilder::with_capacity(ring_count)?;
    let mut polygon_offsets = CsrOffsetBuilder::with_capacity(present_rows)?;
    let mut rings = 0_usize;
    for row in 0..node.length {
        if node.missing_at(row) {
            continue;
        }
        let range = list_range(node, row)?;
        for ring_index in range {
            let vertex_range = list_range(rings_node, ring_index)?;
            let open = coordinates.ring_admission(vertex_range.clone(), row)?;
            builder.extend_window(coordinates.coords(), vertex_range.clone())?;
            if open {
                builder.push_at(coordinates.coords(), vertex_range.start)?;
            }
            ring_offsets.push_end(builder.len(), i32::MAX as usize)?;
            rings += 1;
        }
        polygon_offsets.push_end(rings, i32::MAX as usize)?;
    }
    let coords = builder.finish()?;
    let coord_len = coords.len();
    Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::new(coords),
        ring_offsets: ring_offsets.finish(coord_len)?.cast_level(),
        polygon_offsets: polygon_offsets.finish(rings)?.cast_level(),
        row_map: RowSelection::Identity,
    }))
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
            missing.push(row);
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
                items.push(MultiMember::Empty);
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
                Shape::Polygon(polygon) => items.push(MultiMember::Value(polygon)),
                Shape::Empty(GeometryKind::Polygon, _) => items.push(MultiMember::Empty),
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
        shapes.push(shape);
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Mixed(shapes),
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
        let ring = io::admit_closed_ring(coords).map_err(arrow_ring_error)?;
        shell_and_holes.push(ring);
    }
    let mut rings = shell_and_holes.into_iter();
    let shell = rings.next().expect("non-empty polygon range has a shell");
    let mut holes = Vec::new();
    reserve(&mut holes, rings.len())?;
    for ring in rings {
        holes.push(ring);
    }
    Ok(Shape::Polygon(Polygon::new(shell, holes)))
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
            missing.push(row);
            continue;
        }
        let start = offset(node, row)?;
        let end = offset(node, row + 1)?;
        let bytes = data.get(start..end).ok_or_else(|| {
            NativeArrowDecodeError::geoarrow("Arrow WKB offsets are out of bounds")
        })?;
        let parsed = io::parse_wkb(bytes).map_err(NativeArrowDecodeError::from)?;
        shapes.push(parsed.shape);
        srids.push(parsed.srid);
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Mixed(shapes),
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
            missing.push(row);
            continue;
        }
        let start = row
            .checked_mul(16)
            .ok_or_else(|| NativeArrowDecodeError::geoarrow("Arrow binary-view row overflows"))?;
        // The Arrow binary-view descriptor is a fixed 16-byte record; taking it
        // as an array lets every field below be a constant read of a known size.
        let view = views
            .get(start..)
            .and_then(<[u8]>::first_chunk::<16>)
            .ok_or_else(|| {
                NativeArrowDecodeError::geoarrow(
                    "Arrow binary-view buffer is shorter than declared",
                )
            })?;
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        let length = usize::try_from(length).map_err(|_| {
            NativeArrowDecodeError::geoarrow("Arrow binary-view length is negative")
        })?;
        let bytes = if length <= 12 {
            view.get(4..4 + length).ok_or_else(|| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view inline value is truncated")
            })?
        } else {
            let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
            let buffer_index = usize::try_from(buffer_index).map_err(|_| {
                NativeArrowDecodeError::geoarrow("Arrow binary-view buffer index is negative")
            })?;
            let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
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
        shapes.push(parsed.shape);
        srids.push(parsed.srid);
    }
    Ok(NativeDecodedBatch {
        length: node.length,
        storage: NativeDecodedStorage::Mixed(shapes),
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
    coords: CoordSeq,
    validity: Option<crate::array::ValidityMask>,
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
        let coords = CoordSeq::from_shared_columns(
            SharedSlice::from_arc_vec(Arc::clone(x_node.f64_values().expect("checked above"))),
            SharedSlice::from_arc_vec(Arc::clone(y_node.f64_values().expect("checked above"))),
            z.as_ref()
                .map(|values| SharedSlice::from_arc_vec(Arc::clone(values))),
            m.as_ref()
                .map(|values| MColumn::Shared(SharedSlice::from_arc_vec(Arc::clone(values)))),
        )
        .map_err(NativeArrowDecodeError::from)?;
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
        Ok(Self { coords, validity })
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
            xs.push(values[start]);
            ys.push(values[start + 1]);
            if let Some(zs) = zs.as_mut() {
                zs.push(values[start + 2]);
            }
            if let Some(ms) = ms.as_mut() {
                ms.push(values[start + if has_z { 3 } else { 2 }]);
            }
        }
        let validity = combined_validity(node.length, |row| {
            node.missing_at(row)
                || (0..size).any(|offset| values_node.missing_at(row * size + offset))
        })?;
        let coords = CoordSeq::from_shared_columns(
            SharedSlice::from_vec(xs),
            SharedSlice::from_vec(ys),
            zs.map(SharedSlice::from_vec),
            ms.map(|values| MColumn::Shared(SharedSlice::from_vec(values))),
        )
        .map_err(NativeArrowDecodeError::from)?;
        Ok(Self { coords, validity })
    }

    const fn axes(&self) -> CoordinateAxes {
        self.coords.axes()
    }

    const fn coords(&self) -> &CoordSeq {
        &self.coords
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_ref()
            .is_none_or(|validity| validity.is_valid(index))
    }

    fn point_shape(&self, index: usize, row: usize) -> DecodeResult<Shape> {
        if index >= self.coords.len() {
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
            return Ok(Shape::typed_empty(GeometryKind::Point, self.axes()));
        }
        self.ensure_range_finite(index, index + 1)?;
        Ok(Shape::Point(
            Point::new_axes(
                self.coords.xs()[index],
                self.coords.ys()[index],
                ZOrdinate(self.coords.zs().map(|values| values[index])),
                MOrdinate(self.coords.ms().map(|values| values[index])),
            )
            .map_err(NativeArrowDecodeError::from)?,
        ))
    }

    fn point_packable(&self, index: usize, row: usize) -> DecodeResult<bool> {
        if index >= self.coords.len() {
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
            return Ok(false);
        }
        self.ensure_range_finite(index, index + 1)?;
        Ok(true)
    }

    fn empty_point_sentinel(&self, index: usize) -> bool {
        let point = self.coords.point_at(index);
        point.x.is_nan()
            && point.y.is_nan()
            && point.z().is_none_or(f64::is_nan)
            && point.m().is_none_or(f64::is_nan)
    }

    fn coordseq(&self, start: usize, end: usize, row: usize) -> DecodeResult<CoordSeq> {
        if end < start || end > self.coords.len() {
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
        Ok(self
            .coords
            .view(CoordWindow::from_range(start..end, self.coords.len())))
    }

    fn ensure_range_valid(&self, start: usize, end: usize, row: usize) -> DecodeResult<()> {
        if end < start || end > self.coords.len() {
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
        self.ensure_range_finite(start, end)
    }

    /// Validate one untrusted GeoArrow ring and report whether it needs the
    /// boundary vertex appended by the shared ring-admission policy.
    fn ring_admission(&self, range: std::ops::Range<usize>, row: usize) -> DecodeResult<bool> {
        self.ensure_range_valid(range.start, range.end, row)?;
        let coords = self
            .coords
            .view(CoordWindow::from_range(range, self.coords.len()));
        Ring::closure_requires_append(&coords).map_err(arrow_ring_error)
    }

    fn ensure_range_finite(&self, start: usize, end: usize) -> DecodeResult<()> {
        let finite = column_all_finite(&self.coords.xs()[start..end])
            && column_all_finite(&self.coords.ys()[start..end])
            && self
                .coords
                .zs()
                .is_none_or(|values| column_all_finite(&values[start..end]))
            && self
                .coords
                .ms()
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

fn arrow_ring_error(error: crate::error::Error) -> NativeArrowDecodeError {
    let message = error.to_string();
    if let crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::RingTooShort(n)) =
        error.kind()
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
}

fn combined_validity(
    length: usize,
    missing: impl Fn(usize) -> bool,
) -> DecodeResult<Option<crate::array::ValidityMask>> {
    crate::array::ValidityMask::from_missing_iter(length, (0..length).map(missing))
        .map_err(NativeArrowDecodeError::from)
}

fn reserve<T>(values: &mut Vec<T>, capacity: usize) -> DecodeResult<()> {
    let element_size = std::mem::size_of::<T>();
    let max_capacity = (isize::MAX as usize)
        .checked_div(element_size)
        .unwrap_or(usize::MAX);
    if capacity > max_capacity {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    values.reserve(capacity);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::{
        NativeArrowBatch, NativeArrowClassification, OwnedBuffer, OwnedBufferSlots,
    };
    use super::{ArrowFormat, NativeDecodedStorage, NativeNode};
    use crate::py::arrow::WkbOffsetWidth;
    use crate::py::geoarrow::GeometryEncoding;

    fn f64_leaf(name: &str, values: Vec<f64>) -> NativeNode {
        let length = values.len();
        NativeNode {
            length,
            validity_offset: 0,
            n_buffers: 2,
            format: ArrowFormat::Float64,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![
                Some(OwnedBuffer::Bytes(Arc::new(Vec::new()))),
                Some(OwnedBuffer::F64(Arc::new(values))),
            ])),
            children: Arc::new(Vec::new()),
            schema_name: Arc::new(name.to_owned()),
            field_names: Arc::new(Vec::new()),
            effective_missing: None,
        }
    }

    fn coordinate_struct(xs: Vec<f64>, ys: Vec<f64>) -> NativeNode {
        assert_eq!(xs.len(), ys.len());
        NativeNode {
            length: xs.len(),
            validity_offset: 0,
            n_buffers: 1,
            format: ArrowFormat::Struct,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![Some(OwnedBuffer::Bytes(Arc::new(
                Vec::new(),
            )))])),
            children: Arc::new(vec![f64_leaf("x", xs), f64_leaf("y", ys)]),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(vec![String::from("x"), String::from("y")]),
            effective_missing: None,
        }
    }

    fn list_node(offsets: &[i32], child: NativeNode, missing: Option<Vec<bool>>) -> NativeNode {
        let mut bytes = Vec::new();
        for offset in offsets {
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        NativeNode {
            length: offsets.len() - 1,
            validity_offset: 0,
            n_buffers: 2,
            format: ArrowFormat::List,
            buffers: OwnedBufferSlots::Dense(Arc::new(vec![
                Some(OwnedBuffer::Bytes(Arc::new(Vec::new()))),
                Some(OwnedBuffer::Bytes(Arc::new(bytes))),
            ])),
            children: Arc::new(vec![child]),
            schema_name: Arc::new(String::new()),
            field_names: Arc::new(Vec::new()),
            effective_missing: missing.and_then(|missing| {
                crate::array::ValidityMask::from_vec(missing.len(), missing).unwrap()
            }),
        }
    }

    fn classification(encoding: GeometryEncoding) -> NativeArrowClassification {
        NativeArrowClassification {
            encoding,
            wkb_offset_width: WkbOffsetWidth::Int32,
            crs: None,
            epoch: None,
        }
    }

    #[test]
    fn direct_line_decode_builds_present_only_csr_for_hidden_parent_rows() {
        let coordinates = coordinate_struct(vec![f64::NAN, f64::NAN, 2.0, 3.0, 4.0, 5.0], vec![
            f64::NAN,
            f64::NAN,
            2.0,
            3.0,
            4.0,
            5.0,
        ]);
        let node = list_node(&[0, 2, 4, 6], coordinates, Some(vec![true, false, false]));
        let decoded = NativeArrowBatch {
            node,
            classification: classification(GeometryEncoding::LineString),
        }
        .decode()
        .expect("direct linestring decode");
        let NativeDecodedStorage::Packed(crate::array::GeometryArrayStorage::Packed(
            crate::array::PackedStorage::Lines {
                coords, offsets, ..
            },
        )) = decoded.storage
        else {
            panic!("uniform linestring input must decode directly to packed storage")
        };
        assert_eq!(decoded.missing, vec![0]);
        assert_eq!(coords.xs(), &[2.0, 3.0, 4.0, 5.0]);
        assert_eq!(offsets.as_slice(), &[0, 2, 4]);
    }

    #[test]
    fn direct_polygon_decode_builds_csr_columns_without_shape_rows() {
        let coordinates =
            coordinate_struct(vec![0.0, 1.0, 1.0, 0.0, 0.0], vec![0.0, 0.0, 1.0, 1.0, 0.0]);
        let rings = list_node(&[0, 5], coordinates, None);
        let node = list_node(&[0, 1], rings, None);
        let decoded = NativeArrowBatch {
            node,
            classification: classification(GeometryEncoding::Polygon),
        }
        .decode()
        .expect("direct polygon decode");
        let NativeDecodedStorage::Packed(crate::array::GeometryArrayStorage::Packed(
            crate::array::PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                ..
            },
        )) = decoded.storage
        else {
            panic!("uniform polygon input must decode directly to packed storage")
        };
        assert!(decoded.missing.is_empty());
        assert_eq!(coords.xs(), &[0.0, 1.0, 1.0, 0.0, 0.0]);
        assert_eq!(ring_offsets.as_slice(), &[0, 5]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 1]);
    }
}
