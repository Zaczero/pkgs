#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::{Py, PyAny, PyResult, Python};
use pyo3::types::{PyAnyMethods as _, PyBytes};

use crate::array::ValidityMask;
use crate::geometry::{CoordSeq, CoordinateAxes, Polygon, Shape};
use crate::py::arrow::{
    ArrowCoordinateBuffers, accumulate_geometry_axes, columns_to_pybytes, gometry_arrow_module,
    push_i32_le,
};
use crate::{GeometryArrayStorage, io};

pub(crate) fn validity_bitmap_from_mask(mask: &ValidityMask) -> Vec<u8> {
    let mut validity = vec![0; mask.len().div_ceil(8)];
    for row in 0..mask.len() {
        if mask.is_valid(row) {
            validity[row / 8] |= 1 << (row % 8);
        }
    }
    validity
}

pub(crate) fn push_arrow_polygon(
    polygon: &Polygon,
    ring_offsets: &mut Vec<u8>,
    coordinate_count: &mut usize,
    coordinates: &mut ArrowCoordinateBuffers,
) -> PyResult<()> {
    *coordinate_count = coordinate_count
        .checked_add(polygon.shell.len())
        .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))?;
    push_i32_le(ring_offsets, *coordinate_count)?;
    coordinates.push_points(&polygon.shell)?;
    for hole in polygon.holes.iter() {
        *coordinate_count = coordinate_count
            .checked_add(hole.len())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))?;
        push_i32_le(ring_offsets, *coordinate_count)?;
        coordinates.push_points(hole)?;
    }
    Ok(())
}

/// Packed point-array export straight from the shared columns — no per-row
/// `PyGeometry` materialization, no per-point pushes.
pub(crate) fn packed_points_to_arrow(
    py: Python<'_>,
    seq: &CoordSeq,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    // Zero-copy on little-endian hosts (the Arrow buffer contract):
    // `_Float64Buffer` exposes the shared column storage straight through
    // PEP 3118, so pyarrow wraps it without ANY byte copy. Big-endian
    // hosts keep the byte-swapping PyBytes path.
    if cfg!(target_endian = "little") {
        let columns = seq.column_arcs();
        let window = columns.window.clone();
        let column = |arc: std::sync::Arc<[f64]>, window: std::ops::Range<usize>| {
            crate::py::vectors::Float64Buffer::view(arc, window)
        };
        let m_parts = columns.m_parts();
        let (xs, ys) = (
            column(columns.xs, window.clone())?,
            column(columns.ys, window.clone())?,
        );
        let (zs, ms) = (
            columns
                .zs
                .map(|arc| column(arc, window.clone()))
                .transpose()?,
            m_parts
                .map(|(arc, window)| column(arc, window))
                .transpose()?,
        );
        return Ok(gometry_arrow_module(py)?
            .call_method1(
                pyo3::intern!(py, "to_arrow_point"),
                (xs, ys, zs, ms, crs, epoch),
            )?
            .unbind());
    }
    let (xs, ys, zs, ms) = columns_to_pybytes(py, seq.xs(), seq.ys(), seq.zs(), seq.ms())?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_point"),
            (xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

/// Packed `Lines` storage exports its CSR directly: the `GeoArrow`
/// linestring layout IS offsets + coordinate children, so the coordinate
/// buffers ride zero-copy `_Float64Buffer` views (like packed points) and
/// only the small i32 offset buffer is built fresh (our row offsets are
/// `u32`).
/// Packed `Polygons` storage exports its two-level CSR directly — the
/// `GeoArrow` polygon layout is polygon offsets + ring offsets + coordinates.
pub(crate) fn packed_polygons_to_arrow(
    py: Python<'_>,
    coords: &CoordSeq,
    ring_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
    polygon_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    if cfg!(target_endian = "little") {
        let polygon_offset_bytes =
            crate::py::vectors::Int32Buffer::new(polygon_offsets.as_arc_i32());
        let ring_offset_bytes = crate::py::vectors::Int32Buffer::new(ring_offsets.as_arc_i32());
        let columns = coords.column_arcs();
        let window = columns.window.clone();
        let column = |arc: std::sync::Arc<[f64]>, window: std::ops::Range<usize>| {
            crate::py::vectors::Float64Buffer::view(arc, window)
        };
        let m_parts = columns.m_parts();
        let (xs, ys) = (
            column(columns.xs, window.clone())?,
            column(columns.ys, window.clone())?,
        );
        let (zs, ms) = (
            columns
                .zs
                .map(|arc| column(arc, window.clone()))
                .transpose()?,
            m_parts
                .map(|(arc, window)| column(arc, window))
                .transpose()?,
        );
        return Ok(gometry_arrow_module(py)?
            .call_method1(
                pyo3::intern!(py, "to_arrow_polygon"),
                (
                    polygon_offset_bytes,
                    ring_offset_bytes,
                    xs,
                    ys,
                    zs,
                    ms,
                    crs,
                    epoch,
                ),
            )?
            .unbind());
    }
    let polygon_capacity = polygon_offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| PyOverflowError::new_err("Arrow offset buffer size overflows usize"))?;
    let mut polygon_bytes: Vec<u8> = crate::try_vec_with_capacity(polygon_capacity)?;
    for &offset in polygon_offsets.iter() {
        polygon_bytes.extend_from_slice(&offset.to_le_bytes());
    }
    let ring_capacity = ring_offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| PyOverflowError::new_err("Arrow offset buffer size overflows usize"))?;
    let mut ring_bytes: Vec<u8> = crate::try_vec_with_capacity(ring_capacity)?;
    for &offset in ring_offsets.iter() {
        ring_bytes.extend_from_slice(&offset.to_le_bytes());
    }
    let polygon_offsets = PyBytes::new(py, &polygon_bytes);
    let ring_offsets = PyBytes::new(py, &ring_bytes);
    let (xs, ys, zs, ms) =
        columns_to_pybytes(py, coords.xs(), coords.ys(), coords.zs(), coords.ms())?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_polygon"),
            (polygon_offsets, ring_offsets, xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

pub(crate) fn packed_lines_to_arrow(
    py: Python<'_>,
    coords: &CoordSeq,
    offsets: &crate::geometry::CsrOffsetColumn,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    if cfg!(target_endian = "little") {
        // Offsets are stored as i32 (the GeoArrow list-offset type), so
        // the WHOLE export is zero-copy: offsets ride their own buffer
        // holder, coordinates ride `_Float64Buffer` views.
        let offset_bytes = crate::py::vectors::Int32Buffer::new(offsets.as_arc_i32());
        let columns = coords.column_arcs();
        let window = columns.window.clone();
        let column = |arc: std::sync::Arc<[f64]>, window: std::ops::Range<usize>| {
            crate::py::vectors::Float64Buffer::view(arc, window)
        };
        let m_parts = columns.m_parts();
        let (xs, ys) = (
            column(columns.xs, window.clone())?,
            column(columns.ys, window.clone())?,
        );
        let (zs, ms) = (
            columns
                .zs
                .map(|arc| column(arc, window.clone()))
                .transpose()?,
            m_parts
                .map(|(arc, window)| column(arc, window))
                .transpose()?,
        );
        return Ok(gometry_arrow_module(py)?
            .call_method1(
                pyo3::intern!(py, "to_arrow_linestring"),
                (offset_bytes, xs, ys, zs, ms, crs, epoch),
            )?
            .unbind());
    }
    let offset_capacity = offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| PyOverflowError::new_err("Arrow offset buffer size overflows usize"))?;
    let mut offset_bytes: Vec<u8> = crate::try_vec_with_capacity(offset_capacity)?;
    for &offset in offsets.iter() {
        offset_bytes.extend_from_slice(&offset.to_le_bytes());
    }
    let offset_bytes = PyBytes::new(py, &offset_bytes);
    let (xs, ys, zs, ms) =
        columns_to_pybytes(py, coords.xs(), coords.ys(), coords.zs(), coords.ms())?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_linestring"),
            (offset_bytes, xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

/// Export raw shapes (Mixed storage) without materializing `PyGeometry` rows.
pub(crate) fn shapes_to_arrow(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    if let Some(output) = shapes_to_packed_arrow(py, shapes, crs, epoch)? {
        return Ok(output);
    }
    shapes_to_wkb_arrow(py, shapes, crs, epoch)
}

/// Homogeneous-shape packed GeoArrow export from Mixed storage.
fn shapes_to_packed_arrow(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<Py<PyAny>>> {
    enum PackedKind {
        Point,
        MultiPoint,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
    }
    let Some(first) = shapes.first() else {
        let empty = CoordSeq::from_points(&[]);
        return packed_points_to_arrow(py, &empty, crs, epoch).map(Some);
    };
    let kind = match first {
        Shape::Point(_) => PackedKind::Point,
        Shape::MultiPoint(_) => PackedKind::MultiPoint,
        Shape::LineString(_) => PackedKind::LineString,
        Shape::MultiLineString(_) => PackedKind::MultiLineString,
        Shape::Polygon(_) => PackedKind::Polygon,
        Shape::MultiPolygon(_) => PackedKind::MultiPolygon,
        Shape::GeometryCollection(_) | Shape::Empty(..) => return Ok(None),
    };
    if shapes.iter().any(|shape| match shape {
        Shape::MultiPoint(points) => points.has_empty_members(),
        Shape::MultiLineString(lines) => lines.has_empty_members(),
        Shape::MultiPolygon(polygons) => polygons.has_empty_members(),
        _ => false,
    }) {
        return Ok(None);
    }
    let mut axes = None;
    for shape in shapes {
        let same_kind = matches!(
            (&kind, shape),
            (PackedKind::Point, Shape::Point(_))
                | (PackedKind::MultiPoint, Shape::MultiPoint(_))
                | (PackedKind::LineString, Shape::LineString(_))
                | (PackedKind::MultiLineString, Shape::MultiLineString(_))
                | (PackedKind::Polygon, Shape::Polygon(_))
                | (PackedKind::MultiPolygon, Shape::MultiPolygon(_))
        );
        if !same_kind || !accumulate_geometry_axes(shape, &mut axes) {
            return Ok(None);
        }
    }
    let axes = axes.unwrap_or(CoordinateAxes::XY);
    // Build GeoArrow buffers directly from the array-owned shapes.
    match kind {
        PackedKind::Point => {
            let mut points = crate::try_vec_with_capacity(shapes.len())?;
            for shape in shapes {
                let Shape::Point(point) = shape else {
                    unreachable!()
                };
                crate::try_push(&mut points, *point)?;
            }
            let seq = CoordSeq::from_points(&points);
            packed_points_to_arrow(py, &seq, crs, epoch).map(Some)
        },
        PackedKind::LineString => export_arrow_coordseq_shapes(
            py,
            shapes,
            crs,
            epoch,
            axes,
            |shape| match shape {
                Shape::LineString(points) => points,
                _ => unreachable!(),
            },
            "to_arrow_linestring",
        )
        .map(Some),
        PackedKind::MultiPoint => export_arrow_coordseq_shapes(
            py,
            shapes,
            crs,
            epoch,
            axes,
            |shape| match shape {
                Shape::MultiPoint(points) => points,
                _ => unreachable!(),
            },
            "to_arrow_multipoint",
        )
        .map(Some),
        PackedKind::MultiLineString => {
            export_arrow_multilinestring_shapes(py, shapes, crs, epoch, axes).map(Some)
        },
        PackedKind::Polygon => export_arrow_polygon_shapes(py, shapes, crs, epoch, axes).map(Some),
        PackedKind::MultiPolygon => {
            export_arrow_multipolygon_shapes(py, shapes, crs, epoch, axes).map(Some)
        },
    }
}

fn export_arrow_multilinestring_shapes(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut geometry_offsets = Vec::new();
    let mut line_offsets = Vec::new();
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(shape.coord_count())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))
    })?;
    let mut coordinates =
        ArrowCoordinateBuffers::with_capacity(axes, total).map_err(pyo3::PyErr::from)?;
    let mut line_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut geometry_offsets, line_count)?;
    push_i32_le(&mut line_offsets, coordinate_count)?;
    for shape in shapes {
        let Shape::MultiLineString(lines) = shape else {
            unreachable!()
        };
        line_count = line_count
            .checked_add(lines.len())
            .ok_or_else(|| PyOverflowError::new_err("Arrow line count overflows usize"))?;
        push_i32_le(&mut geometry_offsets, line_count)?;
        for line in lines {
            coordinate_count = coordinate_count.checked_add(line.len()).ok_or_else(|| {
                PyOverflowError::new_err("Arrow coordinate count overflows usize")
            })?;
            push_i32_le(&mut line_offsets, coordinate_count)?;
            coordinates.push_points(line)?;
        }
    }
    let geometry_offsets = PyBytes::new(py, &geometry_offsets);
    let line_offsets = PyBytes::new(py, &line_offsets);
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_multilinestring"),
            (geometry_offsets, line_offsets, xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

fn export_arrow_polygon_shapes(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut polygon_offsets = Vec::new();
    let mut ring_offsets = Vec::new();
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(shape.coord_count())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))
    })?;
    let mut coordinates =
        ArrowCoordinateBuffers::with_capacity(axes, total).map_err(pyo3::PyErr::from)?;
    let mut ring_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut polygon_offsets, ring_count)?;
    push_i32_le(&mut ring_offsets, coordinate_count)?;
    for shape in shapes {
        let Shape::Polygon(polygon) = shape else {
            unreachable!()
        };
        let ring_total = polygon
            .holes
            .len()
            .checked_add(1)
            .ok_or_else(|| PyOverflowError::new_err("Arrow ring count overflows usize"))?;
        ring_count = ring_count
            .checked_add(ring_total)
            .ok_or_else(|| PyOverflowError::new_err("Arrow ring count overflows usize"))?;
        push_i32_le(&mut polygon_offsets, ring_count)?;
        push_arrow_polygon(
            polygon,
            &mut ring_offsets,
            &mut coordinate_count,
            &mut coordinates,
        )?;
    }
    let polygon_offsets = PyBytes::new(py, &polygon_offsets);
    let ring_offsets = PyBytes::new(py, &ring_offsets);
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_polygon"),
            (polygon_offsets, ring_offsets, xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

fn export_arrow_multipolygon_shapes(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut multipolygon_offsets = Vec::new();
    let mut polygon_offsets = Vec::new();
    let mut ring_offsets = Vec::new();
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(shape.coord_count())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))
    })?;
    let mut coordinates =
        ArrowCoordinateBuffers::with_capacity(axes, total).map_err(pyo3::PyErr::from)?;
    let mut polygon_count = 0;
    let mut ring_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut multipolygon_offsets, polygon_count)?;
    push_i32_le(&mut polygon_offsets, ring_count)?;
    push_i32_le(&mut ring_offsets, coordinate_count)?;
    for shape in shapes {
        let Shape::MultiPolygon(polygons) = shape else {
            unreachable!()
        };
        polygon_count = polygon_count
            .checked_add(polygons.len())
            .ok_or_else(|| PyOverflowError::new_err("Arrow polygon count overflows usize"))?;
        push_i32_le(&mut multipolygon_offsets, polygon_count)?;
        for polygon in polygons {
            let ring_total = polygon
                .holes
                .len()
                .checked_add(1)
                .ok_or_else(|| PyOverflowError::new_err("Arrow ring count overflows usize"))?;
            ring_count = ring_count
                .checked_add(ring_total)
                .ok_or_else(|| PyOverflowError::new_err("Arrow ring count overflows usize"))?;
            push_i32_le(&mut polygon_offsets, ring_count)?;
            push_arrow_polygon(
                polygon,
                &mut ring_offsets,
                &mut coordinate_count,
                &mut coordinates,
            )?;
        }
    }
    let multipolygon_offsets = PyBytes::new(py, &multipolygon_offsets);
    let polygon_offsets = PyBytes::new(py, &polygon_offsets);
    let ring_offsets = PyBytes::new(py, &ring_offsets);
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_multipolygon"),
            (
                multipolygon_offsets,
                polygon_offsets,
                ring_offsets,
                xs,
                ys,
                zs,
                ms,
                crs,
                epoch,
            ),
        )?
        .unbind())
}

fn export_arrow_coordseq_shapes(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
    points_of: impl Fn(&Shape) -> &CoordSeq,
    arrow_builder: &str,
) -> PyResult<Py<PyAny>> {
    let mut offsets = Vec::new();
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(points_of(shape).len())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))
    })?;
    let mut coordinates =
        ArrowCoordinateBuffers::with_capacity(axes, total).map_err(pyo3::PyErr::from)?;
    let mut coordinate_count = 0;
    push_i32_le(&mut offsets, coordinate_count)?;
    for shape in shapes {
        let points = points_of(shape);
        coordinate_count = coordinate_count
            .checked_add(points.len())
            .ok_or_else(|| PyOverflowError::new_err("Arrow coordinate count overflows usize"))?;
        push_i32_le(&mut offsets, coordinate_count)?;
        coordinates.push_points(points)?;
    }
    let offsets = PyBytes::new(py, &offsets);
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(arrow_builder, (offsets, xs, ys, zs, ms, crs, epoch))?
        .unbind())
}

pub(crate) fn shapes_to_wkb_arrow(
    py: Python<'_>,
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    // R4-L4 item 2 tried `PyBytes::new_with` (and owner-pinned Arc) to skip the
    // Vec→PyBytes copy. Rebuild-logged dual-build measured ~0.95× — a
    // regression: `PyBytes::new_with` zero-initializes the whole buffer, and on
    // multi-row/large WKB that costs more than the copy it saves. Do not
    // re-attempt without new evidence. Scalar `to_wkb` (item 1) keeps
    // `new_with` — one small buffer measured a win.
    let mut offsets = Vec::new();
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(io::wkb_len(shape, false)?)
            .ok_or_else(|| PyOverflowError::new_err("WKB size overflows usize"))
    })?;
    let mut data = crate::try_vec_with_capacity(total)?;
    push_i32_le(&mut offsets, 0)?;
    for shape in shapes {
        io::write_wkb_to(&mut data, shape, crs, false)?;
        push_i32_le(&mut offsets, data.len())?;
    }
    let offsets = PyBytes::new(py, &offsets);
    let data = PyBytes::new(py, &data);
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_wkb"),
            (offsets, data, crs, epoch),
        )?
        .unbind())
}

/// Export nullable logical rows to Arrow WKB without manufacturing payload for
/// null rows. Binary/list offsets already align null rows at the wire boundary;
/// only the validity bitmap, not a placeholder geometry, represents missingness.
pub(crate) fn nullable_shapes_to_wkb_arrow(
    py: Python<'_>,
    shapes: &[Option<Shape>],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        shape.as_ref().map_or(Ok(total), |shape| {
            total
                .checked_add(io::wkb_len(shape, false)?)
                .ok_or_else(|| PyOverflowError::new_err("WKB size overflows usize"))
        })
    })?;
    let mut offsets = Vec::new();
    let mut data = crate::try_vec_with_capacity(total)?;
    push_i32_le(&mut offsets, 0)?;
    for shape in shapes {
        if let Some(shape) = shape {
            io::write_wkb_to(&mut data, shape, crs, false)?;
        }
        push_i32_le(&mut offsets, data.len())?;
    }
    let offsets = PyBytes::new(py, &offsets);
    let data = PyBytes::new(py, &data);
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_wkb"),
            (offsets, data, crs, epoch),
        )?
        .unbind())
}

pub(crate) fn storage_to_wkb_arrow(
    py: Python<'_>,
    storage: &GeometryArrayStorage,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let mut offsets = Vec::new();
    // Iterate the storage views directly. Packed point/line/polygon rows remain
    // borrowed `ShapeCow` views throughout, so forcing `encoding='wkb'` never
    // materializes a `PyGeometry` per row merely to reach the WKB writer.
    // See `shapes_to_wkb_arrow`: do not switch multi-row WKB to
    // `PyBytes::new_with` without new evidence (measured 0.95× regression).
    let total = storage.iter_shapes().try_fold(0_usize, |total, shape| {
        total
            .checked_add(io::wkb_len(&shape, false)?)
            .ok_or_else(|| PyOverflowError::new_err("WKB size overflows usize"))
    })?;
    let mut data = crate::try_vec_with_capacity(total)?;
    push_i32_le(&mut offsets, 0)?;
    for shape in storage.iter_shapes() {
        io::write_wkb_to(&mut data, &shape, crs, false)?;
        push_i32_le(&mut offsets, data.len())?;
    }
    debug_assert_eq!(data.len(), total);
    let offsets = PyBytes::new(py, &offsets);
    let data = PyBytes::new(py, &data);
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_wkb"),
            (offsets, data, crs, epoch),
        )?
        .unbind())
}
