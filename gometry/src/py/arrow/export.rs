#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::GeometryArrayStorage;
use crate::py::arrow::*;

pub(crate) fn validity_bitmap_from_missing(mask: &[bool]) -> Vec<u8> {
    let mut validity = vec![0_u8; mask.len().div_ceil(8)];
    for (row, &missing) in mask.iter().enumerate() {
        if !missing {
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
    *coordinate_count += polygon.shell.len();
    push_i32_le(ring_offsets, *coordinate_count)?;
    coordinates.push_points(&polygon.shell)?;
    for hole in polygon.holes.iter() {
        *coordinate_count += hole.len();
        push_i32_le(ring_offsets, *coordinate_count)?;
        coordinates.push_points(hole)?;
    }
    Ok(())
}

pub(crate) fn export_arrow_points(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut coordinates = ArrowCoordinateBuffers::with_capacity(axes, geometries.len());
    for geometry in geometries {
        let Shape::Point(point) = geometry.shape.shape() else {
            unreachable!("all geometries are points");
        };
        coordinates.push_point(*point)?;
    }
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(
            pyo3::intern!(py, "to_arrow_point"),
            (xs, ys, zs, ms, crs, epoch),
        )?
        .unbind())
}

/// Shared exporter for the two single-list kinds (`MultiPoint` and
/// `LineString`): one offsets level over a flat coordinate run. `points_of`
/// projects each geometry's coordinate sequence (the classification pass has
/// already proven the kind).
pub(crate) fn export_arrow_coordseq_rows(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
    points_of: impl Fn(&Shape) -> &CoordSeq,
    arrow_builder: &str,
) -> PyResult<Py<PyAny>> {
    let mut offsets = Vec::with_capacity((geometries.len() + 1) * 4);
    let total: usize = geometries
        .iter()
        .map(|geometry| geometry.shape.coord_count())
        .sum();
    let mut coordinates = ArrowCoordinateBuffers::with_capacity(axes, total);
    let mut coordinate_count = 0;
    push_i32_le(&mut offsets, coordinate_count)?;
    for geometry in geometries {
        let points = points_of(geometry.shape.shape());
        coordinate_count += points.len();
        push_i32_le(&mut offsets, coordinate_count)?;
        coordinates.push_points(points)?;
    }
    let offsets = PyBytes::new(py, &offsets);
    let (xs, ys, zs, ms) = coordinates.into_buffers(py)?;
    Ok(gometry_arrow_module(py)?
        .call_method1(arrow_builder, (offsets, xs, ys, zs, ms, crs, epoch))?
        .unbind())
}

pub(crate) fn export_arrow_multipoints(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    export_arrow_coordseq_rows(
        py,
        geometries,
        crs,
        epoch,
        axes,
        |shape| match shape {
            Shape::MultiPoint(points) => points,
            _ => unreachable!("all geometries are multipoints"),
        },
        "to_arrow_multipoint",
    )
}

pub(crate) fn export_arrow_linestrings(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    export_arrow_coordseq_rows(
        py,
        geometries,
        crs,
        epoch,
        axes,
        |shape| match shape {
            Shape::LineString(points) => points,
            _ => unreachable!("all geometries are linestrings"),
        },
        "to_arrow_linestring",
    )
}

pub(crate) fn export_arrow_multilinestrings(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut geometry_offsets = Vec::with_capacity((geometries.len() + 1) * 4);
    let mut line_offsets = Vec::new();
    let total: usize = geometries
        .iter()
        .map(|geometry| geometry.shape.coord_count())
        .sum();
    let mut coordinates = ArrowCoordinateBuffers::with_capacity(axes, total);
    let mut line_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut geometry_offsets, line_count)?;
    push_i32_le(&mut line_offsets, coordinate_count)?;
    for geometry in geometries {
        let Shape::MultiLineString(lines) = geometry.shape.shape() else {
            unreachable!("all geometries are multilinestrings");
        };
        line_count += lines.len();
        push_i32_le(&mut geometry_offsets, line_count)?;
        for line in lines {
            coordinate_count += line.len();
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

pub(crate) fn export_arrow_polygons(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut polygon_offsets = Vec::with_capacity((geometries.len() + 1) * 4);
    let mut ring_offsets = Vec::new();
    let total: usize = geometries
        .iter()
        .map(|geometry| geometry.shape.coord_count())
        .sum();
    let mut coordinates = ArrowCoordinateBuffers::with_capacity(axes, total);
    let mut ring_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut polygon_offsets, ring_count)?;
    push_i32_le(&mut ring_offsets, coordinate_count)?;
    for geometry in geometries {
        let Shape::Polygon(polygon) = geometry.shape.shape() else {
            unreachable!("all geometries are polygons");
        };
        ring_count += 1 + polygon.holes.len();
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

pub(crate) fn export_arrow_multipolygons(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
    axes: CoordinateAxes,
) -> PyResult<Py<PyAny>> {
    let mut multipolygon_offsets = Vec::with_capacity((geometries.len() + 1) * 4);
    let mut polygon_offsets = Vec::new();
    let mut ring_offsets = Vec::new();
    let total: usize = geometries
        .iter()
        .map(|geometry| geometry.shape.coord_count())
        .sum();
    let mut coordinates = ArrowCoordinateBuffers::with_capacity(axes, total);
    let mut polygon_count = 0;
    let mut ring_count = 0;
    let mut coordinate_count = 0;
    push_i32_le(&mut multipolygon_offsets, polygon_count)?;
    push_i32_le(&mut polygon_offsets, ring_count)?;
    push_i32_le(&mut ring_offsets, coordinate_count)?;
    for geometry in geometries {
        let Shape::MultiPolygon(polygons) = geometry.shape.shape() else {
            unreachable!("all geometries are multipolygons");
        };
        polygon_count += polygons.len();
        push_i32_le(&mut multipolygon_offsets, polygon_count)?;
        for polygon in polygons {
            ring_count += 1 + polygon.holes.len();
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
        let column = |arc: std::sync::Arc<[f64]>| {
            crate::py::vectors::Float64Buffer::view(arc, window.clone())
        };
        let (xs, ys) = (column(columns.xs)?, column(columns.ys)?);
        let (zs, ms) = (
            columns.zs.map(&column).transpose()?,
            columns.ms.map(&column).transpose()?,
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
        let column = |arc: std::sync::Arc<[f64]>| {
            crate::py::vectors::Float64Buffer::view(arc, window.clone())
        };
        let (xs, ys) = (column(columns.xs)?, column(columns.ys)?);
        let (zs, ms) = (
            columns.zs.map(&column).transpose()?,
            columns.ms.map(&column).transpose()?,
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
    let mut polygon_bytes: Vec<u8> = Vec::with_capacity(polygon_offsets.len() * 4);
    for &offset in polygon_offsets.iter() {
        polygon_bytes.extend_from_slice(&offset.to_le_bytes());
    }
    let mut ring_bytes: Vec<u8> = Vec::with_capacity(ring_offsets.len() * 4);
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
        let column = |arc: std::sync::Arc<[f64]>| {
            crate::py::vectors::Float64Buffer::view(arc, window.clone())
        };
        let (xs, ys) = (column(columns.xs)?, column(columns.ys)?);
        let (zs, ms) = (
            columns.zs.map(&column).transpose()?,
            columns.ms.map(&column).transpose()?,
        );
        return Ok(gometry_arrow_module(py)?
            .call_method1(
                pyo3::intern!(py, "to_arrow_linestring"),
                (offset_bytes, xs, ys, zs, ms, crs, epoch),
            )?
            .unbind());
    }
    let mut offset_bytes: Vec<u8> = Vec::with_capacity(offsets.len() * 4);
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

pub(crate) fn geometries_to_arrow<'a>(
    py: Python<'_>,
    geometries: impl IntoIterator<Item = &'a PyGeometry>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let geometries = geometries.into_iter().collect::<Vec<_>>();
    if let Some(output) = geometries_to_packed_arrow(py, &geometries, crs, epoch)? {
        return Ok(output);
    }
    geometries_to_wkb_arrow_slice(py, &geometries, crs, epoch)
}

pub(crate) fn geometries_to_packed_arrow(
    py: Python<'_>,
    geometries: &[&PyGeometry],
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

    let Some(first) = geometries.first() else {
        return export_arrow_points(py, geometries, crs, epoch, CoordinateAxes::XY).map(Some);
    };
    let kind = match first.shape.shape() {
        Shape::Point(_) => PackedKind::Point,
        Shape::MultiPoint(_) => PackedKind::MultiPoint,
        Shape::LineString(_) => PackedKind::LineString,
        Shape::MultiLineString(_) => PackedKind::MultiLineString,
        Shape::Polygon(_) => PackedKind::Polygon,
        Shape::MultiPolygon(_) => PackedKind::MultiPolygon,
        Shape::GeometryCollection(_) | Shape::Empty(..) => return Ok(None),
    };
    let mut axes = None;
    for geometry in geometries {
        let shape = geometry.shape.shape();
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
    match kind {
        PackedKind::Point => export_arrow_points(py, geometries, crs, epoch, axes).map(Some),
        PackedKind::MultiPoint => {
            export_arrow_multipoints(py, geometries, crs, epoch, axes).map(Some)
        },
        PackedKind::LineString => {
            export_arrow_linestrings(py, geometries, crs, epoch, axes).map(Some)
        },
        PackedKind::MultiLineString => {
            export_arrow_multilinestrings(py, geometries, crs, epoch, axes).map(Some)
        },
        PackedKind::Polygon => export_arrow_polygons(py, geometries, crs, epoch, axes).map(Some),
        PackedKind::MultiPolygon => {
            export_arrow_multipolygons(py, geometries, crs, epoch, axes).map(Some)
        },
    }
}

pub(crate) fn geometries_to_wkb_arrow<'a>(
    py: Python<'_>,
    geometries: impl IntoIterator<Item = &'a PyGeometry>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let geometries = geometries.into_iter().collect::<Vec<_>>();
    geometries_to_wkb_arrow_slice(py, &geometries, crs, epoch)
}

pub(crate) fn storage_to_wkb_arrow(
    py: Python<'_>,
    storage: &GeometryArrayStorage,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let mut offsets = Vec::with_capacity((storage.len() + 1) * 4);
    // Iterate the storage views directly. Packed point/line/polygon rows remain
    // borrowed `ShapeCow` views throughout, so forcing `encoding='wkb'` never
    // materializes a `PyGeometry` per row merely to reach the WKB writer.
    let total: usize = storage
        .iter_shapes()
        .map(|shape| io::wkb_len(&shape, false))
        .sum();
    let mut data = Vec::with_capacity(total);
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

fn geometries_to_wkb_arrow_slice(
    py: Python<'_>,
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let mut offsets = Vec::with_capacity((geometries.len() + 1) * 4);
    // Exact one-shot allocation: the serialized length is computable per
    // geometry (no SRID in Arrow WKB), so large exports never reallocate.
    let total: usize = geometries
        .iter()
        .map(|geometry| io::wkb_len(geometry.shape.shape(), false))
        .sum();
    let mut data = Vec::with_capacity(total);
    push_i32_le(&mut offsets, 0)?;
    for geometry in geometries {
        io::write_wkb_to(&mut data, &geometry.shape, geometry.crs_str(), false)?;
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
