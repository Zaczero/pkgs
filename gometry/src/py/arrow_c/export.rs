use std::sync::Arc;

use crate::array::{GeometryArrayStorage, RowSelectionRef};
use crate::geometry::{CoordWindow, LineSeq};
use crate::py::arrow::{accumulate_geometry_axes, mixed_axes_error};
use crate::py::arrow_c::{
    ArrowSchema, CoordSeq, CoordinateAxes, ExportedArray, GeometryEncoding, GeometryError,
    GometryArrowArray, Polygon, PyErr, PyGeometry, PyResult, SchemaNode, Shape, binary_array,
    coordinate_array, coordinate_schema, extension_schema, list_array, list_array_windowed,
    list_schema, wkb_schema,
};
use crate::{PyGeometryArray, io};

/// Storage-direct Arrow-C export for a `GeometryArray`.
///
/// Packed Identity/Window rows retain coordinate and CSR `Arc`s in capsule
/// private data (Window is an Arrow offset/length over the parent buffers).
/// Gather normalizes once through `gathered_memo`. Mixed scans `Vec<Shape>`
/// without materializing `PyGeometry` per row.
pub(crate) fn export_from_geometry_array(array: &PyGeometryArray) -> PyResult<ExportedArray> {
    let crs = array.crs_str();
    let epoch = array.epoch();
    // Gather → contiguous Identity once; Identity/Window pass through.
    let storage =
        crate::array::normalized_gather_storage(array.storage_arc(), &array.gathered_memo)
            .map_err(crate::array::packed_columns_err)?;
    export_from_storage(storage.as_ref(), crs, epoch)
}

pub(crate) fn schema_from_geometry_array(array: &PyGeometryArray) -> PyResult<Box<ArrowSchema>> {
    let crs = array.crs_str();
    let epoch = array.epoch();
    let storage =
        crate::array::normalized_gather_storage(array.storage_arc(), &array.gathered_memo)
            .map_err(crate::array::packed_columns_err)?;
    schema_from_storage(storage.as_ref(), crs, epoch)
}

fn export_from_storage(
    storage: &GeometryArrayStorage,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    match storage {
        GeometryArrayStorage::Points { coords, row_map } => {
            export_packed_points(coords, row_map.as_deref(), crs, epoch)
        },
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } => export_packed_lines(coords, offsets, row_map.as_deref(), crs, epoch),
        GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => export_packed_polygons(
            coords,
            ring_offsets,
            polygon_offsets,
            row_map.as_deref(),
            crs,
            epoch,
        ),
        GeometryArrayStorage::Mixed(shapes) => export_from_shapes(shapes, crs, epoch),
    }
}

fn schema_from_storage(
    storage: &GeometryArrayStorage,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Box<ArrowSchema>> {
    let (encoding, storage_schema) = match storage {
        GeometryArrayStorage::Points { coords, .. } => {
            (GeometryEncoding::Point, coordinate_schema(coords.axes()))
        },
        GeometryArrayStorage::Lines { coords, .. } => (
            GeometryEncoding::LineString,
            list_schema(coordinate_schema(coords.axes())),
        ),
        GeometryArrayStorage::Polygons { coords, .. } => (
            GeometryEncoding::Polygon,
            list_schema(list_schema(coordinate_schema(coords.axes()))),
        ),
        GeometryArrayStorage::Mixed(shapes) => {
            let refs: Vec<&Shape> = shapes.iter().collect();
            native_storage_schema_shapes(&refs).unwrap_or((GeometryEncoding::Wkb, wkb_schema()))
        },
    };
    extension_schema(encoding, storage_schema, crs, epoch).map(SchemaNode::into_schema)
}

fn export_packed_points(
    coords: &crate::geometry::CoordSeq,
    row_map: RowSelectionRef<'_>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    // Gather is normalized to Identity before this call; keep the arm for
    // exhaustiveness if a caller skips normalize.
    let seq = match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Gather(_) => coords.clone(),
        RowSelectionRef::Window { start, len } => {
            coords.view(CoordWindow::trusted(start..start + len, coords.len()))
        },
    };
    let axes = seq.axes();
    export_with_schema(
        GeometryEncoding::Point,
        coordinate_schema(axes),
        coordinate_array(&seq)?,
        crs,
        epoch,
    )
}

fn export_packed_lines(
    coords: &crate::geometry::CoordSeq,
    offsets: &crate::geometry::CsrOffsetColumn,
    row_map: RowSelectionRef<'_>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let (start, length) = match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
            (0, offsets.len().saturating_sub(1))
        },
        RowSelectionRef::Window { start, len } => (start, len),
    };
    let axes = coords.axes();
    let storage_schema = list_schema(coordinate_schema(axes));
    // Full parent CSR + coordinate Arcs; Window is Arrow offset/length only.
    let array = list_array_windowed(
        offsets.as_arc_i32(),
        coordinate_array(coords)?,
        start,
        length,
    )?;
    export_with_schema(
        GeometryEncoding::LineString,
        storage_schema,
        array,
        crs,
        epoch,
    )
}

fn export_packed_polygons(
    coords: &crate::geometry::CoordSeq,
    ring_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
    polygon_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
    row_map: RowSelectionRef<'_>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let (start, length) = match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
            (0, polygon_offsets.len().saturating_sub(1))
        },
        RowSelectionRef::Window { start, len } => (start, len),
    };
    let axes = coords.axes();
    let storage_schema = list_schema(list_schema(coordinate_schema(axes)));
    // Nested list: outer polygon offsets windowed; rings + coords fully shared.
    let rings = list_array(ring_offsets.as_arc_i32(), coordinate_array(coords)?)?;
    let array = list_array_windowed(polygon_offsets.as_arc_i32(), rings, start, length)?;
    export_with_schema(GeometryEncoding::Polygon, storage_schema, array, crs, epoch)
}

/// Export from raw shapes (Mixed storage / no `PyGeometry` materialization).
pub(crate) fn export_from_shapes(
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    if let Some(export) = export_native_shapes(shapes, crs, epoch)? {
        return Ok(export);
    }
    export_wkb_shapes(shapes, crs, epoch)
}

pub(crate) fn export_from_geometries<'a>(
    geometries: impl IntoIterator<Item = &'a PyGeometry>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let geometries = geometries.into_iter().collect::<Vec<_>>();
    if let Some(export) = export_native(&geometries, crs, epoch)? {
        return Ok(export);
    }
    export_wkb(&geometries, crs, epoch)
}

pub(crate) fn schema_from_geometries<'a>(
    geometries: impl IntoIterator<Item = &'a PyGeometry>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Box<ArrowSchema>> {
    let geometries = geometries.into_iter().collect::<Vec<_>>();
    let (encoding, storage_schema) =
        native_storage_schema(&geometries).unwrap_or_else(|| (GeometryEncoding::Wkb, wkb_schema()));
    extension_schema(encoding, storage_schema, crs, epoch).map(SchemaNode::into_schema)
}

pub(crate) fn native_storage_schema(
    geometries: &[&PyGeometry],
) -> Option<(GeometryEncoding, SchemaNode)> {
    let shapes: Vec<&Shape> = geometries
        .iter()
        .map(|geometry| geometry.shape.shape())
        .collect();
    native_storage_schema_shapes(&shapes)
}

pub(crate) fn native_storage_schema_shapes(
    shapes: &[&Shape],
) -> Option<(GeometryEncoding, SchemaNode)> {
    let axes = homogeneous_shape_axes(shapes)?;
    if shapes.iter().all(|s| matches!(s, Shape::Point(_))) {
        return Some((GeometryEncoding::Point, coordinate_schema(axes)));
    }
    if shapes.iter().all(|s| matches!(s, Shape::MultiPoint(_))) {
        return Some((
            GeometryEncoding::MultiPoint,
            list_schema(coordinate_schema(axes)),
        ));
    }
    if shapes.iter().all(|s| matches!(s, Shape::LineString(_))) {
        return Some((
            GeometryEncoding::LineString,
            list_schema(coordinate_schema(axes)),
        ));
    }
    if shapes
        .iter()
        .all(|s| matches!(s, Shape::MultiLineString(_)))
    {
        return Some((
            GeometryEncoding::MultiLineString,
            list_schema(list_schema(coordinate_schema(axes))),
        ));
    }
    if shapes.iter().all(|s| matches!(s, Shape::Polygon(_))) {
        return Some((
            GeometryEncoding::Polygon,
            list_schema(list_schema(coordinate_schema(axes))),
        ));
    }
    if shapes.iter().all(|s| matches!(s, Shape::MultiPolygon(_))) {
        return Some((
            GeometryEncoding::MultiPolygon,
            list_schema(list_schema(list_schema(coordinate_schema(axes)))),
        ));
    }
    None
}

fn homogeneous_shape_axes(shapes: &[&Shape]) -> Option<CoordinateAxes> {
    let mut axes = None;
    for shape in shapes {
        if !accumulate_geometry_axes(shape, &mut axes) {
            return None;
        }
    }
    Some(axes.unwrap_or(CoordinateAxes::XY))
}

pub(crate) fn export_native(
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<ExportedArray>> {
    let shapes: Vec<&Shape> = geometries
        .iter()
        .map(|geometry| geometry.shape.shape())
        .collect();
    export_native_shapes_slice(&shapes, crs, epoch)
}

pub(crate) fn export_native_shapes(
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<ExportedArray>> {
    let refs: Vec<&Shape> = shapes.iter().collect();
    export_native_shapes_slice(&refs, crs, epoch)
}

fn export_native_shapes_slice(
    shapes: &[&Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<ExportedArray>> {
    let Some(axes) = homogeneous_shape_axes(shapes) else {
        return Ok(None);
    };
    if shapes.iter().all(|s| matches!(s, Shape::Point(_))) {
        let mut points = Vec::with_capacity(shapes.len());
        for shape in shapes {
            let Shape::Point(point) = shape else {
                unreachable!()
            };
            points.push(*point);
        }
        let seq = CoordSeq::from_points(&points);
        return Ok(Some(export_with_schema(
            GeometryEncoding::Point,
            coordinate_schema(axes),
            coordinate_array(&seq)?,
            crs,
            epoch,
        )?));
    }
    if shapes.iter().all(|s| matches!(s, Shape::MultiPoint(_))) {
        return export_single_list_shapes(
            shapes.iter().copied(),
            axes,
            crs,
            epoch,
            GeometryEncoding::MultiPoint,
            |shape| match shape {
                Shape::MultiPoint(points) => points,
                _ => unreachable!(),
            },
        )
        .map(Some);
    }
    if shapes.iter().all(|s| matches!(s, Shape::LineString(_))) {
        return export_single_list_shapes(
            shapes.iter().copied(),
            axes,
            crs,
            epoch,
            GeometryEncoding::LineString,
            |shape| match shape {
                Shape::LineString(points) => points,
                _ => unreachable!(),
            },
        )
        .map(Some);
    }
    if shapes
        .iter()
        .all(|s| matches!(s, Shape::MultiLineString(_)))
    {
        return export_multilines_shapes(shapes, axes, crs, epoch).map(Some);
    }
    if shapes.iter().all(|s| matches!(s, Shape::Polygon(_))) {
        return export_polygons_shapes(shapes, axes, crs, epoch).map(Some);
    }
    if shapes.iter().all(|s| matches!(s, Shape::MultiPolygon(_))) {
        return export_multipolygons_shapes(shapes, axes, crs, epoch).map(Some);
    }
    Ok(None)
}

pub(crate) fn export_with_schema(
    encoding: GeometryEncoding,
    storage_schema: SchemaNode,
    array: GometryArrowArray,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let schema_node = extension_schema(encoding, storage_schema, crs, epoch)?;
    let schema = schema_node.clone().into_schema();
    Ok(ExportedArray {
        schema,
        schema_node,
        array,
    })
}

pub(crate) fn gather_coordseqs<'a>(
    axes: CoordinateAxes,
    total: usize,
    seqs: impl Iterator<Item = &'a CoordSeq>,
) -> PyResult<CoordSeq> {
    let mut xs = Vec::with_capacity(total);
    let mut ys = Vec::with_capacity(total);
    let mut zs = axes.has_z().then(|| Vec::with_capacity(total));
    let mut ms = axes.has_m().then(|| Vec::with_capacity(total));
    for seq in seqs {
        xs.extend_from_slice(seq.xs());
        ys.extend_from_slice(seq.ys());
        if let Some(out) = &mut zs {
            out.extend_from_slice(seq.zs().ok_or_else(mixed_axes_error)?);
        }
        if let Some(out) = &mut ms {
            out.extend_from_slice(seq.ms().ok_or_else(mixed_axes_error)?);
        }
    }
    Ok(CoordSeq::from_columns(
        xs.into(),
        ys.into(),
        zs.map(Into::into),
        ms.map(Into::into),
    ))
}

/// Single-list GeoArrow layout over shape-owned `CoordSeq`s. One geometry
/// reuses the existing column Arcs; multi-row gathers into one run.
pub(crate) fn export_single_list_shapes<'a>(
    shapes: impl IntoIterator<Item = &'a Shape>,
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
    encoding: GeometryEncoding,
    points_of: impl Fn(&Shape) -> &CoordSeq,
) -> PyResult<ExportedArray> {
    let shapes: Vec<&Shape> = shapes.into_iter().collect();
    // Scalar / single-row: retain the existing CoordSeq Arcs (Item 3).
    if let [shape] = shapes.as_slice() {
        let seq = points_of(shape);
        let count = i32::try_from(seq.len()).map_err(|_| offset_error())?;
        let offsets: Arc<[i32]> = Arc::from([0, count]);
        let storage_schema = list_schema(coordinate_schema(axes));
        let array = list_array(offsets, coordinate_array(seq)?)?;
        return export_with_schema(encoding, storage_schema, array, crs, epoch);
    }
    let total: usize = shapes.iter().map(|shape| points_of(shape).len()).sum();
    let mut offsets = Vec::with_capacity(shapes.len() + 1);
    offsets.push(0);
    let mut count = 0_i32;
    for shape in &shapes {
        count = count
            .checked_add(i32::try_from(points_of(shape).len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        offsets.push(count);
    }
    let coords = gather_coordseqs(axes, total, shapes.iter().map(|shape| points_of(shape)))?;
    let storage_schema = list_schema(coordinate_schema(axes));
    let array = list_array(offsets.into(), coordinate_array(&coords)?)?;
    export_with_schema(encoding, storage_schema, array, crs, epoch)
}

pub(crate) fn export_multilines_shapes(
    shapes: &[&Shape],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let mut geometry_offsets = vec![0_i32];
    let mut line_offsets = vec![0_i32];
    let mut line_count = 0_i32;
    let mut point_count = 0_i32;
    let mut lines = Vec::new();
    for shape in shapes {
        let Shape::MultiLineString(items) = shape else {
            unreachable!()
        };
        line_count = line_count
            .checked_add(i32::try_from(items.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        geometry_offsets.push(line_count);
        for line in items {
            point_count = point_count
                .checked_add(i32::try_from(line.len()).map_err(|_| offset_error())?)
                .ok_or_else(offset_error)?;
            line_offsets.push(point_count);
            lines.push(line);
        }
    }
    let coords = gather_coordseqs(
        axes,
        point_count as usize,
        lines.into_iter().map(LineSeq::as_coords),
    )?;
    let storage_schema = list_schema(list_schema(coordinate_schema(axes)));
    let array = list_array(
        geometry_offsets.into(),
        list_array(line_offsets.into(), coordinate_array(&coords)?)?,
    )?;
    export_with_schema(
        GeometryEncoding::MultiLineString,
        storage_schema,
        array,
        crs,
        epoch,
    )
}

pub(crate) fn push_polygon<'a>(
    polygon: &'a Polygon,
    rings: &mut Vec<&'a CoordSeq>,
    ring_offsets: &mut Vec<i32>,
    point_count: &mut i32,
) -> PyResult<()> {
    for ring in polygon.rings() {
        *point_count = point_count
            .checked_add(i32::try_from(ring.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        ring_offsets.push(*point_count);
        rings.push(ring);
    }
    Ok(())
}

pub(crate) fn export_polygons_shapes(
    shapes: &[&Shape],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut ring_count = 0_i32;
    let mut point_count = 0_i32;
    let mut rings = Vec::new();
    for shape in shapes {
        let Shape::Polygon(polygon) = shape else {
            unreachable!()
        };
        ring_count = ring_count
            .checked_add(i32::try_from(1 + polygon.holes.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        polygon_offsets.push(ring_count);
        push_polygon(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    let storage_schema = list_schema(list_schema(coordinate_schema(axes)));
    let array = list_array(
        polygon_offsets.into(),
        list_array(ring_offsets.into(), coordinate_array(&coords)?)?,
    )?;
    export_with_schema(GeometryEncoding::Polygon, storage_schema, array, crs, epoch)
}

pub(crate) fn export_multipolygons_shapes(
    shapes: &[&Shape],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let mut multipolygon_offsets = vec![0_i32];
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut polygon_count = 0_i32;
    let mut ring_count = 0_i32;
    let mut point_count = 0_i32;
    let mut rings = Vec::new();
    for shape in shapes {
        let Shape::MultiPolygon(polygons) = shape else {
            unreachable!()
        };
        polygon_count = polygon_count
            .checked_add(i32::try_from(polygons.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        multipolygon_offsets.push(polygon_count);
        for polygon in polygons {
            ring_count = ring_count
                .checked_add(i32::try_from(1 + polygon.holes.len()).map_err(|_| offset_error())?)
                .ok_or_else(offset_error)?;
            polygon_offsets.push(ring_count);
            push_polygon(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
        }
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    let storage_schema = list_schema(list_schema(list_schema(coordinate_schema(axes))));
    let array = list_array(
        multipolygon_offsets.into(),
        list_array(
            polygon_offsets.into(),
            list_array(ring_offsets.into(), coordinate_array(&coords)?)?,
        )?,
    )?;
    export_with_schema(
        GeometryEncoding::MultiPolygon,
        storage_schema,
        array,
        crs,
        epoch,
    )
}

pub(crate) fn export_wkb(
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let shapes: Vec<&Shape> = geometries
        .iter()
        .map(|geometry| geometry.shape.shape())
        .collect();
    // Prefer array-frame CRS when all rows share it (caller already gated).
    export_wkb_shapes_with_crs(&shapes, crs, epoch)
}

pub(crate) fn export_wkb_shapes(
    shapes: &[Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let refs: Vec<&Shape> = shapes.iter().collect();
    export_wkb_shapes_with_crs(&refs, crs, epoch)
}

fn export_wkb_shapes_with_crs(
    shapes: &[&Shape],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let total = shapes.iter().map(|shape| io::wkb_len(shape, false)).sum();
    let mut offsets = Vec::with_capacity(shapes.len() + 1);
    let mut data = Vec::with_capacity(total);
    offsets.push(0_i32);
    for shape in shapes {
        io::write_wkb_to(&mut data, shape, crs, false)?;
        offsets.push(i32::try_from(data.len()).map_err(|_| offset_error())?);
    }
    export_with_schema(
        GeometryEncoding::Wkb,
        wkb_schema(),
        binary_array(offsets.into(), data.into())?,
        crs,
        epoch,
    )
}

pub(crate) fn offset_error() -> PyErr {
    GeometryError::new_err("Arrow output exceeds the i32 offset capacity for binary/list arrays")
}
