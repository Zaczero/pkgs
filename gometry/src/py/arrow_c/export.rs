use crate::geometry::LineSeq;
use crate::py::arrow::{homogeneous_geometry_axes, mixed_axes_error};
use crate::py::arrow_c::*;

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
    let axes = homogeneous_geometry_axes(geometries)?;
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::Point(_)))
    {
        return Some((GeometryEncoding::Point, coordinate_schema(axes)));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiPoint(_)))
    {
        return Some((
            GeometryEncoding::MultiPoint,
            list_schema(coordinate_schema(axes)),
        ));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::LineString(_)))
    {
        return Some((
            GeometryEncoding::LineString,
            list_schema(coordinate_schema(axes)),
        ));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiLineString(_)))
    {
        return Some((
            GeometryEncoding::MultiLineString,
            list_schema(list_schema(coordinate_schema(axes))),
        ));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::Polygon(_)))
    {
        return Some((
            GeometryEncoding::Polygon,
            list_schema(list_schema(coordinate_schema(axes))),
        ));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiPolygon(_)))
    {
        return Some((
            GeometryEncoding::MultiPolygon,
            list_schema(list_schema(list_schema(coordinate_schema(axes)))),
        ));
    }
    None
}

pub(crate) fn export_native(
    geometries: &[&PyGeometry],
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<ExportedArray>> {
    let Some(axes) = homogeneous_geometry_axes(geometries) else {
        return Ok(None);
    };
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::Point(_)))
    {
        let mut points = Vec::with_capacity(geometries.len());
        for geometry in geometries {
            let Shape::Point(point) = geometry.shape.shape() else {
                unreachable!()
            };
            points.push(*point);
        }
        let seq = CoordSeq::from_points(&points);
        return Ok(Some(export_with_schema(
            GeometryEncoding::Point,
            coordinate_schema(axes),
            coordinate_array(&seq),
            crs,
            epoch,
        )?));
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiPoint(_)))
    {
        return export_single_list(
            geometries,
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
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::LineString(_)))
    {
        return export_single_list(
            geometries,
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
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiLineString(_)))
    {
        return export_multilines(geometries, axes, crs, epoch).map(Some);
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::Polygon(_)))
    {
        return export_polygons(geometries, axes, crs, epoch).map(Some);
    }
    if geometries
        .iter()
        .all(|g| matches!(g.shape.shape(), Shape::MultiPolygon(_)))
    {
        return export_multipolygons(geometries, axes, crs, epoch).map(Some);
    }
    Ok(None)
}

pub(crate) fn export_with_schema(
    encoding: GeometryEncoding,
    storage_schema: SchemaNode,
    array: Box<ArrowArray>,
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

pub(crate) fn export_single_list(
    geometries: &[&PyGeometry],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
    encoding: GeometryEncoding,
    points_of: impl Fn(&Shape) -> &CoordSeq,
) -> PyResult<ExportedArray> {
    let total = geometries
        .iter()
        .map(|geometry| geometry.shape.coord_count())
        .sum();
    let mut offsets = Vec::with_capacity(geometries.len() + 1);
    offsets.push(0);
    let mut count = 0_i32;
    for geometry in geometries {
        count = count
            .checked_add(i32::try_from(geometry.shape.coord_count()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        offsets.push(count);
    }
    let coords = gather_coordseqs(
        axes,
        total,
        geometries
            .iter()
            .map(|geometry| points_of(geometry.shape.shape())),
    )?;
    let storage_schema = list_schema(coordinate_schema(axes));
    let array = list_array(offsets.into(), coordinate_array(&coords));
    export_with_schema(encoding, storage_schema, array, crs, epoch)
}

pub(crate) fn export_multilines(
    geometries: &[&PyGeometry],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let mut geometry_offsets = vec![0_i32];
    let mut line_offsets = vec![0_i32];
    let mut line_count = 0_i32;
    let mut point_count = 0_i32;
    let mut lines = Vec::new();
    for geometry in geometries {
        let Shape::MultiLineString(items) = geometry.shape.shape() else {
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
        list_array(line_offsets.into(), coordinate_array(&coords)),
    );
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

pub(crate) fn export_polygons(
    geometries: &[&PyGeometry],
    axes: CoordinateAxes,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<ExportedArray> {
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut ring_count = 0_i32;
    let mut point_count = 0_i32;
    let mut rings = Vec::new();
    for geometry in geometries {
        let Shape::Polygon(polygon) = geometry.shape.shape() else {
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
        list_array(ring_offsets.into(), coordinate_array(&coords)),
    );
    export_with_schema(GeometryEncoding::Polygon, storage_schema, array, crs, epoch)
}

pub(crate) fn export_multipolygons(
    geometries: &[&PyGeometry],
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
    for geometry in geometries {
        let Shape::MultiPolygon(polygons) = geometry.shape.shape() else {
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
            list_array(ring_offsets.into(), coordinate_array(&coords)),
        ),
    );
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
    let total = geometries
        .iter()
        .map(|geometry| io::wkb_len(geometry.shape.shape(), false))
        .sum();
    let mut offsets = Vec::with_capacity(geometries.len() + 1);
    let mut data = Vec::with_capacity(total);
    offsets.push(0_i32);
    for geometry in geometries {
        io::write_wkb_to(&mut data, &geometry.shape, geometry.crs_str(), false)?;
        offsets.push(i32::try_from(data.len()).map_err(|_| offset_error())?);
    }
    export_with_schema(
        GeometryEncoding::Wkb,
        wkb_schema(),
        binary_array(offsets.into(), data.into()),
        crs,
        epoch,
    )
}

pub(crate) fn offset_error() -> PyErr {
    GeometryError::new_err("Arrow output exceeds the i32 offset capacity for binary/list arrays")
}
