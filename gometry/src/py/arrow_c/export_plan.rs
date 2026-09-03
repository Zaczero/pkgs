#![allow(
    clippy::absolute_paths,
    reason = "Arrow planning is kept beside the C exporter and its native types"
)]
#![allow(
    clippy::too_many_lines,
    reason = "each GeoArrow layout has one explicit planning arm"
)]

use std::sync::Arc;

use pyo3::prelude::PyResult;

use crate::PyGeometryArray;
use crate::array::{ArrayRow, GeometryArrayStorage, RowSelectionRef};
use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, Polygon, Shape};
use crate::py::arrow::{ArrowEncoding, accumulate_geometry_axes};
use crate::py::arrow_c::{
    ArrowSchema, ExportedArray, GeometryEncoding, GometryArrowArray, NativeArrowExportError,
    NativeArrowExportResult, SchemaNode, admitted_binary_array, admitted_list_array_windowed,
    coordinate_array, coordinate_schema, extension_schema, list_schema, wkb_schema,
};

enum StoragePlan {
    Coordinates(CoordSeq),
    List {
        offsets: Arc<[i32]>,
        start: usize,
        length: usize,
        child: Box<StoragePlan>,
    },
    Binary {
        offsets: Arc<[i32]>,
        data: Arc<[u8]>,
    },
}

impl StoragePlan {
    fn schema(&self) -> SchemaNode {
        match self {
            Self::Coordinates(coords) => coordinate_schema(coords.axes()),
            Self::List { child, .. } => list_schema(child.schema()),
            Self::Binary { .. } => wkb_schema(),
        }
    }

    fn offset(&self) -> usize {
        match self {
            Self::List { start, .. } => *start,
            _ => 0,
        }
    }

    fn materialize(self) -> PyResult<GometryArrowArray> {
        match self {
            Self::Coordinates(coords) => coordinate_array(&coords),
            Self::List {
                offsets,
                start,
                length,
                child,
            } => admitted_list_array_windowed(offsets, child.materialize()?, start, length),
            Self::Binary { offsets, data } => admitted_binary_array(offsets, data),
        }
    }
}

struct ValidityPlan {
    bitmap: Arc<[u8]>,
    null_count: usize,
}

pub(crate) struct ArrowExportPlan {
    encoding: GeometryEncoding,
    storage: StoragePlan,
    crs: Option<String>,
    epoch: Option<f64>,
    validity: Option<ValidityPlan>,
}

pub(crate) struct PreparedArrowExportPlan {
    plan: ArrowExportPlan,
    schema: SchemaNode,
}

pub(crate) fn prepare_export_plan(
    plan: ArrowExportPlan,
) -> NativeArrowExportResult<PreparedArrowExportPlan> {
    let schema = extension_schema(
        plan.encoding,
        plan.storage.schema(),
        plan.crs.as_deref(),
        plan.epoch,
    )?;
    Ok(PreparedArrowExportPlan { plan, schema })
}

pub(crate) fn materialize_export_plan(plan: PreparedArrowExportPlan) -> PyResult<ExportedArray> {
    let PreparedArrowExportPlan { plan, schema } = plan;
    let ArrowExportPlan {
        storage, validity, ..
    } = plan;
    let mut export = crate::py::arrow_c::export_with_schema_node(schema, storage.materialize()?)?;
    if let Some(validity) = validity {
        crate::py::arrow_c::apply_exported_validity_plan(
            &mut export,
            validity.bitmap,
            validity.null_count,
        )?;
    }
    Ok(export)
}

pub(crate) fn materialize_schema_plan(plan: PreparedArrowExportPlan) -> PyResult<Box<ArrowSchema>> {
    plan.schema.into_schema()
}

pub(crate) fn plan_geometry_array(
    array: &PyGeometryArray,
    encoding: ArrowEncoding,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let crs = array.crs_str().map(str::to_owned);
    let epoch = array.epoch();
    let mut plan = match encoding {
        ArrowEncoding::Auto => plan_geometry_array_auto(array, crs, epoch)?,
        ArrowEncoding::Wkb => plan_geometry_array_wkb(array, crs, epoch)?,
    };
    if let Some(mask) = array.missing() {
        plan.validity = Some(plan_validity(mask, plan.storage.offset())?);
    }
    Ok(plan)
}

fn plan_geometry_array_auto(
    array: &PyGeometryArray,
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    if let Some(coords) = array.materialized_compact_point_coords()? {
        return Ok(ArrowExportPlan {
            encoding: GeometryEncoding::Point,
            storage: StoragePlan::Coordinates(coords),
            crs,
            epoch,
            validity: None,
        });
    }
    let gathered =
        crate::array::materialized_gather(array.storage()).map_err(packed_columns_error)?;
    let storage = gathered.as_ref().unwrap_or_else(|| array.storage());
    plan_storage(storage, crs, epoch)
}

fn plan_geometry_array_wkb(
    array: &PyGeometryArray,
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let mut offsets = Vec::new();
    let mut data = Vec::new();
    crate::boundary::try_reserve_vec(
        &mut offsets,
        array
            .len()
            .checked_add(1)
            .ok_or_else(|| NativeArrowExportError::overflow("WKB offset count overflows usize"))?,
    )
    .map_err(NativeArrowExportError::from)?;
    let mut total = 0_usize;
    for row in array.row_operation().rows_with_missing(|| ()) {
        if let ArrayRow::Present(row) = row {
            let size = row
                .row()
                .with_shape(|shape| crate::io::wkb_len(shape, false))?;
            total = total
                .checked_add(size)
                .ok_or_else(|| NativeArrowExportError::overflow("WKB size overflows usize"))?;
        }
    }
    crate::boundary::try_reserve_vec(&mut data, total).map_err(NativeArrowExportError::from)?;
    crate::boundary::try_push_vec(&mut offsets, 0_i32).map_err(NativeArrowExportError::from)?;
    for row in array.row_operation().rows_with_missing(|| ()) {
        if let ArrayRow::Present(row) = row {
            row.row().with_shape(|shape| {
                crate::io::write_wkb_to(&mut data, shape, crs.as_deref(), false)
            })?;
        }
        crate::boundary::try_push_vec(
            &mut offsets,
            i32::try_from(data.len()).map_err(|_| {
                NativeArrowExportError::overflow(
                    "Arrow output exceeds the i32 offset capacity for binary/list arrays",
                )
            })?,
        )
        .map_err(NativeArrowExportError::from)?;
    }
    validate_binary_offsets(&offsets, data.len())?;
    Ok(ArrowExportPlan {
        encoding: GeometryEncoding::Wkb,
        storage: StoragePlan::Binary {
            offsets: offsets.into(),
            data: data.into(),
        },
        crs,
        epoch,
        validity: None,
    })
}

fn plan_storage(
    storage: &GeometryArrayStorage,
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let (encoding, storage) = match storage {
        GeometryArrayStorage::Points { coords, row_map } => {
            let coords = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => coords.as_ref().clone(),
                RowSelectionRef::Window { start, len } => coords.view(
                    crate::geometry::CoordWindow::from_range(start..start + len, coords.len()),
                ),
            };
            (GeometryEncoding::Point, StoragePlan::Coordinates(coords))
        },
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } => {
            let (start, length) = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
                    (0, offsets.len().saturating_sub(1))
                },
                RowSelectionRef::Window { start, len } => (start, len),
            };
            validate_list_offsets(offsets.as_slice(), start, length, coords.len())?;
            (GeometryEncoding::LineString, StoragePlan::List {
                offsets: offsets.as_arc_i32(),
                start,
                length,
                child: Box::new(StoragePlan::Coordinates(coords.as_ref().clone())),
            })
        },
        GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => {
            let (start, length) = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
                    (0, polygon_offsets.len().saturating_sub(1))
                },
                RowSelectionRef::Window { start, len } => (start, len),
            };
            validate_list_offsets(
                ring_offsets.as_slice(),
                0,
                ring_offsets.len().saturating_sub(1),
                coords.len(),
            )?;
            validate_list_offsets(
                polygon_offsets.as_slice(),
                start,
                length,
                ring_offsets.len().saturating_sub(1),
            )?;
            let rings = StoragePlan::List {
                offsets: ring_offsets.as_arc_i32(),
                start: 0,
                length: ring_offsets.len().saturating_sub(1),
                child: Box::new(StoragePlan::Coordinates(coords.as_ref().clone())),
            };
            (GeometryEncoding::Polygon, StoragePlan::List {
                offsets: polygon_offsets.as_arc_i32(),
                start,
                length,
                child: Box::new(rings),
            })
        },
        GeometryArrayStorage::Mixed(shapes) => return plan_shapes(shapes, crs, epoch),
    };
    Ok(ArrowExportPlan {
        encoding,
        storage,
        crs,
        epoch,
        validity: None,
    })
}

pub(crate) fn plan_shapes(
    shapes: &[Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let refs: Vec<&Shape> = shapes.iter().collect();
    plan_shape_refs(&refs, crs, epoch)
}

pub(crate) fn plan_wkb_shapes(
    shapes: &[Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let refs: Vec<&Shape> = shapes.iter().collect();
    plan_wkb_refs(&refs, crs, epoch)
}

fn plan_shape_refs(
    shapes: &[&Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let Some(axes) = homogeneous_shape_axes(shapes) else {
        return plan_wkb_refs(shapes, crs, epoch);
    };
    let Some(first) = shapes.first() else {
        return Ok(ArrowExportPlan {
            encoding: GeometryEncoding::Point,
            storage: StoragePlan::Coordinates(CoordSeq::from_points(&[])),
            crs,
            epoch,
            validity: None,
        });
    };
    let plan = match first {
        Shape::Point(_) if shapes.iter().all(|shape| matches!(shape, Shape::Point(_))) => {
            let mut points = Vec::new();
            crate::boundary::try_reserve_vec(&mut points, shapes.len())
                .map_err(NativeArrowExportError::from)?;
            for shape in shapes {
                let Shape::Point(point) = shape else {
                    unreachable!()
                };
                crate::boundary::try_push_vec(&mut points, *point)
                    .map_err(NativeArrowExportError::from)?;
            }
            ArrowExportPlan {
                encoding: GeometryEncoding::Point,
                storage: StoragePlan::Coordinates(CoordSeq::from_points(&points)),
                crs,
                epoch,
                validity: None,
            }
        },
        Shape::MultiPoint(_)
            if shapes
                .iter()
                .all(|shape| matches!(shape, Shape::MultiPoint(_))) =>
        {
            if shapes.iter().any(
                |shape| matches!(shape, Shape::MultiPoint(points) if points.has_empty_members()),
            ) {
                return plan_wkb_refs(shapes, crs, epoch);
            }
            ArrowExportPlan {
                encoding: GeometryEncoding::MultiPoint,
                storage: plan_single_list(shapes, axes, |shape| match shape {
                    Shape::MultiPoint(points) => points,
                    _ => unreachable!(),
                })?,
                crs,
                epoch,
                validity: None,
            }
        },
        Shape::LineString(_)
            if shapes
                .iter()
                .all(|shape| matches!(shape, Shape::LineString(_))) =>
        {
            ArrowExportPlan {
                encoding: GeometryEncoding::LineString,
                storage: plan_single_list(shapes, axes, |shape| match shape {
                    Shape::LineString(points) => points,
                    _ => unreachable!(),
                })?,
                crs,
                epoch,
                validity: None,
            }
        },
        Shape::MultiLineString(_)
            if shapes
                .iter()
                .all(|shape| matches!(shape, Shape::MultiLineString(_))) =>
        {
            ArrowExportPlan {
                encoding: GeometryEncoding::MultiLineString,
                storage: plan_multilines(shapes, axes)?,
                crs,
                epoch,
                validity: None,
            }
        },
        Shape::Polygon(_)
            if shapes
                .iter()
                .all(|shape| matches!(shape, Shape::Polygon(_))) =>
        {
            ArrowExportPlan {
                encoding: GeometryEncoding::Polygon,
                storage: plan_polygons(shapes, axes)?,
                crs,
                epoch,
                validity: None,
            }
        },
        Shape::MultiPolygon(_)
            if shapes
                .iter()
                .all(|shape| matches!(shape, Shape::MultiPolygon(_))) =>
        {
            ArrowExportPlan {
                encoding: GeometryEncoding::MultiPolygon,
                storage: plan_multipolygons(shapes, axes)?,
                crs,
                epoch,
                validity: None,
            }
        },
        _ => return plan_wkb_refs(shapes, crs, epoch),
    };
    Ok(plan)
}

fn plan_single_list(
    shapes: &[&Shape],
    axes: CoordinateAxes,
    points_of: impl Fn(&Shape) -> &CoordSeq,
) -> NativeArrowExportResult<StoragePlan> {
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total.checked_add(points_of(shape).len()).ok_or_else(|| {
            NativeArrowExportError::overflow("Arrow coordinate count overflows usize")
        })
    })?;
    let mut offsets = Vec::new();
    crate::boundary::try_reserve_vec(
        &mut offsets,
        shapes.len().checked_add(1).ok_or_else(|| {
            NativeArrowExportError::overflow("Arrow offset count overflows usize")
        })?,
    )
    .map_err(NativeArrowExportError::from)?;
    crate::boundary::try_push_vec(&mut offsets, 0_i32).map_err(NativeArrowExportError::from)?;
    let mut count = 0_i32;
    for shape in shapes {
        count = count
            .checked_add(i32::try_from(points_of(shape).len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        crate::boundary::try_push_vec(&mut offsets, count).map_err(NativeArrowExportError::from)?;
    }
    let coords = gather_coordseqs(axes, total, shapes.iter().map(|shape| points_of(shape)))?;
    Ok(StoragePlan::List {
        offsets: offsets.into(),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::Coordinates(coords)),
    })
}

fn plan_multilines(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut geometry_offsets = vec![0_i32];
    let mut line_offsets = vec![0_i32];
    let mut lines = Vec::new();
    let mut line_count = 0_i32;
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::MultiLineString(items) = shape else {
            unreachable!()
        };
        line_count = line_count
            .checked_add(i32::try_from(items.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        crate::boundary::try_push_vec(&mut geometry_offsets, line_count)
            .map_err(NativeArrowExportError::from)?;
        for member in items.members() {
            match member {
                crate::geometry::MultiMember::Empty => {
                    crate::boundary::try_push_vec(&mut line_offsets, point_count)
                        .map_err(NativeArrowExportError::from)?
                },
                crate::geometry::MultiMember::Value(index) => {
                    let line = &items.as_slice()[index];
                    point_count = point_count
                        .checked_add(i32::try_from(line.len()).map_err(|_| offset_error())?)
                        .ok_or_else(offset_error)?;
                    crate::boundary::try_push_vec(&mut line_offsets, point_count)
                        .map_err(NativeArrowExportError::from)?;
                    crate::boundary::try_push_vec(&mut lines, line)
                        .map_err(NativeArrowExportError::from)?;
                },
            }
        }
    }
    let coords = gather_coordseqs(
        axes,
        point_count as usize,
        lines.into_iter().map(LineSeq::as_coords),
    )?;
    Ok(StoragePlan::List {
        offsets: geometry_offsets.into(),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: line_offsets.into(),
            start: 0,
            length: line_count as usize,
            child: Box::new(StoragePlan::Coordinates(coords)),
        }),
    })
}

fn plan_polygons(shapes: &[&Shape], axes: CoordinateAxes) -> NativeArrowExportResult<StoragePlan> {
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut rings = Vec::new();
    let mut ring_count = 0_i32;
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::Polygon(polygon) = shape else {
            unreachable!()
        };
        ring_count = ring_count
            .checked_add(i32::try_from(polygon.holes.len() + 1).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        crate::boundary::try_push_vec(&mut polygon_offsets, ring_count)
            .map_err(NativeArrowExportError::from)?;
        push_polygon_plan(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    Ok(StoragePlan::List {
        offsets: polygon_offsets.into(),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: ring_offsets.into(),
            start: 0,
            length: ring_count as usize,
            child: Box::new(StoragePlan::Coordinates(coords)),
        }),
    })
}

fn plan_multipolygons(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut multipolygon_offsets = vec![0_i32];
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut rings = Vec::new();
    let mut polygon_count = 0_i32;
    let mut ring_count = 0_i32;
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::MultiPolygon(polygons) = shape else {
            unreachable!()
        };
        polygon_count = polygon_count
            .checked_add(i32::try_from(polygons.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        crate::boundary::try_push_vec(&mut multipolygon_offsets, polygon_count)
            .map_err(NativeArrowExportError::from)?;
        for member in polygons.members() {
            match member {
                crate::geometry::MultiMember::Empty => {
                    crate::boundary::try_push_vec(&mut polygon_offsets, ring_count)
                        .map_err(NativeArrowExportError::from)?
                },
                crate::geometry::MultiMember::Value(index) => {
                    let polygon = &polygons.as_slice()[index];
                    ring_count = ring_count
                        .checked_add(
                            i32::try_from(polygon.holes.len() + 1).map_err(|_| offset_error())?,
                        )
                        .ok_or_else(offset_error)?;
                    crate::boundary::try_push_vec(&mut polygon_offsets, ring_count)
                        .map_err(NativeArrowExportError::from)?;
                    push_polygon_plan(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
                },
            }
        }
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    Ok(StoragePlan::List {
        offsets: multipolygon_offsets.into(),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: polygon_offsets.into(),
            start: 0,
            length: polygon_count as usize,
            child: Box::new(StoragePlan::List {
                offsets: ring_offsets.into(),
                start: 0,
                length: ring_count as usize,
                child: Box::new(StoragePlan::Coordinates(coords)),
            }),
        }),
    })
}

fn push_polygon_plan<'a>(
    polygon: &'a Polygon,
    rings: &mut Vec<&'a CoordSeq>,
    ring_offsets: &mut Vec<i32>,
    point_count: &mut i32,
) -> NativeArrowExportResult<()> {
    for ring in polygon.rings() {
        *point_count = point_count
            .checked_add(i32::try_from(ring.len()).map_err(|_| offset_error())?)
            .ok_or_else(offset_error)?;
        crate::boundary::try_push_vec(ring_offsets, *point_count)
            .map_err(NativeArrowExportError::from)?;
        crate::boundary::try_push_vec(rings, ring).map_err(NativeArrowExportError::from)?;
    }
    Ok(())
}

fn gather_coordseqs<'a>(
    axes: CoordinateAxes,
    total: usize,
    seqs: impl Iterator<Item = &'a CoordSeq>,
) -> NativeArrowExportResult<CoordSeq> {
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::boundary::try_reserve_vec(&mut xs, total).map_err(NativeArrowExportError::from)?;
    crate::boundary::try_reserve_vec(&mut ys, total).map_err(NativeArrowExportError::from)?;
    let mut zs = axes.has_z().then(Vec::new);
    let mut ms = axes.has_m().then(Vec::new);
    if let Some(values) = &mut zs {
        crate::boundary::try_reserve_vec(values, total).map_err(NativeArrowExportError::from)?;
    }
    if let Some(values) = &mut ms {
        crate::boundary::try_reserve_vec(values, total).map_err(NativeArrowExportError::from)?;
    }
    for seq in seqs {
        xs.extend_from_slice(seq.xs());
        ys.extend_from_slice(seq.ys());
        if let Some(values) = &mut zs {
            values.extend_from_slice(seq.zs().ok_or_else(mixed_axes_error)?);
        }
        if let Some(values) = &mut ms {
            values.extend_from_slice(seq.ms().ok_or_else(mixed_axes_error)?);
        }
    }
    Ok(CoordSeq::from_columns(
        xs.into(),
        ys.into(),
        zs.map(Into::into),
        ms.map(Into::into),
    ))
}

fn plan_wkb_refs(
    shapes: &[&Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(crate::io::wkb_len(shape, false)?)
            .ok_or_else(|| NativeArrowExportError::overflow("WKB size overflows usize"))
    })?;
    let mut data = Vec::new();
    crate::boundary::try_reserve_vec(&mut data, total).map_err(NativeArrowExportError::from)?;
    let mut offsets = Vec::new();
    crate::boundary::try_reserve_vec(
        &mut offsets,
        shapes
            .len()
            .checked_add(1)
            .ok_or_else(|| NativeArrowExportError::overflow("WKB offset count overflows usize"))?,
    )
    .map_err(NativeArrowExportError::from)?;
    crate::boundary::try_push_vec(&mut offsets, 0_i32).map_err(NativeArrowExportError::from)?;
    for shape in shapes {
        crate::io::write_wkb_to(&mut data, shape, crs.as_deref(), false)?;
        crate::boundary::try_push_vec(
            &mut offsets,
            i32::try_from(data.len()).map_err(|_| offset_error())?,
        )
        .map_err(NativeArrowExportError::from)?;
    }
    validate_binary_offsets(&offsets, data.len())?;
    Ok(ArrowExportPlan {
        encoding: GeometryEncoding::Wkb,
        storage: StoragePlan::Binary {
            offsets: offsets.into(),
            data: data.into(),
        },
        crs,
        epoch,
        validity: None,
    })
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

fn validate_list_offsets(
    offsets: &[i32],
    start: usize,
    length: usize,
    child_len: usize,
) -> NativeArrowExportResult<()> {
    let end = length
        .checked_add(1)
        .and_then(|count| start.checked_add(count))
        .ok_or_else(|| {
            NativeArrowExportError::geoarrow(
                "Arrow offsets buffer is shorter than declared array length",
            )
        })?;
    if end > offsets.len() {
        return Err(NativeArrowExportError::geoarrow(
            "Arrow offsets buffer is shorter than declared array length",
        ));
    }
    let mut previous = usize::try_from(offsets[start])
        .map_err(|_| NativeArrowExportError::geoarrow("Arrow offsets must be non-negative"))?;
    for index in 1..=length {
        let next = usize::try_from(offsets[start + index])
            .map_err(|_| NativeArrowExportError::geoarrow("Arrow offsets must be non-negative"))?;
        if previous > next {
            return Err(NativeArrowExportError::geoarrow(
                "Arrow offsets must be ordered",
            ));
        }
        previous = next;
    }
    if previous > child_len {
        return Err(NativeArrowExportError::geoarrow(format!(
            "Arrow offset terminal ({previous}) exceeds child length ({child_len})"
        )));
    }
    Ok(())
}

fn validate_binary_offsets(offsets: &[i32], child_len: usize) -> NativeArrowExportResult<()> {
    validate_list_offsets(
        offsets,
        0,
        offsets.len().checked_sub(1).unwrap_or(0),
        child_len,
    )
}

fn plan_validity(
    mask: &crate::array::ValidityMask,
    offset: usize,
) -> NativeArrowExportResult<ValidityPlan> {
    let (owned, mask_offset) = mask.arrow_bitmap();
    let total_bits = offset
        .checked_add(mask.len())
        .ok_or_else(|| NativeArrowExportError::overflow("Arrow validity span exceeds usize"))?;
    let bitmap = if mask_offset == offset && owned.len().saturating_mul(8) >= total_bits {
        owned
    } else {
        let mut values = Vec::new();
        crate::boundary::try_reserve_vec(&mut values, total_bits.div_ceil(8))
            .map_err(NativeArrowExportError::from)?;
        values.resize(total_bits.div_ceil(8), 0);
        for row in 0..mask.len() {
            if mask.is_valid(row) {
                let bit = offset + row;
                values[bit / 8] |= 1 << (bit % 8);
            }
        }
        values.into()
    };
    Ok(ValidityPlan {
        bitmap,
        null_count: mask.missing_count(),
    })
}

fn offset_error() -> NativeArrowExportError {
    NativeArrowExportError::overflow(
        "Arrow output exceeds the i32 offset capacity for binary/list arrays",
    )
}

fn mixed_axes_error() -> NativeArrowExportError {
    NativeArrowExportError::invalid_geometry("mixed Z/M coordinate axes")
}

fn packed_columns_error(error: crate::array::PackedColumnError) -> NativeArrowExportError {
    match error {
        crate::array::PackedColumnError::Batch(error) => error.into(),
        crate::array::PackedColumnError::Row { row, error } => {
            NativeArrowExportError::CoreRow { row, error }
        },
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;

    #[test]
    fn native_export_errors_keep_attached_exception_classification() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let overflow = offset_error().into_py_err(py);
            assert!(overflow.is_instance_of::<pyo3::exceptions::PyOverflowError>(py));
            assert!(overflow.value(py).to_string().contains("i32 offset"));

            let geoarrow = validate_list_offsets(&[0, 2, 1], 0, 2, 2)
                .expect_err("decreasing offsets must fail")
                .into_py_err(py);
            assert!(geoarrow.is_instance_of::<crate::py::errors::ParseError>(py));
            assert_eq!(
                geoarrow
                    .value(py)
                    .getattr("format")
                    .expect("parse format")
                    .extract::<String>()
                    .expect("parse format text"),
                "geoarrow"
            );
            assert!(geoarrow.value(py).to_string().contains("must be ordered"));

            let generic = NativeArrowExportError::geometry("epoch is invalid").into_py_err(py);
            assert!(generic.is_instance_of::<crate::py::errors::GeometryError>(py));

            let invalid = mixed_axes_error().into_py_err(py);
            assert!(invalid.is_instance_of::<crate::py::errors::InvalidGeometryError>(py));

            let core: crate::error::Error =
                crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into();
            let core = NativeArrowExportError::from(core).into_py_err(py);
            assert!(core.is_instance_of::<crate::py::errors::GeometryError>(py));
            assert!(core.value(py).to_string().contains("i32 offset"));
        });
    }
}
