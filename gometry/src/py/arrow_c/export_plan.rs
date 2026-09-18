#![expect(
    clippy::absolute_paths,
    reason = "Arrow planning is kept beside the C exporter and its native types"
)]

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;

use pyo3::prelude::PyResult;

use crate::PyGeometryArray;
use crate::array::{ArrayRow, GeometryArrayStorage, PackedStorage, RowSelectionRef};
use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, Polygon, Shape};
use crate::py::arrow::{ArrowEncoding, accumulate_geometry_axes};
use crate::py::arrow_c::{
    ExportedArray, GeometryEncoding, GometryArrowArray, NativeArrowExportError,
    NativeArrowExportResult, SchemaNode, admitted_binary_array, admitted_list_array_windowed,
    coordinate_array, coordinate_schema, extension_schema, list_schema, wkb_schema,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GeometrySchema {
    encoding: GeometryEncoding,
    axes: CoordinateAxes,
}

// GeoArrow's public storage types use PyArrow's default nullable field
// contract, including coordinate and nested-list children.
const GEOARROW_SCHEMA_FIELDS_NULLABLE: bool = true;

/// Borrowed facts needed to describe a public GeoArrow schema.
///
/// This deliberately contains no coordinate, offset, validity, or WKB owner.
/// A full export converts it to an owned description only after this decision
/// is complete; schema-only export materializes the description while its
/// metadata borrows the source frame.
#[derive(Clone, Debug)]
pub(crate) struct SchemaDescription<'a> {
    layout: GeometrySchema,
    nullable: bool,
    crs: Option<Cow<'a, str>>,
    epoch: Option<f64>,
}

impl<'a> SchemaDescription<'a> {
    fn borrowed(layout: GeometrySchema, crs: Option<&'a str>, epoch: Option<f64>) -> Self {
        Self {
            layout,
            nullable: GEOARROW_SCHEMA_FIELDS_NULLABLE,
            crs: crs.map(Cow::Borrowed),
            epoch,
        }
    }

    fn owned(
        layout: GeometrySchema,
        crs: Option<String>,
        epoch: Option<f64>,
    ) -> SchemaDescription<'static> {
        SchemaDescription {
            layout,
            nullable: GEOARROW_SCHEMA_FIELDS_NULLABLE,
            crs: crs.map(Cow::Owned),
            epoch,
        }
    }

    fn into_owned(self) -> NativeArrowExportResult<SchemaDescription<'static>> {
        let crs = match self.crs {
            None => None,
            Some(Cow::Owned(value)) => Some(value),
            Some(Cow::Borrowed(value)) => {
                Some(crate::boundary::try_string_from_str(value).map_err(
                    |()| -> NativeArrowExportError {
                        crate::error::Error::from(crate::error::ResourceError::Allocation).into()
                    },
                )?)
            },
        };
        Ok(SchemaDescription {
            layout: self.layout,
            nullable: self.nullable,
            crs: crs.map(Cow::Owned),
            epoch: self.epoch,
        })
    }

    #[expect(
        clippy::match_same_arms,
        reason = "MultiLineString and Polygon intentionally share a two-level list schema while the encoding remains distinct in extension metadata"
    )]
    pub(crate) fn materialize_schema(&self) -> NativeArrowExportResult<SchemaNode> {
        let storage = match self.layout.encoding {
            GeometryEncoding::Point => coordinate_schema(self.layout.axes, self.nullable),
            GeometryEncoding::MultiPoint | GeometryEncoding::LineString => list_schema(
                coordinate_schema(self.layout.axes, self.nullable)?,
                self.nullable,
            ),
            GeometryEncoding::MultiLineString => list_schema(
                list_schema(
                    coordinate_schema(self.layout.axes, self.nullable)?,
                    self.nullable,
                )?,
                self.nullable,
            ),
            GeometryEncoding::Polygon => list_schema(
                list_schema(
                    coordinate_schema(self.layout.axes, self.nullable)?,
                    self.nullable,
                )?,
                self.nullable,
            ),
            GeometryEncoding::MultiPolygon => Ok(list_schema(
                list_schema(
                    list_schema(
                        coordinate_schema(self.layout.axes, self.nullable)?,
                        self.nullable,
                    )?,
                    self.nullable,
                )?,
                self.nullable,
            )?),
            GeometryEncoding::Wkb => Ok(wkb_schema(self.nullable)),
        }?;
        extension_schema(
            self.layout.encoding,
            storage,
            self.crs.as_deref(),
            self.epoch,
        )
    }
}

#[cfg(test)]
thread_local! {
    static PAYLOAD_BUILDERS: Cell<usize> = const { Cell::new(0) };
    static PAYLOAD_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_payload_events() {
    PAYLOAD_BUILDERS.with(|count| count.set(0));
    PAYLOAD_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn payload_events() -> (usize, usize) {
    (
        PAYLOAD_BUILDERS.with(Cell::get),
        PAYLOAD_MATERIALIZATIONS.with(Cell::get),
    )
}

#[cfg(test)]
fn record_payload_builder() {
    PAYLOAD_BUILDERS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn record_payload_materialization() {
    PAYLOAD_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
}

fn initial_offsets() -> crate::error::Result<Vec<i32>> {
    let mut offsets = Vec::new();
    crate::try_push_vec(&mut offsets, 0)?;
    Ok(offsets)
}

enum StoragePlan {
    Coordinates(CoordSeq),
    List {
        offsets: crate::geometry::SharedSlice<i32>,
        start: usize,
        length: usize,
        child: Box<Self>,
    },
    Binary {
        offsets: crate::geometry::SharedSlice<i32>,
        data: crate::geometry::SharedSlice<u8>,
    },
}

impl StoragePlan {
    const fn offset(&self) -> usize {
        match self {
            Self::List { start, .. } => *start,
            _ => 0,
        }
    }

    fn materialize(self) -> PyResult<GometryArrowArray> {
        #[cfg(test)]
        record_payload_materialization();
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
    bitmap: crate::geometry::SharedSlice<u8>,
    null_count: usize,
}

pub(crate) struct ArrowExportPlan {
    schema: SchemaDescription<'static>,
    storage: StoragePlan,
    validity: Option<ValidityPlan>,
}

pub(crate) struct PreparedArrowExportPlan {
    plan: ArrowExportPlan,
    schema: SchemaNode,
}

pub(crate) fn prepare_export_plan(
    plan: ArrowExportPlan,
) -> NativeArrowExportResult<PreparedArrowExportPlan> {
    let schema = plan.schema.materialize_schema()?;
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

#[cfg(test)]
pub(crate) fn materialize_schema_plan(
    plan: PreparedArrowExportPlan,
) -> PyResult<crate::py::arrow_c::GometryArrowSchema> {
    plan.schema
        .materialize_schema()
        .map_err(|error| pyo3::Python::attach(|py| error.into_py_err(py)))
}

pub(crate) fn describe_geometry_array(
    array: &PyGeometryArray,
    encoding: ArrowEncoding,
) -> SchemaDescription<'_> {
    let layout = match encoding {
        ArrowEncoding::Auto => match array.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => GeometrySchema {
                encoding: GeometryEncoding::Point,
                axes: coords.axes(),
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines { coords, .. }) => GeometrySchema {
                encoding: GeometryEncoding::LineString,
                axes: coords.axes(),
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons { coords, .. }) => {
                GeometrySchema {
                    encoding: GeometryEncoding::Polygon,
                    axes: coords.axes(),
                }
            },
            GeometryArrayStorage::Mixed(shapes) => describe_shape_iter(shapes.iter()),
        },
        ArrowEncoding::Wkb => GeometrySchema {
            encoding: GeometryEncoding::Wkb,
            axes: CoordinateAxes::XY,
        },
    };
    SchemaDescription::borrowed(layout, array.crs_str(), array.epoch())
}

#[cfg(test)]
pub(crate) fn describe_shapes<'a>(
    shapes: &[Shape],
    crs: Option<&'a str>,
    epoch: Option<f64>,
) -> SchemaDescription<'a> {
    SchemaDescription::borrowed(describe_shape_iter(shapes.iter()), crs, epoch)
}

pub(crate) fn describe_shape<'a>(
    shape: &Shape,
    crs: Option<&'a str>,
    epoch: Option<f64>,
) -> SchemaDescription<'a> {
    SchemaDescription::borrowed(describe_shape_iter(std::iter::once(shape)), crs, epoch)
}

pub(crate) fn plan_geometry_array(
    array: &PyGeometryArray,
    encoding: ArrowEncoding,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let schema = describe_geometry_array(array, encoding).into_owned()?;
    #[cfg(test)]
    record_payload_builder();
    let storage = match encoding {
        ArrowEncoding::Auto => plan_geometry_array_auto(array, schema.layout)?,
        ArrowEncoding::Wkb => plan_geometry_array_wkb(array)?,
    };
    let mut plan = ArrowExportPlan {
        schema,
        storage,
        validity: None,
    };
    if let Some(mask) = array.missing() {
        plan.validity = Some(plan_validity(mask, plan.storage.offset())?);
    }
    Ok(plan)
}

fn plan_geometry_array_auto(
    array: &PyGeometryArray,
    layout: GeometrySchema,
) -> NativeArrowExportResult<StoragePlan> {
    if let Some(coords) = array.materialized_compact_point_coords()? {
        return Ok(StoragePlan::Coordinates(coords));
    }
    let gathered =
        crate::array::materialized_gather(array.storage()).map_err(packed_columns_error)?;
    let storage = gathered.as_ref().unwrap_or_else(|| array.storage());
    plan_storage(storage, layout)
}

fn plan_geometry_array_wkb(array: &PyGeometryArray) -> NativeArrowExportResult<StoragePlan> {
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
                .try_with_shape(|shape| crate::io::wkb_len(shape, false))?;
            total = total
                .checked_add(size)
                .ok_or_else(|| NativeArrowExportError::overflow("WKB size overflows usize"))?;
        }
    }
    validate_wkb_total(total)?;
    crate::boundary::try_reserve_vec(&mut data, total).map_err(NativeArrowExportError::from)?;
    crate::boundary::try_push_vec(&mut offsets, 0_i32).map_err(NativeArrowExportError::from)?;
    for row in array.row_operation().rows_with_missing(|| ()) {
        if let ArrayRow::Present(row) = row {
            row.row()
                .try_with_shape(|shape| crate::io::write_wkb_to_srid(&mut data, shape, None))?;
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
    Ok(StoragePlan::Binary {
        offsets: crate::geometry::SharedSlice::try_from_vec(offsets)?,
        data: crate::geometry::SharedSlice::try_from_vec(data)?,
    })
}

fn plan_storage(
    storage: &GeometryArrayStorage,
    layout: GeometrySchema,
) -> NativeArrowExportResult<StoragePlan> {
    match storage {
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let coords = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => coords.as_ref().clone(),
                RowSelectionRef::Window { start, len } => coords.view(
                    crate::geometry::CoordWindow::from_range(start..start + len, coords.len()),
                ),
            };
            Ok(StoragePlan::Coordinates(coords))
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) => {
            let (start, length) = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
                    (0, offsets.len().saturating_sub(1))
                },
                RowSelectionRef::Window { start, len } => (start, len),
            };
            validate_list_offsets(offsets.as_slice(), start, length, coords.len())?;
            Ok(StoragePlan::List {
                offsets: offsets.shared_offsets(),
                start,
                length,
                child: Box::try_new(StoragePlan::Coordinates(coords.as_ref().clone()))
                    .map_err(crate::error::Error::from)?,
            })
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) => {
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
                offsets: ring_offsets.shared_offsets(),
                start: 0,
                length: ring_offsets.len().saturating_sub(1),
                child: Box::try_new(StoragePlan::Coordinates(coords.as_ref().clone()))
                    .map_err(crate::error::Error::from)?,
            };
            Ok(StoragePlan::List {
                offsets: polygon_offsets.shared_offsets(),
                start,
                length,
                child: Box::try_new(rings).map_err(crate::error::Error::from)?,
            })
        },
        GeometryArrayStorage::Mixed(shapes) => {
            let mut refs = Vec::new();
            crate::try_reserve_vec(&mut refs, shapes.len())?;
            refs.extend(shapes.iter());
            plan_shape_refs_with_layout(&refs, layout)
        },
    }
}

pub(crate) fn plan_shapes(
    shapes: &[Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let mut refs = Vec::new();
    crate::try_reserve_vec(&mut refs, shapes.len())?;
    refs.extend(shapes.iter());
    let schema = SchemaDescription::owned(describe_shape_iter(refs.iter().copied()), crs, epoch);
    #[cfg(test)]
    record_payload_builder();
    let storage = plan_shape_refs_with_layout(&refs, schema.layout)?;
    Ok(ArrowExportPlan {
        schema,
        storage,
        validity: None,
    })
}

pub(crate) fn plan_wkb_shapes(
    shapes: &[Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let mut refs = Vec::new();
    crate::try_reserve_vec(&mut refs, shapes.len())?;
    refs.extend(shapes.iter());
    let layout = GeometrySchema {
        encoding: GeometryEncoding::Wkb,
        axes: CoordinateAxes::XY,
    };
    #[cfg(test)]
    record_payload_builder();
    let storage = plan_wkb_refs(&refs)?;
    Ok(ArrowExportPlan {
        schema: SchemaDescription::owned(layout, crs, epoch),
        storage,
        validity: None,
    })
}

fn describe_shape_iter<'a>(mut shapes: impl Iterator<Item = &'a Shape> + Clone) -> GeometrySchema {
    let mut axes = None;
    for shape in shapes.clone() {
        if !accumulate_geometry_axes(shape, &mut axes) {
            return GeometrySchema {
                encoding: GeometryEncoding::Wkb,
                axes: CoordinateAxes::XY,
            };
        }
    }
    let axes = axes.unwrap_or(CoordinateAxes::XY);
    let Some(first) = shapes.next() else {
        return GeometrySchema {
            encoding: GeometryEncoding::Point,
            axes,
        };
    };
    // Top-level typed empties remain WKB; native packing admits concrete
    // storage variants, while multipart values retain their empty members.
    let encoding = match first {
        Shape::Point(_) if shapes.clone().all(|shape| matches!(shape, Shape::Point(_))) => {
            GeometryEncoding::Point
        },
        Shape::MultiPoint(points)
            if shapes
                .clone()
                .all(|shape| matches!(shape, Shape::MultiPoint(_))) =>
        {
            let has_empty_members = points.has_empty_members()
                || shapes.clone().any(|shape| {
                    matches!(shape, Shape::MultiPoint(points) if points.has_empty_members())
                });
            if has_empty_members {
                GeometryEncoding::Wkb
            } else {
                GeometryEncoding::MultiPoint
            }
        },
        Shape::LineString(_)
            if shapes
                .clone()
                .all(|shape| matches!(shape, Shape::LineString(_))) =>
        {
            GeometryEncoding::LineString
        },
        Shape::MultiLineString(_)
            if shapes
                .clone()
                .all(|shape| matches!(shape, Shape::MultiLineString(_))) =>
        {
            GeometryEncoding::MultiLineString
        },
        Shape::Polygon(_)
            if shapes
                .clone()
                .all(|shape| matches!(shape, Shape::Polygon(_))) =>
        {
            GeometryEncoding::Polygon
        },
        Shape::MultiPolygon(_)
            if shapes
                .clone()
                .all(|shape| matches!(shape, Shape::MultiPolygon(_))) =>
        {
            GeometryEncoding::MultiPolygon
        },
        _ => GeometryEncoding::Wkb,
    };
    GeometrySchema { encoding, axes }
}

fn plan_shape_refs_with_layout(
    shapes: &[&Shape],
    layout: GeometrySchema,
) -> NativeArrowExportResult<StoragePlan> {
    match layout.encoding {
        GeometryEncoding::Point => {
            let mut points = Vec::new();
            crate::boundary::try_reserve_vec(&mut points, shapes.len())
                .map_err(NativeArrowExportError::from)?;
            for shape in shapes {
                let Shape::Point(point) = shape else {
                    unreachable!("schema description selected point payload")
                };
                crate::boundary::try_push_vec(&mut points, *point)
                    .map_err(NativeArrowExportError::from)?;
            }
            Ok(StoragePlan::Coordinates(CoordSeq::try_from_points(
                &points,
            )?))
        },
        GeometryEncoding::MultiPoint => {
            plan_single_list(shapes, layout.axes, |shape| match shape {
                Shape::MultiPoint(points) => points,
                _ => unreachable!("schema description selected multipoint payload"),
            })
        },
        GeometryEncoding::LineString => {
            plan_single_list(shapes, layout.axes, |shape| match shape {
                Shape::LineString(points) => points,
                _ => unreachable!("schema description selected linestring payload"),
            })
        },
        GeometryEncoding::MultiLineString => plan_multilines(shapes, layout.axes),
        GeometryEncoding::Polygon => plan_polygons(shapes, layout.axes),
        GeometryEncoding::MultiPolygon => plan_multipolygons(shapes, layout.axes),
        GeometryEncoding::Wkb => plan_wkb_refs(shapes),
    }
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
        offsets: crate::geometry::SharedSlice::try_from_vec(offsets)?,
        start: 0,
        length: shapes.len(),
        child: Box::try_new(StoragePlan::Coordinates(coords)).map_err(crate::error::Error::from)?,
    })
}

fn plan_multilines(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut geometry_offsets = initial_offsets()?;
    let mut line_offsets = initial_offsets()?;
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
                        .map_err(NativeArrowExportError::from)?;
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
        offsets: crate::geometry::SharedSlice::try_from_vec(geometry_offsets)?,
        start: 0,
        length: shapes.len(),
        child: Box::try_new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::try_from_vec(line_offsets)?,
            start: 0,
            length: line_count as usize,
            child: Box::try_new(StoragePlan::Coordinates(coords))
                .map_err(crate::error::Error::from)?,
        })
        .map_err(crate::error::Error::from)?,
    })
}

fn plan_polygons(shapes: &[&Shape], axes: CoordinateAxes) -> NativeArrowExportResult<StoragePlan> {
    let mut polygon_offsets = initial_offsets()?;
    let mut ring_offsets = initial_offsets()?;
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
        offsets: crate::geometry::SharedSlice::try_from_vec(polygon_offsets)?,
        start: 0,
        length: shapes.len(),
        child: Box::try_new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::try_from_vec(ring_offsets)?,
            start: 0,
            length: ring_count as usize,
            child: Box::try_new(StoragePlan::Coordinates(coords))
                .map_err(crate::error::Error::from)?,
        })
        .map_err(crate::error::Error::from)?,
    })
}

fn plan_multipolygons(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut multipolygon_offsets = initial_offsets()?;
    let mut polygon_offsets = initial_offsets()?;
    let mut ring_offsets = initial_offsets()?;
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
                        .map_err(NativeArrowExportError::from)?;
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
        offsets: crate::geometry::SharedSlice::try_from_vec(multipolygon_offsets)?,
        start: 0,
        length: shapes.len(),
        child: Box::try_new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::try_from_vec(polygon_offsets)?,
            start: 0,
            length: polygon_count as usize,
            child: Box::try_new(StoragePlan::List {
                offsets: crate::geometry::SharedSlice::try_from_vec(ring_offsets)?,
                start: 0,
                length: ring_count as usize,
                child: Box::try_new(StoragePlan::Coordinates(coords))
                    .map_err(crate::error::Error::from)?,
            })
            .map_err(crate::error::Error::from)?,
        })
        .map_err(crate::error::Error::from)?,
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
    Ok(CoordSeq::try_from_vecs(xs, ys, zs, ms)?)
}

fn plan_wkb_refs(shapes: &[&Shape]) -> NativeArrowExportResult<StoragePlan> {
    let total = shapes.iter().try_fold(0_usize, |total, shape| {
        total
            .checked_add(crate::io::wkb_len(shape, false)?)
            .ok_or_else(|| NativeArrowExportError::overflow("WKB size overflows usize"))
    })?;
    validate_wkb_total(total)?;
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
        crate::io::write_wkb_to_srid(&mut data, shape, None)?;
        crate::boundary::try_push_vec(
            &mut offsets,
            i32::try_from(data.len()).map_err(|_| offset_error())?,
        )
        .map_err(NativeArrowExportError::from)?;
    }
    validate_binary_offsets(&offsets, data.len())?;
    Ok(StoragePlan::Binary {
        offsets: crate::geometry::SharedSlice::try_from_vec(offsets)?,
        data: crate::geometry::SharedSlice::try_from_vec(data)?,
    })
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
    validate_list_offsets(offsets, 0, offsets.len().saturating_sub(1), child_len)
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
        crate::geometry::SharedSlice::try_from_vec(values)?
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

fn validate_wkb_total(total: usize) -> NativeArrowExportResult<()> {
    i32::try_from(total).map(|_| ()).map_err(|_| offset_error())
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

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn wkb_total_admission_rejects_i32_overflow_without_allocating() {
        let error = validate_wkb_total((i32::MAX as usize) + 1)
            .expect_err("WKB data total must fit Arrow i32 offsets");
        assert!(matches!(error, NativeArrowExportError::Overflow(_)));
        validate_wkb_total(i32::MAX as usize).unwrap();
    }

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

    #[test]
    fn borrowed_schema_crs_copy_reports_allocation_failure() {
        let shapes = vec![Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 2.0))];
        let crs = "EPSG:4326".to_owned();
        let description = describe_shapes(&shapes, Some(&crs), None);
        let error =
            crate::test_support::fail_allocations_after(0, || description.clone().into_owned())
                .expect_err("borrowed CRS copy must use the fallible allocator path");
        assert!(matches!(
            error,
            NativeArrowExportError::Core(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(
                        crate::error::ResourceError::Allocation
                    )
                )
        ));
    }
}
