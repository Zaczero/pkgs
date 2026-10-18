use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;

use pyo3::prelude::{PyResult, Python};

use crate::PyGeometryArray;
use crate::array::ArrayRow;
use crate::error::BatchError;
use crate::geometry::storage::gather::materialized_gather;
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, RowSelectionRef};
use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, Polygon, Shape, storage};
use crate::py::arrow::{ArrowEncoding, accumulate_geometry_axes};
#[cfg(test)]
use crate::py::arrow_c::GometryArrowSchema;
use crate::py::arrow_c::{
    ExportedArray, GeometryEncoding, GometryArrowArray, NativeArrowExportError,
    NativeArrowExportResult, SchemaNode, admitted_binary_array, admitted_list_array_windowed,
    apply_exported_validity_plan, coordinate_array, coordinate_schema, export_with_schema_node,
    extension_schema, list_schema, wkb_schema,
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

    fn into_owned(self) -> SchemaDescription<'static> {
        let crs = match self.crs {
            None => None,
            Some(Cow::Owned(value)) => Some(value),
            Some(Cow::Borrowed(value)) => Some(value.to_owned()),
        };
        SchemaDescription {
            layout: self.layout,
            nullable: self.nullable,
            crs: crs.map(Cow::Owned),
            epoch: self.epoch,
        }
    }

    #[expect(
        clippy::match_same_arms,
        reason = "MultiLineString and Polygon intentionally share a two-level list schema while the encoding remains distinct in extension metadata"
    )]
    pub(crate) fn materialize_schema(&self) -> NativeArrowExportResult<SchemaNode> {
        let storage = match self.layout.encoding {
            GeometryEncoding::Point => coordinate_schema(self.layout.axes, self.nullable),
            GeometryEncoding::MultiPoint | GeometryEncoding::LineString => list_schema(
                coordinate_schema(self.layout.axes, self.nullable),
                self.nullable,
            ),
            GeometryEncoding::MultiLineString => list_schema(
                list_schema(
                    coordinate_schema(self.layout.axes, self.nullable),
                    self.nullable,
                ),
                self.nullable,
            ),
            GeometryEncoding::Polygon => list_schema(
                list_schema(
                    coordinate_schema(self.layout.axes, self.nullable),
                    self.nullable,
                ),
                self.nullable,
            ),
            GeometryEncoding::MultiPolygon => list_schema(
                list_schema(
                    list_schema(
                        coordinate_schema(self.layout.axes, self.nullable),
                        self.nullable,
                    ),
                    self.nullable,
                ),
                self.nullable,
            ),
            GeometryEncoding::Wkb => wkb_schema(self.nullable),
        };
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
    PAYLOAD_BUILDERS.with(|count| count.set(count.get().wrapping_add(1)));
}

#[cfg(test)]
fn record_payload_materialization() {
    PAYLOAD_MATERIALIZATIONS.with(|count| count.set(count.get().wrapping_add(1)));
}

fn initial_offsets(rows: usize) -> Vec<i32> {
    let mut offsets = Vec::with_capacity(rows.saturating_add(1));
    offsets.push(0);
    offsets
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
            Self::Coordinates(_) | Self::Binary { .. } => 0,
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

pub(crate) fn materialize_export_plan(
    py: Python<'_>,
    plan: PreparedArrowExportPlan,
) -> PyResult<ExportedArray> {
    let PreparedArrowExportPlan { plan, schema } = plan;
    let ArrowExportPlan {
        storage, validity, ..
    } = plan;
    let mut export = export_with_schema_node(schema, storage.materialize()?)
        .map_err(|error| error.into_py_err(py))?;
    if let Some(validity) = validity {
        apply_exported_validity_plan(&mut export, validity.bitmap, validity.null_count)?;
    }
    Ok(export)
}

#[cfg(test)]
pub(crate) fn materialize_schema_plan(
    plan: &PreparedArrowExportPlan,
) -> PyResult<GometryArrowSchema> {
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
    let schema = describe_geometry_array(array, encoding).into_owned();
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
    let gathered = materialized_gather(array.storage()).map_err(packed_columns_error)?;
    let storage = gathered.as_ref().unwrap_or_else(|| array.storage());
    plan_storage(storage, layout)
}

fn plan_geometry_array_wkb(array: &PyGeometryArray) -> NativeArrowExportResult<StoragePlan> {
    let sizes = array
        .row_operation()
        .rows_with_missing(|| ())
        .filter_map(|row| match row {
            ArrayRow::Present(row) => {
                let logical = row.logical();
                Some(
                    row.row()
                        .try_with_shape(|shape| crate::io::wkb_len(shape, false))
                        .map_err(|error| NativeArrowExportError::CoreRow {
                            row: logical,
                            error,
                        }),
                )
            },
            ArrayRow::Missing { .. } => None,
        });
    let total = checked_wkb_total(sizes)? as usize;
    let mut data = Vec::with_capacity(total);
    let mut offsets = initial_offsets(array.len());
    for row in array.row_operation().rows_with_missing(|| ()) {
        if let ArrayRow::Present(row) = row {
            let logical = row.logical();
            row.row()
                .try_with_shape(|shape| crate::io::write_wkb_to_srid(&mut data, shape, None))
                .map_err(|error| NativeArrowExportError::CoreRow {
                    row: logical,
                    error,
                })?;
        }
        offsets.push(i32::try_from(data.len()).map_err(|_| {
            NativeArrowExportError::overflow(
                "Arrow output exceeds the i32 offset capacity for binary/list arrays",
            )
        })?);
    }
    validate_binary_offsets(&offsets, data.len())?;
    Ok(StoragePlan::Binary {
        offsets: crate::geometry::SharedSlice::from_vec(offsets),
        data: crate::geometry::SharedSlice::from_vec(data),
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
                RowSelectionRef::Window { start, len } => {
                    coords.view(crate::geometry::CoordWindow::from_range(
                        start..start.wrapping_add(len),
                        coords.len(),
                    ))
                },
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
                child: Box::new(StoragePlan::Coordinates(coords.as_ref().clone())),
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
                child: Box::new(StoragePlan::Coordinates(coords.as_ref().clone())),
            };
            Ok(StoragePlan::List {
                offsets: polygon_offsets.shared_offsets(),
                start,
                length,
                child: Box::new(rings),
            })
        },
        GeometryArrayStorage::Mixed(shapes) => {
            let refs: Vec<_> = shapes.iter().collect();
            plan_shape_refs_with_layout(&refs, layout)
        },
    }
}

pub(crate) fn plan_shapes(
    shapes: &[Shape],
    crs: Option<String>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<ArrowExportPlan> {
    let refs: Vec<_> = shapes.iter().collect();
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
    let refs: Vec<_> = shapes.iter().collect();
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
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => GeometryEncoding::Wkb,
    };
    GeometrySchema { encoding, axes }
}

fn plan_shape_refs_with_layout(
    shapes: &[&Shape],
    layout: GeometrySchema,
) -> NativeArrowExportResult<StoragePlan> {
    match layout.encoding {
        GeometryEncoding::Point => {
            let mut points = Vec::with_capacity(shapes.len());
            for shape in shapes {
                let Shape::Point(point) = shape else {
                    unreachable!("schema description selected point payload")
                };
                points.push(*point);
            }
            Ok(StoragePlan::Coordinates(CoordSeq::try_from_points(
                &points,
            )?))
        },
        GeometryEncoding::MultiPoint => {
            plan_single_list(shapes, layout.axes, |shape| match shape {
                Shape::MultiPoint(points) => points,
                Shape::Point(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => {
                    unreachable!("schema description selected multipoint payload")
                },
            })
        },
        GeometryEncoding::LineString => {
            plan_single_list(shapes, layout.axes, |shape| match shape {
                Shape::LineString(points) => points,
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => {
                    unreachable!("schema description selected linestring payload")
                },
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
    let mut offsets = initial_offsets(shapes.len());
    let mut count = 0_i32;
    for shape in shapes {
        count = checked_i32_offset_total(count, points_of(shape).len())?;
        offsets.push(count);
    }
    let total = count as usize;
    let coords = gather_coordseqs(axes, total, shapes.iter().map(|shape| points_of(shape)))?;
    Ok(StoragePlan::List {
        offsets: crate::geometry::SharedSlice::from_vec(offsets),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::Coordinates(coords)),
    })
}

fn plan_multilines(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut geometry_offsets = initial_offsets(shapes.len());
    let (total_lines, total_stored_lines) = shapes.iter().try_fold(
        (0_i32, 0_usize),
        |(total_lines, total_stored_lines), shape| {
            let Shape::MultiLineString(lines) = shape else {
                unreachable!()
            };
            let total_lines = checked_i32_offset_total(total_lines, lines.member_count())?;
            // Stored values are a subset of the checked member count.
            let total_stored_lines = total_stored_lines.saturating_add(lines.as_slice().len());
            geometry_offsets.push(total_lines);
            Ok::<_, NativeArrowExportError>((total_lines, total_stored_lines))
        },
    )?;
    let total_lines = total_lines as usize;
    let mut line_offsets = initial_offsets(total_lines);
    let mut lines = Vec::with_capacity(total_stored_lines);
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::MultiLineString(items) = shape else {
            unreachable!()
        };
        let mut stored_lines = items.as_slice().iter();
        for member in items.members() {
            match member {
                crate::geometry::MultiMember::Empty => {
                    line_offsets.push(point_count);
                },
                crate::geometry::MultiMember::Value(_) => {
                    let line = stored_lines
                        .next()
                        .expect("multipart value indices are encounter-ordered");
                    point_count = checked_i32_offset_total(point_count, line.len())?;
                    line_offsets.push(point_count);
                    lines.push(line);
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
        offsets: crate::geometry::SharedSlice::from_vec(geometry_offsets),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::from_vec(line_offsets),
            start: 0,
            length: total_lines,
            child: Box::new(StoragePlan::Coordinates(coords)),
        }),
    })
}

fn plan_polygons(shapes: &[&Shape], axes: CoordinateAxes) -> NativeArrowExportResult<StoragePlan> {
    let mut polygon_offsets = initial_offsets(shapes.len());
    let total_rings = shapes.iter().try_fold(0_i32, |total, shape| {
        let Shape::Polygon(polygon) = shape else {
            unreachable!()
        };
        let total = checked_i32_offset_total(total, polygon_ring_count(polygon))?;
        polygon_offsets.push(total);
        Ok::<_, NativeArrowExportError>(total)
    })?;
    let total_rings = total_rings as usize;
    let mut ring_offsets = initial_offsets(total_rings);
    let mut rings = Vec::with_capacity(total_rings);
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::Polygon(polygon) = shape else {
            unreachable!()
        };
        push_polygon_plan(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    Ok(StoragePlan::List {
        offsets: crate::geometry::SharedSlice::from_vec(polygon_offsets),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::from_vec(ring_offsets),
            start: 0,
            length: total_rings,
            child: Box::new(StoragePlan::Coordinates(coords)),
        }),
    })
}

fn plan_multipolygons(
    shapes: &[&Shape],
    axes: CoordinateAxes,
) -> NativeArrowExportResult<StoragePlan> {
    let mut multipolygon_offsets = initial_offsets(shapes.len());
    let (total_polygons, total_rings) =
        shapes
            .iter()
            .try_fold((0_i32, 0_i32), |(polygons, rings), shape| {
                let Shape::MultiPolygon(items) = shape else {
                    unreachable!()
                };
                let polygons = checked_i32_offset_total(polygons, items.member_count())?;
                multipolygon_offsets.push(polygons);
                let rings = items.iter().try_fold(rings, |rings, polygon| {
                    checked_i32_offset_total(rings, polygon_ring_count(polygon))
                })?;
                Ok::<_, NativeArrowExportError>((polygons, rings))
            })?;
    let total_polygons = total_polygons as usize;
    let total_rings = total_rings as usize;
    let mut polygon_offsets = initial_offsets(total_polygons);
    let mut ring_offsets = initial_offsets(total_rings);
    let mut rings = Vec::with_capacity(total_rings);
    let mut point_count = 0_i32;
    for shape in shapes {
        let Shape::MultiPolygon(polygons) = shape else {
            unreachable!()
        };
        let mut stored_polygons = polygons.as_slice().iter();
        for member in polygons.members() {
            if let crate::geometry::MultiMember::Value(_) = member {
                let polygon = stored_polygons
                    .next()
                    .expect("multipart member values are stored in encounter order");
                push_polygon_plan(polygon, &mut rings, &mut ring_offsets, &mut point_count)?;
            }
            // `initial_offsets` keeps this output column nonempty.
            let ring_count =
                i32::try_from(ring_offsets.len().wrapping_sub(1)).map_err(|_| offset_error())?;
            polygon_offsets.push(ring_count);
        }
    }
    let coords = gather_coordseqs(axes, point_count as usize, rings.into_iter())?;
    Ok(StoragePlan::List {
        offsets: crate::geometry::SharedSlice::from_vec(multipolygon_offsets),
        start: 0,
        length: shapes.len(),
        child: Box::new(StoragePlan::List {
            offsets: crate::geometry::SharedSlice::from_vec(polygon_offsets),
            start: 0,
            length: total_polygons,
            child: Box::new(StoragePlan::List {
                offsets: crate::geometry::SharedSlice::from_vec(ring_offsets),
                start: 0,
                length: total_rings,
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
        *point_count = checked_i32_offset_total(*point_count, ring.len())?;
        ring_offsets.push(*point_count);
        rings.push(ring);
    }
    Ok(())
}

fn polygon_ring_count(polygon: &Polygon) -> usize {
    polygon.holes.len().saturating_add(1)
}

fn gather_coordseqs<'a>(
    axes: CoordinateAxes,
    total: usize,
    seqs: impl Iterator<Item = &'a CoordSeq>,
) -> NativeArrowExportResult<CoordSeq> {
    let mut xs = Vec::with_capacity(total);
    let mut ys = Vec::with_capacity(total);
    let mut zs = axes.has_z().then(|| Vec::with_capacity(total));
    let mut ms = axes.has_m().then(|| Vec::with_capacity(total));
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
    let total = checked_wkb_total(shapes.iter().enumerate().map(|(logical, shape)| {
        crate::io::wkb_len(shape, false).map_err(|error| NativeArrowExportError::CoreRow {
            row: logical,
            error,
        })
    }))? as usize;
    let mut data = Vec::with_capacity(total);
    let mut offsets = initial_offsets(shapes.len());
    for (logical, shape) in shapes.iter().enumerate() {
        crate::io::write_wkb_to_srid(&mut data, shape, None).map_err(|error| {
            NativeArrowExportError::CoreRow {
                row: logical,
                error,
            }
        })?;
        offsets.push(i32::try_from(data.len()).map_err(|_| offset_error())?);
    }
    validate_binary_offsets(&offsets, data.len())?;
    Ok(StoragePlan::Binary {
        offsets: crate::geometry::SharedSlice::from_vec(offsets),
        data: crate::geometry::SharedSlice::from_vec(data),
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
    let window = offsets.get(start..end).ok_or_else(|| {
        NativeArrowExportError::geoarrow(
            "Arrow offsets buffer is shorter than declared array length",
        )
    })?;
    let (first, remaining) = window
        .split_first()
        .expect("checked Arrow offset window contains at least its initial offset");
    let mut previous = usize::try_from(*first)
        .map_err(|_| NativeArrowExportError::geoarrow("Arrow offsets must be non-negative"))?;
    for raw_next in remaining {
        let next = usize::try_from(*raw_next)
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
    mask: &storage::ValidityMask,
    offset: usize,
) -> NativeArrowExportResult<ValidityPlan> {
    let (owned, mask_offset) = mask.arrow_bitmap();
    let total_bits = offset
        .checked_add(mask.len())
        .ok_or_else(|| NativeArrowExportError::overflow("Arrow validity span exceeds usize"))?;
    let bitmap = if mask_offset == offset && owned.len().saturating_mul(8) >= total_bits {
        owned
    } else {
        let mut values = vec![0; total_bits.div_ceil(8)];
        let mut missing = mask.iter();
        let mut first_bit = offset % 8;
        'bytes: for byte in values.iter_mut().skip(offset / 8) {
            let mut packed = 0_u8;
            for bit in first_bit..8 {
                let Some(is_missing) = missing.next() else {
                    *byte = packed;
                    break 'bytes;
                };
                if !is_missing {
                    packed |= 1_u8 << bit;
                }
            }
            *byte = packed;
            first_bit = 0;
        }
        crate::geometry::SharedSlice::from_vec(values)
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

fn checked_i32_offset_total(total: i32, increment: usize) -> NativeArrowExportResult<i32> {
    let increment = i32::try_from(increment).map_err(|_| offset_error())?;
    total.checked_add(increment).ok_or_else(offset_error)
}

fn checked_wkb_total(
    mut sizes: impl Iterator<Item = NativeArrowExportResult<usize>>,
) -> NativeArrowExportResult<i32> {
    sizes.try_fold(0_i32, |total, size| checked_i32_offset_total(total, size?))
}

fn mixed_axes_error() -> NativeArrowExportError {
    NativeArrowExportError::invalid_geometry("mixed Z/M coordinate axes")
}

fn packed_columns_error(error: BatchError) -> NativeArrowExportError {
    match error {
        BatchError::Batch(error) => error.into(),
        BatchError::Row { row, error } => NativeArrowExportError::CoreRow { row, error },
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyOverflowError;
    use pyo3::prelude::*;

    use super::*;
    use crate::py::errors::{GeometryError, InvalidGeometryError, ParseError};

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn wkb_total_admission_rejects_i32_overflow_without_allocating() {
        let maximum = i32::MAX as usize;
        assert_eq!(
            checked_wkb_total([Ok(maximum)].into_iter()).unwrap(),
            i32::MAX
        );
        let error = checked_wkb_total([Ok(maximum), Ok(1)].into_iter())
            .expect_err("WKB data total must fit Arrow i32 offsets");
        assert!(matches!(error, NativeArrowExportError::Overflow(_)));
    }

    #[test]
    fn wkb_shape_width_preflight_retains_the_logical_row() {
        let mut wide = Shape::Point(crate::geometry::Point::new_raw_xy(0.0, 0.0));
        for _ in 0..60 {
            let child = wide.clone();
            wide = Shape::geometry_collection(vec![child.clone(), child])
                .expect("collection stays within the nesting limit");
        }
        let shapes = [
            Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 1.0)),
            wide,
        ];
        let refs = shapes.iter().collect::<Vec<_>>();

        let error = plan_wkb_refs(&refs)
            .err()
            .expect("the second shape exceeds the WKB byte limit");
        assert!(matches!(&error, NativeArrowExportError::CoreRow {
            row: 1,
            ..
        }));
        if let NativeArrowExportError::CoreRow { row, error } = error {
            assert_eq!(row, 1);
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
            ));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn native_export_errors_keep_attached_exception_classification() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let overflow = offset_error().into_py_err(py);
            assert!(overflow.is_instance_of::<pyo3::exceptions::PyOverflowError>(py));
            assert!(overflow.value(py).to_string().contains("i32 offset"));

            let geoarrow = validate_list_offsets(&[0, 2, 1], 0, 2, 2)
                .expect_err("decreasing offsets must fail")
                .into_py_err(py);
            assert!(geoarrow.is_instance_of::<ParseError>(py));
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
            assert!(generic.is_instance_of::<GeometryError>(py));

            let invalid = mixed_axes_error().into_py_err(py);
            assert!(invalid.is_instance_of::<InvalidGeometryError>(py));

            let core: crate::error::Error =
                crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into();
            let core = NativeArrowExportError::from(core).into_py_err(py);
            assert!(core.is_instance_of::<PyOverflowError>(py));
            assert!(!core.is_instance_of::<GeometryError>(py));
            assert!(core.value(py).to_string().contains("i32 offset"));
        });
    }
}
