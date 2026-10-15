use std::sync::Arc;

use super::finish::finish_producer_drawing;
use super::receipt::{
    PendingPublicationRow, ProducerDrawingReceipt, ProducerImageId, StorageTarget,
};
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geometry::embedding_completion::literals::{
    LiteralRowLayout, ShapeLiteralLayout, ShapeStructure,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::storage::ValidityMask;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, GeometryKind, Shape};
use crate::py::vectors::Groups;
use crate::work::Unmetered;

pub(crate) fn finish_geographic_line(shape: Shape, frame: crate::Frame) -> Result<Shape> {
    finish_producer_drawing(
        ProducerDrawingReceipt::line(shape)?,
        frame,
        StorageTarget::Scalar,
    )?
    .into_scalar()
    .map(|(shape, _)| shape)
}

pub(crate) fn geographic_line_pending_row(
    shape: Shape,
    logical_row: usize,
    operation: &'static str,
) -> Result<PendingPublicationRow> {
    if !matches!(
        &shape,
        Shape::LineString(_) | Shape::Empty(GeometryKind::LineString, _)
    ) {
        return Err(GeometryErrorKind::Invalid(
            "producer line receipt requires a LineString output".into(),
        )
        .into());
    }
    PendingPublicationRow::drawing(shape, logical_row, operation)
}

pub(crate) fn finish_geographic_point_selection(
    shape: Shape,
    frame: crate::Frame,
) -> Result<Shape> {
    finish_producer_drawing(
        ProducerDrawingReceipt::point_selection(shape)?,
        frame,
        StorageTarget::Scalar,
    )?
    .into_scalar()
    .map(|(shape, _)| shape)
}

pub(crate) fn geographic_point_selection_pending_row(
    shape: Shape,
    logical_row: usize,
    operation: &'static str,
) -> Result<PendingPublicationRow> {
    if !matches!(
        &shape,
        Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..)
    ) {
        return Err(GeometryErrorKind::Invalid(
            "point-selection receipt requires a Point, LineString, or Polygon output".into(),
        )
        .into());
    }
    PendingPublicationRow::drawing(shape, logical_row, operation)
}

pub(crate) fn finish_geographic_nearest_point(shape: Shape, frame: crate::Frame) -> Result<Shape> {
    finish_producer_drawing(
        ProducerDrawingReceipt::point(shape)?,
        frame,
        StorageTarget::Scalar,
    )?
    .into_scalar()
    .map(|(shape, _)| shape)
}

pub(crate) fn finish_geographic_nearest_point_columns(
    left: Vec<Shape>,
    right: Vec<Shape>,
    frame: &crate::Frame,
    missing: Option<&ValidityMask>,
) -> Result<(crate::PyGeometryArray, crate::PyGeometryArray)> {
    fn publish_column(
        shapes: Vec<Shape>,
        frame: &crate::Frame,
        missing: Option<&ValidityMask>,
    ) -> Result<crate::PyGeometryArray> {
        if missing.is_some_and(|mask| mask.len() != shapes.len()) {
            return Err(GeometryErrorKind::Invalid(
                "nearest-point validity does not match its output rows".into(),
            )
            .into());
        }
        let mut writer = StreamingShapes::with_capacity(0, shapes.len());
        let mut row_map = Vec::with_capacity(shapes.len());
        let mut literals = Vec::new();
        let mut present_row = 0_usize;
        for (logical_row, shape) in shapes.into_iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(logical_row)) {
                writer.try_push_missing()?;
                row_map.push(None);
                continue;
            }
            let (points, literal) = match &shape {
                Shape::Point(point) => {
                    let image = ProducerImageId::new(present_row);
                    let (point, literal) = ProducerDrawingReceipt::point_layout(
                        *point,
                        CoordinateAxes::from_point(*point),
                        logical_row,
                        None,
                        None,
                        0,
                        image,
                    );
                    (vec![point], Some(literal))
                },
                Shape::Empty(GeometryKind::Point, _) => (Vec::new(), None),
                Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(
                    GeometryKind::MultiPoint
                    | GeometryKind::LineString
                    | GeometryKind::MultiLineString
                    | GeometryKind::Polygon
                    | GeometryKind::MultiPolygon
                    | GeometryKind::GeometryCollection,
                    _,
                ) => {
                    return Err(GeometryErrorKind::Invalid(
                        "nearest-point column contains a non-Point output".into(),
                    )
                    .into());
                },
            };
            let structure = ShapeStructure::capture(&shape);
            writer.try_push(shape)?;
            row_map.push(Some(LiteralRowLayout {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                layout: ShapeLiteralLayout {
                    structure: Some(structure),
                    points,
                    lines: Vec::new(),
                    polygons: Vec::new(),
                },
            }));
            if let Some(literal) = literal {
                literals.push(literal);
            }
            present_row = present_row
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let (storage, missing, layout) = writer.finish_parts()?;
        let mut work = Unmetered;
        let buffers =
            FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
        let receipt = ProducerDrawingReceipt::new(buffers, literals);
        let finished = finish_producer_drawing(receipt, (*frame).clone(), StorageTarget::Array)?;
        crate::PyGeometryArray::from_finished(finished)
    }

    if left.len() != right.len() {
        return Err(GeometryErrorKind::Invalid(
            "nearest-point columns have different lengths".into(),
        )
        .into());
    }
    let left = publish_column(left, frame, missing)?;
    let right = publish_column(right, frame, missing)?;
    Ok((left, right))
}

pub(crate) fn publish_split_parts_array(
    shapes: Vec<Shape>,
    frame: crate::Frame,
) -> Result<crate::PyGeometryArray> {
    let mut writer = StreamingShapes::with_capacity(0, shapes.len());
    let mut row_map = Vec::with_capacity(shapes.len());
    let mut literals = Vec::new();
    let mut next_image = 0_usize;
    for (logical_row, shape) in shapes.into_iter().enumerate() {
        let (layout, mut row_literals) = ProducerDrawingReceipt::line_layout(
            &shape,
            logical_row,
            None,
            None,
            0,
            &mut next_image,
        )?;
        writer.try_push(shape)?;
        row_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            layout,
        }));
        literals.append(&mut row_literals);
    }
    let (storage, missing, layout) = writer.finish_parts()?;
    let mut work = Unmetered;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
    crate::PyGeometryArray::from_finished(finish_producer_drawing(
        receipt,
        frame,
        StorageTarget::Array,
    )?)
}

pub(crate) fn publish_split_groups(
    shapes: Vec<Shape>,
    offsets: Vec<i64>,
    frame: crate::Frame,
    missing: Option<ValidityMask>,
) -> Result<Groups> {
    let shape_count = shapes.len();
    let terminal = offsets
        .last()
        .copied()
        .and_then(|value| usize::try_from(value).ok());
    if offsets.first() != Some(&0) || terminal != Some(shape_count) {
        return Err(GeometryErrorKind::Invalid(
            "split groups do not cover their flattened output rows".into(),
        )
        .into());
    }
    let mut writer = StreamingShapes::with_capacity(0, shape_count);
    let mut value_map = Vec::with_capacity(shape_count);
    let mut literals = Vec::new();
    let mut next_image = 0_usize;
    let mut shapes = shapes.into_iter();
    for (group_row, range) in offsets.windows(2).enumerate() {
        let [start, end] = range else {
            return Err(
                GeometryErrorKind::Invalid("split group offsets are malformed".into()).into(),
            );
        };
        let start = usize::try_from(*start).map_err(|_| ResourceError::SizeOverflow)?;
        let end = usize::try_from(*end).map_err(|_| ResourceError::SizeOverflow)?;
        if start > end || end > shape_count {
            return Err(GeometryErrorKind::Invalid(
                "split group offsets are outside their flattened output rows".into(),
            )
            .into());
        }
        for value_row in start..end {
            let shape = shapes.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("split output row is out of range".into())
            })?;
            let (layout, mut row_literals) = ProducerDrawingReceipt::line_layout(
                &shape,
                group_row,
                Some(group_row),
                Some(value_row),
                0,
                &mut next_image,
            )?;
            writer.try_push(shape)?;
            value_map.push(Some(LiteralRowLayout {
                operation: 0,
                logical_row: group_row,
                group_row: Some(group_row),
                value_row: Some(value_row),
                layout,
            }));
            literals.append(&mut row_literals);
        }
    }
    if shapes.next().is_some() {
        return Err(GeometryErrorKind::Invalid(
            "split groups left trailing flattened output rows".into(),
        )
        .into());
    }
    let (storage, value_missing, layout) = writer.finish_parts()?;
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::groups(
        Arc::new(storage),
        value_missing,
        layout,
        value_map,
        offsets,
        missing,
        &mut work,
    )?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
    Groups::from_finished(finish_producer_drawing(
        receipt,
        frame,
        StorageTarget::Groups,
    )?)
}

pub(in crate::geometry) fn snap_drawing_receipt(shape: Shape) -> Result<ProducerDrawingReceipt> {
    ProducerDrawingReceipt::drawing(shape)
}

pub(crate) fn finish_geographic_snap(shape: Shape, frame: crate::Frame) -> Result<Shape> {
    finish_producer_drawing(snap_drawing_receipt(shape)?, frame, StorageTarget::Scalar)?
        .into_scalar()
        .map(|(shape, _)| shape)
}

pub(crate) fn geographic_drawing_pending_row(
    shape: Shape,
    logical_row: usize,
    operation: &'static str,
) -> Result<PendingPublicationRow> {
    PendingPublicationRow::drawing(shape, logical_row, operation)
}
