use super::finish::finish_producer_drawing;
use super::receipt::{
    GeometryOutput, PendingPublicationRow, ProducerDrawingReceipt, StorageTarget,
};
use super::storage::PublicationRows;
use crate::error::{ResourceError, Result};
use crate::geometry::storage::ValidityMask;
use crate::geometry::{GeometryErrorKind, GeometryKind, Shape};
use crate::py::vectors::Groups;

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

pub(crate) fn publish_split_parts_array(
    shapes: Vec<Shape>,
    frame: crate::Frame,
) -> Result<crate::PyGeometryArray> {
    let mut rows = PublicationRows::with_producer_drawing_capacity(shapes.len());
    for (logical_row, shape) in shapes.into_iter().enumerate() {
        let pending = geographic_line_pending_row(shape, logical_row, "split")?;
        rows.try_push(GeometryOutput::Pending(pending))?;
    }
    rows.finish(frame)
}

pub(crate) fn publish_split_groups(
    shapes: Vec<Shape>,
    offsets: &[i64],
    frame: crate::Frame,
    missing: Option<ValidityMask>,
) -> Result<Groups> {
    let shape_count = shapes.len();
    if missing
        .as_ref()
        .is_some_and(|mask| mask.len() != offsets.len().saturating_sub(1))
    {
        return Err(GeometryErrorKind::Invalid(
            "split group validity does not match its logical rows".into(),
        )
        .into());
    }
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
    let mut rows = PublicationRows::with_groups_capacity(offsets.len().saturating_sub(1));
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
        for _ in start..end {
            let shape = shapes.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("split output row is out of range".into())
            })?;
            let pending = geographic_line_pending_row(shape, group_row, "split")?;
            rows.try_push(GeometryOutput::Pending(pending))?;
        }
        rows.end_group(
            missing
                .as_ref()
                .is_some_and(|mask| mask.is_missing(group_row)),
        )?;
    }
    if shapes.next().is_some() {
        return Err(GeometryErrorKind::Invalid(
            "split groups left trailing flattened output rows".into(),
        )
        .into());
    }
    rows.finish_groups(frame, missing)
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
