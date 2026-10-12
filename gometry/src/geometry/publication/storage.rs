use std::sync::Arc;

use super::binding::CheckedLiteralSnapshot;
use super::finish::FinishedGeometry;
use super::receipt::{
    ArrayPublicationBatch, GeometryOutput, PendingPublicationRow, ProducerReceipt, PublicationCall,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use crate::Frame;
use crate::array::{ComputedCaches, StreamingShapes};
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralCheck, LiteralRowBinding, bind_final_literals,
};
use crate::geometry::embedding_search::{Unmetered, Work};
use crate::geometry::storage::{ArrayRows, StorageLayout, ValidityMask};
use crate::geometry::{GeometryErrorKind, ShapeCountQuery};

pub(crate) struct PublicationArrayRows {
    writer: StreamingShapes,
    pending: Vec<PendingPublicationRow>,
}

impl PublicationArrayRows {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            pending: Vec::new(),
        }
    }

    pub(crate) fn try_push(&mut self, output: GeometryOutput) -> Result<()> {
        match output {
            GeometryOutput::Shape(shape) => {
                self.writer.try_push(shape)?;
            },
            GeometryOutput::Pending(mut pending) => {
                pending.precharge_append()?;
                self.writer.try_push(pending.take_shape())?;
                self.pending.push(pending);
            },
        }
        Ok(())
    }

    pub(crate) fn try_push_missing(&mut self) -> Result<()> {
        self.writer.try_push_missing()?;
        Ok(())
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<crate::PyGeometryArray> {
        if self.pending.is_empty() {
            let (storage, missing, layout) = self.writer.finish_parts()?;
            return crate::PyGeometryArray::from_result_storage(
                Arc::new(storage),
                frame,
                missing,
                layout,
                ComputedCaches::fresh(),
            );
        }
        let (storage, missing, layout) = self.writer.finish_parts()?;
        let logical_count = missing.as_ref().map_or(storage.len(), ValidityMask::len);
        let mut logical_rows = std::iter::repeat_with(|| None)
            .take(logical_count)
            .collect::<Vec<Option<PendingPublicationRow>>>();
        for pending in self.pending {
            let logical = pending.logical_row;
            let slot = logical_rows.get_mut(logical).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q output row is outside final array storage".into())
            })?;
            if slot.is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "multiple Q receipts claim one final array row".into(),
                )
                .into());
            }
            *slot = Some(pending);
        }
        let batch = ArrayPublicationBatch {
            storage: Arc::new(storage),
            missing,
            layout,
            logical_rows,
        };
        let mut unmetered = Unmetered;
        match super::finish::finish_geographic(
            PublicationCall::new(ProducerReceipt::ArrayBatch(batch), frame, &mut unmetered),
            StorageTarget::Array,
        )? {
            PublicationOutcome::Pass(finished) => {
                let (rows, frame) = finished.into_array()?;
                Ok(crate::PyGeometryArray::from_admitted_rows(
                    rows,
                    frame,
                    ComputedCaches::fresh(),
                ))
            },
            PublicationOutcome::Reject(PublicationRejection::Batch(images)) => {
                Err(GeometryErrorKind::Invalid(
                    format!(
                        "final array writer changed {} checked Q output records",
                        images.len()
                    )
                    .into(),
                )
                .into())
            },
            PublicationOutcome::Reject(PublicationRejection::Native) => {
                Err(GeometryErrorKind::Invalid(
                    "array publication finisher returned a Native candidate rejection".into(),
                )
                .into())
            },
            PublicationOutcome::Reject(PublicationRejection::SourceOnly(_)) => {
                Err(GeometryErrorKind::Invalid(
                    "array publication finisher returned a SourceOnly candidate rejection".into(),
                )
                .into())
            },
        }
    }
}

pub(super) fn finish_array_batch<W: Work>(
    batch: ArrayPublicationBatch,
    frame: Frame,
    work: &mut W,
) -> Result<PublicationOutcome> {
    if batch.logical_rows.is_empty() {
        return Err(GeometryErrorKind::Invalid(
            "array publication batch has no logical rows".into(),
        )
        .into());
    }
    let logical_count = batch
        .missing
        .as_ref()
        .map_or_else(|| batch.storage.len(), ValidityMask::len);
    if batch.logical_rows.len() != logical_count {
        return Err(GeometryErrorKind::Invalid(
            "array publication receipts do not match the logical output length".into(),
        )
        .into());
    }

    let mut row_map = Vec::with_capacity(logical_count);
    let mut pending_rows = Vec::new();
    let mut present_row = 0_usize;
    for (logical, pending) in batch.logical_rows.into_iter().enumerate() {
        work.charge_into_error(1)?;
        let missing = batch
            .missing
            .as_ref()
            .is_some_and(|mask| mask.is_missing(logical));
        if missing {
            if pending.is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "missing array row retained a checked publication receipt".into(),
                )
                .into());
            }
            row_map.push(None);
            continue;
        }

        let storage_row = if batch.layout == StorageLayout::PresentOnly {
            let row = present_row;
            present_row = present_row
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            row
        } else {
            logical
        };
        if let Some(pending) = pending {
            if pending.logical_row != logical
                || pending.shape.is_some()
                || pending.layout.logical_row != logical
            {
                return Err(GeometryErrorKind::Invalid(
                    "pending Q row no longer matches its admitted operation slot".into(),
                )
                .into());
            }

            if storage_row >= batch.storage.len() {
                return Err(GeometryErrorKind::Invalid(
                    "pending Q row is outside final writer storage".into(),
                )
                .into());
            }
            let coordinates = batch
                .storage
                .row(storage_row)
                .coordinates(&mut ShapeCountQuery::default())?;
            let admission_ticks = u64::try_from(coordinates)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?
                .checked_add(3)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let mut pending = pending;
            pending.work.charge_into_error(admission_ticks)?;
            row_map.push(Some(LiteralRowBinding::Checked(pending.layout)));
            pending_rows.push((logical, pending.checked, pending.work));
        } else {
            row_map.push(Some(LiteralRowBinding::OtherProducer));
        }
    }

    let writer_map = if batch.layout == StorageLayout::PresentOnly {
        match &batch.missing {
            Some(missing) => row_map
                .into_iter()
                .enumerate()
                .filter_map(|(logical, binding)| (!missing.is_missing(logical)).then_some(binding))
                .collect(),
            None => row_map,
        }
    } else {
        row_map
    };

    let mut unmetered = Unmetered;
    let (rows, row_map) = ArrayRows::admit_with_row_map(
        batch.storage,
        batch.missing,
        batch.layout,
        writer_map,
        &mut unmetered,
    )?;
    let row_map = Arc::from(row_map);
    for (logical_row, records, mut row_work) in pending_rows {
        let buffers = FrozenLiteralBuffers::admitted_row_view(
            rows.clone(),
            Arc::clone(&row_map),
            logical_row,
        )?;
        let mut receiver = CheckedLiteralSnapshot::new(records)?;
        match bind_final_literals(buffers, &mut receiver, &mut row_work)? {
            LiteralCheck::Pass(_) => {},
            LiteralCheck::Reject(images) => {
                return Ok(PublicationOutcome::Reject(PublicationRejection::Batch(
                    images,
                )));
            },
        }
    }

    Ok(PublicationOutcome::Pass(
        FinishedGeometry::from_admitted_rows(rows, frame),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::receipt::{CheckedPublicationLiteral, PublicationImage};
    use super::*;
    use crate::geometry::embedding_completion::{
        LiteralAddress, LiteralPoint, LiteralRowLayout, ShapeLiteralLayout,
    };
    use crate::geometry::embedding_search::EvaluationBudget;
    use crate::geometry::tessellation::exact::ExactRational;
    use crate::geometry::topology::{ExactChartPoint, QSourceImage};
    use crate::geometry::{CoordinateAxes, MOrdinate, Point, Shape, ZOrdinate};

    fn xyzm(x: f64, y: f64, z: f64, m: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
    }

    fn array_batch(stored: Point, expected: Point) -> (ArrayPublicationBatch, Frame) {
        let frame = Frame::default();
        let image = PublicationImage::SourceOnly(QSourceImage::PublicationSample(0));
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer.try_push(Shape::Point(stored)).unwrap();
        let (storage, missing, layout) = writer.finish_parts().unwrap();
        let pending = PendingPublicationRow {
            shape: None,
            logical_row: 0,
            layout: LiteralRowLayout {
                operation: 0,
                logical_row: 0,
                group_row: None,
                value_row: None,
                layout: ShapeLiteralLayout {
                    points: vec![LiteralPoint {
                        output: 0,
                        axes: CoordinateAxes::XYZM,
                        image,
                    }],
                    lines: Vec::new(),
                    polygons: Vec::new(),
                },
            },
            checked: vec![CheckedPublicationLiteral {
                address: LiteralAddress {
                    operation: 0,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                image,
                record: expected,
                chart: ExactChartPoint::from_rationals(
                    ExactRational::from_f64(expected.x),
                    ExactRational::from_f64(expected.y),
                ),
            }],
            work: EvaluationBudget::new("array publication test", 0).unwrap(),
        };
        (
            ArrayPublicationBatch {
                storage: Arc::new(storage),
                missing,
                layout,
                logical_rows: vec![Some(pending)],
            },
            frame,
        )
    }

    #[test]
    fn final_array_storage_m_corruption_is_rejected() {
        let expected = xyzm(1.0, 2.0, 3.0, 4.0);
        let (batch, frame) = array_batch(expected, expected);
        let mut work = Unmetered;
        assert!(matches!(
            finish_array_batch(batch, frame, &mut work).unwrap(),
            PublicationOutcome::Pass(_)
        ));

        let corrupted = xyzm(1.0, 2.0, 3.0, 40.0);
        let (batch, frame) = array_batch(corrupted, expected);
        let mut work = Unmetered;
        match finish_array_batch(batch, frame, &mut work).unwrap() {
            PublicationOutcome::Reject(PublicationRejection::Batch(images)) => {
                assert_eq!(images, [PublicationImage::SourceOnly(
                    QSourceImage::PublicationSample(0)
                )]);
            },
            PublicationOutcome::Pass(finished) => {
                let (rows, _) = finished.into_array().expect("array output is admitted");
                let actual = match rows.logical_row(0) {
                    Some(crate::geometry::storage::LogicalRow::Present { row, .. }) => {
                        row.point().expect("one point literal is present")
                    },
                    _ => panic!("the final writer retains its present point row"),
                };
                assert_eq!(
                    actual, expected,
                    "an omitted final-storage binder must not publish corrupted M"
                );
            },
            PublicationOutcome::Reject(
                PublicationRejection::Native | PublicationRejection::SourceOnly(_),
            ) => {
                panic!("array storage cannot return a Q candidate rejection")
            },
        }
    }
}
