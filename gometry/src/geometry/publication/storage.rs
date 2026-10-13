use std::sync::Arc;

use super::DirectedChartRestrictionUse;
use super::binding::CheckedLiteralSnapshot;
use super::finish::FinishedGeometry;
use super::receipt::{
    ArrayPublicationBatch, GeometryOutput, PendingPublicationRow, ProducerDrawingReceipt,
    ProducerLiteral, ProducerReceipt, ProducerSourceRootUse, PublicationCall, PublicationImage,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use crate::Frame;
use crate::array::{ComputedCaches, StreamingShapes};
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralCheck, LiteralRowBinding, LiteralRowLayout, bind_final_literals,
};
use crate::geometry::embedding_search::{Unmetered, Work};
use crate::geometry::publication::{ProducerImageId, finish_geographic};
use crate::geometry::storage::{ArrayRows, GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::{GeometryErrorKind, ShapeCountQuery};
use crate::py::vectors::Groups;

pub(crate) struct PublicationRows {
    writer: StreamingShapes,
    pending: Vec<PendingPublicationRow>,
    group_offsets: Option<Vec<i64>>,
    value_count: usize,
    producer_drawing_required: bool,
    has_unreceipted_output: bool,
}

impl PublicationRows {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            pending: Vec::new(),
            group_offsets: None,
            value_count: 0,
            producer_drawing_required: false,
            has_unreceipted_output: false,
        }
    }

    pub(crate) fn with_producer_drawing_capacity(rows: usize) -> Self {
        let mut output = Self::with_capacity(rows);
        output.producer_drawing_required = true;
        output
    }

    pub(crate) fn with_groups_capacity(groups: usize) -> Self {
        let mut offsets = Vec::with_capacity(groups.saturating_add(1));
        offsets.push(0);
        Self {
            writer: StreamingShapes::with_capacity(0, 0),
            pending: Vec::new(),
            group_offsets: Some(offsets),
            value_count: 0,
            producer_drawing_required: true,
            has_unreceipted_output: false,
        }
    }

    pub(crate) fn try_push(&mut self, output: GeometryOutput) -> Result<()> {
        match output {
            GeometryOutput::Shape(shape) => {
                self.writer.try_push(shape)?;
                self.has_unreceipted_output = true;
            },
            GeometryOutput::Pending(mut pending) => {
                pending.precharge_append()?;
                self.writer.try_push(pending.take_shape())?;
                self.has_unreceipted_output |= !pending.producer_drawing;
                self.pending.push(pending);
            },
        }
        self.value_count = self
            .value_count
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }

    pub(crate) fn try_push_missing(&mut self) -> Result<()> {
        if self.group_offsets.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "a missing group cannot be stored as a missing geometry value".into(),
            )
            .into());
        }
        self.writer.try_push_missing()?;
        Ok(())
    }

    pub(crate) fn end_group(&mut self) -> Result<()> {
        let offsets = self.group_offsets.as_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("only a Groups publication has group boundaries".into())
        })?;
        offsets.push(
            i64::try_from(self.value_count)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
        Ok(())
    }

    pub(crate) const fn next_value_row(&self) -> usize {
        self.value_count
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<crate::PyGeometryArray> {
        if self.group_offsets.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "a Groups publication must finish through finish_groups".into(),
            )
            .into());
        }
        let producer_drawing = self.producer_drawing_required
            || (!self.pending.is_empty()
                && !self.has_unreceipted_output
                && self.pending.iter().all(|pending| pending.producer_drawing));
        if self.pending.is_empty() && !producer_drawing {
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
        if producer_drawing {
            if self.has_unreceipted_output {
                return Err(GeometryErrorKind::Invalid(
                    "producer-drawn array publication contains an output without a receipt".into(),
                )
                .into());
            }
            return finish_producer_array(storage, missing, layout, logical_rows, frame);
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
            PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(_)) => {
                Err(GeometryErrorKind::Invalid(
                    "array publication finisher returned a producer-drawn rejection".into(),
                )
                .into())
            },
        }
    }

    pub(crate) fn finish_groups(
        self,
        frame: Frame,
        missing: Option<ValidityMask>,
    ) -> Result<Groups> {
        let Some(group_offsets) = self.group_offsets else {
            return Err(GeometryErrorKind::Invalid(
                "an ArrayRows publication has no group offsets".into(),
            )
            .into());
        };
        if self.has_unreceipted_output {
            return Err(GeometryErrorKind::Invalid(
                "producer-drawn Groups publication contains an output without a receipt".into(),
            )
            .into());
        }
        if !self.producer_drawing_required {
            return Err(GeometryErrorKind::Invalid(
                "Groups publication requires producer-drawn receipts".into(),
            )
            .into());
        }
        if self.pending.len() != self.value_count {
            return Err(GeometryErrorKind::Invalid(
                "producer-drawn Groups storage is missing a value receipt".into(),
            )
            .into());
        }
        let group_count = group_offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Groups offsets have no origin".into()))?;
        if missing
            .as_ref()
            .is_some_and(|mask| mask.len() != group_count)
        {
            return Err(GeometryErrorKind::Invalid(
                "Groups offsets do not match their logical rows".into(),
            )
            .into());
        }
        let (storage, value_missing, layout) = self.writer.finish_parts()?;
        let mut row_map = std::iter::repeat_with(|| None)
            .take(storage.len())
            .collect::<Vec<Option<LiteralRowLayout<ProducerImageId>>>>();
        let (literals, source_root_uses, chart_restrictions) =
            collect_producer_rows(self.pending, &mut row_map, |pending| {
                pending.layout.value_row.ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer-drawn group value has no value row".into())
                        .into()
                })
            })?;
        let mut work = Unmetered;
        let buffers = FrozenLiteralBuffers::groups(
            Arc::new(storage),
            value_missing,
            layout,
            row_map,
            group_offsets,
            missing,
            &mut work,
        )?;
        let receipt =
            ProducerDrawingReceipt::new(buffers, literals, source_root_uses, chart_restrictions);
        match finish_geographic(
            PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
            StorageTarget::Groups,
        )? {
            PublicationOutcome::Pass(finished) => Groups::from_finished(finished),
            PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
                Err(GeometryErrorKind::Invalid(
                    format!(
                        "final Groups writer changed {} producer literal images",
                        images.len()
                    )
                    .into(),
                )
                .into())
            },
            PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
                "Groups publication finisher returned a different rejection family".into(),
            )
            .into()),
        }
    }
}

fn finish_producer_array(
    storage: GeometryArrayStorage,
    missing: Option<ValidityMask>,
    layout: StorageLayout,
    logical_rows: Vec<Option<PendingPublicationRow>>,
    frame: Frame,
) -> Result<crate::PyGeometryArray> {
    let mut row_map = std::iter::repeat_with(|| None)
        .take(logical_rows.len())
        .collect::<Vec<Option<LiteralRowLayout<ProducerImageId>>>>();
    let (literals, source_root_uses, chart_restrictions) = collect_producer_rows(
        logical_rows.into_iter().flatten(),
        &mut row_map,
        |pending| Ok(pending.logical_row),
    )?;
    let row_map = if layout == StorageLayout::PresentOnly {
        if let Some(missing) = &missing {
            row_map
                .into_iter()
                .enumerate()
                .filter_map(|(logical, row)| (!missing.is_missing(logical)).then_some(row))
                .collect()
        } else {
            row_map
        }
    } else {
        row_map
    };
    let mut work = Unmetered;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
    let receipt =
        ProducerDrawingReceipt::new(buffers, literals, source_root_uses, chart_restrictions);
    match finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Array,
    )? {
        PublicationOutcome::Pass(finished) => crate::PyGeometryArray::from_finished(finished),
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            Err(GeometryErrorKind::Invalid(
                format!(
                    "final array writer changed {} producer literal images",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "array publication finisher returned a different rejection family".into(),
        )
        .into()),
    }
}

fn collect_producer_rows(
    pending_rows: impl IntoIterator<Item = PendingPublicationRow>,
    row_map: &mut [Option<LiteralRowLayout<ProducerImageId>>],
    row_index: impl Fn(&PendingPublicationRow) -> Result<usize>,
) -> Result<(
    Vec<ProducerLiteral>,
    Vec<ProducerSourceRootUse>,
    Vec<DirectedChartRestrictionUse>,
)> {
    let mut literals = Vec::new();
    let mut source_root_uses = Vec::new();
    let mut chart_restrictions = Vec::new();
    for pending in pending_rows {
        if !pending.producer_drawing {
            return Err(GeometryErrorKind::Invalid(
                "publication row lost its producer-drawn receipt".into(),
            )
            .into());
        }
        let row = row_index(&pending)?;
        let slot = row_map.get_mut(row).ok_or_else(|| {
            GeometryErrorKind::Invalid("producer-drawn row is outside final storage".into())
        })?;
        if slot.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "multiple producer receipts claim one final storage row".into(),
            )
            .into());
        }
        *slot = Some(super::finish::producer_row_layout(pending.layout)?);
        for literal in pending.checked {
            let PublicationImage::ProducerDrawn(image) = literal.image else {
                return Err(GeometryErrorKind::Invalid(
                    "producer-drawn storage contains a different image family".into(),
                )
                .into());
            };
            literals.push(ProducerLiteral {
                address: literal.address,
                image,
                record: literal.record,
                chart: literal.chart,
            });
        }
        source_root_uses.extend(pending.source_root_uses);
        chart_restrictions.extend(pending.chart_restrictions);
    }
    Ok((literals, source_root_uses, chart_restrictions))
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
            pending_rows.push((
                logical,
                pending.checked,
                pending.source_root_uses,
                pending.chart_restrictions,
                pending.work,
            ));
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
    let mut source_root_uses = Vec::new();
    let mut chart_restrictions = Vec::new();
    for (logical_row, records, row_source_uses, row_chart_restrictions, mut row_work) in
        pending_rows
    {
        let buffers = FrozenLiteralBuffers::admitted_row_view(
            rows.clone(),
            Arc::clone(&row_map),
            logical_row,
        )?;
        let mut receiver = CheckedLiteralSnapshot::new(records)?;
        for source_use in &row_source_uses {
            row_work.charge_into_error(1)?;
            if !receiver.contains(
                source_use.address,
                PublicationImage::ProducerDrawn(source_use.image),
            ) {
                return Err(GeometryErrorKind::Invalid(
                    "producer source-root use has no checked final array literal".into(),
                )
                .into());
            }
        }
        row_work.charge_into_error(
            u64::try_from(row_chart_restrictions.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        match bind_final_literals(buffers, &mut receiver, &mut row_work)? {
            LiteralCheck::Pass(_) => {},
            LiteralCheck::Reject(images) => {
                return Ok(PublicationOutcome::Reject(PublicationRejection::Batch(
                    images,
                )));
            },
        }
        source_root_uses.extend(row_source_uses);
        chart_restrictions.extend(row_chart_restrictions);
    }

    Ok(PublicationOutcome::Pass(
        FinishedGeometry::from_admitted_rows(rows, frame, source_root_uses, chart_restrictions),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::receipt::CheckedPublicationLiteral;
    use super::*;
    use crate::geometry::embedding_completion::{LiteralAddress, LiteralPoint, ShapeLiteralLayout};
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
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
            producer_drawing: false,
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
                PublicationRejection::Native
                | PublicationRejection::SourceOnly(_)
                | PublicationRejection::ProducerDrawing(_),
            ) => {
                panic!("array storage cannot return a Q candidate rejection")
            },
        }
    }
}
