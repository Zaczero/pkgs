use std::sync::Arc;

use super::binding::CheckedLiteralSnapshot;
use super::finish::FinishedGeometry;
use super::receipt::{
    ArrayPublicationBatch, CheckedPublicationLiteral, GeometryOutput, ProducerDrawingReceipt,
    ProducerLiteral, ProducerReceipt, PublicationCall, PublicationImage, PublicationOutcome,
    PublicationRejection, StorageTarget, StoredPublicationRow,
};
use crate::Frame;
use crate::array::{ComputedCaches, StreamingShapes};
use crate::error::Result;
use crate::geometry::embedding_completion::binding::bind_final_literals;
use crate::geometry::embedding_completion::literals::LiteralRowLayout;
use crate::geometry::embedding_completion::storage::{
    FrozenLiteralBuffers, LiteralCheck, LiteralRowBinding,
};
use crate::geometry::publication::{ProducerImageId, finish_geographic};
use crate::geometry::storage::{ArrayRows, GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::{GeometryErrorKind, ShapeCountQuery};
use crate::py::vectors::Groups;
use crate::work::{Unmetered, Work};

pub(crate) struct PublicationRows {
    writer: StreamingShapes,
    pending: Vec<StoredPublicationRow>,
    expected_present: Vec<bool>,
    group_offsets: Option<Vec<i64>>,
    expected_group_present: Vec<bool>,
    value_count: usize,
    producer_drawing_required: bool,
    has_unreceipted_output: bool,
}

impl PublicationRows {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            pending: Vec::new(),
            expected_present: Vec::with_capacity(rows),
            group_offsets: None,
            expected_group_present: Vec::new(),
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
            expected_present: Vec::new(),
            group_offsets: Some(offsets),
            expected_group_present: Vec::with_capacity(groups),
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
                let (shape, stored) = pending.into_stored();
                self.writer.try_push(shape)?;
                self.has_unreceipted_output |= !stored.producer_drawing;
                self.pending.push(stored);
            },
        }
        self.expected_present.push(true);
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
        self.expected_present.push(false);
        Ok(())
    }

    pub(crate) fn end_group(&mut self, missing: bool) -> Result<()> {
        let offsets = self.group_offsets.as_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("only a Groups publication has group boundaries".into())
        })?;
        offsets.push(
            i64::try_from(self.value_count)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
        self.expected_group_present.push(!missing);
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
            ArrayRows::validate_publication_domain(
                missing.as_ref().map_or(storage.len(), ValidityMask::len),
                missing.as_ref(),
                self.expected_present.into_iter(),
            )?;
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
        ArrayRows::validate_publication_domain(
            logical_count,
            missing.as_ref(),
            self.expected_present.iter().copied(),
        )?;
        let mut logical_rows = std::iter::repeat_with(|| None)
            .take(logical_count)
            .collect::<Vec<Option<StoredPublicationRow>>>();
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
            expected_present: self.expected_present,
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
        ArrayRows::validate_publication_domain(
            group_count,
            missing.as_ref(),
            self.expected_group_present.into_iter(),
        )?;
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
        ArrayRows::validate_publication_domain(
            value_missing
                .as_ref()
                .map_or(storage.len(), ValidityMask::len),
            value_missing.as_ref(),
            self.expected_present.into_iter(),
        )?;
        let mut row_map = std::iter::repeat_with(|| None)
            .take(storage.len())
            .collect::<Vec<Option<LiteralRowLayout<ProducerImageId>>>>();
        let literals = collect_producer_rows(self.pending, &mut row_map, |pending| {
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
        let receipt = ProducerDrawingReceipt::new(buffers, literals);
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
    logical_rows: Vec<Option<StoredPublicationRow>>,
    frame: Frame,
) -> Result<crate::PyGeometryArray> {
    let mut row_map = std::iter::repeat_with(|| None)
        .take(logical_rows.len())
        .collect::<Vec<Option<LiteralRowLayout<ProducerImageId>>>>();
    let literals = collect_producer_rows(
        logical_rows.into_iter().flatten(),
        &mut row_map,
        |pending| Ok(pending.logical_row),
    )?;
    let mut work = Unmetered;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
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
    pending_rows: impl IntoIterator<Item = StoredPublicationRow>,
    row_map: &mut [Option<LiteralRowLayout<ProducerImageId>>],
    row_index: impl Fn(&StoredPublicationRow) -> Result<usize>,
) -> Result<Vec<ProducerLiteral>> {
    let mut literals = Vec::new();
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
        literals.extend(pending.producer_literals);
    }
    Ok(literals)
}

pub(super) fn finish_array_batch<W: Work>(
    batch: ArrayPublicationBatch,
    frame: Frame,
    work: &mut W,
) -> Result<PublicationOutcome> {
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
    ArrayRows::validate_publication_domain(
        logical_count,
        batch.missing.as_ref(),
        batch.expected_present.into_iter(),
    )?;

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
            if pending.logical_row != logical || pending.layout.logical_row != logical {
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
            let producer_count = pending.producer_literals.len();
            pending.work.charge_into_error(
                u64::try_from(producer_count)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            )?;
            pending.checked.try_reserve(producer_count).map_err(|_| {
                crate::error::ResourceError::SizeOverflowContext(
                    "publication literal reservation failed",
                )
            })?;
            pending
                .checked
                .extend(pending.producer_literals.into_iter().map(|literal| {
                    CheckedPublicationLiteral {
                        address: literal.address,
                        image: PublicationImage::ProducerDrawn(literal.image),
                        record: literal.record,
                    }
                }));
            row_map.push(Some(LiteralRowBinding::Checked(pending.layout)));
            pending_rows.push((logical, pending.checked, pending.work));
        } else {
            row_map.push(Some(LiteralRowBinding::OtherProducer));
        }
    }

    let mut unmetered = Unmetered;
    let (rows, row_map) = ArrayRows::admit_with_row_map(
        batch.storage,
        batch.missing,
        batch.layout,
        row_map,
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
#[path = "storage/boundary_tests.rs"]
mod boundary_tests;

#[cfg(test)]
mod tests {
    use super::super::receipt::PendingPublicationRow;
    use super::*;
    use crate::crs::frame::crs_arc_static;
    use crate::dispatch::kernels::unary_convex_hull_array;
    use crate::geometry::embedding_completion::literals::{
        LiteralAddress, LiteralPoint, ShapeLiteralLayout, ShapeStructure,
    };
    use crate::geometry::embedding_search::account::EvaluationBudget;
    use crate::geometry::topology::QSourceImage;
    use crate::geometry::{CoordinateAxes, MOrdinate, Point, Shape, ZOrdinate};

    fn xyzm(x: f64, y: f64, z: f64, m: f64) -> Point {
        Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
    }

    fn drawing_pending(shape: Shape, row: usize) -> PendingPublicationRow {
        let mut image = 0;
        let mut outputs = (0, 0, 0);
        let (layout, literals) = ProducerDrawingReceipt::drawing_layout(
            &shape,
            row,
            None,
            None,
            &mut image,
            &mut outputs,
        )
        .unwrap();
        PendingPublicationRow::producer_drawing(shape, layout, literals, row, "domain test")
            .unwrap()
    }

    #[test]
    fn final_writer_rejects_present_to_missing() {
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(drawing_pending(
            Shape::Point(xyzm(2., 3., 4., 5.)),
            0,
        )))
        .unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer.try_push_missing().unwrap();
        rows.writer = writer;
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("the final writer dropped an accepted present row");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn array_batch_preserves_missing_and_other_producer_row_domains() {
        for (expected, actual) in [
            (vec![], vec![]),
            (vec![false], vec![false]),
            (vec![true], vec![true]),
            (vec![false], vec![true]),
            (vec![true], vec![false]),
            (vec![true, false], vec![false, true]),
            (vec![true, false], vec![true]),
        ] {
            let correct = expected == actual;
            let logical_rows = std::iter::repeat_with(|| None)
                .take(expected.len())
                .collect();
            let mut writer = StreamingShapes::with_capacity(0, actual.len());
            for present in actual {
                if present {
                    writer.try_push(Shape::Point(xyzm(2., 3., 4., 5.))).unwrap();
                } else {
                    writer.try_push_missing().unwrap();
                }
            }
            let (storage, missing, layout) = writer.finish_parts().unwrap();
            let batch = ArrayPublicationBatch {
                storage: Arc::new(storage),
                missing,
                layout,
                logical_rows,
                expected_present: expected,
            };
            let result = finish_array_batch(batch, Frame::default(), &mut Unmetered);
            if correct {
                assert!(matches!(result.unwrap(), PublicationOutcome::Pass(_)));
            } else {
                let Err(error) = result else {
                    panic!("a writer changed a missing or other-producer row domain");
                };
                assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                ));
            }
        }
    }

    #[test]
    fn final_writer_rejects_typed_empty_substitution() {
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(drawing_pending(
            Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ),
            0,
        )))
        .unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer
            .try_push(Shape::Empty(
                crate::geometry::GeometryKind::Point,
                CoordinateAxes::XY,
            ))
            .unwrap();
        rows.writer = writer;
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("the final writer substituted typed-empty metadata");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn final_writer_rejects_changed_logical_domains() {
        let point = Shape::Point(xyzm(2., 3., 4., 5.));
        let empty = Shape::Empty(
            crate::geometry::GeometryKind::LineString,
            CoordinateAxes::XYZM,
        );
        for (name, expected, actual) in [
            ("missing becomes present", vec![None], vec![Some(
                point.clone(),
            )]),
            (
                "same-count validity swap",
                vec![Some(point.clone()), None],
                vec![None, Some(point.clone())],
            ),
            (
                "trailing missing row deletion",
                vec![Some(point.clone()), None],
                vec![Some(point)],
            ),
            ("all-missing truncation", vec![None, None], vec![None]),
            ("zero-row insertion", vec![], vec![None]),
            ("present empty becomes missing", vec![Some(empty)], vec![
                None,
            ]),
        ] {
            let mut rows = PublicationRows::with_producer_drawing_capacity(expected.len());
            for (logical, shape) in expected.into_iter().enumerate() {
                if let Some(shape) = shape {
                    rows.try_push(GeometryOutput::Pending(drawing_pending(shape, logical)))
                        .unwrap();
                } else {
                    rows.try_push_missing().unwrap();
                }
            }
            let mut writer = StreamingShapes::with_capacity(0, actual.len());
            for shape in actual {
                if let Some(shape) = shape {
                    writer.try_push(shape).unwrap();
                } else {
                    writer.try_push_missing().unwrap();
                }
            }
            rows.writer = writer;
            let Err(error) = rows.finish(Frame::default()) else {
                panic!("{name}: changed final domain was published");
            };
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                ),
                "{name}: {error:?}"
            );
        }
    }

    #[test]
    fn final_writer_preserves_empty_and_missing_domains() {
        for expected in [
            vec![],
            vec![None, None],
            vec![Some(Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ))],
            vec![None, Some(Shape::Point(xyzm(2., 3., 4., 5.))), None],
        ] {
            let mut rows = PublicationRows::with_producer_drawing_capacity(expected.len());
            for (logical, shape) in expected.iter().cloned().enumerate() {
                if let Some(shape) = shape {
                    rows.try_push(GeometryOutput::Pending(drawing_pending(shape, logical)))
                        .unwrap();
                } else {
                    rows.try_push_missing().unwrap();
                }
            }
            let result = rows.finish(Frame::default()).unwrap();
            assert_eq!(result.len(), expected.len());
        }
    }

    #[test]
    fn final_writer_rejects_changed_container_structure() {
        for (expected, actual) in [
            ("POINT (2 3)", "MULTIPOINT ((2 3))"),
            (
                "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 3)))",
                "GEOMETRYCOLLECTION (POINT (2 3))",
            ),
            ("MULTIPOINT (EMPTY, (2 3))", "MULTIPOINT ((2 3), EMPTY)"),
            ("LINESTRING ZM EMPTY", "LINESTRING EMPTY"),
            ("LINESTRING ZM EMPTY", "POINT ZM EMPTY"),
        ] {
            let mut rows = PublicationRows::with_producer_drawing_capacity(1);
            rows.try_push(GeometryOutput::Pending(drawing_pending(
                crate::io::parse_wkt(expected).unwrap(),
                0,
            )))
            .unwrap();
            let mut writer = StreamingShapes::with_capacity(0, 1);
            writer
                .try_push(crate::io::parse_wkt(actual).unwrap())
                .unwrap();
            rows.writer = writer;
            let Err(error) = rows.finish(Frame::default()) else {
                panic!("{expected} was replaced with {actual}");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ));
        }
    }

    #[test]
    fn final_writer_accepts_authorized_typed_empty_normalization() {
        let mut pending = drawing_pending(
            Shape::Empty(crate::geometry::GeometryKind::Point, CoordinateAxes::XY),
            0,
        );
        pending
            .replace_empty_shape(Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ))
            .unwrap();
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        assert_eq!(rows.finish(Frame::default()).unwrap().len(), 1);
    }

    #[test]
    fn pending_append_preserves_captured_empty_structure() {
        let mut pending = drawing_pending(
            Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ),
            0,
        );
        pending.shape = Shape::Empty(crate::geometry::GeometryKind::Point, CoordinateAxes::XY);
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        let Err(error) = rows.finish(Frame::default()) else {
            panic!("append recovered its expectation from substituted output geometry");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn final_writer_rejects_xym_record_corruption_without_domain_changes() {
        let expected = Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(-0.0)));
        for actual in [
            Point::new_raw_axes(0.0, 3.0, ZOrdinate(None), MOrdinate(Some(-0.0))),
            Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(0.0))),
            Point::new_raw_axes(-0.0, 3.0, ZOrdinate(None), MOrdinate(Some(7.0))),
            Point::new_raw_xy(-0.0, 3.0),
        ] {
            let mut rows = PublicationRows::with_producer_drawing_capacity(1);
            rows.try_push(GeometryOutput::Pending(drawing_pending(
                Shape::Point(expected),
                0,
            )))
            .unwrap();
            let mut writer = StreamingShapes::with_capacity(0, 1);
            writer.try_push(Shape::Point(actual)).unwrap();
            rows.writer = writer;
            let Err(error) = rows.finish(Frame::default()) else {
                panic!("the final writer changed active XYM bits or axes");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ));
        }
    }

    #[test]
    fn final_groups_reject_changed_empty_group_domains() {
        for (expected, actual) in [(false, true), (true, false)] {
            let mut rows = PublicationRows::with_groups_capacity(1);
            rows.end_group(expected).unwrap();
            let missing = ValidityMask::from_vec(1, vec![actual]);
            let Err(error) = rows.finish_groups(Frame::default(), missing) else {
                panic!("an empty group's presence changed");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ));
        }
        let mut rows = PublicationRows::with_groups_capacity(2);
        rows.end_group(false).unwrap();
        rows.end_group(false).unwrap();
        rows.group_offsets.as_mut().unwrap().pop();
        let Err(error) = rows.finish_groups(Frame::default(), None) else {
            panic!("a trailing empty group was deleted");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn final_groups_preserve_empty_missing_and_typed_empty_values() {
        let mut rows = PublicationRows::with_groups_capacity(5);
        rows.end_group(false).unwrap();
        rows.end_group(true).unwrap();
        let mut pending = drawing_pending(
            Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ),
            2,
        );
        pending.layout.group_row = Some(2);
        pending.layout.value_row = Some(0);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        rows.end_group(false).unwrap();
        rows.end_group(false).unwrap();
        rows.end_group(false).unwrap();
        let groups = rows
            .finish_groups(
                Frame::default(),
                ValidityMask::from_vec(5, vec![false, true, false, false, false]),
            )
            .unwrap();
        assert_eq!(groups.geometry_values_view().unwrap().unwrap().len(), 1);
    }

    #[test]
    fn final_groups_reject_a_typed_empty_value_substitution() {
        let mut rows = PublicationRows::with_groups_capacity(1);
        let mut pending = drawing_pending(
            Shape::Empty(
                crate::geometry::GeometryKind::LineString,
                CoordinateAxes::XYZM,
            ),
            0,
        );
        pending.layout.group_row = Some(0);
        pending.layout.value_row = Some(0);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        rows.end_group(false).unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer
            .try_push(Shape::Empty(
                crate::geometry::GeometryKind::Point,
                CoordinateAxes::XY,
            ))
            .unwrap();
        rows.writer = writer;
        let Err(error) = rows.finish_groups(Frame::default(), None) else {
            panic!("the final Groups writer replaced an accepted typed-empty value");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn final_groups_reject_shifted_value_boundaries() {
        let mut rows = PublicationRows::with_groups_capacity(2);
        rows.end_group(false).unwrap();
        let mut pending = drawing_pending(Shape::Point(xyzm(2., 3., 4., 5.)), 1);
        pending.layout.group_row = Some(1);
        pending.layout.value_row = Some(0);
        for literal in &mut pending.producer_literals {
            literal.address.logical_row = 1;
            literal.address.group_row = Some(1);
            literal.address.value_row = Some(0);
        }
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        rows.end_group(false).unwrap();
        *rows.group_offsets.as_mut().unwrap().get_mut(1).unwrap() = 1;
        let Err(error) = rows.finish_groups(Frame::default(), None) else {
            panic!("a value moved into another group");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    fn array_batch(stored: Point, expected: Point) -> (ArrayPublicationBatch, Frame) {
        let frame = Frame::default();
        let image = PublicationImage::SourceOnly(QSourceImage::PublicationSample(0));
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer.try_push(Shape::Point(stored)).unwrap();
        let (storage, missing, layout) = writer.finish_parts().unwrap();
        let pending = StoredPublicationRow {
            logical_row: 0,
            layout: LiteralRowLayout {
                operation: 0,
                logical_row: 0,
                group_row: None,
                value_row: None,
                layout: ShapeLiteralLayout {
                    structure: Some(ShapeStructure::capture(&Shape::Point(expected))),
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
            }],
            producer_literals: Vec::new(),
            producer_drawing: false,
            work: EvaluationBudget::new("array publication test", 0).unwrap(),
        };
        (
            ArrayPublicationBatch {
                storage: Arc::new(storage),
                missing,
                layout,
                logical_rows: vec![Some(pending)],
                expected_present: vec![true],
            },
            frame,
        )
    }

    fn geographic_hull_pending() -> (PendingPublicationRow, Frame) {
        let frame = Frame::new(Some(crs_arc_static("OGC:CRS84")), None).unwrap();
        let shape = crate::io::parse_wkt(
            "MULTIPOINT ZM ((0 0 0 0), (3 0 10 100), (2 2 20 200), (0 2 30 300), (1 1 40 400))",
        )
        .unwrap();
        let mut scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&frame, &mut scratch)
            .unwrap();
        let data = crate::geometry::PreparedPlan::new(shape);
        let context = crate::dispatch::CachelessRowContext {
            frame: &frame,
            domain,
            metric,
            lane: crate::dispatch::Lane::Array(0),
        };
        let GeometryOutput::Pending(pending) = unary_convex_hull_array(&data, &context).unwrap()
        else {
            panic!("geographic array hull captures a pending producer row");
        };
        (pending, frame)
    }

    #[test]
    fn geographic_array_hull_final_writer_rejects_m_only_corruption() {
        let (pending, frame) = geographic_hull_pending();
        let expected = pending.shape.points_vec().unwrap();
        let changed = std::cell::Cell::new(false);
        let corrupted = pending
            .shape
            .map_points(&|point| {
                let m = point.m().expect("the hull retains its M ordinate");
                let m = if changed.replace(true) {
                    m
                } else {
                    f64::from_bits(m.to_bits() ^ 1)
                };
                Ok(Point::new_raw_axes(
                    point.x,
                    point.y,
                    ZOrdinate(point.z()),
                    MOrdinate(Some(m)),
                ))
            })
            .unwrap();
        assert!(changed.get(), "the hull has at least one captured literal");
        let corrupted_points = corrupted.points_vec().unwrap();
        assert_eq!(expected.len(), corrupted_points.len());
        assert!(
            expected
                .iter()
                .zip(&corrupted_points)
                .all(|(before, after)| {
                    before.x.to_bits() == after.x.to_bits()
                        && before.y.to_bits() == after.y.to_bits()
                        && before.z().map(f64::to_bits) == after.z().map(f64::to_bits)
                })
        );
        assert!(
            expected
                .iter()
                .zip(&corrupted_points)
                .any(|(before, after)| before.m() != after.m())
        );

        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();
        let mut writer = StreamingShapes::with_capacity(0, 1);
        writer.try_push(corrupted).unwrap();
        rows.writer = writer;

        assert!(
            rows.finish(frame).is_err(),
            "final ArrayRows readback must reject the writer's changed M"
        );
    }

    #[test]
    fn geographic_array_hull_final_writer_rejects_an_omitted_literal() {
        let (mut pending, frame) = geographic_hull_pending();
        assert!(pending.producer_literals.pop().is_some());
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();

        assert!(
            rows.finish(frame).is_err(),
            "final ArrayRows readback must reject a missing captured literal"
        );
    }

    #[test]
    fn geographic_array_hull_final_writer_rejects_a_wrong_logical_identity() {
        let (mut pending, frame) = geographic_hull_pending();
        pending
            .producer_literals
            .first_mut()
            .expect("the hull contains a literal")
            .address
            .logical_row = 1;
        let mut rows = PublicationRows::with_producer_drawing_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending)).unwrap();

        assert!(
            rows.finish(frame).is_err(),
            "final ArrayRows readback must match the captured logical row"
        );
    }

    fn finish_group_with_receipt_identity_error(
        corrupt_group_identity: bool,
    ) -> crate::error::Result<Groups> {
        let shape = Shape::Point(xyzm(1.0, 2.0, 3.0, 4.0));
        let (layout, literals) = ProducerDrawingReceipt::point_selection_layout(&shape)?;
        let mut pending =
            PendingPublicationRow::producer_drawing(shape, layout, literals, 0, "Groups test")?;
        pending.layout.group_row = Some(0);
        pending.layout.value_row = Some(0);
        for literal in &mut pending.producer_literals {
            literal.address.group_row = Some(0);
            literal.address.value_row = Some(0);
        }
        if corrupt_group_identity {
            pending
                .producer_literals
                .first_mut()
                .expect("the point output has a literal")
                .address
                .group_row = Some(1);
        }

        let mut rows = PublicationRows::with_groups_capacity(1);
        rows.try_push(GeometryOutput::Pending(pending))?;
        rows.end_group(false)?;
        rows.finish_groups(Frame::default(), None)
    }

    #[test]
    fn geographic_groups_final_writer_rejects_a_wrong_group_identity() {
        finish_group_with_receipt_identity_error(false)
            .expect("valid publication identity is accepted");
        finish_group_with_receipt_identity_error(true)
            .err()
            .expect("a wrong group identity is rejected");
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
