use std::sync::Arc;

use super::binding::CheckedLiteralSnapshot;
use super::finish::FinishedGeometry;
use super::receipt::{
    ArrayPublicationBatch, CheckedPublicationLiteral, GeometryOutput, PendingAdmission,
    PendingPublicationRow, ProducerImageId, ProducerReceipt, PublicationCall, PublicationImage,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geometry::embedding_completion::binding::bind_final_literals;
use crate::geometry::embedding_completion::literals::{
    FlatLiteralLayout, FlatRowBinding, LiteralAddress,
};
use crate::geometry::embedding_completion::storage::{
    FrozenLiteralBuffers, LiteralCheck, admit_flat_rows,
};
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::storage::{ArrayRows, LogicalRow, ShapeRow, ValidityMask};
use crate::geometry::{Coordinates as _, GeometryErrorKind, Point, Shape, ShapeCountQuery};
use crate::py::vectors::Groups;
use crate::work::{Unmetered, Work};

enum ExpectedRows {
    Unreceipted(Vec<bool>),
    Checked(FlatLiteralLayout<PublicationImage>),
}

impl ExpectedRows {
    const fn len(&self) -> usize {
        match self {
            Self::Unreceipted(present) => present.len(),
            Self::Checked(layout) => layout.rows.len(),
        }
    }

    fn checked(&mut self) -> &mut FlatLiteralLayout<PublicationImage> {
        if let Self::Unreceipted(present) = self {
            let mut layout = FlatLiteralLayout::with_capacity(present.capacity());
            layout.rows.extend(
                present
                    .iter()
                    .map(|&present| present.then_some(FlatRowBinding::OtherProducer)),
            );
            *self = Self::Checked(layout);
        }
        let Self::Checked(layout) = self else {
            unreachable!("the independent domain was upgraded to checked rows");
        };
        layout
    }
}

pub(crate) struct PublicationRows {
    writer: StreamingShapes,
    expected: ExpectedRows,
    records: Vec<CheckedPublicationLiteral>,
    accounts: Vec<(usize, EvaluationBudget)>,
    group_offsets: Option<Vec<i64>>,
    expected_group_present: Vec<bool>,
    producer_drawing_required: bool,
    drawing_only: bool,
}

impl PublicationRows {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            expected: ExpectedRows::Unreceipted(Vec::with_capacity(rows)),
            records: Vec::new(),
            accounts: Vec::new(),
            group_offsets: None,
            expected_group_present: Vec::new(),
            producer_drawing_required: false,
            drawing_only: true,
        }
    }

    pub(crate) fn with_producer_drawing_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            expected: ExpectedRows::Checked(FlatLiteralLayout::with_capacity(rows)),
            producer_drawing_required: true,
            ..Self::with_capacity(0)
        }
    }

    pub(crate) fn with_groups_capacity(groups: usize) -> Self {
        let mut output = Self::with_producer_drawing_capacity(0);
        let mut offsets = Vec::with_capacity(groups.saturating_add(1));
        offsets.push(0);
        output.group_offsets = Some(offsets);
        output.expected_group_present = Vec::with_capacity(groups);
        output
    }

    fn next_address(&self, logical_row: usize) -> LiteralAddress {
        LiteralAddress {
            operation: 0,
            logical_row,
            group_row: self.group_offsets.as_ref().map(|_| logical_row),
            value_row: self.group_offsets.as_ref().map(|_| self.expected.len()),
            output: 0,
        }
    }

    fn capture_shape(&mut self, address: LiteralAddress, shape: &Shape) -> Result<()> {
        let records = &mut self.records;
        let layout = self.expected.checked();
        if records.is_empty() {
            records.reserve_exact(layout.rows.capacity());
        }
        layout.capture_shape(shape, address, &mut |address, record| {
            capture_record(records, address, record)
        })
    }

    pub(crate) fn try_push_drawing(
        &mut self,
        shape: Shape,
        logical_row: usize,
        operation: &'static str,
    ) -> Result<()> {
        self.try_push(GeometryOutput::Pending(PendingPublicationRow::drawing(
            shape,
            logical_row,
            operation,
        )?))
    }

    pub(in crate::geometry) fn try_push_drawing_line(
        &mut self,
        row: ShapeRow<'_>,
        logical_row: usize,
        operation: &'static str,
    ) -> Result<()> {
        let coordinates = row
            .line_coords()
            .ok_or_else(|| invalid("drawing line has no coordinate window"))?;
        let count = coordinates.coord_count();
        let mut work = EvaluationBudget::new(operation, count)?;
        work.charge(
            u64::try_from(count)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let address = self.next_address(logical_row);
        let records = &mut self.records;
        let layout = self.expected.checked();
        if records.is_empty() {
            records.reserve_exact(layout.rows.capacity());
        }
        layout.capture_line(&coordinates, row.axes(), address, &mut |address, record| {
            capture_record(records, address, record)
        })?;
        self.writer.try_push_row(Some(row))
    }

    pub(crate) fn try_push(&mut self, output: GeometryOutput) -> Result<()> {
        match output {
            GeometryOutput::Shape(shape) => {
                match &mut self.expected {
                    ExpectedRows::Unreceipted(present) => present.push(true),
                    ExpectedRows::Checked(layout) => {
                        layout.rows.push(Some(FlatRowBinding::OtherProducer));
                    },
                }
                self.drawing_only = false;
                self.writer.try_push(shape)?;
            },
            GeometryOutput::Pending(PendingPublicationRow {
                shape,
                logical_row,
                admission,
            }) => {
                match admission {
                    PendingAdmission::Drawing => {
                        self.capture_shape(self.next_address(logical_row), &shape)?;
                    },
                    PendingAdmission::Checked {
                        layout,
                        records,
                        mut work,
                        #[cfg(test)]
                        producer_drawing,
                    } => {
                        #[cfg(not(test))]
                        let producer_drawing = false;
                        let ticks = u64::try_from(records.len())
                            .map_err(|_| ResourceError::SizeOverflow)?
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?;
                        work.charge(ticks)?;
                        if layout.logical_row != logical_row {
                            return Err(invalid(
                                "pending row changed its accepted logical address",
                            ));
                        }
                        let index = self.expected.len();
                        self.expected.checked().push_layout(layout)?;
                        self.records.extend(records);
                        if !producer_drawing {
                            self.accounts.push((index, work));
                            self.drawing_only = false;
                        }
                    },
                }
                self.writer.try_push(shape)?;
            },
        }
        Ok(())
    }

    pub(crate) fn try_push_missing(&mut self) -> Result<()> {
        if self.group_offsets.is_some() {
            return Err(invalid(
                "a missing group cannot be stored as a missing geometry value",
            ));
        }
        match &mut self.expected {
            ExpectedRows::Unreceipted(present) => present.push(false),
            ExpectedRows::Checked(layout) => layout.rows.push(None),
        }
        self.writer.try_push_missing()
    }

    pub(crate) fn end_group(&mut self, missing: bool) -> Result<()> {
        let count = self.expected.len();
        let offsets = self
            .group_offsets
            .as_mut()
            .ok_or_else(|| invalid("only a Groups publication has group boundaries"))?;
        offsets.push(i64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?);
        self.expected_group_present.push(!missing);
        Ok(())
    }

    pub(crate) const fn next_value_row(&self) -> usize {
        self.expected.len()
    }

    pub(in crate::geometry) const fn next_group_row(&self) -> usize {
        self.expected_group_present.len()
    }

    pub(in crate::geometry) const fn next_drawing_row(&self) -> usize {
        if self.group_offsets.is_some() {
            self.next_group_row()
        } else {
            self.next_value_row()
        }
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<crate::PyGeometryArray> {
        crate::PyGeometryArray::from_finished(self.finish_geometry(frame)?)
    }

    pub(crate) fn finish_geometry(self, frame: Frame) -> Result<FinishedGeometry> {
        if self.group_offsets.is_some() {
            return Err(invalid(
                "a Groups publication must finish through finish_groups",
            ));
        }
        let (storage, missing, layout) = self.writer.finish_parts()?;
        let expected = match self.expected {
            ExpectedRows::Checked(expected) => expected,
            ExpectedRows::Unreceipted(present) => {
                ArrayRows::validate_publication_domain(
                    missing.as_ref().map_or(storage.len(), ValidityMask::len),
                    missing.as_ref(),
                    present.into_iter(),
                )?;
                return Ok(FinishedGeometry::from_admitted_rows(
                    ArrayRows::admit(Arc::new(storage), missing, layout)?,
                    frame,
                ));
            },
        };
        if self.producer_drawing_required
            && expected
                .rows
                .iter()
                .any(|row| matches!(row, Some(FlatRowBinding::OtherProducer)))
        {
            return Err(invalid(
                "producer-drawn publication contains an output without a receipt",
            ));
        }
        let batch = ArrayPublicationBatch {
            storage: Arc::new(storage),
            missing,
            layout,
            expected,
            records: self.records,
            accounts: self.accounts,
            drawing_only: self.drawing_only,
        };
        let mut work = Unmetered;
        match super::finish::finish_geographic(
            PublicationCall::new(ProducerReceipt::ArrayBatch(batch), frame, &mut work),
            StorageTarget::Array,
        )? {
            PublicationOutcome::Pass(finished) => Ok(finished),
            PublicationOutcome::Reject(PublicationRejection::Batch(images)) => Err(
                changed_records("array", "checked Q output records", images.len()),
            ),
            PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => Err(
                changed_records("array", "producer literal images", images.len()),
            ),
            PublicationOutcome::Reject(_) => Err(invalid(
                "array publication returned a geometric admission rejection",
            )),
        }
    }

    pub(crate) fn finish_groups(
        self,
        frame: Frame,
        missing: Option<ValidityMask>,
    ) -> Result<Groups> {
        Groups::from_finished(self.finish_groups_geometry(frame, missing)?)
    }

    pub(crate) fn finish_groups_geometry(
        self,
        frame: Frame,
        missing: Option<ValidityMask>,
    ) -> Result<FinishedGeometry> {
        let offsets = self
            .group_offsets
            .ok_or_else(|| invalid("an ArrayRows publication has no group offsets"))?;
        if !self.producer_drawing_required || !self.drawing_only || !self.accounts.is_empty() {
            return Err(invalid(
                "Groups publication requires producer-drawn receipts",
            ));
        }
        let ExpectedRows::Checked(expected) = self.expected else {
            return Err(invalid("Groups publication has no accepted value domain"));
        };
        if expected
            .rows
            .iter()
            .any(|row| !matches!(row, Some(FlatRowBinding::Checked(_))))
        {
            return Err(invalid(
                "producer-drawn Groups storage is missing a value receipt",
            ));
        }
        let group_count = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| invalid("Groups offsets have no origin"))?;
        ArrayRows::validate_publication_domain(
            group_count,
            missing.as_ref(),
            self.expected_group_present.iter().copied(),
        )?;
        let (storage, value_missing, storage_layout) = self.writer.finish_parts()?;
        let mut work = Unmetered;
        let buffers = FrozenLiteralBuffers::flat_groups(
            Arc::new(storage),
            value_missing,
            storage_layout,
            expected,
            offsets,
            missing,
            self.expected_group_present,
            &mut work,
        )?;
        let mut receiver = CheckedLiteralSnapshot::new(self.records)?;
        match bind_final_literals(buffers, &mut receiver, &mut work)? {
            LiteralCheck::Pass(sealed) => FinishedGeometry::from_sealed(sealed, frame, Ok),
            LiteralCheck::Reject(images) => Err(changed_records(
                "Groups",
                "producer literal images",
                images.len(),
            )),
        }
    }
}

pub(super) fn finish_array_batch<W: Work>(
    batch: ArrayPublicationBatch,
    frame: Frame,
    work: &mut W,
) -> Result<PublicationOutcome> {
    let ArrayPublicationBatch {
        storage,
        missing,
        layout,
        mut expected,
        records,
        accounts,
        drawing_only,
    } = batch;
    let mut unmetered = Unmetered;
    let rows = admit_flat_rows(storage, missing, layout, &mut expected, &mut unmetered)?;
    let expected = Arc::new(expected);
    let mut receiver = CheckedLiteralSnapshot::new(records)?;
    let mut accounts = accounts.into_iter().peekable();
    for logical in 0..rows.len() {
        work.charge_into_error(1)?;
        let check = if accounts.peek().is_some_and(|(row, _)| *row == logical) {
            let (_, mut row_work) = accounts.next().expect("the account was peeked");
            let Some(LogicalRow::Present { row, .. }) = rows.logical_row(logical) else {
                return Err(invalid("checked Q account has no present final row"));
            };
            let coordinates = row.coordinates(&mut ShapeCountQuery::default())?;
            row_work.charge_into_error(
                u64::try_from(coordinates)
                    .map_err(|_| ResourceError::SizeOverflow)?
                    .checked_add(3)
                    .ok_or(ResourceError::SizeOverflow)?,
            )?;
            let buffers = FrozenLiteralBuffers::admitted_row_view(
                rows.clone(),
                Arc::clone(&expected),
                logical,
            )?;
            bind_final_literals(buffers, &mut receiver, &mut row_work)?
        } else {
            let buffers = FrozenLiteralBuffers::admitted_row_view(
                rows.clone(),
                Arc::clone(&expected),
                logical,
            )?;
            bind_final_literals(buffers, &mut receiver, &mut unmetered)?
        };
        if let LiteralCheck::Reject(images) = check {
            let rejection = if drawing_only {
                PublicationRejection::ProducerDrawing(
                    images
                        .into_iter()
                        .map(|image| match image {
                            PublicationImage::ProducerDrawn(image) => Ok(image),
                            PublicationImage::Native(_) | PublicationImage::SourceOnly(_) => {
                                Err(invalid("drawing publication contains a Q image"))
                            },
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                PublicationRejection::Batch(images)
            };
            return Ok(PublicationOutcome::Reject(rejection));
        }
    }
    if accounts.next().is_some() {
        return Err(invalid(
            "Q work account is outside the final logical domain",
        ));
    }
    Ok(PublicationOutcome::Pass(
        FinishedGeometry::from_admitted_rows(rows, frame),
    ))
}

fn invalid(message: &'static str) -> crate::error::Error {
    GeometryErrorKind::Invalid(message.into()).into()
}

fn capture_record(
    records: &mut Vec<CheckedPublicationLiteral>,
    address: LiteralAddress,
    record: Point,
) -> Result<PublicationImage> {
    let index = records.len();
    index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let image = PublicationImage::ProducerDrawn(ProducerImageId::new(index));
    records.push(CheckedPublicationLiteral {
        address,
        image,
        record,
    });
    Ok(image)
}

fn changed_records(target: &'static str, kind: &'static str, count: usize) -> crate::error::Error {
    GeometryErrorKind::Invalid(format!("final {target} writer changed {count} {kind}").into())
        .into()
}

#[cfg(test)]
#[path = "storage/boundary_tests.rs"]
mod boundary_tests;

#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
