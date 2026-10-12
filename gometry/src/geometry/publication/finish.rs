use super::receipt::{
    CheckedPublicationLiteral, PendingPublicationRow, ProducerReceipt, PublicationCall,
    PublicationImage, PublicationOutcome, PublicationRejection, StorageTarget,
};
use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FinalGroupRows, LiteralPoint, LiteralPolygon, LiteralRowLayout, LiteralSequence,
    LiteralStorage, SealedOutput, ShapeLiteralLayout,
};
use crate::geometry::embedding_search::EvaluationBudget;
use crate::geometry::storage::ArrayRows;
use crate::geometry::topology::q_embedding::publication as q_publication;
use crate::geometry::{GeometryErrorKind, Shape, Work};

pub(crate) struct FinishedGeometry {
    storage: LiteralStorage,
    frame: Frame,
    scalar_layout: Option<ShapeLiteralLayout<PublicationImage>>,
    checked: Vec<CheckedPublicationLiteral>,
}

impl FinishedGeometry {
    pub(super) fn from_sealed<I: Copy>(
        sealed: SealedOutput<I>,
        frame: Frame,
        image: impl Fn(I) -> PublicationImage,
    ) -> Self {
        let (storage, scalar_layout, checked) = sealed.into_publication_parts();
        Self {
            storage,
            frame,
            scalar_layout: scalar_layout.map(|layout| map_shape_layout(layout, &image)),
            checked: checked
                .into_iter()
                .map(|literal| CheckedPublicationLiteral {
                    address: literal.address,
                    image: image(literal.image),
                    record: literal.record,
                    chart: literal.chart,
                })
                .collect(),
        }
    }

    pub(super) const fn from_admitted_rows(rows: ArrayRows, frame: Frame) -> Self {
        Self {
            storage: LiteralStorage::Rows(rows),
            frame,
            scalar_layout: None,
            checked: Vec::new(),
        }
    }

    pub(in crate::geometry) fn into_pending_row(
        mut self,
        logical_row: usize,
        work: EvaluationBudget,
        output_frame: &Frame,
    ) -> Result<PendingPublicationRow> {
        if self.frame != *output_frame {
            return Err(GeometryErrorKind::Invalid(
                "checked Q result frame does not match its admitted array frame".into(),
            )
            .into());
        }
        let LiteralStorage::Scalar(shape) = self.storage else {
            return Err(GeometryErrorKind::Invalid(
                "only a checked scalar Q result can enter a row publication batch".into(),
            )
            .into());
        };
        let layout = self.scalar_layout.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("checked Q result has no retained literal layout".into())
        })?;
        let mut checked = self.checked;
        for literal in &mut checked {
            literal.address.logical_row = logical_row;
        }
        Ok(PendingPublicationRow {
            shape: Some(shape),
            logical_row,
            layout: LiteralRowLayout {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                layout,
            },
            checked,
            work,
        })
    }

    pub(crate) fn into_scalar(self) -> Result<(Shape, Frame)> {
        match self.storage {
            LiteralStorage::Scalar(shape) => Ok((shape, self.frame)),
            LiteralStorage::Rows(_) | LiteralStorage::Groups(_) => Err(GeometryErrorKind::Invalid(
                "non-scalar finished geometry cannot be adopted as a scalar".into(),
            )
            .into()),
        }
    }

    pub(crate) fn into_array(self) -> Result<(ArrayRows, Frame)> {
        match self.storage {
            LiteralStorage::Rows(rows) => Ok((rows, self.frame)),
            LiteralStorage::Scalar(_) | LiteralStorage::Groups(_) => {
                Err(GeometryErrorKind::Invalid(
                    "finished geometry does not own admitted array rows".into(),
                )
                .into())
            },
        }
    }

    pub(crate) fn into_groups(self) -> Result<(FinalGroupRows, Frame)> {
        match self.storage {
            LiteralStorage::Groups(groups) => Ok((groups, self.frame)),
            LiteralStorage::Scalar(_) | LiteralStorage::Rows(_) => Err(GeometryErrorKind::Invalid(
                "finished geometry does not own admitted Groups rows".into(),
            )
            .into()),
        }
    }
}

fn map_sequence<I: Copy>(
    sequence: LiteralSequence<I>,
    image: &impl Fn(I) -> PublicationImage,
) -> LiteralSequence<PublicationImage> {
    LiteralSequence {
        output: sequence.output,
        axes: sequence.axes,
        images: sequence.images.into_iter().map(image).collect(),
    }
}

fn map_shape_layout<I: Copy>(
    layout: ShapeLiteralLayout<I>,
    image: &impl Fn(I) -> PublicationImage,
) -> ShapeLiteralLayout<PublicationImage> {
    ShapeLiteralLayout {
        points: layout
            .points
            .into_iter()
            .map(|point| LiteralPoint {
                output: point.output,
                axes: point.axes,
                image: image(point.image),
            })
            .collect(),
        lines: layout
            .lines
            .into_iter()
            .map(|line| map_sequence(line, image))
            .collect(),
        polygons: layout
            .polygons
            .into_iter()
            .map(|polygon| LiteralPolygon {
                output: polygon.output,
                axes: polygon.axes,
                shell: map_sequence(polygon.shell, image),
                holes: polygon
                    .holes
                    .into_iter()
                    .map(|hole| map_sequence(hole, image))
                    .collect(),
            })
            .collect(),
    }
}

pub(in crate::geometry) fn finish_geographic<W: Work>(
    call: PublicationCall<'_, '_, '_, W>,
    target: StorageTarget,
) -> Result<PublicationOutcome> {
    let PublicationCall {
        receipt,
        frame,
        work,
    } = call;
    match receipt {
        ProducerReceipt::QNative(receipt) => {
            match q_publication::bind_native_publication(receipt, target, work)? {
                q_publication::NativePublicationCheck::Pass(sealed) => {
                    Ok(PublicationOutcome::Pass(FinishedGeometry::from_sealed(
                        sealed,
                        frame,
                        PublicationImage::Native,
                    )))
                },
                q_publication::NativePublicationCheck::Reject => {
                    Ok(PublicationOutcome::Reject(PublicationRejection::Native))
                },
            }
        },
        ProducerReceipt::QSourceOnly(receipt) => {
            match q_publication::bind_source_publication(receipt, target, work)? {
                q_publication::SourcePublicationCheck::Pass(sealed) => {
                    Ok(PublicationOutcome::Pass(FinishedGeometry::from_sealed(
                        sealed,
                        frame,
                        PublicationImage::SourceOnly,
                    )))
                },
                q_publication::SourcePublicationCheck::Reject(images) => Ok(
                    PublicationOutcome::Reject(PublicationRejection::SourceOnly(images)),
                ),
            }
        },
        ProducerReceipt::ArrayBatch(batch) => {
            if target != StorageTarget::Array {
                return Err(GeometryErrorKind::Invalid(
                    "ArrayRows receipt requires an Array storage target".into(),
                )
                .into());
            }
            super::storage::finish_array_batch(batch, frame, work)
        },
    }
}
