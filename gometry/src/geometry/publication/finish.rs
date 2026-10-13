use super::DirectedChartRestrictionUse;
use super::receipt::{
    CheckedPublicationLiteral, PendingPublicationRow, ProducerDrawingReceipt, ProducerImageId,
    ProducerLiteral, ProducerReceipt, ProducerSourceRootUse, PublicationCall, PublicationImage,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::{
    CheckedImages, FinalGroupRows, LiteralAddress, LiteralChart, LiteralCheck, LiteralPoint,
    LiteralPolygon, LiteralRowLayout, LiteralSequence, LiteralStorage, SealedOutput,
    ShapeLiteralLayout, bind_final_literals,
};
use crate::geometry::embedding_search::EvaluationBudget;
use crate::geometry::storage::ArrayRows;
use crate::geometry::topology::ExactChartPoint;
use crate::geometry::topology::q_embedding::publication as q_publication;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point, Shape, Work};

struct ProducerImageReceiver {
    snapshot: super::binding::CheckedLiteralSnapshot,
}

impl ProducerImageReceiver {
    fn new(literals: Vec<ProducerLiteral>) -> Result<Self> {
        let checked = literals
            .into_iter()
            .map(|literal| CheckedPublicationLiteral {
                address: literal.address,
                image: PublicationImage::ProducerDrawn(literal.image),
                record: literal.record,
                chart: literal.chart,
            });
        Ok(Self {
            snapshot: super::binding::CheckedLiteralSnapshot::new(checked)?,
        })
    }
}

impl CheckedImages<ProducerImageId> for ProducerImageReceiver {
    fn image(&self, address: LiteralAddress, image: ProducerImageId) -> Result<&ExactChartPoint> {
        self.snapshot
            .image(address, PublicationImage::ProducerDrawn(image))
    }
}

impl LiteralChart<ProducerImageId> for ProducerImageReceiver {
    fn read_point<W: Work>(
        &mut self,
        address: LiteralAddress,
        image: ProducerImageId,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        self.snapshot.read_point(
            address,
            PublicationImage::ProducerDrawn(image),
            record,
            expected_axes,
            stored_axes,
            work,
        )
    }
}

pub(crate) struct FinishedGeometry {
    storage: LiteralStorage,
    frame: Frame,
    scalar_layout: Option<ShapeLiteralLayout<PublicationImage>>,
    checked: Vec<CheckedPublicationLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
}

impl FinishedGeometry {
    #[cfg(test)]
    pub(super) fn producer_source_root_uses(&self) -> &[ProducerSourceRootUse] {
        &self.source_root_uses
    }

    #[cfg(test)]
    pub(super) fn producer_chart_restrictions(&self) -> &[DirectedChartRestrictionUse] {
        &self.chart_restrictions
    }

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
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
        }
    }

    pub(super) const fn from_admitted_rows(rows: ArrayRows, frame: Frame) -> Self {
        Self {
            storage: LiteralStorage::Rows(rows),
            frame,
            scalar_layout: None,
            checked: Vec::new(),
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
        }
    }

    fn from_producer_sealed(
        sealed: SealedOutput<ProducerImageId>,
        frame: Frame,
        source_root_uses: Vec<ProducerSourceRootUse>,
        chart_restrictions: Vec<DirectedChartRestrictionUse>,
    ) -> Self {
        let (storage, scalar_layout, checked) = sealed.into_publication_parts();
        Self {
            storage,
            frame,
            scalar_layout: scalar_layout.map(|layout| {
                map_shape_layout(layout, &|image| PublicationImage::ProducerDrawn(image))
            }),
            checked: checked
                .into_iter()
                .map(|literal| CheckedPublicationLiteral {
                    address: literal.address,
                    image: PublicationImage::ProducerDrawn(literal.image),
                    record: literal.record,
                    chart: literal.chart,
                })
                .collect(),
            source_root_uses,
            chart_restrictions,
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
        let Self {
            storage,
            frame,
            source_root_uses,
            chart_restrictions,
            ..
        } = self;
        drop(source_root_uses);
        drop(chart_restrictions);
        match storage {
            LiteralStorage::Scalar(shape) => Ok((shape, frame)),
            LiteralStorage::Rows(_) | LiteralStorage::Groups(_) => Err(GeometryErrorKind::Invalid(
                "non-scalar finished geometry cannot be adopted as a scalar".into(),
            )
            .into()),
        }
    }

    pub(crate) fn into_array(self) -> Result<(ArrayRows, Frame)> {
        let Self {
            storage,
            frame,
            source_root_uses,
            chart_restrictions,
            ..
        } = self;
        drop(source_root_uses);
        drop(chart_restrictions);
        match storage {
            LiteralStorage::Rows(rows) => Ok((rows, frame)),
            LiteralStorage::Scalar(_) | LiteralStorage::Groups(_) => {
                Err(GeometryErrorKind::Invalid(
                    "finished geometry does not own admitted array rows".into(),
                )
                .into())
            },
        }
    }

    pub(crate) fn into_groups(self) -> Result<(FinalGroupRows, Frame)> {
        let Self {
            storage,
            frame,
            source_root_uses,
            chart_restrictions,
            ..
        } = self;
        drop(source_root_uses);
        drop(chart_restrictions);
        match storage {
            LiteralStorage::Groups(groups) => Ok((groups, frame)),
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
        ProducerReceipt::ProducerDrawing(ProducerDrawingReceipt {
            buffers,
            literals,
            source_root_uses,
            chart_restrictions,
        }) => {
            let mut receiver = ProducerImageReceiver::new(literals)?;
            for source_use in &source_root_uses {
                work.charge_into_error(1)?;
                let image = PublicationImage::ProducerDrawn(source_use.image);
                if !receiver.snapshot.contains(source_use.address, image) {
                    return Err(GeometryErrorKind::Invalid(
                        "producer source-root use has no checked output literal".into(),
                    )
                    .into());
                }
                let _ = &source_use.range;
            }
            work.charge_into_error(
                u64::try_from(chart_restrictions.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            )?;
            let buffers = buffers.admit_target(target)?;
            match bind_final_literals(buffers, &mut receiver, work)? {
                LiteralCheck::Pass(sealed) => Ok(PublicationOutcome::Pass(
                    FinishedGeometry::from_producer_sealed(
                        sealed,
                        frame,
                        source_root_uses,
                        chart_restrictions,
                    ),
                )),
                LiteralCheck::Reject(images) => Ok(PublicationOutcome::Reject(
                    PublicationRejection::ProducerDrawing(images),
                )),
            }
        },
    }
}

pub(in crate::geometry) fn finish_producer_drawing(
    receipt: ProducerDrawingReceipt,
    frame: Frame,
    target: StorageTarget,
) -> Result<FinishedGeometry> {
    use crate::geometry::Unmetered;

    let mut work = Unmetered;
    match finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        target,
    )? {
        PublicationOutcome::Pass(finished) => Ok(finished),
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            Err(GeometryErrorKind::Invalid(
                format!(
                    "final producer storage changed {} checked output records",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "producer finisher returned a different rejection family".into(),
        )
        .into()),
    }
}
