use std::collections::BTreeMap;

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
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point, Shape, ShapeCountQuery};
use crate::work::{Unmetered, Work};

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

    fn into_literals(self) -> Result<Vec<ProducerLiteral>> {
        self.snapshot.into_producer_literals()
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
    producer_literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    producer_drawing: bool,
}

impl FinishedGeometry {
    pub(in crate::geometry) const fn output_literal_count(&self) -> usize {
        if self.producer_drawing {
            self.producer_literals.len()
        } else {
            self.checked.len()
        }
    }

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
        image: impl Fn(I) -> Result<PublicationImage>,
    ) -> Result<Self> {
        let (storage, scalar_layout, checked) = sealed.into_publication_parts();
        Ok(Self {
            storage,
            frame,
            scalar_layout: scalar_layout
                .map(|layout| map_shape_layout(layout, &image))
                .transpose()?,
            checked: checked
                .into_iter()
                .map(|literal| {
                    Ok(CheckedPublicationLiteral {
                        address: literal.address,
                        image: image(literal.image)?,
                        record: literal.record,
                        chart: literal.chart,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            producer_literals: Vec::new(),
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
            producer_drawing: false,
        })
    }

    pub(super) const fn from_admitted_rows(
        rows: ArrayRows,
        frame: Frame,
        source_root_uses: Vec<ProducerSourceRootUse>,
        chart_restrictions: Vec<DirectedChartRestrictionUse>,
    ) -> Self {
        Self {
            storage: LiteralStorage::Rows(rows),
            frame,
            scalar_layout: None,
            checked: Vec::new(),
            producer_literals: Vec::new(),
            source_root_uses,
            chart_restrictions,
            producer_drawing: false,
        }
    }

    fn from_producer_sealed(
        sealed: SealedOutput<ProducerImageId>,
        frame: Frame,
        producer_literals: Vec<ProducerLiteral>,
        source_root_uses: Vec<ProducerSourceRootUse>,
        chart_restrictions: Vec<DirectedChartRestrictionUse>,
    ) -> Result<Self> {
        let (storage, scalar_layout, checked) = sealed.into_publication_parts();
        Ok(Self {
            storage,
            frame,
            scalar_layout: scalar_layout
                .map(|layout| {
                    map_shape_layout(layout, &|image| Ok(PublicationImage::ProducerDrawn(image)))
                })
                .transpose()?,
            checked: checked
                .into_iter()
                .map(|literal| CheckedPublicationLiteral {
                    address: literal.address,
                    image: PublicationImage::ProducerDrawn(literal.image),
                    record: literal.record,
                    chart: literal.chart,
                })
                .collect(),
            producer_literals,
            source_root_uses,
            chart_restrictions,
            producer_drawing: true,
        })
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
        let mut producer_literals = self.producer_literals;
        for literal in &mut producer_literals {
            literal.address.logical_row = logical_row;
        }
        let mut source_root_uses = self.source_root_uses;
        for source_use in &mut source_root_uses {
            source_use.address.logical_row = logical_row;
        }
        let producer_drawing = self.producer_drawing;
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
            producer_literals,
            source_root_uses,
            chart_restrictions: self.chart_restrictions,
            producer_drawing,
            work,
        })
    }

    pub(in crate::geometry) fn into_pending_polygon_rows(
        self,
        output_frame: &Frame,
        operation: &'static str,
        base_logical_row: usize,
        group_row: Option<usize>,
        value_row_start: Option<usize>,
    ) -> Result<Vec<PendingPublicationRow>> {
        let Self {
            storage,
            frame,
            scalar_layout,
            checked,
            producer_literals,
            source_root_uses,
            chart_restrictions,
            producer_drawing,
        } = self;
        if !producer_drawing {
            return Err(GeometryErrorKind::Invalid(
                "polygon outputs require a producer-drawn receipt".into(),
            )
            .into());
        }
        if frame != *output_frame {
            return Err(GeometryErrorKind::Invalid(
                "checked Q result frame does not match its admitted output frame".into(),
            )
            .into());
        }
        let LiteralStorage::Scalar(shape) = storage else {
            return Err(GeometryErrorKind::Invalid(
                "only a checked scalar Q result can be split into polygon rows".into(),
            )
            .into());
        };
        let mut layout = scalar_layout.ok_or_else(|| {
            GeometryErrorKind::Invalid("checked Q result has no retained literal layout".into())
        })?;
        if !layout.points.is_empty() || !layout.lines.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "polygon publication contains non-polygon literal rows".into(),
            )
            .into());
        }

        let polygons = match shape {
            Shape::Polygon(polygon) => vec![polygon],
            Shape::MultiPolygon(polygons) => polygons.as_slice().to_vec(),
            Shape::Empty(..) => Vec::new(),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "polygon publication produced a non-polygonal shape".into(),
                )
                .into());
            },
        };
        let polygon_count = polygons.len();
        if layout.polygons.len() != polygons.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon literal rows do not match their final storage".into(),
            )
            .into());
        }

        if !checked.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "ProducerDrawing polygon output retained a second literal ledger".into(),
            )
            .into());
        }
        let had_producer_literals = !producer_literals.is_empty();
        let had_source_uses = !source_root_uses.is_empty();
        let had_chart_restrictions = !chart_restrictions.is_empty();
        let mut literals_by_output = BTreeMap::<u32, Vec<ProducerLiteral>>::new();
        for literal in producer_literals {
            literals_by_output
                .entry(literal.address.output)
                .or_default()
                .push(literal);
        }
        let mut source_uses_by_output = BTreeMap::<u32, Vec<ProducerSourceRootUse>>::new();
        for source_use in source_root_uses {
            source_uses_by_output
                .entry(source_use.address.output)
                .or_default()
                .push(source_use);
        }

        let mut output_rows = Vec::with_capacity(polygons.len());
        for (index, (polygon, literal_polygon)) in polygons
            .into_iter()
            .zip(layout.polygons.drain(..))
            .enumerate()
        {
            let output = literal_polygon.output;
            let logical_row = if group_row.is_some() {
                base_logical_row
            } else {
                base_logical_row
                    .checked_add(index)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?
            };
            let value_row = value_row_start
                .map(|start| {
                    start
                        .checked_add(index)
                        .ok_or(crate::error::ResourceError::SizeOverflow)
                })
                .transpose()?;
            let address = |address: &mut LiteralAddress| {
                address.logical_row = logical_row;
                address.group_row = group_row;
                address.value_row = value_row;
                address.output = 0;
            };
            let mut literals = literals_by_output.remove(&output).unwrap_or_default();
            for literal in &mut literals {
                address(&mut literal.address);
            }
            let mut source_uses = source_uses_by_output.remove(&output).unwrap_or_default();
            for source_use in &mut source_uses {
                address(&mut source_use.address);
            }
            let row_layout = ShapeLiteralLayout {
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes: literal_polygon.axes,
                    shell: LiteralSequence {
                        output: 0,
                        axes: literal_polygon.shell.axes,
                        images: literal_polygon.shell.images,
                    },
                    holes: literal_polygon
                        .holes
                        .into_iter()
                        .map(|hole| LiteralSequence {
                            output: 0,
                            axes: hole.axes,
                            images: hole.images,
                        })
                        .collect(),
                }],
            };
            let shape = Shape::Polygon(polygon);
            let coordinates = ShapeCountQuery::default()
                .coordinate_count(&shape)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygon output coordinate count is unavailable".into(),
                    )
                })?;
            output_rows.push(PendingPublicationRow {
                shape: Some(shape),
                logical_row,
                layout: LiteralRowLayout {
                    operation: 0,
                    logical_row,
                    group_row,
                    value_row,
                    layout: row_layout,
                },
                checked: Vec::new(),
                producer_literals: literals,
                source_root_uses: source_uses,
                chart_restrictions: if index == 0 {
                    chart_restrictions.clone()
                } else {
                    Vec::new()
                },
                producer_drawing: true,
                work: EvaluationBudget::new(operation, coordinates)?,
            });
        }
        if !literals_by_output.is_empty() || !source_uses_by_output.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "polygon producer receipt references an unknown output row".into(),
            )
            .into());
        }
        if polygon_count == 0
            && (had_producer_literals || had_source_uses || had_chart_restrictions)
        {
            return Err(GeometryErrorKind::Invalid(
                "empty polygon output retained producer literal uses".into(),
            )
            .into());
        }
        Ok(output_rows)
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

fn map_sequence<I: Copy, O>(
    sequence: LiteralSequence<I>,
    image: &impl Fn(I) -> Result<O>,
) -> Result<LiteralSequence<O>> {
    Ok(LiteralSequence {
        output: sequence.output,
        axes: sequence.axes,
        images: sequence
            .images
            .into_iter()
            .map(image)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn map_shape_layout<I: Copy, O>(
    layout: ShapeLiteralLayout<I>,
    image: &impl Fn(I) -> Result<O>,
) -> Result<ShapeLiteralLayout<O>> {
    Ok(ShapeLiteralLayout {
        points: layout
            .points
            .into_iter()
            .map(|point| {
                Ok(LiteralPoint {
                    output: point.output,
                    axes: point.axes,
                    image: image(point.image)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        lines: layout
            .lines
            .into_iter()
            .map(|line| map_sequence(line, image))
            .collect::<Result<Vec<_>>>()?,
        polygons: layout
            .polygons
            .into_iter()
            .map(|polygon| {
                Ok(LiteralPolygon {
                    output: polygon.output,
                    axes: polygon.axes,
                    shell: map_sequence(polygon.shell, image)?,
                    holes: polygon
                        .holes
                        .into_iter()
                        .map(|hole| map_sequence(hole, image))
                        .collect::<Result<Vec<_>>>()?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

pub(super) fn producer_row_layout(
    row: LiteralRowLayout<PublicationImage>,
) -> Result<LiteralRowLayout<ProducerImageId>> {
    let LiteralRowLayout {
        operation,
        logical_row,
        group_row,
        value_row,
        layout,
    } = row;
    let layout = map_shape_layout(layout, &|image| match image {
        PublicationImage::ProducerDrawn(image) => Ok(image),
        PublicationImage::Native(_) | PublicationImage::SourceOnly(_) => {
            Err(GeometryErrorKind::Invalid(
                "producer-drawn storage contains a different publication image family".into(),
            )
            .into())
        },
    })?;
    Ok(LiteralRowLayout {
        operation,
        logical_row,
        group_row,
        value_row,
        layout,
    })
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
                        |image| Ok(PublicationImage::Native(image)),
                    )?))
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
                        |image| Ok(PublicationImage::SourceOnly(image)),
                    )?))
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
            retain_literals_for_republication,
        }) => {
            let mut receiver = ProducerImageReceiver::new(literals)?;
            for source_use in &source_root_uses {
                work.charge_into_error(1)?;
                let image = PublicationImage::ProducerDrawn(source_use.image);
                if !receiver.snapshot.contains(source_use.address, image) {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "producer source-root use for image {:?} at {:?} has no checked output literal; checked image addresses: {:?}",
                            source_use.image,
                            source_use.address,
                            receiver.snapshot.addresses_for_image(image)
                        )
                        .into(),
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
                LiteralCheck::Pass(sealed) => {
                    let producer_literals = if retain_literals_for_republication {
                        receiver.into_literals()?
                    } else {
                        Vec::new()
                    };
                    Ok(PublicationOutcome::Pass(
                        FinishedGeometry::from_producer_sealed(
                            sealed,
                            frame,
                            producer_literals,
                            source_root_uses,
                            chart_restrictions,
                        )?,
                    ))
                },
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
