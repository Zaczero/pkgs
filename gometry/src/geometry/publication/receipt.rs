use super::DirectedChartRestrictionUse;
use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralRowLayout,
};
use crate::geometry::embedding_search::{EvaluationBudget, Work};
use crate::geometry::ordinates::RootRange;
use crate::geometry::storage::{GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::q_embedding::publication::{
    NativeCandidatePublication, SourceOnlyPublication,
};
use crate::geometry::topology::{EmissionPortId, ExactChartPoint, QSourceImage};
use crate::geometry::{GeometryErrorKind, Shape};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) enum PublicationImage {
    Native(EmissionPortId),
    SourceOnly(QSourceImage),
    ProducerDrawn(ProducerImageId),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) struct ProducerImageId(usize);

impl ProducerImageId {
    pub(in crate::geometry) const fn new(index: usize) -> Self {
        Self(index)
    }
}

pub(in crate::geometry) struct ProducerLiteral {
    pub(in crate::geometry) address: LiteralAddress,
    pub(in crate::geometry) image: ProducerImageId,
    pub(in crate::geometry) record: crate::Point,
    pub(in crate::geometry) chart: ExactChartPoint,
}

impl ProducerLiteral {
    pub(in crate::geometry) fn from_point(
        address: LiteralAddress,
        image: ProducerImageId,
        record: crate::Point,
    ) -> Self {
        Self {
            address,
            image,
            record,
            chart: ExactChartPoint::from_rationals(
                ExactRational::from_f64(record.x),
                ExactRational::from_f64(record.y),
            ),
        }
    }
}

pub(in crate::geometry) struct ProducerSourceRootUse {
    pub(in crate::geometry) address: LiteralAddress,
    pub(in crate::geometry) image: ProducerImageId,
    pub(in crate::geometry) range: RootRange,
}

pub(in crate::geometry) struct ProducerDrawingReceipt {
    pub(super) buffers: FrozenLiteralBuffers<ProducerImageId>,
    pub(super) literals: Vec<ProducerLiteral>,
    pub(super) source_root_uses: Vec<ProducerSourceRootUse>,
    pub(super) chart_restrictions: Vec<DirectedChartRestrictionUse>,
}

impl ProducerDrawingReceipt {
    pub(in crate::geometry) const fn new(
        buffers: FrozenLiteralBuffers<ProducerImageId>,
        literals: Vec<ProducerLiteral>,
        source_root_uses: Vec<ProducerSourceRootUse>,
        chart_restrictions: Vec<DirectedChartRestrictionUse>,
    ) -> Self {
        Self {
            buffers,
            literals,
            source_root_uses,
            chart_restrictions,
        }
    }
}

#[derive(Clone)]
pub(super) struct CheckedPublicationLiteral {
    pub(super) address: LiteralAddress,
    pub(super) image: PublicationImage,
    pub(super) record: crate::Point,
    pub(super) chart: ExactChartPoint,
}

pub(crate) struct PendingPublicationRow {
    pub(super) shape: Option<Shape>,
    pub(super) logical_row: usize,
    pub(super) layout: LiteralRowLayout<PublicationImage>,
    pub(super) checked: Vec<CheckedPublicationLiteral>,
    pub(super) source_root_uses: Vec<ProducerSourceRootUse>,
    pub(super) chart_restrictions: Vec<DirectedChartRestrictionUse>,
    pub(super) producer_drawing: bool,
    pub(super) work: EvaluationBudget,
}

impl PendingPublicationRow {
    pub(crate) const fn take_shape(&mut self) -> Shape {
        self.shape
            .take()
            .expect("pending geographic row is appended exactly once")
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.shape.as_ref().is_some_and(Shape::is_empty)
    }

    pub(crate) fn replace_empty_shape(&mut self, shape: Shape) -> Result<()> {
        if !shape.is_empty()
            || !self.shape.as_ref().is_some_and(Shape::is_empty)
            || !self.checked.is_empty()
        {
            return Err(GeometryErrorKind::Invalid(
                "only an empty pending row can use a typed-empty replacement".into(),
            )
            .into());
        }
        self.shape = Some(shape);
        Ok(())
    }

    pub(crate) fn precharge_append(&mut self) -> Result<()> {
        let ticks = u64::try_from(self.checked.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        self.work.charge(ticks)
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "boxing pending rows would allocate once per geographic array result"
)]
pub(crate) enum GeometryOutput {
    Shape(Shape),
    Pending(PendingPublicationRow),
}

impl From<Shape> for GeometryOutput {
    fn from(shape: Shape) -> Self {
        Self::Shape(shape)
    }
}

pub(in crate::geometry) struct ArrayPublicationBatch {
    pub(super) storage: std::sync::Arc<GeometryArrayStorage>,
    pub(super) missing: Option<ValidityMask>,
    pub(super) layout: StorageLayout,
    pub(super) logical_rows: Vec<Option<PendingPublicationRow>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum StorageTarget {
    Scalar,
    Array,
    Groups,
}

pub(in crate::geometry) enum ProducerReceipt<'call, 'reference, 'topology> {
    QNative(NativeCandidatePublication<'call, 'reference, 'topology>),
    QSourceOnly(SourceOnlyPublication<'call>),
    ArrayBatch(ArrayPublicationBatch),
    ProducerDrawing(ProducerDrawingReceipt),
}

pub(in crate::geometry) struct PublicationCall<'call, 'reference, 'topology, W: Work> {
    pub(super) receipt: ProducerReceipt<'call, 'reference, 'topology>,
    pub(super) frame: Frame,
    pub(super) work: &'call mut W,
}

impl<'call, 'reference, 'topology, W: Work> PublicationCall<'call, 'reference, 'topology, W> {
    pub(in crate::geometry) const fn new(
        receipt: ProducerReceipt<'call, 'reference, 'topology>,
        frame: Frame,
        work: &'call mut W,
    ) -> Self {
        Self {
            receipt,
            frame,
            work,
        }
    }
}

#[derive(Debug)]
pub(in crate::geometry) enum PublicationRejection {
    Native,
    SourceOnly(Vec<QSourceImage>),
    Batch(Vec<PublicationImage>),
    ProducerDrawing(Vec<ProducerImageId>),
}

#[expect(
    clippy::large_enum_variant,
    reason = "the consuming sink returns its admitted owner without a per-call box allocation"
)]
pub(in crate::geometry) enum PublicationOutcome {
    Pass(super::finish::FinishedGeometry),
    Reject(PublicationRejection),
}
