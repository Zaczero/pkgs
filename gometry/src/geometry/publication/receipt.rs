use super::DirectedChartRestrictionUse;
use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowLayout,
    LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_search::EvaluationBudget;
use crate::geometry::ordinates::RootRange;
use crate::geometry::storage::{GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::q_embedding::publication::{
    NativeCandidatePublication, SourceOnlyPublication,
};
use crate::geometry::topology::{EmissionPortId, ExactChartPoint, QSourceImage};
use crate::geometry::{Coordinates as _, GeometryErrorKind, Shape};
use crate::work::Work;

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
    pub(super) retain_literals_for_republication: bool,
}

impl ProducerDrawingReceipt {
    const fn drawing(
        buffers: FrozenLiteralBuffers<ProducerImageId>,
        literals: Vec<ProducerLiteral>,
        retain_literals_for_republication: bool,
    ) -> Self {
        if retain_literals_for_republication {
            Self::new_for_republication(buffers, literals, Vec::new(), Vec::new())
        } else {
            Self::new(buffers, literals, Vec::new(), Vec::new())
        }
    }

    pub(in crate::geometry) fn point_selection(
        shape: Shape,
        retain_literals_for_republication: bool,
    ) -> Result<Self> {
        match shape {
            Shape::Point(point) => {
                Self::point_drawing(Shape::Point(point), retain_literals_for_republication)
            },
            Shape::LineString(line) => {
                Self::line_drawing(Shape::LineString(line), retain_literals_for_republication)
            },
            empty @ Shape::Empty(..) => Ok(Self::drawing(
                FrozenLiteralBuffers::shapes(empty, ShapeLiteralLayout {
                    points: Vec::new(),
                    lines: Vec::new(),
                    polygons: Vec::new(),
                }),
                Vec::new(),
                retain_literals_for_republication,
            )),
            Shape::Polygon(polygon) => {
                let mut next_image = 0_usize;
                let (shell, mut literals) = Self::ring_layout(&polygon.shell, &mut next_image)?;
                let mut holes = Vec::with_capacity(polygon.holes.len());
                for hole in polygon.holes.iter() {
                    let (layout, mut ring_literals) = Self::ring_layout(hole, &mut next_image)?;
                    holes.push(layout);
                    literals.append(&mut ring_literals);
                }
                let layout = ShapeLiteralLayout {
                    points: Vec::new(),
                    lines: Vec::new(),
                    polygons: vec![LiteralPolygon {
                        output: 0,
                        axes: polygon.axes(),
                        shell,
                        holes,
                    }],
                };
                Ok(Self::drawing(
                    FrozenLiteralBuffers::shapes(Shape::Polygon(polygon), layout),
                    literals,
                    retain_literals_for_republication,
                ))
            },
            Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_) => Err(GeometryErrorKind::Invalid(
                "point-selection receipt requires a Point, LineString, or Polygon output".into(),
            )
            .into()),
        }
    }

    pub(in crate::geometry) fn point(shape: Shape) -> Result<Self> {
        Self::point_drawing(shape, false)
    }

    fn point_drawing(shape: Shape, retain_literals_for_republication: bool) -> Result<Self> {
        let image = ProducerImageId::new(0);
        let address = LiteralAddress {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            output: 0,
        };
        let (points, literals) = match &shape {
            Shape::Point(point) => (
                vec![LiteralPoint {
                    output: 0,
                    axes: crate::geometry::CoordinateAxes::from_point(*point),
                    image,
                }],
                vec![ProducerLiteral::from_point(address, image, *point)],
            ),
            Shape::Empty(crate::geometry::GeometryKind::Point, _) => (Vec::new(), Vec::new()),
            Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(
                crate::geometry::GeometryKind::MultiPoint
                | crate::geometry::GeometryKind::LineString
                | crate::geometry::GeometryKind::MultiLineString
                | crate::geometry::GeometryKind::Polygon
                | crate::geometry::GeometryKind::MultiPolygon
                | crate::geometry::GeometryKind::GeometryCollection,
                _,
            ) => {
                return Err(GeometryErrorKind::Invalid(
                    "nearest-point receipt requires a Point output".into(),
                )
                .into());
            },
        };
        Ok(Self::drawing(
            FrozenLiteralBuffers::shapes(shape, ShapeLiteralLayout {
                points,
                lines: Vec::new(),
                polygons: Vec::new(),
            }),
            literals,
            retain_literals_for_republication,
        ))
    }

    pub(in crate::geometry) fn line(shape: Shape) -> Result<Self> {
        Self::line_drawing(shape, false)
    }

    pub(in crate::geometry) fn line_for_republication(shape: Shape) -> Result<Self> {
        Self::line_drawing(shape, true)
    }

    fn line_drawing(shape: Shape, retain_literals_for_republication: bool) -> Result<Self> {
        let mut next_image = 0;
        let (layout, literals) = Self::line_layout(&shape, 0, None, None, &mut next_image)?;
        Ok(Self::drawing(
            FrozenLiteralBuffers::shapes(shape, layout),
            literals,
            retain_literals_for_republication,
        ))
    }

    pub(in crate::geometry) fn line_layout(
        shape: &Shape,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        next_image: &mut usize,
    ) -> Result<(ShapeLiteralLayout<ProducerImageId>, Vec<ProducerLiteral>)> {
        let (layout, literals) = match shape {
            Shape::LineString(line) => {
                let mut images = Vec::with_capacity(line.len());
                let mut literals = Vec::with_capacity(line.len());
                for point in line.iter_coords() {
                    let index = *next_image;
                    *next_image = index
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let image = ProducerImageId::new(index);
                    images.push(image);
                    literals.push(ProducerLiteral::from_point(
                        LiteralAddress {
                            operation: 0,
                            logical_row,
                            group_row,
                            value_row,
                            output: 0,
                        },
                        image,
                        point,
                    ));
                }
                (
                    ShapeLiteralLayout {
                        points: Vec::new(),
                        lines: vec![LiteralSequence {
                            output: 0,
                            axes: line.as_coords().axes(),
                            images,
                        }],
                        polygons: Vec::new(),
                    },
                    literals,
                )
            },
            Shape::Empty(crate::geometry::GeometryKind::LineString, _) => (
                ShapeLiteralLayout {
                    points: Vec::new(),
                    lines: Vec::new(),
                    polygons: Vec::new(),
                },
                Vec::new(),
            ),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(
                crate::geometry::GeometryKind::Point
                | crate::geometry::GeometryKind::MultiPoint
                | crate::geometry::GeometryKind::MultiLineString
                | crate::geometry::GeometryKind::Polygon
                | crate::geometry::GeometryKind::MultiPolygon
                | crate::geometry::GeometryKind::GeometryCollection,
                _,
            ) => {
                return Err(GeometryErrorKind::Invalid(
                    "producer line receipt requires a LineString output".into(),
                )
                .into());
            },
        };
        Ok((layout, literals))
    }

    fn ring_layout(
        ring: &crate::geometry::Ring,
        next_image: &mut usize,
    ) -> Result<(LiteralSequence<ProducerImageId>, Vec<ProducerLiteral>)> {
        let mut images = Vec::with_capacity(ring.coords().len());
        let mut literals = Vec::with_capacity(ring.coords().len());
        for point in ring.iter_coords() {
            let index = *next_image;
            *next_image = index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let image = ProducerImageId::new(index);
            images.push(image);
            literals.push(ProducerLiteral::from_point(
                LiteralAddress {
                    operation: 0,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                image,
                point,
            ));
        }
        Ok((
            LiteralSequence {
                output: 0,
                axes: ring.coords().axes(),
                images,
            },
            literals,
        ))
    }

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
            retain_literals_for_republication: false,
        }
    }

    pub(in crate::geometry) const fn new_for_republication(
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
            retain_literals_for_republication: true,
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
    pub(super) producer_literals: Vec<ProducerLiteral>,
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
        let literal_count = self
            .checked
            .len()
            .checked_add(self.producer_literals.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let ticks = u64::try_from(literal_count)
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
