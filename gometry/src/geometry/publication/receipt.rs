use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::literals::{
    FlatLiteralLayout, LiteralAddress, LiteralRowLayout, ShapeStructure,
};
#[cfg(test)]
use crate::geometry::embedding_completion::literals::{
    LiteralPoint, LiteralPolygon, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::storage::{GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::topology::q_embedding::publication::{
    NativeCandidatePublication, SourceOnlyPublication,
};
use crate::geometry::topology::{EmissionPortId, QSourceImage};
#[cfg(test)]
use crate::geometry::{CoordSeq, CoordinateAxes, Coordinates as _, Point, Polygon, Ring};
use crate::geometry::{GeometryErrorKind, GeometryKind, Shape};
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

pub(in crate::geometry) struct PublicationLiteral<I> {
    pub(in crate::geometry) address: LiteralAddress,
    pub(in crate::geometry) image: I,
    pub(in crate::geometry) record: crate::Point,
}

pub(super) type CheckedPublicationLiteral = PublicationLiteral<PublicationImage>;
pub(in crate::geometry) type ProducerLiteral = PublicationLiteral<ProducerImageId>;

impl PublicationLiteral<ProducerImageId> {
    pub(in crate::geometry) const fn from_point(
        address: LiteralAddress,
        image: ProducerImageId,
        record: crate::Point,
    ) -> Self {
        Self {
            address,
            image,
            record,
        }
    }
}

pub(in crate::geometry) struct ProducerDrawingReceipt {
    pub(super) buffers: FrozenLiteralBuffers<ProducerImageId>,
    pub(super) literals: Vec<ProducerLiteral>,
}

impl ProducerDrawingReceipt {
    pub(in crate::geometry) fn point_selection(shape: Shape) -> Result<Self> {
        if !matches!(
            &shape,
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..)
        ) {
            return Err(GeometryErrorKind::Invalid(
                "point-selection receipt requires a Point, LineString, or Polygon output".into(),
            )
            .into());
        }
        Self::drawing(shape)
    }

    pub(in crate::geometry) fn drawing(shape: Shape) -> Result<Self> {
        let mut layout = FlatLiteralLayout::with_capacity(1);
        let count = crate::geometry::ShapeCountQuery::default().coordinates(&shape)?;
        let mut literals = Vec::with_capacity(count);
        Self::capture_shape(&mut layout, &mut literals, &shape, LiteralAddress {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            output: 0,
        })?;
        Ok(Self::new(
            FrozenLiteralBuffers::flat_shape(shape, layout),
            literals,
        ))
    }

    pub(in crate::geometry) fn capture_shape(
        layout: &mut FlatLiteralLayout<ProducerImageId>,
        literals: &mut Vec<ProducerLiteral>,
        shape: &Shape,
        address: LiteralAddress,
    ) -> Result<()> {
        layout.capture_shape(shape, address, &mut |address, record| {
            let index = literals.len();
            let image = ProducerImageId::new(index);
            literals.push(ProducerLiteral::from_point(address, image, record));
            Ok(image)
        })
    }

    #[cfg(test)]
    pub(super) fn point_selection_layout(
        shape: &Shape,
    ) -> Result<(ShapeLiteralLayout<ProducerImageId>, Vec<ProducerLiteral>)> {
        if !matches!(
            shape,
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..)
        ) {
            return Err(GeometryErrorKind::Invalid(
                "point-selection receipt requires a Point, LineString, or Polygon output".into(),
            )
            .into());
        }
        let mut next_image = 0;
        let mut outputs = (0, 0, 0);
        Self::drawing_layout(shape, 0, None, None, &mut next_image, &mut outputs)
    }

    pub(in crate::geometry) fn point(shape: Shape) -> Result<Self> {
        if !matches!(
            &shape,
            Shape::Point(_) | Shape::Empty(GeometryKind::Point, _)
        ) {
            return Err(GeometryErrorKind::Invalid(
                "nearest-point receipt requires a Point output".into(),
            )
            .into());
        }
        Self::drawing(shape)
    }

    pub(in crate::geometry) fn line(shape: Shape) -> Result<Self> {
        if !matches!(
            &shape,
            Shape::LineString(_) | Shape::Empty(GeometryKind::LineString, _)
        ) {
            return Err(GeometryErrorKind::Invalid(
                "producer line receipt requires a LineString output".into(),
            )
            .into());
        }
        Self::drawing(shape)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn line_layout(
        shape: &Shape,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
        next_image: &mut usize,
    ) -> Result<(ShapeLiteralLayout<ProducerImageId>, Vec<ProducerLiteral>)> {
        let (layout, literals) = match shape {
            Shape::LineString(line) => {
                let (sequence, literals) = Self::sequence_layout(
                    line.as_coords(),
                    logical_row,
                    group_row,
                    value_row,
                    output,
                    next_image,
                )?;
                (
                    ShapeLiteralLayout {
                        structure: None,
                        points: Vec::new(),
                        lines: vec![sequence],
                        polygons: Vec::new(),
                    },
                    literals,
                )
            },
            Shape::Empty(crate::geometry::GeometryKind::LineString, _) => (
                ShapeLiteralLayout {
                    structure: None,
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
        Ok((layout.with_structure(shape), literals))
    }

    #[cfg(test)]
    pub(in crate::geometry) fn drawing_layout(
        shape: &Shape,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        next_image: &mut usize,
        outputs: &mut (u32, u32, u32),
    ) -> Result<(ShapeLiteralLayout<ProducerImageId>, Vec<ProducerLiteral>)> {
        fn next_output(output: &mut u32) -> Result<u32> {
            let current = *output;
            *output = current
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            Ok(current)
        }

        fn visit(
            shape: &Shape,
            logical_row: usize,
            group_row: Option<usize>,
            value_row: Option<usize>,
            next_image: &mut usize,
            outputs: &mut (u32, u32, u32),
            layout: &mut ShapeLiteralLayout<ProducerImageId>,
            literals: &mut Vec<ProducerLiteral>,
        ) -> Result<()> {
            match shape {
                Shape::Point(point) => {
                    let output = next_output(&mut outputs.0)?;
                    let image = ProducerDrawingReceipt::next_image(next_image)?;
                    let (point_layout, literal) = ProducerDrawingReceipt::point_layout(
                        *point,
                        CoordinateAxes::from_point(*point),
                        logical_row,
                        group_row,
                        value_row,
                        output,
                        image,
                    );
                    layout.points.push(point_layout);
                    literals.push(literal);
                },
                Shape::MultiPoint(points) => {
                    for point in points.points() {
                        let output = next_output(&mut outputs.0)?;
                        let image = ProducerDrawingReceipt::next_image(next_image)?;
                        let (point_layout, literal) = ProducerDrawingReceipt::point_layout(
                            point,
                            points.axes(),
                            logical_row,
                            group_row,
                            value_row,
                            output,
                            image,
                        );
                        layout.points.push(point_layout);
                        literals.push(literal);
                    }
                },
                Shape::LineString(line) => {
                    let output = next_output(&mut outputs.1)?;
                    let (sequence, mut row_literals) = ProducerDrawingReceipt::sequence_layout(
                        line.as_coords(),
                        logical_row,
                        group_row,
                        value_row,
                        output,
                        next_image,
                    )?;
                    layout.lines.push(sequence);
                    literals.append(&mut row_literals);
                },
                Shape::MultiLineString(lines) => {
                    for line in lines.as_slice() {
                        let output = next_output(&mut outputs.1)?;
                        let (sequence, mut row_literals) = ProducerDrawingReceipt::sequence_layout(
                            line.as_coords(),
                            logical_row,
                            group_row,
                            value_row,
                            output,
                            next_image,
                        )?;
                        layout.lines.push(sequence);
                        literals.append(&mut row_literals);
                    }
                },
                Shape::Polygon(polygon) => {
                    let output = next_output(&mut outputs.2)?;
                    let (polygon, mut row_literals) = ProducerDrawingReceipt::polygon_layout(
                        polygon,
                        logical_row,
                        group_row,
                        value_row,
                        output,
                        next_image,
                    )?;
                    layout.polygons.push(polygon);
                    literals.append(&mut row_literals);
                },
                Shape::MultiPolygon(polygons) => {
                    for polygon in polygons.as_slice() {
                        let output = next_output(&mut outputs.2)?;
                        let (polygon, mut row_literals) = ProducerDrawingReceipt::polygon_layout(
                            polygon,
                            logical_row,
                            group_row,
                            value_row,
                            output,
                            next_image,
                        )?;
                        layout.polygons.push(polygon);
                        literals.append(&mut row_literals);
                    }
                },
                Shape::GeometryCollection(collection) => {
                    for member in collection {
                        visit(
                            member,
                            logical_row,
                            group_row,
                            value_row,
                            next_image,
                            outputs,
                            layout,
                            literals,
                        )?;
                    }
                },
                Shape::Empty(..) => {},
            }
            Ok(())
        }

        let mut layout = ShapeLiteralLayout {
            structure: None,
            points: Vec::new(),
            lines: Vec::new(),
            polygons: Vec::new(),
        };
        let mut literals = Vec::new();
        visit(
            shape,
            logical_row,
            group_row,
            value_row,
            next_image,
            outputs,
            &mut layout,
            &mut literals,
        )?;
        Ok((layout.with_structure(shape), literals))
    }

    #[cfg(test)]
    fn next_image(next_image: &mut usize) -> Result<ProducerImageId> {
        let index = *next_image;
        *next_image = index
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(ProducerImageId::new(index))
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn point_layout(
        point: Point,
        axes: CoordinateAxes,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
        image: ProducerImageId,
    ) -> (LiteralPoint<ProducerImageId>, ProducerLiteral) {
        (
            LiteralPoint {
                output,
                axes,
                image,
            },
            ProducerLiteral::from_point(
                LiteralAddress {
                    operation: 0,
                    logical_row,
                    group_row,
                    value_row,
                    output,
                },
                image,
                point,
            ),
        )
    }

    #[cfg(test)]
    fn sequence_layout(
        coordinates: &CoordSeq,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
        next_image: &mut usize,
    ) -> Result<(LiteralSequence<ProducerImageId>, Vec<ProducerLiteral>)> {
        let mut images = Vec::with_capacity(coordinates.len());
        let mut literals = Vec::with_capacity(coordinates.len());
        for point in coordinates.iter_coords() {
            let image = Self::next_image(next_image)?;
            images.push(image);
            literals.push(ProducerLiteral::from_point(
                LiteralAddress {
                    operation: 0,
                    logical_row,
                    group_row,
                    value_row,
                    output,
                },
                image,
                point,
            ));
        }
        Ok((
            LiteralSequence {
                output,
                axes: coordinates.axes(),
                images,
            },
            literals,
        ))
    }

    #[cfg(test)]
    fn polygon_layout(
        polygon: &Polygon,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
        next_image: &mut usize,
    ) -> Result<(LiteralPolygon<ProducerImageId>, Vec<ProducerLiteral>)> {
        let (shell, mut literals) = Self::ring_layout(
            &polygon.shell,
            logical_row,
            group_row,
            value_row,
            output,
            next_image,
        )?;
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes.iter() {
            let (layout, mut ring_literals) =
                Self::ring_layout(hole, logical_row, group_row, value_row, output, next_image)?;
            holes.push(layout);
            literals.append(&mut ring_literals);
        }
        Ok((
            LiteralPolygon {
                output,
                axes: polygon.axes(),
                shell,
                holes,
            },
            literals,
        ))
    }

    #[cfg(test)]
    fn ring_layout(
        ring: &Ring,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
        next_image: &mut usize,
    ) -> Result<(LiteralSequence<ProducerImageId>, Vec<ProducerLiteral>)> {
        Self::sequence_layout(
            ring.coords(),
            logical_row,
            group_row,
            value_row,
            output,
            next_image,
        )
    }

    pub(in crate::geometry) const fn new(
        buffers: FrozenLiteralBuffers<ProducerImageId>,
        literals: Vec<ProducerLiteral>,
    ) -> Self {
        Self { buffers, literals }
    }
}

pub(crate) struct PendingPublicationRow {
    pub(super) shape: Shape,
    pub(super) logical_row: usize,
    pub(super) admission: PendingAdmission,
}

#[expect(
    clippy::large_enum_variant,
    reason = "pending admission is moved directly into batch storage; boxing would allocate per Q row"
)]
pub(super) enum PendingAdmission {
    Drawing,
    Checked {
        layout: LiteralRowLayout<PublicationImage>,
        records: Vec<CheckedPublicationLiteral>,
        work: EvaluationBudget,
        #[cfg(test)]
        producer_drawing: bool,
    },
}

impl PendingPublicationRow {
    pub(super) fn drawing(
        shape: Shape,
        logical_row: usize,
        operation: &'static str,
    ) -> Result<Self> {
        let count = crate::geometry::ShapeCountQuery::default().coordinates(&shape)?;
        let mut work = EvaluationBudget::new(operation, count)?;
        work.charge(
            u64::try_from(count)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        )?;
        Ok(Self {
            shape,
            logical_row,
            admission: PendingAdmission::Drawing,
        })
    }

    #[cfg(test)]
    pub(super) fn producer_drawing(
        shape: Shape,
        layout: ShapeLiteralLayout<ProducerImageId>,
        mut producer_literals: Vec<ProducerLiteral>,
        logical_row: usize,
        operation: &'static str,
    ) -> Result<Self> {
        for literal in &mut producer_literals {
            literal.address = LiteralAddress {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                output: literal.address.output,
            };
        }
        let layout = super::finish::map_shape_layout(layout, &PublicationImage::ProducerDrawn);
        let work = EvaluationBudget::new(operation, producer_literals.len())?;
        let records = producer_literals
            .into_iter()
            .map(|literal| CheckedPublicationLiteral {
                address: literal.address,
                image: PublicationImage::ProducerDrawn(literal.image),
                record: literal.record,
            })
            .collect();
        Ok(Self {
            shape,
            logical_row,
            admission: PendingAdmission::Checked {
                layout: LiteralRowLayout {
                    operation: 0,
                    logical_row,
                    group_row: None,
                    value_row: None,
                    layout,
                },
                records,
                producer_drawing: true,
                work,
            },
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.shape.is_empty()
    }

    pub(crate) fn replace_empty_shape(&mut self, shape: Shape) -> Result<()> {
        let has_records = matches!(&self.admission,
            PendingAdmission::Checked { records, .. } if !records.is_empty());
        if !shape.is_empty() || !self.shape.is_empty() || has_records {
            return Err(GeometryErrorKind::Invalid(
                "only an empty pending row can use a typed-empty replacement".into(),
            )
            .into());
        }
        self.shape = shape;
        if let PendingAdmission::Checked { layout, .. } = &mut self.admission {
            layout.layout.structure = Some(ShapeStructure::capture(&self.shape));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) const fn is_producer_drawing(&self) -> bool {
        matches!(
            &self.admission,
            PendingAdmission::Drawing
                | PendingAdmission::Checked {
                    producer_drawing: true,
                    ..
                }
        )
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
    pub(super) expected: FlatLiteralLayout<PublicationImage>,
    pub(super) records: Vec<CheckedPublicationLiteral>,
    pub(super) accounts: Vec<(usize, EvaluationBudget)>,
    pub(super) drawing_only: bool,
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
