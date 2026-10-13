use std::sync::Arc;

use super::super::lineage::{
    EventId, LineageCatalog, LineagePolygon, LineageSequence, MemberId, ShapeLineage,
};
use super::capture::orient_shape_lineage;
use super::restrictions;
use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::publication::{
    DirectedChartRestrictionUse, FinishedGeometry, ProducerDrawingReceipt, ProducerImageId,
    ProducerLiteral, ProducerSourceRootUse, StorageTarget, finish_producer_drawing,
};
use crate::geometry::storage::StorageLayout;
use crate::geometry::{GeometryErrorKind, MultiMember, Shape};
use crate::work::Unmetered;

pub(crate) struct SplitDrawing {
    pub(super) shape: Shape,
    pub(super) frame: GeographicFrame,
    pub(super) longitude_scale: f64,
    pub(super) latitude_scale: f64,
    pub(super) lineage: LineageCatalog,
    pub(super) shape_lineage: ShapeLineage,
}

pub(super) struct SplitRowDrawing {
    pub(super) shape: Shape,
    pub(super) layout: LiteralRowLayout<ProducerImageId>,
    pub(super) literals: Vec<ProducerLiteral>,
    pub(super) source_root_uses: Vec<ProducerSourceRootUse>,
    pub(super) chart_restrictions: Vec<DirectedChartRestrictionUse>,
    pub(super) next_image: usize,
    pub(super) next_node: u32,
}

impl SplitDrawing {
    pub(crate) fn orient_geojson(mut self) -> Result<Self> {
        if matches!(
            &self.shape,
            Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
        ) {
            orient_shape_lineage(&self.shape, &mut self.shape_lineage, &mut self.lineage)?;
            self.shape = self.shape.orient_polygons(false)?;
        }
        Ok(self)
    }

    pub(crate) fn finish_scalar(self, frame: Frame) -> Result<FinishedGeometry> {
        let row = self.into_row(0, 0, 0)?;
        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        finish_producer_drawing(
            ProducerDrawingReceipt::new(
                buffers,
                row.literals,
                row.source_root_uses,
                row.chart_restrictions,
            ),
            frame,
            StorageTarget::Scalar,
        )
    }

    pub(super) fn into_row(
        self,
        logical_row: usize,
        image_start: usize,
        node_start: u32,
    ) -> Result<SplitRowDrawing> {
        let mut next_image = image_start;
        let mut outputs = (0, 0, 0);
        let (shape_layout, literals) = ProducerDrawingReceipt::drawing_layout(
            &self.shape,
            logical_row,
            None,
            None,
            &mut next_image,
            &mut outputs,
        )?;
        let mut source_uses = SourceRootUseBuilder::new(logical_row, &self.lineage);
        source_uses.visit(&self.shape, &self.shape_lineage, &shape_layout)?;
        let source_root_uses = source_uses.finish(&shape_layout)?;
        let (chart_restrictions, next_node) = restrictions::chart_restrictions(
            &self.shape,
            &self.shape_lineage,
            &self.lineage,
            self.frame,
            self.longitude_scale,
            self.latitude_scale,
            node_start,
        )?;
        Ok(SplitRowDrawing {
            shape: self.shape,
            layout: LiteralRowLayout {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                layout: shape_layout,
            },
            literals,
            source_root_uses,
            chart_restrictions,
            next_image,
            next_node,
        })
    }
}

pub(crate) struct SplitArrayPublisher {
    writer: StreamingShapes,
    row_map: Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    next_image: usize,
    next_node: u32,
}

impl SplitArrayPublisher {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            row_map: Vec::with_capacity(rows),
            literals: Vec::new(),
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
            next_image: 0,
            next_node: 0,
        }
    }

    pub(crate) fn push_missing(&mut self) -> Result<()> {
        self.writer.try_push_missing()?;
        self.row_map.push(None);
        Ok(())
    }

    pub(crate) fn push(&mut self, logical_row: usize, drawing: SplitDrawing) -> Result<()> {
        let row = drawing.into_row(logical_row, self.next_image, self.next_node)?;
        self.writer.try_push(row.shape)?;
        self.row_map.push(Some(row.layout));
        self.literals.extend(row.literals);
        self.source_root_uses.extend(row.source_root_uses);
        self.chart_restrictions.extend(row.chart_restrictions);
        self.next_image = row.next_image;
        self.next_node = row.next_node;
        Ok(())
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<FinishedGeometry> {
        let (storage, missing, layout) = self.writer.finish_parts()?;
        let row_map = match layout {
            StorageLayout::PresentOnly => self.row_map.into_iter().flatten().map(Some).collect(),
            StorageLayout::LogicalRows => self.row_map,
        };
        let mut work = Unmetered;
        let buffers =
            FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
        finish_producer_drawing(
            ProducerDrawingReceipt::new(
                buffers,
                self.literals,
                self.source_root_uses,
                self.chart_restrictions,
            ),
            frame,
            StorageTarget::Array,
        )
    }
}

struct SourceRootUseBuilder<'a> {
    logical_row: usize,
    lineage: &'a LineageCatalog,
    source_root_uses: Vec<ProducerSourceRootUse>,
    point: usize,
    line: usize,
    polygon: usize,
}

impl<'a> SourceRootUseBuilder<'a> {
    const fn new(logical_row: usize, lineage: &'a LineageCatalog) -> Self {
        Self {
            logical_row,
            lineage,
            source_root_uses: Vec::new(),
            point: 0,
            line: 0,
            polygon: 0,
        }
    }

    fn next_layout<'b, T>(values: &'b [T], index: &mut usize) -> Result<&'b T> {
        let current = *index;
        *index = current.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        values.get(current).ok_or_else(|| {
            GeometryErrorKind::Invalid("split literal layout and lineage disagree".into()).into()
        })
    }

    fn source_uses(
        &mut self,
        output: u32,
        image: ProducerImageId,
        member: MemberId,
        events: &[EventId],
    ) -> Result<()> {
        let address = LiteralAddress {
            operation: 0,
            logical_row: self.logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        let mut work = Unmetered;
        for &event in events {
            if self.lineage.event_member(event) != member {
                return Err(GeometryErrorKind::Invalid(
                    "split event belongs to a different source member".into(),
                )
                .into());
            }
            for range in self.lineage.event_uses(event) {
                self.source_root_uses.push(ProducerSourceRootUse {
                    address,
                    image,
                    range: range.try_clone_with_work(&mut work)?,
                });
            }
        }
        Ok(())
    }

    fn sequence(
        &mut self,
        sequence: &LiteralSequence<ProducerImageId>,
        lineage: &LineageSequence,
    ) -> Result<()> {
        if sequence.images.len() != lineage.vertices.len() {
            return Err(GeometryErrorKind::Invalid(
                "split sequence layout and vertex lineage disagree".into(),
            )
            .into());
        }
        for (&image, events) in sequence.images.iter().zip(&lineage.vertices) {
            self.source_uses(sequence.output, image, lineage.member, events)?;
        }
        Ok(())
    }

    fn polygon(
        &mut self,
        polygon: &LiteralPolygon<ProducerImageId>,
        lineage: &LineagePolygon,
    ) -> Result<()> {
        if polygon.holes.len() != lineage.holes.len() {
            return Err(GeometryErrorKind::Invalid(
                "split polygon layout and hole lineage disagree".into(),
            )
            .into());
        }
        self.sequence(&polygon.shell, &lineage.shell)?;
        for (hole, lineage) in polygon.holes.iter().zip(&lineage.holes) {
            self.sequence(hole, lineage)?;
        }
        Ok(())
    }

    fn visit(
        &mut self,
        shape: &Shape,
        lineage: &ShapeLineage,
        layout: &ShapeLiteralLayout<ProducerImageId>,
    ) -> Result<()> {
        match (shape, lineage) {
            (Shape::Point(_), ShapeLineage::Point { member, events }) => {
                let point = Self::next_layout(&layout.points, &mut self.point)?;
                self.source_uses(point.output, point.image, *member, events)?;
            },
            (
                Shape::MultiPoint(points),
                ShapeLineage::MultiPoint {
                    member,
                    events: point_events,
                },
            ) => {
                if points.len() != point_events.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian multipoint members and lineage disagree".into(),
                    )
                    .into());
                }
                for events in point_events {
                    let point = Self::next_layout(&layout.points, &mut self.point)?;
                    self.source_uses(point.output, point.image, *member, events)?;
                }
            },
            (Shape::LineString(_), ShapeLineage::LineString(lineage)) => {
                let line = Self::next_layout(&layout.lines, &mut self.line)?;
                self.sequence(line, lineage)?;
            },
            (
                Shape::MultiLineString(lines),
                ShapeLineage::MultiLineString {
                    members: lineages, ..
                },
            ) => {
                if lines.member_count() != lineages.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian line members and lineage disagree".into(),
                    )
                    .into());
                }
                for (line, lineage) in lines.members_with_values().zip(lineages) {
                    match (line, lineage) {
                        (MultiMember::Empty, None) => {},
                        (MultiMember::Value(_), Some(lineage)) => {
                            let line = Self::next_layout(&layout.lines, &mut self.line)?;
                            self.sequence(line, lineage)?;
                        },
                        _ => {
                            return Err(GeometryErrorKind::Invalid(
                                "antimeridian line members and lineage disagree".into(),
                            )
                            .into());
                        },
                    }
                }
            },
            (Shape::Polygon(_), ShapeLineage::Polygon(lineage)) => {
                let polygon = Self::next_layout(&layout.polygons, &mut self.polygon)?;
                self.polygon(polygon, lineage)?;
            },
            (
                Shape::MultiPolygon(polygons),
                ShapeLineage::MultiPolygon {
                    members: lineages, ..
                },
            ) => {
                if polygons.member_count() != lineages.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian polygon members and lineage disagree".into(),
                    )
                    .into());
                }
                for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                    match (polygon, lineage) {
                        (MultiMember::Empty, None) => {},
                        (MultiMember::Value(_), Some(lineage)) => {
                            let polygon = Self::next_layout(&layout.polygons, &mut self.polygon)?;
                            self.polygon(polygon, lineage)?;
                        },
                        _ => {
                            return Err(GeometryErrorKind::Invalid(
                                "antimeridian polygon members and lineage disagree".into(),
                            )
                            .into());
                        },
                    }
                }
            },
            (Shape::GeometryCollection(collection), ShapeLineage::GeometryCollection(lineages)) => {
                if collection.len() != lineages.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian collection members and lineage disagree".into(),
                    )
                    .into());
                }
                for (member, lineage) in collection.iter().zip(lineages) {
                    let lineage = lineage.as_ref().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "antimeridian collection member has no lineage".into(),
                        )
                    })?;
                    self.visit(member, lineage, layout)?;
                }
            },
            (Shape::Empty(..), ShapeLineage::Empty) => {},
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian shape and member lineage disagree".into(),
                )
                .into());
            },
        }
        Ok(())
    }

    fn finish(
        self,
        layout: &ShapeLiteralLayout<ProducerImageId>,
    ) -> Result<Vec<ProducerSourceRootUse>> {
        if self.point != layout.points.len()
            || self.line != layout.lines.len()
            || self.polygon != layout.polygons.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "split literal layout contains an unmatched output".into(),
            )
            .into());
        }
        Ok(self.source_root_uses)
    }
}
