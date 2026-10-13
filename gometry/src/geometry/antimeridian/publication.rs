use std::sync::Arc;

use super::lineage::{
    EventId, LineageCatalog, LineagePolygon, LineageSequence, MemberId, ShapeLineage,
};
use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowLayout,
    LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, FinishedGeometry,
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt,
    ProducerSourceRootUse, PublicationCall, PublicationOutcome, PublicationRejection,
    StorageTarget, SyntheticChartVertex, finish_geographic,
};
use crate::geometry::storage::StorageLayout;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    CoordinateAxes, Coordinates as _, GeometryErrorKind, LineSeq, MultiMember, Point, Polygon,
    Ring, Shape,
};
use crate::work::Unmetered;

pub(super) struct SplitCapture {
    lineage: LineageCatalog,
}

impl SplitCapture {
    pub(super) fn enabled() -> Self {
        Self {
            lineage: LineageCatalog::default(),
        }
    }

    pub(super) fn mark_identity(&mut self) {
        self.lineage = LineageCatalog::default();
    }

    pub(super) fn next_member(&mut self) -> Result<MemberId> {
        self.lineage.member()
    }

    pub(super) fn point_root(&mut self, member: MemberId, point: Point) -> Result<RootId> {
        self.lineage.point_root(member, point)
    }

    pub(super) fn edge_root(
        &mut self,
        member: MemberId,
        points: [Point; 2],
        frame: GeographicFrame,
    ) -> Result<RootId> {
        let longitude = frame
            .longitude()
            .chart_connection(points[0].x, points[1].x)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?
            .retained();
        self.lineage.edge_root(member, points, longitude)
    }

    pub(super) fn identity_lineage(&mut self, shape: &Shape) -> Result<ShapeLineage> {
        self.lineage.identity_shape(shape)
    }

    pub(super) fn lineage_mut(&mut self) -> &mut LineageCatalog {
        &mut self.lineage
    }

    pub(super) fn source_edge_use(
        &mut self,
        member: MemberId,
        root: RootId,
        source_start: &ExactRational,
        source_end: &ExactRational,
    ) -> Result<super::lineage::EdgeUseId> {
        let mut work = Unmetered;
        let range = RootRange::from_mapped_with_work(
            root,
            source_start.try_clone_with_work(&mut work)?,
            source_end.try_clone_with_work(&mut work)?,
            ExactRational::zero(),
            ExactRational::one(),
            &mut work,
        )?;
        self.lineage.push_edge_use(member, vec![range], None)
    }

    pub(super) fn record_root_point(
        &mut self,
        root: RootId,
        member: MemberId,
        source_parameter: &ExactRational,
    ) -> Result<EventId> {
        let mut work = Unmetered;
        let range = RootRange::point_with_work(
            root,
            source_parameter.try_clone_with_work(&mut work)?,
            ExactRational::zero(),
            &mut work,
        )?;
        self.lineage.event(member, vec![range])
    }

    pub(super) fn into_drawing(
        self,
        shape: Shape,
        frame: GeographicFrame,
        longitude_scale: f64,
        latitude_scale: f64,
        lineage: super::lineage::ShapeLineage,
    ) -> SplitDrawing {
        SplitDrawing {
            shape,
            frame,
            longitude_scale,
            latitude_scale,
            lineage: self.lineage,
            shape_lineage: lineage,
        }
    }
}

pub(crate) struct SplitDrawing {
    shape: Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    lineage: LineageCatalog,
    shape_lineage: super::lineage::ShapeLineage,
}

struct SplitRowDrawing {
    shape: Shape,
    layout: LiteralRowLayout<ProducerImageId>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    next_image: usize,
    next_node: u32,
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

    fn into_row(
        self,
        logical_row: usize,
        image_start: usize,
        node_start: u32,
    ) -> Result<SplitRowDrawing> {
        let mut layout = ShapeLayoutBuilder::new(logical_row, image_start, &self.lineage);
        layout.visit(&self.shape, &self.shape_lineage)?;
        let (chart_restrictions, next_node) = chart_restrictions(
            &self.shape,
            &self.shape_lineage,
            &self.lineage,
            self.frame,
            self.longitude_scale,
            self.latitude_scale,
            node_start,
        )?;
        let next_image = layout.next_image;
        let (shape_layout, literals, source_root_uses) = layout.finish();
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

fn orient_lineage_sequence(
    ring: &Ring,
    lineage: &LineageSequence,
    clockwise: bool,
    catalog: &mut LineageCatalog,
) -> Result<LineageSequence> {
    if ring.coords().len() != lineage.vertices.len() {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian ring coordinates and vertex lineage disagree".into(),
        )
        .into());
    }
    if crate::geometry::ring_winding(ring).reverse_for_clockwise(clockwise) {
        catalog.reverse_sequence(lineage)
    } else {
        Ok(lineage.clone())
    }
}

fn orient_lineage_polygon(
    polygon: &Polygon,
    lineage: &LineagePolygon,
    catalog: &mut LineageCatalog,
) -> Result<LineagePolygon> {
    if polygon.holes.len() != lineage.holes.len() {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian polygon holes and lineage disagree".into(),
        )
        .into());
    }
    let shell = orient_lineage_sequence(&polygon.shell, &lineage.shell, false, catalog)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for (ring, ring_lineage) in polygon.holes.iter().zip(&lineage.holes) {
        holes.push(orient_lineage_sequence(ring, ring_lineage, true, catalog)?);
    }
    Ok(LineagePolygon {
        member: lineage.member,
        shell,
        holes,
    })
}

fn orient_shape_lineage(
    shape: &Shape,
    lineage: &mut ShapeLineage,
    catalog: &mut LineageCatalog,
) -> Result<()> {
    match (shape, lineage) {
        (Shape::Polygon(polygon), ShapeLineage::Polygon(polygon_lineage)) => {
            *polygon_lineage = orient_lineage_polygon(polygon, polygon_lineage, catalog)?;
        },
        (
            Shape::MultiPolygon(polygons),
            ShapeLineage::MultiPolygon {
                members: lineages, ..
            },
        ) => {
            if polygons.member_count() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian multipolygon members and lineage disagree".into(),
                )
                .into());
            }
            for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                match (polygon, lineage) {
                    (MultiMember::Empty, None) => {},
                    (MultiMember::Value(polygon), Some(lineage)) => {
                        *lineage = orient_lineage_polygon(polygon, lineage, catalog)?;
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian multipolygon members and lineage disagree".into(),
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
                let lineage = lineage.as_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "antimeridian collection member has no lineage".into(),
                    )
                })?;
                orient_shape_lineage(member, lineage, catalog)?;
            }
        },
        (Shape::Point(_), ShapeLineage::Point { .. })
        | (Shape::MultiPoint(_), ShapeLineage::MultiPoint { .. })
        | (Shape::LineString(_), ShapeLineage::LineString(_))
        | (Shape::MultiLineString(_), ShapeLineage::MultiLineString { .. })
        | (Shape::Empty(..), ShapeLineage::Empty) => {},
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian shape and lineage disagree during orientation".into(),
            )
            .into());
        },
    }
    Ok(())
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

fn finish_producer_drawing(
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
                    "final antimeridian storage changed {} checked output records",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "antimeridian finisher returned a different producer rejection family".into(),
        )
        .into()),
    }
}

fn chart_restrictions(
    shape: &Shape,
    lineage: &ShapeLineage,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    node_start: u32,
) -> Result<(Vec<DirectedChartRestrictionUse>, u32)> {
    let mut restrictions = Vec::new();
    let mut next_node_id = node_start;
    collect_restrictions(
        shape,
        lineage,
        catalog,
        frame,
        longitude_scale,
        latitude_scale,
        &mut next_node_id,
        &mut restrictions,
    )?;
    Ok((restrictions, next_node_id))
}

struct ShapeLayoutBuilder<'a> {
    logical_row: usize,
    next_image: usize,
    point_output: u32,
    line_output: u32,
    polygon_output: u32,
    lineage: &'a LineageCatalog,
    points: Vec<LiteralPoint<ProducerImageId>>,
    lines: Vec<LiteralSequence<ProducerImageId>>,
    polygons: Vec<LiteralPolygon<ProducerImageId>>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
}

impl<'a> ShapeLayoutBuilder<'a> {
    const fn new(logical_row: usize, image_start: usize, lineage: &'a LineageCatalog) -> Self {
        Self {
            logical_row,
            next_image: image_start,
            point_output: 0,
            line_output: 0,
            polygon_output: 0,
            lineage,
            points: Vec::new(),
            lines: Vec::new(),
            polygons: Vec::new(),
            literals: Vec::new(),
            source_root_uses: Vec::new(),
        }
    }

    fn image(
        &mut self,
        point: Point,
        output: u32,
        member: MemberId,
        events: &[EventId],
    ) -> Result<ProducerImageId> {
        let image = ProducerImageId::new(self.next_image);
        self.next_image = self
            .next_image
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let address = LiteralAddress {
            operation: 0,
            logical_row: self.logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        self.literals
            .push(ProducerLiteral::from_point(address, image, point));
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
        Ok(image)
    }

    fn visit(&mut self, shape: &Shape, lineage: &ShapeLineage) -> Result<()> {
        match (shape, lineage) {
            (Shape::Point(point), ShapeLineage::Point { member, events }) => {
                let output = self.point_output;
                self.point_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let image = self.image(*point, output, *member, events)?;
                self.points.push(LiteralPoint {
                    output,
                    axes: CoordinateAxes::from_point(*point),
                    image,
                });
            },
            (
                Shape::MultiPoint(points),
                ShapeLineage::MultiPoint {
                    member,
                    events: point_events,
                },
            ) => {
                for (point, events) in points.points().zip(point_events) {
                    let output = self.point_output;
                    self.point_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    let image = self.image(point, output, *member, events)?;
                    self.points.push(LiteralPoint {
                        output,
                        axes: points.axes(),
                        image,
                    });
                }
            },
            (Shape::LineString(line), ShapeLineage::LineString(lineage)) => {
                let output = self.line_output;
                self.line_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let sequence = self.sequence(line, output, lineage)?;
                self.lines.push(sequence);
            },
            (
                Shape::MultiLineString(lines),
                ShapeLineage::MultiLineString {
                    members: lineages, ..
                },
            ) => {
                for (line, lineage) in lines.members_with_values().zip(lineages) {
                    match (line, lineage) {
                        (MultiMember::Empty, None) => {},
                        (MultiMember::Value(line), Some(lineage)) => {
                            let output = self.line_output;
                            self.line_output =
                                output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                            let sequence = self.sequence(line, output, lineage)?;
                            self.lines.push(sequence);
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
            (Shape::Polygon(polygon), ShapeLineage::Polygon(lineage)) => {
                let output = self.polygon_output;
                self.polygon_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let polygon = self.polygon(polygon, output, lineage)?;
                self.polygons.push(polygon);
            },
            (
                Shape::MultiPolygon(polygons),
                ShapeLineage::MultiPolygon {
                    members: lineages, ..
                },
            ) => {
                for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                    match (polygon, lineage) {
                        (MultiMember::Empty, None) => {},
                        (MultiMember::Value(polygon), Some(lineage)) => {
                            let output = self.polygon_output;
                            self.polygon_output =
                                output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                            let polygon = self.polygon(polygon, output, lineage)?;
                            self.polygons.push(polygon);
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
                    self.visit(member, lineage)?;
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

    fn sequence(
        &mut self,
        line: &LineSeq,
        output: u32,
        lineage: &LineageSequence,
    ) -> Result<LiteralSequence<ProducerImageId>> {
        let axes = line.as_coords().axes();
        let mut images = Vec::with_capacity(line.len());
        for (point, events) in line.iter_coords().zip(&lineage.vertices) {
            images.push(self.image(point, output, lineage.member, events)?);
        }
        Ok(LiteralSequence {
            output,
            axes,
            images,
        })
    }

    fn polygon(
        &mut self,
        polygon: &Polygon,
        output: u32,
        lineage: &LineagePolygon,
    ) -> Result<LiteralPolygon<ProducerImageId>> {
        let axes = polygon.axes();
        let shell = self.ring(&polygon.shell, output, &lineage.shell)?;
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for (hole, hole_lineage) in polygon.holes.iter().zip(&lineage.holes) {
            holes.push(self.ring(hole, output, hole_lineage)?);
        }
        Ok(LiteralPolygon {
            output,
            axes,
            shell,
            holes,
        })
    }

    fn ring(
        &mut self,
        ring: &Ring,
        output: u32,
        lineage: &LineageSequence,
    ) -> Result<LiteralSequence<ProducerImageId>> {
        let axes = ring.coords().axes();
        let mut images = Vec::with_capacity(ring.coords().len());
        for (point, events) in ring.iter_coords().zip(&lineage.vertices) {
            images.push(self.image(point, output, lineage.member, events)?);
        }
        Ok(LiteralSequence {
            output,
            axes,
            images,
        })
    }

    fn finish(
        self,
    ) -> (
        ShapeLiteralLayout<ProducerImageId>,
        Vec<ProducerLiteral>,
        Vec<ProducerSourceRootUse>,
    ) {
        (
            ShapeLiteralLayout {
                points: self.points,
                lines: self.lines,
                polygons: self.polygons,
            },
            self.literals,
            self.source_root_uses,
        )
    }
}

fn chart_longitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    let axis = frame.longitude();
    if (value % axis.period()).to_bits() == axis.half_period().to_bits() {
        return Ok(180.0);
    }
    axis.canonicalize(value)
        .map(|longitude| longitude.get() * scale)
        .map_err(|error| {
            crate::geometry::GeometryErrorKind::antimeridian_split_failed(error.to_string())
        })
}

fn chart_latitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    crate::geographic::Latitude::try_new(frame, value)
        .map(|latitude| latitude.get() * scale)
        .map_err(|error| {
            crate::geometry::GeometryErrorKind::antimeridian_split_failed(error.to_string())
        })
}

fn chart_point(
    point: Point,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Point> {
    point.with_xy(
        chart_longitude(point.x, frame, longitude_scale)?,
        chart_latitude(point.y, frame, latitude_scale)?,
    )
}

const fn chart_pole(point: Point) -> Option<ChartPole> {
    if point.y.to_bits() == 90.0_f64.to_bits() {
        Some(ChartPole::North)
    } else if point.y.to_bits() == (-90.0_f64).to_bits() {
        Some(ChartPole::South)
    } else {
        None
    }
}

fn chart_side(point: Point) -> ChartCutSide {
    if point.x > 0.0 {
        ChartCutSide::East
    } else {
        ChartCutSide::West
    }
}

fn synthetic_vertex(
    native_point: Point,
    chart_point: Point,
    node: u32,
) -> Option<SyntheticChartVertex> {
    if let Some(pole) = chart_pole(chart_point) {
        return Some(SyntheticChartVertex::Pole {
            node,
            pole,
            native_longitude_bits: native_point.x.to_bits(),
        });
    }
    if chart_point.x.abs() == 180.0 {
        return Some(SyntheticChartVertex::Cut {
            node,
            side: chart_side(chart_point),
        });
    }
    None
}

pub(super) fn restriction_for_edge(start: Point, end: Point) -> Option<ChartRestriction> {
    let start_cut = start.x.abs() == 180.0;
    let end_cut = end.x.abs() == 180.0;
    let start_pole = chart_pole(start);
    let end_pole = chart_pole(end);
    match (start_pole, end_pole, start_cut, end_cut) {
        (Some(left), Some(right), ..) if left == right && start.x != end.x => {
            Some(ChartRestriction::PolarCap { pole: left })
        },
        (Some(pole), None, _, true) | (None, Some(pole), true, _) => {
            Some(ChartRestriction::PolarCap { pole })
        },
        (Some(_), Some(_), true, true) if start.x == end.x => Some(ChartRestriction::CutChord),
        (_, _, true, true) => Some(ChartRestriction::CutChord),
        _ => None,
    }
}

fn next_node(node: &mut u32) -> Result<u32> {
    let current = *node;
    *node = node.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(current)
}

fn ring_vertex_node(
    vertex_index: usize,
    last_index: usize,
    closed_duplicate: bool,
    nodes: &mut [Option<u32>],
    next_node_id: &mut u32,
) -> Result<u32> {
    let node_index = if closed_duplicate && vertex_index == last_index {
        0
    } else {
        vertex_index
    };
    let slot = nodes.get_mut(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("restriction vertex lies outside its ring".into())
    })?;
    if let Some(node) = *slot {
        return Ok(node);
    }
    let node = next_node(next_node_id)?;
    *slot = Some(node);
    Ok(node)
}

fn ring_restrictions(
    ring: &Ring,
    lineage: &LineageSequence,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    let native_points = ring.iter_coords().collect::<Vec<_>>();
    if native_points.len() < 2 {
        return Ok(());
    }
    if native_points.len() != lineage.vertices.len()
        || native_points.len().saturating_sub(1) != lineage.edges.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian ring coordinates and edge lineage disagree".into(),
        )
        .into());
    }
    let last_index = native_points
        .len()
        .checked_sub(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let closed_duplicate =
        native_points
            .first()
            .zip(native_points.last())
            .is_some_and(|(first, last)| {
                first.x.to_bits() == last.x.to_bits()
                    && first.y.to_bits() == last.y.to_bits()
                    && first.z().map(f64::to_bits) == last.z().map(f64::to_bits)
                    && first.m().map(f64::to_bits) == last.m().map(f64::to_bits)
            });
    let chart_points = native_points
        .iter()
        .copied()
        .map(|point| chart_point(point, frame, longitude_scale, latitude_scale))
        .collect::<Result<Vec<_>>>()?;
    let mut nodes = vec![None; native_points.len()];
    for (index, (native_pair, chart_pair)) in native_points
        .array_windows::<2>()
        .zip(chart_points.array_windows::<2>())
        .enumerate()
    {
        let [start_native, end_native] = *native_pair;
        let [start_chart, end_chart] = *chart_pair;
        let edge = lineage.edges.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("antimeridian ring edge lineage is unavailable".into())
        })?;
        let edge_use = catalog.edge_use_at(edge);
        if edge_use.member != lineage.member {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian chart restriction belongs to a different member".into(),
            )
            .into());
        }
        let Some(restriction) = edge_use.restriction else {
            continue;
        };
        let end_index = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let start_node = ring_vertex_node(
            index,
            last_index,
            closed_duplicate,
            &mut nodes,
            next_node_id,
        )?;
        let end_node = ring_vertex_node(
            end_index,
            last_index,
            closed_duplicate,
            &mut nodes,
            next_node_id,
        )?;
        let (Some(start), Some(end)) = (
            synthetic_vertex(start_native, start_chart, start_node),
            synthetic_vertex(end_native, end_chart, end_node),
        ) else {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian chart restriction has a nonsynthetic endpoint".into(),
            )
            .into());
        };
        output.push(DirectedChartRestrictionUse {
            restriction,
            start,
            end,
        });
    }
    Ok(())
}

fn collect_restrictions(
    shape: &Shape,
    lineage: &ShapeLineage,
    catalog: &LineageCatalog,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    match (shape, lineage) {
        (Shape::Polygon(polygon), ShapeLineage::Polygon(polygon_lineage)) => {
            ring_restrictions(
                &polygon.shell,
                &polygon_lineage.shell,
                catalog,
                frame,
                longitude_scale,
                latitude_scale,
                next_node_id,
                output,
            )?;
            for (hole, hole_lineage) in polygon.holes.iter().zip(&polygon_lineage.holes) {
                ring_restrictions(
                    hole,
                    hole_lineage,
                    catalog,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        (
            Shape::MultiPolygon(polygons),
            ShapeLineage::MultiPolygon {
                members: lineages, ..
            },
        ) => {
            if polygons.member_count() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian multipolygon members and lineage disagree".into(),
                )
                .into());
            }
            for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                match (polygon, lineage) {
                    (MultiMember::Empty, None) => {},
                    (MultiMember::Value(polygon), Some(lineage)) => {
                        ring_restrictions(
                            &polygon.shell,
                            &lineage.shell,
                            catalog,
                            frame,
                            longitude_scale,
                            latitude_scale,
                            next_node_id,
                            output,
                        )?;
                        for (hole, hole_lineage) in polygon.holes.iter().zip(&lineage.holes) {
                            ring_restrictions(
                                hole,
                                hole_lineage,
                                catalog,
                                frame,
                                longitude_scale,
                                latitude_scale,
                                next_node_id,
                                output,
                            )?;
                        }
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian multipolygon members and lineage disagree".into(),
                        )
                        .into());
                    },
                }
            }
            Ok(())
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
                collect_restrictions(
                    member,
                    lineage,
                    catalog,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        (Shape::Point(_), ShapeLineage::Point { .. })
        | (Shape::MultiPoint(_), ShapeLineage::MultiPoint { .. })
        | (Shape::LineString(_), ShapeLineage::LineString(_))
        | (Shape::MultiLineString(_), ShapeLineage::MultiLineString { .. })
        | (Shape::Empty(..), ShapeLineage::Empty) => Ok(()),
        _ => Err(GeometryErrorKind::Invalid(
            "antimeridian shape and lineage disagree during chart restriction collection".into(),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::geographic::AngularFactor;
    use crate::geometry::tessellation::exact::ExactDyadic;
    use crate::geometry::{ChartMetric as _, CoordSeq, MOrdinate, ZOrdinate};

    fn independent_oblique_edge_fraction(
        parameter: f64,
        start: (f64, f64),
        end: (f64, f64),
        radians_per_unit: f64,
        semi_major: f64,
        flattening: f64,
    ) -> f64 {
        fn integrate(
            limit: f64,
            start: (f64, f64),
            end: (f64, f64),
            radians_per_unit: f64,
            semi_major: f64,
            flattening: f64,
        ) -> f64 {
            const INTERVALS: usize = 16_384;
            let step = limit / INTERVALS as f64;
            let longitude_rate = (end.0 - start.0) * radians_per_unit;
            let latitude_rate = (end.1 - start.1) * radians_per_unit;
            let eccentricity_squared = flattening * (2.0 - flattening);
            let mut sum = 0.0;
            for index in 0..=INTERVALS {
                let parameter = index as f64 * step;
                let latitude = (start.1 + (end.1 - start.1) * parameter) * radians_per_unit;
                let sine = latitude.sin();
                let denominator = 1.0 - eccentricity_squared * sine * sine;
                let meridional_radius =
                    semi_major * (1.0 - eccentricity_squared) / denominator.powf(1.5);
                let prime_vertical_radius = semi_major / denominator.sqrt();
                let northing = meridional_radius * latitude_rate;
                let easting = prime_vertical_radius * latitude.cos() * longitude_rate;
                let weight = if index == 0 || index == INTERVALS {
                    1.0
                } else if index % 2 == 0 {
                    2.0
                } else {
                    4.0
                };
                sum += weight * northing.hypot(easting);
            }
            step * sum / 3.0
        }

        integrate(
            parameter,
            start,
            end,
            radians_per_unit,
            semi_major,
            flattening,
        ) / integrate(1.0, start, end, radians_per_unit, semi_major, flattening)
    }

    fn measured_crossing_line() -> Shape {
        let points = [
            Point::new_axes(170.0, 10.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(100.0))).unwrap(),
            Point::new_axes(-170.0, 30.0, ZOrdinate(Some(20.0)), MOrdinate(Some(300.0))).unwrap(),
        ];
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
    }

    #[test]
    fn split_receipt_keeps_both_seam_banks_on_the_exact_source_root() {
        let row = measured_crossing_line()
            .split_antimeridian_captured_in_frame(GeographicFrame::crs84())
            .unwrap()
            .into_row(0, 0, 0)
            .unwrap();
        let seam = row
            .literals
            .iter()
            .filter(|literal| literal.record.x.abs() == 180.0)
            .collect::<Vec<_>>();
        assert_eq!(seam.len(), 2);

        let parameter =
            ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2)).unwrap();
        for literal in seam {
            let uses = row
                .source_root_uses
                .iter()
                .filter(|source| source.address == literal.address && source.image == literal.image)
                .collect::<Vec<_>>();
            assert_eq!(uses.len(), 1);
            assert_eq!(uses[0].range.root(), RootId::from_index(0));
            assert_eq!(uses[0].range.source_start(), &parameter);
            assert_eq!(uses[0].range.source_end(), &parameter);
        }
    }

    #[test]
    fn split_receipt_maps_native_unit_roots_after_chart_conversion() {
        let grad = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        let frame = GeographicFrame::try_new(grad, grad, 0.0).unwrap();
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![185.0, -190.0], vec![10.0, 30.0], None, None).unwrap(),
            )
            .unwrap(),
        );
        let row = source
            .split_antimeridian_captured_in_frame(frame)
            .unwrap()
            .into_row(0, 0, 0)
            .unwrap();
        let seam = row
            .literals
            .iter()
            .filter(|literal| literal.record.x.abs() == 200.0)
            .collect::<Vec<_>>();
        assert_eq!(seam.len(), 2);

        let parameter =
            ExactRational::new(ExactDyadic::from_i64(3), ExactDyadic::from_i64(5)).unwrap();
        for literal in seam {
            let source = row
                .source_root_uses
                .iter()
                .find(|source| source.address == literal.address && source.image == literal.image)
                .expect("each native seam bank keeps its source edge");
            assert_eq!(source.range.root(), RootId::from_index(0));
            assert_eq!(source.range.source_start(), &parameter);
            assert_eq!(source.range.source_end(), &parameter);
        }
    }

    #[test]
    fn split_hole_overlay_keeps_the_original_root_for_a_late_coowned_sample() {
        const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        let source_start =
            Point::new_axes(160.0, -40.0, ZOrdinate(Some(20.0)), MOrdinate(Some(200.0))).unwrap();
        let source_end =
            Point::new_axes(240.0, 60.0, ZOrdinate(Some(120.0)), MOrdinate(Some(1200.0))).unwrap();
        let parameter =
            ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2)).unwrap();
        let (fixture_z, fixture_m) =
            crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
                let edge = metric.chart_edge(source_start, source_end)?;
                let fraction =
                    metric.chart_edge_arclength_fraction(&edge, parameter.round()?, None)?;
                Ok(crate::geometry::source_ordinate_values_at_fraction(
                    source_start,
                    source_end,
                    CoordinateAxes::XYZM,
                    fraction,
                ))
            })
            .unwrap();
        let expected_fraction = independent_oblique_edge_fraction(
            0.5,
            (160.0, -40.0),
            (240.0, 60.0),
            0.015_707_963_267_948_967,
            7_000_000.0,
            1.0 / 150.0,
        );
        let expected_z = 20.0 + (120.0 - 20.0) * expected_fraction;
        let expected_m = 200.0 + (1200.0 - 200.0) * expected_fraction;
        assert!(
            (expected_fraction - 0.5).abs() > 1.0e-4,
            "the oblique custom-ellipsoid oracle must distinguish arclength from chart t"
        );
        let fixture_z = fixture_z.0.expect("source edge has Z");
        let fixture_m = fixture_m.0.expect("source edge has M");

        let ring = |records: &[(f64, f64, f64, f64)]| {
            let points = records
                .iter()
                .map(|&(x, y, z, m)| {
                    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap()
                })
                .collect::<Vec<_>>();
            Ring::closed_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap()
        };
        let shell = ring(&[
            (150.0, -80.0, fixture_z, fixture_m),
            (-150.0, -80.0, fixture_z, fixture_m),
            (-150.0, 80.0, fixture_z, fixture_m),
            (150.0, 80.0, fixture_z, fixture_m),
            (150.0, -80.0, fixture_z, fixture_m),
        ]);
        let hole = ring(&[
            (160.0, -50.0, fixture_z, fixture_m),
            (160.0, -40.0, 20.0, 200.0),
            (240.0, 60.0, 120.0, 1200.0),
            (240.0, -50.0, fixture_z, fixture_m),
            (160.0, -50.0, fixture_z, fixture_m),
        ]);
        // The split hole's seam chord partially co-owns the shell's cut chord
        // in reverse. The point member shares the sample's full record, but
        // not its polygon-edge ancestry.
        let point = Shape::Point(
            Point::new_axes(
                200.0,
                10.0,
                ZOrdinate(Some(fixture_z)),
                MOrdinate(Some(fixture_m)),
            )
            .unwrap(),
        );
        let source = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(shell, vec![hole])),
            point,
        ])
        .unwrap();
        let drawing = crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
            let metric_access = || Ok(metric);
            source.split_antimeridian_captured_in_frame_with_metric(frame, Some(&metric_access))
        })
        .unwrap()
        .orient_geojson()
        .unwrap();
        let row = drawing.into_row(0, 0, 0).unwrap();
        let sample = row
            .literals
            .iter()
            .find(|literal| literal.record.x.abs() == 200.0 && literal.record.y == 10.0)
            .expect("the hole overlay retains its oblique seam sample");
        assert!(
            (sample.record.z().unwrap() - expected_z).abs() <= 2.0e-8,
            "the later sample must use original-edge ellipsoidal Z: {:?}, expected {expected_z}",
            sample.record
        );
        assert!(
            (sample.record.m().unwrap() - expected_m).abs() <= 2.0e-7,
            "the later sample must use original-edge ellipsoidal M: {:?}, expected {expected_m}",
            sample.record
        );

        let uses = row
            .source_root_uses
            .iter()
            .filter(|source| source.address == sample.address && source.image == sample.image)
            .collect::<Vec<_>>();
        assert_eq!(
            uses.len(),
            1,
            "the seam sample has one source edge use, found {:?}",
            uses.iter()
                .map(|source| (
                    source.range.root(),
                    source.range.source_start(),
                    source.range.source_end()
                ))
                .collect::<Vec<_>>()
        );
        let sample_use = uses
            .first()
            .expect("the sample has exactly one source edge use");
        assert_eq!(sample_use.range.root(), RootId::from_index(5));
        assert_eq!(sample_use.range.source_start(), &parameter);
        assert_eq!(sample_use.range.source_end(), &parameter);

        let point_image = row.layout.layout.points[0].image;
        let point_uses = row
            .source_root_uses
            .iter()
            .filter(|source| source.image == point_image)
            .collect::<Vec<_>>();
        let point_parameter = ExactRational::zero();
        let point_roots = point_uses
            .iter()
            .map(|source| source.range.root())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            point_roots,
            BTreeSet::from([RootId::from_index(8)]),
            "the independent point member must not inherit its coincident hole-edge root; uses {:?}",
            point_uses
                .iter()
                .map(|source| (
                    source.range.root(),
                    source.range.source_start(),
                    source.range.source_end()
                ))
                .collect::<Vec<_>>()
        );
        assert!(
            point_uses.iter().all(|source| {
                source.range.source_start() == &point_parameter
                    && source.range.source_end() == &point_parameter
            }),
            "the point member's source uses must remain zero-length roots"
        );

        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        let receipt = ProducerDrawingReceipt::new(
            buffers,
            row.literals,
            row.source_root_uses,
            row.chart_restrictions,
        );
        let mut work = Unmetered;
        let outcome = finish_geographic(
            PublicationCall::new(
                ProducerReceipt::ProducerDrawing(receipt),
                Frame::None,
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .unwrap();
        let PublicationOutcome::Pass(finished) = outcome else {
            panic!("the final scalar storage accepts the coowned split receipt");
        };
        let (published, _) = finished.into_scalar().unwrap();
        let mut retained = false;
        published.for_each_point(|point| {
            retained |= point.x.abs() == 200.0
                && point.y == 10.0
                && point.z().is_some_and(|z| (z - expected_z).abs() <= 2.0e-8)
                && point.m().is_some_and(|m| (m - expected_m).abs() <= 2.0e-7);
        });
        assert!(
            retained,
            "the final storage readback retains the late sample"
        );
    }

    #[test]
    fn split_receipt_marks_both_generated_polygon_cut_chords() {
        let shell = Ring::closed_coordseq(
            CoordSeq::try_from_vecs(
                vec![170.0, -170.0, -170.0, 170.0, 170.0],
                vec![0.0, 0.0, 10.0, 10.0, 0.0],
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
        let row = source
            .split_antimeridian_captured_in_frame(GeographicFrame::crs84())
            .unwrap()
            .into_row(0, 0, 0)
            .unwrap();

        assert_eq!(row.chart_restrictions.len(), 2);
        let mut banks = BTreeSet::new();
        for restriction in &row.chart_restrictions {
            assert_eq!(restriction.restriction, ChartRestriction::CutChord);
            let (
                SyntheticChartVertex::Cut { side: start, .. },
                SyntheticChartVertex::Cut { side: end, .. },
            ) = (restriction.start, restriction.end)
            else {
                panic!("a cut chord has two cut vertices");
            };
            assert_eq!(start, end);
            banks.insert(start);
        }
        assert_eq!(
            banks,
            BTreeSet::from([ChartCutSide::East, ChartCutSide::West])
        );
    }

    #[test]
    fn omitted_synthetic_pole_literal_is_rejected_by_final_readback() {
        let shell = Ring::closed_coordseq(
            CoordSeq::try_from_vecs(
                vec![-45.0, 45.0, 135.0, -135.0, -45.0],
                vec![40.0, 40.0, 40.0, 40.0, 40.0],
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
        let mut row = source
            .split_antimeridian_captured_in_frame(GeographicFrame::crs84())
            .unwrap()
            .into_row(0, 0, 0)
            .unwrap();
        let omitted_index = row
            .literals
            .iter()
            .position(|literal| literal.record.y.abs() == 90.0)
            .expect("the XY polar split emits a synthetic pole literal");
        let omitted = row.literals.remove(omitted_index);
        assert!(
            !row.source_root_uses.iter().any(|source| {
                source.address == omitted.address && source.image == omitted.image
            }),
            "a synthetic pole has no source-edge root"
        );
        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        let receipt = ProducerDrawingReceipt::new(
            buffers,
            row.literals,
            row.source_root_uses,
            row.chart_restrictions,
        );
        let mut work = Unmetered;
        let outcome = finish_geographic(
            PublicationCall::new(
                ProducerReceipt::ProducerDrawing(receipt),
                Frame::None,
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .expect("an omitted receipt record is a readback rejection");
        let PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) = outcome
        else {
            panic!("the shared readback rejects a missing producer literal");
        };
        assert_eq!(images, vec![omitted.image]);
    }
}
