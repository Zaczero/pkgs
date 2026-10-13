use std::collections::BTreeMap;
use std::sync::Arc;

use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeodeticPole, GeographicFrame};
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowLayout, LiteralSequence,
    ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
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
    CoordinateAxes, Coordinates as _, GeometryErrorKind, LineSeq, Point, Polygon, Ring, Shape,
    Unmetered,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PointRecordKey {
    x: u64,
    y: u64,
    z: Option<u64>,
    m: Option<u64>,
}

impl PointRecordKey {
    fn new(point: Point) -> Self {
        Self {
            x: point.x.to_bits(),
            y: point.y.to_bits(),
            z: point.z().map(f64::to_bits),
            m: point.m().map(f64::to_bits),
        }
    }

    fn chart_edge(point: Point) -> Self {
        let mut key = Self::new(point);
        if point.x.abs() == 180.0 {
            key.x = 180.0_f64.to_bits();
        }
        key
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChartEdgeKey(PointRecordKey, PointRecordKey);

impl ChartEdgeKey {
    fn new(start: Point, end: Point) -> Self {
        let start = PointRecordKey::chart_edge(start);
        let end = PointRecordKey::chart_edge(end);
        if start <= end {
            Self(start, end)
        } else {
            Self(end, start)
        }
    }
}

struct CapturedRootPoint {
    point: Point,
    range: RootRange,
}

pub(super) struct SplitCapture {
    identity: bool,
    next_root: u32,
    root_points: Vec<CapturedRootPoint>,
    source_edges: Vec<(Point, Point)>,
}

impl SplitCapture {
    pub(super) const fn enabled() -> Self {
        Self {
            identity: false,
            next_root: 0,
            root_points: Vec::new(),
            source_edges: Vec::new(),
        }
    }

    pub(super) fn mark_identity(&mut self) {
        self.identity = true;
        self.root_points.clear();
        self.source_edges.clear();
    }

    pub(super) fn next_source_root(&mut self) -> Result<RootId> {
        let root = RootId::from_index(self.next_root);
        self.next_root = self
            .next_root
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(root)
    }

    pub(super) fn record_root_point(
        &mut self,
        root: RootId,
        point: Point,
        source_parameter: &ExactRational,
    ) -> Result<()> {
        let mut work = Unmetered;
        let range = RootRange::point_with_work(
            root,
            source_parameter.try_clone_with_work(&mut work)?,
            ExactRational::zero(),
            &mut work,
        )?;
        self.root_points.push(CapturedRootPoint { point, range });
        if point.x.abs() == 180.0 {
            let range = self
                .root_points
                .last()
                .expect("the source root was just captured")
                .range
                .try_clone_with_work(&mut work)?;
            self.root_points.push(CapturedRootPoint {
                point: point.with_xy_raw(-point.x, point.y),
                range,
            });
        }
        Ok(())
    }

    pub(super) fn capture_source_edges(&mut self, shape: &Shape) {
        collect_shape_edges(shape, &mut self.source_edges);
    }

    pub(super) fn into_drawing(
        self,
        shape: Shape,
        frame: GeographicFrame,
        longitude_scale: f64,
        latitude_scale: f64,
    ) -> SplitDrawing {
        SplitDrawing {
            shape,
            frame,
            longitude_scale,
            latitude_scale,
            identity: self.identity,
            root_points: self.root_points,
            source_edges: self.source_edges,
        }
    }
}

pub(crate) struct SplitDrawing {
    shape: Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    identity: bool,
    root_points: Vec<CapturedRootPoint>,
    source_edges: Vec<(Point, Point)>,
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
        let source_roots = self.source_root_map()?;
        let source_edges = self.source_edge_counts()?;
        let mut layout = ShapeLayoutBuilder::new(logical_row, image_start, &source_roots);
        layout.visit(&self.shape)?;
        let (chart_restrictions, next_node) = chart_restrictions(
            &self.shape,
            self.frame,
            self.longitude_scale,
            self.latitude_scale,
            source_edges,
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

    fn source_root_map(&self) -> Result<BTreeMap<PointRecordKey, Vec<RootRange>>> {
        let mut result = BTreeMap::new();
        let mut work = Unmetered;
        for captured in &self.root_points {
            let point = unmap_chart_point(
                captured.point,
                self.frame,
                self.longitude_scale,
                self.latitude_scale,
            )?;
            result
                .entry(PointRecordKey::new(point))
                .or_insert_with(Vec::new)
                .push(captured.range.try_clone_with_work(&mut work)?);
        }
        Ok(result)
    }

    fn source_edge_counts(&self) -> Result<BTreeMap<ChartEdgeKey, usize>> {
        let mut result = BTreeMap::new();
        if self.identity {
            let mut edges = Vec::new();
            collect_shape_edges(&self.shape, &mut edges);
            for (start, end) in edges {
                add_edge_count(
                    &mut result,
                    chart_point(start, self.frame, self.longitude_scale, self.latitude_scale)?,
                    chart_point(end, self.frame, self.longitude_scale, self.latitude_scale)?,
                )?;
            }
            return Ok(result);
        }
        for &(start, end) in &self.source_edges {
            add_edge_count(&mut result, start, end)?;
        }
        Ok(result)
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
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    mut source_edges: BTreeMap<ChartEdgeKey, usize>,
    node_start: u32,
) -> Result<(Vec<DirectedChartRestrictionUse>, u32)> {
    let mut restrictions = Vec::new();
    let mut next_node_id = node_start;
    collect_restrictions(
        shape,
        frame,
        longitude_scale,
        latitude_scale,
        &mut source_edges,
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
    source_roots: &'a BTreeMap<PointRecordKey, Vec<RootRange>>,
    points: Vec<LiteralPoint<ProducerImageId>>,
    lines: Vec<LiteralSequence<ProducerImageId>>,
    polygons: Vec<LiteralPolygon<ProducerImageId>>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
}

impl<'a> ShapeLayoutBuilder<'a> {
    const fn new(
        logical_row: usize,
        image_start: usize,
        source_roots: &'a BTreeMap<PointRecordKey, Vec<RootRange>>,
    ) -> Self {
        Self {
            logical_row,
            next_image: image_start,
            point_output: 0,
            line_output: 0,
            polygon_output: 0,
            source_roots,
            points: Vec::new(),
            lines: Vec::new(),
            polygons: Vec::new(),
            literals: Vec::new(),
            source_root_uses: Vec::new(),
        }
    }

    fn image(&mut self, point: Point, output: u32) -> Result<ProducerImageId> {
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
        if let Some(ranges) = self.source_roots.get(&PointRecordKey::new(point)) {
            let mut work = Unmetered;
            for range in ranges {
                self.source_root_uses.push(ProducerSourceRootUse {
                    address,
                    image,
                    range: range.try_clone_with_work(&mut work)?,
                });
            }
        }
        Ok(image)
    }

    fn visit(&mut self, shape: &Shape) -> Result<()> {
        match shape {
            Shape::Point(point) => {
                let output = self.point_output;
                self.point_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let image = self.image(*point, output)?;
                self.points.push(LiteralPoint {
                    output,
                    axes: CoordinateAxes::from_point(*point),
                    image,
                });
            },
            Shape::MultiPoint(points) => {
                for point in points.points() {
                    let output = self.point_output;
                    self.point_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    let image = self.image(point, output)?;
                    self.points.push(LiteralPoint {
                        output,
                        axes: points.axes(),
                        image,
                    });
                }
            },
            Shape::LineString(line) => {
                let output = self.line_output;
                self.line_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let sequence = self.sequence(line, output)?;
                self.lines.push(sequence);
            },
            Shape::MultiLineString(lines) => {
                for line in lines.as_slice() {
                    let output = self.line_output;
                    self.line_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    let sequence = self.sequence(line, output)?;
                    self.lines.push(sequence);
                }
            },
            Shape::Polygon(polygon) => {
                let output = self.polygon_output;
                self.polygon_output = output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let polygon = self.polygon(polygon, output)?;
                self.polygons.push(polygon);
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    let output = self.polygon_output;
                    self.polygon_output =
                        output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    let polygon = self.polygon(polygon, output)?;
                    self.polygons.push(polygon);
                }
            },
            Shape::GeometryCollection(collection) => {
                for member in collection {
                    self.visit(member)?;
                }
            },
            Shape::Empty(..) => {},
        }
        Ok(())
    }

    fn sequence(
        &mut self,
        line: &LineSeq,
        output: u32,
    ) -> Result<LiteralSequence<ProducerImageId>> {
        let axes = line.as_coords().axes();
        let mut images = Vec::with_capacity(line.len());
        for point in line.iter_coords() {
            images.push(self.image(point, output)?);
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
    ) -> Result<LiteralPolygon<ProducerImageId>> {
        let axes = polygon.axes();
        let shell = self.ring(&polygon.shell, output)?;
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes.iter() {
            holes.push(self.ring(hole, output)?);
        }
        Ok(LiteralPolygon {
            output,
            axes,
            shell,
            holes,
        })
    }

    fn ring(&mut self, ring: &Ring, output: u32) -> Result<LiteralSequence<ProducerImageId>> {
        let axes = ring.coords().axes();
        let mut images = Vec::with_capacity(ring.coords().len());
        for point in ring.iter_coords() {
            images.push(self.image(point, output)?);
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

fn unmap_chart_point(
    point: Point,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Point> {
    let latitude = if point.y == 90.0 {
        frame.native_latitude_pole(GeodeticPole::North)
    } else if point.y == -90.0 {
        frame.native_latitude_pole(GeodeticPole::South)
    } else {
        point.y / latitude_scale
    };
    point.with_xy(point.x / longitude_scale, latitude)
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

fn collect_shape_edges(shape: &Shape, output: &mut Vec<(Point, Point)>) {
    match shape {
        Shape::LineString(line) => {
            output.extend(line.segment_pairs().map(<(Point, Point)>::from));
        },
        Shape::MultiLineString(lines) => {
            for line in lines.as_slice() {
                output.extend(line.segment_pairs().map(<(Point, Point)>::from));
            }
        },
        Shape::Polygon(polygon) => {
            for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                output.extend(ring.segment_pairs().map(<(Point, Point)>::from));
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.as_slice() {
                for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                    output.extend(ring.segment_pairs().map(<(Point, Point)>::from));
                }
            }
        },
        Shape::GeometryCollection(collection) => {
            for member in collection {
                collect_shape_edges(member, output);
            }
        },
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => {},
    }
}

fn add_edge_count(
    edges: &mut BTreeMap<ChartEdgeKey, usize>,
    start: Point,
    end: Point,
) -> Result<()> {
    let count = edges.entry(ChartEdgeKey::new(start, end)).or_default();
    *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(())
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

fn restriction_for_edge(start: Point, end: Point) -> Option<ChartRestriction> {
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
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    source_edges: &mut BTreeMap<ChartEdgeKey, usize>,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    let native_points = ring.iter_coords().collect::<Vec<_>>();
    if native_points.len() < 2 {
        return Ok(());
    }
    let last_index = native_points
        .len()
        .checked_sub(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let closed_duplicate = native_points
        .first()
        .zip(native_points.last())
        .is_some_and(|(first, last)| PointRecordKey::new(*first) == PointRecordKey::new(*last));
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
        let edge = ChartEdgeKey::new(start_chart, end_chart);
        if let Some(count) = source_edges.get_mut(&edge)
            && let Some(remaining) = count.checked_sub(1)
        {
            *count = remaining;
            continue;
        }
        let Some(restriction) = restriction_for_edge(start_chart, end_chart) else {
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
            continue;
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
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    source_edges: &mut BTreeMap<ChartEdgeKey, usize>,
    next_node_id: &mut u32,
    output: &mut Vec<DirectedChartRestrictionUse>,
) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            ring_restrictions(
                &polygon.shell,
                frame,
                longitude_scale,
                latitude_scale,
                source_edges,
                next_node_id,
                output,
            )?;
            for hole in polygon.holes.iter() {
                ring_restrictions(
                    hole,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    source_edges,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.as_slice() {
                ring_restrictions(
                    &polygon.shell,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    source_edges,
                    next_node_id,
                    output,
                )?;
                for hole in polygon.holes.iter() {
                    ring_restrictions(
                        hole,
                        frame,
                        longitude_scale,
                        latitude_scale,
                        source_edges,
                        next_node_id,
                        output,
                    )?;
                }
            }
            Ok(())
        },
        Shape::GeometryCollection(collection) => {
            for member in collection {
                collect_restrictions(
                    member,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    source_edges,
                    next_node_id,
                    output,
                )?;
            }
            Ok(())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::geographic::AngularFactor;
    use crate::geometry::tessellation::exact::ExactDyadic;
    use crate::geometry::{CoordSeq, MOrdinate, ZOrdinate};

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
