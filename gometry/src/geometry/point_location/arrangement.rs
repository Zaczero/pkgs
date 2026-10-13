//! Exact point location in a frozen, labelled planar arrangement.

use std::cmp::Ordering;
use std::sync::Arc;
#[cfg(all(test, debug_assertions))]
use std::{cell::Cell, thread_local};

use crate::error::Result;
use crate::geometry::GeometryErrorKind;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::ordering::try_sort_by;
use crate::geometry::source_support::SourcePointLocation;
use crate::geometry::tessellation::exact::{CompactLimbs, ExactDyadic, compare_ratios};
use crate::heap_size::ARC_CONTROL_BYTES;

#[cfg(all(test, debug_assertions))]
thread_local! {
    static QUERY_COMPARISONS: Cell<usize> = const { Cell::new(0) };
    static QUERY_EDGE_TESTS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(all(test, debug_assertions))]
pub(crate) fn reset_query_comparisons() {
    QUERY_COMPARISONS.with(|count| count.set(0));
    QUERY_EDGE_TESTS.with(|count| count.set(0));
}

#[cfg(all(test, debug_assertions))]
pub(crate) fn query_comparisons() -> usize {
    QUERY_COMPARISONS.with(Cell::get)
}

#[cfg(all(test, debug_assertions))]
pub(crate) fn query_edge_tests() -> usize {
    QUERY_EDGE_TESTS.with(Cell::get)
}

#[cfg(all(test, debug_assertions))]
fn record_query_comparison() {
    QUERY_COMPARISONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(not(all(test, debug_assertions)))]
const fn record_query_comparison() {}

#[cfg(all(test, debug_assertions))]
fn record_query_edge_test() {
    QUERY_EDGE_TESTS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(not(all(test, debug_assertions)))]
const fn record_query_edge_test() {}

pub(in crate::geometry) type ExactLocationPoint = [ExactDyadic; 3];
pub(in crate::geometry) type ExactLocationPoints = Arc<[ExactLocationPoint]>;

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct ExactLocationEdge {
    pub(crate) start: u32,
    pub(crate) end: u32,
    /// Global faces on the left and right of `start -> end`.
    pub(crate) faces: [u32; 2],
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct ExactLocationDart {
    pub(in crate::geometry) target: u32,
    pub(in crate::geometry) span: u32,
    pub(in crate::geometry) left_face: u32,
}

#[derive(Debug)]
struct HorizontalSpan {
    edge: u32,
    low: u32,
    high: u32,
}

#[derive(Debug)]
struct Event {
    point: u32,
    vertices: Vec<u32>,
    starts: Vec<u32>,
    ends: Vec<u32>,
    horizontal: Vec<HorizontalSpan>,
    root: Option<u32>,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactLocationGeometry {
    pub(in crate::geometry) points: ExactLocationPoints,
    edges: Arc<[ExactLocationEdge]>,
    node_fans: Vec<Vec<ExactLocationDart>>,
    node_hosts: Arc<[u32]>,
    events: Vec<Event>,
    slabs: crate::geometry::PersistentOrder<u32>,
}

enum LocatedCell {
    Face(u32),
    Span(u32),
    Node(u32),
}

enum OffsetCell {
    Face(u32),
    Span(u32),
}

impl ExactLocationGeometry {
    #[expect(
        clippy::indexing_slicing,
        reason = "edge endpoints, event points, and edge ordinals are built from these checked point and edge tables"
    )]
    fn new(
        points: ExactLocationPoints,
        edges: Vec<ExactLocationEdge>,
        node_fans: Vec<Vec<ExactLocationDart>>,
        node_hosts: Vec<u32>,
    ) -> Result<Arc<Self>> {
        let edge_count = edges.len();
        let point_count =
            u32::try_from(points.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        u32::try_from(edge_count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if points.iter().any(|point| point[2].is_zero()) {
            return Err(GeometryErrorKind::Invalid(
                "exact location point has zero homogeneous weight".into(),
            )
            .into());
        }
        for edge in &edges {
            if edge.start >= point_count || edge.end >= point_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact location edge endpoint exceeds its point table".into(),
                )
                .into());
            }
        }
        if node_fans.len() != points.len() || node_hosts.len() != points.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact location node tables do not match their point table".into(),
            )
            .into());
        }

        let mut point_order = Vec::with_capacity(points.len());
        point_order.extend(0..point_count);
        try_sort_by(&mut point_order, |&left, &right| {
            compare_point_yx(&points[left as usize], &points[right as usize])
        })?;

        let mut events: Vec<Event> = Vec::new();
        for point in point_order {
            if let Some(event) = events.last_mut()
                && compare_point_y(&points[event.point as usize], &points[point as usize])?
                    == Ordering::Equal
            {
                event.vertices.push(point);
            } else {
                events.push(Event {
                    point,
                    vertices: vec![point],
                    starts: Vec::new(),
                    ends: Vec::new(),
                    horizontal: Vec::new(),
                    root: None,
                });
            }
        }

        for (edge_id, edge) in edges.iter().enumerate() {
            let start = edge.start;
            let end = edge.end;
            match compare_point_y(&points[start as usize], &points[end as usize])? {
                Ordering::Less => {
                    let event = event_at_y(&points, &events, &points[start as usize])?;
                    events[event].starts.push(edge_id as u32);
                    let event = event_at_y(&points, &events, &points[end as usize])?;
                    events[event].ends.push(edge_id as u32);
                },
                Ordering::Greater => {
                    let event = event_at_y(&points, &events, &points[end as usize])?;
                    events[event].starts.push(edge_id as u32);
                    let event = event_at_y(&points, &events, &points[start as usize])?;
                    events[event].ends.push(edge_id as u32);
                },
                Ordering::Equal => {
                    let event = event_at_y(&points, &events, &points[start as usize])?;
                    let (low, high) =
                        if compare_point_x(&points[start as usize], &points[end as usize])?
                            == Ordering::Greater
                        {
                            (end, start)
                        } else {
                            (start, end)
                        };
                    events[event].horizontal.push(HorizontalSpan {
                        edge: edge_id as u32,
                        low,
                        high,
                    });
                },
            }
        }
        for event in &mut events {
            try_sort_by(&mut event.horizontal, |left, right| match compare_point_x(
                &points[left.low as usize],
                &points[right.low as usize],
            )? {
                Ordering::Equal => {
                    compare_point_x(&points[left.high as usize], &points[right.high as usize])
                },
                Ordering::Less => Ok(Ordering::Less),
                Ordering::Greater => Ok(Ordering::Greater),
            })?;
        }

        let mut geometry = Self {
            points,
            edges: edges.into(),
            node_fans,
            node_hosts: node_hosts.into(),
            events,
            slabs: crate::geometry::PersistentOrder::with_capacity(edge_count),
        };
        let mut root = None;
        for event_index in 0..geometry.events.len() {
            let ending = std::mem::take(&mut geometry.events[event_index].ends);
            for edge in ending {
                root = geometry.remove_edge(root, edge, SweepSide::Below)?;
            }
            let starting = std::mem::take(&mut geometry.events[event_index].starts);
            for edge in starting {
                root = Some(geometry.insert_edge(root, edge, SweepSide::Above)?);
            }
            geometry.events[event_index].root = root;
        }
        if root.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "exact location slab sweep left active edges above the arrangement".into(),
            )
            .into());
        }
        Ok(Arc::new(geometry))
    }

    fn locate<L: CellLabels>(&self, labels: &L, point: &[ExactDyadic; 3]) -> Result<L::Value> {
        Ok(match self.locate_cell(point)? {
            LocatedCell::Face(face) => labels.face(face),
            LocatedCell::Span(span) => labels.span(span),
            LocatedCell::Node(node) => labels.node(node),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "cell and span IDs come from this geometry's search tree and its partition-owned edge table"
    )]
    fn locate_offset<L: CellLabels>(
        &self,
        labels: &L,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<L::Value> {
        let [dx, dy] = direction;
        if dx.is_zero() && dy.is_zero() {
            return self.locate(labels, point);
        }
        match self.locate_cell(point)? {
            LocatedCell::Face(face) => Ok(labels.face(face)),
            LocatedCell::Span(span) => {
                let edge = self.edges[span as usize];
                match orientation_with_direction(&self.points, edge.start, edge.end, direction)? {
                    ExactSign::Zero => Ok(labels.span(span)),
                    ExactSign::Positive => Ok(labels.face(edge.faces[0])),
                    ExactSign::Negative => Ok(labels.face(edge.faces[1])),
                }
            },
            LocatedCell::Node(node) => match self.locate_node_offset(node, direction)? {
                OffsetCell::Face(face) => Ok(labels.face(face)),
                OffsetCell::Span(span) => Ok(labels.span(span)),
            },
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "event, edge, and face IDs are emitted together by this geometry's exact arrangement builder"
    )]
    fn locate_cell(&self, point: &[ExactDyadic; 3]) -> Result<LocatedCell> {
        let mut low = 0_usize;
        let mut high = self.events.len();
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            record_query_comparison();
            if compare_point_y(&self.points[self.events[middle].point as usize], point)?
                == Ordering::Less
            {
                low = middle.wrapping_add(1);
            } else {
                high = middle;
            }
        }
        let event_index = low;
        let exact_event = if let Some(event) = self.events.get(event_index) {
            record_query_comparison();
            if compare_point_y(&self.points[event.point as usize], point)? == Ordering::Equal {
                if let Some(node) = self.find_vertex(event, point)? {
                    return Ok(LocatedCell::Node(node));
                }
                if let Some(edge) = self.find_horizontal(event, point)? {
                    return Ok(LocatedCell::Span(edge));
                }
                true
            } else {
                false
            }
        } else {
            false
        };

        let root_index = if exact_event {
            Some(event_index)
        } else {
            event_index.checked_sub(1)
        };
        let root = root_index
            .and_then(|index| self.events.get(index))
            .and_then(|event| event.root);
        let mut current = root;
        let mut predecessor = None;
        while let Some(node_id) = current {
            let node = self.slabs.node(node_id);
            record_query_comparison();
            record_query_edge_test();
            match compare_edge_x_to_point(&self.points, self.edges[node.item as usize], point)? {
                Ordering::Less => {
                    predecessor = Some(node.item);
                    current = node.right;
                },
                Ordering::Greater => current = node.left,
                Ordering::Equal => return Ok(LocatedCell::Span(node.item)),
            }
        }
        let Some(edge_id) = predecessor else {
            return Ok(LocatedCell::Face(0));
        };
        let edge = self.edges[edge_id as usize];
        record_query_comparison();
        let rising = compare_point_y(
            &self.points[edge.start as usize],
            &self.points[edge.end as usize],
        )? == Ordering::Less;
        let east_face = edge.faces[usize::from(rising)];
        Ok(LocatedCell::Face(east_face))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node IDs address fan and host tables sized from the same exact point table"
    )]
    fn locate_node_offset(&self, node: u32, direction: &[ExactDyadic; 2]) -> Result<OffsetCell> {
        let fan = &self.node_fans[node as usize];
        if fan.is_empty() {
            return Ok(OffsetCell::Face(self.node_hosts[node as usize]));
        }
        let mut low = 0_usize;
        let mut high = fan.len();
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            let dart = fan[middle];
            record_query_comparison();
            match compare_dart_direction(&self.points, node, dart.target, direction)? {
                Ordering::Less => low = middle.wrapping_add(1),
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(OffsetCell::Span(dart.span)),
            }
        }
        let predecessor = if low == 0 {
            fan.len().wrapping_sub(1)
        } else {
            low.wrapping_sub(1)
        };
        Ok(OffsetCell::Face(fan[predecessor].left_face))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "event vertices are point IDs copied from this geometry's exact point table"
    )]
    fn find_vertex(&self, event: &Event, point: &[ExactDyadic; 3]) -> Result<Option<u32>> {
        let mut low = 0_usize;
        let mut high = event.vertices.len();
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            let node = event.vertices[middle];
            record_query_comparison();
            match compare_point_x(&self.points[node as usize], point)? {
                Ordering::Less => low = middle.wrapping_add(1),
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(Some(node)),
            }
        }
        Ok(None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "horizontal span endpoint IDs are checked against this geometry's exact point table"
    )]
    fn find_horizontal(&self, event: &Event, point: &[ExactDyadic; 3]) -> Result<Option<u32>> {
        let mut low = 0_usize;
        let mut high = event.horizontal.len();
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            let span = &event.horizontal[middle];
            record_query_comparison();
            record_query_edge_test();
            match compare_point_x(&self.points[span.high as usize], point)? {
                Ordering::Less => low = middle.wrapping_add(1),
                Ordering::Greater | Ordering::Equal => high = middle,
            }
        }
        let Some(span) = event.horizontal.get(low) else {
            return Ok(None);
        };
        record_query_comparison();
        record_query_edge_test();
        if compare_point_x(&self.points[span.low as usize], point)? == Ordering::Greater {
            return Ok(None);
        }
        record_query_comparison();
        record_query_edge_test();
        if compare_point_x(&self.points[span.high as usize], point)? == Ordering::Less {
            return Ok(None);
        }
        Ok(Some(span.edge))
    }

    fn insert_edge(&mut self, root: Option<u32>, edge: u32, side: SweepSide) -> Result<u32> {
        let points = &self.points;
        let edges = &self.edges;
        self.slabs.insert(root, edge, |left, right| {
            compare_edges_at_event(points, edges, left, right, side)
        })
    }

    fn remove_edge(
        &mut self,
        root: Option<u32>,
        edge: u32,
        side: SweepSide,
    ) -> Result<Option<u32>> {
        let points = &self.points;
        let edges = &self.edges;
        self.slabs.remove(root, edge, |left, right| {
            compare_edges_at_event(points, edges, left, right, side)
        })
    }
}

#[derive(Clone, Copy)]
enum SweepSide {
    Above,
    Below,
}

#[expect(
    clippy::indexing_slicing,
    reason = "edge IDs and their exact endpoint IDs were admitted while the slab geometry was constructed"
)]
fn compare_edges_at_event(
    points: &[[ExactDyadic; 3]],
    edges: &[ExactLocationEdge],
    left: u32,
    right: u32,
    side: SweepSide,
) -> Result<Ordering> {
    if left == right {
        return Ok(Ordering::Equal);
    }
    let left_edge = edges[left as usize];
    let right_edge = edges[right as usize];
    let point = match side {
        SweepSide::Above => low_high(points, left_edge)?.0,
        SweepSide::Below => low_high(points, left_edge)?.1,
    };
    let relative = compare_edge_x_to_point(points, right_edge, &points[point as usize])?;
    if relative != Ordering::Equal {
        return Ok(relative.reverse());
    }
    let left_slope = compare_edge_slopes(points, left_edge, right_edge)?;
    Ok(match side {
        SweepSide::Above => left_slope.then_with(|| left.cmp(&right)),
        SweepSide::Below => left_slope.reverse().then_with(|| left.cmp(&right)),
    })
}

pub(in crate::geometry) trait CellLabels {
    type Value: Copy;

    fn face(&self, face: u32) -> Self::Value;
    fn span(&self, span: u32) -> Self::Value;
    fn node(&self, node: u32) -> Self::Value;
}

#[derive(Debug)]
pub(in crate::geometry) struct SourceLocationLabels {
    faces: Arc<[SourcePointLocation]>,
    spans: Arc<[SourcePointLocation]>,
    nodes: Arc<[SourcePointLocation]>,
}

impl SourceLocationLabels {
    pub(in crate::geometry) fn new(
        faces: Vec<SourcePointLocation>,
        spans: Vec<SourcePointLocation>,
        nodes: Vec<SourcePointLocation>,
    ) -> Self {
        Self {
            faces: faces.into(),
            spans: spans.into(),
            nodes: nodes.into(),
        }
    }
}

impl CellLabels for SourceLocationLabels {
    type Value = SourcePointLocation;

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs and label rows are emitted from the same source partition"
    )]
    fn face(&self, face: u32) -> Self::Value {
        self.faces[face as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and label rows are emitted from the same source partition"
    )]
    fn span(&self, span: u32) -> Self::Value {
        self.spans[span as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node IDs and label rows are emitted from the same source partition"
    )]
    fn node(&self, node: u32) -> Self::Value {
        self.nodes[node as usize]
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct AreaLocationLabels {
    faces: Arc<[bool]>,
    spans: Arc<[bool]>,
    nodes: Arc<[bool]>,
}

impl AreaLocationLabels {
    pub(in crate::geometry) fn new(faces: Vec<bool>, spans: Vec<bool>, nodes: Vec<bool>) -> Self {
        Self {
            faces: faces.into(),
            spans: spans.into(),
            nodes: nodes.into(),
        }
    }
}

impl CellLabels for AreaLocationLabels {
    type Value = bool;

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs and area labels are emitted from the same metric-support partition"
    )]
    fn face(&self, face: u32) -> Self::Value {
        self.faces[face as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and area labels are emitted from the same metric-support partition"
    )]
    fn span(&self, span: u32) -> Self::Value {
        self.spans[span as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node IDs and area labels are emitted from the same metric-support partition"
    )]
    fn node(&self, node: u32) -> Self::Value {
        self.nodes[node as usize]
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactLocationCells<L: CellLabels> {
    pub(in crate::geometry) geometry: Arc<ExactLocationGeometry>,
    pub(in crate::geometry) labels: L,
}

impl<L: CellLabels> ExactLocationCells<L> {
    pub(in crate::geometry) fn new(
        points: ExactLocationPoints,
        edges: Vec<ExactLocationEdge>,
        node_fans: Vec<Vec<ExactLocationDart>>,
        node_hosts: Vec<u32>,
        labels: L,
    ) -> Result<Self> {
        Ok(Self {
            geometry: ExactLocationGeometry::new(points, edges, node_fans, node_hosts)?,
            labels,
        })
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactLocationIndex<L: CellLabels> {
    geometry: Arc<ExactLocationGeometry>,
    labels: L,
}

impl crate::HeapSize for ExactLocationIndex<SourceLocationLabels> {
    fn heap_bytes(&self) -> usize {
        let geometry = &self.geometry;
        let points = std::mem::size_of_val(geometry.points.as_ref()).saturating_add(
            geometry
                .points
                .iter()
                .flatten()
                .map(|point| match &point.limbs {
                    CompactLimbs::Inline { .. } => 0,
                    CompactLimbs::Heap(limbs) => {
                        limbs.capacity().saturating_mul(std::mem::size_of::<u64>())
                    },
                })
                .fold(0, usize::saturating_add),
        );
        let events = geometry
            .events
            .capacity()
            .saturating_mul(std::mem::size_of::<Event>())
            .saturating_add(
                geometry
                    .events
                    .iter()
                    .map(|event| {
                        event
                            .vertices
                            .capacity()
                            .saturating_add(event.starts.capacity())
                            .saturating_add(event.ends.capacity())
                            .saturating_mul(std::mem::size_of::<u32>())
                            .saturating_add(
                                event
                                    .horizontal
                                    .capacity()
                                    .saturating_mul(std::mem::size_of::<HorizontalSpan>()),
                            )
                    })
                    .fold(0, usize::saturating_add),
            );
        ARC_CONTROL_BYTES
            .saturating_add(std::mem::size_of::<ExactLocationGeometry>())
            .saturating_add(ARC_CONTROL_BYTES.saturating_add(points))
            .saturating_add(events)
            .saturating_add(ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of_val(geometry.edges.as_ref()))
            .saturating_add(ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of_val(geometry.node_hosts.as_ref()))
            .saturating_add(
                geometry
                    .node_fans
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<ExactLocationDart>>()),
            )
            .saturating_add(
                geometry
                    .node_fans
                    .iter()
                    .map(|fan| {
                        fan.capacity()
                            .saturating_mul(std::mem::size_of::<ExactLocationDart>())
                    })
                    .fold(0, usize::saturating_add),
            )
            .saturating_add(geometry.slabs.heap_bytes())
            .saturating_add(ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of_val(self.labels.faces.as_ref()))
            .saturating_add(ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of_val(self.labels.spans.as_ref()))
            .saturating_add(ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of_val(self.labels.nodes.as_ref()))
    }
}

impl<L: CellLabels> ExactLocationIndex<L> {
    pub(in crate::geometry) fn from_cells(cells: ExactLocationCells<L>) -> Self {
        Self {
            geometry: cells.geometry,
            labels: cells.labels,
        }
    }

    pub(in crate::geometry) fn locate_exact(&self, point: &[ExactDyadic; 3]) -> Result<L::Value> {
        self.geometry.locate(&self.labels, point)
    }

    pub(in crate::geometry) fn locate_xy(&self, x: f64, y: f64) -> Result<L::Value> {
        self.locate_exact(&[
            ExactDyadic::from_f64(x),
            ExactDyadic::from_f64(y),
            ExactDyadic::one(),
        ])
    }

    pub(in crate::geometry) fn locate_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<L::Value> {
        self.geometry.locate_offset(&self.labels, point, direction)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "event point IDs are built from the exact point table before event sorting"
)]
fn event_at_y(
    points: &[[ExactDyadic; 3]],
    events: &[Event],
    point: &[ExactDyadic; 3],
) -> Result<usize> {
    let mut low = 0_usize;
    let mut high = events.len();
    while low < high {
        let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
        match compare_point_y(&points[events[middle].point as usize], point)? {
            Ordering::Less => low = middle.wrapping_add(1),
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(middle),
        }
    }
    Err(GeometryErrorKind::Invalid("exact location endpoint has no height event".into()).into())
}

fn compare_point_y(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<Ordering> {
    compare_ratios(&left[1], &left[2], &right[1], &right[2])
}

fn compare_point_x(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<Ordering> {
    compare_ratios(&left[0], &left[2], &right[0], &right[2])
}

fn compare_point_yx(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<Ordering> {
    match compare_point_y(left, right)? {
        Ordering::Equal => compare_point_x(left, right),
        Ordering::Less => Ok(Ordering::Less),
        Ordering::Greater => Ok(Ordering::Greater),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "low/high endpoint IDs are checked against the owned point table when the edge geometry is built"
)]
fn compare_edge_x_to_point(
    points: &[[ExactDyadic; 3]],
    edge: ExactLocationEdge,
    point: &[ExactDyadic; 3],
) -> Result<Ordering> {
    let (low, high) = low_high(points, edge)?;
    let [ax, ay, aw] = &points[low as usize];
    let [bx, by, bw] = &points[high as usize];
    let [x, y, w] = point;
    if aw.is_zero() || bw.is_zero() || w.is_zero() {
        return Err(GeometryErrorKind::Invalid(
            "exact point-location coordinate has zero homogeneous weight".into(),
        )
        .into());
    }
    let dx = bx.try_product(aw)?.try_subtract(ax.try_product(bw)?)?;
    let dy = by.try_product(aw)?.try_subtract(ay.try_product(bw)?)?;
    let query_x = x.try_product(aw)?.try_subtract(ax.try_product(w)?)?;
    let query_y = y.try_product(aw)?.try_subtract(ay.try_product(w)?)?;
    let determinant = dx
        .try_product(&query_y)?
        .try_subtract(dy.try_product(&query_x)?)?;
    let sign = flip_if_negative_weight(
        determinant.sign(),
        (bw.sign() == ExactSign::Negative) ^ (w.sign() == ExactSign::Negative),
    );
    Ok(match sign {
        ExactSign::Positive => Ordering::Greater,
        ExactSign::Negative => Ordering::Less,
        ExactSign::Zero => Ordering::Equal,
    })
}

fn compare_edge_slopes(
    points: &[[ExactDyadic; 3]],
    left: ExactLocationEdge,
    right: ExactLocationEdge,
) -> Result<Ordering> {
    let (left_low, left_high) = low_high(points, left)?;
    let (right_low, right_high) = low_high(points, right)?;
    let (left_dx, left_dy) = direction(points, left_low, left_high)?;
    let (right_dx, right_dy) = direction(points, right_low, right_high)?;
    compare_ratios(&left_dx, &left_dy, &right_dx, &right_dy)
}

#[expect(
    clippy::indexing_slicing,
    reason = "edge endpoint IDs are checked against the shared exact point table when the geometry is built"
)]
fn low_high(points: &[[ExactDyadic; 3]], edge: ExactLocationEdge) -> Result<(u32, u32)> {
    match compare_point_y(&points[edge.start as usize], &points[edge.end as usize])? {
        Ordering::Less => Ok((edge.start, edge.end)),
        Ordering::Greater => Ok((edge.end, edge.start)),
        Ordering::Equal => Err(GeometryErrorKind::Invalid(
            "horizontal span entered the nonhorizontal slab status".into(),
        )
        .into()),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "low/high endpoint IDs are selected from edges range-checked by the geometry constructor"
)]
fn direction(
    points: &[[ExactDyadic; 3]],
    low: u32,
    high: u32,
) -> Result<(ExactDyadic, ExactDyadic)> {
    let [lx, ly, lw] = &points[low as usize];
    let [hx, hy, hw] = &points[high as usize];
    Ok((
        hx.try_product(lw)?.try_subtract(lx.try_product(hw)?)?,
        hy.try_product(lw)?.try_subtract(ly.try_product(hw)?)?,
    ))
}

#[expect(
    clippy::indexing_slicing,
    reason = "offset dart endpoint IDs are created from the same exact point table"
)]
fn oriented_vector(
    points: &[[ExactDyadic; 3]],
    start: u32,
    end: u32,
) -> Result<(ExactDyadic, ExactDyadic)> {
    let [sx, sy, sw] = &points[start as usize];
    let [ex, ey, ew] = &points[end as usize];
    let mut dx = ex.try_product(sw)?.try_subtract(sx.try_product(ew)?)?;
    let mut dy = ey.try_product(sw)?.try_subtract(sy.try_product(ew)?)?;
    if (sw.sign() == ExactSign::Negative) ^ (ew.sign() == ExactSign::Negative) {
        dx = dx.neg();
        dy = dy.neg();
    }
    Ok((dx, dy))
}

fn orientation_with_direction(
    points: &[[ExactDyadic; 3]],
    start: u32,
    end: u32,
    direction: &[ExactDyadic; 2],
) -> Result<ExactSign> {
    let (dx, dy) = oriented_vector(points, start, end)?;
    let [query_dx, query_dy] = direction;
    Ok(dx
        .try_product(query_dy)?
        .try_subtract(dy.try_product(query_dx)?)?
        .sign())
}

fn compare_dart_direction(
    points: &[[ExactDyadic; 3]],
    origin: u32,
    target: u32,
    direction: &[ExactDyadic; 2],
) -> Result<Ordering> {
    let (dx, dy) = oriented_vector(points, origin, target)?;
    let [query_dx, query_dy] = direction;
    let vector_half = direction_half(&dx, &dy);
    let query_half = direction_half(query_dx, query_dy);
    match vector_half.cmp(&query_half) {
        Ordering::Equal => Ok(
            match dx
                .try_product(query_dy)?
                .try_subtract(dy.try_product(query_dx)?)?
                .sign()
            {
                ExactSign::Positive => Ordering::Less,
                ExactSign::Negative => Ordering::Greater,
                ExactSign::Zero => Ordering::Equal,
            },
        ),
        Ordering::Less => Ok(Ordering::Less),
        Ordering::Greater => Ok(Ordering::Greater),
    }
}

fn direction_half(dx: &ExactDyadic, dy: &ExactDyadic) -> u8 {
    u8::from(dy.sign() == ExactSign::Negative || (dy.is_zero() && dx.sign() == ExactSign::Negative))
}

const fn flip_if_negative_weight(sign: ExactSign, negative: bool) -> ExactSign {
    if !negative {
        return sign;
    }
    match sign {
        ExactSign::Negative => ExactSign::Positive,
        ExactSign::Zero => ExactSign::Zero,
        ExactSign::Positive => ExactSign::Negative,
    }
}
