#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use rstar::{AABB, RTreeNode, RTreeObject};

use crate::HeapSize;
use crate::geometry::*;

/// One segment as an R-tree leaf, carrying its position in the build slice
/// (so pair-once sweeps can keep `ordinal > i` candidates).
pub(crate) struct SegmentEntry {
    pub segment: Segment,
    pub ordinal: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for SegmentEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

pub(crate) fn segment_envelope(segment: Segment) -> AABB<[f64; 2]> {
    // Corners are ordered here — skip from_corners' second min/max pass.
    AABB::from_bounds(
        [
            segment.start.x.min(segment.end.x),
            segment.start.y.min(segment.end.y),
        ],
        [
            segment.start.x.max(segment.end.x),
            segment.start.y.max(segment.end.y),
        ],
    )
}

/// An R-tree over a segment set.
pub(crate) struct SegmentIndex {
    tree: BulkRTree<SegmentEntry>,
}

impl SegmentIndex {
    pub(crate) fn build(segments: &[Segment]) -> Self {
        Self::build_from_iter(segments.iter().copied())
    }

    /// Build straight from a segment iterator — entries are gathered once for
    /// the bulk load, with no caller-side `Vec<Segment>` staging.
    pub(crate) fn build_from_iter(segments: impl IntoIterator<Item = Segment>) -> Self {
        Self {
            tree: BulkRTree::bulk_load_with_params(
                segments
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, segment)| SegmentEntry {
                        segment,
                        ordinal,
                        envelope: segment_envelope(segment),
                    })
                    .collect(),
            ),
        }
    }

    /// The realizing segment of the minimum [`point_segment_distance_squared`]
    /// from `point` to any indexed segment that improves on `best`, with its
    /// squared distance — or `None` when no candidate beats `best`. `keep`
    /// filters which computed candidate distances participate (minimum
    /// clearance skips zero/endpoint contacts); pruning uses the running
    /// accepted minimum, so filtered-out zeros never poison the bound.
    /// Squared space — callers own the finite-range guarantee.
    pub(crate) fn nearest_segment_if(
        &self,
        point: Point,
        best: f64,
        keep: impl Fn(&Segment, f64) -> bool,
    ) -> Option<(Segment, f64)> {
        self.nearest_segment_ordinal_if(point, best, keep)
            .map(|(_, segment, distance)| (segment, distance))
    }

    pub(crate) fn nearest_segment_ordinal_if(
        &self,
        point: Point,
        mut best: f64,
        keep: impl Fn(&Segment, f64) -> bool,
    ) -> Option<(usize, Segment, f64)> {
        let query = [point.x, point.y];
        let mut witness = None;
        let mut stack: Vec<&RTreeNode<SegmentEntry>> = self.tree.root().children().iter().collect();
        while let Some(node) = stack.pop() {
            match node {
                RTreeNode::Parent(parent) => {
                    if parent.envelope().distance_2(&query) < best {
                        stack.extend(parent.children());
                    }
                },
                RTreeNode::Leaf(entry) => {
                    if entry.envelope.distance_2(&query) < best {
                        let distance = point_segment_distance_squared(point, entry.segment);
                        if distance < best && keep(&entry.segment, distance) {
                            best = distance;
                            witness = Some((entry.ordinal, entry.segment, distance));
                        }
                    }
                },
            }
        }
        witness
    }

    /// Indexed entries whose envelope intersects `query`'s — the noding /
    /// pair-sweep candidate set (each entry carries its build ordinal).
    pub(crate) fn intersecting_candidates(
        &self,
        query: Segment,
    ) -> impl Iterator<Item = &SegmentEntry> + '_ {
        self.tree
            .locate_in_envelope_intersecting(segment_envelope(query))
    }
}

/// An R-tree over a point set (Hausdorff refinement, clearance candidates).
pub(crate) struct PointSetIndex {
    tree: BulkRTree<[f64; 2]>,
}

impl PointSetIndex {
    pub(crate) fn build(points: impl IntoIterator<Item = Point>) -> Self {
        Self {
            tree: BulkRTree::bulk_load_with_params(
                points.into_iter().map(|point| [point.x, point.y]).collect(),
            ),
        }
    }

    /// Squared distance from `point` to its nearest indexed point
    /// (`INFINITY` for an empty set).
    pub(crate) fn nearest_distance_squared(&self, point: Point) -> f64 {
        self.tree
            .nearest_neighbor_with_distance_2([point.x, point.y])
            .map_or(f64::INFINITY, |(_, distance)| distance)
    }

    /// The nearest indexed point (INCLUDING a coincident one, distance 0) with
    /// its squared distance — the nearest-points/shortest-line query. `None`
    /// only for an empty set.
    pub(crate) fn nearest(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_with_distance_2([point.x, point.y])
            .map(|(coords, distance)| (Point::new_unchecked_xy(coords[0], coords[1]), distance))
    }

    /// The nearest indexed point that is not bit-identical to `point`, with
    /// its squared distance (`None` when none exists) — the
    /// minimum-clearance "nearest other vertex" query.
    pub(crate) fn nearest_other(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
            .find(|&(_, distance)| distance > 0.0)
            .map(|(coords, distance)| (Point::new_unchecked_xy(coords[0], coords[1]), distance))
    }
}

impl HeapSize for PointSetIndex {
    fn heap_bytes(&self) -> usize {
        fn node_bytes(node: &RTreeNode<[f64; 2]>) -> usize {
            match node {
                RTreeNode::Leaf(_) => std::mem::size_of::<RTreeNode<[f64; 2]>>(),
                RTreeNode::Parent(parent) => {
                    std::mem::size_of::<RTreeNode<[f64; 2]>>()
                        + parent.children().iter().map(node_bytes).sum::<usize>()
                },
            }
        }

        self.tree.root().children().iter().map(node_bytes).sum()
    }
}

/// Y-banded edge index over one ring: probes answer boundary + crossing
/// parity by scanning only the edges whose Y-span overlaps the probe's band,
/// instead of the whole ring. Build once per (polygon × point-batch) call.
///
/// Exactness: an edge contributes to a probe's ray-cast parity or boundary
/// verdict only if its Y-span contains the probe's Y, and every such edge is
/// in the probe's band by construction — so the per-band scan folds exactly
/// the terms the full single-pass scan folds, in ascending edge order.
pub(crate) struct RingRaycaster {
    min_y: f64,
    inv_band_height: f64,
    band_count: usize,
    /// CSR adjacency: edges of band `b` are
    /// `band_edges[band_offsets[b]..band_offsets[b + 1]]`, ascending.
    band_offsets: Vec<u32>,
    band_edges: Vec<u32>,
    xs: Vec<f64>,
    ys: Vec<f64>,
}

impl RingRaycaster {
    fn build(xs: &[f64], ys: &[f64]) -> Self {
        let edge_count = xs.len().saturating_sub(1);
        let (min_y, max_y) = ys
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &y| {
                (lo.min(y), hi.max(y))
            });
        let band_count = edge_count.max(1);
        let span = (max_y - min_y).max(f64::MIN_POSITIVE);
        let inv_band_height = band_count as f64 / span;
        let band_of = |y: f64| -> usize {
            // Clamped to the band range; the cast saturates negative to 0.
            (((y - min_y) * inv_band_height) as usize).min(band_count - 1)
        };
        // Two-pass CSR fill: count band occupancy, then place edge ids.
        let mut band_offsets = vec![0_u32; band_count + 1];
        let edge_bands = |edge: usize| {
            let (a, b) = (ys[edge], ys[edge + 1]);
            band_of(a.min(b))..=band_of(a.max(b))
        };
        for edge in 0..edge_count {
            for band in edge_bands(edge) {
                band_offsets[band + 1] += 1;
            }
        }
        for band in 0..band_count {
            band_offsets[band + 1] += band_offsets[band];
        }
        let mut cursor = band_offsets.clone();
        let mut band_edges = vec![0_u32; band_offsets[band_count] as usize];
        for edge in 0..edge_count {
            for band in edge_bands(edge) {
                band_edges[cursor[band] as usize] = edge as u32;
                cursor[band] += 1;
            }
        }
        Self {
            min_y,
            inv_band_height,
            band_count,
            band_offsets,
            band_edges,
            xs: xs.to_vec(),
            ys: ys.to_vec(),
        }
    }

    /// The fused boundary+parity classification over the probe's band only.
    fn classify(&self, x: f64, y: f64) -> RingClass {
        let band = (((y - self.min_y) * self.inv_band_height) as usize).min(self.band_count - 1);
        let start = self.band_offsets[band] as usize;
        let end = self.band_offsets[band + 1] as usize;
        let mut inside = false;
        for &edge in &self.band_edges[start..end] {
            let edge = edge as usize;
            let (ax, ay, bx, by) = (
                self.xs[edge],
                self.ys[edge],
                self.xs[edge + 1],
                self.ys[edge + 1],
            );
            if x >= ax.min(bx)
                && x <= ax.max(bx)
                && y >= ay.min(by)
                && y <= ay.max(by)
                && orientation_xy(ax, ay, bx, by, x, y) == Orientation::Collinear
            {
                return RingClass::Boundary;
            }
            // Sign-form crossing decision (exact; the cross-multiplied form
            // overflowed to `inf` vs `inf` at extreme finite coordinates and
            // silently dropped genuine crossings — same class as the ring
            // kernels' old division form).
            if (ay > y) != (by > y) && ray_crossing_is_right(ax, ay, bx, by, x, y) {
                inside = !inside;
            }
        }
        if inside {
            RingClass::Interior
        } else {
            RingClass::Exterior
        }
    }
}

/// Batched point-membership tester for a fixed shape: polygonal shapes get
/// per-ring [`RingRaycaster`]s (built once, probes cost the band scan);
/// everything else falls through to the point kernels.
pub(crate) enum PointBatchTester {
    Polygons(Vec<PolygonRaycaster>),
    Generic(Shape),
}

/// One polygon's raycasters: shell plus holes, with the polygon bounds gate.
pub(crate) struct PolygonRaycaster {
    bounds: Option<Bounds>,
    shell: RingRaycaster,
    holes: Vec<RingRaycaster>,
}

impl PolygonRaycaster {
    fn build(polygon: &Polygon) -> Self {
        let ring = |coords: &CoordSeq| RingRaycaster::build(coords.xs(), coords.ys());
        Self {
            bounds: Bounds::from_coords(polygon.shell.coords()),
            shell: ring(polygon.shell.coords()),
            holes: polygon
                .holes
                .iter()
                .map(|hole| ring(hole.coords()))
                .collect(),
        }
    }

    fn contains(&self, point: Point) -> bool {
        self.in_bounds(point)
            && self.shell.classify(point.x, point.y) == RingClass::Interior
            && !self
                .holes
                .iter()
                .any(|hole| hole.classify(point.x, point.y) != RingClass::Exterior)
    }

    fn covers(&self, point: Point) -> bool {
        self.in_bounds(point)
            && self.shell.classify(point.x, point.y) != RingClass::Exterior
            && !self
                .holes
                .iter()
                .any(|hole| hole.classify(point.x, point.y) == RingClass::Interior)
    }

    /// Three-way membership against this one polygon: `Boundary` if the point
    /// lies on the shell or any hole ring, `Interior` if strictly inside the
    /// shell and strictly outside every hole, else `Exterior`. Bounds-gate
    /// only rules out `Exterior`, never boundary/interior.
    fn classify(&self, point: Point) -> RingClass {
        if !self.in_bounds(point) {
            return RingClass::Exterior;
        }
        match self.shell.classify(point.x, point.y) {
            RingClass::Exterior => RingClass::Exterior,
            RingClass::Boundary => RingClass::Boundary,
            RingClass::Interior => {
                let mut on_hole = false;
                for hole in &self.holes {
                    match hole.classify(point.x, point.y) {
                        RingClass::Interior => return RingClass::Exterior,
                        RingClass::Boundary => on_hole = true,
                        RingClass::Exterior => {},
                    }
                }
                if on_hole {
                    RingClass::Boundary
                } else {
                    RingClass::Interior
                }
            },
        }
    }

    fn in_bounds(&self, point: Point) -> bool {
        self.bounds.is_some_and(|bounds| {
            point.x >= bounds.minx()
                && point.x <= bounds.maxx()
                && point.y >= bounds.miny()
                && point.y <= bounds.maxy()
        })
    }
}

impl PointBatchTester {
    /// Probe count past which building the band index beats per-probe ring
    /// scans.
    pub(crate) const MIN_PROBES: usize = 64;

    pub(crate) fn new(shape: &Shape) -> Self {
        match shape {
            Shape::Polygon(polygon) => Self::Polygons(vec![PolygonRaycaster::build(polygon)]),
            Shape::MultiPolygon(polygons) => {
                Self::Polygons(polygons.iter().map(PolygonRaycaster::build).collect())
            },
            _ => Self::Generic(shape.clone()),
        }
    }

    /// Strict membership — [`Shape::contains_point`] semantics.
    pub(crate) fn contains_point(&self, point: Point) -> bool {
        match self {
            Self::Polygons(polygons) => polygons.iter().any(|polygon| polygon.contains(point)),
            Self::Generic(shape) => shape.contains_point(point),
        }
    }

    /// Boundary-inclusive membership — [`Shape::covers_point`] semantics.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygons(polygons) => polygons.iter().any(|polygon| polygon.covers(point)),
            Self::Generic(shape) => shape.covers_point(point),
        }
    }

    /// Three-way areal membership: `Interior` (strictly inside the area),
    /// `Boundary` (on a ring), or `Exterior`. For a `Polygons` tester this is
    /// the banded-raycaster fusion of `contains`/`covers` — strictly interior
    /// to ANY part wins over a boundary hit on another, matching the union
    /// semantics of the per-point ring scans it replaces. `Generic` testers
    /// have no prepared area, so callers must not route those here.
    pub(crate) fn classify_area_point(&self, point: Point) -> Option<RingClass> {
        match self {
            Self::Polygons(polygons) => {
                let mut on_boundary = false;
                for polygon in polygons {
                    match polygon.classify(point) {
                        RingClass::Interior => return Some(RingClass::Interior),
                        RingClass::Boundary => on_boundary = true,
                        RingClass::Exterior => {},
                    }
                }
                Some(if on_boundary {
                    RingClass::Boundary
                } else {
                    RingClass::Exterior
                })
            },
            Self::Generic(_) => None,
        }
    }
}

impl HeapSize for PointBatchTester {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Polygons(polygons) => polygons.heap_bytes(),
            Self::Generic(shape) => shape.coordinate_bytes(),
        }
    }
}

impl HeapSize for PolygonRaycaster {
    fn heap_bytes(&self) -> usize {
        self.shell.heap_bytes() + self.holes.heap_bytes()
    }
}

impl HeapSize for RingRaycaster {
    fn heap_bytes(&self) -> usize {
        self.band_offsets.heap_bytes()
            + self.band_edges.heap_bytes()
            + self.xs.heap_bytes()
            + self.ys.heap_bytes()
    }
}
