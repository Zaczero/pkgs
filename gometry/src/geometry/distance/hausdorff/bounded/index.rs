//! Spatial index over the support primitives.
//!
//! Branch and bound needs the nearest candidates first and the separated
//! ones refused outright; the index answers both from one node tree built
//! over the boundary edges.

use super::{
    BinaryHeap, ExactPoint, ExactSign, FloatBox, HULL_SEPARATION_LIMIT, NEAREST_CANDIDATE_LIMIT,
    NOMINATED_SITE_COUNT, PreparedSupport, Rational, Result, SelectedSpan, SquaredBounds,
    SupportSpan, Tightness, XY, box_distance_lower, exact, line_filter_enabled, nonnegative_bounds,
    nonnegative_upper_bound, point_segment_squared_f64, span_point, supporting_line_lower_squared,
};
use crate::scratch_pool::ScratchPool;

thread_local! {
    static NEAREST_STACK: ScratchPool<Vec<usize>> = const { ScratchPool::new(Vec::new()) };
    static NEAREST_FRONTIER: ScratchPool<Vec<(usize, f64)>> = const { ScratchPool::new(Vec::new()) };
}

#[derive(Debug)]
pub(super) struct BoundaryEdge {
    pub(super) start: ExactPoint,
    pub(super) end: ExactPoint,
    /// Outward binary64 bounds, which a box must meet to hold a point of the
    /// edge.
    pub(super) bounds: FloatBox,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PrimitiveId {
    Span(usize),
    Point(usize),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct IndexItem {
    pub(super) primitive: PrimitiveId,
    pub(super) bounds: FloatBox,
}

pub(super) struct NearestSupport<B = SquaredBounds> {
    /// The smallest-upper-bound primitive; exact nearest when bounds are exact.
    pub(super) primitive: PrimitiveId,
    pub(super) bounds: B,
}

/// Approximate squared distance for candidate ranking, restricted to the
/// span's selected parameter window: a noded subspan must not rank by its
/// whole carrier, or every span sharing a carrier ties and nomination stalls.
fn span_nomination_squared(point: XY, span: &SupportSpan) -> Result<f64> {
    let window = match span.range.as_deref() {
        Some(SelectedSpan { parameters, .. }) => (parameters[0].round()?, parameters[1].round()?),
        None => (0.0, 1.0),
    };
    Ok(point_segment_squared_f64(
        point,
        span.source.start,
        span.source.end,
        window,
    ))
}

trait NearestBounds: Sized {
    const FILTER_LINES: bool;
    fn upper(&self) -> f64;
    fn merge_nearest(self, other: Self) -> Result<(Self, bool)>;
}

impl NearestBounds for SquaredBounds {
    const FILTER_LINES: bool = true;
    fn upper(&self) -> f64 {
        self.upper_f64
    }

    fn merge_nearest(self, other: Self) -> Result<(Self, bool)> {
        self.min_with_upper_source(other)
    }
}

impl NearestBounds for (f64, f64) {
    const FILTER_LINES: bool = false;
    fn upper(&self) -> f64 {
        self.1
    }

    fn merge_nearest(self, other: Self) -> Result<(Self, bool)> {
        Ok(((self.0.min(other.0), self.1.min(other.1)), other.1 < self.1))
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct IndexNode {
    pub(super) bounds: FloatBox,
    children: IndexChildren,
}

#[derive(Clone, Copy, Debug)]
enum IndexChildren {
    Leaf { start: usize, end: usize },
    Branch { left: usize, right: usize },
}

#[derive(Debug, Default)]
pub(super) struct SupportIndex {
    pub(super) items: Vec<IndexItem>,
    pub(super) order: Vec<usize>,
    pub(super) nodes: Vec<IndexNode>,
    pub(super) root: Option<usize>,
}

impl SupportIndex {
    pub(super) fn build(spans: &[SupportSpan], points: &[ExactPoint]) -> Result<Self> {
        let item_count = spans.len().wrapping_add(points.len());
        let mut items = Vec::with_capacity(item_count);
        for (index, span) in spans.iter().enumerate() {
            items.push(IndexItem {
                primitive: PrimitiveId::Span(index),
                bounds: span.bounds,
            });
        }
        for (index, point) in points.iter().enumerate() {
            items.push(IndexItem {
                primitive: PrimitiveId::Point(index),
                bounds: FloatBox::from_point(point)?,
            });
        }
        Ok(Self::from_items(items))
    }

    fn from_items(items: Vec<IndexItem>) -> Self {
        let mut order = Vec::with_capacity(items.len());
        order.extend(0..items.len());
        // Splitting a range of at least five leaves at least two items per
        // terminal node, so the binary tree never has more nodes than items.
        let node_capacity = items.len();
        let mut index = Self {
            items,
            order,
            nodes: Vec::with_capacity(node_capacity),
            root: None,
        };
        if !index.items.is_empty() {
            index.root = Some(index.build_range(0, index.order.len()));
        }

        index
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "order/item indices and child ranges are created from the same immutable SupportIndex vectors"
    )]
    pub(super) fn build_range(&mut self, start: usize, end: usize) -> usize {
        let bounds = self.range_bounds(start, end);
        if end.saturating_sub(start) <= 4 {
            let node = IndexNode {
                bounds,
                children: IndexChildren::Leaf { start, end },
            };
            let index = self.nodes.len();
            self.nodes.push(node);
            return index;
        }

        let split_x = (bounds.max_x - bounds.min_x) >= (bounds.max_y - bounds.min_y);
        let middle = start.wrapping_add(end.wrapping_sub(start) / 2);
        self.order[start..end].select_nth_unstable_by(middle.wrapping_sub(start), |left, right| {
            let left_bounds = self.items[*left].bounds;
            let right_bounds = self.items[*right].bounds;
            let left_key = if split_x {
                f64::midpoint(left_bounds.min_x, left_bounds.max_x)
            } else {
                f64::midpoint(left_bounds.min_y, left_bounds.max_y)
            };
            let right_key = if split_x {
                f64::midpoint(right_bounds.min_x, right_bounds.max_x)
            } else {
                f64::midpoint(right_bounds.min_y, right_bounds.max_y)
            };
            left_key.total_cmp(&right_key).then_with(|| left.cmp(right))
        });
        let left = self.build_range(start, middle);
        let right = self.build_range(middle, end);
        let index = self.nodes.len();
        self.nodes.push(IndexNode {
            bounds,
            children: IndexChildren::Branch { left, right },
        });
        index
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "tree construction supplies a nonempty half-open range into the index's own order and item vectors"
    )]
    pub(super) fn range_bounds(&self, start: usize, end: usize) -> FloatBox {
        let first = self.items[self.order[start]].bounds;
        self.order[start..end]
            .iter()
            .skip(1)
            .fold(first, |bounds, index| {
                bounds.include(self.items[*index].bounds)
            })
    }

    /// Bounds on the squared distance from `point` to the nearest primitive.
    /// With `tightness`, primitives may be bounded in double-double
    /// intervals; without it every evaluation is exact and both bounds equal
    /// the exact distance.
    pub(super) fn nearest(
        &self,
        support: &PreparedSupport,
        point: &ExactPoint,
        tightness: Option<Tightness>,
    ) -> Result<NearestSupport> {
        let point_bounds = FloatBox::from_point(point)?;
        if let Some(tightness) = tightness {
            let nearest = self.nearest_with(support, point_bounds, |primitive| {
                if let Some(bounds) =
                    support.primitive_interval_bounds(primitive, point_bounds, tightness)
                {
                    return Ok(bounds);
                }
                let exact = support
                    .primitive_distance_squared(primitive, point)?
                    .to_rational()?;
                nonnegative_bounds(&exact)
            })?;
            let (lower, upper) = nearest.bounds;
            if lower.is_finite() && upper.is_finite() && tightness.admits(lower, upper) {
                return Ok(NearestSupport {
                    primitive: nearest.primitive,
                    bounds: SquaredBounds::enclosing(lower.max(0.0), upper),
                });
            }
        }
        self.nearest_with(support, point_bounds, |primitive| {
            support.primitive_squared_bounds(primitive, point, point_bounds, None)
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "root, branch, leaf, and primitive IDs come from this immutable SupportIndex and its PreparedSupport"
    )]
    fn nearest_with<B: NearestBounds>(
        &self,
        support: &PreparedSupport,
        point_bounds: FloatBox,
        mut evaluate: impl FnMut(PrimitiveId) -> Result<B>,
    ) -> Result<NearestSupport<B>> {
        let Some(root) = self.root else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        };
        let mut best: Option<NearestSupport<B>> = None;
        let mut stack = ScratchPool::checkout(&NEAREST_FRONTIER);
        stack.push((
            root,
            box_distance_lower(point_bounds, self.nodes[root].bounds),
        ));
        while let Some((node_index, node_lower)) = stack.pop() {
            let best_upper = best
                .as_ref()
                .map_or(f64::INFINITY, |best| best.bounds.upper());
            let node = self.nodes[node_index];
            if node_lower > best_upper {
                continue;
            }
            let (start, end) = match node.children {
                IndexChildren::Branch { left, right } => {
                    // Pop the nearer child first.
                    let left_lower = box_distance_lower(point_bounds, self.nodes[left].bounds);
                    let right_lower = box_distance_lower(point_bounds, self.nodes[right].bounds);
                    let (first, second) = if left_lower <= right_lower {
                        ((left, left_lower), (right, right_lower))
                    } else {
                        ((right, right_lower), (left, left_lower))
                    };
                    stack.push(second);
                    stack.push(first);
                    continue;
                },
                IndexChildren::Leaf { start, end } => (start, end),
            };
            for order_index in &self.order[start..end] {
                let item = self.items[*order_index];
                let best_upper = best
                    .as_ref()
                    .map_or(f64::INFINITY, |best| best.bounds.upper());
                if box_distance_lower(point_bounds, item.bounds) > best_upper {
                    continue;
                }
                if B::FILTER_LINES
                    && line_filter_enabled()
                    && let PrimitiveId::Span(index) = item.primitive
                    && supporting_line_lower_squared(point_bounds, &support.spans[index])
                        > best_upper
                {
                    continue;
                }
                let bounds = evaluate(item.primitive)?;
                let (bounds, primitive) = match best {
                    None => (bounds, item.primitive),
                    Some(current) => {
                        let (bounds, other_is_upper_minimum) =
                            current.bounds.merge_nearest(bounds)?;
                        let primitive = if other_is_upper_minimum {
                            item.primitive
                        } else {
                            current.primitive
                        };
                        (bounds, primitive)
                    },
                };
                best = Some(NearestSupport { primitive, bounds });
            }
        }
        best.ok_or_else(|| {
            crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            )
        })
    }

    /// The primitives nearest `point`, ranked best-first by binary64
    /// distance from its rounded position. Nomination only chooses which
    /// exact sites bound a patch, so an approximate ranking costs tightness,
    /// never soundness.
    #[expect(
        clippy::indexing_slicing,
        reason = "frontier nodes, leaf ranges, and primitive IDs come from this immutable SupportIndex and its PreparedSupport"
    )]
    pub(super) fn nominate(
        &self,
        support: &PreparedSupport,
        point: &ExactPoint,
    ) -> Result<Vec<PrimitiveId>> {
        let mut nominated: Vec<(f64, PrimitiveId)> =
            Vec::with_capacity(NOMINATED_SITE_COUNT.wrapping_add(1));
        let Some(root) = self.root else {
            return Ok(Vec::new());
        };
        let XY { x, y } = point.rounded_xy()?;
        let query = FloatBox {
            min_x: x,
            max_x: x,
            min_y: y,
            max_y: y,
        };
        // Pending subtrees are disjoint and each contains at least one leaf.
        let mut frontier = BinaryHeap::with_capacity(self.nodes.len().div_ceil(2));
        let lower = |bounds| box_distance_lower(query, bounds).to_bits();
        frontier.push(std::cmp::Reverse((lower(self.nodes[root].bounds), root)));
        while let Some(std::cmp::Reverse((lower_bits, node_index))) = frontier.pop() {
            if nominated.len() == NOMINATED_SITE_COUNT
                && f64::from_bits(lower_bits) > nominated[NOMINATED_SITE_COUNT - 1].0
            {
                break;
            }
            let node = self.nodes[node_index];
            let (start, end) = match node.children {
                IndexChildren::Branch { left, right } => {
                    for child in [left, right] {
                        frontier.push(std::cmp::Reverse((lower(self.nodes[child].bounds), child)));
                    }
                    continue;
                },
                IndexChildren::Leaf { start, end } => (start, end),
            };
            for order_index in &self.order[start..end] {
                let item = self.items[*order_index];
                let squared = match item.primitive {
                    PrimitiveId::Span(index) => {
                        span_nomination_squared(XY::new(x, y), &support.spans[index])?
                    },
                    PrimitiveId::Point(_) => box_distance_lower(query, item.bounds),
                };
                let squared = if squared.is_nan() {
                    f64::INFINITY
                } else {
                    squared
                };
                let rank = nominated.partition_point(|&(other, _)| other <= squared);
                if rank < NOMINATED_SITE_COUNT {
                    nominated.insert(rank, (squared, item.primitive));
                    nominated.truncate(NOMINATED_SITE_COUNT);
                }
            }
        }
        let mut primitives = Vec::with_capacity(nominated.len());
        primitives.extend(nominated.into_iter().map(|(_, primitive)| primitive));
        Ok(primitives)
    }

    /// Whether no point of `bounds` can be a local maximum of the distance
    /// to this support. Such a maximum lies in the convex hull of its nearest
    /// points, and a primitive can be nearest somewhere in the box only when
    /// its conservative lower bound does not exceed `site_squared`, an upper
    /// bound on the squared distance to the support over the box. One such
    /// candidate, a monotone chain of them, or a box strictly separated from
    /// the hull of all of them excludes a maximum.
    #[expect(
        clippy::indexing_slicing,
        reason = "stack nodes, leaf ranges, and primitive IDs come from this immutable SupportIndex and its PreparedSupport"
    )]
    pub(super) fn excludes_local_maximum(
        &self,
        support: &PreparedSupport,
        bounds: FloatBox,
        site_squared: &Rational,
    ) -> Result<bool> {
        let Some(root) = self.root else {
            return Ok(false);
        };
        let threshold = nonnegative_upper_bound(site_squared)?;
        if !threshold.is_finite() {
            return Ok(false);
        }
        let mut candidates = Vec::with_capacity(NEAREST_CANDIDATE_LIMIT);
        let mut stack = Vec::new();
        stack.push(root);
        while let Some(node_index) = stack.pop() {
            let node = self.nodes[node_index];
            if box_distance_lower(bounds, node.bounds) > threshold {
                continue;
            }
            let (start, end) = match node.children {
                IndexChildren::Branch { left, right } => {
                    stack.push(left);
                    stack.push(right);
                    continue;
                },
                IndexChildren::Leaf { start, end } => (start, end),
            };
            for order_index in &self.order[start..end] {
                let item = self.items[*order_index];
                if box_distance_lower(bounds, item.bounds) > threshold {
                    continue;
                }
                if let PrimitiveId::Span(index) = item.primitive
                    && supporting_line_lower_squared(bounds, &support.spans[index]) > threshold
                {
                    continue;
                }
                if candidates.len() == NEAREST_CANDIDATE_LIMIT {
                    return Ok(false);
                }
                candidates.push(item);
            }
        }
        if candidates.len() <= 1 || forms_monotone_chain(support, &candidates)? {
            return Ok(true);
        }
        if candidates.len() > HULL_SEPARATION_LIMIT {
            return Ok(false);
        }
        // Whole source segments and outward point boxes enclose the
        // candidates, so separating the box from their hull suffices.
        let mut hull_points = Vec::with_capacity(candidates.len().wrapping_mul(4));
        for item in &candidates {
            match item.primitive {
                PrimitiveId::Span(index) => {
                    let source = support.spans[index].source;
                    hull_points.push(source.start);
                    hull_points.push(source.end);
                },
                PrimitiveId::Point(_) => {
                    hull_points.extend(item.bounds.corners().map(|(x, y)| XY::new(x, y)));
                },
            }
        }
        Ok(box_separated_from_points(bounds, &hull_points))
    }
}

/// Whether the candidates are spans forming one open chain whose directions,
/// taken along the chain, lie in an open half-plane. Such a chain is strictly
/// monotone in some direction `w`, and it cannot touch an empty disk on both
/// sides of the center: past its lowest touch point it rises in `w`, which
/// enters the disk, and between touch points it cannot cross the disk to the
/// other side.
#[expect(
    clippy::indexing_slicing,
    reason = "span ordinals and two-end neighbor links are built from the same candidate slice and traversed as a keyed path"
)]
pub(super) fn forms_monotone_chain(
    support: &PreparedSupport,
    candidates: &[IndexItem],
) -> Result<bool> {
    let mut spans = Vec::with_capacity(candidates.len());
    let mut ends = Vec::with_capacity(candidates.len());
    for item in candidates {
        let PrimitiveId::Span(index) = item.primitive else {
            return Ok(false);
        };
        let span = &support.spans[index];
        spans.push(span);
        ends.push([span_point(span, span.lo())?, span_point(span, span.hi())?]);
    }
    // `links[i][end]` is the span sharing endpoint `end` (0 at `t_lo`) of span
    // `i`; an endpoint shared three ways is a branch, not a chain.
    let mut links = vec![[None::<usize>; 2]; ends.len()];
    for first in 0..ends.len() {
        for second in first.wrapping_add(1)..ends.len() {
            for first_end in 0..2 {
                for second_end in 0..2 {
                    if ends[first][first_end].same_position(&ends[second][second_end])? {
                        if links[first][first_end].is_some() || links[second][second_end].is_some()
                        {
                            return Ok(false);
                        }
                        links[first][first_end] = Some(second);
                        links[second][second_end] = Some(first);
                    }
                }
            }
        }
    }
    let Some(start) = (0..ends.len()).find(|&index| links[index].contains(&None)) else {
        return Ok(false);
    };
    // Walk from the free end, recording each span with whether the walk runs
    // against its `t_lo -> t_hi` carrier.
    let mut walk = Vec::with_capacity(ends.len());
    let mut current = start;
    let mut reversed = links[start][0].is_some();
    loop {
        walk.push((current, reversed));
        let exit = usize::from(!reversed);
        let Some(next) = links[current][exit] else {
            break;
        };
        if walk.len() == ends.len() || links[next][0] == links[next][1] {
            return Ok(false);
        }
        reversed = links[next][1] == Some(current);
        current = next;
    }
    if walk.len() != ends.len() {
        return Ok(false);
    }
    // In an open half-plane exactly when some direction has every other one
    // strictly counterclockwise within a half turn, or equal to it.
    for &(first, first_reversed) in &walk {
        let mut leads = true;
        for &(second, second_reversed) in &walk {
            let left = spans[first].carrier()?;
            let right = spans[second].carrier()?;
            let flip = first_reversed != second_reversed;
            let cross = oriented_sign(
                left.dx
                    .try_product(&right.dy)?
                    .try_subtract(&left.dy.try_product(&right.dx)?)?
                    .sign(),
                flip,
            );
            let dot = oriented_sign(
                left.dx
                    .try_product(&right.dx)?
                    .try_add(&left.dy.try_product(&right.dy)?)?
                    .sign(),
                flip,
            );
            if !(cross == ExactSign::Positive
                || (cross == ExactSign::Zero && dot == ExactSign::Positive))
            {
                leads = false;
                break;
            }
        }
        if leads {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) const fn oriented_sign(sign: ExactSign, flip: bool) -> ExactSign {
    match (sign, flip) {
        (ExactSign::Positive, true) => ExactSign::Negative,
        (ExactSign::Negative, true) => ExactSign::Positive,
        (sign, _) => sign,
    }
}

/// Whether a line strictly separates the box from the convex hull of
/// `points`. Two disjoint convex polygons are separated by a line through an
/// edge of one of them, so the box axes and the lines through pairs of
/// points are the only candidates.
pub(super) fn box_separated_from_points(bounds: FloatBox, points: &[XY]) -> bool {
    if points.iter().all(|point| point.x > bounds.max_x)
        || points.iter().all(|point| point.x < bounds.min_x)
        || points.iter().all(|point| point.y > bounds.max_y)
        || points.iter().all(|point| point.y < bounds.min_y)
    {
        return true;
    }
    let corners = bounds.corners().map(|(x, y)| XY::new(x, y));
    points.iter().any(|&start| {
        points.iter().any(|&end| {
            start != end
                && points
                    .iter()
                    .all(|&point| exact::orient2d(start, end, point) != ExactSign::Negative)
                && corners
                    .iter()
                    .all(|&corner| exact::orient2d(start, end, corner) == ExactSign::Negative)
        })
    })
}
