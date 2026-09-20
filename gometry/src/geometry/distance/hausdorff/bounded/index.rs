//! Spatial index over the support primitives.
//!
//! Branch and bound needs the nearest candidates first and the separated
//! ones refused outright; the index answers both from one node tree built
//! over the boundary edges.

use super::{
    BinaryHeap, ExactPoint, ExactSign, FloatBox, HULL_SEPARATION_LIMIT, NEAREST_CANDIDATE_LIMIT,
    NOMINATED_SITE_COUNT, PreparedSupport, Rational, Result, SquaredBounds, SupportSpan, Tightness,
    XY, box_distance_lower, exact, line_filter_enabled, nonnegative_upper_bound,
    point_segment_squared_f64, span_point, supporting_line_lower_squared, try_push_vec,
    try_reserve_vec,
};

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

#[derive(Clone, Copy, Debug)]
pub(super) struct IndexNode {
    pub(super) bounds: FloatBox,
    pub(super) left: Option<usize>,
    pub(super) right: Option<usize>,
    pub(super) start: usize,
    pub(super) end: usize,
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
        let item_count = spans
            .len()
            .checked_add(points.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut items = Vec::new();
        try_reserve_vec(&mut items, item_count)?;
        for (index, span) in spans.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Span(index),
                bounds: span.bounds,
            })?;
        }
        for (index, point) in points.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Point(index),
                bounds: FloatBox::from_point(point)?,
            })?;
        }
        let mut order = Vec::new();
        try_reserve_vec(&mut order, items.len())?;
        for index in 0..items.len() {
            try_push_vec(&mut order, index)?;
        }
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut index = Self {
            items,
            order,
            nodes: Vec::new(),
            root: None,
        };
        try_reserve_vec(&mut index.nodes, node_capacity)?;
        if !index.items.is_empty() {
            index.root = Some(index.build_range(0, index.order.len())?);
        }

        Ok(index)
    }

    pub(super) fn build_range(&mut self, start: usize, end: usize) -> Result<usize> {
        let bounds = self.range_bounds(start, end);
        if end.saturating_sub(start) <= 4 {
            let node = IndexNode {
                bounds,
                left: None,
                right: None,
                start,
                end,
            };
            let index = self.nodes.len();
            try_push_vec(&mut self.nodes, node)?;
            return Ok(index);
        }

        let split_x = (bounds.max_x - bounds.min_x) >= (bounds.max_y - bounds.min_y);
        self.order[start..end].sort_unstable_by(|left, right| {
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
        let middle = start + (end - start) / 2;
        let left = self.build_range(start, middle)?;
        let right = self.build_range(middle, end)?;
        let index = self.nodes.len();
        try_push_vec(&mut self.nodes, IndexNode {
            bounds,
            left: Some(left),
            right: Some(right),
            start: 0,
            end: 0,
        })?;
        Ok(index)
    }

    pub(super) fn range_bounds(&self, start: usize, end: usize) -> FloatBox {
        let first = self.items[self.order[start]].bounds;
        self.order[start + 1..end]
            .iter()
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
    ) -> Result<SquaredBounds> {
        let Some(root) = self.root else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        };
        let point_bounds = FloatBox::from_point(point)?;
        let mut best: Option<SquaredBounds> = None;
        let mut stack = Vec::new();
        try_push_vec(&mut stack, root)?;
        while let Some(node_index) = stack.pop() {
            let best_upper = best.as_ref().map_or(f64::INFINITY, |best| best.upper_f64);
            let node = self.nodes[node_index];
            if box_distance_lower(point_bounds, node.bounds) > best_upper {
                continue;
            }
            if let (Some(left), Some(right)) = (node.left, node.right) {
                // Pop the nearer child first.
                let left_lower = box_distance_lower(point_bounds, self.nodes[left].bounds);
                let right_lower = box_distance_lower(point_bounds, self.nodes[right].bounds);
                let (first, second) = if left_lower <= right_lower {
                    (left, right)
                } else {
                    (right, left)
                };
                try_push_vec(&mut stack, second)?;
                try_push_vec(&mut stack, first)?;
                continue;
            }
            for order_index in &self.order[node.start..node.end] {
                let item = self.items[*order_index];
                let best_upper = best.as_ref().map_or(f64::INFINITY, |best| best.upper_f64);
                if box_distance_lower(point_bounds, item.bounds) > best_upper {
                    continue;
                }
                if line_filter_enabled()
                    && let PrimitiveId::Span(index) = item.primitive
                    && supporting_line_lower_squared(point_bounds, &support.spans[index])
                        > best_upper
                {
                    continue;
                }
                let bounds = support.primitive_squared_bounds(
                    item.primitive,
                    point,
                    point_bounds,
                    tightness,
                )?;
                best = Some(match best {
                    None => bounds,
                    Some(current) => current.min(bounds)?,
                });
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
    pub(super) fn nominate(
        &self,
        support: &PreparedSupport,
        point: &ExactPoint,
    ) -> Result<Vec<PrimitiveId>> {
        let mut nominated: Vec<(f64, PrimitiveId)> = Vec::new();
        try_reserve_vec(&mut nominated, NOMINATED_SITE_COUNT + 1)?;
        let Some(root) = self.root else {
            return Ok(Vec::new());
        };
        let (x, y) = (point.x.round()?, point.y.round()?);
        let query = FloatBox {
            min_x: x,
            max_x: x,
            min_y: y,
            max_y: y,
        };
        let mut frontier = BinaryHeap::new();
        frontier.try_reserve(self.nodes.len())?;
        let lower = |bounds| box_distance_lower(query, bounds).to_bits();
        frontier.push(std::cmp::Reverse((lower(self.nodes[root].bounds), root)));
        while let Some(std::cmp::Reverse((lower_bits, node_index))) = frontier.pop() {
            if nominated.len() == NOMINATED_SITE_COUNT
                && f64::from_bits(lower_bits) > nominated[NOMINATED_SITE_COUNT - 1].0
            {
                break;
            }
            let node = self.nodes[node_index];
            if let (Some(left), Some(right)) = (node.left, node.right) {
                for child in [left, right] {
                    frontier.push(std::cmp::Reverse((lower(self.nodes[child].bounds), child)));
                }
                continue;
            }
            for order_index in &self.order[node.start..node.end] {
                let item = self.items[*order_index];
                let squared = match item.primitive {
                    PrimitiveId::Span(index) => {
                        let source = support.spans[index].source;
                        point_segment_squared_f64(XY::new(x, y), source.start, source.end)
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
        let mut primitives = Vec::new();
        try_reserve_vec(&mut primitives, nominated.len())?;
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
        let mut candidates = Vec::new();
        try_reserve_vec(&mut candidates, NEAREST_CANDIDATE_LIMIT)?;
        let mut stack = Vec::new();
        try_push_vec(&mut stack, root)?;
        while let Some(node_index) = stack.pop() {
            let node = self.nodes[node_index];
            if box_distance_lower(bounds, node.bounds) > threshold {
                continue;
            }
            if let (Some(left), Some(right)) = (node.left, node.right) {
                try_push_vec(&mut stack, left)?;
                try_push_vec(&mut stack, right)?;
                continue;
            }
            for order_index in &self.order[node.start..node.end] {
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
        let mut hull_points = Vec::new();
        try_reserve_vec(&mut hull_points, 4 * candidates.len())?;
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
pub(super) fn forms_monotone_chain(
    support: &PreparedSupport,
    candidates: &[IndexItem],
) -> Result<bool> {
    let mut spans = Vec::new();
    let mut ends = Vec::new();
    try_reserve_vec(&mut spans, candidates.len())?;
    try_reserve_vec(&mut ends, candidates.len())?;
    for item in candidates {
        let PrimitiveId::Span(index) = item.primitive else {
            return Ok(false);
        };
        let span = &support.spans[index];
        spans.push(span);
        ends.push([span_point(span, &span.t_lo)?, span_point(span, &span.t_hi)?]);
    }
    // `links[i][end]` is the span sharing endpoint `end` (0 at `t_lo`) of span
    // `i`; an endpoint shared three ways is a branch, not a chain.
    let mut links = Vec::new();
    try_reserve_vec(&mut links, ends.len())?;
    links.resize(ends.len(), [None::<usize>; 2]);
    for first in 0..ends.len() {
        for second in first + 1..ends.len() {
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
    let mut walk = Vec::new();
    try_reserve_vec(&mut walk, ends.len())?;
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
            let left = &spans[first].carrier;
            let right = &spans[second].carrier;
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
