//! Leftward ray contacts over exact points.
//!
//! A cyclic walk needs to know which span lies immediately left of a
//! component's lexicographic minimum. [`RayPoints`] abstracts over the two
//! point sources that question is asked of -- the partition's exact arena
//! during construction, and the rounded items at emission -- so the sweep
//! itself is written once.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use super::Axis;
use super::construction::PointArena;
#[cfg(test)]
use super::metrics::update as update_partition_metrics;
use crate::geometry::arrangement::compare_departures;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{stored_orientation_sign, try_sort_by};
use crate::geometry::tessellation::exact::{Interval, compare_ratios};
use crate::geometry::{GeometryErrorKind, OrderTree, Result, XY};

#[derive(Clone, Copy)]
struct SweepEdge {
    lower: u32,
    upper: u32,
    item: usize,
}

trait SweepStatus: Sized {
    fn new_status(capacity: usize) -> Self;

    fn prefix_rank<F>(&self, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge) -> Result<bool>;

    fn get_at_rank(&self, rank: usize) -> Option<SweepEdge>;

    fn insert_rank(&mut self, rank: usize, edge: SweepEdge);

    fn remove_rank(&mut self, rank: usize) -> SweepEdge;
}

impl SweepStatus for OrderTree<SweepEdge> {
    fn new_status(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn prefix_rank<F>(&self, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge) -> Result<bool>,
    {
        Self::partition_point_by(self, before)
    }

    fn get_at_rank(&self, rank: usize) -> Option<SweepEdge> {
        Self::item_at(self, rank)
    }

    fn insert_rank(&mut self, rank: usize, edge: SweepEdge) {
        Self::insert_at_rank(self, rank, edge);
    }

    fn remove_rank(&mut self, rank: usize) -> SweepEdge {
        Self::remove_at_rank(self, rank)
    }
}

#[cfg(test)]
impl SweepStatus for Vec<SweepEdge> {
    fn new_status(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn prefix_rank<F>(&self, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge) -> Result<bool>,
    {
        first_not_left(self, before)
    }

    fn get_at_rank(&self, rank: usize) -> Option<SweepEdge> {
        self.get(rank).copied()
    }

    fn insert_rank(&mut self, rank: usize, edge: SweepEdge) {
        self.insert(rank, edge);
    }

    fn remove_rank(&mut self, rank: usize) -> SweepEdge {
        self.remove(rank)
    }
}

/// Points a leftward ray sweep reads: rounded coordinates, an order on each
/// axis, and orientation.
pub(in crate::geometry) trait RayPoints {
    fn rounded(&self, index: u32) -> XY;
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering>;
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign>;

    /// Sort items by their points' coordinate on `axis`, reporting each
    /// adjacent pair at one coordinate to `equal`. The rounded coordinate
    /// sorts; exact points break only runs of equal rounded coordinates.
    #[expect(
        clippy::indexing_slicing,
        reason = "run bounds are advanced only within the sorted item slice"
    )]
    fn sort_by_axis<T: Copy, P: Fn(T) -> u32, E: FnMut(T, T) -> Result<()>>(
        &self,
        items: &mut [T],
        axis: Axis,
        point: P,
        mut equal: E,
    ) -> Result<()> {
        let key = |item: T| {
            let value = axis.of(self.rounded(point(item)));
            if value == 0.0 { 0.0 } else { value }
        };
        items.sort_unstable_by(|&left, &right| key(left).total_cmp(&key(right)));
        let mut run = 0;
        while run < items.len() {
            let mut end = run.wrapping_add(1);
            while end < items.len() && key(items[end]).total_cmp(&key(items[run])).is_eq() {
                end = end.wrapping_add(1);
            }
            if end.wrapping_sub(run) > 1 {
                let tied = &mut items[run..end];
                try_sort_by(tied, |&left, &right| {
                    self.compare_axis(point(left), point(right), axis)
                })?;
                for &[left, right] in tied.array_windows::<2>() {
                    if self.compare_axis(point(left), point(right), axis)? == Ordering::Equal {
                        equal(left, right)?;
                    }
                }
            }
            run = end;
        }
        Ok(())
    }
}

impl RayPoints for PointArena<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the point ID was admitted by PointArena before it entered the sweep"
    )]
    fn rounded(&self, index: u32) -> XY {
        self.rounded[index as usize]
    }

    /// Order two points along one axis. Nearest-even rounding is monotone, so
    /// rounded coordinates that differ order the exact points the same way;
    /// only a tie that is not between two stored values reads exact points.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        let (l, r) = (
            axis.of(self.rounded[left as usize]),
            axis.of(self.rounded[right as usize]),
        );
        if l < r {
            return Ok(Ordering::Less);
        }
        if l > r {
            return Ok(Ordering::Greater);
        }
        if left == right || (self.is_stored(left) && self.is_stored(right)) {
            return Ok(Ordering::Equal);
        }
        let (left, right) = (self.exact(left)?, self.exact(right)?);
        let index = axis.index();
        compare_ratios(&left[index], &left[2], &right[index], &right[2])
    }

    /// The orientation of `c` against the directed line through `a` and `b`:
    /// the stored predicate when all three are stored, otherwise an interval
    /// filter over the certified bounds and the exact homogeneous determinant.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        if self.is_stored(a) && self.is_stored(b) && self.is_stored(c) {
            let [a, b, c] = [a, b, c].map(|index| self.rounded[index as usize]);
            return Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y));
        }
        let interval = |index: u32, axis: Axis| {
            let (lo, hi) = self.axis_bounds(index, axis);
            Interval { lo, hi }
        };
        let (ax, ay) = (interval(a, Axis::X), interval(a, Axis::Y));
        let determinant = interval(b, Axis::X)
            .sub(ax)
            .mul(interval(c, Axis::Y).sub(ay))
            .sub(
                interval(b, Axis::Y)
                    .sub(ay)
                    .mul(interval(c, Axis::X).sub(ax)),
            );
        if determinant.is_finite()
            && let Some(sign) = determinant.sign()
        {
            return Ok(sign);
        }
        let [ax, ay, aw] = &*self.exact(a)?;
        let [bx, by, bw] = &*self.exact(b)?;
        let [cx, cy, cw] = &*self.exact(c)?;
        let dx = bx.try_product(aw)?.try_subtract(ax.try_product(bw)?)?;
        let dy = by.try_product(aw)?.try_subtract(ay.try_product(bw)?)?;
        let cx_rel = cx.try_product(aw)?.try_subtract(ax.try_product(cw)?)?;
        let cy_rel = cy.try_product(aw)?.try_subtract(ay.try_product(cw)?)?;
        let sign = dx
            .try_product(&cy_rel)?
            .try_subtract(dy.try_product(&cx_rel)?)?
            .sign();
        // The determinant carries aw^2 bw cw, and aw^2 is positive.
        let flipped = (bw.sign() == ExactSign::Negative) != (cw.sign() == ExactSign::Negative);
        Ok(match (flipped, sign) {
            (true, ExactSign::Negative) => ExactSign::Positive,
            (true, ExactSign::Positive) => ExactSign::Negative,
            (_, sign) => sign,
        })
    }
}

/// Emitted node coordinates, exact as the stored doubles they are.
pub(super) struct EmittedPoints<'p>(pub(super) &'p [XY]);

impl RayPoints for EmittedPoints<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn rounded(&self, index: u32) -> XY {
        self.0[index as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        let (left, right) = (
            axis.of(self.0[left as usize]),
            axis.of(self.0[right as usize]),
        );
        Ok(if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        let [a, b, c] = [a, b, c].map(|index| self.0[index as usize]);
        Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y))
    }
}

/// Leftward ray shooting for many queries in one sweep over y.
///
/// The ray from a query runs toward -x just above the query's height, so an
/// edge whose lower endpoint lies at that height is crossed and one whose
/// upper endpoint does is not; every hit is an edge, and the region just east
/// of it holds the query. A ray passing just above a vertex therefore meets
/// the faces its wedge toward +x belongs to, and a horizontal edge at the
/// query's height is never crossed. Each query must be its component's
/// lexicographic minimum, so its own edges never lie strictly left of it.
/// Answers follow `queries`, as native indices into `edges`.
///
/// The status holds the edges crossing the sweep line in x order: an edge
/// enters at its lower endpoint and leaves at its upper one. An implicit AVL
/// sequence locates and updates each edge by rank using the exact left-above
/// and left-below orientation comparisons. Sorting and status work cost
/// O((n + q) log(n + q)); no update shifts the remaining edges.
pub(in crate::geometry) fn shoot_left<R: RayPoints, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<usize>>> {
    shoot_left_with_status::<OrderTree<SweepEdge>, _, _>(points, edge_count, edge, queries)
}

/// Compare each emitted component's local host with the face east of its
/// leftward ray hit. `sides(edge)` gives the exact face on the left and right
/// of the directed edge returned by `edge`.
#[expect(
    clippy::indexing_slicing,
    clippy::float_cmp,
    reason = "emitted node IDs index this face graph; its face-side labels and edge ordinals are paired"
)]
pub(in crate::geometry) fn visit_component_faces<
    R: RayPoints,
    E: Fn(usize) -> [u32; 2],
    F: Fn(usize) -> [u32; 2],
    V: FnMut(u32, Option<usize>) -> ControlFlow<()>,
>(
    points: &R,
    node_count: usize,
    edge_count: usize,
    edge: E,
    sides: F,
    mut visit: V,
) -> Result<ControlFlow<()>> {
    let incidence_rows = node_count.wrapping_add(1);
    let mut incident_starts = vec![0_usize; incidence_rows];
    let mut parents = Vec::with_capacity(node_count);
    parents.extend(0..node_count as u32);
    for index in 0..edge_count {
        let [start, end] = edge(index);
        let start_node = start as usize;
        let start_offset = start_node.wrapping_add(1);
        incident_starts[start_offset] = incident_starts[start_offset].wrapping_add(1);
        if end != start {
            let end_node = end as usize;
            let end_offset = end_node.wrapping_add(1);
            incident_starts[end_offset] = incident_starts[end_offset].wrapping_add(1);
        }
        let mut left = start;
        let mut right = end;
        while parents[left as usize] != left {
            parents[left as usize] = parents[parents[left as usize] as usize];
            left = parents[left as usize];
        }
        while parents[right as usize] != right {
            parents[right as usize] = parents[parents[right as usize] as usize];
            right = parents[right as usize];
        }
        parents[left.max(right) as usize] = left.min(right);
    }
    super::construction::prefix_contact_offsets(&mut incident_starts);
    let incident_count = incident_starts[node_count];
    let mut incident = vec![0_usize; incident_count];
    {
        let mut cursor = Vec::with_capacity(node_count);
        cursor.extend(incident_starts.iter().take(node_count).copied());
        for index in 0..edge_count {
            let [start, end] = edge(index);
            for node in [start, end]
                .into_iter()
                .take(if end == start { 1 } else { 2 })
            {
                let node = node as usize;
                let position = cursor[node];
                incident[position] = index;
                cursor[node] = position.wrapping_add(1);
            }
        }
    }
    let mut roots: Vec<Option<(usize, XY)>> = Vec::with_capacity(node_count);
    roots.resize(node_count, None);
    for node in 0..node_count {
        if incident_starts[node] == incident_starts[node.wrapping_add(1)] {
            continue;
        }
        let mut root = node as u32;
        while parents[root as usize] != root {
            root = parents[root as usize];
        }
        let point = points.rounded(node as u32);
        let entry = &mut roots[root as usize];
        if entry.is_none_or(|(_, prior): (usize, XY)| {
            point.x < prior.x || (point.x == prior.x && point.y < prior.y)
        }) {
            *entry = Some((node, point));
        }
    }
    drop(parents);

    let mut queries = Vec::new();
    let mut exteriors = Vec::new();
    for &(node, point) in roots.iter().flatten() {
        let mut exterior = None;
        let mut chosen = None;
        let mut has_upper = false;
        let range = incident_starts[node]..incident_starts[node.wrapping_add(1)];
        for &index in &incident[range] {
            let [start, end] = edge(index);
            let (target, target_point, face) = if start as usize == node {
                (end, points.rounded(end), sides(index)[0])
            } else {
                (start, points.rounded(start), sides(index)[1])
            };
            if target as usize == node {
                exterior = Some(face);
                continue;
            }
            let upper = target_point.y >= point.y;
            if chosen.is_none()
                || (upper && !has_upper)
                || (upper == has_upper && {
                    let (prior_target, prior_point) =
                        chosen.expect("a selected departure exists before comparing the next one");
                    compare_departures(point, prior_point, prior_target, target_point, target)
                        == Ordering::Less
                })
            {
                chosen = Some((target, target_point));
                exterior = Some(face);
                has_upper = upper;
            }
        }
        queries.push(node as u32);
        exteriors.push(exterior.expect("a component root has an incident emitted edge"));
    }
    drop(roots);
    drop(incident);
    drop(incident_starts);

    let hits = shoot_left(points, edge_count, &edge, &queries)?;
    for ((&root, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
        let host = if let Some(index) = hit {
            let [start, end] = edge(index);
            if points.compare_axis(start, end, Axis::Y)?.is_lt() {
                sides(index)[1]
            } else {
                sides(index)[0]
            }
        } else {
            0
        };
        if host != exterior && visit(root, hit).is_break() {
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

#[cfg(test)]
pub(super) fn shoot_left_vector_reference<R: RayPoints, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<usize>>> {
    shoot_left_with_status::<Vec<SweepEdge>, _, _>(points, edge_count, edge, queries)
}

#[expect(
    clippy::indexing_slicing,
    reason = "query positions come from the order permutation; status ranks come from prefix searches"
)]
fn shoot_left_with_status<S: SweepStatus, R: RayPoints, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<usize>>> {
    let mut order = Vec::with_capacity(queries.len());
    order.extend(0..queries.len());
    points.sort_by_axis(&mut order, Axis::Y, |index| queries[index], |_, _| Ok(()))?;
    // A horizontal or zero-length edge never crosses the sweep, nor does one
    // whose vertical extent contains no query height.
    let mut starts: Vec<SweepEdge> = Vec::new();
    for index in 0..edge_count {
        let [a, b] = edge(index);
        let (lower, upper) = match points.compare_axis(a, b, Axis::Y)? {
            Ordering::Less => (a, b),
            Ordering::Greater => (b, a),
            Ordering::Equal => continue,
        };
        let first = first_not_left(&order, |query| {
            Ok(points.compare_axis(queries[query], lower, Axis::Y)? == Ordering::Less)
        })?;
        if let Some(&query) = order.get(first)
            && points.compare_axis(queries[query], upper, Axis::Y)? == Ordering::Less
        {
            starts.push(SweepEdge {
                lower,
                upper,
                item: index,
            });
        }
    }
    let mut ends = Vec::with_capacity(starts.len());
    ends.extend_from_slice(&starts);
    points.sort_by_axis(&mut starts, Axis::Y, |edge| edge.lower, |_, _| Ok(()))?;
    points.sort_by_axis(&mut ends, Axis::Y, |edge| edge.upper, |_, _| Ok(()))?;
    let mut answers = Vec::with_capacity(queries.len());
    answers.resize(queries.len(), None);

    let orientation = |a, b, c| {
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.ray_orientations = metrics.ray_orientations.wrapping_add(1);
        });
        points.orientation(a, b, c)
    };
    let negative = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Negative) };
    let positive = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Positive) };
    // Whether `edge` lies left of every point just above `point`'s height on
    // the ray through it; for an edge starting at `point`, the fan order.
    let left_above = |edge: SweepEdge, starting: SweepEdge| -> Result<bool> {
        if edge.lower == starting.lower {
            positive(starting.lower, starting.upper, edge.upper)
        } else {
            negative(edge.lower, edge.upper, starting.lower)
        }
    };
    // The same just below the height where `ending` leaves, among the edges
    // converging on its upper endpoint by their lower directions.
    let left_below = |edge: SweepEdge, ending: SweepEdge| -> Result<bool> {
        if edge.upper == ending.upper {
            positive(ending.lower, ending.upper, edge.lower)
        } else {
            negative(edge.lower, edge.upper, ending.upper)
        }
    };
    let mut status = S::new_status(starts.len());
    let (mut next_start, mut next_end, mut next_query) = (0, 0, 0);
    while next_query < order.len() {
        let mut height = queries[order[next_query]];
        if let Some(starting) = starts.get(next_start)
            && points.compare_axis(starting.lower, height, Axis::Y)? == Ordering::Less
        {
            height = starting.lower;
        }
        if let Some(ending) = ends.get(next_end)
            && points.compare_axis(ending.upper, height, Axis::Y)? == Ordering::Less
        {
            height = ending.upper;
        }
        while let Some(&ending) = ends.get(next_end)
            && points.compare_axis(ending.upper, height, Axis::Y)? == Ordering::Equal
        {
            let index = status.prefix_rank(|edge| left_below(edge, ending))?;
            if status.get_at_rank(index).map(|edge| edge.item) != Some(ending.item) {
                return Err(GeometryErrorKind::Invalid(
                    "leftward ray sweep lost an edge from its status".into(),
                )
                .into());
            }
            status.remove_rank(index);
            next_end = next_end.wrapping_add(1);
        }
        while let Some(&starting) = starts.get(next_start)
            && points.compare_axis(starting.lower, height, Axis::Y)? == Ordering::Equal
        {
            let index = status.prefix_rank(|edge| left_above(edge, starting))?;
            status.insert_rank(index, starting);
            next_start = next_start.wrapping_add(1);
        }
        while let Some(&query) = order.get(next_query)
            && points.compare_axis(queries[query], height, Axis::Y)? == Ordering::Equal
        {
            let point = queries[query];
            let rank = status.prefix_rank(|edge| negative(edge.lower, edge.upper, point))?;
            answers[query] = rank
                .checked_sub(1)
                .and_then(|left| status.get_at_rank(left))
                .map(|edge| edge.item);
            next_query = next_query.wrapping_add(1);
        }
    }
    Ok(answers)
}

/// The first item that is not left of the probe, for items ordered so that
/// `left` holds on a prefix.
#[expect(
    clippy::indexing_slicing,
    reason = "binary-search middle is strictly below high, which starts at items.len()"
)]
fn first_not_left<T: Copy, L: FnMut(T) -> Result<bool>>(items: &[T], mut left: L) -> Result<usize> {
    let (mut low, mut high) = (0, items.len());
    while low < high {
        let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
        if left(items[middle])? {
            low = middle.wrapping_add(1);
        } else {
            high = middle;
        }
    }
    Ok(low)
}
