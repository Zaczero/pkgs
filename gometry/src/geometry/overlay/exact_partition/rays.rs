//! Leftward ray contacts over exact points.
//!
//! A cyclic walk needs to know which span lies immediately left of a
//! component's lexicographic minimum. [`RayPoints`] abstracts over the two
//! point sources that question is asked of -- the partition's exact arena
//! during construction, and the rounded items at emission -- so the sweep
//! itself is written once.

use std::cmp::Ordering;
use std::ops::ControlFlow;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use super::Axis;
use super::construction::PointArena;
#[cfg(test)]
use super::metrics::update as update_partition_metrics;
use crate::geometry::arrangement::compare_departures;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{stored_orientation_sign, try_sort_by_with_work};
use crate::geometry::tessellation::exact::{Interval, compare_ratios_with_work};
use crate::geometry::{GeometryErrorKind, OrderTree, Result, Unmetered, Work, XY};

#[derive(Clone, Copy)]
struct SweepEdge {
    lower: u32,
    upper: u32,
    item: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RayWorkProgress {
    axis_attempted: usize,
    axis_completed: usize,
    orientation_attempted: usize,
    orientation_completed: usize,
}

#[cfg(test)]
thread_local! {
    static RAY_WORK_PROGRESS: Cell<RayWorkProgress> =
        const { Cell::new(RayWorkProgress {
            axis_attempted: 0,
            axis_completed: 0,
            orientation_attempted: 0,
            orientation_completed: 0,
        }) };
}

#[cfg(test)]
fn reset_ray_work_progress() {
    RAY_WORK_PROGRESS.with(|progress| progress.set(RayWorkProgress::default()));
}

#[cfg(test)]
fn ray_work_progress() -> RayWorkProgress {
    RAY_WORK_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_axis_attempt() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.axis_attempted = value.axis_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_axis_complete() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.axis_completed = value.axis_completed.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_orientation_attempt() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.orientation_attempted = value.orientation_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_orientation_complete() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.orientation_completed = value.orientation_completed.wrapping_add(1);
        progress.set(value);
    });
}

trait SweepStatus: Sized {
    fn new_status(capacity: usize) -> Self;

    fn prefix_rank<W: Work, F>(&self, work: &mut W, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge, &mut W) -> Result<bool>;

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> Result<Option<SweepEdge>>;

    fn insert_rank<W: Work>(&mut self, rank: usize, edge: SweepEdge, work: &mut W) -> Result<()>;

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> Result<SweepEdge>;
}

impl SweepStatus for OrderTree<SweepEdge> {
    fn new_status(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn prefix_rank<W: Work, F>(&self, work: &mut W, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge, &mut W) -> Result<bool>,
    {
        Self::partition_point_by_with_work(self, work, before)
    }

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> Result<Option<SweepEdge>> {
        Self::item_at_with_work(self, rank, work)
    }

    fn insert_rank<W: Work>(&mut self, rank: usize, edge: SweepEdge, work: &mut W) -> Result<()> {
        Self::insert_at_rank_with_work(self, rank, edge, work)
    }

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> Result<SweepEdge> {
        Self::remove_at_rank_with_work(self, rank, work)
    }
}

#[cfg(test)]
impl SweepStatus for Vec<SweepEdge> {
    fn new_status(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn prefix_rank<W: Work, F>(&self, work: &mut W, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge, &mut W) -> Result<bool>,
    {
        first_not_left_with_work(self, work, before)
    }

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> Result<Option<SweepEdge>> {
        work.charge(1)?;
        Ok(self.get(rank).copied())
    }

    fn insert_rank<W: Work>(&mut self, rank: usize, edge: SweepEdge, work: &mut W) -> Result<()> {
        work.charge(
            u64::try_from(self.len().wrapping_sub(rank))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        self.insert(rank, edge);
        Ok(())
    }

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> Result<SweepEdge> {
        work.charge(
            u64::try_from(self.len().wrapping_sub(rank).wrapping_sub(1))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        Ok(self.remove(rank))
    }
}

/// Points a leftward ray sweep reads: rounded coordinates, an order on each
/// axis, and orientation.
pub(in crate::geometry) trait RayPoints {
    /// For either axis, strict order between rounded hints implies the same
    /// order from `compare_axis`. Equal hints can cover distinct exact points;
    /// constant hints are valid and disable hint-based rejection.
    fn rounded(&self, index: u32) -> XY;
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering>;
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign>;

    /// Sort items by their points' coordinate on `axis`, reporting each
    /// adjacent pair at one coordinate to `equal`. The rounded coordinate
    /// sorts; exact points break only runs of equal rounded coordinates.
    #[cfg(test)]
    fn sort_by_axis<T: Copy, P: Fn(T) -> u32, E: FnMut(T, T) -> Result<()>>(
        &self,
        items: &mut [T],
        axis: Axis,
        point: P,
        equal: E,
    ) -> Result<()> {
        let mut work = Unmetered;
        sort_by_axis_with_work(
            self,
            items,
            axis,
            point,
            equal,
            &mut work,
            |left, right, axis, _work| self.compare_axis(left, right, axis),
        )
    }
}

/// Exact ray comparisons whose variable-width work must share the owner's
/// mutable budget. Ordinary ray providers need not implement this capability.
pub(in crate::geometry) trait RayPointsWithWork: RayPoints {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        work: &mut W,
    ) -> Result<Ordering>;

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign>;

    #[expect(
        clippy::indexing_slicing,
        reason = "runs advance within the keyed permutation created from this exact item slice"
    )]
    fn sort_by_axis_with_work<T: Copy, P: Fn(T) -> u32, E: FnMut(T, T) -> Result<()>, W: Work>(
        &self,
        items: &mut [T],
        axis: Axis,
        point: P,
        equal: E,
        work: &mut W,
    ) -> Result<()> {
        sort_by_axis_with_work(
            self,
            items,
            axis,
            point,
            equal,
            work,
            |left, right, axis, work| self.compare_axis_with_work(left, right, axis, work),
        )
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "runs advance within the keyed permutation created from this exact item slice"
)]
fn sort_by_axis_with_work<
    R: RayPoints + ?Sized,
    T: Copy,
    P: Fn(T) -> u32,
    E: FnMut(T, T) -> Result<()>,
    W: Work,
    C: FnMut(u32, u32, Axis, &mut W) -> Result<Ordering>,
>(
    points: &R,
    items: &mut [T],
    axis: Axis,
    point: P,
    mut equal: E,
    work: &mut W,
    mut compare_axis: C,
) -> Result<()> {
    let key = |item: T| {
        let value = axis.of(points.rounded(point(item)));
        if value == 0.0 { 0.0 } else { value }
    };
    work.charge(
        u64::try_from(items.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut keyed = Vec::with_capacity(items.len());
    for &item in items.iter() {
        work.charge(1)?;
        keyed.push((key(item), item));
    }
    try_sort_by_with_work(&mut keyed, work, |left, right, work| {
        work.charge(1)?;
        Ok(left.0.total_cmp(&right.0))
    })?;
    let mut run = 0;
    while run < keyed.len() {
        work.charge(1)?;
        let mut end = run.wrapping_add(1);
        while end < keyed.len() && keyed[end].0.total_cmp(&keyed[run].0).is_eq() {
            work.charge(1)?;
            end = end.wrapping_add(1);
        }
        if end.wrapping_sub(run) > 1 {
            let tied = &mut keyed[run..end];
            try_sort_by_with_work(tied, work, |left, right, work| {
                work.charge(1)?;
                compare_axis(point(left.1), point(right.1), axis, work)
            })?;
            for pair in tied.array_windows::<2>() {
                work.charge(1)?;
                if compare_axis(point(pair[0].1), point(pair[1].1), axis, work)? == Ordering::Equal
                {
                    equal(pair[0].1, pair[1].1)?;
                }
            }
        }
        run = end;
    }
    for (item, (_, value)) in items.iter_mut().zip(keyed) {
        work.charge(1)?;
        *item = value;
    }
    Ok(())
}

struct UnmeteredRayPoints<'p, R: ?Sized>(&'p R);

impl<R: RayPoints + ?Sized> RayPoints for UnmeteredRayPoints<'_, R> {
    fn rounded(&self, index: u32) -> XY {
        self.0.rounded(index)
    }

    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        self.0.compare_axis(left, right, axis)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.0.orientation(a, b, c)
    }
}

impl<R: RayPoints + ?Sized> RayPointsWithWork for UnmeteredRayPoints<'_, R> {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        _work: &mut W,
    ) -> Result<Ordering> {
        self.0.compare_axis(left, right, axis)
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        _work: &mut W,
    ) -> Result<ExactSign> {
        self.0.orientation(a, b, c)
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

    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        self.compare_axis_with_work(left, right, axis, &mut Unmetered)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.orientation_with_work(a, b, c, &mut Unmetered)
    }
}

impl RayPointsWithWork for PointArena<'_> {
    /// Order two points along one axis. Nearest-even rounding is monotone, so
    /// rounded coordinates that differ order the exact points the same way;
    /// only a tie that is not between two stored values reads exact points.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        work: &mut W,
    ) -> Result<Ordering> {
        #[cfg(test)]
        record_axis_attempt();
        let (l, r) = (
            axis.of(self.rounded[left as usize]),
            axis.of(self.rounded[right as usize]),
        );
        if l < r {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Less);
        }
        if l > r {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Greater);
        }
        if left == right || (self.is_stored(left) && self.is_stored(right)) {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Equal);
        }
        let (left, right) = (
            self.exact_with_work(left, work)?,
            self.exact_with_work(right, work)?,
        );
        let index = axis.index();
        let order =
            compare_ratios_with_work(&left[index], &left[2], &right[index], &right[2], work)?;
        #[cfg(test)]
        record_axis_complete();
        Ok(order)
    }

    /// The orientation of `c` against the directed line through `a` and `b`:
    /// the stored predicate when all three are stored, otherwise an interval
    /// filter over the certified bounds and the exact homogeneous determinant.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign> {
        #[cfg(test)]
        record_orientation_attempt();
        if self.is_stored(a) && self.is_stored(b) && self.is_stored(c) {
            let [a, b, c] = [a, b, c].map(|index| self.rounded[index as usize]);
            #[cfg(test)]
            record_orientation_complete();
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
            #[cfg(test)]
            record_orientation_complete();
            return Ok(sign);
        }
        let a = self.exact_with_work(a, work)?;
        let b = self.exact_with_work(b, work)?;
        let c = self.exact_with_work(c, work)?;
        let [ax, ay, aw] = &*a;
        let [bx, by, bw] = &*b;
        let [cx, cy, cw] = &*c;
        let dx_left = bx.try_product_with_work(aw, work)?;
        let dx_right = ax.try_product_with_work(bw, work)?;
        let dx = dx_left.try_subtract_with_work(dx_right, work)?;
        let dy_left = by.try_product_with_work(aw, work)?;
        let dy_right = ay.try_product_with_work(bw, work)?;
        let dy = dy_left.try_subtract_with_work(dy_right, work)?;
        let cx_left = cx.try_product_with_work(aw, work)?;
        let cx_right = ax.try_product_with_work(cw, work)?;
        let cx_rel = cx_left.try_subtract_with_work(cx_right, work)?;
        let cy_left = cy.try_product_with_work(aw, work)?;
        let cy_right = ay.try_product_with_work(cw, work)?;
        let cy_rel = cy_left.try_subtract_with_work(cy_right, work)?;
        let determinant_left = dx.try_product_with_work(&cy_rel, work)?;
        let determinant_right = dy.try_product_with_work(&cx_rel, work)?;
        let sign = determinant_left
            .try_subtract_with_work(determinant_right, work)?
            .sign();
        #[cfg(test)]
        record_orientation_complete();
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

impl RayPointsWithWork for EmittedPoints<'_> {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        _work: &mut W,
    ) -> Result<Ordering> {
        self.compare_axis(left, right, axis)
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        _work: &mut W,
    ) -> Result<ExactSign> {
        self.orientation(a, b, c)
    }
}

pub(in crate::geometry) fn compare_departures_with_work<R: RayPointsWithWork + ?Sized, W: Work>(
    points: &R,
    origin: u32,
    left: u32,
    right: u32,
    work: &mut W,
) -> Result<Ordering> {
    work.charge(4)?;
    let left_y = points.compare_axis_with_work(left, origin, Axis::Y, work)?;
    let left_x = points.compare_axis_with_work(left, origin, Axis::X, work)?;
    let right_y = points.compare_axis_with_work(right, origin, Axis::Y, work)?;
    let right_x = points.compare_axis_with_work(right, origin, Axis::X, work)?;
    let upper = |y: Ordering, x: Ordering| y.is_gt() || (y.is_eq() && x.is_gt());
    let halves = upper(right_y, right_x).cmp(&upper(left_y, left_x));
    if !halves.is_eq() {
        return Ok(halves);
    }
    work.charge(1)?;
    let sign = points.orientation_with_work(origin, left, right, work)?;
    if sign == ExactSign::Zero && left != right {
        return Err(GeometryErrorKind::Invalid(
            "distinct graph edges share a departure ray".into(),
        )
        .into());
    }
    Ok(match sign {
        ExactSign::Positive => Ordering::Less,
        ExactSign::Negative => Ordering::Greater,
        ExactSign::Zero => Ordering::Equal,
    })
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
pub(in crate::geometry) fn shoot_left<R: RayPoints + ?Sized, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<usize>>> {
    let mut work = Unmetered;
    let points = UnmeteredRayPoints(points);
    shoot_left_with_work(
        &points,
        edge_count,
        |index| Ok(edge(index)),
        queries,
        &mut work,
    )
}

pub(in crate::geometry) fn shoot_left_with_work<
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]>,
    W: Work,
>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
    work: &mut W,
) -> Result<Vec<Option<usize>>> {
    shoot_left_with_status::<OrderTree<SweepEdge>, _, _, _>(points, edge_count, edge, queries, work)
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
    R: RayPoints + ?Sized,
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
pub(super) fn shoot_left_vector_reference<R: RayPointsWithWork, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<usize>>> {
    let mut work = Unmetered;
    shoot_left_with_status::<Vec<SweepEdge>, _, _, _>(
        points,
        edge_count,
        |index| Ok(edge(index)),
        queries,
        &mut work,
    )
}

#[expect(
    clippy::indexing_slicing,
    reason = "query positions come from the order permutation; status ranks come from prefix searches"
)]
fn shoot_left_with_status<
    S: SweepStatus,
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]>,
    W: Work,
>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
    work: &mut W,
) -> Result<Vec<Option<usize>>> {
    work.charge(
        u64::try_from(queries.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut order = Vec::with_capacity(queries.len());
    order.extend(0..queries.len());
    points.sort_by_axis_with_work(
        &mut order,
        Axis::Y,
        |index| queries[index],
        |_, _| Ok(()),
        work,
    )?;
    let Some((&first_query, &last_query)) = order.first().zip(order.last()) else {
        return Ok(Vec::new());
    };
    let min_query_y = points.rounded(queries[first_query]).y;
    let max_query_y = points.rounded(queries[last_query]).y;
    // A horizontal or zero-length edge never crosses the sweep, nor does one
    // whose vertical extent contains no query height.
    let mut starts: Vec<SweepEdge> = Vec::new();
    for index in 0..edge_count {
        work.charge(1)?;
        let [a, b] = edge(index)?;
        let a_y = points.rounded(a).y;
        let b_y = points.rounded(b).y;
        if (a_y < min_query_y && b_y < min_query_y) || (a_y > max_query_y && b_y > max_query_y) {
            continue;
        }
        work.charge(1)?;
        let (lower, upper) = match points.compare_axis_with_work(a, b, Axis::Y, work)? {
            Ordering::Less => (a, b),
            Ordering::Greater => (b, a),
            Ordering::Equal => continue,
        };
        let first = first_not_left_with_work(&order, work, |query, work| {
            work.charge(1)?;
            Ok(
                points.compare_axis_with_work(queries[query], lower, Axis::Y, work)?
                    == Ordering::Less,
            )
        })?;
        work.charge(1)?;
        if let Some(&query) = order.get(first)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(queries[query], upper, Axis::Y, work)?
                    == Ordering::Less
            }
        {
            starts.push(SweepEdge {
                lower,
                upper,
                item: index,
            });
        }
    }
    work.charge(
        u64::try_from(starts.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ends = Vec::with_capacity(starts.len());
    ends.extend_from_slice(&starts);
    points.sort_by_axis_with_work(&mut starts, Axis::Y, |edge| edge.lower, |_, _| Ok(()), work)?;
    points.sort_by_axis_with_work(&mut ends, Axis::Y, |edge| edge.upper, |_, _| Ok(()), work)?;
    work.charge(
        u64::try_from(queries.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut answers = Vec::with_capacity(queries.len());
    answers.resize(queries.len(), None);

    let negative = |a, b, c, work: &mut W| -> Result<bool> {
        work.charge(1)?;
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.ray_orientations = metrics.ray_orientations.wrapping_add(1);
        });
        Ok(points.orientation_with_work(a, b, c, work)? == ExactSign::Negative)
    };
    let positive = |a, b, c, work: &mut W| -> Result<bool> {
        work.charge(1)?;
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.ray_orientations = metrics.ray_orientations.wrapping_add(1);
        });
        Ok(points.orientation_with_work(a, b, c, work)? == ExactSign::Positive)
    };
    // Whether `edge` lies left of every point just above `point`'s height on
    // the ray through it; for an edge starting at `point`, the fan order.
    let left_above = |edge: SweepEdge, starting: SweepEdge, work: &mut W| -> Result<bool> {
        if edge.lower == starting.lower {
            positive(starting.lower, starting.upper, edge.upper, work)
        } else {
            negative(edge.lower, edge.upper, starting.lower, work)
        }
    };
    // The same just below the height where `ending` leaves, among the edges
    // converging on its upper endpoint by their lower directions.
    let left_below = |edge: SweepEdge, ending: SweepEdge, work: &mut W| -> Result<bool> {
        if edge.upper == ending.upper {
            positive(ending.lower, ending.upper, edge.lower, work)
        } else {
            negative(edge.lower, edge.upper, ending.upper, work)
        }
    };
    work.charge(
        u64::try_from(starts.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut status = S::new_status(starts.len());
    let (mut next_start, mut next_end, mut next_query) = (0, 0, 0);
    while next_query < order.len() {
        work.charge(1)?;
        let mut height = queries[order[next_query]];
        if let Some(starting) = starts.get(next_start)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(starting.lower, height, Axis::Y, work)?
                    == Ordering::Less
            }
        {
            height = starting.lower;
        }
        if let Some(ending) = ends.get(next_end)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(ending.upper, height, Axis::Y, work)?
                    == Ordering::Less
            }
        {
            height = ending.upper;
        }
        while let Some(&ending) = ends.get(next_end)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(ending.upper, height, Axis::Y, work)?
                    == Ordering::Equal
            }
        {
            let index = status.prefix_rank(work, |edge, work| left_below(edge, ending, work))?;
            work.charge(1)?;
            if status.get_at_rank(index, work)?.map(|edge| edge.item) != Some(ending.item) {
                return Err(GeometryErrorKind::Invalid(
                    "leftward ray sweep lost an edge from its status".into(),
                )
                .into());
            }
            work.charge(1)?;
            status.remove_rank(index, work)?;
            next_end = next_end.wrapping_add(1);
        }
        while let Some(&starting) = starts.get(next_start)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(starting.lower, height, Axis::Y, work)?
                    == Ordering::Equal
            }
        {
            let index = status.prefix_rank(work, |edge, work| left_above(edge, starting, work))?;
            work.charge(1)?;
            status.insert_rank(index, starting, work)?;
            next_start = next_start.wrapping_add(1);
        }
        while let Some(&query) = order.get(next_query)
            && {
                work.charge(1)?;
                points.compare_axis_with_work(queries[query], height, Axis::Y, work)?
                    == Ordering::Equal
            }
        {
            let point = queries[query];
            let rank = status.prefix_rank(work, |edge, work| {
                negative(edge.lower, edge.upper, point, work)
            })?;
            work.charge(1)?;
            answers[query] = if let Some(left) = rank.checked_sub(1) {
                status.get_at_rank(left, work)?.map(|edge| edge.item)
            } else {
                None
            };
            next_query = next_query.wrapping_add(1);
        }
    }
    Ok(answers)
}

#[expect(
    clippy::indexing_slicing,
    reason = "binary-search middle is strictly below high, which starts at items.len()"
)]
fn first_not_left_with_work<T: Copy, W: Work, L>(
    items: &[T],
    work: &mut W,
    mut left: L,
) -> Result<usize>
where
    L: FnMut(T, &mut W) -> Result<bool>,
{
    let (mut low, mut high) = (0, items.len());
    while low < high {
        work.charge(1)?;
        let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
        if left(items[middle], work)? {
            low = middle.wrapping_add(1);
        } else {
            high = middle;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::super::NodeRecipe;
    use super::{
        Axis, PointArena, RayPointsWithWork, RayWorkProgress, compare_departures_with_work,
        ray_work_progress, reset_ray_work_progress,
    };
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::tessellation::exact::ExactDyadic;
    use crate::geometry::{Unmetered, Work, XY};

    #[derive(Clone, Copy)]
    struct ChargeEvent {
        spent_before: u128,
        ticks: u128,
        progress: RayWorkProgress,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        spent: u128,
        events: Vec<ChargeEvent>,
        refused: Option<ChargeEvent>,
    }

    impl<'a> RecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget) -> Self {
            Self {
                budget,
                spent: 0,
                events: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = ChargeEvent {
                spent_before: self.spent,
                ticks,
                progress: ray_work_progress(),
            };
            if result.is_ok() {
                self.events.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for RecordingWork<'_> {
        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(operation: &'static str, event: ChargeEvent) -> EvaluationBudget {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .expect("the target ray charge is positive and fits the budget");
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
            .expect("calibrated ray budget is representable")
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn add_f64(left: f64, right: f64) -> ExactDyadic {
        ExactDyadic::from_f64(left)
            .try_add_with_work(ExactDyadic::from_f64(right), &mut Unmetered)
            .expect("test dyadics are representable")
    }

    fn push_point(points: &mut PointArena<'_>, rounded: XY, x: ExactDyadic, y: ExactDyadic) -> u32 {
        points
            .push_constructed(rounded, [x, y, ExactDyadic::one()])
            .expect("test point ID is representable")
    }

    fn crossing_is_materialized(points: &PointArena<'_>, node: u32) -> bool {
        matches!(
            points.recipes.get(node as usize),
            Some(NodeRecipe::Crossing { exact, .. }) if exact.get().is_some()
        )
    }

    fn tied_axis_points() -> PointArena<'static> {
        let segments: &'static [_] = &[];
        let mut points = PointArena::with_capacity(segments, 2);
        let one = ExactDyadic::one();
        let x_next = add_f64(1.0, 2.0_f64.powi(-256));
        push_point(&mut points, XY::new(1.0, 0.0), one, ExactDyadic::zero());
        push_point(&mut points, XY::new(1.0, 0.0), x_next, ExactDyadic::zero());
        points
    }

    fn tied_orientation_points() -> PointArena<'static> {
        let segments: &'static [_] = &[];
        let mut points = PointArena::with_capacity(segments, 3);
        let rounded = XY::new(1.0, 1.0);
        let y = add_f64(1.0, 2.0_f64.powi(-256));
        push_point(&mut points, rounded, ExactDyadic::one(), ExactDyadic::one());
        push_point(
            &mut points,
            rounded,
            add_f64(1.0, 2.0_f64.powi(-256)),
            y.clone(),
        );
        push_point(&mut points, rounded, add_f64(1.0, 2.0_f64.powi(-255)), y);
        points
    }

    fn sort_tied_axis<W: Work>(
        points: &PointArena<'_>,
        items: &mut [u32],
        work: &mut W,
    ) -> Result<()> {
        RayPointsWithWork::sort_by_axis_with_work(
            points,
            items,
            Axis::X,
            |index| index,
            |_, _| Ok(()),
            work,
        )
    }

    #[test]
    fn native_ray_tied_axis_refuses_before_exact_ordering() -> Result<()> {
        const OPERATION: &str = "native_ray_axis";
        let points = tied_axis_points();
        let mut items = [1_u32, 0_u32];
        reset_ray_work_progress();
        let (ordered, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            sort_tied_axis(&points, &mut items, &mut work)?;
            (items, work.events)
        };
        assert_eq!(ordered, [0, 1]);
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress.axis_attempted == 1
                    && event.progress.axis_completed == 0
                    && event.ticks > 0
            })
            .expect("tied-axis ordering performs metered exact comparison work");

        reset_ray_work_progress();
        let mut items = [1_u32, 0_u32];
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = sort_tied_axis(&points, &mut items, &mut work)
                .expect_err("budget refusal precedes exact tied-axis comparison");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(
            items,
            [1, 0],
            "failed exact sort did not publish a permutation"
        );
        assert_eq!(ray_work_progress(), target.progress);

        reset_ray_work_progress();
        let mut unmetered_items = [1_u32, 0_u32];
        sort_tied_axis(&points, &mut unmetered_items, &mut Unmetered)?;
        assert_eq!(unmetered_items, ordered);

        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        let mut evaluation_items = [1_u32, 0_u32];
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            sort_tied_axis(&points, &mut evaluation_items, &mut work)?;
        }
        assert_eq!(evaluation_items, ordered);
        assert_eq!(
            ordinary_budget.spent_ticks_for_test(),
            ordinary_spend,
            "EvaluationOnly erases ray arithmetic and sorter hooks"
        );
        Ok(())
    }

    #[test]
    fn native_point_arena_lazy_crossing_refuses_before_cache_publication() -> Result<()> {
        const OPERATION: &str = "native_ray_lazy_crossing";
        let segments = [
            crate::geometry::Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(2.0, 2.0),
            },
            crate::geometry::Segment {
                start: XY::new(0.0, 2.0),
                end: XY::new(2.0, 0.0),
            },
        ];
        let make_points = || {
            let mut points = PointArena::with_capacity(&segments, 2);
            let crossing = points
                .push_crossing([0, 1])
                .expect("crossing node ID is representable");
            let query = push_point(
                &mut points,
                XY::new(1.0, 1.0),
                add_f64(1.0, 2.0_f64.powi(-256)),
                ExactDyadic::one(),
            );
            (points, crossing, query)
        };
        let (points, crossing, query) = make_points();
        assert!(!crossing_is_materialized(&points, crossing));
        reset_ray_work_progress();
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = points.compare_axis_with_work(crossing, query, Axis::X, &mut work)?;
            (order, work.events)
        };
        assert_eq!(order, Ordering::Less);
        assert!(crossing_is_materialized(&points, crossing));
        let crossing_charges: Vec<_> = charges
            .into_iter()
            .filter(|event| {
                event.progress.axis_attempted == 1 && event.progress.axis_completed == 0
            })
            .collect();
        let target = *crossing_charges
            .get(1)
            .expect("lazy exact crossing construction has its own Work charge");

        let (points, crossing, query) = make_points();
        assert!(!crossing_is_materialized(&points, crossing));
        reset_ray_work_progress();
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = points
                .compare_axis_with_work(crossing, query, Axis::X, &mut work)
                .expect_err("crossing construction refuses before publishing the cache");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(ray_work_progress(), target.progress);
        assert!(
            !crossing_is_materialized(&points, crossing),
            "failed lazy exact construction leaves the OnceCell empty"
        );

        let (points, crossing, query) = make_points();
        let mut unmetered = Unmetered;
        assert_eq!(
            points.compare_axis_with_work(crossing, query, Axis::X, &mut unmetered)?,
            order
        );
        let (points, crossing, query) = make_points();
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                points.compare_axis_with_work(crossing, query, Axis::X, &mut work)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    fn native_ray_tied_nonzero_orientation_refuses_before_determinant() -> Result<()> {
        const OPERATION: &str = "native_ray_orientation";
        let points = tied_orientation_points();
        reset_ray_work_progress();
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = compare_departures_with_work(&points, 0, 1, 2, &mut work)?;
            (order, work.events)
        };
        assert_eq!(order, Ordering::Greater);
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress.orientation_attempted == 1
                    && event.progress.orientation_completed == 0
                    && event.ticks > 0
            })
            .expect("same-rounded nonzero rays reach metered exact orientation");

        reset_ray_work_progress();
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = compare_departures_with_work(&points, 0, 1, 2, &mut work)
                .expect_err("budget refusal precedes exact determinant completion");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        let progress = ray_work_progress();
        assert_eq!(progress.orientation_attempted, 1);
        assert_eq!(progress.orientation_completed, 0);
        assert_eq!(progress.axis_attempted, 4);
        assert_eq!(progress.axis_completed, 4);

        let mut unmetered = Unmetered;
        assert_eq!(
            compare_departures_with_work(&points, 0, 1, 2, &mut unmetered)?,
            order
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut evaluation_only = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                compare_departures_with_work(&points, 0, 1, 2, &mut evaluation_only)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
}
