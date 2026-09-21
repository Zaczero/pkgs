//! Leftward ray contacts over exact points.
//!
//! A cyclic walk needs to know which span lies immediately left of a
//! component's lexicographic minimum. [`RayPoints`] abstracts over the two
//! point sources that question is asked of -- the partition's exact arena
//! during construction, and the rounded items at emission -- so the sweep
//! itself is written once.

use std::cmp::Ordering;

use super::Axis;
use super::construction::PointArena;
#[cfg(test)]
use super::metrics::update as update_partition_metrics;
use crate::error::ResourceError;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{stored_orientation_sign, try_sort_by};
use crate::geometry::tessellation::exact::{Interval, compare_ratios};
use crate::geometry::{GeometryErrorKind, Result, XY};

/// Points a leftward ray sweep reads: rounded coordinates, an order on each
/// axis, and orientation.
pub(super) trait RayPoints {
    fn rounded(&self, index: u32) -> XY;
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering>;
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign>;

    /// Sort items by their points' coordinate on `axis`, reporting each
    /// adjacent pair at one coordinate to `equal`. The rounded coordinate
    /// sorts; exact points break only runs of equal rounded coordinates.
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
            let mut end = run + 1;
            while end < items.len() && key(items[end]).total_cmp(&key(items[run])).is_eq() {
                end += 1;
            }
            if end - run > 1 {
                let tied = &mut items[run..end];
                try_sort_by(tied, |&left, &right| {
                    self.compare_axis(point(left), point(right), axis)
                })?;
                for pair in 1..tied.len() {
                    if self.compare_axis(point(tied[pair - 1]), point(tied[pair]), axis)?
                        == Ordering::Equal
                    {
                        equal(tied[pair - 1], tied[pair])?;
                    }
                }
            }
            run = end;
        }
        Ok(())
    }
}

impl RayPoints for PointArena<'_> {
    fn rounded(&self, index: u32) -> XY {
        self.rounded[index as usize]
    }

    /// Order two points along one axis. Nearest-even rounding is monotone, so
    /// rounded coordinates that differ order the exact points the same way;
    /// only a tie that is not between two stored values reads exact points.
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
    fn rounded(&self, index: u32) -> XY {
        self.0[index as usize]
    }

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
/// Answers follow `queries`, as indices into `edges`.
///
/// The status holds the edges crossing the sweep line in x order: an edge
/// enters at its lower endpoint and leaves at its upper one, placed and found
/// by binary search on orientations, so the sweep costs O((n + q) log n)
/// comparisons plus the status shifts.
pub(super) fn shoot_left<R: RayPoints, E: Fn(usize) -> [u32; 2]>(
    points: &R,
    edge_count: usize,
    edge: E,
    queries: &[u32],
) -> Result<Vec<Option<u32>>> {
    let mut order = Vec::with_capacity(queries.len());
    order.extend(0..queries.len());
    points.sort_by_axis(&mut order, Axis::Y, |index| queries[index], |_, _| Ok(()))?;
    // [lower, upper, edge] for the edges some ray crosses: a horizontal or
    // zero-length edge never does, nor one no query height falls within.
    let mut starts: Vec<[u32; 3]> = Vec::new();
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
            let index = u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?;
            starts.push([lower, upper, index]);
        }
    }
    let mut ends = Vec::with_capacity(starts.len());
    ends.extend_from_slice(&starts);
    points.sort_by_axis(&mut starts, Axis::Y, |[lower, ..]| lower, |_, _| Ok(()))?;
    points.sort_by_axis(&mut ends, Axis::Y, |[_, upper, _]| upper, |_, _| Ok(()))?;
    let mut answers = Vec::with_capacity(queries.len());
    answers.resize(queries.len(), None);

    let orientation = |a, b, c| {
        #[cfg(test)]
        update_partition_metrics(|metrics| metrics.ray_orientations += 1);
        points.orientation(a, b, c)
    };
    let negative = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Negative) };
    let positive = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Positive) };
    // Whether `edge` lies left of every point just above `point`'s height on
    // the ray through it; for an edge starting at `point`, the fan order.
    let left_above = |[lower, upper, _]: [u32; 3], [start, end, _]: [u32; 3]| -> Result<bool> {
        if lower == start {
            positive(start, end, upper)
        } else {
            negative(lower, upper, start)
        }
    };
    // The same just below the height where `ending` leaves, among the edges
    // converging on its upper endpoint by their lower directions.
    let left_below = |[lower, upper, _]: [u32; 3], [start, end, _]: [u32; 3]| -> Result<bool> {
        if upper == end {
            positive(start, end, lower)
        } else {
            negative(lower, upper, end)
        }
    };
    let mut status: Vec<[u32; 3]> = Vec::with_capacity(starts.len());
    let (mut next_start, mut next_end, mut next_query) = (0, 0, 0);
    while next_query < order.len() {
        let mut height = queries[order[next_query]];
        if let Some(&[lower, ..]) = starts.get(next_start)
            && points.compare_axis(lower, height, Axis::Y)? == Ordering::Less
        {
            height = lower;
        }
        if let Some(&[_, upper, _]) = ends.get(next_end)
            && points.compare_axis(upper, height, Axis::Y)? == Ordering::Less
        {
            height = upper;
        }
        while let Some(&ending) = ends.get(next_end)
            && points.compare_axis(ending[1], height, Axis::Y)? == Ordering::Equal
        {
            let index = first_not_left(&status, |edge| left_below(edge, ending))?;
            if status.get(index).map(|edge| edge[2]) != Some(ending[2]) {
                return Err(GeometryErrorKind::Invalid(
                    "leftward ray sweep lost an edge from its status".into(),
                )
                .into());
            }
            status.remove(index);
            next_end += 1;
        }
        while let Some(&starting) = starts.get(next_start)
            && points.compare_axis(starting[0], height, Axis::Y)? == Ordering::Equal
        {
            let index = first_not_left(&status, |edge| left_above(edge, starting))?;
            status.insert(index, starting);
            next_start += 1;
        }
        while let Some(&query) = order.get(next_query)
            && points.compare_axis(queries[query], height, Axis::Y)? == Ordering::Equal
        {
            let point = queries[query];
            let index = first_not_left(&status, |[lower, upper, _]| negative(lower, upper, point))?;
            answers[query] = index.checked_sub(1).map(|left| status[left][2]);
            next_query += 1;
        }
    }
    Ok(answers)
}

/// The first item that is not left of the probe, for items ordered so that
/// `left` holds on a prefix.
pub(super) fn first_not_left<T: Copy, L: FnMut(T) -> Result<bool>>(
    items: &[T],
    mut left: L,
) -> Result<usize> {
    let (mut low, mut high) = (0, items.len());
    while low < high {
        let middle = low + (high - low) / 2;
        if left(items[middle])? {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}
