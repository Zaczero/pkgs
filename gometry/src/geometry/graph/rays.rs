//! Generic exact-ray ordering and leftward shooting.

use std::cmp::Ordering;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::{GeometryErrorKind, OrderTree, XY};
use crate::work::{Unmetered, Work, WorkResult};

#[cfg(test)]
thread_local! {
    static GRAPH_RAY_ORIENTATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_graph_ray_orientation() {
    GRAPH_RAY_ORIENTATIONS.with(|count| count.set(count.get().wrapping_add(1)));
}

#[cfg(test)]
pub(in crate::geometry) fn reset_graph_ray_orientations() {
    GRAPH_RAY_ORIENTATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(in crate::geometry) fn graph_ray_orientations() -> usize {
    GRAPH_RAY_ORIENTATIONS.with(Cell::get)
}

#[derive(Clone, Copy)]
pub(in crate::geometry) enum RayAxis {
    X,
    Y,
}

impl RayAxis {
    pub(in crate::geometry) const fn of(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }

    pub(in crate::geometry) const fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
        }
    }
}

#[derive(Clone, Copy)]
struct SweepEdge {
    lower: u32,
    upper: u32,
    item: usize,
}

trait SweepStatus: Sized {
    fn new_status(capacity: usize) -> Self;

    fn prefix_rank<W: Work, F>(&self, work: &mut W, before: F) -> Result<usize>
    where
        F: FnMut(SweepEdge, &mut W) -> Result<bool>;

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> WorkResult<W, Option<SweepEdge>>;

    fn insert_rank<W: Work>(
        &mut self,
        rank: usize,
        edge: SweepEdge,
        work: &mut W,
    ) -> WorkResult<W, ()>;

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> WorkResult<W, SweepEdge>;
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

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> WorkResult<W, Option<SweepEdge>> {
        Self::item_at_with_work(self, rank, work)
    }

    fn insert_rank<W: Work>(
        &mut self,
        rank: usize,
        edge: SweepEdge,
        work: &mut W,
    ) -> WorkResult<W, ()> {
        Self::insert_at_rank_with_work(self, rank, edge, work)
    }

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> WorkResult<W, SweepEdge> {
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

    fn get_at_rank<W: Work>(&self, rank: usize, work: &mut W) -> WorkResult<W, Option<SweepEdge>> {
        work.charge(1)?;
        Ok(self.get(rank).copied())
    }

    fn insert_rank<W: Work>(
        &mut self,
        rank: usize,
        edge: SweepEdge,
        work: &mut W,
    ) -> WorkResult<W, ()> {
        work.charge_product(self.len().wrapping_sub(rank), 1)?;
        self.insert(rank, edge);
        Ok(())
    }

    fn remove_rank<W: Work>(&mut self, rank: usize, work: &mut W) -> WorkResult<W, SweepEdge> {
        work.charge_product(self.len().wrapping_sub(rank).wrapping_sub(1), 1)?;
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
    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering>;
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign>;

    /// Sort items by their points' coordinate on `axis`, reporting each
    /// adjacent pair at one coordinate to `equal`. The rounded coordinate
    /// sorts; exact points break only runs of equal rounded coordinates.
    #[cfg(test)]
    fn sort_by_axis<T: Copy, P: Fn(T) -> u32, E: FnMut(T, T) -> Result<()>>(
        &self,
        items: &mut [T],
        axis: RayAxis,
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
        axis: RayAxis,
        work: &mut W,
    ) -> Result<Ordering>;

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign>;

    fn sort_by_axis_with_work<T: Copy, P: Fn(T) -> u32, E: FnMut(T, T) -> Result<()>, W: Work>(
        &self,
        items: &mut [T],
        axis: RayAxis,
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
    C: FnMut(u32, u32, RayAxis, &mut W) -> Result<Ordering>,
>(
    points: &R,
    items: &mut [T],
    axis: RayAxis,
    point: P,
    mut equal: E,
    work: &mut W,
    mut compare_axis: C,
) -> Result<()> {
    let key = |item: T| {
        let value = axis.of(points.rounded(point(item)));
        if value == 0.0 { 0.0 } else { value }
    };
    work.charge_into_error(
        u64::try_from(items.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut keyed = Vec::with_capacity(items.len());
    for &item in items.iter() {
        work.charge_into_error(1)?;
        keyed.push((key(item), item));
    }
    try_sort_by_with_work(&mut keyed, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.0.total_cmp(&right.0))
    })?;
    let mut run = 0;
    while run < keyed.len() {
        work.charge_into_error(1)?;
        let mut end = run.wrapping_add(1);
        while end < keyed.len() && keyed[end].0.total_cmp(&keyed[run].0).is_eq() {
            work.charge_into_error(1)?;
            end = end.wrapping_add(1);
        }
        if end.wrapping_sub(run) > 1 {
            let tied = &mut keyed[run..end];
            try_sort_by_with_work(tied, work, |left, right, work| {
                work.charge_into_error(1)?;
                compare_axis(point(left.1), point(right.1), axis, work)
            })?;
            for pair in tied.array_windows::<2>() {
                work.charge_into_error(1)?;
                if compare_axis(point(pair[0].1), point(pair[1].1), axis, work)? == Ordering::Equal
                {
                    equal(pair[0].1, pair[1].1)?;
                }
            }
        }
        run = end;
    }
    for (item, (_, value)) in items.iter_mut().zip(keyed) {
        work.charge_into_error(1)?;
        *item = value;
    }
    Ok(())
}

struct UnmeteredRayPoints<'p, R: ?Sized>(&'p R);

impl<R: RayPoints + ?Sized> RayPoints for UnmeteredRayPoints<'_, R> {
    fn rounded(&self, index: u32) -> XY {
        self.0.rounded(index)
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
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
        axis: RayAxis,
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

pub(in crate::geometry) fn compare_departures_with_work<R: RayPointsWithWork + ?Sized, W: Work>(
    points: &R,
    origin: u32,
    left: u32,
    right: u32,
    work: &mut W,
) -> Result<Ordering> {
    work.charge_into_error(4)?;
    let left_y = points.compare_axis_with_work(left, origin, RayAxis::Y, work)?;
    let left_x = points.compare_axis_with_work(left, origin, RayAxis::X, work)?;
    let right_y = points.compare_axis_with_work(right, origin, RayAxis::Y, work)?;
    let right_x = points.compare_axis_with_work(right, origin, RayAxis::X, work)?;
    let upper = |y: Ordering, x: Ordering| y.is_gt() || (y.is_eq() && x.is_gt());
    let halves = upper(right_y, right_x).cmp(&upper(left_y, left_x));
    if !halves.is_eq() {
        return Ok(halves);
    }
    work.charge_into_error(1)?;
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

#[cfg(test)]
pub(in crate::geometry) fn shoot_left_vector_reference<
    R: RayPointsWithWork,
    E: Fn(usize) -> [u32; 2],
>(
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
    work.charge_into_error(
        u64::try_from(queries.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut order = Vec::with_capacity(queries.len());
    order.extend(0..queries.len());
    points.sort_by_axis_with_work(
        &mut order,
        RayAxis::Y,
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
        work.charge_into_error(1)?;
        let [a, b] = edge(index)?;
        let a_y = points.rounded(a).y;
        let b_y = points.rounded(b).y;
        if (a_y < min_query_y && b_y < min_query_y) || (a_y > max_query_y && b_y > max_query_y) {
            continue;
        }
        work.charge_into_error(1)?;
        let (lower, upper) = match points.compare_axis_with_work(a, b, RayAxis::Y, work)? {
            Ordering::Less => (a, b),
            Ordering::Greater => (b, a),
            Ordering::Equal => continue,
        };
        let first = first_not_left_with_work(&order, work, |query, work| {
            work.charge_into_error(1)?;
            Ok(
                points.compare_axis_with_work(queries[query], lower, RayAxis::Y, work)?
                    == Ordering::Less,
            )
        })?;
        work.charge_into_error(1)?;
        if let Some(&query) = order.get(first)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(queries[query], upper, RayAxis::Y, work)?
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
    work.charge_into_error(
        u64::try_from(starts.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ends = Vec::with_capacity(starts.len());
    ends.extend_from_slice(&starts);
    points.sort_by_axis_with_work(
        &mut starts,
        RayAxis::Y,
        |edge| edge.lower,
        |_, _| Ok(()),
        work,
    )?;
    points.sort_by_axis_with_work(
        &mut ends,
        RayAxis::Y,
        |edge| edge.upper,
        |_, _| Ok(()),
        work,
    )?;
    work.charge_into_error(
        u64::try_from(queries.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut answers = Vec::with_capacity(queries.len());
    answers.resize(queries.len(), None);

    let negative = |a, b, c, work: &mut W| -> Result<bool> {
        work.charge_into_error(1)?;
        #[cfg(test)]
        record_graph_ray_orientation();
        Ok(points.orientation_with_work(a, b, c, work)? == ExactSign::Negative)
    };
    let positive = |a, b, c, work: &mut W| -> Result<bool> {
        work.charge_into_error(1)?;
        #[cfg(test)]
        record_graph_ray_orientation();
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
    work.charge_into_error(
        u64::try_from(starts.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut status = S::new_status(starts.len());
    let (mut next_start, mut next_end, mut next_query) = (0, 0, 0);
    while next_query < order.len() {
        work.charge_into_error(1)?;
        let mut height = queries[order[next_query]];
        if let Some(starting) = starts.get(next_start)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(starting.lower, height, RayAxis::Y, work)?
                    == Ordering::Less
            }
        {
            height = starting.lower;
        }
        if let Some(ending) = ends.get(next_end)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(ending.upper, height, RayAxis::Y, work)?
                    == Ordering::Less
            }
        {
            height = ending.upper;
        }
        while let Some(&ending) = ends.get(next_end)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(ending.upper, height, RayAxis::Y, work)?
                    == Ordering::Equal
            }
        {
            let index = status.prefix_rank(work, |edge, work| left_below(edge, ending, work))?;
            work.charge_into_error(1)?;
            if status
                .get_at_rank(index, work)
                .map_err(Into::into)?
                .map(|edge| edge.item)
                != Some(ending.item)
            {
                return Err(GeometryErrorKind::Invalid(
                    "leftward ray sweep lost an edge from its status".into(),
                )
                .into());
            }
            work.charge_into_error(1)?;
            status.remove_rank(index, work).map_err(Into::into)?;
            next_end = next_end.wrapping_add(1);
        }
        while let Some(&starting) = starts.get(next_start)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(starting.lower, height, RayAxis::Y, work)?
                    == Ordering::Equal
            }
        {
            let index = status.prefix_rank(work, |edge, work| left_above(edge, starting, work))?;
            work.charge_into_error(1)?;
            status
                .insert_rank(index, starting, work)
                .map_err(Into::into)?;
            next_start = next_start.wrapping_add(1);
        }
        while let Some(&query) = order.get(next_query)
            && {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(queries[query], height, RayAxis::Y, work)?
                    == Ordering::Equal
            }
        {
            let point = queries[query];
            let rank = status.prefix_rank(work, |edge, work| {
                negative(edge.lower, edge.upper, point, work)
            })?;
            work.charge_into_error(1)?;
            answers[query] = if let Some(left) = rank.checked_sub(1) {
                status
                    .get_at_rank(left, work)
                    .map_err(Into::into)?
                    .map(|edge| edge.item)
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
        work.charge_into_error(1)?;
        let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
        if left(items[middle], work)? {
            low = middle.wrapping_add(1);
        } else {
            high = middle;
        }
    }
    Ok(low)
}
