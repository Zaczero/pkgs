//! Certified binary64 coordinates for exact-partition output.
//!
//! One node-coordinate table is checked for live-node identity, prescribed
//! chord incidence, cyclic order, and optional global-face correspondence.
//! Emissions carry the topology labels that those checks need; selection stays
//! in the exact partition.
//!
//! Every emitted item, including an isolated point as `[n, n]`, is checked
//! for identity and pairwise incidence. Pure linework has no areal faces and
//! skips fan and global-face checks. A face-bearing emission checks fan order
//! at each node incident to an areal chord, including every emitted chord at
//! that node. Fans with at most two departures have a unique cyclic order and
//! satisfy F vacuously. Global-face correspondence covers every component and
//! isolated point in the emission.

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::mem;
use std::ops::ControlFlow;

use super::rays::{EmittedPoints, shoot_left};
use super::{Axis, ExactPartition, NodeId};
use crate::collections::{HashMap, UnionFind, try_hash_map_with_capacity};
use crate::error::ResourceError;
use crate::geographic::{GeographicFrame, Latitude, SeamIdentity};
use crate::geometry::arrangement::compare_departures;
use crate::geometry::embedding_search::{
    ClassAssignment, EvaluationBudget, GlobalCheck, Problem, Search,
};
use crate::geometry::overlay::exact_partition::emission::emitted_pair_preserves_incidence;
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    Bounds, Contact, GeometryErrorKind, PointKey, Result, Segment, XY, for_each_candidate_pair,
    for_each_overlapping_bounds_cross_pair, segment_contact_exact, single_chain, try_sort_by,
};

const PROXIMITY_RELATIVE_SLACK: f64 = 9.094_947_017_729_282e-13;

/// The coordinate domain of a partition's output embedding.
#[derive(Clone, Copy, Debug)]
pub(super) enum EmbeddingDomain {
    Plane,
    Rectangle(Bounds),
    Geographic(GeographicFrame),
}

impl EmbeddingDomain {
    pub(super) fn admits(self, nearest: XY, candidate: XY) -> bool {
        if !candidate.x.is_finite() || !candidate.y.is_finite() {
            return false;
        }
        match self {
            Self::Plane => true,
            Self::Rectangle(bounds) => bounds.contains_xy(candidate),
            Self::Geographic(frame) => {
                let longitude_axis = frame.longitude();
                let half_period = longitude_axis.half_period();
                if candidate.x < -half_period || candidate.x > half_period {
                    return false;
                }
                let Ok(latitude) = Latitude::try_new(frame, candidate.y) else {
                    return false;
                };
                let Ok(longitude) = longitude_axis.canonicalize(candidate.x) else {
                    return false;
                };
                if longitude.identity(longitude_axis) == SeamIdentity::Seam {
                    let Ok(nearest_longitude) = longitude_axis.canonicalize(nearest.x) else {
                        return false;
                    };
                    if nearest_longitude.identity(longitude_axis) != SeamIdentity::Seam {
                        return false;
                    }
                }
                if latitude.identity(frame) == SeamIdentity::Pole {
                    let Ok(nearest_latitude) = Latitude::try_new(frame, nearest.y) else {
                        return false;
                    };
                    if nearest_latitude.identity(frame) != SeamIdentity::Pole {
                        return false;
                    }
                }
                true
            },
        }
    }
}

fn single_step_radius(value: f64) -> f64 {
    if !value.is_finite() {
        return f64::INFINITY;
    }
    (value.next_up() - value).max(value - value.next_down())
}

fn swept_bounds(segment: Segment, radius: f64) -> Bounds {
    let bounds = Bounds::from_xy_iter([segment.start, segment.end]);
    if radius == 0.0 {
        return bounds;
    }
    let radius = if radius.is_finite() {
        (radius + radius * PROXIMITY_RELATIVE_SLACK).next_up()
    } else {
        f64::INFINITY
    };
    Bounds::new_raw(
        (bounds.minx() - radius).next_down(),
        (bounds.miny() - radius).next_down(),
        (bounds.maxx() + radius).next_up(),
        (bounds.maxy() + radius).next_up(),
    )
}

fn proximity_limit(left_radius: f64, right_radius: f64) -> f64 {
    let radius = left_radius + right_radius;
    if radius == 0.0 || !radius.is_finite() {
        return radius;
    }
    (radius + radius * PROXIMITY_RELATIVE_SLACK).next_up()
}

fn axis_parameter_interval(point: f64, start: f64, end: f64, limit: f64) -> Option<(f64, f64)> {
    // Keep arithmetic uncertainty outward; overflow or an indeterminate quotient
    // retains the whole segment rather than excluding a possible close point.
    let difference = point - start;
    let direction = end - start;
    if !difference.is_finite() || !direction.is_finite() {
        return Some((0.0, 1.0));
    }
    let (difference_low, difference_high) = if point == start {
        (0.0, 0.0)
    } else {
        (difference.next_down(), difference.next_up())
    };
    if start == end {
        return (difference_low <= limit && difference_high >= -limit).then_some((0.0, 1.0));
    }
    let (mut direction_low, mut direction_high) = (direction.next_down(), direction.next_up());
    let minimum_subnormal = f64::from_bits(1);
    if end > start {
        direction_low = direction_low.max(minimum_subnormal);
    } else {
        direction_high = direction_high.min(-minimum_subnormal);
    }
    let numerator_low = (difference_low - limit).next_down();
    let numerator_high = (difference_high + limit).next_up();
    let mut parameter_low = f64::INFINITY;
    let mut parameter_high = f64::NEG_INFINITY;
    for numerator in [numerator_low, numerator_high] {
        for denominator in [direction_low, direction_high] {
            let parameter = numerator / denominator;
            if parameter.is_nan() {
                return Some((0.0, 1.0));
            }
            parameter_low = parameter_low.min(parameter.next_down());
            parameter_high = parameter_high.max(parameter.next_up());
        }
    }
    let parameter_low = parameter_low.max(0.0);
    let parameter_high = parameter_high.min(1.0);
    (parameter_low <= parameter_high).then_some((parameter_low, parameter_high))
}

fn point_within_chebyshev_segment(point: XY, segment: Segment, limit: f64) -> bool {
    if !limit.is_finite()
        || !point.x.is_finite()
        || !point.y.is_finite()
        || !segment.start.x.is_finite()
        || !segment.start.y.is_finite()
        || !segment.end.x.is_finite()
        || !segment.end.y.is_finite()
    {
        return true;
    }
    let Some((x_low, x_high)) =
        axis_parameter_interval(point.x, segment.start.x, segment.end.x, limit)
    else {
        return false;
    };
    let Some((y_low, y_high)) =
        axis_parameter_interval(point.y, segment.start.y, segment.end.y, limit)
    else {
        return false;
    };
    x_low.max(y_low) <= x_high.min(y_high)
}

/// The minimum L-infinity distance between disjoint segments is attained by
/// an endpoint-to-segment pair; exact contact handles the zero-distance case.
fn segments_within_chebyshev(left: Segment, right: Segment, limit: f64) -> bool {
    if !matches!(segment_contact_exact(left, right), Contact::Disjoint) {
        return true;
    }
    if limit == 0.0 {
        return false;
    }
    point_within_chebyshev_segment(left.start, right, limit)
        || point_within_chebyshev_segment(left.end, right, limit)
        || point_within_chebyshev_segment(right.start, left, limit)
        || point_within_chebyshev_segment(right.end, left, limit)
}

#[cfg(test)]
mod proximity_tests {
    use super::*;
    use crate::geometry::tessellation::exact::ExactRational;

    fn segment(start: XY, end: XY) -> Segment {
        Segment { start, end }
    }

    fn exact_axis_interval(
        point: f64,
        start: f64,
        end: f64,
        limit: f64,
    ) -> Result<Option<(ExactRational, ExactRational)>> {
        let point = ExactRational::from_f64(point);
        let start = ExactRational::from_f64(start);
        let end = ExactRational::from_f64(end);
        let negative_limit = ExactRational::from_f64(-limit);
        let limit = ExactRational::from_f64(limit);
        let difference = point.try_subtract(&start)?;
        let direction = end.try_subtract(&start)?;
        if direction.is_zero() {
            let upper = limit;
            return Ok((difference.try_cmp(&negative_limit)? != Ordering::Less
                && difference.try_cmp(&upper)? != Ordering::Greater)
                .then_some((ExactRational::zero(), ExactRational::one())));
        }
        let mut lower = difference.try_subtract(&limit)?.try_quotient(&direction)?;
        let mut upper = difference.try_add(&limit)?.try_quotient(&direction)?;
        if lower.try_cmp(&upper)? == Ordering::Greater {
            mem::swap(&mut lower, &mut upper);
        }
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        if upper.try_cmp(&zero)? == Ordering::Less || lower.try_cmp(&one)? == Ordering::Greater {
            return Ok(None);
        }
        if lower.try_cmp(&zero)? == Ordering::Less {
            lower = zero;
        }
        if upper.try_cmp(&one)? == Ordering::Greater {
            upper = one;
        }
        Ok(Some((lower, upper)))
    }

    fn exact_point_within_segment(point: XY, segment: Segment, limit: f64) -> Result<bool> {
        let Some((x_low, x_high)) =
            exact_axis_interval(point.x, segment.start.x, segment.end.x, limit)?
        else {
            return Ok(false);
        };
        let Some((y_low, y_high)) =
            exact_axis_interval(point.y, segment.start.y, segment.end.y, limit)?
        else {
            return Ok(false);
        };
        let low = if x_low.try_cmp(&y_low)? == Ordering::Less {
            y_low
        } else {
            x_low
        };
        let high = if x_high.try_cmp(&y_high)? == Ordering::Greater {
            y_high
        } else {
            x_high
        };
        Ok(low.try_cmp(&high)? != Ordering::Greater)
    }

    fn exact_segments_within(left: Segment, right: Segment, limit: f64) -> Result<bool> {
        if !matches!(segment_contact_exact(left, right), Contact::Disjoint) {
            return Ok(true);
        }
        Ok(exact_point_within_segment(left.start, right, limit)?
            || exact_point_within_segment(left.end, right, limit)?
            || exact_point_within_segment(right.start, left, limit)?
            || exact_point_within_segment(right.end, left, limit)?)
    }

    #[test]
    fn chebyshev_proximity_checks_the_segment_distance_and_exact_contact() {
        let horizontal = segment(XY::new(0.0, 0.0), XY::new(4.0, 0.0));
        assert!(segments_within_chebyshev(
            horizontal,
            segment(XY::new(0.0, 1.0), XY::new(4.0, 1.0)),
            1.0
        ));
        assert!(!segments_within_chebyshev(
            horizontal,
            segment(XY::new(0.0, 1.25), XY::new(4.0, 1.25)),
            1.0
        ));
        assert!(segments_within_chebyshev(
            segment(XY::new(0.0, 0.0), XY::new(2.0, 2.0)),
            segment(XY::new(0.0, 2.0), XY::new(2.0, 0.0)),
            0.0
        ));
    }

    #[test]
    fn chebyshev_proximity_does_not_confuse_overlapping_boxes_with_near_segments() {
        let diagonal = segment(XY::new(0.0, 0.0), XY::new(2.0, 2.0));
        let separated = segment(XY::new(0.0, 2.0), XY::new(1.0, 1.5));
        assert!(
            Bounds::from_xy_iter([diagonal.start, diagonal.end])
                .intersects(Bounds::from_xy_iter([separated.start, separated.end]))
        );
        assert!(!segments_within_chebyshev(diagonal, separated, 0.24));
        assert!(segments_within_chebyshev(diagonal, separated, 0.25));
    }

    #[test]
    fn chebyshev_proximity_includes_isolated_points_and_uses_relative_slack() {
        let point = segment(XY::new(1.0, 0.0), XY::new(1.0, 0.0));
        let line = segment(XY::new(0.0, 0.0), XY::new(2.0, 0.0));
        assert!(segments_within_chebyshev(point, line, 0.0));

        let limit = proximity_limit(1.0, 0.0);
        assert!(segments_within_chebyshev(
            segment(
                XY::new(0.0, 1.0 + 2.0_f64.powi(-41)),
                XY::new(2.0, 1.0 + 2.0_f64.powi(-41))
            ),
            segment(XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
            limit
        ));
        assert!(!segments_within_chebyshev(
            segment(
                XY::new(0.0, 1.0 + 2.0_f64.powi(-38)),
                XY::new(2.0, 1.0 + 2.0_f64.powi(-38))
            ),
            segment(XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
            limit
        ));
    }

    #[test]
    fn chebyshev_distance_intervals_never_reject_an_exactly_near_pair() {
        let minimum = f64::from_bits(1);
        let values = [
            -f64::MAX,
            -2.0,
            -1.0,
            -minimum,
            -0.0,
            0.0,
            minimum,
            0.5,
            1.0_f64.next_down(),
            1.0,
            1.0_f64.next_up(),
            2.0,
            f64::MAX,
        ];
        let limits = [0.0, minimum, 2.0_f64.powi(-52), 0.125, 0.5, 1.0, 1.0e100];
        let mut state = 0x6A09_E667_F3BC_C909_u64;
        let mut sample = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as usize
        };
        for _ in 0..384 {
            let point = XY::new(
                values[sample() % values.len()],
                values[sample() % values.len()],
            );
            let left = segment(
                XY::new(
                    values[sample() % values.len()],
                    values[sample() % values.len()],
                ),
                XY::new(
                    values[sample() % values.len()],
                    values[sample() % values.len()],
                ),
            );
            let right = segment(
                XY::new(
                    values[sample() % values.len()],
                    values[sample() % values.len()],
                ),
                XY::new(
                    values[sample() % values.len()],
                    values[sample() % values.len()],
                ),
            );
            let limit = limits[sample() % limits.len()];
            if exact_point_within_segment(point, left, limit).unwrap() {
                assert!(point_within_chebyshev_segment(point, left, limit));
            }
            if exact_segments_within(left, right, limit).unwrap() {
                assert!(segments_within_chebyshev(left, right, limit));
            }
        }
    }
}

/// The coordinates used to check an emission. Nearest borrows the immutable
/// arena table; an accepted search assignment owns a patched copy.
#[derive(Debug)]
pub(super) struct Embedding<'p> {
    table: Cow<'p, [XY]>,
}

impl<'p> Embedding<'p> {
    pub(super) fn nearest(partition: &'p ExactPartition<'_>) -> Self {
        Self {
            table: Cow::Borrowed(&partition.points.rounded),
        }
    }

    pub(super) const fn from_owned(table: Vec<XY>) -> Self {
        Self {
            table: Cow::Owned(table),
        }
    }

    /// Borrow once at serialization boundaries instead of redispatching `Cow`
    /// for each emitted node.
    pub(super) fn coordinates(&self) -> &[XY] {
        self.table.as_ref()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each NodeId is admitted by the same ExactPartition whose coordinate table this embedding owns"
    )]
    pub(super) fn xy(&self, node: NodeId) -> XY {
        Self::xy_at(self.coordinates(), node)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the node ID and coordinate slice are paired from the same certified embedding"
    )]
    pub(super) fn xy_at(coordinates: &[XY], node: NodeId) -> XY {
        coordinates[node.0 as usize]
    }

    pub(super) fn segment_at(coordinates: &[XY], [start, end]: [NodeId; 2]) -> Segment {
        Segment {
            start: Self::xy_at(coordinates, start),
            end: Self::xy_at(coordinates, end),
        }
    }
}

/// One output's chords and points. Areal boundary chords form the initial
/// `areal_chords` prefix; face-bearing emissions retain fan spans and host
/// labels for every chord and point.
pub(super) struct Emission {
    pub(super) items: Vec<[NodeId; 2]>,
    pub(super) chords: usize,
    pub(super) areal_chords: usize,
    pub(super) fan_spans: Vec<[u32; 2]>,
    pub(super) faces: Option<Vec<[u32; 2]>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Witness {
    Collision(NodeId, NodeId),
    Contact {
        left_emission: usize,
        left: usize,
        right_emission: usize,
        right: usize,
    },
    Fan(NodeId),
    Face {
        emission: usize,
        root: NodeId,
        hit: Option<usize>,
    },
}

impl Witness {
    fn into_error(self) -> crate::error::Error {
        match self {
            Self::Collision(first, second) => GeometryErrorKind::ExactNodeCollision {
                first: first.0,
                second: second.0,
            }
            .into(),
            Self::Contact { left, right, .. } => GeometryErrorKind::ExactUnexpectedContact {
                first: left,
                second: right,
            }
            .into(),
            Self::Fan(node) => GeometryErrorKind::ExactCyclicOrder { node: node.0 }.into(),
            Self::Face { root, .. } => GeometryErrorKind::ExactGlobalFace { node: root.0 }.into(),
        }
    }
}

pub(super) trait Sink {
    fn report(&mut self, witness: Witness) -> ControlFlow<()>;
}

#[derive(Default)]
pub(super) struct FirstFailure {
    witness: Option<Witness>,
}

impl FirstFailure {
    fn witness(&self) -> Witness {
        self.witness
            .expect("a broken certificate reports its witness")
    }

    #[cfg(test)]
    fn into_error(self) -> crate::error::Error {
        self.witness().into_error()
    }
}

impl Sink for FirstFailure {
    fn report(&mut self, witness: Witness) -> ControlFlow<()> {
        debug_assert!(self.witness.is_none());
        self.witness = Some(witness);
        ControlFlow::Break(())
    }
}

struct Collect<'p, 'source, 'e, 'emission> {
    partition: &'p ExactPartition<'source>,
    emissions: &'e [&'emission Emission],
    movable_cache: Vec<Option<bool>>,
    seed_seen: Vec<bool>,
    seeds: Vec<NodeId>,
    has_unmovable_witness: bool,
    error: Option<crate::error::Error>,
}

impl<'p, 'source, 'e, 'emission> Collect<'p, 'source, 'e, 'emission> {
    fn new(partition: &'p ExactPartition<'source>, emissions: &'e [&'emission Emission]) -> Self {
        let node_count = partition.node_count();
        Self {
            partition,
            emissions,
            movable_cache: vec![None; node_count],
            seed_seen: vec![false; node_count],
            seeds: Vec::new(),
            has_unmovable_witness: false,
            error: None,
        }
    }

    fn into_seed_state(self) -> Result<(Vec<NodeId>, Vec<Option<bool>>, bool)> {
        let Self {
            movable_cache,
            seeds,
            has_unmovable_witness,
            error,
            ..
        } = self;
        match error {
            Some(error) => Err(error),
            None => Ok((seeds, movable_cache, has_unmovable_witness)),
        }
    }
}

impl Sink for Collect<'_, '_, '_, '_> {
    fn report(&mut self, witness: Witness) -> ControlFlow<()> {
        if self.error.is_some() {
            return ControlFlow::Break(());
        }
        let mut has_movable_node = false;
        match for_each_witness_node(witness, self.emissions, |node| {
            if movable_node(self.partition, node, &mut self.movable_cache)? {
                has_movable_node = true;
                let seen = self
                    .seed_seen
                    .get_mut(node.0 as usize)
                    .expect("witness node belongs to the partition");
                if !mem::replace(seen, true) {
                    self.seeds.push(node);
                }
            }
            Ok(())
        }) {
            Ok(()) => {
                if !has_movable_node {
                    self.has_unmovable_witness = true;
                }
                ControlFlow::Continue(())
            },
            Err(error) => {
                self.error = Some(error);
                ControlFlow::Break(())
            },
        }
    }
}

fn visit_item_nodes(
    emissions: &[&Emission],
    emission: usize,
    item: usize,
    visit: &mut impl FnMut(NodeId) -> Result<()>,
) -> Result<()> {
    let [start, end] = emissions
        .get(emission)
        .expect("witness emission comes from certify")
        .items
        .get(item)
        .copied()
        .expect("witness item comes from certify");
    visit(start)?;
    visit(end)
}

#[expect(
    clippy::indexing_slicing,
    reason = "witness emission and item ordinals are produced by certify from these aligned emissions"
)]
fn for_each_witness_node(
    witness: Witness,
    emissions: &[&Emission],
    mut visit: impl FnMut(NodeId) -> Result<()>,
) -> Result<()> {
    match witness {
        Witness::Collision(first, second) => {
            visit(first)?;
            visit(second)?;
        },
        Witness::Contact {
            left_emission,
            left,
            right_emission,
            right,
        } => {
            visit_item_nodes(emissions, left_emission, left, &mut visit)?;
            visit_item_nodes(emissions, right_emission, right, &mut visit)?;
        },
        Witness::Fan(node) => {
            visit(node)?;
            for emission in emissions {
                for &[start, end] in emission
                    .items
                    .get(..emission.chords)
                    .expect("areal chord prefix is within its emission")
                {
                    if start == node {
                        visit(end)?;
                    } else if end == node {
                        visit(start)?;
                    }
                }
            }
        },
        Witness::Face {
            emission,
            root,
            hit,
        } => {
            visit(root)?;
            let output = emissions
                .get(emission)
                .expect("witness emission comes from certify");
            for &[start, end] in &output.items {
                if start == root || end == root {
                    visit(start)?;
                    visit(end)?;
                }
            }
            if let Some(hit) = hit {
                visit_item_nodes(emissions, emission, hit, &mut visit)?;
            }
        },
    }
    Ok(())
}

fn movable_node(
    partition: &ExactPartition<'_>,
    node: NodeId,
    cache: &mut [Option<bool>],
) -> Result<bool> {
    if let Some(movable) = cache.get(node.0 as usize).copied().flatten() {
        return Ok(movable);
    }
    let movable = if partition.is_stored(node) {
        false
    } else {
        let nearest = partition.nearest_xy(node);
        partition.compare_axis_to(node, Axis::X, nearest.x)? != Ordering::Equal
            || partition.compare_axis_to(node, Axis::Y, nearest.y)? != Ordering::Equal
    };
    *cache
        .get_mut(node.0 as usize)
        .expect("node ID belongs to the partition") = Some(movable);
    Ok(movable)
}

fn unique_emission_items(emissions: &[&Emission]) -> Result<Vec<(usize, usize)>> {
    let capacity = emissions.iter().try_fold(0_usize, |count, emission| {
        count
            .checked_add(emission.items.len())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    let mut items = Vec::with_capacity(capacity);
    let mut seen = (emissions.len() > 1)
        .then(|| try_hash_map_with_capacity::<[u32; 2], ()>(capacity))
        .transpose()?;
    for (emission_index, emission) in emissions.iter().enumerate() {
        for (item_index, &[start, end]) in emission.items.iter().enumerate() {
            if let Some(seen) = &mut seen {
                let key = [start.0.min(end.0), start.0.max(end.0)];
                if seen.insert(key, ()).is_some() {
                    continue;
                }
            }
            items.push((emission_index, item_index));
        }
    }
    Ok(items)
}

fn squared_displacement(
    partition: &ExactPartition<'_>,
    node: NodeId,
    candidate: XY,
) -> Result<ExactDyadic> {
    let point = partition.exact(node)?;
    let [x, y, weight] = point.as_ref();
    let x = ExactDyadic::from_f64(candidate.x)
        .try_product(weight)?
        .try_subtract_ref(x)?;
    let y = ExactDyadic::from_f64(candidate.y)
        .try_product(weight)?
        .try_subtract_ref(y)?;
    let xx = x.try_product(&x)?;
    let yy = y.try_product(&y)?;
    ExactDyadic::try_add_refs(&xx, &yy)
}

#[derive(Clone, Copy)]
struct PlanarChord {
    nodes: [NodeId; 2],
    nearest: Segment,
    sweep_bounds: Bounds,
}

#[derive(Clone)]
pub(super) struct PlanarClassState {
    pairs_by_rank: Vec<Vec<usize>>,
    fans_by_rank: Vec<Vec<usize>>,
}

pub(super) struct PlanarProblem<'p, 's, 'e> {
    partition: &'p ExactPartition<'s>,
    emissions: Vec<&'e Emission>,
    chords: Vec<PlanarChord>,
    item_chords: Vec<Vec<usize>>,
    live_nodes: Vec<NodeId>,
    classes: Vec<Vec<NodeId>>,
    node_location: Vec<Option<(usize, usize)>>,
    fan_rows: Vec<FanRow>,
    proximity: RefCell<Option<(Vec<(usize, usize)>, Vec<PlanarClassState>)>>,
    radius_cache: RefCell<Vec<Option<f64>>>,
    movable_cache: RefCell<Vec<Option<bool>>>,
    initial_evaluations: u64,
}

fn possible_chord_radius(partition: &ExactPartition<'_>, nodes: [NodeId; 2]) -> f64 {
    // This only broadens the sweep; exact movability and radius are resolved
    // for each candidate pair before it can join the closure.
    nodes
        .into_iter()
        .filter(|&node| !partition.is_stored(node))
        .map(|node| {
            let nearest = partition.nearest_xy(node);
            single_step_radius(nearest.x).max(single_step_radius(nearest.y))
        })
        .fold(0.0_f64, f64::max)
}

fn chord_movable_endpoints(
    partition: &ExactPartition<'_>,
    nodes: [NodeId; 2],
    movable_cache: &mut [Option<bool>],
) -> Result<[Option<NodeId>; 2]> {
    let first = movable_node(partition, nodes[0], movable_cache)?.then_some(nodes[0]);
    let second = if nodes[0] == nodes[1] {
        None
    } else {
        movable_node(partition, nodes[1], movable_cache)?.then_some(nodes[1])
    };
    Ok([first, second])
}

fn exact_chord_radius(
    partition: &ExactPartition<'_>,
    chord_index: usize,
    chord: PlanarChord,
    radius_cache: &mut [Option<f64>],
    movable_cache: &mut [Option<bool>],
) -> Result<f64> {
    if let Some(radius) = radius_cache.get(chord_index).copied().flatten() {
        return Ok(radius);
    }
    let endpoints = chord_movable_endpoints(partition, chord.nodes, movable_cache)?;
    let mut radius = 0.0_f64;
    for node in endpoints.into_iter().flatten() {
        let nearest = partition.nearest_xy(node);
        radius = radius
            .max(single_step_radius(nearest.x))
            .max(single_step_radius(nearest.y));
    }
    *radius_cache
        .get_mut(chord_index)
        .expect("chord radius cache aligns with the chord table") = Some(radius);
    Ok(radius)
}

impl<'p, 's, 'e> PlanarProblem<'p, 's, 'e> {
    pub(super) fn new(
        partition: &'p ExactPartition<'s>,
        emissions: &[&'e Emission],
        seeds: &[NodeId],
        mut movable_cache: Vec<Option<bool>>,
    ) -> Result<Self> {
        let item_refs = unique_emission_items(emissions)?;
        let mut item_chords: Vec<Vec<usize>> = emissions
            .iter()
            .map(|emission| vec![usize::MAX; emission.items.len()])
            .collect();
        let mut live_nodes = Vec::new();
        let mut live = vec![false; partition.node_count()];
        let mut chords = Vec::with_capacity(item_refs.len());
        let mut chord_by_nodes =
            crate::collections::try_hash_map_with_capacity::<[u32; 2], usize>(item_refs.len())?;
        for (emission_index, item_index) in item_refs {
            let emission = emissions
                .get(emission_index)
                .expect("emission item reference belongs to its slice");
            let nodes = *emission
                .items
                .get(item_index)
                .expect("emission item reference belongs to its plan");
            for node in nodes {
                let was_live = live
                    .get_mut(node.0 as usize)
                    .expect("emitted node belongs to the partition");
                if !mem::replace(was_live, true) {
                    live_nodes.push(node);
                }
            }
            let nearest = Embedding::segment_at(Embedding::nearest(partition).coordinates(), nodes);
            let sweep_bounds = swept_bounds(nearest, possible_chord_radius(partition, nodes));
            let chord_index = chords.len();
            chords.push(PlanarChord {
                nodes,
                nearest,
                sweep_bounds,
            });
            *item_chords
                .get_mut(emission_index)
                .and_then(|items| items.get_mut(item_index))
                .expect("item-to-chord table aligns with emitted items") = chord_index;
            chord_by_nodes
                .entry([nodes[0].0.min(nodes[1].0), nodes[0].0.max(nodes[1].0)])
                .or_insert(chord_index);
        }
        for (emission_index, emission) in emissions.iter().enumerate() {
            for (item_index, nodes) in emission.items.iter().enumerate() {
                let mapping = item_chords
                    .get_mut(emission_index)
                    .and_then(|items| items.get_mut(item_index))
                    .expect("item-to-chord table aligns with emitted items");
                if *mapping == usize::MAX {
                    *mapping = *chord_by_nodes
                        .get(&[nodes[0].0.min(nodes[1].0), nodes[0].0.max(nodes[1].0)])
                        .expect("deduplicated item has a canonical emitted chord");
                }
            }
        }
        live_nodes.sort_unstable_by_key(|node| node.0);
        if movable_cache.len() < partition.node_count() {
            movable_cache.resize(partition.node_count(), None);
        }
        let mut incident_chords = Vec::new();
        incident_chords.resize_with(partition.node_count(), Vec::new);
        for (chord_index, chord) in chords.iter().enumerate() {
            incident_chords[chord.nodes[0].0 as usize].push(chord_index);
            if chord.nodes[1] != chord.nodes[0] {
                incident_chords[chord.nodes[1].0 as usize].push(chord_index);
            }
        }
        let (closure, initial_evaluations, mut union, radius_cache) = build_proximity_closure(
            partition,
            &chords,
            &incident_chords,
            seeds,
            &mut movable_cache,
        )?;
        let (classes, node_location) = build_proximity_classes(partition, closure, &mut union)?;
        let emissions = emissions.to_vec();
        let fan_rows = collect_fan_rows(partition, &emissions);
        Ok(Self {
            partition,
            emissions,
            chords,
            item_chords,
            live_nodes,
            classes,
            node_location,
            fan_rows,
            proximity: RefCell::new(None),
            radius_cache: RefCell::new(radius_cache),
            movable_cache: RefCell::new(movable_cache),
            initial_evaluations,
        })
    }

    fn check_local_pairs(
        &self,
        class_state: &PlanarClassState,
        class: usize,
        rank: usize,
        assigned: &[Option<XY>],
        budget: &mut EvaluationBudget,
    ) -> Result<bool> {
        let proximity = self.proximity.borrow();
        let proximity_pairs = &proximity
            .as_ref()
            .expect("proximity state was prepared before class search")
            .0;
        for &pair_index in class_state
            .pairs_by_rank
            .get(rank)
            .expect("rank belongs to its class")
        {
            budget.charge(1)?;
            let &(left, right) = proximity_pairs
                .get(pair_index)
                .expect("class pair index belongs to the problem");
            let left_chord = self.chords[left];
            let right_chord = self.chords[right];
            if !emitted_pair_preserves_incidence(
                [
                    (
                        left_chord.nodes[0],
                        self.local_xy(left_chord.nodes[0], class, assigned),
                    ),
                    (
                        left_chord.nodes[1],
                        self.local_xy(left_chord.nodes[1], class, assigned),
                    ),
                ],
                [
                    (
                        right_chord.nodes[0],
                        self.local_xy(right_chord.nodes[0], class, assigned),
                    ),
                    (
                        right_chord.nodes[1],
                        self.local_xy(right_chord.nodes[1], class, assigned),
                    ),
                ],
            ) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn check_local_fans(
        &self,
        class_state: &PlanarClassState,
        class: usize,
        rank: usize,
        assigned: &[Option<XY>],
        budget: &mut EvaluationBudget,
    ) -> Result<bool> {
        // Prune local fan inversions before G composes the candidate face table.
        for &row_index in class_state
            .fans_by_rank
            .get(rank)
            .expect("rank belongs to its class")
        {
            budget.charge(1)?;
            let row = self
                .fan_rows
                .get(row_index)
                .expect("class fan index belongs to the problem");
            let emission = self
                .emissions
                .get(row.emission)
                .expect("fan row belongs to an emission");
            let items = emission
                .items
                .get(..emission.chords)
                .expect("emission chord prefix is within its items");
            if !fan_ok(row.origin, items, &row.departures, |node| {
                self.local_xy(node, class, assigned)
            }) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn check_local_without_fans(
        &self,
        class_state: &PlanarClassState,
        class: usize,
        _variables: &[NodeId],
        rank: usize,
        assigned: &[Option<XY>],
        budget: &mut EvaluationBudget,
    ) -> Result<bool> {
        self.check_local_pairs(class_state, class, rank, assigned, budget)
    }

    fn chord_has_member(&self, chord: PlanarChord) -> bool {
        chord.nodes.into_iter().any(|node| {
            self.node_location
                .get(node.0 as usize)
                .copied()
                .flatten()
                .is_some()
        })
    }

    fn prepare_proximity(&self, budget: &mut EvaluationBudget) -> Result<()> {
        if self.proximity.borrow().is_some() {
            return Ok(());
        }
        budget.charge(
            u64::try_from(self.chords.len().div_ceil(16))
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut left_indices = Vec::new();
        let mut left_bounds = Vec::new();
        let all_bounds: Vec<_> = self.chords.iter().map(|chord| chord.sweep_bounds).collect();
        for (index, &chord) in self.chords.iter().enumerate() {
            if self.chord_has_member(chord) {
                left_indices.push(index);
                left_bounds.push(chord.sweep_bounds);
            }
        }
        let mut proximity_pairs = Vec::new();
        let mut radius_cache = self.radius_cache.borrow_mut();
        let mut movable_cache = self.movable_cache.borrow_mut();
        let mut callback_error = None;
        let flow =
            for_each_overlapping_bounds_cross_pair(&left_bounds, &all_bounds, |left, right| {
                let left_index = left_indices[left];
                if left_index == right {
                    return ControlFlow::Continue(());
                }
                let left_chord = self.chords[left_index];
                let right_chord = self.chords[right];
                let left_radius = match exact_chord_radius(
                    self.partition,
                    left_index,
                    left_chord,
                    &mut radius_cache,
                    &mut movable_cache,
                ) {
                    Ok(radius) => radius,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                let right_radius = match exact_chord_radius(
                    self.partition,
                    right,
                    right_chord,
                    &mut radius_cache,
                    &mut movable_cache,
                ) {
                    Ok(radius) => radius,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                if segments_within_chebyshev(
                    left_chord.nearest,
                    right_chord.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    proximity_pairs.push((left_index.min(right), left_index.max(right)));
                }
                ControlFlow::Continue(())
            })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(flow.is_continue());
        proximity_pairs.sort_unstable();
        proximity_pairs.dedup();
        let class_states = build_class_states(
            &self.classes,
            &self.node_location,
            &self.chords,
            &proximity_pairs,
            &self.emissions,
            &self.fan_rows,
        );
        *self.proximity.borrow_mut() = Some((proximity_pairs, class_states));
        Ok(())
    }

    fn local_xy(&self, node: NodeId, class: usize, assigned: &[Option<XY>]) -> XY {
        match self.node_location.get(node.0 as usize).copied().flatten() {
            Some((owner, rank)) if owner == class => assigned
                .get(rank)
                .copied()
                .flatten()
                .expect("ready local predicates have every class endpoint assigned"),
            _ => self.partition.nearest_xy(node),
        }
    }

    fn mark_blame_for_chord(&self, chord_index: usize, blamed: &mut [bool]) {
        let chord = self
            .chords
            .get(chord_index)
            .expect("face witness names a chord in this problem");
        for node in chord.nodes {
            if let Some((class, _)) = self.node_location.get(node.0 as usize).copied().flatten() {
                *blamed
                    .get_mut(class)
                    .expect("class ID belongs to the problem") = true;
            }
        }
    }
}

fn build_proximity_closure(
    partition: &ExactPartition<'_>,
    chords: &[PlanarChord],
    incident_chords: &[Vec<usize>],
    seeds: &[NodeId],
    movable_cache: &mut [Option<bool>],
) -> Result<(Vec<NodeId>, u64, UnionFind, Vec<Option<f64>>)> {
    let node_count = partition.node_count();
    let mut closure = vec![false; node_count];
    let mut closure_nodes = Vec::new();
    for &node in seeds {
        if !closure[node.0 as usize] {
            closure[node.0 as usize] = true;
            closure_nodes.push(node);
        }
    }
    let mut frontier_nodes = closure_nodes.clone();
    let mut radius_cache = vec![None; chords.len()];
    let mut union = UnionFind::new(node_count);
    let all_bounds: Vec<_> = chords.iter().map(|chord| chord.sweep_bounds).collect();
    let mut frontier_seen = vec![false; chords.len()];
    let mut initial_evaluations = 0_u64;
    while !frontier_nodes.is_empty() {
        let mut frontier_chords = Vec::new();
        for node in frontier_nodes.drain(..) {
            for &chord in incident_chords
                .get(node.0 as usize)
                .expect("frontier node belongs to the partition")
            {
                let seen = frontier_seen
                    .get_mut(chord)
                    .expect("incident chord belongs to the chord table");
                if !mem::replace(seen, true) {
                    frontier_chords.push(chord);
                }
            }
        }
        for &chord in &frontier_chords {
            *frontier_seen
                .get_mut(chord)
                .expect("frontier chord belongs to the chord table") = false;
        }
        frontier_chords.sort_unstable();
        if frontier_chords.is_empty() {
            break;
        }
        let left_bounds: Vec<_> = frontier_chords
            .iter()
            .map(|&index| {
                chords
                    .get(index)
                    .expect("frontier chord belongs to the chord table")
                    .sweep_bounds
            })
            .collect();
        let sweep_cost =
            u64::try_from(chords.len().div_ceil(16)).map_err(|_| ResourceError::SizeOverflow)?;
        initial_evaluations = initial_evaluations
            .checked_add(sweep_cost)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut newly_reached = Vec::new();
        let mut callback_error = None;
        let flow = for_each_overlapping_bounds_cross_pair(
            &left_bounds,
            &all_bounds,
            |left_index, right_index| {
                let left_chord_index = frontier_chords[left_index];
                let left = chords[left_chord_index];
                let right = chords[right_index];
                let left_radius = match exact_chord_radius(
                    partition,
                    left_chord_index,
                    left,
                    &mut radius_cache,
                    movable_cache,
                ) {
                    Ok(radius) => radius,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                let right_radius = match exact_chord_radius(
                    partition,
                    right_index,
                    right,
                    &mut radius_cache,
                    movable_cache,
                ) {
                    Ok(radius) => radius,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                if !segments_within_chebyshev(
                    left.nearest,
                    right.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    return ControlFlow::Continue(());
                }
                let left_movable =
                    match chord_movable_endpoints(partition, left.nodes, movable_cache) {
                        Ok(nodes) => nodes,
                        Err(error) => {
                            callback_error = Some(error);
                            return ControlFlow::Break(());
                        },
                    };
                let right_movable =
                    match chord_movable_endpoints(partition, right.nodes, movable_cache) {
                        Ok(nodes) => nodes,
                        Err(error) => {
                            callback_error = Some(error);
                            return ControlFlow::Break(());
                        },
                    };
                for node in left_movable
                    .into_iter()
                    .flatten()
                    .chain(right_movable.into_iter().flatten())
                {
                    let reached = closure
                        .get_mut(node.0 as usize)
                        .expect("chord endpoint belongs to the partition");
                    if !mem::replace(reached, true) {
                        closure_nodes.push(node);
                        newly_reached.push(node);
                    }
                }
                for left_node in left_movable.into_iter().flatten() {
                    for right_node in right_movable.into_iter().flatten() {
                        union.union(left_node.0 as usize, right_node.0 as usize);
                    }
                }
                ControlFlow::Continue(())
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(flow.is_continue());
        frontier_nodes = newly_reached;
    }
    closure_nodes.sort_unstable_by_key(|node| node.0);
    Ok((closure_nodes, initial_evaluations, union, radius_cache))
}

fn build_proximity_classes(
    partition: &ExactPartition<'_>,
    members: Vec<NodeId>,
    union: &mut UnionFind,
) -> Result<(Vec<Vec<NodeId>>, Vec<Option<(usize, usize)>>)> {
    let mut components: HashMap<usize, Vec<NodeId>> =
        crate::collections::try_hash_map_with_capacity(members.len())?;
    for node in members {
        let root = union.find(node.0 as usize);
        components.entry(root).or_default().push(node);
    }
    let mut classes: Vec<_> = components.into_values().collect();
    for class in &mut classes {
        try_sort_by(class, |left, right| partition.compare_nodes(*left, *right))?;
    }
    try_sort_by(&mut classes, |left, right| {
        partition.compare_nodes(
            *left.first().expect("proximity class is nonempty"),
            *right.first().expect("proximity class is nonempty"),
        )
    })?;
    let mut node_location = vec![None; partition.node_count()];
    for (class, members) in classes.iter().enumerate() {
        for (rank, &node) in members.iter().enumerate() {
            *node_location
                .get_mut(node.0 as usize)
                .expect("class member belongs to the partition") = Some((class, rank));
        }
    }
    Ok((classes, node_location))
}

struct Injectivity {
    owners: crate::collections::HashMap<PointKey, NodeId>,
}

impl Injectivity {
    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            owners: try_hash_map_with_capacity(capacity)?,
        })
    }

    fn witness(&mut self, node: NodeId, coordinate: XY) -> Option<Witness> {
        self.owners
            .insert(PointKey::new(coordinate), node)
            .filter(|&first| first != node)
            .map(|first| Witness::Collision(first, node))
    }
}

/// Certify one shared coordinate table against every emission.
#[expect(
    clippy::indexing_slicing,
    reason = "emission item, segment, span, face, and node IDs are built from the same partition and aligned emission"
)]
pub(super) fn certify<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    emissions: &[&Emission],
    segments: &[&[Segment]],
    sink: &mut S,
) -> Result<ControlFlow<()>> {
    debug_assert_eq!(emissions.len(), segments.len());
    let mut prior_witness = false;

    let mut seen = Vec::with_capacity(partition.node_count());
    seen.resize(partition.node_count(), false);
    let mut injectivity = Injectivity::new(
        emissions
            .iter()
            .map(|emission| emission.items.len())
            .fold(0, usize::wrapping_add),
    )?;
    for emission in emissions {
        for &[start, end] in &emission.items {
            for node in [start, end] {
                if mem::replace(&mut seen[node.0 as usize], true) {
                    continue;
                }
                if let Some(witness) = injectivity.witness(node, embedding.xy(node)) {
                    let flow = sink.report(witness);
                    if flow.is_break() {
                        return Ok(flow);
                    }
                    prior_witness = true;
                }
            }
        }
    }

    let contact_items = unique_emission_items(emissions)?;
    let mut contact_segments = Vec::with_capacity(contact_items.len());
    for &(emission_index, item_index) in &contact_items {
        contact_segments.push(segments[emission_index][item_index]);
    }
    if for_each_candidate_pair(&contact_segments, single_chain, |left, right| {
        let (left_emission, left_item) = contact_items[left];
        let (right_emission, right_item) = contact_items[right];
        let left_nodes = emissions[left_emission].items[left_item];
        let right_nodes = emissions[right_emission].items[right_item];
        let left_segment = contact_segments[left];
        let right_segment = contact_segments[right];
        let endpoints = |[start, end]: [NodeId; 2], segment: Segment| {
            [(start, segment.start), (end, segment.end)]
        };
        if emitted_pair_preserves_incidence(
            endpoints(left_nodes, left_segment),
            endpoints(right_nodes, right_segment),
        ) {
            ControlFlow::Continue(())
        } else {
            let flow = sink.report(Witness::Contact {
                left_emission,
                left: left_item,
                right_emission,
                right: right_item,
            });
            if flow.is_continue() {
                prior_witness = true;
            }
            flow
        }
    })?
    .is_break()
    {
        return Ok(ControlFlow::Break(()));
    }

    for (emission_index, emission) in emissions.iter().enumerate() {
        debug_assert_eq!(emission.faces.is_some(), emission.areal_chords > 0);
        if emission.faces.is_none() {
            continue;
        }
        debug_assert!(emission.areal_chords <= emission.chords);
        debug_assert_eq!(emission.chords, emission.fan_spans.len());
        debug_assert!(emission.chords <= segments[emission_index].len());
        let flow = visit_fans(
            partition,
            embedding,
            &emission.items[..emission.chords],
            &emission.fan_spans,
            &segments[emission_index][..emission.chords],
            emission.areal_chords,
            &mut prior_witness,
            sink,
        );
        if flow.is_break() {
            return Ok(flow);
        }
    }

    if prior_witness {
        return Ok(ControlFlow::Continue(()));
    }

    for (emission_index, emission) in emissions.iter().enumerate() {
        let Some(faces) = &emission.faces else {
            continue;
        };
        debug_assert_eq!(emission.items.len(), faces.len());
        debug_assert_eq!(emission.items.len(), segments[emission_index].len());
        let flow = visit_faces(
            partition,
            embedding,
            &emission.items,
            faces,
            segments[emission_index],
            emission_index,
            sink,
        )?;
        if flow.is_break() {
            return Ok(flow);
        }
    }
    Ok(ControlFlow::Continue(()))
}

pub(super) fn rounding_moves(partition: &ExactPartition<'_>, items: &[[NodeId; 2]]) -> bool {
    let moved = items
        .iter()
        .flatten()
        .any(|&node| !partition.is_stored(node));
    #[cfg(test)]
    if !moved {
        super::metrics::update(|metrics| {
            metrics.unmoved_emissions = metrics.unmoved_emissions.wrapping_add(1);
        });
    }
    moved
}

impl ExactPartition<'_> {
    /// Return one nearest-coordinate table only after every emission passes its
    /// complete certificate.
    pub(super) fn embed<'p>(
        &'p self,
        emissions: &[&Emission],
        operation: &'static str,
    ) -> Result<Embedding<'p>> {
        let embedding = Embedding::nearest(self);
        let mut needs_certificate = false;
        for emission in emissions {
            needs_certificate |= rounding_moves(self, &emission.items);
        }
        if needs_certificate {
            let coordinates = embedding.coordinates();
            let segments: Vec<Vec<_>> = emissions
                .iter()
                .map(|emission| {
                    emission
                        .items
                        .iter()
                        .map(|&item| Embedding::segment_at(coordinates, item))
                        .collect()
                })
                .collect();
            let segment_views: Vec<_> = segments.iter().map(Vec::as_slice).collect();
            let mut first_failure = FirstFailure::default();
            if certify(
                self,
                &embedding,
                emissions,
                &segment_views,
                &mut first_failure,
            )?
            .is_break()
            {
                let first_witness = first_failure.witness();
                let mut collect = Collect::new(self, emissions);
                let _ = certify(self, &embedding, emissions, &segment_views, &mut collect)?;
                let (mut seeds, movable_cache, has_unmovable_witness) =
                    collect.into_seed_state()?;
                if has_unmovable_witness {
                    return Err(first_witness.into_error());
                }
                seeds.sort_unstable_by_key(|node| node.0);
                if seeds.is_empty() {
                    return Err(first_witness.into_error());
                }
                let problem = PlanarProblem::new(self, emissions, &seeds, movable_cache)?;
                let constructed = problem.classes.iter().try_fold(0_usize, |count, class| {
                    count
                        .checked_add(class.len())
                        .ok_or(ResourceError::SizeOverflow)
                })?;
                let assignment = Search::new(problem, operation, constructed)?.run()?;
                let mut coordinates = embedding.coordinates().to_vec();
                for class in assignment.classes {
                    for (node, candidate) in class.variables.into_iter().zip(class.values) {
                        *coordinates
                            .get_mut(node.0 as usize)
                            .expect("search assignment belongs to the partition") = candidate;
                    }
                }
                return Ok(Embedding::from_owned(coordinates));
            }
        }
        Ok(embedding)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each selected span ID comes from this ExactPartition's span domain"
    )]
    pub(super) fn span_emission(&self, spans: impl IntoIterator<Item = u32>) -> Emission {
        let mut items = Vec::new();
        for span_id in spans {
            let span = self.spans[span_id as usize];
            items.push([span.start, span.end]);
        }
        let chords = items.len();
        Emission {
            items,
            chords,
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        }
    }
}

#[cfg(test)]
pub(super) fn check_node_injectivity(nodes: &[NodeId], coordinates: &[XY]) -> Result<()> {
    debug_assert_eq!(nodes.len(), coordinates.len());
    let mut injectivity = Injectivity::new(nodes.len())?;
    for (&node, &coordinate) in nodes.iter().zip(coordinates) {
        if let Some(witness) = injectivity.witness(node, coordinate) {
            return Err(witness.into_error());
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn check_emitted_fans(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    fan_spans: &[[u32; 2]],
    segments: &[Segment],
    areal_chords: usize,
) -> Result<()> {
    let mut sink = FirstFailure::default();
    let mut had_witness = false;
    if visit_fans(
        partition,
        embedding,
        items,
        fan_spans,
        segments,
        areal_chords,
        &mut had_witness,
        &mut sink,
    )
    .is_break()
    {
        return Err(sink.into_error());
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct FanDeparture {
    span: u32,
    item: usize,
    at_start: bool,
}

struct FanRow {
    emission: usize,
    origin: NodeId,
    departures: Vec<FanDeparture>,
}

fn for_each_fan_row(
    partition: &ExactPartition<'_>,
    items: &[[NodeId; 2]],
    fan_spans: &[[u32; 2]],
    areal_chords: usize,
    node_count: usize,
    mut visit: impl FnMut(NodeId, &[FanDeparture]) -> ControlFlow<()>,
) -> ControlFlow<()> {
    debug_assert_eq!(items.len(), fan_spans.len());
    debug_assert!(areal_chords <= items.len());
    let mut areal_nodes = vec![false; node_count];
    for &[start, end] in &items[..areal_chords] {
        areal_nodes[start.0 as usize] = true;
        areal_nodes[end.0 as usize] = true;
    }
    let mut starts = vec![0_usize; node_count.wrapping_add(1)];
    for &[start, end] in items {
        let start = start.0 as usize;
        let end = end.0 as usize;
        starts[start.wrapping_add(1)] = starts[start.wrapping_add(1)].wrapping_add(1);
        starts[end.wrapping_add(1)] = starts[end.wrapping_add(1)].wrapping_add(1);
    }
    let mut total = 0_usize;
    for offset in starts.iter_mut().skip(1) {
        total = total.wrapping_add(*offset);
        *offset = total;
    }
    let mut cursor = starts[..node_count].to_vec();
    let mut departures = Vec::new();
    departures.resize(total, FanDeparture::default());
    for (item, &[start, end]) in items.iter().enumerate() {
        let [start_span, end_span] = fan_spans[item];
        let start_position = cursor[start.0 as usize];
        departures[start_position] = FanDeparture {
            span: start_span,
            item,
            at_start: true,
        };
        cursor[start.0 as usize] = start_position.wrapping_add(1);
        let end_position = cursor[end.0 as usize];
        departures[end_position] = FanDeparture {
            span: end_span,
            item,
            at_start: false,
        };
        cursor[end.0 as usize] = end_position.wrapping_add(1);
    }
    for node in 0..node_count {
        let range = starts[node]..starts[node.wrapping_add(1)];
        if !areal_nodes[node] || range.len() <= 2 {
            continue;
        }
        let origin = NodeId(node as u32);
        let row = &mut departures[range];
        row.sort_unstable_by(|left, right| {
            partition.compare_departures(origin, left.span as usize, right.span as usize)
        });
        if visit(origin, row).is_break() {
            return ControlFlow::Break(());
        }
    }
    ControlFlow::Continue(())
}

fn fan_ok(
    origin: NodeId,
    items: &[[NodeId; 2]],
    row: &[FanDeparture],
    mut xy: impl FnMut(NodeId) -> XY,
) -> bool {
    let origin_xy = xy(origin);
    let mut first: Option<(NodeId, XY)> = None;
    let mut previous: Option<(NodeId, XY)> = None;
    let mut descents = 0_usize;
    for &departure in row {
        let [start, end] = *items
            .get(departure.item)
            .expect("fan departure belongs to its emission");
        let target = if departure.at_start { end } else { start };
        let point = xy(target);
        first.get_or_insert((target, point));
        if let Some((last_target, last_point)) = previous {
            descents = descents.wrapping_add(usize::from(
                compare_departures(origin_xy, last_point, last_target.0, point, target.0)
                    == Ordering::Greater,
            ));
        }
        previous = Some((target, point));
    }
    let Some((first_target, first_point)) = first else {
        return false;
    };
    let Some((last_target, last_point)) = previous else {
        return false;
    };
    descents = descents.wrapping_add(usize::from(
        compare_departures(
            origin_xy,
            last_point,
            last_target.0,
            first_point,
            first_target.0,
        ) == Ordering::Greater,
    ));
    descents == 1
}

fn collect_fan_rows(partition: &ExactPartition<'_>, emissions: &[&Emission]) -> Vec<FanRow> {
    let mut rows = Vec::new();
    for (emission_index, emission) in emissions.iter().enumerate() {
        if emission.faces.is_none() {
            continue;
        }
        let items = emission
            .items
            .get(..emission.chords)
            .expect("emission chord prefix is within its items");
        let flow = for_each_fan_row(
            partition,
            items,
            &emission.fan_spans,
            emission.areal_chords,
            partition.node_count(),
            |origin, departures| {
                rows.push(FanRow {
                    emission: emission_index,
                    origin,
                    departures: departures.to_vec(),
                });
                ControlFlow::Continue(())
            },
        );
        debug_assert!(flow.is_continue());
    }
    rows
}

fn include_class_rank(
    node: NodeId,
    node_location: &[Option<(usize, usize)>],
    owner: &mut Option<usize>,
    ready_rank: &mut usize,
) {
    if let Some((class, rank)) = node_location.get(node.0 as usize).copied().flatten() {
        debug_assert!(owner.is_none_or(|prior| prior == class));
        *owner = Some(class);
        *ready_rank = (*ready_rank).max(rank);
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "class IDs and readiness ranks are derived from the same location table used to size these rows"
)]
fn build_class_states(
    classes: &[Vec<NodeId>],
    node_location: &[Option<(usize, usize)>],
    chords: &[PlanarChord],
    proximity_pairs: &[(usize, usize)],
    emissions: &[&Emission],
    fan_rows: &[FanRow],
) -> Vec<PlanarClassState> {
    let mut states: Vec<_> = classes
        .iter()
        .map(|members| PlanarClassState {
            pairs_by_rank: (0..members.len()).map(|_| Vec::new()).collect(),
            fans_by_rank: (0..members.len()).map(|_| Vec::new()).collect(),
        })
        .collect();
    for (pair_index, &(left_index, right_index)) in proximity_pairs.iter().enumerate() {
        let left = chords
            .get(left_index)
            .expect("proximity pair names a chord in this problem");
        let right = chords
            .get(right_index)
            .expect("proximity pair names a chord in this problem");
        let mut owner = None;
        let mut ready_rank = 0;
        for node in left.nodes.into_iter().chain(right.nodes) {
            include_class_rank(node, node_location, &mut owner, &mut ready_rank);
        }
        if let Some(class) = owner {
            states[class].pairs_by_rank[ready_rank].push(pair_index);
        }
    }
    for (row_index, row) in fan_rows.iter().enumerate() {
        let emission = emissions
            .get(row.emission)
            .expect("fan row belongs to an emission");
        let items = emission
            .items
            .get(..emission.chords)
            .expect("emission chord prefix is within its items");
        let mut owner = None;
        let mut ready_rank = 0;
        include_class_rank(row.origin, node_location, &mut owner, &mut ready_rank);
        for departure in &row.departures {
            let [start, end] = *items
                .get(departure.item)
                .expect("fan departure belongs to its emission");
            include_class_rank(start, node_location, &mut owner, &mut ready_rank);
            include_class_rank(end, node_location, &mut owner, &mut ready_rank);
        }
        if let Some(class) = owner {
            states[class].fans_by_rank[ready_rank].push(row_index);
        }
    }
    states
}

impl Problem for PlanarProblem<'_, '_, '_> {
    type Node = NodeId;
    type Candidate = XY;
    type Key = PointKey;
    type ClassState = PlanarClassState;

    fn classes(&self) -> &[Vec<Self::Node>] {
        &self.classes
    }

    fn fixed_keys(&self, class: usize, _variables: &[Self::Node]) -> Result<Vec<Self::Key>> {
        let mut keys = Vec::with_capacity(self.live_nodes.len());
        for &node in &self.live_nodes {
            if self
                .node_location
                .get(node.0 as usize)
                .copied()
                .flatten()
                .is_none_or(|(owner, _)| owner != class)
            {
                keys.push(PointKey::new(self.partition.nearest_xy(node)));
            }
        }
        Ok(keys)
    }

    fn compare_nodes(&self, left: Self::Node, right: Self::Node) -> Result<Ordering> {
        self.partition.compare_nodes(left, right)
    }

    fn fill_candidates(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
    ) -> Result<()> {
        let nearest = self.partition.nearest_xy(node);
        let x_values = [nearest.x.next_down(), nearest.x, nearest.x.next_up()];
        let y_values = [nearest.y.next_down(), nearest.y, nearest.y.next_up()];
        let x_count =
            if self.partition.compare_axis_to(node, Axis::X, nearest.x)? == Ordering::Equal {
                1
            } else {
                3
            };
        let y_count =
            if self.partition.compare_axis_to(node, Axis::Y, nearest.y)? == Ordering::Equal {
                1
            } else {
                3
            };
        for &x in x_values
            .get(..x_count)
            .expect("candidate axis count is one or three")
        {
            for &y in y_values
                .get(..y_count)
                .expect("candidate axis count is one or three")
            {
                let candidate = XY::new(x, y);
                if self.partition.embedding_domain.admits(nearest, candidate) {
                    candidates.push(candidate);
                }
            }
        }
        Ok(())
    }

    fn compare_candidates(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
    ) -> Result<Ordering> {
        let order = squared_displacement(self.partition, node, left)?.cmp(&squared_displacement(
            self.partition,
            node,
            right,
        )?);
        Ok(order
            .then_with(|| left.x.total_cmp(&right.x))
            .then_with(|| left.y.total_cmp(&right.y)))
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        PointKey::new(candidate)
    }

    fn prepare_class(
        &self,
        class: usize,
        _variables: &[Self::Node],
        budget: &mut EvaluationBudget,
    ) -> Result<Self::ClassState> {
        self.prepare_proximity(budget)?;
        let prepared = self.proximity.borrow();
        Ok(prepared
            .as_ref()
            .expect("proximity state was prepared before class search")
            .1
            .get(class)
            .expect("class state belongs to the problem")
            .clone())
    }

    fn check_local(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        _variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<bool> {
        if !self.check_local_pairs(class_state, class, rank, assigned, budget)? {
            return Ok(false);
        }
        self.check_local_fans(class_state, class, rank, assigned, budget)
    }

    fn check_global(
        &self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<GlobalCheck> {
        if self
            .emissions
            .iter()
            .all(|emission| emission.faces.is_none())
        {
            return Ok(GlobalCheck::Pass);
        }
        let mut coordinates = Embedding::nearest(self.partition).coordinates().to_vec();
        for assignment in assignments {
            for (&node, &candidate) in assignment.variables.iter().zip(assignment.values) {
                *coordinates
                    .get_mut(node.0 as usize)
                    .expect("assignment node belongs to the partition") = candidate;
            }
        }
        let embedding = Embedding::from_owned(coordinates);
        for (emission_index, emission) in self.emissions.iter().enumerate() {
            let Some(faces) = &emission.faces else {
                continue;
            };
            budget.charge(
                u64::try_from(emission.items.len().div_ceil(16))
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let segments: Vec<_> = emission
                .items
                .iter()
                .map(|&item| Embedding::segment_at(embedding.coordinates(), item))
                .collect();
            let mut sink = FirstFailure::default();
            if visit_faces(
                self.partition,
                &embedding,
                &emission.items,
                faces,
                &segments,
                emission_index,
                &mut sink,
            )?
            .is_break()
            {
                let Witness::Face { root, hit, .. } = sink
                    .witness
                    .expect("a rejected face certificate records its witness")
                else {
                    unreachable!("global face checking emits only face witnesses");
                };
                let mut anchors = vec![false; self.chords.len()];
                for (item_index, nodes) in emission.items.iter().enumerate() {
                    if nodes.contains(&root) {
                        let chord = self.item_chords[emission_index][item_index];
                        anchors[chord] = true;
                    }
                }
                if let Some(hit) = hit {
                    let chord = self.item_chords[emission_index][hit];
                    anchors[chord] = true;
                }
                let mut blamed = vec![false; self.classes.len()];
                for (chord, &is_anchor) in anchors.iter().enumerate() {
                    if is_anchor {
                        self.mark_blame_for_chord(chord, &mut blamed);
                    }
                }
                let proximity = self.proximity.borrow();
                let proximity_pairs = &proximity
                    .as_ref()
                    .expect("proximity state was prepared before global search")
                    .0;
                for &(left, right) in proximity_pairs {
                    if anchors[left] {
                        self.mark_blame_for_chord(right, &mut blamed);
                    }
                    if anchors[right] {
                        self.mark_blame_for_chord(left, &mut blamed);
                    }
                }
                let classes = blamed
                    .into_iter()
                    .enumerate()
                    .filter_map(|(class, blamed)| blamed.then_some(class))
                    .collect();
                return Ok(GlobalCheck::Reject(classes));
            }
        }
        Ok(GlobalCheck::Pass)
    }

    fn initial_evaluations(&self) -> u64 {
        self.initial_evaluations
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "emitted item, span and node IDs are admitted by the same exact partition"
)]
fn visit_fans<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    fan_spans: &[[u32; 2]],
    segments: &[Segment],
    areal_chords: usize,
    had_witness: &mut bool,
    sink: &mut S,
) -> ControlFlow<()> {
    debug_assert_eq!(items.len(), fan_spans.len());
    debug_assert_eq!(items.len(), segments.len());
    debug_assert!(areal_chords <= items.len());
    for_each_fan_row(
        partition,
        items,
        fan_spans,
        areal_chords,
        partition.node_count(),
        |origin, row| {
            if fan_ok(origin, items, row, |node| embedding.xy(node)) {
                ControlFlow::Continue(())
            } else {
                *had_witness = true;
                sink.report(Witness::Fan(origin))
            }
        },
    )
}

#[cfg(test)]
pub(super) fn check_global_faces(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    sides: &[[u32; 2]],
    segments: &[Segment],
) -> Result<()> {
    let mut sink = FirstFailure::default();
    if visit_faces(partition, embedding, items, sides, segments, 0, &mut sink)?.is_break() {
        return Err(sink.into_error());
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::float_cmp,
    reason = "emitted node IDs index the certified coordinate table; face labels and item ordinals are built from those same items"
)]
fn visit_faces<S: Sink>(
    partition: &ExactPartition<'_>,
    embedding: &Embedding<'_>,
    items: &[[NodeId; 2]],
    sides: &[[u32; 2]],
    segments: &[Segment],
    emission: usize,
    sink: &mut S,
) -> Result<ControlFlow<()>> {
    debug_assert_eq!(sides.len(), items.len());
    debug_assert_eq!(segments.len(), items.len());
    let node_count = partition.node_count();
    let coordinates = embedding.coordinates();
    let incidence_rows = node_count.wrapping_add(1);
    let mut incident_starts = vec![0_usize; incidence_rows];
    let mut parents = Vec::with_capacity(node_count);
    parents.extend(0..node_count as u32);
    for (&[start, end], _) in items.iter().zip(segments) {
        let start_node = start.0 as usize;
        let start_offset = start_node.wrapping_add(1);
        incident_starts[start_offset] = incident_starts[start_offset].wrapping_add(1);
        if end != start {
            let end_node = end.0 as usize;
            let end_offset = end_node.wrapping_add(1);
            incident_starts[end_offset] = incident_starts[end_offset].wrapping_add(1);
        }
        let mut left = start.0;
        let mut right = end.0;
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
        for (index, &[start, end]) in items.iter().enumerate() {
            for node in [start, end]
                .into_iter()
                .take(if end == start { 1 } else { 2 })
            {
                let node = node.0 as usize;
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
        let point = coordinates[node];
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
            let [start, end] = items[index];
            let (target, target_point, face) = if start.0 as usize == node {
                (end.0, segments[index].end, sides[index][0])
            } else {
                (start.0, segments[index].start, sides[index][1])
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
    let hits = shoot_left(
        &EmittedPoints(coordinates),
        items.len(),
        |index| [items[index][0].0, items[index][1].0],
        &queries,
    )?;
    for ((&node, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
        let host = hit.map_or(0, |chord| {
            let segment = segments[chord];
            if segment.start.y < segment.end.y {
                sides[chord][1]
            } else {
                sides[chord][0]
            }
        });
        if host != exterior {
            let flow = sink.report(Witness::Face {
                emission,
                root: NodeId(node),
                hit,
            });
            if flow.is_break() {
                return Ok(flow);
            }
        }
    }
    Ok(ControlFlow::Continue(()))
}
