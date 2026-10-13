//! Index-range hulls and halfplane-constrained farthest-point queries.
//!
//! Segment distance is convex, but endpoint distance along a hull need not be
//! unimodal. Supporting-line extrema cover the interior projection regime;
//! each endpoint cap is a cyclic hull interval queried through farthest-site
//! point location. A source-order descent recovers discarded collinear ties.

use std::cmp::Ordering;
use std::hash::{BuildHasher as _, Hasher as _};
use std::ops::Range;

use super::farthest::Diagram;
use super::point;
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::segment_index::StoredHull;
use crate::geometry::segments::stored_cross_sign;
use crate::geometry::{
    Segment, SquaredDistanceKey, XY, point_pair_distance_key, point_segment_exact_distance_key,
    same_point,
};

const SCAN: usize = 16;

struct PointNode {
    range: Range<usize>,
    children: Option<[usize; 2]>,
    diagram: Option<Diagram>,
}

struct PointRanges {
    nodes: Vec<PointNode>,
}

impl PointRanges {
    fn new(
        vertices: &[usize],
        xs: &[f64],
        ys: &[f64],
        y_extent: [f64; 2],
        seed: u64,
    ) -> Result<Self> {
        let mut result = Self {
            nodes: Vec::with_capacity(vertices.len().wrapping_mul(2)),
        };
        result.build(vertices, xs, ys, y_extent, seed, 0..vertices.len())?;
        Ok(result)
    }

    fn build(
        &mut self,
        vertices: &[usize],
        xs: &[f64],
        ys: &[f64],
        y_extent: [f64; 2],
        seed: u64,
        range: Range<usize>,
    ) -> Result<usize> {
        let index = self.nodes.len();
        self.nodes.push(PointNode {
            range: range.clone(),
            children: None,
            diagram: None,
        });
        if range.len() > SCAN {
            let middle = range.start.wrapping_add(range.len() / 2);
            let left = self.build(vertices, xs, ys, y_extent, seed, range.start..middle)?;
            let right = self.build(vertices, xs, ys, y_extent, seed, middle..range.end)?;
            let diagram = Diagram::build(
                vertices.get(range).expect("hull interval"),
                xs,
                ys,
                y_extent,
                seed.wrapping_add(index as u64),
            )?;
            let node = self.nodes.get_mut(index).expect("hull interval node");
            node.children = Some([left, right]);
            node.diagram = Some(diagram);
        }
        Ok(index)
    }

    fn query(
        &self,
        node: usize,
        range: &Range<usize>,
        vertices: &[usize],
        query: XY,
        source: &Source<'_>,
        best: &mut Option<usize>,
    ) -> Result<()> {
        let node = self.nodes.get(node).expect("hull interval node");
        if node.range.end <= range.start || range.end <= node.range.start {
            return Ok(());
        }
        if range.start <= node.range.start
            && node.range.end <= range.end
            && let Some(diagram) = &node.diagram
        {
            let winner = diagram.query(
                query,
                vertices
                    .get(node.range.clone())
                    .expect("hull interval sites"),
                source.xs,
                source.ys,
            )?;
            return choose_pair(best, winner, query, source);
        }
        if let Some([left, right]) = node.children {
            self.query(left, range, vertices, query, source, best)?;
            self.query(right, range, vertices, query, source, best)
        } else {
            let start = range.start.max(node.range.start);
            let end = range.end.min(node.range.end);
            for &winner in vertices.get(start..end).expect("hull interval sites") {
                choose_pair(best, winner, query, source)?;
            }
            Ok(())
        }
    }
}

fn choose_pair(
    best: &mut Option<usize>,
    winner: usize,
    query: XY,
    source: &Source<'_>,
) -> Result<()> {
    if best.map_or(Ok(true), |incumbent| {
        point_pair_distance_key(query, source.at(winner))
            .cmp_distance(&point_pair_distance_key(query, source.at(incumbent)))
            .map(std::cmp::Ordering::is_gt)
    })? {
        *best = Some(winner);
    }
    Ok(())
}

struct Hull {
    outline: StoredHull<usize>,
    points: PointRanges,
}

struct Source<'a> {
    xs: &'a [f64],
    ys: &'a [f64],
}

impl Source<'_> {
    fn at(&self, ordinal: usize) -> XY {
        point(self.xs, self.ys, ordinal)
    }

    fn compare(&self, left: usize, right: usize) -> Ordering {
        let (left, right) = (self.at(left), self.at(right));
        left.x
            .partial_cmp(&right.x)
            .expect("finite source X")
            .then(left.y.partial_cmp(&right.y).expect("finite source Y"))
    }
}

fn rotated(point: XY) -> XY {
    XY::new(-point.y, point.x)
}

const fn negated(sign: ExactSign, negate: bool) -> ExactSign {
    if !negate {
        return sign;
    }
    match sign {
        ExactSign::Negative => ExactSign::Positive,
        ExactSign::Zero => ExactSign::Zero,
        ExactSign::Positive => ExactSign::Negative,
    }
}

impl Hull {
    fn build(sorted: &[usize], source: &Source<'_>, y_extent: [f64; 2], seed: u64) -> Result<Self> {
        let outline = StoredHull::new(sorted, |site| source.at(site));
        let points = PointRanges::new(&outline.vertices, source.xs, source.ys, y_extent, seed)?;
        Ok(Self { outline, points })
    }

    fn support<const DOT: bool>(&self, chord: Segment, negate: bool, source: &Source<'_>) -> usize {
        self.outline
            .support::<DOT>(chord, negate, |site| source.at(site))
    }

    fn cap(
        &self,
        chord: Segment,
        endpoint: XY,
        negate: bool,
        source: &Source<'_>,
    ) -> Result<Option<usize>> {
        let minimum = self.support::<true>(chord, !negate, source);
        let maximum = self.support::<true>(chord, negate, source);
        let inside = |position: usize| {
            let vertex = source.at(*self
                .outline
                .vertices
                .get(position)
                .expect("hull cap vertex"));
            negated(
                stored_cross_sign(chord.start, chord.end, rotated(endpoint), rotated(vertex)),
                negate,
            ) != ExactSign::Positive
        };
        if !inside(minimum) {
            return Ok(None);
        }
        let count = self.outline.vertices.len();
        let mut best = None;
        if inside(maximum) {
            self.points.query(
                0,
                &(0..count),
                &self.outline.vertices,
                endpoint,
                source,
                &mut best,
            )?;
            return Ok(best);
        }
        let span = |forward: bool| {
            let (from, to) = if forward {
                (minimum, maximum)
            } else {
                (maximum, minimum)
            };
            if to >= from {
                to.wrapping_sub(from)
            } else {
                count.wrapping_sub(from).wrapping_add(to)
            }
        };
        let position = |step: usize, forward: bool| {
            if forward {
                let remaining = count.wrapping_sub(minimum);
                if step < remaining {
                    minimum.wrapping_add(step)
                } else {
                    step.wrapping_sub(remaining)
                }
            } else if step <= minimum {
                minimum.wrapping_sub(step)
            } else {
                count.wrapping_sub(step.wrapping_sub(minimum))
            }
        };
        let last_inside = |forward: bool| {
            let (mut low, mut high) = (0_usize, span(forward));
            while high.wrapping_sub(low) > 1 {
                let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
                if inside(position(middle, forward)) {
                    low = middle;
                } else {
                    high = middle;
                }
            }
            position(low, forward)
        };
        let start = last_inside(false);
        let end = last_inside(true).wrapping_add(1);
        if start < end {
            self.points.query(
                0,
                &(start..end),
                &self.outline.vertices,
                endpoint,
                source,
                &mut best,
            )?;
        } else {
            self.points.query(
                0,
                &(start..count),
                &self.outline.vertices,
                endpoint,
                source,
                &mut best,
            )?;
            self.points.query(
                0,
                &(0..end),
                &self.outline.vertices,
                endpoint,
                source,
                &mut best,
            )?;
        }
        Ok(best)
    }

    fn maximum(&self, chord: Segment, source: &Source<'_>) -> Result<usize> {
        if same_point(chord.start, chord.end) {
            let mut best = None;
            self.points.query(
                0,
                &(0..self.outline.vertices.len()),
                &self.outline.vertices,
                chord.start,
                source,
                &mut best,
            )?;
            return Ok(best.expect("a hull contains a source point"));
        }
        let mut best = *self
            .outline
            .vertices
            .get(self.support::<false>(chord, false, source))
            .expect("hull supporting vertex");
        let opposite = *self
            .outline
            .vertices
            .get(self.support::<false>(chord, true, source))
            .expect("hull supporting vertex");
        if point_segment_exact_distance_key(source.at(opposite), chord)
            .cmp_distance(&point_segment_exact_distance_key(source.at(best), chord))?
            .is_gt()
        {
            best = opposite;
        }
        for (endpoint, negate) in [(chord.start, false), (chord.end, true)] {
            if let Some(candidate) = self.cap(chord, endpoint, negate, source)?
                && point_segment_exact_distance_key(source.at(candidate), chord)
                    .cmp_distance(&point_segment_exact_distance_key(source.at(best), chord))?
                    .is_gt()
            {
                best = candidate;
            }
        }
        Ok(best)
    }
}

struct Node {
    range: Range<usize>,
    children: Option<[usize; 2]>,
    hull: Hull,
}

pub(in crate::geometry::constructive::simplify) struct Index<'a> {
    source: Source<'a>,
    nodes: Vec<Node>,
}

impl<'a> Index<'a> {
    pub(in crate::geometry::constructive::simplify) fn new(
        xs: &'a [f64],
        ys: &'a [f64],
    ) -> Result<Self> {
        let source = Source { xs, ys };
        let minimum = ys.iter().copied().fold(f64::INFINITY, f64::min);
        let maximum = ys.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let seed = std::hash::RandomState::new().build_hasher().finish();
        // For nonempty input, bisection depth is at most ceil(log2(ceil(n / SCAN))).
        // The power-of-two leaf bound gives a full-tree node bound of 2L - 1.
        let leaf_capacity = xs.len().div_ceil(SCAN).next_power_of_two();
        let node_capacity = leaf_capacity.wrapping_mul(2).wrapping_sub(1);
        let mut result = Self {
            source,
            nodes: Vec::with_capacity(node_capacity),
        };
        let (_, root_order) = result.build(0..xs.len(), [minimum, maximum], seed)?;
        drop(root_order);
        Ok(result)
    }

    fn build(
        &mut self,
        range: Range<usize>,
        y_extent: [f64; 2],
        seed: u64,
    ) -> Result<(usize, Vec<usize>)> {
        let (children, mut sorted) = if range.len() <= SCAN {
            let mut sorted: Vec<_> = range.clone().collect();
            sorted.sort_unstable_by(|&left, &right| {
                self.source.compare(left, right).then(left.cmp(&right))
            });
            (None, sorted)
        } else {
            let middle = range.start.wrapping_add(range.len() / 2);
            let (left, left_order) = self.build(range.start..middle, y_extent, seed)?;
            let (right, right_order) = self.build(middle..range.end, y_extent, seed)?;
            let mut sorted = Vec::with_capacity(left_order.len().wrapping_add(right_order.len()));
            let (mut ai, mut bi) = (0, 0);
            while let (Some(&av), Some(&bv)) = (left_order.get(ai), right_order.get(bi)) {
                if self.source.compare(av, bv).then(av.cmp(&bv)).is_le() {
                    sorted.push(av);
                    ai = ai.wrapping_add(1);
                } else {
                    sorted.push(bv);
                    bi = bi.wrapping_add(1);
                }
            }
            sorted.extend_from_slice(left_order.get(ai..).expect("source hull merge suffix"));
            sorted.extend_from_slice(right_order.get(bi..).expect("source hull merge suffix"));
            (Some([left, right]), sorted)
        };
        sorted.dedup_by(|left, right| self.source.compare(*left, *right).is_eq());
        let index = self.nodes.len();
        let hull = Hull::build(
            &sorted,
            &self.source,
            y_extent,
            seed.wrapping_add(index as u64),
        )?;
        drop(sorted);
        let outline_order = hull
            .outline
            .sorted_sites(|site| self.source.at(site))
            .collect();
        self.nodes.push(Node {
            range,
            children,
            hull,
        });
        Ok((index, outline_order))
    }

    fn maximum(
        &self,
        node: usize,
        range: &Range<usize>,
        chord: Segment,
        best: &mut Option<usize>,
    ) -> Result<()> {
        let node = self.nodes.get(node).expect("source range node");
        if node.range.end <= range.start || range.end <= node.range.start {
            return Ok(());
        }
        if range.start <= node.range.start && node.range.end <= range.end {
            let winner = node.hull.maximum(chord, &self.source)?;
            if best.map_or(Ok(true), |incumbent| {
                point_segment_exact_distance_key(self.source.at(winner), chord)
                    .cmp_distance(&point_segment_exact_distance_key(
                        self.source.at(incumbent),
                        chord,
                    ))
                    .map(std::cmp::Ordering::is_gt)
            })? {
                *best = Some(winner);
            }
            return Ok(());
        }
        if let Some([left, right]) = node.children {
            self.maximum(left, range, chord, best)?;
            self.maximum(right, range, chord, best)
        } else {
            for winner in range.start.max(node.range.start)..range.end.min(node.range.end) {
                if best.map_or(Ok(true), |incumbent| {
                    point_segment_exact_distance_key(self.source.at(winner), chord)
                        .cmp_distance(&point_segment_exact_distance_key(
                            self.source.at(incumbent),
                            chord,
                        ))
                        .map(std::cmp::Ordering::is_gt)
                })? {
                    *best = Some(winner);
                }
            }
            Ok(())
        }
    }

    fn first(
        &self,
        node: usize,
        range: &Range<usize>,
        chord: Segment,
        maximum: &SquaredDistanceKey,
    ) -> Result<Option<usize>> {
        let node = self.nodes.get(node).expect("source range node");
        if node.range.end <= range.start || range.end <= node.range.start {
            return Ok(None);
        }
        if range.start <= node.range.start && node.range.end <= range.end {
            let winner = node.hull.maximum(chord, &self.source)?;
            if point_segment_exact_distance_key(self.source.at(winner), chord)
                .cmp_distance(maximum)?
                .is_lt()
            {
                return Ok(None);
            }
        }
        if let Some([left, right]) = node.children {
            if let Some(first) = self.first(left, range, chord, maximum)? {
                return Ok(Some(first));
            }
            self.first(right, range, chord, maximum)
        } else {
            for ordinal in range.start.max(node.range.start)..range.end.min(node.range.end) {
                if point_segment_exact_distance_key(self.source.at(ordinal), chord)
                    .cmp_distance(maximum)?
                    .is_eq()
                {
                    return Ok(Some(ordinal));
                }
            }
            Ok(None)
        }
    }

    pub(in crate::geometry::constructive::simplify) fn split(
        &self,
        start: usize,
        end: usize,
        tolerance: f64,
    ) -> Result<Option<usize>> {
        if !tolerance.is_finite() {
            return Ok(None);
        }
        if end.wrapping_sub(start) <= SCAN {
            return super::split(self.source.xs, self.source.ys, start, end, tolerance);
        }
        let range = start.wrapping_add(1)..end;
        let chord = Segment {
            start: self.source.at(start),
            end: self.source.at(end),
        };
        let root = self.nodes.len().wrapping_sub(1);
        let mut winner = None;
        self.maximum(root, &range, chord, &mut winner)?;
        let maximum = point_segment_exact_distance_key(
            self.source
                .at(winner.expect("a nonempty RDP range has a maximum")),
            chord,
        );
        let threshold = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(tolerance, 0.0));
        if !maximum.cmp_distance(&threshold)?.is_gt() {
            return Ok(None);
        }
        self.first(root, &range, chord, &maximum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(
        xs: &[f64],
        ys: &[f64],
        start: usize,
        end: usize,
        tolerance: f64,
    ) -> Option<usize> {
        let chord = Segment {
            start: point(xs, ys, start),
            end: point(xs, ys, end),
        };
        let mut winner = start.wrapping_add(1);
        let mut maximum = point_segment_exact_distance_key(point(xs, ys, winner), chord);
        for ordinal in start.wrapping_add(2)..end {
            let candidate = point_segment_exact_distance_key(point(xs, ys, ordinal), chord);
            if candidate.cmp_distance(&maximum).unwrap().is_gt() {
                winner = ordinal;
                maximum = candidate;
            }
        }
        maximum
            .cmp_distance(&point_pair_distance_key(
                XY::new(0.0, 0.0),
                XY::new(tolerance, 0.0),
            ))
            .unwrap()
            .is_gt()
            .then_some(winner)
    }

    #[test]
    fn index_matches_exact_scan_over_convex_arcs_and_endpoint_caps() {
        let xs: [f64; 64] =
            std::array::from_fn(|index| (index as f64 * std::f64::consts::TAU / 64.0).cos());
        let ys: [f64; 64] =
            std::array::from_fn(|index| (index as f64 * std::f64::consts::TAU / 64.0).sin());
        let index = Index::new(&xs, &ys).unwrap();
        for (start, end) in [(0, 63), (1, 62), (0, 32), (10, 45), (3, 60), (16, 48)] {
            for tolerance in [0.0, 0.01, 0.3, 1.0, 1.99999] {
                assert_eq!(
                    index.split(start, end, tolerance).unwrap(),
                    reference(&xs, &ys, start, end, tolerance),
                    "range {start}..{end} tolerance {tolerance}",
                );
            }
        }
    }

    #[test]
    fn source_descent_recovers_collinear_and_repeated_coordinate_ties() {
        let xs: [f64; 64] = std::array::from_fn(|index| index as f64);
        let ys: [f64; 64] = std::array::from_fn(|index| (index % 2) as f64);
        let index = Index::new(&xs, &ys).unwrap();
        for (start, end) in [(0, 63), (1, 63), (0, 32), (10, 45), (3, 60), (16, 48)] {
            assert_eq!(
                index.split(start, end, 0.1).unwrap(),
                reference(&xs, &ys, start, end, 0.1),
            );
        }
        let xs: [f64; 64] = std::array::from_fn(|index| (index % 4) as f64);
        let ys: [f64; 64] = std::array::from_fn(|index| if index % 4 == 1 { 1.0 } else { 0.0 });
        let index = Index::new(&xs, &ys).unwrap();
        for (start, end) in [(0, 63), (1, 63), (0, 32), (10, 45), (3, 60), (16, 48)] {
            assert_eq!(
                index.split(start, end, 0.1).unwrap(),
                reference(&xs, &ys, start, end, 0.1),
            );
        }
    }

    #[test]
    fn source_descent_keeps_the_first_equal_maximum_after_reversal() {
        let xs: [f64; 32] = std::array::from_fn(|index| index as f64);
        let ys: [f64; 32] =
            std::array::from_fn(|index| if index == 2 || index == 9 { 1.0 } else { 0.0 });
        let index = Index::new(&xs, &ys).unwrap();
        assert_eq!(index.split(0, 31, 0.5).unwrap(), Some(2));

        let mut reversed_xs = xs;
        reversed_xs.reverse();
        let mut reversed_ys = ys;
        reversed_ys.reverse();
        let reversed = Index::new(&reversed_xs, &reversed_ys).unwrap();
        assert_eq!(reversed.split(0, 31, 0.5).unwrap(), Some(22));
    }

    #[test]
    fn endpoint_caps_keep_the_farthest_source_in_wrapped_and_linear_windows() {
        let xs = [0.0, 0.0, 2.0];
        let ys = [0.0, 2.0, 1.0];
        let source = Source { xs: &xs, ys: &ys };
        let hull = Hull::build(&[0, 1, 2], &source, [0.0, 2.0], 0).unwrap();
        let chord = Segment {
            start: XY::new(0.5, 0.0),
            end: XY::new(1.5, 0.0),
        };

        assert_eq!(
            hull.cap(chord, chord.start, false, &source).unwrap(),
            Some(1)
        );
        assert_eq!(hull.cap(chord, chord.end, true, &source).unwrap(), Some(2));
    }
}
