use crate::HeapSize;
use crate::error::{ResourceError, Result};
use crate::geometry::facet_bvh::engine::bvh::refine_facet_nearest;
use crate::geometry::facet_bvh::{
    FACET_SEGMENTS, NearestCandidate, aabb_distance, aabb_distance_lower, aabb_distance_squared,
    aabb_distance_squared_lower, point_aabb,
};
use crate::geometry::{
    Bounds, Component, CoordSeq, Point, RunIdentity, Segment, Shape, XY, point_on_segment,
    point_segment_distance_squared, segments_intersect, xy_bounds_columns,
};

/// One run of up to [`FACET_SEGMENTS`] consecutive segments inside a single
/// chain: vertices `first_vertex ..= first_vertex + segment_count` (local
/// to that chain).
#[derive(Clone, Copy)]
pub(crate) struct Facet {
    pub(crate) chain: u32,
    /// Chain-local first vertex index — `u32` (not `usize`) halves the facet
    /// footprint so the BVH node/brute-scan arrays stay denser in cache. A
    /// single chain can never hold >4G vertices.
    pub(crate) first_vertex: u32,
    pub(crate) segment_count: u8,
}

/// A shape's linework as per-chain [`CoordSeq`] views plus its facet
/// partition — the storage every BVH traversal and brute scan reads.
/// Facets never span a chain boundary, so every facet's vertex run is one
/// contiguous slice inside its chain's columns.
pub(crate) struct PreparedLinework {
    chains: Vec<CoordSeq>,
    /// Global vertex index where each chain starts (`offsets[i]` is chain
    /// `i`'s first vertex in the packed probe API).
    chain_vertex_offsets: Vec<usize>,
    pub(crate) facets: Vec<Facet>,
    segment_count: usize,
    /// Total bounds over every linework vertex (`None` when empty) — the
    /// O(1) probe gate for the flat scans (the tree root carries the same
    /// bound for traversals).
    aabb: Option<[f64; 4]>,
}

/// Packed `u32` index count: `base + additional` must fit in `u32`, so the last
/// real index is `<= u32::MAX - 1` and `u32::MAX` stays the absent-witness
/// sentinel.
fn packed_index_count(base: usize, additional: usize) -> Result<usize> {
    let total = base
        .checked_add(additional)
        .ok_or(ResourceError::SizeOverflow)?;
    u32::try_from(total).map_err(|_| ResourceError::SizeOverflow)?;
    Ok(total)
}

fn push_chain_facets(
    facets: &mut Vec<Facet>,
    chain: u32,
    vertices: usize,
    global_segments: usize,
) -> Result<usize> {
    if vertices < 2 {
        return Ok(0);
    }
    let segments = vertices.wrapping_sub(1);
    packed_index_count(global_segments, segments)?;
    facets.reserve(segments.div_ceil(FACET_SEGMENTS));
    let mut offset = 0;
    while offset < segments {
        let count = segments.wrapping_sub(offset).min(FACET_SEGMENTS);
        debug_assert!(u32::try_from(offset).is_ok(), "CoordSeq window is u32");
        facets.push(Facet {
            chain,
            first_vertex: offset as u32,
            segment_count: count as u8,
        });
        offset = offset.wrapping_add(count);
    }
    Ok(segments)
}

// Stamp the shared fold at each probe so full-facet rounding stays identical
// without an out-of-line call per facet.
macro_rules! facet_point_distance_columns {
    ($squared:expr, $simd:expr, $count:expr, $xs:expr, $ys:expr, $point:expr, $best:expr $(,)?) => {{
        use $crate::error::Result;
        use $crate::geometry::facet_bvh::{
            FACET_SEGMENTS, aabb_distance_squared, point_aabb, segment_aabb,
            simd_point_facet_distance_squared,
        };
        use $crate::geometry::{
            Segment, XY, point_segment_distance, point_segment_distance_squared,
        };
        let count = $count;
        let point = $point;
        'minimum: {
            if $squared && $simd && count == FACET_SEGMENTS {
                let (xs, ys) = ($xs, $ys);
                let squared = simd_point_facet_distance_squared(xs, ys, point.x, point.y);
                if squared.is_finite() {
                    break 'minimum Result::<f64>::Ok(squared);
                }
            }
            let (xs, ys) = ($xs, $ys);
            let mut best = $best;
            for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
                let segment = Segment {
                    start: XY::new(*ax, *ay),
                    end: XY::new(*bx, *by),
                };
                // Equal bounds retain the projection's rounded distance tie.
                if $squared
                    && aabb_distance_squared(point_aabb(point.x, point.y), segment_aabb(segment))
                        > best
                {
                    continue;
                }
                best = best.min(if $squared {
                    point_segment_distance_squared(point, segment)?
                } else {
                    point_segment_distance(point, segment)?
                });
            }
            Result::<f64>::Ok(best)
        }
    }};
}
pub(crate) use facet_point_distance_columns;

impl PreparedLinework {
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    const fn empty() -> Self {
        Self {
            chains: Vec::new(),
            chain_vertex_offsets: Vec::new(),
            facets: Vec::new(),
            segment_count: 0,
            aabb: None,
        }
    }

    pub(crate) fn build(shape: &Shape) -> Result<Self> {
        if let Shape::MultiLineString(lines) = shape {
            // Route stationing indexes occurrences, including repeated shared
            // chains. Proximity and locate must retain the same source ordinals.
            return Self::from_chains(lines.iter().map(|line| line.as_coords().clone()).collect());
        }
        let mut chains = Vec::new();
        shape.try_for_each_first_segment_chain(RunIdentity::Full, |chain| {
            chains.push(chain.clone());
            Ok::<_, crate::error::Error>(())
        })?;
        Self::from_chains(chains)
    }

    pub(crate) fn build_all(shape: &Shape) -> Result<Self> {
        let mut chains = Vec::new();
        shape.for_each_component(&mut |component| match component {
            Component::Point(_) => {},
            Component::LineString(line) => chains.push(line.as_coords().clone()),
            Component::Polygon(polygon) => {
                chains.extend(polygon.rings().map(|ring| ring.coords().clone()));
            },
        });
        Self::from_chains(chains)
    }

    pub(crate) fn from_chains(chains: Vec<CoordSeq>) -> Result<Self> {
        let chains: Vec<_> = chains
            .into_iter()
            .filter(|chain| chain.len() >= 2)
            .collect();
        let mut chain_vertex_offsets = Vec::new();
        let mut facets = Vec::new();
        let mut global_vertices = 0_usize;
        let mut global_segments = 0_usize;
        for (chain_index, chain) in chains.iter().enumerate() {
            let vertices = chain.len();
            let chain_index =
                u32::try_from(chain_index).map_err(|_| ResourceError::SizeOverflow)?;
            let segments = push_chain_facets(&mut facets, chain_index, vertices, global_segments)?;
            chain_vertex_offsets.push(global_vertices);
            global_vertices = global_vertices
                .checked_add(vertices)
                .ok_or(ResourceError::SizeOverflow)?;
            global_segments = global_segments
                .checked_add(segments)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        if chains.is_empty() {
            return Ok(Self::empty());
        }
        let aabb = chains
            .iter()
            .filter_map(Bounds::from_coords)
            .reduce(|mut bounds, chain_bounds| {
                bounds.include_bounds(chain_bounds);
                bounds
            })
            .map(Bounds::into_array);
        Ok(Self {
            chains,
            chain_vertex_offsets,
            facets,
            segment_count: global_segments,
            aabb,
        })
    }

    fn facet_chain(&self, facet: Facet) -> (&CoordSeq, Facet) {
        (
            self.chains
                .get(facet.chain as usize)
                .expect("facet chain belongs to prepared linework"),
            facet,
        )
    }

    /// Global vertex index for a facet-local source vertex.
    #[cfg(test)]
    pub(crate) fn facet_vertex_index(&self, facet: Facet, local: usize) -> usize {
        self.chain_vertex_offsets
            .get(facet.chain as usize)
            .expect("facet chain belongs to prepared linework")
            .wrapping_add(facet.first_vertex as usize)
            .wrapping_add(local)
    }

    fn locate_vertex(&self, global: usize) -> (usize, usize) {
        let chain = self
            .chain_vertex_offsets
            .partition_point(|&offset| offset <= global)
            .saturating_sub(1);
        let offset = *self
            .chain_vertex_offsets
            .get(chain)
            .expect("global vertex belongs to a prepared chain");
        (chain, global.wrapping_sub(offset))
    }

    /// The full vertex (with any Z/M) at a global column index.
    pub(crate) fn vertex_at(&self, index: usize) -> Point {
        let (chain, local) = self.locate_vertex(index);
        self.chains
            .get(chain)
            .expect("global vertex belongs to a prepared chain")
            .point_at(local)
    }

    /// Full vertex at an admitted chain-local support position.
    pub(crate) fn vertex_at_chain_local(&self, chain: u32, local: u32) -> Point {
        self.chains
            .get(chain as usize)
            .expect("support chain belongs to prepared linework")
            .point_at(local as usize)
    }

    pub(crate) fn segment_ordinal(&self, facet: Facet, index: usize) -> usize {
        self.chain_vertex_offsets
            .get(facet.chain as usize)
            .expect("facet chain belongs to prepared linework")
            .wrapping_sub(facet.chain as usize)
            .wrapping_add(facet.first_vertex as usize)
            .wrapping_add(index)
    }

    /// Full source endpoints at the ordinal retained by nearest selection.
    pub(crate) fn vertex_pair_at_segment(&self, ordinal: usize) -> (Point, Point) {
        let mut low = 0;
        let mut high = self.chains.len();
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            if self
                .chain_vertex_offsets
                .get(middle)
                .expect("binary-search midpoint belongs to prepared chains")
                .wrapping_sub(middle)
                <= ordinal
            {
                low = middle.wrapping_add(1);
            } else {
                high = middle;
            }
        }
        let chain = low.saturating_sub(1);
        let offset = *self
            .chain_vertex_offsets
            .get(chain)
            .expect("segment ordinal belongs to a prepared chain");
        let local = ordinal.wrapping_sub(offset.wrapping_sub(chain));
        let coords = self
            .chains
            .get(chain)
            .expect("segment ordinal belongs to a prepared chain");
        (
            coords.point_at(local),
            coords.point_at(local.wrapping_add(1)),
        )
    }

    /// Every linework vertex as streamed XY pairs — the probe source for the
    /// vertex sweeps (no `Vec<Point>` materialization).
    pub(crate) fn vertex_coords(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.chains.iter().flat_map(|chain| {
            std::iter::zip(chain.xs().iter().copied(), chain.ys().iter().copied())
        })
    }

    /// Every linework vertex as full `Point`s — the argmin probe source.
    pub(crate) fn vertex_points(&self) -> impl Iterator<Item = Point> + '_ {
        self.chains.iter().flat_map(CoordSeq::points)
    }

    /// Single-chain fast path: direct column refs when the shape has exactly
    /// one linework chain (avoids iterator overhead in hot squared-safe scans).
    pub(crate) fn vertex_columns(&self) -> Option<(&[f64], &[f64])> {
        match self.chains.as_slice() {
            [chain] => Some((chain.xs(), chain.ys())),
            _ => None,
        }
    }

    /// Visit every chain's XY columns — single-chain operands hit the fast
    /// path once; multi-chain operands walk each chain in turn.
    pub(crate) fn for_each_chain_xy_columns(&self, mut visit: impl FnMut(&[f64], &[f64])) {
        if let Some((xs, ys)) = self.vertex_columns() {
            visit(xs, ys);
        } else {
            for chain in &self.chains {
                visit(chain.xs(), chain.ys());
            }
        }
    }

    /// Visit every segment (the brute crossing scan below the tree
    /// crossover, and the probe stream for one-sided tree queries).
    pub(crate) fn for_each_segment(&self, mut visit: impl FnMut(Segment)) {
        for &facet in &self.facets {
            for index in 0..facet.segment_count as usize {
                visit(self.segment(facet, index));
            }
        }
    }

    /// Short-circuiting [`for_each_segment`](Self::for_each_segment):
    /// `true` as soon as `test` accepts a segment — decisive hits stop
    /// the walk instead of draining the remaining facets.
    pub(crate) fn any_segment(&self, mut test: impl FnMut(Segment) -> bool) -> bool {
        self.facets.iter().any(|&facet| {
            (0..facet.segment_count as usize).any(|index| test(self.segment(facet, index)))
        })
    }

    pub(crate) const fn segment_count(&self) -> usize {
        self.segment_count
    }

    /// The `index`-th segment of `facet` as a `Segment` (XY; planar distance
    /// kernels read no other ordinates).
    pub(crate) fn segment(&self, facet: Facet, index: usize) -> Segment {
        let (chain, facet) = self.facet_chain(facet);
        let v = (facet.first_vertex as usize).wrapping_add(index);
        let xs = chain.xs();
        let ys = chain.ys();
        let end = v.wrapping_add(2);
        let Some([start_x, end_x]) = xs.get(v..end) else {
            unreachable!("facet segment x range belongs to its prepared chain");
        };
        let Some([start_y, end_y]) = ys.get(v..end) else {
            unreachable!("facet segment y range belongs to its prepared chain");
        };
        // Direct `XY` construction — the segment kernels are XY-only, so the
        // packed columns build the endpoints straight (no full `Point` with its
        // z/m/axes, then narrow): this `segment` is the per-pair contact sweep's
        // hot pool-fill, run for every linework vertex of every batch element.
        Segment {
            start: XY::new(*start_x, *start_y),
            end: XY::new(*end_x, *end_y),
        }
    }

    pub(crate) fn facet_segments(&self, facet: Facet) -> impl Iterator<Item = Segment> + '_ {
        let (chain, facet) = self.facet_chain(facet);
        let first = facet.first_vertex as usize;
        let last = first.wrapping_add(facet.segment_count as usize);
        let xs = chain
            .xs()
            .get(first..=last)
            .expect("prepared facet x window");
        let ys = chain
            .ys()
            .get(first..=last)
            .expect("prepared facet y window");
        xs.array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .map(|(&[ax, bx], &[ay, by])| Segment {
                start: XY::new(ax, ay),
                end: XY::new(bx, by),
            })
    }

    /// Flat-scan minimum distance from a probe coordinate stream (below the
    /// BVH crossover): the total-bounds gate skips far probes in O(1), then
    /// the SIMD facet kernel folds the rest. `SQUARED` selects squared space
    /// + the SIMD kernel; `false` runs overflow-safe `hypot` scalars.
    pub(crate) fn min_points_distance<const SQUARED: bool>(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
        mut best: f64,
    ) -> crate::error::Result<f64> {
        let Some(aabb) = self.aabb else {
            return Ok(best);
        };
        for (x, y) in probes {
            let gate = if SQUARED {
                aabb_distance_squared(aabb, point_aabb(x, y))
            } else {
                aabb_distance(aabb, point_aabb(x, y))
            };
            if gate >= best {
                continue;
            }
            for &facet in &self.facets {
                let candidate = if SQUARED {
                    self.facet_point_distance_squared(facet, x, y)?
                } else {
                    self.facet_point_distance(facet, Point::new_raw_xy(x, y))?
                };
                best = best.min(candidate);
            }
        }
        Ok(best)
    }

    /// Flat-scan boundary-inclusive on-segment test (below the tree)
    /// crossover) — exact `point_on_segment`, box-gated per facet.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        self.facets.iter().any(|&facet| {
            self.facet_segments(facet)
                .any(|segment| point_on_segment(point, segment.start, segment.end))
        })
    }

    /// Flat-scan `dwithin` from a probe coordinate stream (squared,
    /// inclusive); `simd` as in [`FacetBvh::any_points_within`].
    pub(crate) fn any_points_within(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
        limit: f64,
        simd: bool,
    ) -> crate::error::Result<bool> {
        let Some(aabb) = self.aabb else {
            return Ok(false);
        };
        for (x, y) in probes {
            if aabb_distance_squared(aabb, point_aabb(x, y)) > limit {
                continue;
            }
            for &facet in &self.facets {
                let candidate = if simd {
                    self.facet_point_distance_squared(facet, x, y)?
                } else {
                    let point = Point::new_raw_xy(x, y);
                    let mut best = f64::INFINITY;
                    for index in 0..facet.segment_count as usize {
                        best = best.min(point_segment_distance_squared(
                            point,
                            self.segment(facet, index),
                        )?);
                    }
                    best
                };
                if candidate <= limit {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Flat-scan argmin nearest (below the tree crossover) — see
    /// [`FacetBvh::nearest_to_points`].
    pub(crate) fn nearest_to_points<const SQUARED: bool>(
        &self,
        probes: impl Iterator<Item = Point>,
        keep: &impl Fn(Point, Segment) -> bool,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        let mut changed = false;
        let Some(aabb) = self.aabb else {
            return Ok(false);
        };
        for probe in probes {
            let limit = best
                .as_ref()
                .map(|b| {
                    if SQUARED {
                        Ok(b.distance_key.upper_bound())
                    } else {
                        b.distance_key.distance_upper_bound()
                    }
                })
                .transpose()?
                .unwrap_or(f64::INFINITY);
            let bound = if SQUARED {
                aabb_distance_squared_lower(aabb, point_aabb(probe.x, probe.y))
            } else {
                aabb_distance_lower(aabb, point_aabb(probe.x, probe.y))
            };
            if bound > limit {
                continue;
            }
            for &facet in &self.facets {
                changed |=
                    refine_facet_nearest::<false>(self, facet, probe, f64::INFINITY, keep, best)?;
            }
        }
        Ok(changed)
    }

    /// Flat-scan first touching/crossing pair against another linework
    /// (full ordinates) — witness extraction below the tree crossover.
    pub(crate) fn find_intersecting_pair_flat(&self, other: &Self) -> Option<(Segment, Segment)> {
        for &facet in &self.facets {
            for i in 0..facet.segment_count as usize {
                let probe = self.segment(facet, i);
                for &other_facet in &other.facets {
                    for j in 0..other_facet.segment_count as usize {
                        if segments_intersect(probe, other.segment(other_facet, j)) {
                            return Some((self.segment(facet, i), other.segment(other_facet, j)));
                        }
                    }
                }
            }
        }
        None
    }

    /// Rounded minimum squared distance from `(px, py)` to one facet's
    /// segments — eight clamped-projection distances in one SIMD register
    /// for full facets, a scalar fold for chain tails.
    pub(crate) fn facet_point_distance_squared(
        &self,
        facet: Facet,
        px: f64,
        py: f64,
    ) -> crate::error::Result<f64> {
        let (chain, facet) = self.facet_chain(facet);
        let first = facet.first_vertex as usize;
        let count = facet.segment_count as usize;
        let xs = chain.xs();
        let ys = chain.ys();
        facet_point_distance_columns!(
            true,
            true,
            count,
            xs.get(first..=first.saturating_add(count))
                .expect("prepared facet x window"),
            ys.get(first..=first.saturating_add(count))
                .expect("prepared facet y window"),
            Point::new_raw_xy(px, py),
            f64::INFINITY,
        )
    }

    /// Scalar `hypot`-space fold for one facet — the overflow-safe sibling of
    /// the SIMD kernel, for extreme-coordinate operands.
    pub(crate) fn facet_point_distance(
        &self,
        facet: Facet,
        point: Point,
    ) -> crate::error::Result<f64> {
        let (chain, facet) = self.facet_chain(facet);
        let first = facet.first_vertex as usize;
        let count = facet.segment_count as usize;
        facet_point_distance_columns!(
            false,
            false,
            count,
            chain
                .xs()
                .get(first..=first.saturating_add(count))
                .expect("prepared facet x window"),
            chain
                .ys()
                .get(first..=first.saturating_add(count))
                .expect("prepared facet y window"),
            point,
            f64::INFINITY,
        )
    }

    /// The AABB of one facet's vertex run, as `[minx, miny, maxx, maxy]`.
    pub(crate) fn facet_aabb(&self, facet: Facet) -> [f64; 4] {
        let (chain, facet) = self.facet_chain(facet);
        let first = facet.first_vertex as usize;
        let last = first.wrapping_add(facet.segment_count as usize);
        let xs = chain
            .xs()
            .get(first..=last)
            .expect("facet x window belongs to its prepared chain");
        let ys = chain
            .ys()
            .get(first..=last)
            .expect("facet y window belongs to its prepared chain");
        xy_bounds_columns(xs, ys)
    }
}

crate::heapless!(Facet);

impl HeapSize for PreparedLinework {
    fn heap_bytes(&self) -> usize {
        self.chains
            .heap_bytes()
            .saturating_add(self.chain_vertex_offsets.heap_bytes())
            .saturating_add(self.facets.heap_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::{
        CoordinateAxes, LineSeq, MOrdinate, MultiLineString, MultiMember, ZOrdinate,
    };

    fn line_xy(points: &[(f64, f64)]) -> LineSeq {
        let points: Vec<Point> = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect();
        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    fn linework_xy(points: &[(f64, f64)]) -> PreparedLinework {
        PreparedLinework::build(&Shape::LineString(line_xy(points))).unwrap()
    }

    #[test]
    fn one_segment_linework_is_one_facet() {
        let linework = linework_xy(&[(0.0, 0.0), (1.0, 0.0)]);
        assert_eq!(linework.segment_count(), 1);
        assert_eq!(linework.facets.len(), 1);
        assert_eq!(linework.facets[0].first_vertex, 0);
        assert_eq!(linework.facets[0].segment_count, 1);
        assert_eq!(linework.facets[0].chain, 0);
    }

    #[test]
    fn nine_segment_linework_splits_into_two_facets() {
        let points: Vec<_> = (0..10).map(|value| (f64::from(value), 0.0)).collect();
        let linework = linework_xy(&points);
        assert_eq!(linework.segment_count(), 9);
        assert_eq!(linework.facets.len(), 2);
        assert_eq!(linework.facets[0].segment_count, 8);
        assert_eq!(linework.facets[1].segment_count, 1);
        assert_eq!(linework.facets[1].first_vertex, 8);
    }

    #[test]
    fn point_shape_is_empty_linework() {
        let linework = PreparedLinework::build(&Shape::empty_point()).unwrap();
        assert_eq!(linework.segment_count(), 0);
        assert!(linework.facets.is_empty());
    }

    #[test]
    fn three_one_segment_chains_are_three_facets() {
        let shape = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(line_xy(&[(0.0, 0.0), (1.0, 0.0)])),
                MultiMember::Value(line_xy(&[(2.0, 0.0), (3.0, 0.0)])),
                MultiMember::Value(line_xy(&[(4.0, 0.0), (5.0, 0.0)])),
            ])
            .unwrap(),
        );
        let linework = PreparedLinework::build(&shape).unwrap();
        assert_eq!(linework.segment_count(), 3);
        assert_eq!(linework.facets.len(), 3);
        assert_eq!(linework.facets[0].chain, 0);
        assert_eq!(linework.facets[1].chain, 1);
        assert_eq!(linework.facets[2].chain, 2);
    }

    #[test]
    fn mixed_axes_retain_full_ordinate_chain_views() {
        let xy = Shape::LineString(line_xy(&[(0.0, 0.0), (1.0, 0.0)]));
        let xyz = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_axes(0.0, 1.0, ZOrdinate(Some(1.0)), MOrdinate(None)),
                    Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(2.0)), MOrdinate(None)),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let shape = Shape::geometry_collection(vec![xy, xyz]).unwrap();
        let linework = PreparedLinework::build(&shape).unwrap();
        assert_eq!(linework.segment_count(), 2);
        assert_eq!(linework.facets.len(), 2);
        assert_eq!(linework.vertex_at(0), Point::new_raw_xy(0.0, 0.0));
        assert_eq!(
            linework.vertex_at(2),
            Point::new_raw_axes(0.0, 1.0, ZOrdinate(Some(1.0)), MOrdinate(None)),
        );
        assert_eq!(
            linework.vertex_at(3),
            Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(2.0)), MOrdinate(None)),
        );
    }

    #[test]
    fn push_chain_facets_rejects_count_that_would_claim_the_absent_sentinel() {
        let mut facets = Vec::new();
        let error = push_chain_facets(&mut facets, 0, 2, u32::MAX as usize).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
        assert!(facets.is_empty());
    }

    #[test]
    fn push_chain_facets_last_real_index_is_below_the_absent_sentinel() {
        let mut facets = Vec::new();
        let segments = push_chain_facets(&mut facets, 0, 2, u32::MAX as usize - 1).unwrap();
        assert_eq!(segments, 1);
        assert_eq!(facets.len(), 1);
        assert_eq!(facets[0].first_vertex, 0);
        assert_eq!(facets[0].segment_count, 1);
    }
}
