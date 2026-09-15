use crate::HeapSize;
use crate::error::{ResourceError, Result};
use crate::geometry::facet_bvh::engine::bvh::refine_facet_nearest;
use crate::geometry::facet_bvh::{
    FACET_SEGMENTS, NearestCandidate, aabb_distance, aabb_distance_squared, point_aabb,
    simd_point_facet_distance_squared,
};
use crate::geometry::{
    Bounds, CoordSeq, Point, Segment, Shape, XY, point_on_segment, point_segment_distance,
    point_segment_distance_squared, segments_intersect, try_copy_slice, xy_bounds_columns,
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
    /// Global segment index of this facet's first edge. Witness queries must
    /// return this packed-linework index without rescanning preceding facets.
    first_segment: u32,
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
    /// Whether every chain agrees on axis presence (Z/M). Mixed axes make
    /// `nearest_points` fall back to the brute shape path.
    uniform_ordinates: bool,
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
    let segments = vertices - 1;
    packed_index_count(global_segments, segments)?;
    crate::boundary::try_reserve_vec(facets, segments.div_ceil(FACET_SEGMENTS))?;
    let mut offset = 0;
    while offset < segments {
        let count = (segments - offset).min(FACET_SEGMENTS);
        debug_assert!(u32::try_from(offset).is_ok(), "CoordSeq window is u32");
        crate::try_push_vec(facets, Facet {
            chain,
            first_vertex: offset as u32,
            segment_count: count as u8,
            first_segment: (global_segments + offset) as u32,
        })?;
        offset += count;
    }
    Ok(segments)
}

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
            uniform_ordinates: true,
            facets: Vec::new(),
            segment_count: 0,
            aabb: None,
        }
    }

    /// One open chain from a column window — the Hausdorff column-target fast
    /// path (no `Shape` staging, single-chain facet partition).
    pub(crate) fn from_open_xy_columns(xs: &[f64], ys: &[f64]) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        if xs.len() < 2 {
            return Ok(Self::empty());
        }
        let chain = CoordSeq::try_from_vecs(try_copy_slice(xs)?, try_copy_slice(ys)?, None, None)?;
        let aabb = Bounds::from_coords(&chain).map(Bounds::into_array);
        let mut facets = Vec::new();
        let segments = push_chain_facets(&mut facets, 0, chain.len(), 0)?;
        let mut chains = Vec::new();
        crate::try_push_vec(&mut chains, chain)?;
        let mut chain_vertex_offsets = Vec::new();
        crate::try_push_vec(&mut chain_vertex_offsets, 0)?;
        Ok(Self {
            chains,
            chain_vertex_offsets,
            uniform_ordinates: true,
            facets,
            segment_count: segments,
            aabb,
        })
    }

    pub(crate) fn build(shape: &Shape) -> Result<Self> {
        let mut chains = Vec::new();
        let mut chain_vertex_offsets = Vec::new();
        let mut facets = Vec::new();
        let mut reference_axes = None;
        let mut uniform_ordinates = true;
        let mut global_vertices = 0_usize;
        let mut global_segments = 0_usize;
        shape.try_for_each_segment_chain(|chain| {
            let vertices = chain.len();
            if vertices < 2 {
                return Ok::<(), crate::error::Error>(());
            }
            let chain_index =
                u32::try_from(chains.len()).map_err(|_| ResourceError::SizeOverflow)?;
            let segments = push_chain_facets(&mut facets, chain_index, vertices, global_segments)?;
            let chain_axes = chain.axes();
            match reference_axes {
                None => reference_axes = Some(chain_axes),
                Some(axes) if axes != chain_axes => uniform_ordinates = false,
                _ => {},
            }
            crate::try_push_vec(&mut chain_vertex_offsets, global_vertices)?;
            crate::try_push_vec(&mut chains, chain.clone())?;
            global_vertices = global_vertices
                .checked_add(vertices)
                .ok_or(ResourceError::SizeOverflow)?;
            global_segments = global_segments
                .checked_add(segments)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        })?;
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
            uniform_ordinates,
            facets,
            segment_count: global_segments,
            aabb,
        })
    }

    fn facet_chain(&self, facet: Facet) -> (&CoordSeq, Facet) {
        (&self.chains[facet.chain as usize], facet)
    }

    /// Total linework vertex count across every chain.
    pub(crate) fn vertex_count(&self) -> usize {
        self.chains.last().map_or(0, |chain| {
            let chain_idx = self.chains.len() - 1;
            self.chain_vertex_offsets[chain_idx] + chain.len()
        })
    }

    fn locate_vertex(&self, global: usize) -> (usize, usize) {
        let chain = self
            .chain_vertex_offsets
            .partition_point(|&offset| offset <= global)
            .saturating_sub(1);
        (chain, global - self.chain_vertex_offsets[chain])
    }

    /// Whether Z/M reconstruction through [`vertex_at`](Self::vertex_at) is
    /// faithful (no chain carried an ordinate the columns dropped).
    pub(crate) const fn uniform_ordinates(&self) -> bool {
        self.uniform_ordinates
    }

    /// The full vertex (with any Z/M) at a global column index.
    pub(crate) fn vertex_at(&self, index: usize) -> Point {
        let (chain, local) = self.locate_vertex(index);
        self.chains[chain].point_at(local)
    }

    /// The `index`-th edge of `facet` as its two FULL-ordinate vertices —
    /// the witness lanes lift their planar feet back through these so Z/M
    /// interpolate.
    pub(crate) fn vertex_pair_full(&self, facet: Facet, index: usize) -> (Point, Point) {
        let chain = &self.chains[facet.chain as usize];
        let v = facet.first_vertex as usize + index;
        (chain.point_at(v), chain.point_at(v + 1))
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
        (0..self.vertex_count()).map(|index| self.vertex_at(index))
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
        let v = facet.first_vertex as usize + index;
        let xs = chain.xs();
        let ys = chain.ys();
        // Direct `XY` construction — the segment kernels are XY-only, so the
        // packed columns build the endpoints straight (no full `Point` with its
        // z/m/axes, then narrow): this `segment` is the per-pair contact sweep's
        // hot pool-fill, run for every linework vertex of every batch element.
        Segment {
            start: XY::new(xs[v], ys[v]),
            end: XY::new(xs[v + 1], ys[v + 1]),
        }
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
            (0..facet.segment_count as usize).any(|index| {
                let segment = self.segment(facet, index);
                point_on_segment(point, segment.start, segment.end)
            })
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
    pub(crate) fn nearest_to_points(
        &self,
        probes: impl Iterator<Item = Point>,
        mut best: Option<NearestCandidate>,
    ) -> crate::error::Result<Option<NearestCandidate>> {
        let Some(aabb) = self.aabb else {
            return Ok(best);
        };
        for probe in probes {
            let limit = best
                .as_ref()
                .map_or(f64::INFINITY, |b| b.distance_key.upper_bound());
            if aabb_distance_squared(aabb, point_aabb(probe.x, probe.y)) > limit {
                continue;
            }
            for &facet in &self.facets {
                let limit = best
                    .as_ref()
                    .map_or(f64::INFINITY, |b| b.distance_key.upper_bound());
                if self.facet_point_distance_squared(facet, probe.x, probe.y)? <= limit {
                    refine_facet_nearest(self, facet, probe, &mut best)?;
                }
            }
        }
        Ok(best)
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

    /// Exact minimum squared distance from `(px, py)` to one facet's
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
        if count == FACET_SEGMENTS {
            let simd = simd_point_facet_distance_squared(
                &xs[first..=first + FACET_SEGMENTS],
                &ys[first..=first + FACET_SEGMENTS],
                px,
                py,
            );
            if simd.is_finite() {
                return Ok(simd);
            }
        }
        let point = Point::new_raw_xy(px, py);
        let mut best = f64::INFINITY;
        for index in 0..count {
            best = best.min(point_segment_distance_squared(
                point,
                self.segment(facet, index),
            )?);
        }
        Ok(best)
    }

    /// Scalar `hypot`-space fold for one facet — the overflow-safe sibling of
    /// the SIMD kernel, for extreme-coordinate operands.
    pub(crate) fn facet_point_distance(
        &self,
        facet: Facet,
        point: Point,
    ) -> crate::error::Result<f64> {
        let mut best = f64::INFINITY;
        for index in 0..facet.segment_count as usize {
            best = best.min(point_segment_distance(point, self.segment(facet, index))?);
        }
        Ok(best)
    }

    /// The AABB of one facet's vertex run, as `[minx, miny, maxx, maxy]`.
    pub(crate) fn facet_aabb(&self, facet: Facet) -> [f64; 4] {
        let (chain, facet) = self.facet_chain(facet);
        let first = facet.first_vertex as usize;
        let last = first + facet.segment_count as usize;
        let xs = &chain.xs()[first..=last];
        let ys = &chain.ys()[first..=last];
        xy_bounds_columns(xs, ys)
    }
}

crate::heapless!(Facet);

impl HeapSize for PreparedLinework {
    fn heap_bytes(&self) -> usize {
        self.chains.heap_bytes() + self.chain_vertex_offsets.heap_bytes() + self.facets.heap_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{
        CoordinateAxes, LineSeq, MOrdinate, MultiLineString, MultiMember, Point, Shape, ZOrdinate,
    };
    use crate::test_support::fail_allocations_after;

    fn line_xy(points: &[(f64, f64)]) -> LineSeq {
        let points: Vec<Point> = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect();
        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    #[test]
    fn columns_shorter_than_two_vertices_are_empty() {
        let linework = PreparedLinework::from_open_xy_columns(&[0.0], &[0.0]).unwrap();
        assert_eq!(linework.segment_count(), 0);
        assert!(linework.facets.is_empty());
        assert!(linework.chains.is_empty());
    }

    #[test]
    fn columns_two_vertices_are_one_facet() {
        let linework = PreparedLinework::from_open_xy_columns(&[0.0, 1.0], &[0.0, 0.0]).unwrap();
        assert_eq!(linework.segment_count(), 1);
        assert_eq!(linework.facets.len(), 1);
        assert_eq!(linework.facets[0].first_vertex, 0);
        assert_eq!(linework.facets[0].first_segment, 0);
        assert_eq!(linework.facets[0].segment_count, 1);
        assert_eq!(linework.facets[0].chain, 0);
    }

    #[test]
    fn columns_nine_segments_split_into_two_facets() {
        let xs: Vec<f64> = (0..10).map(f64::from).collect();
        let ys = vec![0.0; 10];
        let linework = PreparedLinework::from_open_xy_columns(&xs, &ys).unwrap();
        assert_eq!(linework.segment_count(), 9);
        assert_eq!(linework.facets.len(), 2);
        assert_eq!(linework.facets[0].segment_count, 8);
        assert_eq!(linework.facets[1].segment_count, 1);
        assert_eq!(linework.facets[1].first_vertex, 8);
        assert_eq!(linework.facets[1].first_segment, 8);
    }

    #[test]
    fn columns_return_resource_on_allocation_failure() {
        let xs = [0.0, 1.0];
        let ys = [0.0, 0.0];
        match fail_allocations_after(0, || PreparedLinework::from_open_xy_columns(&xs, &ys)) {
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            )),
            Ok(_) => panic!("expected allocation failure"),
        }
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
        assert_eq!(linework.facets[0].first_segment, 0);
        assert_eq!(linework.facets[1].first_segment, 1);
        assert_eq!(linework.facets[2].first_segment, 2);
        assert_eq!(linework.facets[0].chain, 0);
        assert_eq!(linework.facets[1].chain, 1);
        assert_eq!(linework.facets[2].chain, 2);
        assert!(linework.uniform_ordinates());
    }

    #[test]
    fn mixed_axes_are_not_uniform() {
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
        assert!(!linework.uniform_ordinates());
    }

    #[test]
    fn build_returns_resource_on_allocation_failure() {
        let shape = Shape::LineString(line_xy(&[(0.0, 0.0), (1.0, 0.0)]));
        match fail_allocations_after(0, || PreparedLinework::build(&shape)) {
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            )),
            Ok(_) => panic!("expected allocation failure"),
        }
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
        assert_eq!(facets[0].first_segment, u32::MAX - 1);
    }
}
