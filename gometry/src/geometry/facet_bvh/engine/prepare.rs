use std::sync::Arc;

use crate::HeapSize;
use crate::geometry::facet_bvh::engine::bvh::refine_facet_nearest;
use crate::geometry::facet_bvh::{
    FACET_SEGMENTS, NearestCandidate, aabb_distance, aabb_distance_squared, point_aabb,
    simd_point_facet_distance_squared,
};
use crate::geometry::{
    Bounds, CoordSeq, Point, Segment, Shape, XY, point_on_segment, point_segment_distance,
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

const _: () = assert!(size_of::<Facet>() <= 12, "Facet must stay <=12 bytes");

/// A shape's linework as per-chain [`CoordSeq`] views plus its facet
/// partition — the storage every BVH traversal and brute scan reads.
/// Facets never span a chain boundary, so every facet's vertex run is one
/// contiguous slice inside its chain's columns.
pub(crate) struct PreparedLinework {
    chains: Box<[CoordSeq]>,
    /// Global vertex index where each chain starts (`offsets[i]` is chain
    /// `i`'s first vertex in the packed probe API).
    chain_vertex_offsets: Box<[usize]>,
    /// Whether every chain agrees on axis presence (Z/M). Mixed axes make
    /// `nearest_points` fall back to the brute shape path.
    uniform_ordinates: bool,
    pub(crate) facets: Box<[Facet]>,
    segment_count: usize,
    /// Total bounds over every linework vertex (`None` when empty) — the
    /// O(1) probe gate for the flat scans (the tree root carries the same
    /// bound for traversals).
    aabb: Option<[f64; 4]>,
}

impl PreparedLinework {
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// One open chain from a column window — the Hausdorff column-target fast
    /// path (no `Shape` staging, single-chain facet partition).
    pub(crate) fn from_open_xy_columns(xs: &[f64], ys: &[f64]) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        if xs.len() < 2 {
            return Self {
                chains: Box::default(),
                chain_vertex_offsets: Box::default(),
                uniform_ordinates: true,
                facets: Box::default(),
                segment_count: 0,
                aabb: None,
            };
        }
        let chain = CoordSeq::from_columns(Arc::from(xs), Arc::from(ys), None, None);
        let segments = xs.len() - 1;
        let mut facets = Vec::with_capacity(segments.div_ceil(FACET_SEGMENTS));
        let mut offset = 0;
        while offset < segments {
            let count = (segments - offset).min(FACET_SEGMENTS);
            facets.push(Facet {
                chain: 0,
                first_vertex: offset as u32,
                segment_count: count as u8,
            });
            offset += count;
        }
        let aabb = Bounds::from_coords(&chain).map(Bounds::into_array);
        Self {
            chains: Box::new([chain]),
            chain_vertex_offsets: Box::new([0]),
            uniform_ordinates: true,
            facets: facets.into_boxed_slice(),
            segment_count: segments,
            aabb,
        }
    }

    pub(crate) fn build(shape: &Shape) -> Self {
        let segment_count = shape.segment_count();
        if segment_count == 0 {
            // Point-only / empty shapes carry no linework: no chain staging.
            return Self {
                chains: Box::default(),
                chain_vertex_offsets: Box::default(),
                uniform_ordinates: true,
                facets: Box::default(),
                segment_count: 0,
                aabb: None,
            };
        }
        let mut chains = Vec::new();
        let mut chain_vertex_offsets = Vec::new();
        let mut reference_axes = None;
        let mut uniform_ordinates = true;
        let mut facets = Vec::with_capacity(segment_count.div_ceil(FACET_SEGMENTS));
        let mut global_vertices = 0;
        shape.for_each_segment_chain(|chain| {
            let vertices = chain.len();
            if vertices < 2 {
                return;
            }
            let chain_axes = chain.axes();
            match reference_axes {
                None => reference_axes = Some(chain_axes),
                Some(axes) if axes != chain_axes => uniform_ordinates = false,
                _ => {},
            }
            let chain_index = chains.len();
            chain_vertex_offsets.push(global_vertices);
            chains.push(chain.clone());
            global_vertices += vertices;
            let segments = vertices - 1;
            let mut offset = 0;
            while offset < segments {
                let count = (segments - offset).min(FACET_SEGMENTS);
                facets.push(Facet {
                    chain: chain_index as u32,
                    first_vertex: offset as u32,
                    segment_count: count as u8,
                });
                offset += count;
            }
        });
        let aabb = chains
            .iter()
            .filter_map(Bounds::from_coords)
            .reduce(|mut bounds, chain_bounds| {
                bounds.include_bounds(chain_bounds);
                bounds
            })
            .map(Bounds::into_array);
        Self {
            chains: chains.into_boxed_slice(),
            chain_vertex_offsets: chain_vertex_offsets.into_boxed_slice(),
            uniform_ordinates,
            facets: facets.into_boxed_slice(),
            segment_count,
            aabb,
        }
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
        match self.chains.as_ref() {
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

    /// One probe's minimum squared distance to the linework and the witness
    /// segment index (global, `u32::MAX` when empty).
    pub(crate) fn min_point_distance_with_witness<const SQUARED: bool>(
        &self,
        x: f64,
        y: f64,
        mut best: f64,
    ) -> (f64, u32) {
        let Some(aabb) = self.aabb else {
            return (best, u32::MAX);
        };
        let gate = if SQUARED {
            aabb_distance_squared(aabb, point_aabb(x, y))
        } else {
            let gate = aabb_distance(aabb, point_aabb(x, y));
            if gate >= best {
                return (best, u32::MAX);
            }
            // Hypot-space path: fold distances, track witness by segment index.
            let point = Point::new_unchecked_xy(x, y);
            let mut witness = u32::MAX;
            for (facet_index, &facet) in self.facets.iter().enumerate() {
                let base = self.facet_segment_base(facet_index);
                for index in 0..facet.segment_count as usize {
                    let candidate = point_segment_distance(point, self.segment(facet, index));
                    if candidate < best {
                        best = candidate;
                        witness = base + index as u32;
                    }
                }
            }
            return (best * best, witness);
        };
        if gate >= best {
            return (best, u32::MAX);
        }
        let mut witness = u32::MAX;
        for (facet_index, &facet) in self.facets.iter().enumerate() {
            let candidate = self.facet_point_distance_squared(facet, x, y);
            if candidate < best {
                best = candidate;
                witness = self
                    .facet_point_distance_with_witness(facet, facet_index, x, y)
                    .1;
            }
        }
        (best, witness)
    }

    /// Batch flat-scan point queries with per-probe witness segment indices.
    pub(crate) fn batch_min_point_distance_with_witness<const SQUARED: bool>(
        &self,
        probes: &[(f64, f64)],
        out_dist: &mut [f64],
        out_witness: &mut [u32],
    ) {
        debug_assert_eq!(probes.len(), out_dist.len());
        debug_assert_eq!(probes.len(), out_witness.len());
        for (index, &(x, y)) in probes.iter().enumerate() {
            let (distance_squared, witness) =
                self.min_point_distance_with_witness::<SQUARED>(x, y, f64::INFINITY);
            out_dist[index] = distance_squared;
            out_witness[index] = witness;
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
    ) -> f64 {
        let Some(aabb) = self.aabb else {
            return best;
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
                    self.facet_point_distance_squared(facet, x, y)
                } else {
                    self.facet_point_distance(facet, Point::new_unchecked_xy(x, y))
                };
                best = best.min(candidate);
            }
        }
        best
    }

    /// Flat-scan boundary-inclusive on-segment test (below the tree
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
        mut probes: impl Iterator<Item = (f64, f64)>,
        limit: f64,
        simd: bool,
    ) -> bool {
        let Some(aabb) = self.aabb else {
            return false;
        };
        probes.any(|(x, y)| {
            aabb_distance_squared(aabb, point_aabb(x, y)) <= limit
                && self.facets.iter().any(|&facet| {
                    let candidate = if simd {
                        self.facet_point_distance_squared(facet, x, y)
                    } else {
                        let point = Point::new_unchecked_xy(x, y);
                        (0..facet.segment_count as usize).fold(f64::INFINITY, |best, index| {
                            best.min(point_segment_distance_squared(
                                point,
                                self.segment(facet, index),
                            ))
                        })
                    };
                    candidate <= limit
                })
        })
    }

    /// Flat-scan argmin nearest (below the tree crossover) — see
    /// [`FacetBvh::nearest_to_points`].
    pub(crate) fn nearest_to_points(
        &self,
        probes: impl Iterator<Item = Point>,
        mut best: Option<NearestCandidate>,
    ) -> Option<NearestCandidate> {
        let Some(aabb) = self.aabb else {
            return best;
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
                if self.facet_point_distance_squared(facet, probe.x, probe.y) <= limit {
                    refine_facet_nearest(self, facet, probe, &mut best);
                }
            }
        }
        best
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

    /// Global segment index of the first edge in `facet`.
    fn facet_segment_base(&self, facet_index: usize) -> u32 {
        self.facets[..facet_index]
            .iter()
            .map(|facet| u32::from(facet.segment_count))
            .sum()
    }

    /// Minimum squared distance from `(px, py)` to one facet's segments and
    /// the global segment index of the witness edge.
    pub(crate) fn facet_point_distance_with_witness(
        &self,
        facet: Facet,
        facet_index: usize,
        px: f64,
        py: f64,
    ) -> (f64, u32) {
        let base = self.facet_segment_base(facet_index);
        let point = Point::new_unchecked_xy(px, py);
        let mut best = f64::INFINITY;
        let mut witness = base;
        for index in 0..facet.segment_count as usize {
            let distance_squared =
                point_segment_distance_squared(point, self.segment(facet, index));
            if distance_squared < best {
                best = distance_squared;
                witness = base + index as u32;
            }
        }
        (best, witness)
    }

    /// Exact minimum squared distance from `(px, py)` to one facet's
    /// segments — eight clamped-projection distances in one SIMD register
    /// for full facets, a scalar fold for chain tails.
    pub(crate) fn facet_point_distance_squared(&self, facet: Facet, px: f64, py: f64) -> f64 {
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
                return simd;
            }
        }
        let point = Point::new_unchecked_xy(px, py);
        (0..count).fold(f64::INFINITY, |best, index| {
            best.min(point_segment_distance_squared(
                point,
                self.segment(facet, index),
            ))
        })
    }

    /// Scalar `hypot`-space fold for one facet — the overflow-safe sibling of
    /// the SIMD kernel, for extreme-coordinate operands.
    pub(crate) fn facet_point_distance(&self, facet: Facet, point: Point) -> f64 {
        (0..facet.segment_count as usize).fold(f64::INFINITY, |best, index| {
            best.min(point_segment_distance(point, self.segment(facet, index)))
        })
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
