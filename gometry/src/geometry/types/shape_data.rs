#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::types::{
    Bounds, Coordinates as _, GeodesicSegment, Point, Segment, Shape, XY,
};
use crate::geometry::{
    DistanceParts, GeodesicParts, GeodesicPartsKey, GeodesicSweepCaps, LineIndex, LineIndexSlot,
    PlanarMetric, PointBatchTester, SegmentMetric, bounds_to_shape, ring_winding, shell_is_convex,
};

#[derive(Default)]
struct ColdCaches {
    staged_rings: OnceLock<Option<crate::geometry::topology::StagedRings>>,
    relate_topology: OnceLock<crate::geometry::relate::RelateTopology>,
    distance_3d: OnceLock<crate::geometry::Distance3dParts>,
}

/// One-frame sidecar for state that depends on the coordinate frame rather
/// than immutable coordinates alone. Ownership guarantees one logical
/// CRS/ellipsoid per sidecar; the key remains explicit so a PROJ runtime
/// generation change atomically replaces stale products.
#[derive(Default)]
pub(crate) struct FrameDependentCaches {
    entry: Mutex<Option<(GeodesicPartsKey, FrameDependentEntry)>>,
}

impl fmt::Debug for FrameDependentCaches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameDependentCaches")
            .finish_non_exhaustive()
    }
}

impl HeapSize for FrameDependentCaches {
    fn heap_bytes(&self) -> usize {
        self.entry.lock().ok().map_or(0, |entry| entry.heap_bytes())
    }
}

#[derive(Clone, Default)]
struct FrameDependentEntry {
    line_index: Option<Arc<LineIndex>>,
    parts: Option<Arc<GeodesicParts>>,
}

impl FrameDependentCaches {
    fn get(&self, key: &GeodesicPartsKey) -> Option<FrameDependentEntry> {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|(candidate, _)| candidate == key)
            .map(|(_, value)| value.clone())
    }

    fn merge(
        &self,
        key: GeodesicPartsKey,
        update: impl FnOnce(&mut FrameDependentEntry),
    ) -> Option<FrameDependentEntry> {
        let mut slot = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((current, _)) = slot.as_ref()
            && current.same_frame_parameters(&key)
            && current.runtime_generation() > key.runtime_generation()
        {
            return None;
        }
        let mut value = slot
            .take()
            .filter(|(candidate, _)| candidate == &key)
            .map_or_else(FrameDependentEntry::default, |(_, value)| value);
        update(&mut value);
        *slot = Some((key, value.clone()));
        drop(slot);
        Some(value)
    }
}

impl HeapSize for FrameDependentEntry {
    fn heap_bytes(&self) -> usize {
        self.line_index.heap_bytes() + self.parts.heap_bytes()
    }
}

/// Frozen geometry payload plus its lazily-built prepared state.
///
/// The keystone handle every `PyGeometry` (and detached worker clone) shares
/// behind one `Arc`, so an index built by any operation amortizes across
/// every later operation on the same geometry.
///
/// Large prepared working sets (`DistanceParts`, `PointBatchTester`,
/// `LineIndexSlot`, `ValidationIssue`) live behind `OnceLock<Box<_>>` so a
/// cold handle does not reserve hundreds of bytes of inline `OnceLock`
/// storage per geometry. Hot scalar paths that never touch those products
/// keep a small `ShapeData` header; first use allocates the product once.
/// The caches are pure functions of `shape` (which is immutable), so
/// identity, equality, and hashing delegate to the shape alone. The geo-rs
/// prepared relation deliberately lives elsewhere (it is `!Send`; this
/// handle crosses into detached work).
pub struct ShapeData {
    shape: Shape,
    bounds: OnceLock<Option<Bounds>>,
    /// Structured validity verdict, computed once — the shape is frozen,
    /// so the answer can never change. Boxed: the issue payload is large
    /// relative to a cold handle and rare on the hot path.
    validity: OnceLock<Box<Option<ValidationIssue>>>,
    /// OGC simplicity verdict, computed once (see `validity`).
    simplicity: OnceLock<bool>,
    /// Antimeridian-crossing verdict, computed once — the geographic split
    /// gate consults it on every topology op, often repeatedly per handle.
    antimeridian_crossing: OnceLock<bool>,
    /// Distance working set — boxed so cold `OnceLock` is pointer-sized.
    distance: OnceLock<Box<DistanceParts>>,
    /// Prepared point-membership tester — boxed (large hierarchical index).
    point_tester: OnceLock<Box<Option<PointBatchTester>>>,
    /// `Some(shell_is_ccw)` when the shape is a hole-free CONVEX polygon
    /// — such a region is exactly the intersection of its edge
    /// halfplanes, so point membership is pure `orient2d` sign tests.
    convex_shell: OnceLock<Option<bool>>,
    /// Planar LRS index — boxed (large prefix tables).
    line_index: OnceLock<Box<LineIndexSlot>>,
    /// Rarely-used lazy caches (geodesic, areal relate) boxed behind one
    /// `OnceLock` so transient and persistent handles stay small.
    cold: OnceLock<Box<ColdCaches>>,
}

// Large lazy products are `OnceLock<Box<_>>` (~16 B cold) rather than inline
// `OnceLock`s of 128-280 B values; the pre-shrink header was ~968 B, which cost
// ~1.1 KB per scalar geometry.

impl ShapeData {
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.heap_bytes()
    }

    pub const fn new(shape: Shape) -> Self {
        Self {
            shape,
            bounds: OnceLock::new(),
            validity: OnceLock::new(),
            simplicity: OnceLock::new(),
            antimeridian_crossing: OnceLock::new(),
            distance: OnceLock::new(),
            point_tester: OnceLock::new(),
            convex_shell: OnceLock::new(),
            line_index: OnceLock::new(),
            cold: OnceLock::new(),
        }
    }

    fn cold(&self) -> &ColdCaches {
        self.cold.get_or_init(|| Box::new(ColdCaches::default()))
    }

    pub const fn shape(&self) -> &Shape {
        &self.shape
    }

    /// Whole-operand topology for DE-9IM's mod-2 boundary semantics.
    pub(in crate::geometry) fn relate_topology(&self) -> &crate::geometry::relate::RelateTopology {
        self.cold()
            .relate_topology
            .get_or_init(|| crate::geometry::relate::RelateTopology::build(&self.shape))
    }

    /// The areal operand's rings, oriented once for the relate sweep. `None`
    /// when the shape is not polygonal.
    pub(in crate::geometry) fn staged_rings(
        &self,
    ) -> Option<&crate::geometry::topology::StagedRings> {
        self.cold()
            .staged_rings
            .get_or_init(|| {
                crate::geometry::relate::polygon_parts(&self.shape)
                    .map(crate::geometry::topology::StagedRings::build)
            })
            .as_ref()
    }

    /// The cached validity verdict (`None` = valid). Shapely re-validates
    /// every call; a frozen handle answers repeats for free.
    pub fn validate_cached(&self) -> Option<&ValidationIssue> {
        self.validity
            .get_or_init(|| Box::new(self.shape.validate()))
            .as_ref()
            .as_ref()
    }

    /// The cached OGC simplicity verdict (see [`Self::validate_cached`]).
    pub fn is_simple_cached(&self) -> bool {
        *self.simplicity.get_or_init(|| self.shape.is_simple())
    }

    /// The cached antimeridian-crossing verdict. An inherent method that
    /// shadows the `Deref`→[`Shape::crosses_antimeridian`], so the geographic
    /// split gate walks the rings only once per handle no matter how many
    /// topology ops (or how many times within one op) consult it.
    pub fn crosses_antimeridian(&self) -> bool {
        *self
            .antimeridian_crossing
            .get_or_init(|| self.shape.crosses_antimeridian())
    }

    /// Take the shape out of a uniquely-owned handle (the caches are
    /// derived state and drop with it).
    pub fn into_shape(self) -> Shape {
        self.shape
    }

    /// Planar bounds, computed once.
    pub fn bounds(&self) -> Option<Bounds> {
        *self.bounds.get_or_init(|| self.shape.bounds())
    }

    /// Seed the bounds cache from an already-known box (the array path computes
    /// every row's box ONCE with the SIMD `per_element_bounds` fold, so the
    /// per-pair transient handle skips a cold scalar rescan). A no-op if bounds
    /// were already cached. Returns `self` for builder-style materialization.
    #[must_use]
    pub(crate) fn with_seeded_bounds(self, bounds: Option<Bounds>) -> Self {
        let _ = self.bounds.set(bounds);
        self
    }

    /// Envelope (bounding-box polygon) built from the cached bounds, so a
    /// prior `bounds`/`envelope` call makes this free. Inherent method that
    /// shadows the `Deref`→[`Shape::envelope`], which would otherwise rescan
    /// every coordinate on each call. Mirrors GEOS, which caches the
    /// geometry envelope rather than recomputing it.
    pub fn envelope(&self) -> Shape {
        // `envelope` produces boxes → `POLYGON EMPTY` for empty input (the
        // output-type convention), matching `Shape::envelope`. A non-empty
        // degenerate box still collapses via `bounds_to_shape`.
        self.bounds()
            .map_or_else(Shape::empty_polygon, bounds_to_shape)
    }

    /// The distance working set (packed linework + isolated points + lazy
    /// facet BVH), built once and shared by every distance/dwithin/intersects
    /// call on this geometry.
    pub fn distance_parts(&self) -> &DistanceParts {
        self.distance
            .get_or_init(|| Box::new(self.shape.distance_parts()))
    }

    /// The 3D segment working set (packed segments + lazy AABB BVH), built
    /// once and shared by every `distance_3d` call on this geometry.
    pub(crate) fn distance_3d_parts(&self) -> &crate::geometry::Distance3dParts {
        self.cold()
            .distance_3d
            .get_or_init(|| crate::geometry::Distance3dParts::build(&self.shape))
    }

    /// The planar linear-referencing index (prefix lengths over the
    /// linework), built once; non-lineal/empty shapes re-raise their build
    /// error.
    pub fn line_index(&self) -> Result<&LineIndex> {
        self.line_index
            .get_or_init(|| Box::new(LineIndexSlot::build(&self.shape, &PlanarMetric)))
            .get()
    }

    /// The geodesic linear-referencing index for `crs` — per-segment Karney
    /// lengths paid once per (shape, CRS), shared by every later
    /// interpolate/substring/locate call.
    pub(crate) fn geodesic_line_index(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl SegmentMetric,
    ) -> Result<std::sync::Arc<LineIndex>> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let cached = frame_cache.get(&key).and_then(|entry| entry.line_index);
        if let Some(index) = cached {
            return Ok(index);
        }
        // Build outside the lock (Karney work); a racing duplicate build is
        // cheaper than holding the lock across it.
        let index = Arc::new(LineIndex::build(&self.shape, metric)?);
        let entry = frame_cache.merge(key, |entry| {
            entry.line_index.get_or_insert_with(|| Arc::clone(&index));
        });
        Ok(entry.and_then(|entry| entry.line_index).unwrap_or(index))
    }

    /// The geodesic distance/nearest working set for one normalized CRS and
    /// ellipsoid. Builds outside the cache lock; duplicate racing builds are
    /// acceptable, and failed domain validation is never stored.
    pub(in crate::geometry) fn cached_geodesic_parts(
        &self,
        frame_cache: &FrameDependentCaches,
        key: &GeodesicPartsKey,
    ) -> Option<Arc<GeodesicParts>> {
        frame_cache.get(key).and_then(|entry| entry.parts)
    }

    pub(in crate::geometry) fn geodesic_parts(
        &self,
        frame_cache: &FrameDependentCaches,
        key: GeodesicPartsKey,
        metric: &impl GeodesicMetric,
    ) -> Result<Arc<GeodesicParts>> {
        let cached = frame_cache.get(&key).and_then(|entry| entry.parts);
        if let Some(parts) = cached {
            return Ok(parts);
        }
        let parts = Arc::new(self.shape.geodesic_parts(metric)?);
        let entry = frame_cache.merge(key, |entry| {
            entry.parts.get_or_insert_with(|| Arc::clone(&parts));
        });
        Ok(entry.and_then(|entry| entry.parts).unwrap_or(parts))
    }

    pub(crate) fn prepare_geodesic_parts(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Result<()> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        self.geodesic_parts(frame_cache, key, metric).map(|_| ())
    }

    /// `Some(shell_is_ccw)` for a hole-free CONVEX polygon, computed
    /// once — such a region is exactly the intersection of its edge
    /// halfplanes (see `convex_halfplanes_cover`).
    pub(crate) fn convex_shell(&self) -> Option<bool> {
        *self.convex_shell.get_or_init(|| match &self.shape {
            Shape::Polygon(polygon)
                if polygon.holes.is_empty()
                    && polygon.shell.coords().coord_count() >= 4
                    && shell_is_convex(polygon.shell.coords()) =>
            {
                Some(ring_winding(polygon.shell.coords()).is_ccw())
            },
            _ => None,
        })
    }

    /// The DE-9IM matrix string over the PREPARED operands: the bbox-disjoint
    /// shortcut (identical to [`Shape::relate`]), then the cached native path,
    /// so repeated calls on the same handles reuse each side's prepared point
    /// tester instead of rebuilding it. Caller has already gated CRS/epoch.
    ///
    /// `pub` for benches/integration over the prepared path; the Python binding
    /// calls it directly. Use [`Shape::relate`] for a one-shot uncached relate.
    pub fn relate(&self, other: &Self) -> String {
        self.relate_matrix(other).text()
    }

    /// The DE-9IM matrix over the PREPARED operands: the bbox-disjoint shortcut
    /// then the cached native path (reusing each side's staged rings + point
    /// tester). Shared by [`Self::relate`] and [`Self::relate_pattern`].
    fn relate_matrix(&self, other: &Self) -> crate::geometry::relate::De9im {
        if let Some(text) =
            crate::geometry::predicates::geographic_point_relate_matrix(&self.shape, &other.shape)
        {
            let mut bytes = [b'F'; 9];
            for (slot, byte) in bytes.iter_mut().zip(text.bytes()) {
                *slot = byte;
            }
            return crate::geometry::relate::De9im(bytes);
        }
        if let (Some(left), Some(right)) = (self.bounds(), other.bounds())
            && !left.intersects(right)
            && let Some(matrix) =
                crate::geometry::predicates::disjoint_de9im(&self.shape, &other.shape)
        {
            return matrix;
        }
        crate::geometry::relate::native_relate_data(self, other)
    }

    /// DE-9IM pattern test over the PREPARED operands — evaluated on the cached
    /// matrix (see [`Self::relate_matrix`]), so it reuses the staged rings and
    /// point testers instead of the one-shot uncached
    /// [`Shape::relate_pattern`]. Each canonical predicate pattern IS its
    /// exact OGC definition, so matching the full matrix is
    /// verdict-identical to the specialized kernels.
    /// Prepared DE-9IM pattern test with a pre-compiled pattern.
    pub(crate) fn relate_pattern_compiled(
        &self,
        other: &Self,
        compiled: crate::geometry::CompiledPattern<'_>,
    ) -> bool {
        compiled.matches(self.relate_matrix(other))
    }

    /// The prepared point-membership tester (hierarchical
    /// [`PointBatchTester`] / Y-stabbing for polygonal shapes; `None`
    /// otherwise), built once.
    pub(crate) fn point_tester(&self) -> Option<&PointBatchTester> {
        self.point_tester
            .get_or_init(|| {
                Box::new(
                    matches!(self.shape, Shape::Polygon(_) | Shape::MultiPolygon(_))
                        .then(|| PointBatchTester::new(&self.shape)),
                )
            })
            .as_ref()
            .as_ref()
    }
}

impl HeapSize for ColdCaches {
    fn heap_bytes(&self) -> usize {
        self.staged_rings
            .get()
            .and_then(Option::as_ref)
            .map_or(0, crate::geometry::topology::StagedRings::heap_bytes)
            + self
                .relate_topology
                .get()
                .map_or(0, crate::geometry::relate::RelateTopology::heap_bytes)
            + self
                .distance_3d
                .get()
                .map_or(0, crate::geometry::Distance3dParts::heap_bytes)
    }
}

impl HeapSize for ShapeData {
    fn heap_bytes(&self) -> usize {
        // Shape payload (coordinates + container Vec/Arc structure) plus any
        // *initialized* prepared products. Uninitialized `OnceLock`s
        // contribute 0 and are never forced here. Boxed product allocations
        // are counted via `HeapSize for Box<T>`.
        self.shape.heap_bytes()
            + self.distance.heap_bytes()
            + self.point_tester.heap_bytes()
            + self.line_index.heap_bytes()
            + self.validity.heap_bytes()
            + self.cold.heap_bytes()
    }
}

impl HeapSize for DistanceParts {
    fn heap_bytes(&self) -> usize {
        self.point_only.heap_bytes()
            + self.linework.heap_bytes()
            + self.facet_bvh.heap_bytes()
            + self.point_index.heap_bytes()
    }
}

impl HeapSize for GeodesicParts {
    fn heap_bytes(&self) -> usize {
        self.points.heap_bytes()
            + self.segments.heap_bytes()
            + self.point_only.heap_bytes()
            + self.antimeridian_segments.heap_bytes()
            + self.caps.heap_bytes()
            + self.facet_bvh.heap_bytes()
    }
}

impl HeapSize for GeodesicSweepCaps {
    fn heap_bytes(&self) -> usize {
        self.lengths.heap_bytes() + self.groups.heap_bytes()
    }
}

crate::heapless!(PointKey);

impl std::ops::Deref for ShapeData {
    type Target = Shape;

    fn deref(&self) -> &Shape {
        &self.shape
    }
}

impl From<Shape> for ShapeData {
    fn from(shape: Shape) -> Self {
        Self::new(shape)
    }
}

impl From<Shape> for std::sync::Arc<ShapeData> {
    fn from(shape: Shape) -> Self {
        Self::new(ShapeData::new(shape))
    }
}

impl PartialEq for ShapeData {
    fn eq(&self, other: &Self) -> bool {
        self.shape == other.shape
    }
}

#[cfg(test)]
mod footprint_tests {
    use super::*;

    // The footprint bounds this test used to assert are now `const _`
    // assertions beside the struct definition, so they fail the build rather
    // than only `cargo test`.

    #[test]
    fn retained_heap_bytes_does_not_force_caches() {
        let data = ShapeData::new(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)));
        let cold = data.retained_heap_bytes();
        assert!(data.distance.get().is_none());
        assert!(data.point_tester.get().is_none());
        assert_eq!(data.retained_heap_bytes(), cold);
        assert!(data.distance.get().is_none());
        let _ = data.bounds();
        // bounds is inline OnceLock — still no boxed products forced
        assert!(data.distance.get().is_none());
        assert_eq!(data.retained_heap_bytes(), cold);
        let _ = data.distance_parts();
        assert!(data.distance.get().is_some());
        assert!(data.retained_heap_bytes() > cold);
    }
}

#[cfg(test)]
mod frame_cache_tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq};

    #[test]
    fn frame_cache_key_includes_crs_ellipsoid_and_runtime_generation() {
        let base =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 4);
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4267", 6_378_137.0, 1.0 / 298.257_223_563, 4)
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_136.0, 1.0 / 298.257_223_563, 4)
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 5)
        );
    }

    #[test]
    fn differing_frame_or_generation_replaces_the_single_entry() {
        let cache = FrameDependentCaches::default();
        let first =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 4);
        let next_generation =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 5);
        let other_frame =
            GeodesicPartsKey::new_at_generation("EPSG:4267", 6_378_206.4, 1.0 / 294.978_698_2, 5);
        let _ = cache.merge(first.clone(), |_| {});
        assert!(cache.get(&first).is_some());
        let _ = cache.merge(next_generation.clone(), |_| {});
        assert!(cache.get(&first).is_none());
        assert!(cache.get(&next_generation).is_some());
        let _ = cache.merge(other_frame.clone(), |_| {});
        assert!(cache.get(&next_generation).is_none());
        assert!(cache.get(&other_frame).is_some());
    }

    #[test]
    fn stale_generation_cannot_replace_a_newer_entry() {
        let cache = FrameDependentCaches::default();
        let older = GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 4);
        let newer = GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 5);
        assert!(cache.merge(newer.clone(), |_| {}).is_some());
        assert!(cache.merge(older.clone(), |_| {}).is_none());
        assert!(cache.get(&newer).is_some());
        assert!(cache.get(&older).is_none());
    }

    #[test]
    fn repeated_lrs_reuses_the_same_line_index_arc() {
        let shape = ShapeData::new(Shape::LineString(LineSeq::from_trusted(
            CoordSeq::from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None),
        )));
        let cache = FrameDependentCaches::default();
        let first = shape
            .geodesic_line_index(&cache, "TEST", 1.0, 0.0, &PlanarMetric)
            .expect("line index");
        let second = shape
            .geodesic_line_index(&cache, "TEST", 1.0, 0.0, &PlanarMetric)
            .expect("cached line index");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn single_entry_container_adds_no_heap_allocation() {
        let cache = FrameDependentCaches::default();
        let key = GeodesicPartsKey::new_at_generation("TEST", 1.0, 0.0, 0);
        let key_heap = key.heap_bytes();
        let _ = cache.merge(key, |_| {});
        // The key is inline and the entry has no products: unlike the former
        // Vec-backed LRU, the container itself retains no heap allocation.
        assert_eq!(cache.heap_bytes(), key_heap);
    }

    #[test]
    fn frame_cache_remains_one_entry_under_concurrent_replacement() {
        let cache = Arc::new(FrameDependentCaches::default());
        std::thread::scope(|scope| {
            for worker in 0..8_u64 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    for generation in 0..64_u64 {
                        let key = GeodesicPartsKey::new_at_generation(
                            "EPSG:4326",
                            6_378_137.0,
                            1.0 / 298.257_223_563,
                            generation + worker,
                        );
                        let _ = cache.merge(key, |_| {});
                    }
                });
            }
        });
        let latest = GeodesicPartsKey::new_at_generation(
            "EPSG:4326",
            6_378_137.0,
            1.0 / 298.257_223_563,
            70,
        );
        assert!(cache.get(&latest).is_some());
    }
}

impl Eq for ShapeData {}

impl std::hash::Hash for ShapeData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.shape.hash(state);
    }
}

impl fmt::Debug for ShapeData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shape.fmt(f)
    }
}

/// The nearest point on a geodesic segment to a probe.
///
/// Carries the minimum distance, the witness `foot` (materialized on the
/// ellipsoid, correct across the antimeridian and poles), and the along-track
/// `along` (meters from the segment start). The shared result behind geodesic
/// nearest-points, clearance, and linear referencing.
#[derive(Clone, Copy, Debug)]
pub struct GeodesicSegmentWitness {
    /// Minimum geodesic distance (meters) from the probe to the segment.
    pub distance: f64,
    /// The nearest point on the segment — the perpendicular foot, or the nearer
    /// endpoint when the foot falls outside the segment. Carries Z/M
    /// interpolated along the segment (the endpoint's, at an endpoint).
    pub foot: Point,
    /// Along-track distance (meters) from the segment start to `foot`.
    pub along: f64,
}

/// Ellipsoidal distance primitive injected into [`Shape::geodesic_distance`] so
/// the geometry layer stays free of any CRS/ellipsoid dependency. Implemented
/// in `crs.rs` over a `geographiclib` `Geodesic`.
pub trait GeodesicMetric {
    /// Precompute the ellipsoidal metadata shared by every exact probe of one
    /// segment.
    fn make_segment(&self, start: Point, end: Point) -> GeodesicSegment;

    /// Geodesic distance (meters) from `point` to the geodesic segment `a`–`b`
    /// (or to `a` when degenerate). `best` is the smallest distance found so
    /// far: implementations may skip expensive along-track refinement and
    /// return any value `>= best` once a cheap lower bound proves the segment
    /// cannot improve on it.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn point_to_segment(&self, point: Point, segment: GeodesicSegment, best: f64) -> f64;

    /// Whether the geodesic segments `a`–`b` and `c`–`d` properly cross on the
    /// ellipsoid (used to detect a zero distance the planar test misses near
    /// the antimeridian). Uses the orientation test on geodesic azimuth
    /// sides.
    fn segments_cross(&self, a: Point, b: Point, c: Point, d: Point) -> bool;

    /// Geodesic length (meters) of segment `a`–`b`.
    fn segment_length(&self, a: Point, b: Point) -> f64;

    /// The point a `fraction` of the geodesic length along `a`→`b`, with Z and
    /// M interpolated linearly by `fraction`.
    fn interpolate(&self, a: Point, b: Point, fraction: f64) -> Point;

    /// `(min geodesic distance, along-track meters from a)` for the foot of
    /// `point` on segment `a`–`b` — the geodesic linear-referencing primitive.
    /// `(distance, along-track offset)` of `point`'s nearest position on the
    /// geodesic segment `a`–`b`. `best` is the smallest distance found so
    /// far: implementations may skip the expensive along-track refinement
    /// and return any distance `>= best` once a cheap lower bound proves the
    /// segment cannot improve on it.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn locate_on_segment(&self, point: Point, segment: GeodesicSegment, best: f64) -> (f64, f64);

    /// The full nearest-point witness of `point` on segment `a`–`b`: distance,
    /// the foot point on the ellipsoid (carrying interpolated Z/M), and the
    /// along-track offset. `best` enables the same lower-bound pruning as
    /// [`point_to_segment`](Self::point_to_segment). Drives geodesic
    /// `nearest_points`/`shortest_line` and `minimum_clearance`.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn point_segment_witness(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> GeodesicSegmentWitness;

    /// A cheap (no-inverse) lower bound on the geodesic distance between two
    /// points — the auxiliary-sphere reduced-latitude bound. Drives the
    /// segment pruning in batch locate sweeps; `0.0` is always sound.
    fn point_distance_lower_bound(&self, a: Point, b: Point) -> f64;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexedSegment {
    pub segment: Segment,
    pub line: usize,
    pub index: usize,
    pub count: usize,
    pub closed: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PolylabelCell {
    pub center: Point,
    pub half_size: f64,
    pub distance: f64,
    pub max_distance: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PointKey {
    pub x: u64,
    pub y: u64,
}

impl PointKey {
    pub(crate) fn new(point: impl Into<XY>) -> Self {
        let point = point.into();
        Self {
            x: canonical_f64_bits(point.x),
            y: canonical_f64_bits(point.y),
        }
    }

    /// The keyed coordinate back as an `XY` (bit-exact; keys are built
    /// from canonicalized coordinate bits).
    pub(crate) const fn xy(self) -> XY {
        XY::new(f64::from_bits(self.x), f64::from_bits(self.y))
    }
}

pub(crate) fn ordered_edge(left: PointKey, right: PointKey) -> (PointKey, PointKey) {
    if (left.x, left.y) <= (right.x, right.y) {
        (left, right)
    } else {
        (right, left)
    }
}

pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValidationIssue {
    pub reason: String,
    pub location: Option<Point>,
    pub path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PolygonizeFull {
    pub polygons: Vec<Shape>,
    pub cuts: Vec<Shape>,
    pub dangles: Vec<Shape>,
    pub invalid_rings: Vec<Shape>,
}
