#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::collections::BinaryHeap;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::types::{Bounds, GeodesicSegment, Point, Segment, Shape, XY};
use crate::geometry::{
    DistanceParts, GeodesicParts, GeodesicPartsKey, GeodesicSweepCaps, LineIndex, LineIndexSlot,
    MeasuredLineIndex, MeasuredLineIndexSlot, PlanarMetric, PointBatchTester, SegmentMetric,
    bounds_to_shape,
};

/// One-frame sidecar for state that depends on the coordinate frame rather
/// than immutable coordinates alone. Ownership guarantees one logical
/// CRS/ellipsoid per sidecar; the key remains explicit so a PROJ runtime
/// generation change atomically replaces stale products.
#[derive(Default)]
pub(crate) struct FrameDependentCaches {
    entry: Mutex<Option<(GeodesicPartsKey, FrameDependentEntry)>>,
    geographic_topology: Mutex<GeographicTopologySlot>,
    #[cfg(test)]
    geographic_topology_accesses: AtomicUsize,
    #[cfg(test)]
    geographic_topology_builds: AtomicUsize,
}

impl fmt::Debug for FrameDependentCaches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameDependentCaches")
            .finish_non_exhaustive()
    }
}

impl HeapSize for FrameDependentCaches {
    fn heap_bytes(&self) -> usize {
        self.entry.heap_bytes()
            + self
                .geographic_topology
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .graph
                .as_ref()
                .map_or(0, |graph| {
                    std::mem::size_of_val(graph.as_ref()) + graph.heap_bytes()
                })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GeographicTopologyKey {
    longitude_unit: u64,
    longitude_period: u64,
    latitude_unit: u64,
    latitude_pole: u64,
    prime_meridian: u64,
}

impl GeographicTopologyKey {
    fn new(frame: crate::geographic::GeographicFrame) -> Self {
        Self {
            longitude_unit: canonical_bits(frame.longitude().unit().radians_per_unit()),
            longitude_period: canonical_bits(frame.longitude().period()),
            latitude_unit: canonical_bits(frame.latitude_unit().radians_per_unit()),
            latitude_pole: canonical_bits(frame.pole()),
            prime_meridian: canonical_bits(frame.prime_meridian_radians()),
        }
    }
}

fn canonical_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

#[derive(Default)]
struct GeographicTopologySlot {
    key: Option<GeographicTopologyKey>,
    generation: u64,
    graph: Option<Arc<crate::geometry::topology::TopologyGraph>>,
}

#[derive(Clone, Default)]
struct FrameDependentEntry {
    line_index: Option<Arc<LineIndex>>,
    parts: Option<Arc<GeodesicParts>>,
}

impl FrameDependentCaches {
    pub(crate) fn geographic_topology_graph(
        &self,
        view: crate::geographic::ScalarGeographicView<'_>,
    ) -> Result<Arc<crate::geometry::topology::TopologyGraph>> {
        #[cfg(test)]
        self.geographic_topology_accesses
            .fetch_add(1, Ordering::Relaxed);
        let key = GeographicTopologyKey::new(view.frame());
        let generation = {
            let mut slot = self
                .geographic_topology
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.key == Some(key) {
                if let Some(graph) = &slot.graph {
                    return Ok(Arc::clone(graph));
                }
                slot.generation
            } else {
                slot.generation = slot.generation.wrapping_add(1);
                slot.key = Some(key);
                slot.graph = None;
                slot.generation
            }
        };

        #[cfg(test)]
        self.geographic_topology_builds
            .fetch_add(1, Ordering::Relaxed);
        let built = Arc::try_new(crate::geometry::topology::TopologyGraph::geographic(view)?)
            .map_err(crate::error::Error::from)?;
        let mut slot = self
            .geographic_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.key == Some(key) && slot.generation == generation {
            if let Some(graph) = &slot.graph {
                return Ok(Arc::clone(graph));
            }
            slot.graph = Some(Arc::clone(&built));
        }
        drop(slot);
        Ok(built)
    }

    #[cfg(test)]
    pub(crate) fn geographic_topology_accesses(&self) -> usize {
        self.geographic_topology_accesses.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn geographic_topology_builds(&self) -> usize {
        self.geographic_topology_builds.load(Ordering::Relaxed)
    }

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

/// Frozen geometry payload and its cheap bounds cache.
///
/// Heavy working sets belong to [`PreparedPlan`], not to ordinary geometry
/// values. This keeps a geometry that is never used as a repeated operand
/// free of topology, distance, tester, and linear-index slots.
pub struct ShapeData {
    shape: Shape,
    bounds: OnceLock<Option<Bounds>>,
}

/// Shape-derived topology products shared by persistent and transient plans.
///
/// The geographic graph owns seam charts and pole quotient topology. The point
/// tester is separate and lazy so preparing a linear shape never constructs an
/// inapplicable areal tester.
#[derive(Default)]
struct TopologyPlan {
    validity: OnceLock<Option<Box<ValidationIssue>>>,
    simplicity: OnceLock<bool>,
    antimeridian_crossing: OnceLock<bool>,
    point_tester: OnceLock<Option<Box<PointBatchTester>>>,
}

/// Shape-derived metric products shared by persistent and transient plans.
#[derive(Default)]
struct MetricPlan {
    distance: OnceLock<Box<DistanceParts>>,
    distance_3d: OnceLock<Box<crate::geometry::Distance3dParts>>,
    line_index: OnceLock<Box<LineIndexSlot>>,
    measured_line_index: OnceLock<Box<MeasuredLineIndexSlot>>,
}

/// One reusable execution plan for one immutable geometry.
///
/// Persistent prepared geometries, array rows, and spatial-index rows retain
/// this same type. Ordinary one-shot calls construct a transient plan and drop
/// it after the operation. Frame-keyed geodesic products remain in the
/// caller-owned [`FrameDependentCaches`] sidecar because their interpretation
/// depends on CRS, ellipsoid, and runtime generation.
pub struct PreparedPlan {
    shape: Arc<ShapeData>,
    topology: OnceLock<Box<TopologyPlan>>,
    metric: OnceLock<Box<MetricPlan>>,
}

impl ShapeData {
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.heap_bytes()
    }

    pub const fn new(shape: Shape) -> Self {
        Self {
            shape,
            bounds: OnceLock::new(),
        }
    }

    pub const fn shape(&self) -> &Shape {
        &self.shape
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

    fn seed_bounds(&self, bounds: Option<Bounds>) {
        let _ = self.bounds.set(bounds);
    }

    /// Envelope (bounding-box polygon) built from the cached bounds, so a
    /// prior `bounds`/`envelope` call makes this free. Inherent method that
    /// shadows the `Deref`→[`Shape::envelope`], which would otherwise rescan
    /// every coordinate on each call.
    pub fn envelope(&self) -> crate::error::Result<Shape> {
        // `envelope` produces boxes → `POLYGON EMPTY` for empty input (the
        // output-type convention), matching `Shape::envelope`. A non-empty
        // degenerate box still collapses via `bounds_to_shape`.
        self.bounds()
            .map_or_else(|| Ok(Shape::empty_polygon()), bounds_to_shape)
    }
}

impl PreparedPlan {
    pub(crate) fn new(shape: impl IntoSharedShape) -> crate::error::Result<Self> {
        Ok(Self {
            shape: shape.try_into_shared()?,
            topology: OnceLock::new(),
            metric: OnceLock::new(),
        })
    }

    pub(crate) fn transient(shape: &Shape) -> crate::error::Result<Self> {
        Self::new(ShapeData::new(shape.clone()))
    }

    pub(crate) fn shape_data(&self) -> &ShapeData {
        &self.shape
    }

    pub(crate) const fn shape_arc(&self) -> &Arc<ShapeData> {
        &self.shape
    }

    fn topology(&self) -> Result<&TopologyPlan> {
        self.topology
            .get_or_try_init(|| {
                Box::try_new(TopologyPlan::default()).map_err(crate::error::Error::from)
            })
            .map(Box::as_ref)
    }

    fn metric(&self) -> Result<&MetricPlan> {
        self.metric
            .get_or_try_init(|| {
                Box::try_new(MetricPlan::default()).map_err(crate::error::Error::from)
            })
            .map(Box::as_ref)
    }

    pub(crate) fn validate_cached(&self) -> Result<Option<&ValidationIssue>> {
        Ok(self
            .topology()?
            .validity
            .get_or_try_init(|| {
                self.shape()
                    .validate()?
                    .map(|issue| Box::try_new(issue).map_err(crate::error::Error::from))
                    .transpose()
            })?
            .as_deref())
    }

    pub(crate) fn is_simple_cached(&self) -> Result<bool> {
        Ok(*self
            .topology()?
            .simplicity
            .get_or_try_init(|| self.shape().is_simple())?)
    }

    pub(crate) fn crosses_antimeridian(&self) -> Result<bool> {
        Ok(*self
            .topology()?
            .antimeridian_crossing
            .get_or_init(|| self.shape().crosses_antimeridian()))
    }

    pub(crate) fn distance_parts(&self) -> Result<&DistanceParts> {
        if let Some(parts) = self.metric.get().and_then(|metric| metric.distance.get()) {
            return Ok(parts.as_ref());
        }
        self.metric()?
            .distance
            .get_or_try_init(|| {
                Box::try_new(self.shape().distance_parts()?).map_err(crate::error::Error::from)
            })
            .map(Box::as_ref)
    }

    pub(crate) fn distance_3d_parts(&self) -> Result<&crate::geometry::Distance3dParts> {
        if let Some(parts) = self
            .metric
            .get()
            .and_then(|metric| metric.distance_3d.get())
        {
            return Ok(parts.as_ref());
        }
        self.metric()?
            .distance_3d
            .get_or_try_init(|| {
                Box::try_new(crate::geometry::Distance3dParts::build(self.shape())?)
                    .map_err(crate::error::Error::from)
            })
            .map(Box::as_ref)
    }

    pub(crate) fn line_index(&self) -> Result<&LineIndex> {
        self.metric()?
            .line_index
            .get_or_try_init(|| {
                Box::try_new(LineIndexSlot::build(self.shape(), &PlanarMetric)?)
                    .map_err(crate::error::Error::from)
            })?
            .get()
    }

    pub(crate) fn measured_line_index(&self) -> Result<&MeasuredLineIndex> {
        self.metric()?
            .measured_line_index
            .get_or_try_init(|| {
                Box::try_new(MeasuredLineIndexSlot::build(self.shape())?)
                    .map_err(crate::error::Error::from)
            })?
            .get()
    }

    pub(crate) fn geodesic_line_index(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        runtime_generation: u64,
        metric: &impl SegmentMetric,
    ) -> Result<Arc<LineIndex>> {
        let key =
            GeodesicPartsKey::new_at_generation(crs, semi_major, flattening, runtime_generation)?;
        let cached = frame_cache.get(&key).and_then(|entry| entry.line_index);
        if let Some(index) = cached {
            return Ok(index);
        }
        let index = Arc::try_new(LineIndex::build(self.shape(), metric)?)
            .map_err(crate::error::Error::from)?;
        let entry = frame_cache.merge(key, |entry| {
            entry.line_index.get_or_insert_with(|| Arc::clone(&index));
        });
        Ok(entry.and_then(|entry| entry.line_index).unwrap_or(index))
    }

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
        let parts = Arc::try_new(self.shape().geodesic_parts(metric)?)
            .map_err(crate::error::Error::from)?;
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
        let key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        self.geodesic_parts(frame_cache, key, metric).map(|_| ())
    }

    pub(in crate::geometry) fn point_tester_required(
        &self,
        _required: crate::geometry::distance::RequiredPointTester,
    ) -> Result<Option<&PointBatchTester>> {
        Ok(self
            .topology()?
            .point_tester
            .get_or_try_init(|| -> Result<_> {
                if matches!(self.shape(), Shape::Polygon(_) | Shape::MultiPolygon(_)) {
                    let tester = PointBatchTester::new(self.shape())?;
                    Ok(Some(
                        Box::try_new(tester).map_err(crate::error::Error::from)?,
                    ))
                } else {
                    Ok(None)
                }
            })?
            .as_deref())
    }

    pub(crate) fn with_seeded_bounds(self, bounds: Option<Bounds>) -> Self {
        self.shape.seed_bounds(bounds);
        self
    }

    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.heap_bytes()
    }
}

impl std::ops::Deref for PreparedPlan {
    type Target = ShapeData;

    fn deref(&self) -> &Self::Target {
        self.shape_data()
    }
}

impl fmt::Debug for PreparedPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shape.fmt(f)
    }
}

impl HeapSize for ShapeData {
    fn heap_bytes(&self) -> usize {
        self.shape.heap_bytes()
    }
}

impl HeapSize for TopologyPlan {
    fn heap_bytes(&self) -> usize {
        self.validity.heap_bytes() + self.point_tester.heap_bytes()
    }
}

impl HeapSize for MetricPlan {
    fn heap_bytes(&self) -> usize {
        self.distance.heap_bytes()
            + self.distance_3d.heap_bytes()
            + self.line_index.heap_bytes()
            + self.measured_line_index.heap_bytes()
    }
}

impl HeapSize for PreparedPlan {
    fn heap_bytes(&self) -> usize {
        std::mem::size_of_val(self.shape.as_ref())
            + self.shape.as_ref().heap_bytes()
            + self.topology.heap_bytes()
            + self.metric.heap_bytes()
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
            + self.caps.heap_bytes()
            + self.facet_bvh.heap_bytes()
    }
}

impl HeapSize for GeodesicSweepCaps {
    fn heap_bytes(&self) -> usize {
        self.groups.heap_bytes()
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

pub(crate) trait IntoSharedShape {
    fn try_into_shared(self) -> crate::error::Result<Arc<ShapeData>>;
}

impl IntoSharedShape for Shape {
    fn try_into_shared(self) -> crate::error::Result<Arc<ShapeData>> {
        ShapeData::new(self).try_into_shared()
    }
}

impl IntoSharedShape for ShapeData {
    fn try_into_shared(self) -> crate::error::Result<Arc<ShapeData>> {
        Arc::try_new(self).map_err(crate::error::Error::from)
    }
}

impl IntoSharedShape for Arc<ShapeData> {
    fn try_into_shared(self) -> crate::error::Result<Arc<ShapeData>> {
        Ok(self)
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
        let data = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        let cold = data.retained_heap_bytes();
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.retained_heap_bytes(), cold);
        let _ = data.bounds();
        // Bounds are owned by ShapeData; they do not force a boxed product.
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.retained_heap_bytes(), cold);
        let _ = data.distance_parts().unwrap();
        assert!(data.metric.get().is_some());
        assert!(data.retained_heap_bytes() > cold);
    }

    #[test]
    fn cold_metric_sidecar_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        assert!(plan.metric.get().is_none());
        let Err(error) = fail_allocations_after(0, || plan.distance_parts()) else {
            panic!("distance parts allocation unexpectedly succeeded");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(plan.metric.get().is_none());
        let first = std::ptr::from_ref(plan.distance_parts().expect("retry after rejection"));
        let second = std::ptr::from_ref(plan.distance_parts().expect("cached distance parts"));
        assert_eq!(first, second);
        assert!(std::ptr::eq(
            plan.metric.get().expect("published sidecar").as_ref(),
            plan.metric.get().expect("reused sidecar").as_ref()
        ));
    }

    #[test]
    fn cold_nested_metric_product_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        plan.metric().expect("publish metric sidecar");
        assert!(plan.metric.get().expect("sidecar").distance.get().is_none());
        let Err(error) = fail_allocations_after(0, || plan.distance_parts()) else {
            panic!("nested distance parts allocation unexpectedly succeeded");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(
            plan.metric
                .get()
                .expect("sidecar remains")
                .distance
                .get()
                .is_none()
        );
        let first = std::ptr::from_ref(plan.distance_parts().expect("retry after rejection"));
        let second = std::ptr::from_ref(plan.distance_parts().expect("cached distance parts"));
        assert_eq!(first, second);
    }
}

#[cfg(test)]
mod frame_cache_tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, LineSeq};
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[test]
    fn frame_cache_key_includes_crs_ellipsoid_and_runtime_generation() {
        let base =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 4)
                .expect("test cache key");
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation(
                "EPSG:4267",
                6_378_137.0,
                1.0 / 298.257_223_563,
                4,
            )
            .expect("test cache key")
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation(
                "EPSG:4326",
                6_378_136.0,
                1.0 / 298.257_223_563,
                4,
            )
            .expect("test cache key")
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation(
                "EPSG:4326",
                6_378_137.0,
                1.0 / 298.257_223_563,
                5,
            )
            .expect("test cache key")
        );
    }

    #[test]
    fn differing_frame_or_generation_replaces_the_single_entry() {
        let cache = FrameDependentCaches::default();
        let first =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 4)
                .expect("test cache key");
        let next_generation =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 5)
                .expect("test cache key");
        let other_frame =
            GeodesicPartsKey::new_at_generation("EPSG:4267", 6_378_206.4, 1.0 / 294.978_698_2, 5)
                .expect("test cache key");
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
        let older = GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 4)
            .expect("test key");
        let newer = GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 5)
            .expect("test key");
        assert!(cache.merge(newer.clone(), |_| {}).is_some());
        assert!(cache.merge(older.clone(), |_| {}).is_none());
        assert!(cache.get(&newer).is_some());
        assert!(cache.get(&older).is_none());
    }

    #[test]
    fn repeated_lrs_reuses_the_same_line_index_arc() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        let cache = FrameDependentCaches::default();
        let first = shape
            .geodesic_line_index(&cache, "TEST", 1.0, 0.0, 0, &PlanarMetric)
            .expect("line index");
        let second = shape
            .geodesic_line_index(&cache, "TEST", 1.0, 0.0, 0, &PlanarMetric)
            .expect("cached line index");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn repeated_measured_lrs_reuses_one_column_product() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 1.0, 2.0],
                    vec![0.0, 1.0, 0.0],
                    None,
                    Some(vec![0.0, 1.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        let first = std::ptr::from_ref(shape.measured_line_index().expect("measured line index"));
        let second = std::ptr::from_ref(
            shape
                .measured_line_index()
                .expect("cached measured line index"),
        );
        assert_eq!(first, second);
        assert!(shape.retained_heap_bytes() > 0);
        let (interpolation, interpolation_allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("measured interpolation")
        });
        let (repeat_interpolation, repeat_interpolation_allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("repeated measured interpolation")
        });
        let (located, locate_allocations) = allocation_count(|| {
            shape
                .line_locate_point_m_with(Point::new_raw_xy(1.0, 0.0), &PlanarMetric)
                .expect("measured locate")
        });
        let (repeat_located, repeat_locate_allocations) = allocation_count(|| {
            shape
                .line_locate_point_m_with(Point::new_raw_xy(1.0, 0.0), &PlanarMetric)
                .expect("repeated measured locate")
        });
        assert_eq!(interpolation, repeat_interpolation);
        assert_eq!(located, repeat_located);
        assert_eq!(interpolation_allocations, 0);
        assert_eq!(repeat_interpolation_allocations, 0);
        assert_eq!(locate_allocations, 0);
        assert_eq!(repeat_locate_allocations, 0);
        let Shape::Point(interpolation) = interpolation else {
            panic!("measured interpolation must return a point");
        };
        assert_eq!(interpolation.x.to_bits(), 1.5_f64.to_bits());
        assert_eq!(interpolation.y.to_bits(), 0.5_f64.to_bits());
        assert_eq!(interpolation.m().unwrap().to_bits(), 1.5_f64.to_bits());
    }

    #[test]
    fn measured_planar_lrs_clamps_endpoints_and_carries_zm() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 1.0, 2.0],
                    vec![0.0, 1.0, 0.0],
                    Some(vec![10.0, 20.0, 30.0]),
                    Some(vec![0.0, 1.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        shape.measured_line_index().expect("measured line index");

        let (interpolation, allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("interior measured interpolation")
        });
        assert_eq!(allocations, 0);
        let Shape::Point(interpolation) = interpolation else {
            panic!("interior measured interpolation must return a point");
        };
        assert_eq!(interpolation.x.to_bits(), 1.5_f64.to_bits());
        assert_eq!(interpolation.y.to_bits(), 0.5_f64.to_bits());
        assert_eq!(interpolation.z().unwrap().to_bits(), 25.0_f64.to_bits());
        assert_eq!(interpolation.m().unwrap().to_bits(), 1.5_f64.to_bits());

        let Shape::Point(first) = shape
            .line_interpolate_point_m_with(-1.0, &PlanarMetric)
            .expect("lower measured clamp")
        else {
            panic!("lower measured clamp must return a point");
        };
        let Shape::Point(last) = shape
            .line_interpolate_point_m_with(3.0, &PlanarMetric)
            .expect("upper measured clamp")
        else {
            panic!("upper measured clamp must return a point");
        };
        assert_eq!(first.x.to_bits(), 0.0_f64.to_bits());
        assert_eq!(first.z().unwrap().to_bits(), 10.0_f64.to_bits());
        assert_eq!(first.m().unwrap().to_bits(), 0.0_f64.to_bits());
        assert_eq!(last.x.to_bits(), 2.0_f64.to_bits());
        assert_eq!(last.z().unwrap().to_bits(), 30.0_f64.to_bits());
        assert_eq!(last.m().unwrap().to_bits(), 2.0_f64.to_bits());

        let (located, allocations) = allocation_count(|| {
            shape
                .line_locate_point_m_with(Point::new_raw_xy(1.0, 0.0), &PlanarMetric)
                .expect("interior measured locate")
        });
        assert_eq!(located.to_bits(), 0.5_f64.to_bits());
        assert_eq!(allocations, 0);
    }

    #[test]
    fn measured_planar_lrs_skips_duplicate_zero_length_segment() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 0.0, 2.0],
                    vec![0.0, 0.0, 0.0],
                    Some(vec![7.0, 7.0, 9.0]),
                    Some(vec![0.0, 0.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        shape.measured_line_index().expect("measured line index");

        let (located, allocations) = allocation_count(|| {
            shape
                .line_locate_point_m_with(Point::new_raw_xy(1.0, 1.0), &PlanarMetric)
                .expect("locate past duplicate segment")
        });
        assert_eq!(located.to_bits(), 1.0_f64.to_bits());
        assert_eq!(allocations, 0);

        let Shape::Point(interpolated) = shape
            .line_interpolate_point_m_with(0.0, &PlanarMetric)
            .expect("duplicate endpoint interpolation")
        else {
            panic!("duplicate endpoint interpolation must return a point");
        };
        assert_eq!(interpolated.x.to_bits(), 0.0_f64.to_bits());
        assert_eq!(interpolated.z().unwrap().to_bits(), 7.0_f64.to_bits());
        assert_eq!(interpolated.m().unwrap().to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn measured_planar_lrs_exact_locate_preserves_subnormal_projection() {
        let minimum = f64::from_bits(1);
        let maximum = f64::MAX;
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, maximum],
                    vec![0.0, 0.0],
                    None,
                    Some(vec![0.0, maximum]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        shape.measured_line_index().expect("measured line index");

        let located = shape
            .line_locate_point_m_with(Point::new_raw_xy(minimum, 1.0), &PlanarMetric)
            .expect("exact subnormal measured locate");
        assert_eq!(located.to_bits(), minimum.to_bits());
    }

    #[test]
    fn cold_measured_line_product_allocation_failure_is_retryable() {
        let shape = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, Some(vec![0.0, 1.0]))
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let new_plan = || PreparedPlan::new(shape.clone()).expect("prepared line");
        let plan = new_plan();
        let (_, allocations) =
            allocation_count(|| plan.measured_line_index().expect("measured line product"));
        assert!(allocations > 0);
        for prefix in 0..allocations {
            let plan = new_plan();
            let Err(error) = fail_allocations_after(prefix, || plan.measured_line_index()) else {
                panic!("measured line product allocation unexpectedly succeeded");
            };
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            assert!(
                plan.metric
                    .get()
                    .is_none_or(|metric| metric.measured_line_index.get().is_none())
            );
        }

        let shape = new_plan();
        let first =
            std::ptr::from_ref(shape.measured_line_index().expect("retry measured product"));
        let second = std::ptr::from_ref(
            shape
                .measured_line_index()
                .expect("cached measured product"),
        );
        assert_eq!(first, second);
    }

    #[test]
    fn exact_measured_fraction_propagates_each_allocation_failure() {
        let max = f64::MAX;
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 2.0],
                    vec![0.0, 0.0],
                    Some(vec![10.0, 20.0]),
                    Some(vec![-max, max]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        shape.measured_line_index().expect("measured line index");

        let (_, allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(0.0, &PlanarMetric)
                .expect("exact measured interpolation")
        });
        // Planar exact measured interpolation is intentionally allocation-free;
        // lock zero so a future heap spill fails rather than weakening coverage.
        assert_eq!(allocations, 0);
        for prefix in 0..allocations {
            let error = fail_allocations_after(prefix, || {
                shape.line_interpolate_point_m_with(0.0, &PlanarMetric)
            })
            .unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn frame_entry_heap_bytes_survive_poison() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ))
        .unwrap();
        let cache = FrameDependentCaches::default();
        shape
            .geodesic_line_index(&cache, "TEST", 1.0, 0.0, 0, &PlanarMetric)
            .expect("line index");
        let before = cache.heap_bytes();
        assert!(before > 0);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = cache.entry.lock().unwrap();
            panic!("poison the frame entry mutex");
        }));
        assert!(result.is_err());

        assert_eq!(cache.heap_bytes(), before);
    }

    #[test]
    fn geographic_graph_heap_bytes_survive_poison() {
        let cache = FrameDependentCaches::default();
        let graph = Arc::new(
            crate::geometry::TopologyGraph::cartesian(&Shape::Point(Point::new_raw_xy(1.0, 2.0)))
                .unwrap(),
        );
        let expected = std::mem::size_of_val(graph.as_ref()) + graph.heap_bytes();
        assert!(expected > 0);
        cache.geographic_topology.lock().unwrap().graph = Some(Arc::clone(&graph));
        let before = cache.heap_bytes();
        assert_eq!(before, expected);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = cache.geographic_topology.lock().unwrap();
            panic!("poison the geographic graph mutex");
        }));
        assert!(result.is_err());

        assert_eq!(cache.heap_bytes(), before);
    }

    #[test]
    fn single_entry_container_adds_no_heap_allocation() {
        let cache = FrameDependentCaches::default();
        let key = GeodesicPartsKey::new_at_generation("TEST", 1.0, 0.0, 0).expect("test cache key");
        let key_heap = key.heap_bytes();
        let _ = cache.merge(key, |_| {});
        // The key's existing string storage and the empty entry account for
        // all retained bytes; unlike the former Vec-backed LRU, the container
        // itself retains no additional heap allocation.
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
                        )
                        .expect("test cache key");
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
        )
        .expect("test cache key");
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

/// Operation requested from the geodesic point-to-segment owner.
#[derive(Clone, Copy, Debug)]
pub(crate) enum GeodesicSegmentQuery {
    /// Return an attained minimum only when it is strictly below `cutoff`.
    Minimum {
        cutoff: f64,
        witness: GeodesicWitnessNeed,
    },
    /// Return an attained threshold hit after globally processing the live
    /// interval frontier.
    Threshold {
        limit: f64,
        witness: GeodesicWitnessNeed,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum GeodesicWitnessNeed {
    No,
    Yes,
}

/// Attained point-to-segment witness in the segment's along-track parameter.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicSegmentMinimum {
    pub(crate) distance: f64,
    pub(crate) along: f64,
}

impl GeodesicSegmentMinimum {
    pub(crate) const fn new(distance: f64, along: f64) -> Self {
        Self { distance, along }
    }
}

/// A resolved geodesic numerical minimum.
///
/// `witness` is the attained value and parameter. `lower` is the completed
/// search lower estimate, while `evaluation_error` names the position/inverse
/// allowance used to attain the witness. Neither field is a universal
/// real-arithmetic certificate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicMinimum {
    pub(crate) witness: GeodesicSegmentMinimum,
    pub(crate) lower: f64,
    pub(crate) evaluation_error: f64,
}

impl GeodesicMinimum {
    pub(crate) const fn from_witness(witness: GeodesicSegmentMinimum) -> Self {
        Self {
            witness,
            lower: 0.0,
            evaluation_error: 0.0,
        }
    }
}

impl std::ops::Deref for GeodesicMinimum {
    type Target = GeodesicSegmentMinimum;

    fn deref(&self) -> &Self::Target {
        &self.witness
    }
}

impl std::ops::DerefMut for GeodesicMinimum {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.witness
    }
}

/// Typed result of a point-to-segment query.
#[derive(Clone, Copy, Debug)]
pub(crate) enum GeodesicSegmentQueryResult {
    /// An attained value, with an optional actual foot when requested.
    Attained {
        minimum: GeodesicMinimum,
        foot: Option<Point>,
    },
    /// A pruning fact, never a metric value.
    Excluded,
    /// The standing conservative backend lower bound is above the threshold.
    ThresholdOutside,
    /// The resolved numerical minimum is outside a threshold. This result is
    /// not a real-arithmetic lower-bound certificate.
    ThresholdResolvedOutside { minimum: f64 },
}

/// The contact state of two admitted finite geodesic segments.
///
/// `Unresolved` deliberately retains an attained candidate rather than
/// promoting proximity to structural contact. It is an internal numerical
/// state; callers that need a value can use its witness and lower estimate.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SegmentContact {
    Contact { first_along: f64, second_along: f64 },
    Disjoint,
    Unresolved { candidate: SegmentPairWitness },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SegmentPairWitness {
    pub(crate) first: Point,
    pub(crate) second: Point,
    pub(crate) first_along: f64,
    pub(crate) second_along: f64,
    pub(crate) distance: f64,
}

/// Numerical budgets shared by the geodesic minimum, threshold, and Hausdorff
/// resolvers. Search runs in normalized units; public metric values use
/// `semi_major`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicNumericalPolicy {
    semi_major: f64,
}

impl GeodesicNumericalPolicy {
    pub(crate) const BACKEND_RELATIVE: f64 = 1e-11;
    pub(crate) const MINIMUM_GAP_RELATIVE: f64 = 4e-11;
    pub(crate) const HAUSDORFF_GAP_RELATIVE: f64 = 0.01 / 6_378_137.0;

    pub(crate) const fn new(semi_major: f64) -> Self {
        Self { semi_major }
    }

    pub(crate) const fn semi_major(self) -> f64 {
        self.semi_major
    }

    pub(crate) const fn normalized_backend_floor(self) -> f64 {
        Self::BACKEND_RELATIVE
    }

    pub(crate) const fn normalized_minimum_gap(self) -> f64 {
        Self::MINIMUM_GAP_RELATIVE
    }

    pub(crate) const fn normalized_hausdorff_gap(self) -> f64 {
        Self::HAUSDORFF_GAP_RELATIVE
    }

    pub(crate) fn backend_floor(self) -> f64 {
        self.semi_major * Self::BACKEND_RELATIVE
    }

    pub(crate) fn minimum_gap(self) -> f64 {
        self.semi_major * Self::MINIMUM_GAP_RELATIVE
    }

    pub(crate) fn hausdorff_gap(self) -> f64 {
        self.semi_major * self.normalized_hausdorff_gap()
    }

    pub(crate) fn evaluation_allowance(self, evaluations: f64) -> f64 {
        self.backend_floor() * evaluations.max(1.0)
    }
}

/// Reusable inner branch-and-bound frontier for one geodesic operation.
pub(crate) struct GeodesicSegmentScratch {
    pub(crate) inner: BinaryHeap<GeodesicInnerInterval>,
}

impl GeodesicSegmentScratch {
    pub(crate) const fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner.clear();
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.inner
            .capacity()
            .saturating_mul(std::mem::size_of::<GeodesicInnerInterval>())
    }
}

impl Default for GeodesicSegmentScratch {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicInnerInterval {
    pub(crate) lower: f64,
    pub(crate) left: f64,
    pub(crate) right: f64,
    pub(crate) left_value: f64,
    pub(crate) right_value: f64,
}

impl PartialEq for GeodesicInnerInterval {
    fn eq(&self, other: &Self) -> bool {
        self.lower.to_bits() == other.lower.to_bits()
            && self.left.to_bits() == other.left.to_bits()
            && self.right.to_bits() == other.right.to_bits()
    }
}

impl Eq for GeodesicInnerInterval {}

impl PartialOrd for GeodesicInnerInterval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GeodesicInnerInterval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .lower
            .total_cmp(&self.lower)
            .then_with(|| other.left.total_cmp(&self.left))
            .then_with(|| other.right.total_cmp(&self.right))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicSegmentDerivative {
    pub(crate) value: f64,
    pub(crate) error: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicSegmentSample {
    pub(crate) derivative: Option<GeodesicSegmentDerivative>,
}

/// One cached geodesic line with source-frame point, tangent, and sample
/// access for a continuous Hausdorff search.
pub(crate) trait GeodesicPreparedSegment: Copy {
    fn segment(&self) -> GeodesicSegment;
    fn sample(&self, point: Point, along: f64, derivative: bool) -> Result<GeodesicSegmentSample>;
    fn position(&self, along: f64) -> Result<Point>;
    fn tangent(&self, along: f64) -> Result<Option<[f64; 3]>>;
}

/// Ellipsoidal distance primitive injected into [`Shape::geodesic_distance`] so
/// the geometry layer stays free of any CRS/ellipsoid dependency. Implemented
/// in `crs.rs` over a `geographiclib` `Geodesic`.
pub(crate) trait GeodesicMetric {
    type PreparedSegment: GeodesicPreparedSegment;

    /// Runtime generation used to namespace CRS-dependent prepared state.
    /// Geometry-only test metrics use the default zero generation.
    fn runtime_generation(&self) -> u64 {
        0
    }

    /// Admit a shape in the metric's coordinate frame before any geodesic
    /// working set is built. Geometry-only metrics leave admission to their
    /// caller; CRS-backed metrics validate their native angular axes here.
    fn ensure_shape(&self, _shape: &Shape) -> Result<()> {
        Ok(())
    }

    /// Auxiliary-sphere location used only to partition the geodesic facet
    /// tree. Providers receive native-frame XY only: Z/M and axis presence are
    /// not part of this heuristic. Implementations with a native angular frame
    /// convert here; the default retains the historical degree-frame behavior
    /// for geometry-only metrics.
    fn auxiliary_unit(&self, point: XY) -> [f64; 3] {
        let lon = point.x.to_radians();
        let lat = point.y.to_radians();
        let beta = f64::atan((1.0 - 1.0 / 298.257_223_563) * lat.tan())
            .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2);
        [beta.cos() * lon.cos(), beta.cos() * lon.sin(), beta.sin()]
    }

    /// Precompute the ellipsoidal metadata shared by every exact probe of one
    /// segment.
    fn make_segment(&self, start: Point, end: Point) -> Result<GeodesicSegment>;

    /// Prepare one reusable geodesic line for continuous source/target
    /// interval work.
    fn prepare_segment(&self, segment: &GeodesicSegment) -> Result<Self::PreparedSegment>;

    /// Run the shared point-to-segment query owner.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn point_segment_query(
        &self,
        point: Point,
        segment: GeodesicSegment,
        query: GeodesicSegmentQuery,
        scratch: &mut GeodesicSegmentScratch,
    ) -> Result<GeodesicSegmentQueryResult>;

    /// Resolve contact for two already-admitted geodesic segments. The
    /// segments retain their source-frame endpoints and metric along-track
    /// lengths; an implementation may only convert that frame here.
    fn segment_contact(
        &self,
        first: &GeodesicSegment,
        second: &GeodesicSegment,
    ) -> Result<SegmentContact>;

    /// Geodesic length (meters) of segment `a`–`b`.
    fn segment_length(&self, a: Point, b: Point) -> f64;

    /// The point a `fraction` of the geodesic length along `a`→`b`, with Z and
    /// M interpolated linearly by `fraction`.
    fn interpolate(&self, a: Point, b: Point, fraction: f64) -> Point;

    /// A cheap (no-inverse) lower bound on the geodesic distance between two
    /// points — the auxiliary-sphere reduced-latitude bound. Drives the
    /// segment pruning in batch locate sweeps; preparation failures propagate
    /// instead of disabling the bound.
    fn point_distance_lower_bound(&self, a: Point, b: Point) -> Result<f64>;

    /// Curvature and injectivity bounds for the admitted ellipsoid.
    fn geodesic_curvature_domain(&self) -> Option<(f64, f64, f64)>;

    /// Shared numerical policy for this ellipsoid.
    fn geodesic_numerical_policy(&self) -> GeodesicNumericalPolicy;

    /// Radius-scaled allowance for the binary64 geodesic backend.
    fn geodesic_backend_floor(&self) -> f64 {
        self.geodesic_numerical_policy().backend_floor()
    }

    /// Semi-major radius used by exact spherical family formulas.
    fn geodesic_radius(&self) -> f64 {
        self.geodesic_numerical_policy().semi_major()
    }

    /// Unit surface vector in the ellipsoid's canonical Cartesian frame.
    fn surface_unit(&self, point: Point) -> Option<[f64; 3]>;

    /// Exact family predicates used only to close focal plateaus.
    fn is_spherical(&self) -> bool;
    fn is_equatorial_segment(&self, segment: &GeodesicSegment) -> bool;
    fn is_polar_point(&self, point: Point) -> bool;

    /// Along-track parameter of an equatorial source at the target point's
    /// opposite meridian, when the metric can apply that exact family.
    fn equatorial_opposite_meridian_parameter(
        &self,
        _segment: &GeodesicSegment,
        _target: Point,
    ) -> Option<f64> {
        None
    }
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
    pub code: &'static str,
    pub detail: String,
    pub location: Option<Point>,
    pub path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PolygonizeFull {
    pub polygons: Vec<Shape>,
    pub cuts: Vec<Vec<Shape>>,
    pub dangles: Vec<Vec<Shape>>,
    pub invalid_rings: Vec<Vec<Shape>>,
}
