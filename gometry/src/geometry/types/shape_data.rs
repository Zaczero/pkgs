#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
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
    cartesian_graph: OnceLock<Box<crate::geometry::topology::TopologyGraph>>,
    #[cfg(test)]
    cartesian_graph_accesses: AtomicUsize,
    validity: OnceLock<Option<Box<ValidationIssue>>>,
    simplicity: OnceLock<bool>,
    antimeridian_crossing: OnceLock<bool>,
    point_tester: OnceLock<Option<Box<PointBatchTester>>>,
    /// The direct relation engines' stagings, built once per operand.
    linework: OnceLock<Option<Box<crate::geometry::StagedLinework>>>,
    rings: OnceLock<Option<Box<crate::geometry::StagedRings>>>,
    /// Whether every ring of an areal shape has area (the direct engines'
    /// gate); `false` for a non-areal shape. Each ring is scanned once per
    /// plan.
    areal_rings_have_area: OnceLock<bool>,
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
    /// every coordinate on each call. Mirrors GEOS, which caches the
    /// geometry envelope rather than recomputing it.
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

    pub(crate) fn topology_graph(&self) -> Result<&crate::geometry::TopologyGraph> {
        let topology = self.topology()?;
        #[cfg(test)]
        topology
            .cartesian_graph_accesses
            .fetch_add(1, Ordering::Relaxed);
        topology
            .cartesian_graph
            .get_or_try_init(|| {
                Box::try_new(crate::geometry::topology::TopologyGraph::cartesian(
                    self.shape(),
                )?)
                .map_err(crate::error::Error::from)
            })
            .map(Box::as_ref)
    }

    #[cfg(test)]
    pub(crate) fn topology_graph_accesses(&self) -> usize {
        self.topology.get().map_or(0, |topology| {
            topology.cartesian_graph_accesses.load(Ordering::Relaxed)
        })
    }

    /// The shape staged as linework for the direct relation engine, `None`
    /// when it is not a line operand they admit.
    pub(crate) fn staged_linework(&self) -> Result<Option<&crate::geometry::StagedLinework>> {
        Ok(self
            .topology()?
            .linework
            .get_or_try_init(|| {
                crate::geometry::stage_linework(self.shape())
                    .map(|staged| Box::try_new(staged).map_err(crate::error::Error::from))
                    .transpose()
            })?
            .as_deref())
    }

    /// Whether every ring of this areal shape has a nonzero exact area
    /// (`ring_has_area`), decided once per plan.
    pub(crate) fn areal_rings_have_area(&self) -> Result<bool> {
        Ok(*self.topology()?.areal_rings_have_area.get_or_init(|| {
            let mut polygons = Vec::new();
            crate::geometry::collect_polygons(self.shape(), &mut polygons).is_some()
                && polygons
                    .iter()
                    .all(|polygon| polygon.rings().all(crate::geometry::ring_has_area))
        }))
    }

    /// The shape's rings staged for the direct relation engine, `None` when
    /// it is not an areal operand.
    pub(crate) fn staged_rings(&self) -> Result<Option<&crate::geometry::StagedRings>> {
        Ok(self
            .topology()?
            .rings
            .get_or_try_init(|| {
                crate::geometry::stage_rings(self.shape())
                    .map(|staged| Box::try_new(staged).map_err(crate::error::Error::from))
                    .transpose()
            })?
            .as_deref())
    }

    pub(crate) fn validate_cached(&self) -> Result<Option<&ValidationIssue>> {
        Ok(self
            .topology()?
            .validity
            .get_or_try_init(|| {
                self.shape()
                    .validate()
                    .map(|issue| Box::try_new(issue).map_err(crate::error::Error::from))
                    .transpose()
            })?
            .as_deref())
    }

    pub(crate) fn is_simple_cached(&self) -> Result<bool> {
        Ok(*self
            .topology()?
            .simplicity
            .get_or_init(|| self.shape().is_simple()))
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
                Box::try_new(LineIndexSlot::build(self.shape(), &PlanarMetric))
                    .map_err(crate::error::Error::from)
            })?
            .get()
    }

    pub(crate) fn measured_line_index(&self) -> Result<&MeasuredLineIndex> {
        self.metric()?
            .measured_line_index
            .get_or_try_init(|| {
                Box::try_new(MeasuredLineIndexSlot::build(self.shape()))
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
            GeodesicPartsKey::new_at_generation(crs, semi_major, flattening, runtime_generation);
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
        );
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
        self.cartesian_graph.heap_bytes()
            + self.validity.heap_bytes()
            + self.point_tester.heap_bytes()
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
    fn cold_topology_sidecar_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        assert!(plan.topology.get().is_none());
        assert_eq!(plan.topology_graph_accesses(), 0);
        let error = fail_allocations_after(0, || plan.topology_graph()).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(plan.topology.get().is_none());
        let first = plan.topology_graph().expect("retry after rejection") as *const _;
        let second = plan.topology_graph().expect("cached topology graph") as *const _;
        assert_eq!(first, second);
        assert!(std::ptr::eq(
            plan.topology.get().expect("published sidecar").as_ref(),
            plan.topology.get().expect("reused sidecar").as_ref()
        ));
    }

    #[test]
    fn cold_metric_sidecar_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        assert!(plan.metric.get().is_none());
        let error = fail_allocations_after(0, || plan.distance_parts()).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(plan.metric.get().is_none());
        let first = plan.distance_parts().expect("retry after rejection") as *const _;
        let second = plan.distance_parts().expect("cached distance parts") as *const _;
        assert_eq!(first, second);
        assert!(std::ptr::eq(
            plan.metric.get().expect("published sidecar").as_ref(),
            plan.metric.get().expect("reused sidecar").as_ref()
        ));
    }

    #[test]
    fn cold_nested_topology_product_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::geometry::{CoordSeq, LineSeq};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 0.0], None, None).unwrap(),
            )
            .expect("test line has two vertices"),
        ))
        .unwrap();
        plan.topology().expect("publish topology sidecar");
        assert!(
            plan.topology
                .get()
                .expect("sidecar")
                .linework
                .get()
                .is_none()
        );
        let error = fail_allocations_after(0, || plan.staged_linework()).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(
            plan.topology
                .get()
                .expect("sidecar remains")
                .linework
                .get()
                .is_none()
        );
        let first = plan
            .staged_linework()
            .expect("retry after rejection")
            .expect("linework is admitted") as *const _;
        let second = plan
            .staged_linework()
            .expect("cached linework")
            .expect("linework is admitted") as *const _;
        assert_eq!(first, second);
    }

    #[test]
    fn cold_nested_metric_product_allocation_failure_is_retryable() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))).unwrap();
        plan.metric().expect("publish metric sidecar");
        assert!(plan.metric.get().expect("sidecar").distance.get().is_none());
        let error = fail_allocations_after(0, || plan.distance_parts()).unwrap_err();
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
        let first = plan.distance_parts().expect("retry after rejection") as *const _;
        let second = plan.distance_parts().expect("cached distance parts") as *const _;
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
        let (_, interpolation_allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("measured interpolation")
        });
        let (_, locate_allocations) = allocation_count(|| {
            shape
                .line_locate_point_m_with(Point::new_raw_xy(1.0, 0.0), &PlanarMetric)
                .expect("measured locate")
        });
        assert_eq!(interpolation_allocations, 0);
        assert_eq!(locate_allocations, 0);
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
        assert!(allocations > 0);
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

    /// Geodesic distance (meters) from `point` to the geodesic segment `a`–`b`
    /// (or to `a` when degenerate). `best` is the smallest distance found so
    /// far: implementations may skip expensive along-track refinement and
    /// return any value `>= best` once a cheap lower bound proves the segment
    /// cannot improve on it.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn point_to_segment(&self, point: Point, segment: GeodesicSegment, best: f64) -> Result<f64>;

    /// Whether the distance from `point` to `segment` is at most `limit`.
    /// Implementations return `false` only after proving the whole segment is
    /// outside the threshold.
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn point_to_segment_within(
        &self,
        point: Point,
        segment: GeodesicSegment,
        limit: f64,
    ) -> Result<bool>;

    /// Whether the selected shortest geodesic segments `a`–`b` and `c`–`d`
    /// intersect on the ellipsoid.
    fn segments_cross(&self, a: Point, b: Point, c: Point, d: Point) -> Result<bool>;

    /// Certified intersection witness for two geodesic segments. `None` means
    /// the documented segment solver found no intersection.
    fn segment_intersection(
        &self,
        a: Point,
        b: Point,
        c: Point,
        d: Point,
    ) -> Result<Option<(Point, Point)>>;

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
    fn locate_on_segment(
        &self,
        point: Point,
        segment: GeodesicSegment,
        best: f64,
    ) -> Result<(f64, f64)>;

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
    ) -> Result<GeodesicSegmentWitness>;

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
