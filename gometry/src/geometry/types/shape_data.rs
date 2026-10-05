use std::collections::BinaryHeap;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::HeapSize;
use crate::crs::ChartEdge;
use crate::error::Result;
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::antimeridian::validate_structure;
use crate::geometry::distance::ChartParts;
use crate::geometry::source_support::{
    OccupiedToward, PointMembership, SourcePointLocation, cartesian_exact_offset_location_and_area,
    cartesian_exact_point_location_and_area, cartesian_point_location_and_area, exact_stored_point,
    occupied_toward,
};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::topology::ExactGeographicTopology;
use crate::geometry::types::{Bounds, GeodesicSegment, Point, Segment, Shape, XY};
use crate::geometry::{
    ChartLineIndex, DistanceParts, GeodesicParts, GeodesicPartsKey, GeographicPrepared,
    GeographicTopologyRoute, LineIndex, LineIndexSlot, MeasuredLineIndex, MeasuredLineIndexSlot,
    PlanarMetric, PointBatchTester, ShapeCountQuery,
};

fn point_outside_bounds(bounds: Option<Bounds>, point: Point) -> bool {
    bounds.is_none_or(|bounds| {
        point.x < bounds.minx()
            || point.x > bounds.maxx()
            || point.y < bounds.miny()
            || point.y > bounds.maxy()
    })
}

const POINT_SCANS_BEFORE_INDEXING: u32 = 3;
// The index's fixed setup does not amortize for small source arrays.
const SMALL_POINT_INDEX_OPERAND: usize = 16;

/// Sidecar for geodesic products keyed by ellipsoid and PROJ runtime generation.
#[derive(Default)]
pub(crate) struct FrameDependentCaches {
    entry: Mutex<Option<(GeodesicPartsKey, FrameDependentEntry)>>,
    /// Admission facts for one geographic frame.
    admission: Mutex<
        Option<(
            crate::geographic::GeographicFrame,
            bool,
            GeographicTopologyRoute,
        )>,
    >,
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
struct GeographicTopologyProducts {
    route: OnceLock<GeographicTopologyRoute>,
    exact: OnceLock<Arc<ExactGeographicTopology>>,
    diagnostic_exact: OnceLock<Arc<ExactGeographicTopology>>,
    validity: OnceLock<Option<Box<ValidationIssue>>>,
}

#[derive(Clone, Default)]
struct FrameDependentEntry {
    line_index: Option<Arc<ChartLineIndex>>,
    parts: Option<Arc<GeodesicParts>>,
    chart_parts: Option<Arc<ChartParts>>,
    measured_chart_parts: Option<Arc<ChartParts>>,
}

impl FrameDependentCaches {
    /// Whether the raw box is sound in `frame`, once the coordinates have
    /// been admitted there; `None` before that admission.
    pub(crate) fn geographic_admission(
        &self,
        frame: crate::geographic::GeographicFrame,
    ) -> Option<(bool, GeographicTopologyRoute)> {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .filter(|&(admitted, ..)| admitted == frame)
            .map(|(_, bounds_sound, route)| (bounds_sound, route))
    }

    pub(crate) fn record_admission(
        &self,
        frame: crate::geographic::GeographicFrame,
        bounds_sound: bool,
        route: GeographicTopologyRoute,
    ) {
        *self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((frame, bounds_sound, route));
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

    pub(in crate::geometry) fn cached_chart_parts(
        &self,
        key: &GeodesicPartsKey,
    ) -> Option<Arc<ChartParts>> {
        self.get(key).and_then(|entry| entry.chart_parts)
    }

    pub(in crate::geometry) fn merge_chart_parts(
        &self,
        key: &GeodesicPartsKey,
        parts: &Arc<ChartParts>,
    ) -> Option<Arc<ChartParts>> {
        self.merge(key.clone(), |entry| {
            entry.chart_parts.get_or_insert_with(|| Arc::clone(parts));
        })
        .and_then(|entry| entry.chart_parts)
    }
}

impl HeapSize for FrameDependentEntry {
    fn heap_bytes(&self) -> usize {
        let shared_chart_parts = self
            .line_index
            .as_ref()
            .zip(self.chart_parts.as_ref())
            .is_some_and(|(index, parts)| Arc::ptr_eq(index.parts(), parts));
        self.line_index
            .heap_bytes()
            .saturating_add(self.parts.heap_bytes())
            .saturating_add(if shared_chart_parts {
                0
            } else {
                self.chart_parts.heap_bytes()
            })
            .saturating_add(self.measured_chart_parts.heap_bytes())
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
/// The geographic graph owns seam charts and frame-keyed pole quotient topology.
/// The point tester is separate and lazy so preparing a linear shape never
/// constructs an inapplicable areal tester.
#[derive(Default)]
struct TopologyPlan {
    structure: OnceLock<Option<Box<crate::geometry::ValidationIssue>>>,
    validity: OnceLock<Option<Box<crate::geometry::ValidationIssue>>>,
    simplicity: OnceLock<bool>,
    geographic: Mutex<Option<(GeographicTopologyKey, Arc<GeographicTopologyProducts>)>>,
    point_tester: OnceLock<Option<Box<PointBatchTester>>>,
    point_scans: std::sync::atomic::AtomicU32,
    #[cfg(test)]
    geographic_exact_topology_accesses: AtomicUsize,
    #[cfg(test)]
    geographic_exact_topology_builds: AtomicUsize,
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
/// it after the operation. Geodesic products remain in the caller-owned
/// [`FrameDependentCaches`] sidecar because their interpretation depends on
/// ellipsoid and PROJ runtime generation.
pub(crate) struct PreparedPlan {
    shape: Arc<ShapeData>,
    topology: OnceLock<Box<TopologyPlan>>,
    metric: OnceLock<Box<MetricPlan>>,
}

impl ShapeData {
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
        self.bounds.get_or_init(|| bounds);
    }
}

impl PreparedPlan {
    pub(crate) fn new(shape: impl IntoSharedShape) -> Self {
        Self {
            shape: shape.into_shared(),
            topology: OnceLock::new(),
            metric: OnceLock::new(),
        }
    }

    pub(crate) fn transient(shape: &Shape) -> Self {
        Self::new(ShapeData::new(shape.clone()))
    }

    pub(crate) fn shape_data(&self) -> &ShapeData {
        &self.shape
    }

    pub(crate) const fn shape_arc(&self) -> &Arc<ShapeData> {
        &self.shape
    }

    fn topology(&self) -> &TopologyPlan {
        self.topology
            .get_or_init(|| Box::new(TopologyPlan::default()))
            .as_ref()
    }

    fn metric(&self) -> &MetricPlan {
        self.metric
            .get_or_init(|| Box::new(MetricPlan::default()))
            .as_ref()
    }

    pub(crate) fn validate_cached(&self) -> Result<Option<&ValidationIssue>> {
        Ok(self
            .topology()
            .validity
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(self.shape().validate()?.map(Box::new))
            })?
            .as_deref())
    }

    pub(crate) fn validate_structure_cached(&self) -> Option<&ValidationIssue> {
        self.topology()
            .structure
            .get_or_init(|| validate_structure(self.shape(), "$").map(Box::new))
            .as_deref()
    }

    pub(crate) fn is_simple_cached(&self) -> Result<bool> {
        Ok(*self
            .topology()
            .simplicity
            .get_or_try_init(|| self.shape().is_simple())?)
    }

    pub(crate) fn geographic_validity_route(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> GeographicTopologyRoute {
        *self
            .topology()
            .geographic_products(view.frame())
            .route
            .get_or_init(|| crate::geometry::classify_geographic_validity_chart(view))
    }

    pub(crate) fn geographic_exact_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        self.topology().geographic_exact_topology(view)
    }

    pub(crate) fn quotient_validity(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Option<ValidationIssue>> {
        let products = self.topology().geographic_products(view.frame());
        Ok(products
            .validity
            .get_or_try_init(|| {
                self.geographic_exact_topology(view)?
                    .validity_issue(self.shape())
                    .map(|issue| issue.map(Box::new))
            })?
            .as_deref()
            .cloned())
    }

    pub(crate) fn geographic_diagnostic_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        if !matches!(self.shape(), Shape::GeometryCollection(_)) {
            return self.geographic_exact_topology(view);
        }
        let products = self.topology().geographic_products(view.frame());
        products
            .diagnostic_exact
            .get_or_try_init(|| {
                // Set membership can fold shared owners; diagnostics observe
                // the contacts between their distinct source occurrences.
                if self.shape().has_repeated_shared_owner() {
                    ExactGeographicTopology::from_views(view.frame(), std::iter::once(view))
                        .map(Arc::new)
                } else {
                    self.geographic_exact_topology(view)
                }
            })
            .map(Arc::clone)
    }

    #[cfg(test)]
    pub(crate) fn geographic_exact_topology_accesses(&self) -> usize {
        self.topology.get().map_or(0, |plan| {
            plan.geographic_exact_topology_accesses
                .load(Ordering::Relaxed)
        })
    }

    #[cfg(test)]
    pub(crate) fn geographic_exact_topology_builds(&self) -> usize {
        self.topology.get().map_or(0, |plan| {
            plan.geographic_exact_topology_builds
                .load(Ordering::Relaxed)
        })
    }

    pub(crate) fn distance_parts(&self) -> Result<&DistanceParts> {
        if let Some(parts) = self.metric.get().and_then(|metric| metric.distance.get()) {
            return Ok(parts.as_ref());
        }
        self.metric()
            .distance
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(self.shape().distance_parts()?))
            })
            .map(Box::as_ref)
    }

    pub(crate) fn distance_3d_parts(&self) -> &crate::geometry::Distance3dParts {
        if let Some(parts) = self
            .metric
            .get()
            .and_then(|metric| metric.distance_3d.get())
        {
            return parts.as_ref();
        }
        self.metric()
            .distance_3d
            .get_or_init(|| Box::new(crate::geometry::Distance3dParts::build(self.shape())))
            .as_ref()
    }

    pub(crate) fn line_index(&self) -> Result<&LineIndex> {
        self.metric()
            .line_index
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(LineIndexSlot::build(
                    self.shape(),
                    &PlanarMetric,
                )?))
            })?
            .get()
    }

    pub(crate) fn measured_line_index(&self) -> Result<&MeasuredLineIndex> {
        self.metric()
            .measured_line_index
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(MeasuredLineIndexSlot::build(self.shape())?))
            })?
            .get()
    }

    pub(crate) fn chart_line_index(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        runtime_generation: u64,
        metric: &impl ChartMetric,
    ) -> Result<Arc<ChartLineIndex>> {
        let key =
            GeodesicPartsKey::new_at_generation(crs, semi_major, flattening, runtime_generation);
        let cached = frame_cache.get(&key).and_then(|entry| entry.line_index);
        if let Some(index) = cached {
            return Ok(index);
        }
        if frame_cache.cached_chart_parts(&key).is_none() {
            GeographicPrepared::try_new(self, frame_cache, metric.chart_frame())?;
        }
        self.shape().linework()?;
        let parts = self.cached_chart_parts(frame_cache, &key, metric)?;
        let index = Arc::new(ChartLineIndex::from_chart_parts(
            self.shape(),
            parts,
            metric,
        )?);
        let entry = frame_cache.merge(key, |entry| {
            entry.line_index.get_or_insert_with(|| Arc::clone(&index));
        });
        Ok(entry.and_then(|entry| entry.line_index).unwrap_or(index))
    }

    pub(crate) fn measured_chart_parts(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        generation: u64,
        metric: &impl ChartMetric,
    ) -> Result<Arc<ChartParts>> {
        let key = GeodesicPartsKey::new_at_generation(crs, semi_major, flattening, generation);
        if let Some(parts) = frame_cache
            .get(&key)
            .and_then(|entry| entry.measured_chart_parts)
        {
            return Ok(parts);
        }
        GeographicPrepared::try_new(self, frame_cache, metric.chart_frame())?;
        let index = self.measured_line_index()?;
        let parts = Arc::new(ChartParts::measured_route(index.points(), metric)?);
        let entry = frame_cache.merge(key, |entry| {
            entry
                .measured_chart_parts
                .get_or_insert_with(|| Arc::clone(&parts));
        });
        Ok(entry
            .and_then(|entry| entry.measured_chart_parts)
            .unwrap_or(parts))
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
        let parts = Arc::new(self.shape().geodesic_parts(metric)?);
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

    /// Exact Cartesian location of `point`. Early probes scan the source;
    /// the indexed tester costs about three scans to build, so it is built
    /// once this plan has paid that much, and a repeated operand ends up
    /// indexed at no more than twice the cost of knowing in advance. Small
    /// sources stay streamed because their index setup cannot pay back.
    pub(crate) fn cartesian_point_location(&self, point: Point) -> Result<crate::geometry::Loc> {
        if let Some(tester) = self.point_tester_for_location()? {
            return Ok(tester.location(point));
        }
        Ok(crate::geometry::cartesian_point_location(
            self.shape(),
            point,
        ))
    }

    pub(in crate::geometry) fn locate_stored(&self, point: Point) -> Result<SourcePointLocation> {
        if let Some(tester) = self.point_tester_for_location()? {
            return Ok(tester.location_and_area(point));
        }
        Ok(cartesian_point_location_and_area(self.shape(), point))
    }

    pub(in crate::geometry) fn locate_exact(
        &self,
        point: &[ExactDyadic; 3],
    ) -> Result<SourcePointLocation> {
        self.locate_exact_with_stored(point, exact_stored_point(point))
    }

    /// Locate from a borrowed source, deferring its owned plan until a stored
    /// exact point survives the source bounds already available to the caller.
    pub(in crate::geometry) fn locate_exact_lazy(
        cached_plan: &OnceLock<Self>,
        shape: &Shape,
        bounds: Option<Bounds>,
        point: &[ExactDyadic; 3],
    ) -> Result<SourcePointLocation> {
        let stored = exact_stored_point(point);
        if stored.is_some_and(|stored| point_outside_bounds(bounds, stored)) {
            return Ok(SourcePointLocation {
                location: crate::geometry::Loc::Exterior,
                area: false,
            });
        }
        let plan = cached_plan.get_or_init(|| {
            let data = ShapeData::new(shape.clone());
            data.seed_bounds(bounds);
            Self::new(data)
        });
        plan.locate_exact_with_stored(point, stored)
    }

    fn locate_exact_with_stored(
        &self,
        point: &[ExactDyadic; 3],
        stored: Option<Point>,
    ) -> Result<SourcePointLocation> {
        if let Some(stored) = stored {
            if self
                .shape
                .bounds
                .get()
                .is_some_and(|bounds| point_outside_bounds(*bounds, stored))
            {
                return Ok(SourcePointLocation {
                    location: crate::geometry::Loc::Exterior,
                    area: false,
                });
            }
            return self.locate_stored(stored);
        }
        if let Some(tester) = self.point_tester_for_location()?
            && let Some(location) = tester.location_and_area_exact(point)?
        {
            return Ok(location);
        }
        cartesian_exact_point_location_and_area(self.shape(), point)
    }

    pub(in crate::geometry) fn locate_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<SourcePointLocation> {
        if let Some(tester) = self.point_tester_for_location()?
            && let Some(location) = tester.location_and_area_exact_offset(point, direction)?
        {
            return Ok(location);
        }
        cartesian_exact_offset_location_and_area(self.shape(), point, direction)
    }

    pub(in crate::geometry) fn occupied_toward_exact(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<OccupiedToward> {
        self.locate_exact_offset(point, direction)
            .map(occupied_toward)
    }

    fn point_tester_for_location(&self) -> Result<Option<&PointBatchTester>> {
        let topology = self.topology();
        if let Some(tester) = topology.point_tester.get() {
            return Ok(tester.as_deref());
        }
        if topology
            .point_scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            >= POINT_SCANS_BEFORE_INDEXING
        {
            self.point_tester()
        } else {
            Ok(None)
        }
    }

    /// Boolean set membership does not observe a line point's boundary versus
    /// interior label, so shared collection owners can be visited once.
    pub(crate) fn cartesian_point_covers(&self, point: Point) -> Result<bool> {
        if matches!(self.shape(), Shape::GeometryCollection(_)) {
            return Ok(crate::geometry::cartesian_point_covers(self.shape(), point));
        }
        self.cartesian_point_location(point)
            .map(|location| location != crate::geometry::Loc::Exterior)
    }

    pub(crate) fn cartesian_point_membership(
        &self,
        point: Point,
        membership: PointMembership,
    ) -> Result<bool> {
        if self
            .bounds()
            .is_none_or(|bounds| !bounds.contains_xy(point.xy()))
        {
            return Ok(false);
        }
        match membership {
            PointMembership::Strict => self
                .cartesian_point_location(point)
                .map(|location| location == crate::geometry::Loc::Interior),
            PointMembership::BoundaryInclusive => self.cartesian_point_covers(point),
        }
    }

    #[cfg(test)]
    pub(crate) fn point_location_scans(&self) -> u32 {
        self.topology()
            .point_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The indexed point tester of a polygonal shape, built on first use.
    pub(crate) fn point_tester(&self) -> Result<Option<&PointBatchTester>> {
        Ok(self
            .topology()
            .point_tester
            .get_or_try_init(|| -> Result<_> {
                if matches!(self.shape(), Shape::Polygon(_) | Shape::MultiPolygon(_)) {
                    let mut counts = ShapeCountQuery::default();
                    if counts.coordinates_at_most(self.shape(), SMALL_POINT_INDEX_OPERAND) {
                        return Ok(None);
                    }
                    let tester = PointBatchTester::new(self.shape())?;
                    Ok(Some(Box::new(tester)))
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
}

impl TopologyPlan {
    fn geographic_products(&self, frame: GeographicFrame) -> Arc<GeographicTopologyProducts> {
        let key = GeographicTopologyKey::new(frame);
        let mut slot = self
            .geographic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((known, products)) = slot.as_ref()
            && *known == key
        {
            return Arc::clone(products);
        }
        let products = Arc::new(GeographicTopologyProducts::default());
        *slot = Some((key, Arc::clone(&products)));
        products
    }

    fn geographic_exact_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        #[cfg(test)]
        self.geographic_exact_topology_accesses
            .fetch_add(1, Ordering::Relaxed);
        let products = self.geographic_products(view.frame());
        products
            .exact
            .get_or_try_init(|| {
                #[cfg(test)]
                self.geographic_exact_topology_builds
                    .fetch_add(1, Ordering::Relaxed);
                ExactGeographicTopology::from_views_as_set(view.frame(), std::iter::once(view))
                    .map(Arc::new)
            })
            .map(Arc::clone)
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
        self.shape.logical_heap_bytes()
    }
}

impl HeapSize for TopologyPlan {
    fn heap_bytes(&self) -> usize {
        self.structure
            .heap_bytes()
            .saturating_add(self.validity.heap_bytes())
            .saturating_add(self.point_tester.heap_bytes())
            .saturating_add(
                self.geographic
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map_or(0, |(_, products)| products.heap_bytes()),
            )
    }
}

impl HeapSize for GeographicTopologyProducts {
    fn heap_bytes(&self) -> usize {
        let exact = self.exact.heap_bytes();
        let diagnostic = self.diagnostic_exact.get().map_or(0, |diagnostic| {
            if self
                .exact
                .get()
                .is_some_and(|exact| Arc::ptr_eq(exact, diagnostic))
            {
                0
            } else {
                diagnostic.heap_bytes()
            }
        });
        exact
            .saturating_add(diagnostic)
            .saturating_add(self.validity.heap_bytes())
    }
}

impl HeapSize for MetricPlan {
    fn heap_bytes(&self) -> usize {
        self.distance
            .heap_bytes()
            .saturating_add(self.distance_3d.heap_bytes())
            .saturating_add(self.line_index.heap_bytes())
            .saturating_add(self.measured_line_index.heap_bytes())
    }
}

impl HeapSize for PreparedPlan {
    fn heap_bytes(&self) -> usize {
        self.shape
            .heap_bytes()
            .saturating_add(self.topology.heap_bytes())
            .saturating_add(self.metric.heap_bytes())
    }
}

impl HeapSize for DistanceParts {
    fn heap_bytes(&self) -> usize {
        self.point_only
            .heap_bytes()
            .saturating_add(self.linework.heap_bytes())
            .saturating_add(self.facet_bvh.heap_bytes())
            .saturating_add(self.point_index.heap_bytes())
    }
}

impl HeapSize for GeodesicParts {
    fn heap_bytes(&self) -> usize {
        self.points
            .heap_bytes()
            .saturating_add(self.segments.heap_bytes())
            .saturating_add(self.point_only.heap_bytes())
            .saturating_add(self.facet_bvh.heap_bytes())
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
    fn into_shared(self) -> Arc<ShapeData>;
}

impl IntoSharedShape for Shape {
    fn into_shared(self) -> Arc<ShapeData> {
        ShapeData::new(self).into_shared()
    }
}

impl IntoSharedShape for ShapeData {
    fn into_shared(self) -> Arc<ShapeData> {
        Arc::new(self)
    }
}

impl IntoSharedShape for Arc<ShapeData> {
    fn into_shared(self) -> Arc<ShapeData> {
        self
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
        let data = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)));
        let cold = data.heap_bytes();
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.heap_bytes(), cold);
        let _ = data.bounds();
        // Bounds are owned by ShapeData; they do not force a boxed product.
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.heap_bytes(), cold);
        let _ = data.distance_parts().unwrap();
        assert!(data.metric.get().is_some());
        assert!(data.heap_bytes() > cold);
    }

    #[test]
    fn nested_metric_product_initializes_once_and_reuses_storage() {
        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)));
        plan.metric();
        assert!(plan.metric.get().expect("sidecar").distance.get().is_none());
        let first = std::ptr::from_ref(plan.distance_parts().expect("initialize distance parts"));
        let second = std::ptr::from_ref(crate::test_support::assert_no_allocations(|| {
            plan.distance_parts().expect("cached distance parts")
        }));
        assert_eq!(first, second);
    }
}

#[cfg(test)]
mod frame_cache_tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::geometry::{CoordSeq, LineSeq};
    use crate::test_support::allocation_count;

    #[test]
    fn frame_cache_key_includes_crs_ellipsoid_and_runtime_generation() {
        let base =
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 4);
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4267", 6_378_137.0, 1.0 / 298.257_223_563, 4,)
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_136.0, 1.0 / 298.257_223_563, 4,)
        );
        assert_ne!(
            base,
            GeodesicPartsKey::new_at_generation("EPSG:4326", 6_378_137.0, 1.0 / 298.257_223_563, 5,)
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
        drop(cache.merge(first.clone(), |_| {}));
        assert!(cache.get(&first).is_some());
        drop(cache.merge(next_generation.clone(), |_| {}));
        assert!(cache.get(&first).is_none());
        assert!(cache.get(&next_generation).is_some());
        drop(cache.merge(other_frame.clone(), |_| {}));
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
        ));
        let cache = FrameDependentCaches::default();
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(
            &geodesic,
            0,
            GeographicFrame::crs84(),
        );
        let first = shape
            .chart_line_index(&cache, "TEST", geodesic.a, geodesic.f, 0, &metric)
            .expect("line index");
        let second = shape
            .chart_line_index(&cache, "TEST", geodesic.a, geodesic.f, 0, &metric)
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
        ));
        let first = std::ptr::from_ref(shape.measured_line_index().expect("measured line index"));
        let second = std::ptr::from_ref(
            shape
                .measured_line_index()
                .expect("cached measured line index"),
        );
        assert_eq!(first, second);
        assert!(shape.heap_bytes() > 0);
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
        ));
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
        ));
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
        ));
        shape.measured_line_index().expect("measured line index");

        let located = shape
            .line_locate_point_m_with(Point::new_raw_xy(minimum, 1.0), &PlanarMetric)
            .expect("exact subnormal measured locate");
        assert_eq!(located.to_bits(), minimum.to_bits());
    }

    #[test]
    fn exact_measured_fraction_is_allocation_free() {
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
        ));
        shape.measured_line_index().expect("measured line index");

        let (_, allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(0.0, &PlanarMetric)
                .expect("exact measured interpolation")
        });
        // Planar exact measured interpolation is intentionally allocation-free;
        // lock zero so a future heap spill fails rather than weakening coverage.
        assert_eq!(allocations, 0);
    }

    #[test]
    fn frame_entry_heap_bytes_survive_poison() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        let cache = FrameDependentCaches::default();
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(
            &geodesic,
            0,
            GeographicFrame::crs84(),
        );
        shape
            .chart_line_index(&cache, "TEST", geodesic.a, geodesic.f, 0, &metric)
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
    fn prepared_geographic_exact_topology_heap_bytes_survive_poison() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let point = Shape::Point(Point::new_raw_xy(1.0, 2.0));
        let plan = PreparedPlan::new(point);
        let view = crate::geographic::ScalarGeographicView::try_new(frame, plan.shape())
            .expect("fixture admits");
        let topology = plan.topology();
        let products = topology.geographic_products(frame);
        let before = topology.heap_bytes();
        let exact = plan
            .geographic_exact_topology(view)
            .expect("fixture builds exact geographic topology");
        let expected = HeapSize::heap_bytes(&exact);
        assert!(expected > 0);
        assert_eq!(topology.heap_bytes(), before.saturating_add(expected));

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = topology.geographic.lock().unwrap();
            panic!("poison the exact geographic topology mutex");
        }));
        assert!(result.is_err());

        assert_eq!(topology.heap_bytes(), before.saturating_add(expected));
        assert!(Arc::ptr_eq(&products, &topology.geographic_products(frame)));
    }

    #[test]
    fn single_entry_container_adds_no_heap_allocation() {
        let cache = FrameDependentCaches::default();
        let key = GeodesicPartsKey::new_at_generation("TEST", 1.0, 0.0, 0);
        let key_heap = key.heap_bytes();
        drop(cache.merge(key, |_| {}));
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
                        );
                        drop(cache.merge(key, |_| {}));
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

/// Operation requested from the geodesic point-to-segment owner.
#[derive(Clone, Copy, Debug)]
pub(crate) enum GeodesicSegmentQuery {
    /// Return an attained minimum only when it is strictly below `cutoff`.
    Minimum {
        cutoff: f64,
        witness: GeodesicWitnessNeed,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum GeodesicWitnessNeed {
    #[cfg(test)]
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
}

/// Numerical budgets shared by the geodesic minimum and Hausdorff resolvers.
/// Metric searches use metres; dimensionless supports use relative budgets.
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

/// Ellipsoidal primitives for point-to-point distance and operations that still
/// use geodesic segments, including continuous Hausdorff.
/// Chart-edge operations use [`ChartMetric`] and the same ellipsoid adapter.
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
    fn point_segment_query(
        &self,
        point: Point,
        segment: &GeodesicSegment,
        query: GeodesicSegmentQuery,
        scratch: &mut GeodesicSegmentScratch,
    ) -> Result<GeodesicSegmentQueryResult>;

    /// Geodesic length (meters) of segment `a`–`b`.
    fn segment_length(&self, a: Point, b: Point) -> f64;

    /// The point a `fraction` of the geodesic length along `a`→`b`, with Z and
    /// M interpolated linearly by `fraction`. Retained for geodesic-segment
    /// kernel oracles; geographic LRS uses chart-edge interpolation.
    #[cfg(test)]
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

/// Metric operations on the straight edges of an admitted coordinate chart.
///
/// The edge representation belongs to the metric provider; geometry code
/// remains independent of the CRS and ellipsoid implementation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartBox {
    /// Canonical Greenwich degrees. `west > east` denotes a seam-crossing
    /// interval; `[-180, 180]` denotes the full longitude range.
    pub(crate) west: f64,
    pub(crate) south: f64,
    pub(crate) east: f64,
    pub(crate) north: f64,
}
crate::heapless!(ChartBox);

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointEdgeQuery {
    Minimum { cutoff: f64 },
    Locate { cutoff: f64 },
    Dwithin { limit: f64 },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PointEdgeMinimum {
    pub(crate) distance: f64,
    pub(crate) parameter: f64,
    pub(crate) lower_bound: f64,
    pub(crate) evaluations: usize,
    pub(crate) budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointEdgeQueryResult {
    Minimum(PointEdgeMinimum),
    Excluded,
    Within {
        evaluations: usize,
    },
    Outside {
        lower_bound: f64,
        evaluations: usize,
        budget_exhausted: bool,
    },
}

pub(crate) trait ChartMetric {
    type PointEdgeScratch: Default;

    fn chart_frame(&self) -> GeographicFrame;
    fn chart_edge(&self, start: Point, end: Point) -> Result<ChartEdge>;
    fn chart_edge_length(&self, edge: &ChartEdge) -> Result<f64>;
    fn chart_edge_length_to(&self, edge: &ChartEdge, parameter: f64) -> Result<f64>;
    fn chart_edge_locate_t(&self, edge: &ChartEdge, distance: f64, length: f64) -> Result<f64>;
    fn chart_edge_bounds(&self, edge: &ChartEdge) -> ChartBox;
    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
    ) -> Result<Point>;
    fn chart_edge_witness_with_length(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
        length: f64,
    ) -> Result<Point>;
    fn point_edge_query(
        &self,
        point: Point,
        edge: &ChartEdge,
        envelope_lower: f64,
        query: PointEdgeQuery,
        scratch: &mut Self::PointEdgeScratch,
    ) -> Result<PointEdgeQueryResult>;
    fn point_envelope_bound(&self, point: Point) -> impl Fn(ChartBox) -> f64;
    fn chart_edge_seam_span(&self, edge: &ChartEdge) -> Option<[f64; 2]>;
    fn chart_seam_span_length(&self, south: f64, north: f64) -> Result<f64>;
    fn chart_area_edge_term(&self, edge: &ChartEdge, north: bool) -> f64;
    fn chart_area_factor(&self) -> f64;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexedSegment {
    pub segment: Segment,
    pub line: usize,
    pub index: usize,
    pub count: usize,
    pub closed: bool,
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
