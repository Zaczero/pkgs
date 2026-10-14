//! Z/M carry for source-less operation output and exact source-bound geographic
//! emission.

use std::borrow::Borrow;
use std::cell::Cell;

use crate::collections::HashMap;
use crate::error::Result;
use crate::geographic::retained_exact::edges::retained_edge_from_lifted_binary64_pair;
use crate::geographic::{ComputedSampleOrdinates, RetainedChartDisplacement};
use crate::geometry::segments::projection::{
    point_segment_distance_and_projection, segment_projection,
};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, ON_SEGMENT_EPSILON,
    OverlayOp, Point, PointKey, RunIdentity, Segment, SegmentProjection, Shape, TopologyDomain,
    VertexKey, XY, ZOrdinate, axis_pow2_scale, interpolate_optional_ordinate,
    interpolate_subdivision_ordinate, overlay, point_distance, point_on_segment, same_point,
    scaled_residual,
};
use crate::work::{Unmetered, Work};

mod trace;
pub(crate) use trace::{
    RequestRowId, RequestRowInput, RootCatalog, RootId, RootRange, RootRequest, TraceRows,
    TraceRowsBuilder, resolve_request_rows, resolve_source_root,
};
#[cfg(test)]
pub(crate) use trace::{
    reset_root_range_mapping_counts, root_range_mapping_counts, root_request_row_sorts,
    root_request_table_builds,
};

pub(crate) type SourceArcMetricAccess<'access, 'metric, M = crate::crs::GeodesicMetric> =
    dyn Fn() -> crate::error::Result<&'metric M> + 'access;

/// Source domain and lazy metric authority for one operation's ordinate emission.
pub(crate) struct SourceArcContext<'access, 'metric, M: ChartMetric = crate::crs::GeodesicMetric> {
    domain: crate::geometry::TopologyDomain,
    metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
}

impl<M: ChartMetric> Copy for SourceArcContext<'_, '_, M> {}

impl<M: ChartMetric> Clone for SourceArcContext<'_, '_, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'access, 'metric, M: ChartMetric> SourceArcContext<'access, 'metric, M> {
    pub(crate) const fn new(
        domain: crate::geometry::TopologyDomain,
        metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
    ) -> Self {
        Self {
            domain,
            metric_access,
        }
    }

    pub(crate) const fn domain(&self) -> crate::geometry::TopologyDomain {
        self.domain
    }

    pub(crate) const fn metric_access(
        &self,
    ) -> Option<&'access SourceArcMetricAccess<'access, 'metric, M>> {
        self.metric_access
    }
}

/// Optional authority to resolve source-root ordinates for one operation's
/// output axes. XY output has no root-capture capability.
pub(crate) struct SourceArcCapture<'access, 'metric, M: ChartMetric = crate::crs::GeodesicMetric> {
    context: Option<SourceArcContext<'access, 'metric, M>>,
}

impl<M: ChartMetric> Copy for SourceArcCapture<'_, '_, M> {}

impl<M: ChartMetric> Clone for SourceArcCapture<'_, '_, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'access, 'metric, M: ChartMetric> SourceArcCapture<'access, 'metric, M> {
    pub(crate) const fn none() -> Self {
        Self { context: None }
    }

    pub(crate) const fn with_context(context: &SourceArcContext<'access, 'metric, M>) -> Self {
        Self {
            context: Some(*context),
        }
    }

    pub(crate) const fn for_output_axes(
        output_axes: CoordinateAxes,
        context: &SourceArcContext<'access, 'metric, M>,
    ) -> Self {
        if output_axes.has_z() || output_axes.has_m() {
            Self::with_context(context)
        } else {
            Self::none()
        }
    }

    pub(crate) const fn context(&self) -> Option<&SourceArcContext<'access, 'metric, M>> {
        self.context.as_ref()
    }
}

pub(crate) const fn source_arc_ordinates_eligible(
    axes: CoordinateAxes,
    domain: crate::geometry::TopologyDomain,
) -> bool {
    (axes.has_z() || axes.has_m()) && domain.is_geographic()
}

impl SourceArcContext<'static, 'static, crate::crs::GeodesicMetric> {
    pub(crate) const fn cartesian() -> Self {
        Self::new(crate::geometry::TopologyDomain::Cartesian, None)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RootWorkCounters {
    pub(crate) native_chain_entries: usize,
    pub(crate) nearest_source_context_calls: usize,
    pub(crate) source_context_setups: usize,
    pub(crate) source_arc_requests: usize,
    pub(crate) source_arc_resolutions: usize,
    pub(crate) root_requests: usize,
    pub(crate) root_ranges: usize,
    pub(crate) trace_rows_builders: usize,
    pub(crate) request_tables: usize,
    pub(crate) root_catalog_resolutions: usize,
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum RootWorkEvent {
    NativeChainEntry,
    NearestSourceContext,
    SourceContextSetup,
    SourceArcRequest,
    SourceArcResolution,
    RootRequest,
    RootRange,
    TraceRowsBuilder,
    RequestTable,
    RootCatalogResolution,
}

#[cfg(test)]
thread_local! {
    static ROOT_WORK_COUNTERS: Cell<RootWorkCounters> =
        const { Cell::new(RootWorkCounters {
            native_chain_entries: 0,
            nearest_source_context_calls: 0,
            source_context_setups: 0,
            source_arc_requests: 0,
            source_arc_resolutions: 0,
            root_requests: 0,
            root_ranges: 0,
            trace_rows_builders: 0,
            request_tables: 0,
            root_catalog_resolutions: 0,
        }) };
}

#[cfg(test)]
pub(crate) fn reset_root_work_counters() {
    ROOT_WORK_COUNTERS.with(|counters| counters.set(RootWorkCounters::default()));
}

#[cfg(test)]
pub(crate) fn root_work_counters() -> RootWorkCounters {
    ROOT_WORK_COUNTERS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn record_root_work(event: RootWorkEvent) {
    ROOT_WORK_COUNTERS.with(|counters| {
        let mut current = counters.get();
        match event {
            RootWorkEvent::NativeChainEntry => {
                current.native_chain_entries = current.native_chain_entries.wrapping_add(1);
            },
            RootWorkEvent::NearestSourceContext => {
                current.nearest_source_context_calls =
                    current.nearest_source_context_calls.wrapping_add(1);
            },
            RootWorkEvent::SourceContextSetup => {
                current.source_context_setups = current.source_context_setups.wrapping_add(1);
            },
            RootWorkEvent::SourceArcRequest => {
                current.source_arc_requests = current.source_arc_requests.wrapping_add(1);
            },
            RootWorkEvent::SourceArcResolution => {
                current.source_arc_resolutions = current.source_arc_resolutions.wrapping_add(1);
            },
            RootWorkEvent::RootRequest => {
                current.root_requests = current.root_requests.wrapping_add(1);
            },
            RootWorkEvent::RootRange => {
                current.root_ranges = current.root_ranges.wrapping_add(1);
            },
            RootWorkEvent::TraceRowsBuilder => {
                current.trace_rows_builders = current.trace_rows_builders.wrapping_add(1);
            },
            RootWorkEvent::RequestTable => {
                current.request_tables = current.request_tables.wrapping_add(1);
            },
            RootWorkEvent::RootCatalogResolution => {
                current.root_catalog_resolutions = current.root_catalog_resolutions.wrapping_add(1);
            },
        }
        counters.set(current);
    });
}

/// Cartesian Z/M lookup for source-preserving operations without exact source
/// parameters. Geographic producers retain and resolve their source occurrences.
pub(super) struct OrdinateSource {
    vertices: HashMap<VertexKey, Vec<Point>>,
    segments: Vec<(Point, Point)>,
    /// Cartesian output from snapped overlay kernels can differ slightly from
    /// its exact source intersection.
    tolerance: f64,
}

/// The engine-snap allowance relative to the inputs' coordinate magnitude.
const OVERLAY_SNAP_RELATIVE: f64 = 1.0 / (1_u64 << 28) as f64;

impl OrdinateSource {
    fn new<S: Borrow<Shape>>(inputs: &[S]) -> Result<Self> {
        // Shared XY owners bound dedup storage without expanding logical
        // occurrences. Non-collection shapes use their exact count.
        let capacity = inputs.iter().try_fold(0_usize, |total, input| {
            let shape = Borrow::<Shape>::borrow(input);
            shape
                .point_set_capacity_hint(RunIdentity::XY)
                .map(|count| total.wrapping_add(count))
        });
        let mut vertices = crate::collections::try_hash_map()?;
        let mut segments = Vec::new();
        if let Some(capacity) = capacity {
            vertices.reserve(capacity);
            segments.reserve(capacity);
        }
        Ok(Self {
            vertices,
            segments,
            tolerance: ordinate_tolerance(inputs),
        })
    }

    fn add_vertex(&mut self, point: Point) {
        add_vertex(
            &mut self.vertices,
            point,
            crate::geometry::TopologyDomain::Cartesian,
        );
    }

    fn add_source_edge(&mut self, start: Point, end: Point) {
        self.add_vertex(start);
        self.add_vertex(end);
        if start.z().is_some() || start.m().is_some() || end.z().is_some() || end.m().is_some() {
            self.segments.push(canonical_segment(start, end));
        }
    }

    fn build_direct<S: Borrow<Shape>>(
        inputs: &[S],
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        if context.domain().is_geographic() {
            return Err(GeometryErrorKind::Invalid(
                "geographic ordinate carry requires retained source occurrences".into(),
            )
            .into());
        }
        let mut result = Self::new(inputs)?;
        for input in inputs {
            let shape = Borrow::<Shape>::borrow(input);
            match shape {
                Shape::Point(point) => result.add_vertex(*point),
                Shape::MultiPoint(points) => {
                    for point in points.points() {
                        result.add_vertex(point);
                    }
                },
                Shape::Empty(..)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_) => {},
            }
            if !(shape.has_z() || shape.has_m()) {
                continue;
            }
            shape.for_each_vertex_pair(|start, end| {
                if same_point(start, end) {
                    // A collapsed source edge still contributes both endpoint occurrences.
                    result.add_vertex(start);
                    result.add_vertex(end);
                } else {
                    result.add_source_edge(start, end);
                }
            });
        }
        Ok(result)
    }

    /// Resolve each axis independently. A missing source value is not a zero,
    /// and distinct bit patterns (including signed zero) are conflicting
    /// values rather than an arbitrary first-source winner.
    fn resolve(&self, point: Point) -> Result<(Option<f64>, Option<f64>)> {
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        let source_vertex = self.vertices.get(&VertexKey::new(
            point,
            crate::geometry::TopologyDomain::Cartesian,
        ));
        if let Some(candidates) = source_vertex {
            for &candidate in candidates {
                z.observe(candidate.z());
                m.observe(candidate.m());
            }
        }
        // An exact source occurrence owns its XY even when it is merely near
        // another ordinate-carrying segment. Do not turn that near miss into
        // a fabricated interpolation through a tolerance envelope.
        if source_vertex.is_some()
            && !self
                .segments
                .iter()
                .any(|&(start, end)| point_on_segment(point.xy(), start.xy(), end.xy()))
        {
            return Ok((z.value(), m.value()));
        }
        if self.segments.is_empty() || (z.conflict && m.conflict) {
            return Ok((z.value(), m.value()));
        }
        for &(start, end) in &self.segments {
            let pad = self.tolerance;
            let segment_pad = ON_SEGMENT_EPSILON * point_distance(start, end);
            let min_x = start.x.min(end.x) - segment_pad;
            let max_x = start.x.max(end.x) + segment_pad;
            let min_y = start.y.min(end.y) - segment_pad;
            let max_y = start.y.max(end.y) + segment_pad;
            let resolved = if point.x < min_x - pad
                || point.x > max_x + pad
                || point.y < min_y - pad
                || point.y > max_y + pad
            {
                None
            } else {
                segment_ordinate_at(start, end, point, self.tolerance)?
            };
            if let Some((source_z, source_m)) = resolved {
                if !same_point(start, point) && !same_point(end, point) {
                    z.observe(source_z);
                    m.observe(source_m);
                }
            }
        }
        Ok((z.value(), m.value()))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::geometry) struct AxisValue {
    value: Option<f64>,
    conflict: bool,
}

impl AxisValue {
    pub(in crate::geometry) const fn observe(&mut self, candidate: Option<f64>) {
        let Some(candidate) = candidate else {
            return;
        };
        match self.value {
            None => self.value = Some(candidate),
            Some(value) if value.to_bits() == candidate.to_bits() => {},
            Some(_) => self.conflict = true,
        }
    }

    pub(in crate::geometry) fn value(self) -> Option<f64> {
        (!self.conflict).then_some(self.value).flatten()
    }

    pub(in crate::geometry) const fn merge(&mut self, other: Self) {
        self.conflict |= other.conflict;
        self.observe(other.value);
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct NodeOrdinates {
    pub(in crate::geometry) z: AxisValue,
    pub(in crate::geometry) m: AxisValue,
}

impl NodeOrdinates {
    pub(crate) fn z_value(self) -> Option<f64> {
        self.z.value()
    }

    pub(crate) fn m_value(self) -> Option<f64> {
        self.m.value()
    }

    pub(in crate::geometry) const fn merge(&mut self, other: Self) {
        self.z.merge(other.z);
        self.m.merge(other.m);
    }
}

/// The original root's frame and retained branch stay separate from the lazy
/// ellipsoid authority used to measure its chart edge.
pub(in crate::geometry) enum SourceArcRequest<'a> {
    NoOrdinates,
    Endpoint {
        point: Point,
        axes: CoordinateAxes,
    },
    Interior {
        source: &'a [Point; 2],
        source_frame: crate::geographic::GeographicFrame,
        longitude: RetainedChartDisplacement,
        chart_parameter: &'a ExactRational,
        axes: CoordinateAxes,
    },
}

pub(crate) type ParallelSourceArcMetricAccess<'a> =
    dyn Fn() -> crate::error::Result<&'a crate::crs::GeodesicMetric> + Sync + 'a;

/// Derived chart geometry for one original root. The owner supplies the keyed
/// slot; exact-request fractions remain beside their `(root, parameter)` rows.
#[derive(Clone, Copy, Default)]
pub(crate) struct RootMetricState {
    edge: Option<crate::crs::ChartEdge>,
    total_length: Option<f64>,
}

impl RootMetricState {
    pub(crate) const fn from_admitted_edge(
        edge: &crate::crs::ChartEdge,
        total_length: Option<f64>,
    ) -> Self {
        Self {
            edge: Some(*edge),
            total_length,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SourceArcActivity {
    pub(crate) projection_interpolations: usize,
    pub(crate) request_constructions: usize,
    pub(crate) root_resolutions: usize,
    pub(crate) root_request_tables: usize,
    pub(crate) trace_row_tables: usize,
}

#[cfg(test)]
thread_local! {
    static SOURCE_ARC_ACTIVITY: Cell<SourceArcActivity> =
        const { Cell::new(SourceArcActivity {
            projection_interpolations: 0,
            request_constructions: 0,
            root_resolutions: 0,
            root_request_tables: 0,
            trace_row_tables: 0,
        }) };
}

#[cfg(test)]
fn update_source_arc_activity(update: impl FnOnce(&mut SourceArcActivity)) {
    SOURCE_ARC_ACTIVITY.with(|activity| {
        let mut current = activity.get();
        update(&mut current);
        activity.set(current);
    });
}

#[cfg(test)]
pub(crate) fn take_source_arc_activity() -> SourceArcActivity {
    SOURCE_ARC_ACTIVITY.with(|activity| activity.replace(SourceArcActivity::default()))
}

#[cfg(test)]
pub(crate) fn record_source_arc_projection() {
    update_source_arc_activity(|activity| {
        activity.projection_interpolations = activity.projection_interpolations.wrapping_add(1);
    });
}

#[cfg(test)]
pub(crate) fn record_source_arc_request() {
    update_source_arc_activity(|activity| {
        activity.request_constructions = activity.request_constructions.wrapping_add(1);
    });
}

#[cfg(test)]
pub(crate) fn record_source_root_resolution() {
    update_source_arc_activity(|activity| {
        activity.root_resolutions = activity.root_resolutions.wrapping_add(1);
    });
}

#[cfg(test)]
pub(crate) fn record_source_root_request_table() {
    update_source_arc_activity(|activity| {
        activity.root_request_tables = activity.root_request_tables.wrapping_add(1);
    });
}

#[cfg(test)]
pub(crate) fn record_source_trace_row_table() {
    update_source_arc_activity(|activity| {
        activity.trace_row_tables = activity.trace_row_tables.wrapping_add(1);
    });
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceArcObservation {
    pub(crate) source: [Point; 2],
    pub(crate) parameter: f64,
    pub(crate) z: Option<f64>,
    pub(crate) m: Option<f64>,
}

#[cfg(test)]
thread_local! {
    static SOURCE_ARC_OBSERVATIONS: std::cell::RefCell<Vec<SourceArcObservation>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_source_arc_observations() -> Vec<SourceArcObservation> {
    SOURCE_ARC_OBSERVATIONS.with(|observations| std::mem::take(&mut *observations.borrow_mut()))
}

impl<'a> SourceArcRequest<'a> {
    /// The frame is the original root's frame, not the resolved metric's frame.
    pub(in crate::geometry) fn new(
        source_frame: crate::geographic::GeographicFrame,
        source: &'a [Point; 2],
        axes: CoordinateAxes,
        chart_parameter: &'a ExactRational,
        longitude: impl FnOnce() -> crate::error::Result<RetainedChartDisplacement>,
    ) -> crate::error::Result<Self> {
        #[cfg(test)]
        record_root_work(RootWorkEvent::SourceArcRequest);
        #[cfg(test)]
        record_source_arc_request();
        if !axes.has_z() && !axes.has_m() {
            return Ok(Self::NoOrdinates);
        }
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        if chart_parameter.try_cmp(&zero)?.is_lt() || chart_parameter.try_cmp(&one)?.is_gt() {
            return Err(GeometryErrorKind::Invalid(
                "source chart parameter is outside the retained edge".into(),
            )
            .into());
        }
        if chart_parameter.is_zero() {
            return Ok(Self::Endpoint {
                point: source[0],
                axes,
            });
        }
        if chart_parameter.is_one() {
            return Ok(Self::Endpoint {
                point: source[1],
                axes,
            });
        }
        let eligible_axes = CoordinateAxes::new(
            HasZ(axes.has_z() && source[0].z().is_some() && source[1].z().is_some()),
            HasM(axes.has_m() && source[0].m().is_some() && source[1].m().is_some()),
        );
        if !eligible_axes.has_z() && !eligible_axes.has_m() {
            return Ok(Self::NoOrdinates);
        }
        Ok(Self::Interior {
            source,
            source_frame,
            longitude: longitude()?,
            chart_parameter,
            axes: eligible_axes,
        })
    }
}

/// Resolve one source vertex's available axes from partial/total chart-edge
/// arclength. The caller owns a root-keyed state and an exact-request fraction
/// slot. Eligible interior requests resolve metric authority before rounding or
/// consulting a cached fraction, and use the same `Work` account for both
/// rounding and chart-edge measurement.
pub(crate) fn source_arc_ordinates<'m, M: ChartMetric + 'm>(
    request: &SourceArcRequest<'_>,
    metric: impl FnOnce() -> crate::error::Result<&'m M>,
    state: &mut RootMetricState,
    fraction: &mut Option<f64>,
    work: &mut impl Work,
) -> crate::error::Result<NodeOrdinates> {
    #[cfg(test)]
    record_root_work(RootWorkEvent::SourceArcResolution);
    match request {
        SourceArcRequest::NoOrdinates => Ok(NodeOrdinates::default()),
        SourceArcRequest::Endpoint { point, axes } => Ok(node_ordinates_at(*point, *axes)),
        SourceArcRequest::Interior {
            source,
            source_frame,
            longitude,
            chart_parameter,
            axes,
        } => {
            let metric = metric()?;
            let [start, end] = **source;
            let value = if let Some(fraction) = *fraction {
                fraction
            } else {
                let parameter = chart_parameter.round_with_work(work)?;
                let edge = if let Some(edge) = state.edge {
                    edge
                } else {
                    work.charge_into_error(1)?;
                    let edge = crate::crs::ChartEdge::from_retained(
                        start,
                        end,
                        *source_frame,
                        *longitude,
                    )?;
                    state.edge = Some(edge);
                    edge
                };
                if parameter > 0.0 && parameter < 1.0 && state.total_length.is_none() {
                    state.total_length = Some(metric.chart_edge_length_with_work(&edge, work)?);
                }
                let value = metric.chart_edge_arclength_fraction_with_work(
                    &edge,
                    parameter,
                    state.total_length,
                    work,
                )?;
                *fraction = Some(value);
                value
            };
            let (z, m) = source_ordinate_values_at_fraction(start, end, *axes, value);
            #[cfg(test)]
            SOURCE_ARC_OBSERVATIONS.with(|observations| {
                observations.borrow_mut().push(SourceArcObservation {
                    source: [start, end],
                    parameter: chart_parameter
                        .round()
                        .expect("source request parameters stay in the unit interval"),
                    z: z.0,
                    m: m.0,
                });
            });
            let mut ordinates = NodeOrdinates::default();
            ordinates.z.observe(z.0);
            ordinates.m.observe(m.0);
            Ok(ordinates)
        },
    }
}

/// Carry ordinates at a publisher sample on one retained Native chart edge.
///
/// The publisher owns X and supplies the original edge parameter. Y stays
/// linear in that chart parameter; eligible Z/M follow the edge's ellipsoidal
/// arclength. `RootMetricState` is scoped by the caller to this edge root;
/// this one-shot sample adapter owns one local fraction slot.
pub(crate) fn computed_native_sample_ordinates<'m, M: ChartMetric + 'm>(
    source_frame: crate::geographic::GeographicFrame,
    source: &[Point; 2],
    longitude: RetainedChartDisplacement,
    parameter: f64,
    axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, 'm, M>>,
    state: &mut RootMetricState,
    work: &mut impl Work,
) -> crate::error::Result<ComputedSampleOrdinates> {
    let chart_parameter = ExactRational::from_f64(parameter);
    let mut fraction = None;
    let ordinates = resolve_source_root(
        source_frame,
        source,
        longitude,
        &chart_parameter,
        axes,
        metric_access,
        state,
        &mut fraction,
        work,
    )?;
    let [start, end] = *source;
    Ok(ComputedSampleOrdinates {
        y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
        z: ZOrdinate(ordinates.z.value()),
        m: MOrdinate(ordinates.m.value()),
    })
}

/// Standalone ordinary resolution owns one fresh root/request slot and delegates
/// to the same numerical path with `Unmetered`.
#[cfg(test)]
pub(in crate::geometry) fn source_arc_ordinates_unmetered<'m, M: ChartMetric + 'm>(
    request: &SourceArcRequest<'_>,
    metric: impl FnOnce() -> crate::error::Result<&'m M>,
) -> crate::error::Result<NodeOrdinates> {
    let mut state = RootMetricState::default();
    let mut fraction = None;
    let mut work = Unmetered;
    source_arc_ordinates(request, metric, &mut state, &mut fraction, &mut work)
}

/// Interpolate the producer's XY projection and resolve eligible geographic
/// ordinates on the original directed source edge at its exact parameter.
pub(crate) fn interpolate_source_projection<M: ChartMetric>(
    context: &SourceArcContext<'_, '_, M>,
    start: Point,
    end: Point,
    projection: &SegmentProjection,
    root_state: &mut Option<RootMetricState>,
) -> crate::error::Result<Point> {
    #[cfg(test)]
    record_source_arc_projection();

    let point = projection.interpolate_point(start, end)?;
    let axes = CoordinateAxes::new(
        HasZ(start.z().is_some() && end.z().is_some()),
        HasM(start.m().is_some() && end.m().is_some()),
    );
    if !axes.has_z() && !axes.has_m() {
        return Ok(point);
    }
    let TopologyDomain::Geographic(frame) = context.domain() else {
        return Ok(point);
    };
    let source = [start, end];
    let parameter = projection.exact_fraction()?;
    let longitude = retained_edge_from_lifted_binary64_pair(frame.longitude(), [start.x, end.x])?;
    let mut fraction = None;
    let mut work = Unmetered;
    let state = root_state.get_or_insert_with(RootMetricState::default);
    let ordinates = resolve_source_root(
        frame,
        &source,
        longitude,
        &parameter,
        axes,
        context.metric_access(),
        state,
        &mut fraction,
        &mut work,
    )?;
    Point::new_axes(
        point.x,
        point.y,
        ZOrdinate(ordinates.z.value().or_else(|| point.z())),
        MOrdinate(ordinates.m.value().or_else(|| point.m())),
    )
}

fn node_ordinates_at(point: Point, axes: CoordinateAxes) -> NodeOrdinates {
    let mut ordinates = NodeOrdinates::default();
    if axes.has_z() {
        ordinates.z.observe(point.z());
    }
    if axes.has_m() {
        ordinates.m.observe(point.m());
    }
    ordinates
}

pub(crate) fn source_ordinate_values_at_fraction(
    start: Point,
    end: Point,
    axes: CoordinateAxes,
    fraction: f64,
) -> (ZOrdinate, MOrdinate) {
    let z = if axes.has_z() {
        interpolate_optional_ordinate(start.z(), end.z(), fraction)
    } else {
        None
    };
    let m = if axes.has_m() {
        interpolate_optional_ordinate(start.m(), end.m(), fraction)
    } else {
        None
    };
    (ZOrdinate(z), MOrdinate(m))
}

fn add_vertex(
    vertices: &mut HashMap<VertexKey, Vec<Point>>,
    point: Point,
    domain: crate::geometry::TopologyDomain,
) {
    let key = VertexKey::new(point, domain);
    let values = vertices.entry(key).or_default();
    values.push(point);
}

fn canonical_segment(start: Point, end: Point) -> (Point, Point) {
    if PointKey::new(start) <= PointKey::new(end) {
        (start, end)
    } else {
        (end, start)
    }
}

/// Interpolate Z/M for `point` if it lies on the segment `start`..`end`,
/// restoring Z/M relative to the segment length.
pub(super) fn segment_ordinate_at(
    start: Point,
    end: Point,
    point: Point,
    tolerance: f64,
) -> crate::error::Result<Option<(Option<f64>, Option<f64>)>> {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3.
    let squared_length = dx * dx + dy * dy;
    if squared_length == 0.0 {
        if dx == 0.0 && dy == 0.0 {
            return Ok(same_point(start, point).then_some((start.z(), start.m())));
        }
        // A live reciprocal axis can have a zero squared length. Reuse the
        // segment owner's source distance before carrying Z/M.
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let (distance, projection) = point_segment_distance_and_projection(point.xy(), segment)?;
        let length = point_distance(segment.start, segment.end);
        let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
        if distance > allowed {
            return Ok(None);
        }
        let lifted = projection.interpolate_point(start, end)?;
        return Ok(Some((lifted.z(), lifted.m())));
    }
    // The perpendicular slack combines the per-segment relative epsilon
    // with the caller's ABSOLUTE engine-snap tolerance: short segments in
    // a large extent would otherwise reject the engine's jittered copies
    // of their own vertices.
    let (dx, dy, px, py, length, tolerance) = if squared_length.is_finite() {
        let length = squared_length.sqrt();
        (
            dx,
            dy,
            point.x - start.x,
            point.y - start.y,
            length,
            tolerance,
        )
    } else {
        // The overlay engine returns ordinary finite XY vertices, but the
        // source segment can span opposite-sign extremes.  Frame the ORIGINAL
        // operands before every subtraction: `(x * s) - (origin * s)`, never
        // `(x - origin) * s`, whose first subtraction is already infinite.
        let max_abs = start
            .x
            .abs()
            .max(start.y.abs())
            .max(end.x.abs())
            .max(end.y.abs())
            .max(point.x.abs())
            .max(point.y.abs());
        let scale = axis_pow2_scale(max_abs);
        let dx = scaled_residual(end.x, start.x, scale);
        let dy = scaled_residual(end.y, start.y, scale);
        let px = scaled_residual(point.x, start.x, scale);
        let py = scaled_residual(point.y, start.y, scale);
        let length = point_distance(XY::new(0.0, 0.0), XY::new(dx, dy));
        (dx, dy, px, py, length, tolerance * scale)
    };
    if length == 0.0 || !length.is_finite() {
        return Ok(same_point(start, point).then_some((start.z(), start.m())));
    }
    let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
    if (dx * py - dy * px).abs() / length > allowed {
        return Ok(None);
    }
    let denominator = (dx * dx) + (dy * dy);
    let fraction = ((px * dx) + (py * dy)) / denominator;
    let slack = ON_SEGMENT_EPSILON.max(tolerance / length);
    if !(-slack..=1.0 + slack).contains(&fraction) {
        return Ok(None);
    }
    let projection = segment_projection(point, Segment {
        start: start.xy(),
        end: end.xy(),
    })?;
    let lifted = projection.interpolate_point(start, end)?;
    Ok(Some((lifted.z(), lifted.m())))
}

fn ordinate_tolerance<S: Borrow<Shape>>(inputs: &[S]) -> f64 {
    let mut magnitude: f64 = 0.0;
    for input in inputs {
        let shape = Borrow::<Shape>::borrow(input);
        if let Some(bounds) = shape.bounds() {
            magnitude = magnitude
                .max(bounds.minx().abs())
                .max(bounds.maxx().abs())
                .max(bounds.miny().abs())
                .max(bounds.maxy().abs());
        }
    }
    magnitude * OVERLAY_SNAP_RELATIVE
}

/// Carry Z/M from an operation's inputs onto a single XY output shape. In
/// strict internal verification, an unsourceable vertex raises; ordinary
/// constructive operations naturally degrade the result to XY.
pub(super) fn carry_ordinates(
    output: Shape,
    inputs: &[&Shape],
    operation: &str,
    strict: bool,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    // Pure-XY inputs: nothing to carry, the XY output is already final.
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(output);
    }
    let source = OrdinateSource::build_direct(inputs, context)?;
    carry_or_degrade(
        &output,
        &source,
        inputs
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes())),
        operation,
        strict,
    )
}

/// Carry ordinates for an overlay. The declared axes of an empty result follow
/// the set operation, while non-empty vertices are admitted by provenance.
pub(crate) fn carry_overlay_ordinates(
    output: Shape,
    inputs: &[&Shape],
    operation: OverlayOp,
    strict: bool,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(output);
    }
    let source = OrdinateSource::build_direct(inputs, context)?;
    let declared_axes = overlay::overlay_declared_axes(inputs, operation);
    carry_or_degrade(&output, &source, declared_axes, operation.name(), strict)
}

/// Source every vertex's Z/M from `source`; strict internal verification raises
/// when a declared axis cannot be carried. Lenient output drops only axes that
/// have a conflict or an unsourced vertex.
fn carry_or_degrade(
    output: &Shape,
    source: &OrdinateSource,
    declared_axes: CoordinateAxes,
    operation: &str,
    strict: bool,
) -> Result<Shape> {
    match output {
        Shape::Empty(kind, _) => Ok(Shape::typed_empty(*kind, declared_axes)),
        Shape::GeometryCollection(geometries) => {
            Ok(Shape::GeometryCollection(geometries.try_map_members(
                |geometry| carry_or_degrade(geometry, source, declared_axes, operation, strict),
            )?))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => {
            let point_count = crate::geometry::ShapeCountQuery::default().coordinates(output)?;
            let mut resolved = Vec::with_capacity(point_count);
            output.try_for_each_point(|point| {
                resolved.push(source.resolve(point)?);
                Ok::<_, crate::error::Error>(())
            })?;
            let mut axes = declared_axes;
            for (z, m) in &resolved {
                axes = CoordinateAxes::new(
                    HasZ(axes.has_z() && z.is_some()),
                    HasM(axes.has_m() && m.is_some()),
                );
            }
            if strict && axes != declared_axes && !output.is_empty() {
                return Err(GeometryErrorKind::ordinate_dropped(operation));
            }
            let index = Cell::new(0_usize);
            let carried = output.map_points(&|point| {
                let current = index.get();
                let (z, m) = resolved
                    .get(current)
                    .copied()
                    .expect("point mapping follows the source-resolution traversal");
                index.set(current.wrapping_add(1));
                debug_assert!(
                    !axes.has_z() || z.is_some(),
                    "retained Z is not reproducible at {point:?}; axes={axes:?}"
                );
                debug_assert!(
                    !axes.has_m() || m.is_some(),
                    "retained M is not reproducible at {point:?}; axes={axes:?}"
                );
                Ok(Point::new_raw_axes(
                    point.x,
                    point.y,
                    ZOrdinate(axes.has_z().then(|| z.expect("retained Z is sourced"))),
                    MOrdinate(axes.has_m().then(|| m.expect("retained M is sourced"))),
                ))
            })?;
            debug_assert_eq!(index.get(), resolved.len());
            Ok(carried)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::embedding_search::account::EvaluationBudget;
    use crate::geometry::{Coordinates as _, LineSeq};

    fn xy_source_line() -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(10.0, 0.0),
                ])
                .expect("XY source coordinates are valid"),
            )
            .expect("two coordinates form a line"),
        )
    }

    fn geographic_xy_context() -> SourceArcContext<'static, 'static> {
        SourceArcContext::new(
            TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
            None,
        )
    }

    #[test]
    fn xy_split_context_skips_source_arc_resolution() {
        let _ = take_source_arc_activity();
        let source = xy_source_line();
        let splitter = [Point::new_raw_xy(5.0, 0.0)];
        let result = source.split_at_points_with_context(&splitter, 0.0, &geographic_xy_context());
        let activity = take_source_arc_activity();

        assert_eq!(activity, SourceArcActivity::default());
        let pieces = result.expect("XY split does not need a geographic metric");
        assert_eq!(pieces.len(), 2);
        assert!(
            pieces
                .iter()
                .all(|piece| matches!(piece, Shape::LineString(line) if line.coord_count() == 2))
        );
    }

    #[test]
    fn xy_snap_context_skips_source_arc_resolution() {
        let _ = take_source_arc_activity();
        let source = xy_source_line();
        let reference = Shape::Point(Point::new_raw_xy(5.0, 1.0));
        let result = source.snap_with_context(&reference, 1.0, &geographic_xy_context());
        let activity = take_source_arc_activity();

        assert_eq!(activity, SourceArcActivity::default());
        let Shape::LineString(snapped) = result.expect("XY snap does not need a geographic metric")
        else {
            panic!("snapping a line preserves its geometry kind");
        };
        let points = snapped.iter_coords().collect::<Vec<_>>();
        assert_eq!(points.len(), 3);
        assert_eq!(points[1].xy(), XY::new(5.0, 1.0));
    }

    #[derive(Default)]
    struct CountingWork {
        ticks: u64,
    }

    impl crate::work::Work for CountingWork {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            self.ticks = self
                .ticks
                .checked_add(ticks)
                .expect("test work count fits in u64");
            Ok(())
        }

        fn charge_product(
            &mut self,
            count: usize,
            ticks_per_item: usize,
        ) -> crate::error::Result<()> {
            self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
        }
    }

    fn retained_source_longitude(
        frame: crate::geographic::GeographicFrame,
        source: &[Point; 2],
    ) -> RetainedChartDisplacement {
        frame
            .longitude()
            .chart_connection(source[0].x, source[1].x)
            .expect("source test endpoints are admitted")
            .retained()
    }

    #[test]
    fn reciprocal_axis_segment_interpolates_live_ordinate() {
        let large = 1e162;
        let tiny = 1e-162;
        let start =
            Point::new_axes(large, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(4.0))).unwrap();
        let end = Point::new_axes(large, tiny, ZOrdinate(Some(6.0)), MOrdinate(Some(8.0))).unwrap();
        let midpoint = Point::new_raw_xy(large, tiny / 2.0);
        assert_eq!(
            segment_ordinate_at(start, end, midpoint, 0.0).unwrap(),
            Some((Some(4.0), Some(6.0)))
        );
    }

    #[test]
    fn assigned_xy_uses_the_original_arc_request_on_an_actual_non_wgs84_ellipsoid() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            assert_eq!(metric.ellipsoid_parameters(), (7_000_000.0, 1.0 / 150.0));
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let range_start = ExactRational::from_f64(0.125);
            let range_end = ExactRational::from_f64(0.375);
            let local_parameter = ExactRational::from_f64(0.5);
            let range_delta = range_end.try_subtract(&range_start)?;
            let parameter = range_start.try_add(&range_delta.try_product(&local_parameter)?)?;
            assert_eq!(parameter, ExactRational::from_f64(0.25));
            let mut longitude_accesses = 0;
            let request =
                SourceArcRequest::new(frame, &source, CoordinateAxes::XYZM, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let mut metric_accesses = 0;
            let ordinates = source_arc_ordinates_unmetered(&request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 1);
            assert_eq!(metric_accesses, 1);
            assert!((ordinates.z.value().unwrap() - 16.963_739_317_749_453).abs() <= 2.0e-13);
            assert!((ordinates.m.value().unwrap() - 250.456_089_766_241_8).abs() <= 2.0e-12);
            let assigned = Point::new_axes(
                12.25,
                -4.75,
                ZOrdinate(ordinates.z.value()),
                MOrdinate(ordinates.m.value()),
            )?;
            assert_eq!((assigned.x, assigned.y), (12.25, -4.75));
            assert!((assigned.m().unwrap() - 250.456_089_766_241_8).abs() <= 2.0e-12);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn source_arc_uses_original_root_frame_not_metric_chart_frame() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let runtime = crate::crs::runtime_state_snapshot().unwrap();
        let grad =
            crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 200.0)
                .unwrap();
        let source_frame = crate::geographic::GeographicFrame::try_new_with_orientation(
            grad,
            grad,
            0.17,
            crate::geographic::GeographicAxisOrientation::from_axis_directions(false, false),
        )
        .unwrap();

        crate::crs::with_ellipsoid_metric_at(CRS, &[], &runtime, |metric| {
            let metric_frame = metric.chart_frame();
            assert_ne!(source_frame, metric_frame);
            let source = [
                Point::new_axes(-30.0, -10.0, ZOrdinate(Some(3.0)), MOrdinate(Some(17.0)))?,
                Point::new_axes(50.0, -80.0, ZOrdinate(Some(49.0)), MOrdinate(Some(881.0)))?,
            ];
            let longitude = retained_source_longitude(source_frame, &source);
            let parameter = ExactRational::from_f64(0.375);
            let request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &parameter,
                || Ok(longitude),
            )?;
            let source_edge = crate::crs::ChartEdge::from_retained(
                source[0],
                source[1],
                source_frame,
                longitude,
            )?;
            let expected_fraction =
                metric.chart_edge_arclength_fraction(&source_edge, 0.375, None)?;

            let metric_longitude = retained_source_longitude(metric_frame, &source);
            let metric_frame_edge = crate::crs::ChartEdge::from_retained(
                source[0],
                source[1],
                metric_frame,
                metric_longitude,
            )?;
            let metric_frame_fraction =
                metric.chart_edge_arclength_fraction(&metric_frame_edge, 0.375, None)?;
            assert_ne!(expected_fraction.to_bits(), metric_frame_fraction.to_bits());

            let mut root = RootMetricState::default();
            let mut fraction = None;
            let mut work = Unmetered;
            let ordinates =
                source_arc_ordinates(&request, || Ok(metric), &mut root, &mut fraction, &mut work)?;
            let expected = source_ordinate_values_at_fraction(
                source[0],
                source[1],
                CoordinateAxes::XYZM,
                expected_fraction,
            );
            assert_eq!(
                ordinates.z.value().map(f64::to_bits),
                expected.0.0.map(f64::to_bits)
            );
            assert_eq!(
                ordinates.m.value().map(f64::to_bits),
                expected.1.0.map(f64::to_bits)
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn source_endpoint_records_do_not_acquire_an_interior_metric() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(Some(701.0)))?,
            ];
            let parameter = ExactRational::zero();
            let mut longitude_accesses = 0;
            let request =
                SourceArcRequest::new(frame, &source, CoordinateAxes::XYZM, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let mut metric_accesses = 0;
            let ordinates = source_arc_ordinates_unmetered(&request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 0);
            assert_eq!(metric_accesses, 0);
            assert_eq!(ordinates.z.value().unwrap().to_bits(), (-0.0_f64).to_bits());
            assert_eq!(ordinates.m.value().unwrap().to_bits(), 101.0_f64.to_bits());

            let end_parameter = ExactRational::one();
            let end_request = SourceArcRequest::new(
                frame,
                &source,
                CoordinateAxes::XYZM,
                &end_parameter,
                || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                },
            )?;
            let end_ordinates = source_arc_ordinates_unmetered(&end_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            assert_eq!(metric_accesses, 0);
            assert_eq!(longitude_accesses, 0);
            assert_eq!(end_ordinates.z.value(), None);
            assert_eq!(end_ordinates.m.value().unwrap(), 701.0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn computed_native_sample_endpoint_and_xy_requests_skip_the_metric() {
        let frame = crate::geographic::GeographicFrame::crs84();
        let source = [
            Point::new_axes(0.0, -30.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(101.0))).unwrap(),
            Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0))).unwrap(),
        ];
        let longitude = retained_source_longitude(frame, &source);
        let endpoint_metric_accesses = Cell::new(0_usize);
        let endpoint_metric = || -> crate::error::Result<&crate::crs::GeodesicMetric> {
            endpoint_metric_accesses.set(endpoint_metric_accesses.get().wrapping_add(1));
            Err(crate::crs::CrsError::invalid(
                "unexpected computed-edge metric access",
            ))
        };
        let mut state = RootMetricState::default();
        let mut work = Unmetered;

        let endpoint = computed_native_sample_ordinates(
            frame,
            &source,
            longitude,
            0.0,
            CoordinateAxes::XYZM,
            Some(&endpoint_metric),
            &mut state,
            &mut work,
        )
        .unwrap();
        assert_eq!(endpoint.y, -30.0);
        assert_eq!(endpoint.z.0.unwrap().to_bits(), (-0.0_f64).to_bits());
        assert_eq!(endpoint.m.0, Some(101.0));
        assert_eq!(endpoint_metric_accesses.get(), 0);
        assert!(state.edge.is_none());

        let xy_metric_accesses = Cell::new(0_usize);
        let xy_metric = || -> crate::error::Result<&crate::crs::GeodesicMetric> {
            xy_metric_accesses.set(xy_metric_accesses.get().wrapping_add(1));
            Err(crate::crs::CrsError::invalid(
                "unexpected computed-edge metric access",
            ))
        };
        let xy = computed_native_sample_ordinates(
            frame,
            &source,
            longitude,
            0.25,
            CoordinateAxes::XY,
            Some(&xy_metric),
            &mut state,
            &mut work,
        )
        .unwrap();
        assert_eq!(xy.y, -5.0);
        assert_eq!(xy.z.0, None);
        assert_eq!(xy.m.0, None);
        assert_eq!(xy_metric_accesses.get(), 0);
        assert!(state.edge.is_none());
    }

    #[test]
    fn computed_native_publisher_sample_uses_independent_wgs84_chart_arclength() {
        let frame = crate::geographic::GeographicFrame::crs84();
        let source = [
            Point::new_axes(0.0, 10.0, ZOrdinate(Some(10.0)), MOrdinate(Some(100.0))).unwrap(),
            Point::new_axes(120.0, 70.0, ZOrdinate(Some(210.0)), MOrdinate(Some(500.0))).unwrap(),
        ];
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
        let metric_access = || Ok(&metric);
        let longitude = retained_source_longitude(frame, &source);
        let mut state = RootMetricState::default();
        let mut work = Unmetered;

        let sample = computed_native_sample_ordinates(
            frame,
            &source,
            longitude,
            0.5,
            CoordinateAxes::XYZM,
            Some(&metric_access),
            &mut state,
            &mut work,
        )
        .unwrap();

        assert_eq!(sample.y, 40.0);
        assert!((sample.z.0.unwrap() - 124.792_495_251_506_88).abs() <= 2.0e-12);
        assert!((sample.m.0.unwrap() - 329.584_990_503_013_7).abs() <= 4.0e-12);
    }

    #[test]
    fn exact_interior_parameters_that_round_to_endpoints_skip_full_length() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let tiny = ExactRational::from_f64(f64::from_bits(1))
                .try_product(&ExactRational::from_f64(0.25))?;
            let near_one = ExactRational::one().try_subtract(&tiny)?;
            let cases = [(tiny, source[0], 0.0_f64), (near_one, source[1], 1.0_f64)];

            for (parameter, endpoint, rounded) in cases {
                let request = SourceArcRequest::new(
                    frame,
                    &source,
                    CoordinateAxes::XYZM,
                    &parameter,
                    || Ok(retained_source_longitude(frame, &source)),
                )?;
                let mut metric_accesses = 0;
                let mut metric_state = RootMetricState::default();
                let mut fraction = None;
                let mut work = Unmetered;
                let ordinates = source_arc_ordinates(
                    &request,
                    || {
                        metric_accesses += 1;
                        Ok(metric)
                    },
                    &mut metric_state,
                    &mut fraction,
                    &mut work,
                )?;

                assert_eq!(metric_accesses, 1);
                assert_eq!(fraction.map(f64::to_bits), Some(rounded.to_bits()));
                assert!(metric_state.edge.is_some());
                assert!(metric_state.total_length.is_none());
                assert_eq!(
                    ordinates.z.value().map(f64::to_bits),
                    endpoint.z().map(f64::to_bits)
                );
                assert_eq!(
                    ordinates.m.value().map(f64::to_bits),
                    endpoint.m().map(f64::to_bits)
                );
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn xy_occurrence_leaves_the_exact_request_fraction_for_later_eligible_axes() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let runtime = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &runtime, |metric| {
            let source_frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(85.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let longitude = retained_source_longitude(source_frame, &source);
            let parameter = ExactRational::from_f64(0.25);
            let xy_request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XY,
                &parameter,
                || Ok(longitude),
            )?;
            let eligible_request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &parameter,
                || Ok(longitude),
            )?;
            let mut root = RootMetricState::default();
            let mut fraction = None;
            let mut xy_work = Unmetered;
            let mut xy_metric_accesses = 0;
            crate::crs::reset_quadrature_test_progress();
            let xy = source_arc_ordinates(
                &xy_request,
                || {
                    xy_metric_accesses += 1;
                    Ok(metric)
                },
                &mut root,
                &mut fraction,
                &mut xy_work,
            )?;
            assert!(xy.z.value().is_none() && xy.m.value().is_none());
            assert_eq!(xy_metric_accesses, 0);
            assert!(fraction.is_none());
            assert!(root.edge.is_none() && root.total_length.is_none());
            assert_eq!(crate::crs::quadrature_test_progress().seeds, 0);

            let eligible = source_arc_ordinates(
                &eligible_request,
                || Ok(metric),
                &mut root,
                &mut fraction,
                &mut xy_work,
            )?;
            assert!(eligible.z.value().is_some() && eligible.m.value().is_some());
            assert!(fraction.is_some());
            assert!(root.edge.is_some() && root.total_length.is_some());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn exact_requests_reuse_root_total_without_collapsing_rounded_parameters() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let runtime = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &runtime, |metric| {
            let source_frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(85.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let longitude = retained_source_longitude(source_frame, &source);
            let quarter = ExactRational::from_f64(0.25);
            let epsilon = ExactRational::from_f64(f64::from_bits(0x3C30_0000_0000_0000));
            let nearby_quarter = quarter.try_add(&epsilon)?;
            assert_ne!(quarter, nearby_quarter);
            assert_eq!(quarter.round()?, nearby_quarter.round()?);
            assert_eq!(nearby_quarter.round()?, 0.25);
            let three_quarters = ExactRational::from_f64(0.75);

            let first_request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &quarter,
                || Ok(longitude),
            )?;
            let nearby_request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &nearby_quarter,
                || Ok(longitude),
            )?;
            let later_request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &three_quarters,
                || Ok(longitude),
            )?;

            let mut root = RootMetricState::default();
            let mut first_fraction = None;
            let mut first_work = Unmetered;
            let first = source_arc_ordinates(
                &first_request,
                || Ok(metric),
                &mut root,
                &mut first_fraction,
                &mut first_work,
            )?;
            let total_length = root
                .total_length
                .expect("first interior request measures total");
            assert!(root.edge.is_some());
            assert!(first_fraction.is_some());
            let root_edge_before_rejection = format!("{:?}", root.edge);

            let mut nearby_fraction = None;
            let mut rejected_work =
                EvaluationBudget::with_remaining_ticks_for_test("source_arc_test", 1, 0)?;
            let mut rejected_metric_accesses = 0;
            let rejected = source_arc_ordinates(
                &nearby_request,
                || {
                    rejected_metric_accesses += 1;
                    Ok(metric)
                },
                &mut root,
                &mut nearby_fraction,
                &mut rejected_work,
            );
            match rejected {
                Err(refusal)
                    if matches!(
                        refusal.kind(),
                        crate::error::ErrorKind::Geometry(
                            crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                                operation: "source_arc_test",
                                ..
                            }
                        )
                    ) => {},
                Err(_) => {
                    return Err(crate::crs::CrsError::invalid(
                        "distinct exact source request failed for a reason other than Work exhaustion",
                    ));
                },
                Ok(_) => {
                    return Err(crate::crs::CrsError::invalid(
                        "distinct exact source request reused another request's fraction",
                    ));
                },
            }
            assert_eq!(rejected_metric_accesses, 1);
            assert!(nearby_fraction.is_none());
            assert!(root.edge.is_some());
            assert_eq!(format!("{:?}", root.edge), root_edge_before_rejection);
            assert_eq!(
                root.total_length.map(f64::to_bits),
                Some(total_length.to_bits())
            );

            let mut rounding_work = CountingWork::default();
            let rounded_parameter = nearby_quarter.round_with_work(&mut rounding_work)?;
            assert!(rounding_work.ticks > 0);
            let edge = root.edge.expect("the first request retains its ChartEdge");
            let mut fraction_work = CountingWork::default();
            let expected_fraction = metric.chart_edge_arclength_fraction_with_work(
                &edge,
                rounded_parameter,
                Some(total_length),
                &mut fraction_work,
            )?;
            assert!(fraction_work.ticks > 0);
            let remaining_ticks = u128::from(rounding_work.ticks) + u128::from(fraction_work.ticks);
            let mut nearby_work = EvaluationBudget::with_remaining_ticks_for_test(
                "source_arc_test",
                1,
                remaining_ticks,
            )?;
            let nearby = match source_arc_ordinates(
                &nearby_request,
                || Ok(metric),
                &mut root,
                &mut nearby_fraction,
                &mut nearby_work,
            ) {
                Ok(nearby) => nearby,
                Err(error) => {
                    return Err(crate::crs::CrsError::invalid(format!(
                        "cached root total reuse exceeded fraction-only Work: {error}"
                    )));
                },
            };
            assert_eq!(
                first_fraction.map(f64::to_bits),
                nearby_fraction.map(f64::to_bits)
            );
            assert_eq!(
                nearby_fraction.map(f64::to_bits),
                Some(expected_fraction.to_bits())
            );
            assert_eq!(
                root.total_length.map(f64::to_bits),
                Some(total_length.to_bits())
            );
            assert_eq!(
                first.z.value().map(f64::to_bits),
                nearby.z.value().map(f64::to_bits)
            );
            assert_eq!(
                first.m.value().map(f64::to_bits),
                nearby.m.value().map(f64::to_bits)
            );

            let mut duplicate_work =
                EvaluationBudget::with_remaining_ticks_for_test("source_arc_test", 1, 0)?;
            let mut duplicate_metric_accesses = 0;
            let duplicate = source_arc_ordinates(
                &nearby_request,
                || {
                    duplicate_metric_accesses += 1;
                    Ok(metric)
                },
                &mut root,
                &mut nearby_fraction,
                &mut duplicate_work,
            )?;
            assert_eq!(duplicate_metric_accesses, 1);
            assert_eq!(
                duplicate.z.value().map(f64::to_bits),
                nearby.z.value().map(f64::to_bits)
            );
            assert_eq!(
                duplicate.m.value().map(f64::to_bits),
                nearby.m.value().map(f64::to_bits)
            );

            let mut shared_root_work = Unmetered;
            let mut later_fraction = None;
            let shared_root_result = source_arc_ordinates(
                &later_request,
                || Ok(metric),
                &mut root,
                &mut later_fraction,
                &mut shared_root_work,
            )?;
            let mut fresh_root = RootMetricState::default();
            let mut fresh_fraction = None;
            let mut fresh_root_work = Unmetered;
            let fresh_root_result = source_arc_ordinates(
                &later_request,
                || Ok(metric),
                &mut fresh_root,
                &mut fresh_fraction,
                &mut fresh_root_work,
            )?;
            assert_eq!(
                shared_root_result.z.value().map(f64::to_bits),
                fresh_root_result.z.value().map(f64::to_bits)
            );
            assert_eq!(
                shared_root_result.m.value().map(f64::to_bits),
                fresh_root_result.m.value().map(f64::to_bits)
            );
            println!(
                "root_metric_request_reuse={{\"rounded_parameter_bits\":\"{:016x}\",\"rounding_work_ticks\":{},\"fraction_work_ticks\":{},\"fraction_only_allowance_ticks\":{},\"root_total_bits\":\"{:016x}\",\"fraction_bits\":\"{:016x}\",\"distinct_request_zero_work\":\"refused\",\"fraction_empty_after_refusal\":true,\"root_edge_unchanged\":true,\"root_total_unchanged\":true,\"rejected_metric_accesses\":{},\"duplicate_zero_work\":\"succeeded\",\"duplicate_metric_accesses\":{}}}",
                rounded_parameter.to_bits(),
                rounding_work.ticks,
                fraction_work.ticks,
                remaining_ticks,
                total_length.to_bits(),
                expected_fraction.to_bits(),
                rejected_metric_accesses,
                duplicate_metric_accesses,
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn cached_exact_request_fraction_still_requires_metric_authority() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let runtime = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &runtime, |metric| {
            let source_frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(85.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let longitude = retained_source_longitude(source_frame, &source);
            let parameter = ExactRational::from_f64(0.25);
            let request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &parameter,
                || Ok(longitude),
            )?;
            let mut root = RootMetricState::default();
            let mut fraction = None;
            let mut initial_work = Unmetered;
            let original = source_arc_ordinates(
                &request,
                || Ok(metric),
                &mut root,
                &mut fraction,
                &mut initial_work,
            )?;
            assert!(fraction.is_some());

            let mut metric_accesses = 0;
            let mut cached_work = Unmetered;
            crate::crs::reset_quadrature_test_progress();
            let duplicate = source_arc_ordinates(
                &request,
                || {
                    metric_accesses += 1;
                    Ok(metric)
                },
                &mut root,
                &mut fraction,
                &mut cached_work,
            )?;
            assert_eq!(metric_accesses, 1);
            assert_eq!(crate::crs::quadrature_test_progress().seeds, 0);
            assert_eq!(
                duplicate.z.value().map(f64::to_bits),
                original.z.value().map(f64::to_bits)
            );
            assert_eq!(
                duplicate.m.value().map(f64::to_bits),
                original.m.value().map(f64::to_bits)
            );

            let mut rejected_work =
                EvaluationBudget::with_remaining_ticks_for_test("source_arc_test", 1, 0)?;
            let Err(unavailable) = source_arc_ordinates(
                &request,
                || {
                    metric_accesses += 1;
                    if metric_accesses == 2 {
                        Err(crate::crs::CrsError::invalid(
                            "cached source fraction still requires metric authority",
                        ))
                    } else {
                        Ok(metric)
                    }
                },
                &mut root,
                &mut fraction,
                &mut rejected_work,
            ) else {
                return Err(crate::crs::CrsError::invalid(
                    "cached fraction must not bypass metric authority",
                ));
            };
            assert!(
                unavailable
                    .to_string()
                    .contains("cached source fraction still requires metric authority")
            );
            assert_eq!(metric_accesses, 2);
            assert_eq!(crate::crs::quadrature_test_progress().seeds, 0);
            assert!(fraction.is_some());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn exact_parameter_rounding_is_paid_before_chart_edge_mutation() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let runtime = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &runtime, |metric| {
            let source_frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(85.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let longitude = retained_source_longitude(source_frame, &source);
            let parameter = ExactRational::from_f64(0.25);
            let request = SourceArcRequest::new(
                source_frame,
                &source,
                CoordinateAxes::XYZM,
                &parameter,
                || Ok(longitude),
            )?;
            let mut rounding_work = CountingWork::default();
            parameter.round_with_work(&mut rounding_work)?;
            assert!(rounding_work.ticks > 0);

            let mut budget = EvaluationBudget::with_remaining_ticks_for_test(
                "source_arc_test",
                1,
                u128::from(rounding_work.ticks),
            )?;
            let mut root = RootMetricState::default();
            let mut fraction = None;
            crate::crs::reset_quadrature_test_progress();
            let resolution = source_arc_ordinates(
                &request,
                || Ok(metric),
                &mut root,
                &mut fraction,
                &mut budget,
            );
            match resolution {
                Err(refusal)
                    if matches!(
                        refusal.kind(),
                        crate::error::ErrorKind::Geometry(
                            crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                                operation: "source_arc_test",
                                ..
                            }
                        )
                    ) => {},
                Err(_) => {
                    return Err(crate::crs::CrsError::invalid(
                        "source arclength failed for a reason other than Work exhaustion",
                    ));
                },
                Ok(_) => {
                    return Err(crate::crs::CrsError::invalid(
                        "the exact-round allowance funded later source-edge work",
                    ));
                },
            }
            if root.edge.is_some() {
                return Err(crate::crs::CrsError::invalid(
                    "the source ChartEdge was installed before its Work debit",
                ));
            }
            assert_eq!(crate::crs::quadrature_test_progress().seeds, 0);
            assert!(root.total_length.is_none());
            assert!(fraction.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn presentation_and_axis_ineligible_source_requests_do_not_acquire_a_metric() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(None))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(None))?,
            ];
            let parameter = ExactRational::from_f64(0.25);
            let mut longitude_accesses = 0;
            let unavailable_request =
                SourceArcRequest::new(frame, &source, CoordinateAxes::XYZM, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let mut metric_accesses = 0;
            let ordinate_less =
                source_arc_ordinates_unmetered(&SourceArcRequest::NoOrdinates, || {
                    metric_accesses += 1;
                    Ok(metric)
                })?;
            let unavailable = source_arc_ordinates_unmetered(&unavailable_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            let xy_request =
                SourceArcRequest::new(frame, &source, CoordinateAxes::XY, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let xy = source_arc_ordinates_unmetered(&xy_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 0);
            assert_eq!(metric_accesses, 0);
            assert_eq!(ordinate_less.z.value(), None);
            assert_eq!(ordinate_less.m.value(), None);
            assert_eq!(unavailable.z.value(), None);
            assert_eq!(unavailable.m.value(), None);
            assert_eq!(xy.z.value(), None);
            assert_eq!(xy.m.value(), None);

            let subnormal = ExactRational::from_f64(f64::from_bits(1));
            let quarter = ExactRational::from_f64(0.25);
            let near_zero = subnormal.try_product(&quarter)?;
            let near_one = ExactRational::one().try_subtract(&near_zero)?;
            assert_eq!(near_zero.round()?, 0.0);
            assert_eq!(near_one.round()?, 1.0);
            let interior_start_missing = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(None))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(None))?,
            ];
            let near_start_request = SourceArcRequest::new(
                frame,
                &interior_start_missing,
                CoordinateAxes::XYZM,
                &near_zero,
                || Ok(retained_source_longitude(frame, &interior_start_missing)),
            )?;
            let near_end_request = SourceArcRequest::new(
                frame,
                &interior_start_missing,
                CoordinateAxes::XYZM,
                &near_one,
                || Ok(retained_source_longitude(frame, &interior_start_missing)),
            )?;
            let near_start = source_arc_ordinates_unmetered(&near_start_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            let near_end = source_arc_ordinates_unmetered(&near_end_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            assert_eq!(metric_accesses, 0);
            assert_eq!(near_start.z.value(), None);
            assert_eq!(near_end.z.value(), None);
            Ok(())
        })
        .unwrap();
    }
}
