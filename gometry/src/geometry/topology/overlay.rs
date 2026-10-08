//! Exact n-ary geographic set selection and output emission over global Q.

use super::exact::{
    ExactChartPoint, ExactSourceEdge, ExactSourceRange, ExactSourceRing, SourceEdgeId,
    SourceEdgeKind, SourceRingId, SourceRowId,
};
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, QNodeId, QPortId, row_locations};
use crate::error::{ResourceError, Result};
use crate::geographic::{
    ChartPole, GeographicRing, Latitude, RetainedChartDisplacement, RingSummary, SeamIdentity,
};
use crate::geometry::ordinates::{
    NodeOrdinates, SourceArcMetricAccess, SourceArcRequest, source_arc_ordinates,
};
use crate::geometry::overlay::{OverlayOp, build_certified_overlay_shape_with_axes};
use crate::geometry::segments::try_sort_by;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates as _, ExpansionBudget,
    GeometryErrorKind, GeometryKind, HasM, HasZ, LineSeq, Loc, MOrdinate, Point, Polygon, Ring,
    RingWinding, Shape, XY, ZOrdinate, interpolate_optional_ordinate, open_point_cycle_winding,
    ring_winding,
};

#[derive(Clone, Copy)]
struct LineStep {
    span: usize,
    from_endpoint: SpanEndpoint,
    owner: usize,
}

#[derive(Clone, Copy)]
struct SelectedLineSpan {
    owner: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SpanEndpoint {
    Start,
    End,
}

impl SpanEndpoint {
    const fn opposite(self) -> Self {
        match self {
            Self::Start => Self::End,
            Self::End => Self::Start,
        }
    }
}

struct BoundaryCycle {
    darts: Vec<QDartId>,
    region: usize,
}

struct QOverlayPlan {
    whole_sphere: bool,
    polygons: PolygonPlan,
    cycles: Vec<BoundaryCycle>,
    line_paths: Vec<Vec<LineStep>>,
    point_nodes: Vec<QNodeId>,
}

enum PolygonPlan {
    WholeSphere(Vec<usize>),
    Regions {
        rings: Vec<PlannedAreaRing>,
        region_count: usize,
    },
}

#[derive(Clone, Copy)]
enum PlannedAreaRing {
    Source {
        ring: usize,
        region: usize,
        reverse: bool,
    },
    Cycle {
        cycle: usize,
        region: usize,
    },
}

struct QCoordinates {
    node: Vec<XY>,
    pole_port_longitude: Vec<Option<f64>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum EmittedNodeKey {
    Coordinate(u64, u64),
    NorthPole,
    SouthPole,
}

#[derive(Clone)]
struct EmittedVertex {
    node: QNodeId,
    port: QPortId,
    point: Point,
    chart: ExactChartPoint,
}

struct QPublicationCertificate<'a> {
    topology: &'a ExactGeographicTopology,
    live: &'a [(EmittedNodeKey, QNodeId)],
    inserted: Option<crate::collections::HashMap<EmittedNodeKey, (ExactChartPoint, QNodeId)>>,
}

impl QPublicationCertificate<'_> {
    fn record(&mut self, exact: ExactChartPoint, point: Point) -> Result<()> {
        let frame = self.topology.sources.frame;
        let key = emitted_node_key(frame, point.xy())?;
        let next = QNodeId(
            u32::try_from(
                self.topology.q.nodes.len().saturating_add(
                    self.inserted
                        .as_ref()
                        .map_or(0, std::collections::HashMap::len),
                ),
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        );
        if let Ok(index) = self.live.binary_search_by_key(&key, |&(key, _)| key) {
            let &(_, node) = self
                .live
                .get(index)
                .expect("binary search returns a live node");
            let source = self
                .topology
                .q
                .nodes
                .get(node.0 as usize)
                .expect("live node belongs to the certified Q")
                .ports
                .start;
            let port = self
                .topology
                .q
                .node_ports
                .get(source)
                .expect("every Q node has a representative port");
            let previous = &self
                .topology
                .q
                .ports
                .get(port.0 as usize)
                .expect("representative port belongs to the Q")
                .point;
            return certify_publication_identity(frame, previous, &exact, node, next);
        }
        let inserted = match &mut self.inserted {
            Some(inserted) => inserted,
            slot @ None => slot.insert(crate::collections::try_hash_map_with_capacity(0)?),
        };
        match inserted.entry(key) {
            std::collections::hash_map::Entry::Occupied(previous) => {
                let (previous, node) = previous.get();
                certify_publication_identity(frame, previous, &exact, *node, next)
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert((exact, next));
                Ok(())
            },
        }
    }
}

fn certify_publication_identity(
    frame: crate::geographic::GeographicFrame,
    previous: &ExactChartPoint,
    point: &ExactChartPoint,
    first: QNodeId,
    second: QNodeId,
) -> Result<()> {
    let same_latitude = previous.y.try_cmp(&point.y)?.is_eq();
    let same_position = if same_latitude {
        let north =
            ExactRational::from_f64(frame.chart_pole_latitude(crate::geographic::ChartPole::North));
        let south =
            ExactRational::from_f64(frame.chart_pole_latitude(crate::geographic::ChartPole::South));
        if point.y.try_cmp(&north)?.is_eq() || point.y.try_cmp(&south)?.is_eq() {
            true
        } else {
            let period = ExactRational::from_f64(frame.longitude().period());
            let delta = point.x.try_subtract(&previous.x)?;
            let turns = delta.try_quotient(&period)?.round()?;
            turns.fract() == 0.0
                && delta
                    .try_cmp(&period.try_product(&ExactRational::from_f64(turns))?)?
                    .is_eq()
        }
    } else {
        false
    };
    if same_position {
        Ok(())
    } else {
        Err(GeometryErrorKind::ExactNodeCollision {
            first: first.0,
            second: second.0,
        }
        .into())
    }
}

/// Select all dimensions from the one exact source chart and its global Q.
/// Rounding happens only after face, span, and node selection is complete.
pub(in crate::geometry) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, M>>,
) -> Result<Shape> {
    let plan = QOverlayPlan::build(topology, operation)?;
    let coordinates = QCoordinates::build(topology)?;
    let source_points = source_point_coordinates(topology)?;
    let ordinates = node_ordinates(topology, declared_axes, metric_access)?;
    let mut live_node_keys = plan.live_node_keys(topology, &coordinates, &source_points)?;
    certify_node_injectivity(&mut live_node_keys)?;
    let axes = emitted_axes(declared_axes, &live_node_keys, &ordinates)?;
    if strict && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped(operation.name()));
    }
    let mut edge_budget = ExpansionBudget::new(operation.name(), "computed edges");
    let mut certificate = QPublicationCertificate {
        topology,
        live: &live_node_keys,
        inserted: None,
    };
    let mut polygons = emit_polygons(
        topology,
        &plan,
        &ordinates,
        &coordinates,
        &live_node_keys,
        axes,
        &mut edge_budget,
        &mut certificate,
    )?;
    let lines = emit_lines(topology, &plan.line_paths, &ordinates, &coordinates)?;
    let points = emit_points(topology, &plan.point_nodes, &source_points, &coordinates)?;

    if points.is_empty() && lines.is_empty() && polygons.is_empty() {
        return build_certified_overlay_shape_with_axes(points, lines, polygons, declared_axes);
    }

    let mut points = points;
    let mut lines = lines;
    apply_axes(
        topology.sources.frame,
        &ordinates,
        &live_node_keys,
        axes,
        &mut points,
        &mut lines,
        &mut [],
    )?;
    if plan.whole_sphere {
        return match polygons.len() {
            0 => Err(GeometryErrorKind::Invalid(
                "Q selected the whole sphere without source area polygons".into(),
            )
            .into()),
            1 => {
                Ok(Shape::Polygon(polygons.pop().expect(
                    "a one-polygon whole-sphere output has its polygon",
                )))
            },
            _ => Shape::geometry_collection(polygons.into_iter().map(Shape::Polygon).collect()),
        };
    }
    build_certified_overlay_shape_with_axes(points, lines, polygons, axes)
}

pub(in crate::geometry) fn exact_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, M>>,
) -> Result<Shape> {
    if topology.sources.row_count != 1 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic boundary requires one source row".into(),
        )
        .into());
    }
    let q = &topology.q;
    let row = SourceRowId(0);
    let selected_faces = select_faces(q, 1, OverlayOp::Union)?;
    let has_area = selected_faces.iter().any(|&selected| selected);
    let (line_paths, covered_by_area_cycles) = {
        let mut regions = face_regions(q, &selected_faces)?;
        let cycles = selected_area_boundaries(q, &selected_faces, &mut regions)?;
        boundary_cycle_line_paths(topology, &cycles)?
    };
    let row_locations = row_locations(topology, row)?;
    let mut point_nodes = Vec::new();
    let mut has_point_support = false;
    for (node, &covered) in q.nodes.iter().zip(&covered_by_area_cycles) {
        let location = row_locations.node(node.id)?;
        has_point_support |= location == Loc::Interior;
        if location == Loc::Boundary && !covered {
            point_nodes.push(node.id);
        }
    }

    let mut has_curve_support = false;
    for span_index in 0..q.spans.len() {
        if row_locations.span(span_index)? == Some(Loc::Interior) {
            has_curve_support = true;
            break;
        }
    }

    let plan = QOverlayPlan {
        whole_sphere: false,
        polygons: PolygonPlan::Regions {
            rings: Vec::new(),
            region_count: q.global_face_count,
        },
        cycles: Vec::new(),
        line_paths,
        point_nodes,
    };
    let coordinates = QCoordinates::build(topology)?;
    let source_points = boundary_source_points(topology, row)?;
    let ordinates = node_ordinates(topology, declared_axes, metric_access)?;
    let mut live_node_keys = plan.live_node_keys(topology, &coordinates, &source_points)?;
    certify_node_injectivity(&mut live_node_keys)?;
    let mut lines = emit_lines(topology, &plan.line_paths, &ordinates, &coordinates)?;
    let axes = emitted_axes(declared_axes, &live_node_keys, &ordinates)?;
    let mut points = [];
    let mut polygons = [];
    apply_axes(
        topology.sources.frame,
        &ordinates,
        &live_node_keys,
        axes,
        &mut points,
        &mut lines,
        &mut polygons,
    )?;
    let point_part = emit_boundary_points(
        topology,
        &plan.point_nodes,
        &source_points,
        &coordinates,
        &ordinates,
        axes,
    )?;

    let line_part = if lines.is_empty() {
        None
    } else {
        let mut values = Vec::with_capacity(lines.len());
        for line in lines {
            values.push(LineSeq::from_proven(line));
        }
        Some(Shape::narrow_lines(axes, values))
    };

    if has_area {
        let line_part = line_part
            .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiLineString, declared_axes));
        return match point_part {
            Some(point_part) => Shape::geometry_collection(vec![line_part, point_part]),
            None => Ok(line_part),
        };
    }
    if has_curve_support {
        return Ok(point_part
            .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiPoint, declared_axes)));
    }
    if has_point_support {
        return Ok(Shape::typed_empty(
            GeometryKind::GeometryCollection,
            declared_axes,
        ));
    }
    Ok(Shape::typed_empty(
        GeometryKind::MultiLineString,
        declared_axes,
    ))
}

fn emitted_axes(
    declared: CoordinateAxes,
    live: &[(EmittedNodeKey, QNodeId)],
    ordinates: &[NodeOrdinates],
) -> Result<CoordinateAxes> {
    let mut has_z = declared.has_z();
    let mut has_m = declared.has_m();
    for &(_, node) in live {
        let ordinates = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an unknown node".into()))?;
        has_z &= ordinates.z.value().is_some();
        has_m &= ordinates.m.value().is_some();
    }
    Ok(CoordinateAxes::new(HasZ(has_z), HasM(has_m)))
}

fn boundary_cycle_line_paths(
    topology: &ExactGeographicTopology,
    cycles: &[BoundaryCycle],
) -> Result<(Vec<Vec<LineStep>>, Vec<bool>)> {
    let q = &topology.q;
    let mut paths = Vec::with_capacity(cycles.len());
    let mut covered = vec![false; q.nodes.len()];
    for cycle in cycles {
        let mut path = Vec::with_capacity(cycle.darts.len());
        for &dart_id in &cycle.darts {
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown dart".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown span".into())
            })?;
            let from_endpoint = if dart.from_node == span.nodes[0] {
                SpanEndpoint::Start
            } else if dart.from_node == span.nodes[1] {
                SpanEndpoint::End
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not start at a span endpoint".into(),
                )
                .into());
            };
            let to_node = match from_endpoint {
                SpanEndpoint::Start => span.nodes[1],
                SpanEndpoint::End => span.nodes[0],
            };
            if dart.to_node != to_node {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not end at the opposite span endpoint".into(),
                )
                .into());
            }
            *covered.get_mut(dart.from_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart starts at an unknown node".into())
            })? = true;
            *covered.get_mut(dart.to_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart ends at an unknown node".into())
            })? = true;
            path.push(LineStep {
                span: span_index,
                from_endpoint,
                owner: area_owner_index(topology, span_index)?,
            });
        }
        paths.push(path);
    }
    Ok((paths, covered))
}

impl QOverlayPlan {
    fn build(topology: &ExactGeographicTopology, operation: OverlayOp) -> Result<Self> {
        let q = &topology.q;
        let row_count = topology.sources.row_count;
        let faces = select_faces(q, row_count, operation)?;
        let whole_sphere = !faces.is_empty() && faces.iter().all(|&selected| selected);
        let mut face_regions = face_regions(q, &faces)?;
        let line_spans = select_line_spans(topology, &faces, row_count, operation)?;
        let cycles = selected_area_boundaries(q, &faces, &mut face_regions)?;
        let point_nodes = select_points(topology, &faces, &line_spans, row_count, operation)?;
        let line_paths = line_steps(topology, &line_spans)?;
        let polygons = PolygonPlan::build(topology, whole_sphere, operation, &cycles)?;
        Ok(Self {
            whole_sphere,
            polygons,
            cycles,
            line_paths,
            point_nodes,
        })
    }

    fn live_node_keys(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<Point>],
    ) -> Result<Vec<(EmittedNodeKey, QNodeId)>> {
        let mut live = Vec::new();
        match &self.polygons {
            PolygonPlan::WholeSphere(source_polygons) => {
                for &polygon_index in source_polygons {
                    let polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q whole-sphere polygon plan references an unknown polygon"
                                        .into(),
                                )
                            })?;
                    for ring_index in polygon.rings.clone() {
                        let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere polygon plan references an unknown ring".into(),
                            )
                        })?;
                        append_source_ring_nodes(topology, ring, &mut live)?;
                    }
                }
            },
            PolygonPlan::Regions { rings, .. } => {
                for ring in rings {
                    match *ring {
                        PlannedAreaRing::Source { ring, .. } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q area plan references an unknown source ring".into(),
                                    )
                                })?;
                            append_source_ring_nodes(topology, source_ring, &mut live)?;
                        },
                        PlannedAreaRing::Cycle { cycle, .. } => {
                            let cycle = self.cycles.get(cycle).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown cycle".into(),
                                )
                            })?;
                            for &dart_id in &cycle.darts {
                                let dart =
                                    topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                        GeometryErrorKind::Invalid(
                                            "Q area plan references an unknown dart".into(),
                                        )
                                    })?;
                                for port_id in [dart.from_port, dart.to_port] {
                                    let port =
                                        topology.q.ports.get(port_id.0 as usize).ok_or_else(
                                            || {
                                                GeometryErrorKind::Invalid(
                                                    "Q area plan references an unknown port".into(),
                                                )
                                            },
                                        )?;
                                    if !port.cut {
                                        record_emitted_node(
                                            topology.sources.frame,
                                            port.node,
                                            coordinates.point(topology, port_id)?,
                                            &mut live,
                                        )?;
                                    }
                                }
                            }
                        },
                    }
                }
            },
        }
        for path in &self.line_paths {
            for step in path {
                let span = topology.q.spans.get(step.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line plan references an unknown span".into())
                })?;
                for port_id in span.ports {
                    let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q line plan references an unknown port".into())
                    })?;
                    if !port.cut {
                        record_emitted_node(
                            topology.sources.frame,
                            port.node,
                            coordinates.point(topology, port_id)?,
                            &mut live,
                        )?;
                    }
                }
            }
        }
        for &node in &self.point_nodes {
            let point = emitted_point_for_node(topology, node, source_points, coordinates)?;
            record_emitted_node(topology.sources.frame, node, point, &mut live)?;
        }
        Ok(live)
    }
}

impl PolygonPlan {
    fn build(
        topology: &ExactGeographicTopology,
        whole_sphere: bool,
        operation: OverlayOp,
        cycles: &[BoundaryCycle],
    ) -> Result<Self> {
        if whole_sphere {
            let mut source_polygons = Vec::new();
            for (index, source_polygon) in topology.sources.polygons.iter().enumerate() {
                if !source_polygon_contributes_to_whole_sphere(source_polygon, operation) {
                    continue;
                }
                let has_areal_shell = source_polygon
                    .rings
                    .clone()
                    .filter_map(|ring| topology.sources.rings.get(ring))
                    .any(|ring| !ring.hole && ring.has_areal_winding());
                if has_areal_shell {
                    source_polygons.push(index);
                }
            }
            return Ok(Self::WholeSphere(source_polygons));
        }

        let mut rings = Vec::new();
        let mut emitted_cycles = vec![false; cycles.len()];
        for (ring_index, source_ring) in topology.sources.rings.iter().enumerate() {
            if !source_ring.summary.exact_axis_band {
                continue;
            }
            let mut source_cycles = Vec::new();
            for (cycle_index, cycle) in cycles.iter().enumerate() {
                if cycle_belongs_to_source_ring(topology, cycle, source_ring.id)? {
                    source_cycles.push(cycle_index);
                }
            }
            if source_cycles.len() != 2 {
                continue;
            }
            let Some(&first_cycle) = source_cycles.first() else {
                continue;
            };
            let region = cycles
                .get(first_cycle)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?
                .region;
            let mut cycles_are_consistent = true;
            for &cycle_index in &source_cycles {
                let cycle = cycles.get(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into())
                })?;
                let emitted = emitted_cycles.get(cycle_index).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band cycle has no emission state".into())
                })?;
                if cycle.region != region || emitted {
                    cycles_are_consistent = false;
                    break;
                }
            }
            if !cycles_are_consistent
                || !source_axis_band_cycles_cover_boundary(
                    topology,
                    source_ring,
                    cycles,
                    &source_cycles,
                )?
            {
                continue;
            }
            let Some(follows_source) = source_axis_band_cycles_follow_source(
                topology,
                source_ring,
                cycles,
                &source_cycles,
            )?
            else {
                continue;
            };
            rings.push(PlannedAreaRing::Source {
                ring: ring_index,
                region,
                reverse: !follows_source,
            });
            for cycle_index in source_cycles {
                *emitted_cycles.get_mut(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band cycle has no emission state".into())
                })? = true;
            }
        }
        for (cycle_index, (cycle, &emitted)) in cycles.iter().zip(&emitted_cycles).enumerate() {
            if !emitted {
                rings.push(PlannedAreaRing::Cycle {
                    cycle: cycle_index,
                    region: cycle.region,
                });
            }
        }
        Ok(Self::Regions {
            rings,
            region_count: topology.q.global_face_count,
        })
    }
}

impl QCoordinates {
    fn build(topology: &ExactGeographicTopology) -> Result<Self> {
        let frame = topology.sources.frame;
        let q = &topology.q;
        let mut node = Vec::with_capacity(q.nodes.len());
        let mut pole_port_longitude = vec![None; q.ports.len()];
        for (node_index, q_node) in q.nodes.iter().enumerate() {
            if q_node.id.0 as usize != node_index {
                return Err(GeometryErrorKind::Invalid(
                    "Q node coordinates are not indexed by QNodeId".into(),
                )
                .into());
            }
            let first_port_id = *q.node_ports.get(q_node.ports.start).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has no first port for coordinates".into())
            })?;
            let first_port = q
                .ports
                .get(first_port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q node port is unknown".into()))?;
            let xy = XY::new(
                round_canonical_longitude(frame, &first_port.point.x)?,
                Latitude::try_new(frame, first_port.point.y.round()?)
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                    .get(),
            );
            let is_pole = matches!(
                emitted_node_key(frame, xy)?,
                EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
            );
            node.push(xy);
            if is_pole {
                for &port_id in q.node_ports.get(q_node.ports.clone()).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node ports are incomplete".into())
                })? {
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q pole port is unknown".into())
                    })?;
                    *pole_port_longitude
                        .get_mut(port_id.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q pole port coordinate is unknown".into())
                        })? = Some(round_canonical_longitude(frame, &port.point.x)?);
                }
            }
        }
        Ok(Self {
            node,
            pole_port_longitude,
        })
    }

    fn point(&self, topology: &ExactGeographicTopology, port_id: QPortId) -> Result<Point> {
        let port = topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?;
        let xy = self
            .node
            .get(port.node.0 as usize)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q emitted node has no coordinates".into())
            })?;
        let longitude = self
            .pole_port_longitude
            .get(port_id.0 as usize)
            .copied()
            .flatten()
            .unwrap_or(xy.x);
        Ok(Point::new_raw_xy(longitude, xy.y))
    }
}

const fn selects_support(
    count: usize,
    first: bool,
    row_count: usize,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Union => count != 0,
        OverlayOp::Intersection => count == row_count,
        OverlayOp::Difference => first && count == 1,
        OverlayOp::SymmetricDifference => !count.is_multiple_of(2),
    }
}

/// Incident faces can differ only on rows owning an incident source edge.
/// Count their union from one face and those rows, without expanding ancestors.
#[derive(Default)]
struct AreaSupport {
    faces: Vec<GlobalFaceId>,
    rows: Vec<SourceRowId>,
}

impl AreaSupport {
    fn clear(&mut self) {
        self.faces.clear();
        self.rows.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span indices come from this topology's span table or admitted darts"
    )]
    fn append_span(&mut self, topology: &ExactGeographicTopology, span: usize) -> Result<()> {
        for owner in &topology.q.spans[span].owners {
            let edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q support references an unknown source edge".into())
                })?;
            self.rows.push(edge.row);
        }
        Ok(())
    }

    fn contains(&self, q: &super::q::ExactGeographicQ, row: SourceRowId) -> bool {
        self.faces
            .iter()
            .any(|&face| q.face_rows.contains(face, row))
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the union counts distinct admitted source rows"
    )]
    fn count(&mut self, q: &super::q::ExactGeographicQ) -> usize {
        let Some(&first) = self.faces.first() else {
            return 0;
        };
        self.rows.sort_unstable();
        self.rows.dedup();
        q.face_rows.count(first)
            + self
                .rows
                .iter()
                .filter(|&&row| !q.face_rows.contains(first, row) && self.contains(q, row))
                .count()
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "sorted unique support rows add only rows absent from the area union"
    )]
    fn with_rows(&mut self, q: &super::q::ExactGeographicQ, rows: &[SourceRowId]) -> (usize, bool) {
        let count = self.count(q) + rows.iter().filter(|&&row| !self.contains(q, row)).count();
        let first = rows.binary_search(&SourceRowId(0)).is_ok() || self.contains(q, SourceRowId(0));
        (count, first)
    }
}

fn append_unique_rows(output: &mut Vec<SourceRowId>, left: &[SourceRowId], right: &[SourceRowId]) {
    output.clear();
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    output.sort_unstable();
    output.dedup();
}

fn select_faces(
    q: &super::q::ExactGeographicQ,
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<bool>> {
    let mut selected = Vec::with_capacity(q.global_face_count);
    for face in 0..q.global_face_count {
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        selected.push(selects_support(
            q.face_rows.count(face),
            q.face_rows.contains(face, SourceRowId(0)),
            row_count,
            operation,
        ));
    }
    Ok(selected)
}

struct FaceUnion {
    parents: Vec<usize>,
    selected: Vec<bool>,
}

impl FaceUnion {
    fn new(selected: &[bool]) -> Self {
        Self {
            parents: (0..selected.len()).collect(),
            selected: selected.to_vec(),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Face IDs enter the union only from its selected-face table"
    )]
    fn find(&mut self, mut face: usize) -> usize {
        while self.parents[face] != face {
            self.parents[face] = self.parents[self.parents[face]];
            face = self.parents[face];
        }
        face
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Both joined face IDs were admitted against the selected-face table"
    )]
    fn join(&mut self, left: usize, right: usize) {
        let (left, right) = (self.find(left), self.find(right));
        if left != right {
            self.parents[left.max(right)] = left.min(right);
        }
    }

    fn selected_root(&mut self, face: GlobalFaceId) -> Result<usize> {
        let index = face.0 as usize;
        let selected = self.selected.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary references an unknown global face".into())
        })?;
        if !selected {
            return Err(GeometryErrorKind::Invalid(
                "Q boundary references an unselected global face".into(),
            )
            .into());
        }
        Ok(self.find(index))
    }
}

fn face_regions(q: &super::q::ExactGeographicQ, selected: &[bool]) -> Result<FaceUnion> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    let mut regions = FaceUnion::new(selected);
    for faces in &q.span_global_faces {
        let Some([left, right]) = faces else {
            continue;
        };
        let left = left.0 as usize;
        let right = right.0 as usize;
        let left_selected = selected.get(left).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let right_selected = selected.get(right).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        if left_selected && right_selected {
            regions.join(left, right);
        }
    }
    Ok(regions)
}

#[expect(
    clippy::indexing_slicing,
    reason = "global face indices and the selected-face table belong to the same admitted Q"
)]
fn select_line_spans(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<Option<SelectedLineSpan>>> {
    let q = &topology.q;
    let mut selected = Vec::with_capacity(q.spans.len());
    let mut area = AreaSupport::default();
    let mut curve_rows = Vec::new();
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        if q.span_is_collapsed_pole(span_index)? {
            selected.push(None);
            continue;
        }
        area.clear();
        if let Some([left, right]) = faces {
            if selected_faces[left.0 as usize] || selected_faces[right.0 as usize] {
                selected.push(None);
                continue;
            }
            area.faces.extend([*left, *right]);
            area.append_span(topology, span_index)?;
        }
        append_unique_rows(
            &mut curve_rows,
            q.line_rows_for_span(span_index)?,
            q.collapsed_rows_for_span(span_index)?,
        );
        let curve_selected = match operation {
            OverlayOp::Union => !curve_rows.is_empty(),
            OverlayOp::Intersection => area.with_rows(q, &curve_rows).0 == row_count,
            OverlayOp::Difference => {
                curve_rows.binary_search(&SourceRowId(0)).is_ok()
                    && area.with_rows(q, &curve_rows).0 == 1
            },
            OverlayOp::SymmetricDifference => !curve_rows.len().is_multiple_of(2),
        };
        selected.push(if curve_selected {
            Some(SelectedLineSpan {
                owner: line_owner_index(topology, span_index)?,
            })
        } else {
            None
        });
    }
    Ok(selected)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "node, dart, local and global face indices and fan offsets are admitted by this Q"
)]
fn select_points(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    selected_spans: &[Option<SelectedLineSpan>],
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<QNodeId>> {
    let q = &topology.q;
    let mut line_nodes = vec![false; q.nodes.len()];
    for (span, &selected) in q.spans.iter().zip(selected_spans) {
        if selected.is_some() {
            for node in span.nodes {
                *line_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span references an unknown node".into())
                })? = true;
            }
        }
    }

    let mut output = Vec::new();
    let mut area = AreaSupport::default();
    let mut support_rows = Vec::new();
    for (node_index, node) in q.nodes.iter().enumerate() {
        if line_nodes[node_index] {
            continue;
        }
        area.clear();
        support_rows.clear();
        let fan = &q.outgoing[q.outgoing_offsets[node_index]..q.outgoing_offsets[node_index + 1]];
        if fan.is_empty() {
            let face = q.source_node_faces[node_index].ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
            area.faces.push(face);
        } else {
            for &dart in fan {
                let span_index = q.darts[dart.0 as usize].span.0 as usize;
                support_rows.extend_from_slice(q.line_rows_for_span(span_index)?);
                support_rows.extend_from_slice(q.collapsed_rows_for_span(span_index)?);
                let local = q.dart_faces[dart.0 as usize];
                area.faces.push(q.global_face_of_local[local.0 as usize]);
                area.append_span(topology, span_index)?;
            }
        }
        if area
            .faces
            .iter()
            .any(|face| selected_faces[face.0 as usize])
        {
            continue;
        }
        support_rows.extend_from_slice(q.point_rows_for_node(node_index)?);
        support_rows.sort_unstable();
        support_rows.dedup();
        let (count, first) = area.with_rows(q, &support_rows);
        if selects_support(count, first, row_count, operation) {
            output.push(node.id);
        }
    }
    Ok(output)
}

fn selected_area_boundaries(
    q: &super::q::ExactGeographicQ,
    selected_faces: &[bool],
    regions: &mut FaceUnion,
) -> Result<Vec<BoundaryCycle>> {
    let mut boundary = vec![false; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        let Some([left, right]) = faces else {
            continue;
        };
        let left_selected = selected_faces
            .get(left.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown left face".into()))?;
        let right_selected = selected_faces
            .get(right.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown right face".into()))?;
        if left_selected == right_selected {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a selected Q face boundary has no dart".into())
            })?;
        let dart = if left_selected {
            forward
        } else {
            QDartId(forward.0 ^ 1)
        };
        let slot = boundary.get_mut(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("a Q face boundary dart is unknown".into())
        })?;
        *slot = true;
    }

    let mut visited = vec![false; q.darts.len()];
    let mut positions = vec![None; q.nodes.len()];
    let mut walks = Vec::new();
    for (seed, &is_boundary) in boundary.iter().enumerate() {
        let was_visited = visited.get(seed).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay boundary has no visited state".into())
        })?;
        if !is_boundary || was_visited {
            continue;
        }
        let start = QDartId(u32::try_from(seed).map_err(|_| ResourceError::SizeOverflow)?);
        let mut current = start;
        let mut walk = Vec::new();
        loop {
            let current_index = current.0 as usize;
            if visited.get(current_index).copied().unwrap_or(false) {
                if current == start {
                    break;
                }
                return Err(GeometryErrorKind::Invalid(
                    "Q overlay boundary walks intersect before closing".into(),
                )
                .into());
            }
            *visited.get_mut(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })? = true;
            let dart = *q.darts.get(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })?;
            *positions
                .get_mut(dart.from_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary starts at an unknown node".into())
                })? = Some(walk.len());
            walk.push(current);

            if let Some(begin) = positions.get(dart.to_node.0 as usize).copied().flatten() {
                let mut cycle = Vec::with_capacity(walk.len().saturating_sub(begin));
                for dart in walk.drain(begin..) {
                    let node = q
                        .darts
                        .get(dart.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q boundary cycle has an unknown dart".into(),
                            )
                        })?
                        .from_node;
                    *positions.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary cycle has an unknown node".into())
                    })? = None;
                    cycle.push(dart);
                }
                if !cycle.is_empty() {
                    walks.push(cycle);
                }
            }
            current = next_boundary_dart(q, &boundary, current)?;
            if current == start {
                break;
            }
        }
        if !walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay boundary did not decompose into closed cycles".into(),
            )
            .into());
        }
    }
    if boundary
        .iter()
        .zip(visited)
        .any(|(&selected, visited)| selected && !visited)
    {
        return Err(GeometryErrorKind::Invalid(
            "Q overlay left a selected boundary dart unvisited".into(),
        )
        .into());
    }

    let mut cycles = Vec::with_capacity(walks.len());
    for darts in walks {
        let first = *darts.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay emitted an empty boundary cycle".into())
        })?;
        let local = *q.dart_faces.get(first.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary cycle has no left face".into())
        })?;
        let face = *q
            .global_face_of_local
            .get(local.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle left face has no global ID".into())
            })?;
        let region = regions.selected_root(face)?;
        cycles.push(BoundaryCycle { darts, region });
    }
    Ok(cycles)
}

fn next_boundary_dart(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    dart_id: QDartId,
) -> Result<QDartId> {
    let dart = q
        .darts
        .get(dart_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
    let twin = QDartId(dart_id.0 ^ 1);
    let twin_position = *q
        .dart_positions
        .get(twin.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary twin has no fan position".into()))?;
    let start = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan start".into()))?;
    let end = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan end".into()))?;
    let length = end.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary node has reversed fan offsets".into())
    })?;
    if length == 0 || twin_position >= length {
        return Err(
            GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into()).into(),
        );
    }
    let fan = q
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node fan is incomplete".into()))?;
    let (before_twin, twin_and_after) = fan.split_at(twin_position);
    let (twin, after_twin) = twin_and_after.split_first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into())
    })?;
    for &candidate in std::iter::once(twin)
        .chain(before_twin.iter().rev())
        .chain(after_twin.iter().rev())
    {
        if selected.get(candidate.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary fan has an unknown dart".into())
        })? {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("a selected Q boundary dart has no successor".into()).into())
}

fn source_edge_at(
    topology: &ExactGeographicTopology,
    source_edge: SourceEdgeId,
) -> Result<&ExactSourceEdge> {
    topology
        .sources
        .edges
        .get(source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q owner has an unknown source edge".into()).into()
        })
}

fn line_owner_index(topology: &ExactGeographicTopology, span_index: usize) -> Result<usize> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
    let line_rows = q.line_rows_for_span(span_index)?;
    let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
    let mut ring_owner = None;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        let edge = source_edge_at(topology, owner.source_edge)?;
        let supported = match edge.kind {
            SourceEdgeKind::Line { .. } => line_rows.binary_search(&edge.row).is_ok(),
            SourceEdgeKind::Ring {
                hole: false,
                kind: crate::geometry::RingKind::Collapsed,
                ..
            } => collapsed_rows.binary_search(&edge.row).is_ok(),
            SourceEdgeKind::Ring { .. } => {
                ring_owner.get_or_insert(owner_index);
                false
            },
        };
        if supported {
            return Ok(owner_index);
        }
    }
    if let Some(owner) = ring_owner {
        return Ok(owner);
    }
    Err(
        GeometryErrorKind::Invalid("selected Q curve span has no source interval owner".into())
            .into(),
    )
}

fn area_owner_index(topology: &ExactGeographicTopology, span_index: usize) -> Result<usize> {
    let span = topology
        .q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area span is unknown".into()))?;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        if matches!(
            source_edge_at(topology, owner.source_edge)?.kind,
            SourceEdgeKind::Ring { .. }
        ) {
            return Ok(owner_index);
        }
    }
    Err(GeometryErrorKind::Invalid("selected Q area boundary has no ring owner".into()).into())
}

fn line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
) -> Result<Vec<Vec<LineStep>>> {
    let q = &topology.q;
    let node_count = q.nodes.len();
    let mut degree = vec![0_usize; node_count];
    let mut incidences = Vec::new();
    let edge_count = selected.iter().flatten().count();
    incidences.reserve(
        edge_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    for (span_index, (span, selection)) in q.spans.iter().zip(selected).enumerate() {
        if selection.is_none() {
            continue;
        }
        let [start_node, end_node] = span.nodes;
        for (endpoint, node_id) in [
            (SpanEndpoint::Start, start_node),
            (SpanEndpoint::End, end_node),
        ] {
            let node = node_id.0 as usize;
            let next = degree.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown node".into())
            })?;
            *next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            incidences.push((node, span_index, endpoint));
        }
    }
    incidences.sort_unstable();
    let offset_count = node_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0_usize);
    for count in &degree {
        offsets.push(
            offsets
                .last()
                .copied()
                .expect("line incidence offsets start at zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if offsets.last().copied() != Some(incidences.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q line incidence offsets do not match their entries".into(),
        )
        .into());
    }
    let mut visited = vec![false; q.spans.len()];
    let mut paths = Vec::new();
    for (node, &node_degree) in degree.iter().enumerate() {
        if node_degree == 2 {
            continue;
        }
        let [start, end] = offsets
            .get(node..node.saturating_add(2))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has invalid offsets".into()))?
        else {
            return Err(
                GeometryErrorKind::Invalid("Q line node has invalid offsets".into()).into(),
            );
        };
        let incident = incidences.get(*start..*end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })?;
        for &(_, span, endpoint) in incident {
            let selection = selected.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence references an unknown span".into())
            })?;
            let already_visited = visited.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence has no visited state".into())
            })?;
            if selection.is_some() && !already_visited {
                paths.push(walk_line_steps(
                    topology,
                    selected,
                    &degree,
                    &offsets,
                    &incidences,
                    &mut visited,
                    span,
                    endpoint,
                )?);
            }
        }
    }
    for (span, selection) in selected.iter().enumerate() {
        let already_visited = visited
            .get(span)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span has no visited state".into()))?;
        if selection.is_some() && !already_visited {
            paths.push(walk_line_steps(
                topology,
                selected,
                &degree,
                &offsets,
                &incidences,
                &mut visited,
                span,
                SpanEndpoint::Start,
            )?);
        }
    }
    Ok(paths)
}

fn walk_line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
    degree: &[usize],
    offsets: &[usize],
    incidences: &[(usize, usize, SpanEndpoint)],
    visited: &mut [bool],
    start_span: usize,
    start_endpoint: SpanEndpoint,
) -> Result<Vec<LineStep>> {
    let q = &topology.q;
    let mut steps = Vec::new();
    let (mut span_index, mut from_endpoint) = (start_span, start_endpoint);
    loop {
        let already_visited = visited
            .get_mut(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        if *already_visited {
            if span_index == start_span && from_endpoint == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line traversal entered an earlier span".into(),
            )
            .into());
        }
        *already_visited = true;
        let owner = selected
            .get(span_index)
            .copied()
            .flatten()
            .map(|selection| selection.owner)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "selected Q curve span has no source interval owner".into(),
                )
            })?;
        steps.push(LineStep {
            span: span_index,
            from_endpoint,
            owner,
        });
        let span = q
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        let arrived_endpoint = from_endpoint.opposite();
        let [start_node, end_node] = span.nodes;
        let node = match arrived_endpoint {
            SpanEndpoint::Start => start_node,
            SpanEndpoint::End => end_node,
        }
        .0 as usize;
        if degree.get(node).copied() != Some(2) {
            break;
        }
        let start = *offsets.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has no incidence start".into())
        })?;
        let end = *offsets
            .get(node + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no incidence end".into()))?;
        let next = incidences
            .get(start..end)
            .and_then(|incident| {
                incident.iter().find(|&&(_, candidate, endpoint)| {
                    candidate != span_index || endpoint != arrived_endpoint
                })
            })
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no continuation".into()))?;
        if visited.get(next.1).copied().unwrap_or(false) {
            if next.1 == start_span && next.2 == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line cycle closes on a different used span".into(),
            )
            .into());
        }
        span_index = next.1;
        from_endpoint = next.2;
    }
    if steps.is_empty() {
        return Err(GeometryErrorKind::Invalid("Q line traversal emitted no spans".into()).into());
    }
    Ok(steps)
}

fn source_arc_node_ordinates<M: crate::geometry::ChartMetric>(
    frame: crate::geographic::GeographicFrame,
    source: &[Point; 2],
    parameter: &ExactRational,
    axes: CoordinateAxes,
    longitude: impl FnOnce() -> Result<RetainedChartDisplacement>,
    metric_access: Option<&SourceArcMetricAccess<'_, M>>,
) -> Result<NodeOrdinates> {
    let request = SourceArcRequest::new(frame, source, axes, parameter, longitude)?;
    source_arc_ordinates(&request, || match metric_access {
        Some(access) => access(),
        None => Err(crate::crs::CrsError::invalid(
            "geographic source-ordinate carry requires the operation's CRS metric authority",
        )
        .into()),
    })
}

fn node_ordinates<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, M>>,
) -> Result<Vec<NodeOrdinates>> {
    let q = &topology.q;
    if q.source_edge_ports.len() != topology.sources.edges.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-edge ports do not match their source intervals".into(),
        )
        .into());
    }
    if q.source_point_ports.len() != topology.sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match their source points".into(),
        )
        .into());
    }
    let mut output = vec![NodeOrdinates::default(); q.nodes.len()];
    if !declared_axes.has_z() && !declared_axes.has_m() {
        return Ok(output);
    }
    for (edge, ports) in topology
        .sources
        .edges
        .iter()
        .zip(q.source_edge_ports.iter())
    {
        for (point, &port) in edge.source.iter().zip(ports) {
            let node = q
                .ports
                .get(port.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint port is unknown".into())
                })?
                .node;
            let values = output.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source endpoint node is unknown".into())
            })?;
            values.z.observe(point.z());
            values.m.observe(point.m());
        }
    }
    for span in &q.spans {
        for owner in &span.owners {
            let edge = source_edge_at(topology, owner.source_edge)?;
            let start_parameter = if owner.reversed {
                &owner.source_range.end
            } else {
                &owner.source_range.start
            };
            let end_parameter = if owner.reversed {
                &owner.source_range.start
            } else {
                &owner.source_range.end
            };
            let start_values = source_arc_node_ordinates(
                topology.sources.frame,
                &edge.source,
                start_parameter,
                declared_axes,
                || Ok(edge.retained_longitude()),
                metric_access,
            )?;
            let end_values = source_arc_node_ordinates(
                topology.sources.frame,
                &edge.source,
                end_parameter,
                declared_axes,
                || Ok(edge.retained_longitude()),
                metric_access,
            )?;
            let start = output
                .get_mut(span.nodes[0].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span start node is unknown".into()))?;
            start.z.observe(start_values.z.value());
            start.m.observe(start_values.m.value());
            let end = output
                .get_mut(span.nodes[1].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span end node is unknown".into()))?;
            end.z.observe(end_values.z.value());
            end.m.observe(end_values.m.value());
        }
    }
    for (source, &port) in topology
        .sources
        .points
        .iter()
        .zip(q.source_point_ports.iter())
    {
        let node = q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        let values = output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        values.z.observe(source.point.z());
        values.m.observe(source.point.m());
    }
    Ok(output)
}

fn emitted_point(
    topology: &ExactGeographicTopology,
    coordinates: &QCoordinates,
    ordinates: &[NodeOrdinates],
    span_index: usize,
    owner_index: usize,
    port: QPortId,
) -> Result<EmittedVertex> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted span is unknown".into()))?;
    span.owners
        .get(owner_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted owner is unknown".into()))?;
    if !span.ports.contains(&port) {
        return Err(GeometryErrorKind::Invalid("Q port is not on its emitted span".into()).into());
    }
    let node = q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?
        .node;
    let _ = ordinates
        .get(node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted node has no ordinates".into()))?;
    Ok(EmittedVertex {
        node,
        port,
        point: coordinates.point(topology, port)?,
        chart: q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?
            .point
            .clone(),
    })
}

fn round_canonical_longitude(
    frame: crate::geographic::GeographicFrame,
    longitude: &ExactRational,
) -> Result<f64> {
    let axis = frame.longitude();
    crate::geometry::exact_arith::round_native_longitude(axis, longitude)
}

fn push_line_part(output: &mut Vec<CoordSeq>, points: &mut Vec<Point>) -> Result<()> {
    if points.len() >= 2 {
        output.push(CoordSeq::try_from_points(points)?);
    }
    points.clear();
    Ok(())
}

fn emit_polygons(
    topology: &ExactGeographicTopology,
    plan: &QOverlayPlan,
    ordinates: &[NodeOrdinates],
    coordinates: &QCoordinates,
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
    edge_budget: &mut ExpansionBudget,
    certificate: &mut QPublicationCertificate<'_>,
) -> Result<Vec<Polygon>> {
    match &plan.polygons {
        PolygonPlan::WholeSphere(source_polygons) => {
            // A whole-sphere selection has no exterior boundary to trace. Its
            // closed source polygon union is the representable output boundary.
            let mut output = Vec::with_capacity(source_polygons.len());
            for &polygon_index in source_polygons {
                let source_polygon =
                    topology
                        .sources
                        .polygons
                        .get(polygon_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere plan references an unknown polygon".into(),
                            )
                        })?;
                let polygon = emit_source_polygon(topology, source_polygon)?;
                output.push(carry_polygon_axes(
                    topology.sources.frame,
                    &polygon,
                    ordinates,
                    live,
                    axes,
                )?);
            }
            if output.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "Q selected the whole sphere without source area polygons".into(),
                )
                .into());
            }
            Ok(output)
        },
        PolygonPlan::Regions {
            rings,
            region_count,
        } => {
            let mut shells = Vec::with_capacity(*region_count);
            let mut holes = Vec::with_capacity(*region_count);
            shells.resize_with(*region_count, Vec::<Ring>::new);
            holes.resize_with(*region_count, Vec::<Ring>::new);
            for planned in rings {
                let (region, ring, is_hole) = match *planned {
                    PlannedAreaRing::Source {
                        ring: source_ring_index,
                        region,
                        reverse,
                    } => {
                        let source_ring = topology
                            .sources
                            .rings
                            .get(source_ring_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown source ring".into(),
                                )
                            })?;
                        let mut ring = emit_source_ring(topology, source_ring)?;
                        if reverse {
                            ring = ring.reversed()?;
                        }
                        let is_hole = chart_ring_is_hole(topology.sources.frame, &ring)?;
                        let ring =
                            carry_ring_axes(topology.sources.frame, &ring, ordinates, live, axes)?;
                        (region, ring, is_hole)
                    },
                    PlannedAreaRing::Cycle { cycle, region } => {
                        let cycle = plan.cycles.get(cycle).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q area plan references an unknown cycle".into(),
                            )
                        })?;
                        let (ring, is_hole) = emit_ring(
                            topology,
                            coordinates,
                            ordinates,
                            cycle,
                            axes,
                            edge_budget,
                            certificate,
                        )?;
                        (region, ring, is_hole)
                    },
                };
                let destination = if is_hole {
                    holes.get_mut(region)
                } else {
                    shells.get_mut(region)
                }
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary region is unknown".into()))?;
                destination.push(ring);
            }

            let mut output = Vec::new();
            for (region, (mut region_shells, region_holes)) in
                shells.into_iter().zip(holes).enumerate()
            {
                if region_shells.is_empty() && region_holes.is_empty() {
                    continue;
                }
                if region_shells.len() != 1 {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "selected Q region {region} has {} shells",
                            region_shells.len(),
                        )
                        .into(),
                    )
                    .into());
                }
                let shell = region_shells
                    .pop()
                    .expect("the selected region has one shell");
                output.push(Polygon::new(shell, region_holes));
            }
            Ok(output)
        },
    }
}

const fn source_polygon_contributes_to_whole_sphere(
    source_polygon: &super::exact::ExactSourcePolygon,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Intersection | OverlayOp::Difference => source_polygon.row.0 == 0,
        OverlayOp::Union | OverlayOp::SymmetricDifference => true,
    }
}

fn cycle_belongs_to_source_ring(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    source_ring: SourceRingId,
) -> Result<bool> {
    let q = &topology.q;
    for &dart_id in &cycle.darts {
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let span = q
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
        let mut owned = false;
        for owner in &span.owners {
            let edge = source_edge_at(topology, owner.source_edge)?;
            if matches!(
                edge.kind,
                SourceEdgeKind::Ring { ring, .. } if ring == source_ring
            ) {
                owned = true;
                break;
            }
        }
        if !owned {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_axis_band_cycles_follow_source(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
) -> Result<Option<bool>> {
    let q = &topology.q;
    let mut follows_source = None;
    for &cycle_index in cycle_indices {
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            let dart = q
                .darts
                .get(dart_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
            let span = q
                .spans
                .get(dart.span.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
            let mut span_direction = None;
            for owner in &span.owners {
                let edge = source_edge_at(topology, owner.source_edge)?;
                if !matches!(
                    edge.kind,
                    SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id
                ) {
                    continue;
                }
                let forward = (dart_id.0 & 1 == 0) != owner.reversed;
                if span_direction.is_some_and(|previous| previous != forward) {
                    return Ok(None);
                }
                span_direction = Some(forward);
            }
            let Some(span_direction) = span_direction else {
                return Ok(None);
            };
            if follows_source.is_some_and(|previous| previous != span_direction) {
                return Ok(None);
            }
            follows_source = Some(span_direction);
        }
    }
    Ok(follows_source)
}

fn source_axis_band_cycles_cover_boundary(
    topology: &ExactGeographicTopology,
    source_ring: &super::exact::ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
) -> Result<bool> {
    let frame = topology.sources.frame;
    let axis = frame.longitude();
    let south = canonical_bits(source_ring.summary.south.get());
    let north = canonical_bits(source_ring.summary.north.get());
    let mut parallel_edges = Vec::new();
    let mut parallel_mask = 0_u8;
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let latitude = canonical_bits(edge.source[0].y);
        if latitude != canonical_bits(edge.source[1].y) {
            continue;
        }
        let parallel = if latitude == south {
            1
        } else if latitude == north {
            2
        } else {
            continue;
        };
        let full_turn = axis
            .chart_step(edge.source[0].x, edge.source[1].x)
            .expect("Q source longitude was admitted")
            .is_circle();
        if !full_turn {
            let start = axis
                .canonicalize(edge.source[0].x)
                .expect("source longitude was admitted")
                .get();
            let end = axis
                .canonicalize(edge.source[1].x)
                .expect("source longitude was admitted")
                .get();
            if start.to_bits() == end.to_bits() {
                continue;
            }
        }
        parallel_mask |= parallel;
        parallel_edges.push(edge.id);
    }
    if parallel_mask != 0b11 {
        return Ok(false);
    }

    for edge_id in parallel_edges {
        let mut ranges = Vec::new();
        for &cycle_index in cycle_indices {
            let cycle = all_cycles
                .get(cycle_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?;
            for &dart_id in &cycle.darts {
                let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band dart is unknown".into())
                })?;
                let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band span is unknown".into())
                })?;
                ranges.extend(
                    span.owners
                        .iter()
                        .filter(|owner| owner.source_edge == edge_id)
                        .map(|owner| owner.source_range.clone()),
                );
            }
        }
        if !source_ranges_cover_unit_interval(&mut ranges)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_ranges_cover_unit_interval(ranges: &mut [ExactSourceRange]) -> Result<bool> {
    try_sort_by(ranges, |left, right| left.start.try_cmp(&right.start))?;
    let mut covered = ExactRational::zero();
    let one = ExactRational::one();
    for range in ranges {
        if range.start.try_cmp(&covered)?.is_gt() {
            return Ok(false);
        }
        if range.end.try_cmp(&covered)?.is_gt() {
            covered = range.end.clone();
        }
    }
    Ok(covered.try_cmp(&one)?.is_ge())
}

fn emit_source_ring(
    topology: &ExactGeographicTopology,
    source_ring: &super::exact::ExactSourceRing,
) -> Result<Ring> {
    let mut points = Vec::with_capacity(source_ring.source_edges.len().saturating_add(1));
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        points.push(edge.source[0].with_xy_raw(
            edge.source[0].x,
            Latitude::from_admitted(topology.sources.frame, edge.source[0].y).get(),
        ));
    }
    let last_edge = source_ring
        .source_edges
        .end
        .checked_sub(1)
        .and_then(|index| topology.sources.edges.get(index))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring has no edges".into()))?;
    points.push(last_edge.source[1].with_xy_raw(
        last_edge.source[1].x,
        Latitude::from_admitted(topology.sources.frame, last_edge.source[1].y).get(),
    ));
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

fn append_source_ring_nodes(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
        for (endpoint, &port_id) in edge.source.iter().zip(ports) {
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring port is unknown".into())
            })?;
            record_emitted_node(
                topology.sources.frame,
                port.node,
                endpoint.with_xy_raw(
                    endpoint.x,
                    Latitude::from_admitted(topology.sources.frame, endpoint.y).get(),
                ),
                live,
            )?;
        }
    }
    Ok(())
}

fn emit_source_polygon(
    topology: &ExactGeographicTopology,
    source_polygon: &super::exact::ExactSourcePolygon,
) -> Result<Polygon> {
    let mut shell = None;
    let mut holes = Vec::new();
    for ring_index in source_polygon.rings.clone() {
        let source_ring =
            topology.sources.rings.get(ring_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source polygon ring is unknown".into())
            })?;
        let ring = emit_source_ring(topology, source_ring)?;
        if source_ring.hole {
            holes.push(ring);
        } else if shell.replace(ring).is_some() {
            return Err(GeometryErrorKind::Invalid(
                "Q source polygon has more than one shell".into(),
            )
            .into());
        }
    }
    Ok(Polygon::new(
        shell.ok_or_else(|| GeometryErrorKind::Invalid("Q source polygon has no shell".into()))?,
        holes,
    ))
}

fn append_q_source_range(
    pending: &mut Option<(SourceEdgeId, ExactRational)>,
    contiguous: &mut bool,
    edge: SourceEdgeId,
    start: &ExactRational,
    end: &ExactRational,
) -> Result<()> {
    match pending {
        Some((previous_edge, previous_end))
            if *previous_edge == edge && previous_end.try_cmp(start)?.is_eq() =>
        {
            *previous_end = end.clone();
        },
        Some(_) => *contiguous = false,
        None => *pending = Some((edge, end.clone())),
    }
    Ok(())
}

fn finish_q_output_edge(
    edge_deltas: &mut Vec<ExactRational>,
    computed_edges: &mut Vec<bool>,
    pending_delta: &mut ExactRational,
    pending_source: &mut Option<(SourceEdgeId, ExactRational)>,
    source_contiguous: &mut bool,
) {
    edge_deltas.push(pending_delta.clone());
    computed_edges.push(pending_source.is_none() || !*source_contiguous);
    *pending_delta = ExactRational::zero();
    *pending_source = None;
    *source_contiguous = true;
}

fn q_vertex_touches_preserved_edge(computed_edges: &[bool], index: usize) -> bool {
    let edge_count = computed_edges.len();
    let incoming = if index == 0 {
        edge_count.saturating_sub(1)
    } else {
        index.wrapping_sub(1)
    };
    let outgoing = if index == edge_count { 0 } else { index };
    computed_edges
        .get(incoming)
        .is_none_or(|&computed| !computed)
        || computed_edges
            .get(outgoing)
            .is_none_or(|&computed| !computed)
}

fn emit_ring(
    topology: &ExactGeographicTopology,
    coordinates: &QCoordinates,
    ordinates: &[NodeOrdinates],
    cycle: &BoundaryCycle,
    axes: CoordinateAxes,
    edge_budget: &mut ExpansionBudget,
    certificate: &mut QPublicationCertificate<'_>,
) -> Result<(Ring, bool)> {
    let q = &topology.q;
    let mut pivot = 0;
    for (index, &dart_id) in cycle.darts.iter().enumerate() {
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let port = q
            .ports
            .get(dart.from_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?;
        if !port.cut {
            pivot = index;
            break;
        }
    }
    let (before_pivot, from_pivot) = cycle.darts.split_at(pivot);
    let mut vertices = Vec::new();
    let mut edge_deltas = Vec::new();
    let mut computed_edges = Vec::new();
    let mut pending_delta = ExactRational::zero();
    let mut pending_source = None;
    let mut source_contiguous = true;
    for &dart_id in from_pivot.iter().chain(before_pivot) {
        let dart = *q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let owner_index = area_owner_index(topology, dart.span.0 as usize)?;
        let span = q
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
        let owner = span
            .owners
            .get(owner_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary owner is unknown".into()))?;
        let from = emitted_point(
            topology,
            coordinates,
            ordinates,
            dart.span.0 as usize,
            owner_index,
            dart.from_port,
        )?;
        let to = emitted_point(
            topology,
            coordinates,
            ordinates,
            dart.span.0 as usize,
            owner_index,
            dart.to_port,
        )?;
        let span_delta = to.chart.x.try_subtract(&from.chart.x)?;
        let source_forward = (dart.from_port == span.ports[0]) != owner.reversed;
        let (source_start, source_end) = if source_forward {
            (&owner.source_range.start, &owner.source_range.end)
        } else {
            (&owner.source_range.end, &owner.source_range.start)
        };
        let from_cut = q
            .ports
            .get(dart.from_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?
            .cut;
        let to_cut = q
            .ports
            .get(dart.to_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?
            .cut;
        if !from_cut
            && vertices
                .last()
                .is_none_or(|previous: &EmittedVertex| previous.port != from.port)
        {
            if !vertices.is_empty() {
                finish_q_output_edge(
                    &mut edge_deltas,
                    &mut computed_edges,
                    &mut pending_delta,
                    &mut pending_source,
                    &mut source_contiguous,
                );
            }
            vertices.push(from);
        }
        pending_delta = pending_delta.try_add(&span_delta)?;
        append_q_source_range(
            &mut pending_source,
            &mut source_contiguous,
            owner.source_edge,
            source_start,
            source_end,
        )?;
        if !to_cut
            && vertices
                .last()
                .is_none_or(|previous: &EmittedVertex| previous.port != to.port)
        {
            if !vertices.is_empty() {
                finish_q_output_edge(
                    &mut edge_deltas,
                    &mut computed_edges,
                    &mut pending_delta,
                    &mut pending_source,
                    &mut source_contiguous,
                );
            }
            vertices.push(to);
        }
    }
    if vertices.len() < 3 {
        return Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has fewer than three emitted vertices".into(),
        )
        .into());
    }
    if vertices
        .first()
        .is_some_and(|first| vertices.last().is_some_and(|last| last.port != first.port))
    {
        let first = vertices.first().expect("the boundary has vertices").clone();
        finish_q_output_edge(
            &mut edge_deltas,
            &mut computed_edges,
            &mut pending_delta,
            &mut pending_source,
            &mut source_contiguous,
        );
        vertices.push(first);
    }
    if edge_deltas.len() != vertices.len().saturating_sub(1) {
        return Err(GeometryErrorKind::Invalid(
            "Q computed-edge intents do not match the emitted ring".into(),
        )
        .into());
    }
    if computed_edges.len() != edge_deltas.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q computed-edge roles do not match the emitted ring".into(),
        )
        .into());
    }
    let mut points = vertices
        .iter()
        .map(|vertex| vertex.point)
        .collect::<Vec<_>>();
    canonicalize_ring_vertices(topology.sources.frame, &vertices, &mut points)?;
    let is_hole = if points.array_windows::<2>().any(|[start, end]| {
        !topology
            .sources
            .frame
            .longitude()
            .ordinary_chart_edge(start.x, end.x)
    }) {
        let chart_ring = Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?;
        chart_ring_is_hole(topology.sources.frame, &chart_ring)?
    } else {
        match open_point_cycle_winding(&points) {
            RingWinding::CounterClockwise => false,
            RingWinding::Clockwise => true,
            RingWinding::Degenerate => {
                let chart_ring = Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?;
                chart_ring_is_hole(topology.sources.frame, &chart_ring)?
            },
        }
    };
    for (vertex, point) in vertices.iter().zip(&mut points) {
        let ordinates = ordinates
            .get(vertex.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q output ring has no ordinates".into()))?;
        *point = with_axes(vertex.point, *ordinates, axes)?;
    }
    let first = *points
        .first()
        .expect("selected Q area boundary has at least three vertices");
    *points
        .last_mut()
        .expect("selected Q area boundary has at least three vertices") = first;
    let frame = topology.sources.frame;
    let exact_latitude_at = |index: usize, parameter: f64| -> Result<ExactRational> {
        let from = vertices
            .get(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q ring sample start is unknown".into()))?;
        let to = vertices
            .get(index.wrapping_add(1))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q ring sample end is unknown".into()))?;
        let latitude_delta = to.chart.y.try_subtract(&from.chart.y)?;
        let exact_parameter = ExactRational::from_f64(parameter);
        from.chart
            .y
            .try_add(&latitude_delta.try_product(&exact_parameter)?)
    };
    let published = crate::geographic::publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points.as_slice(),
        true,
        edge_budget,
        |index, _, _| {
            let computed = *computed_edges
                .get(index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q ring edge role is unknown".into()))?;
            let delta = edge_deltas.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q ring edge intent is unknown".into())
            })?;
            if !computed {
                return Ok(crate::geographic::ComputedEdgeIntent::preserved(
                    delta.is_zero(),
                ));
            }
            let from = vertices
                .get(index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q ring edge start is unknown".into()))?;
            let to = vertices
                .get(index.wrapping_add(1))
                .ok_or_else(|| GeometryErrorKind::Invalid("Q ring edge end is unknown".into()))?;
            Ok(crate::geographic::ComputedEdgeIntent::exact_lifted([
                &from.chart.x,
                &to.chart.x,
            ]))
        },
        |index, _| {
            if q_vertex_touches_preserved_edge(&computed_edges, index) {
                crate::geographic::ComputedVertexIntent::Preserved
            } else {
                crate::geographic::ComputedVertexIntent::Sampled
            }
        },
        |index, endpoints, _, parameter, _| {
            let latitude = Latitude::try_new(frame, exact_latitude_at(index, parameter)?.round()?)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                .get();
            let [start, end] = endpoints;
            Ok(crate::geographic::ComputedSampleOrdinates {
                y: latitude,
                z: ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), parameter)),
                m: MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), parameter)),
            })
        },
        |index, _, _, parameter, retained, point| {
            let exact = ExactChartPoint::from_rationals(
                retained.exact_chart_longitude.clone(),
                exact_latitude_at(index, parameter)?,
            );
            certificate.record(exact, point)
        },
    )?;
    let published = published.unwrap_or(points);
    Ok((
        Ring::try_from_closed(CoordSeq::try_from_points(&published)?)?,
        is_hole,
    ))
}

fn chart_ring_is_hole(frame: crate::geographic::GeographicFrame, ring: &Ring) -> Result<bool> {
    // Overlay can create cycles with no single source-ring role; classify the emitted chart.
    let winding = if crate::geometry::chain_crosses_antimeridian(ring.coords(), frame) {
        match crate::geometry::unwrap_coordinates(ring.coords(), frame) {
            Ok(coordinates) => ring_winding(&Ring::try_from_closed(coordinates)?),
            Err(crate::geometry::UnwrapChartError::Winding) => RingWinding::Degenerate,
            Err(crate::geometry::UnwrapChartError::Geometry(error)) => return Err(error),
        }
    } else {
        ring_winding(ring)
    };
    match winding {
        RingWinding::CounterClockwise => Ok(false),
        RingWinding::Clockwise => Ok(true),
        RingWinding::Degenerate => {
            let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            if summary.chart_turn == 0 {
                return Err(GeometryErrorKind::Invalid(
                    "selected Q area boundary has no chart winding".into(),
                )
                .into());
            }
            // A polar ring can have zero planar shoelace area; its signed
            // longitude winding places the selected chart side at a pole.
            match summary.bounded_pole() {
                Some(ChartPole::North) => Ok(summary.chart_turn < 0),
                Some(ChartPole::South) => Ok(summary.chart_turn > 0),
                None => Err(GeometryErrorKind::Invalid(
                    "selected Q area boundary has no bounded pole".into(),
                )
                .into()),
            }
        },
    }
}

fn emit_lines(
    topology: &ExactGeographicTopology,
    paths: &[Vec<LineStep>],
    ordinates: &[NodeOrdinates],
    coordinates: &QCoordinates,
) -> Result<Vec<CoordSeq>> {
    let q = &topology.q;
    let mut output = Vec::new();
    for path in paths {
        let mut start = None;
        for (index, step) in path.iter().enumerate() {
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let from_port = match step.from_endpoint {
                SpanEndpoint::Start => start_port,
                SpanEndpoint::End => end_port,
            };
            let port = q.ports.get(from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown port".into())
            })?;
            if !port.cut {
                start = Some(index);
                break;
            }
        }
        let start = start.unwrap_or(0);
        let (before_start, from_start) = path.split_at(start);
        let mut points = Vec::new();
        let mut previous: Option<EmittedVertex> = None;
        for step in from_start.iter().chain(before_start) {
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let (from_port, to_port) = match step.from_endpoint {
                SpanEndpoint::Start => (start_port, end_port),
                SpanEndpoint::End => (end_port, start_port),
            };
            let from = emitted_point(
                topology,
                coordinates,
                ordinates,
                step.span,
                step.owner,
                from_port,
            )?;
            let to = emitted_point(
                topology,
                coordinates,
                ordinates,
                step.span,
                step.owner,
                to_port,
            )?;
            let from_cut = q
                .ports
                .get(from_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;
            let to_cut = q
                .ports
                .get(to_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;

            if !from_cut {
                if let Some(previous) = previous {
                    if previous.node != from.node {
                        return Err(GeometryErrorKind::Invalid(
                            "Q line path changed quotient nodes between spans".into(),
                        )
                        .into());
                    }
                    if previous.point != from.point {
                        let previous_key =
                            emitted_node_key(topology.sources.frame, previous.point.xy())?;
                        let from_key = emitted_node_key(topology.sources.frame, from.point.xy())?;
                        if previous_key != from_key {
                            return Err(GeometryErrorKind::ExactNodeCollision {
                                first: previous.node.0,
                                second: from.node.0,
                            }
                            .into());
                        }
                        let is_seam = matches!(
                            topology
                                .sources
                                .frame
                                .longitude()
                                .canonicalize(from.point.x)
                                .map_err(|error| {
                                    GeometryErrorKind::Invalid(error.to_string().into())
                                })?
                                .identity(topology.sources.frame.longitude()),
                            SeamIdentity::Seam
                        );
                        let is_pole = matches!(
                            from_key,
                            EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
                        );
                        if is_seam || is_pole {
                            push_line_part(&mut output, &mut points)?;
                        } else if points.last().is_some_and(|last| *last != previous.point) {
                            return Err(GeometryErrorKind::Invalid(
                                "Q line endpoint aliases disagree away from a quotient singularity"
                                    .into(),
                            )
                            .into());
                        }
                    }
                }
                if points.last().is_none_or(|last| *last != from.point) {
                    points.push(from.point);
                }
            }
            if !to_cut {
                points.push(to.point);
            }
            previous = Some(to);
        }
        push_line_part(&mut output, &mut points)?;
    }
    Ok(output)
}

fn emit_points(
    topology: &ExactGeographicTopology,
    nodes: &[QNodeId],
    source_points: &[Option<Point>],
    coordinates: &QCoordinates,
) -> Result<Vec<Point>> {
    let mut output = Vec::with_capacity(nodes.len());
    for &node in nodes {
        output.push(emitted_point_for_node(
            topology,
            node,
            source_points,
            coordinates,
        )?);
    }
    Ok(output)
}

fn emitted_point_for_node(
    topology: &ExactGeographicTopology,
    node: QNodeId,
    source_points: &[Option<Point>],
    coordinates: &QCoordinates,
) -> Result<Point> {
    if let Some(source) = source_points.get(node.0 as usize).copied().flatten() {
        return Ok(source);
    }
    let node_data = topology
        .q
        .nodes
        .get(node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q point node is unknown".into()))?;
    let port_id = *topology
        .q
        .node_ports
        .get(node_data.ports.start)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q point node has no first port".into()))?;
    coordinates.point(topology, port_id)
}

fn emit_boundary_points(
    topology: &ExactGeographicTopology,
    nodes: &[QNodeId],
    source_points: &[Option<Point>],
    coordinates: &QCoordinates,
    ordinates: &[NodeOrdinates],
    axes: CoordinateAxes,
) -> Result<Option<Shape>> {
    if nodes.is_empty() {
        return Ok(None);
    }
    let mut output = CoordSeqBuilder::with_capacity(axes, nodes.len());
    for &node in nodes {
        let point = emitted_point_for_node(topology, node, source_points, coordinates)?;
        let source = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q point node has no ordinates".into()))?;
        output.push(with_axes(point, *source, axes)?);
    }
    Ok(Some(Shape::multi_point(axes, output.finish()?)))
}

fn source_point_coordinates(topology: &ExactGeographicTopology) -> Result<Vec<Option<Point>>> {
    let mut source_points = vec![None; topology.q.nodes.len()];
    for (source, &port) in topology
        .sources
        .points
        .iter()
        .zip(topology.q.source_point_ports.iter())
    {
        let node = topology
            .q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        source_points
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?
            .get_or_insert_with(|| {
                Point::new_raw_xy(
                    source.point.x,
                    Latitude::from_admitted(topology.sources.frame, source.point.y).get(),
                )
            });
    }
    Ok(source_points)
}

fn boundary_source_points(
    topology: &ExactGeographicTopology,
    row: SourceRowId,
) -> Result<Vec<Option<Point>>> {
    let q = &topology.q;
    let mut selected = Vec::with_capacity(q.nodes.len());
    selected.resize(q.nodes.len(), None::<(bool, SourceEdgeId, Point)>);
    for (span_index, span) in q.spans.iter().enumerate() {
        if span.nodes[0] == span.nodes[1] || !q.collapsed_rows_for_span(span_index)?.contains(&row)
        {
            continue;
        }
        for owner in &span.owners {
            let edge = source_edge_at(topology, owner.source_edge)?;
            if edge.row != row
                || !matches!(edge.kind, SourceEdgeKind::Ring {
                    kind: crate::geometry::RingKind::Collapsed,
                    ..
                })
            {
                continue;
            }
            let (start_parameter, end_parameter) = if owner.reversed {
                (&owner.source_range.end, &owner.source_range.start)
            } else {
                (&owner.source_range.start, &owner.source_range.end)
            };
            let [start_node, end_node] = span.nodes;
            for (node, parameter) in [(start_node, start_parameter), (end_node, end_parameter)] {
                // Prefer a stored source start at the node; its ring traversal spelling wins
                // over the canonical chart port and over an incoming edge's endpoint alias.
                let candidate = if parameter.is_zero() {
                    Some((false, edge.id, edge.source[0]))
                } else if parameter.is_one() {
                    Some((true, edge.id, edge.source[1]))
                } else {
                    None
                };
                let Some(candidate) = candidate else {
                    continue;
                };
                let stored = selected.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint has an unknown node".into())
                })?;
                if stored
                    .is_none_or(|previous| (candidate.0, candidate.1) < (previous.0, previous.1))
                {
                    *stored = Some(candidate);
                }
            }
        }
    }
    Ok(selected
        .into_iter()
        .map(|entry| entry.map(|(_, _, point)| point))
        .collect())
}

fn record_emitted_node(
    frame: crate::geographic::GeographicFrame,
    node: QNodeId,
    point: Point,
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    live.push((emitted_node_key(frame, point.xy())?, node));
    Ok(())
}

fn emitted_node_key(
    frame: crate::geographic::GeographicFrame,
    point: XY,
) -> Result<EmittedNodeKey> {
    let longitude = frame
        .longitude()
        .canonicalize(point.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let latitude = Latitude::try_new(frame, point.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    match latitude.chart_pole(frame) {
        Some(ChartPole::North) => Ok(EmittedNodeKey::NorthPole),
        Some(ChartPole::South) => Ok(EmittedNodeKey::SouthPole),
        None => Ok(EmittedNodeKey::Coordinate(
            canonical_bits(longitude.get()),
            canonical_bits(latitude.get()),
        )),
    }
}

const fn canonical_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn certify_node_injectivity(live: &mut Vec<(EmittedNodeKey, QNodeId)>) -> Result<()> {
    live.sort_unstable_by_key(|&(key, node)| (node, key));
    for pair in live.windows(2) {
        let [(left_key, left_node), (right_key, right_node)] = pair else {
            unreachable!("windows(2) yields adjacent emitted nodes");
        };
        if left_node == right_node && left_key != right_key {
            return Err(GeometryErrorKind::ExactNodeCollision {
                first: left_node.0,
                second: right_node.0,
            }
            .into());
        }
    }
    live.sort_unstable();
    let mut unique: Vec<(EmittedNodeKey, QNodeId)> = Vec::with_capacity(live.len());
    for &(key, node) in live.iter() {
        if let Some(&(previous_key, previous_node)) = unique.last() {
            if previous_key == key && previous_node != node {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: previous_node.0,
                    second: node.0,
                }
                .into());
            }
            if previous_key == key && previous_node == node {
                continue;
            }
        }
        unique.push((key, node));
    }
    *live = unique;
    Ok(())
}

fn with_axes(point: Point, ordinates: NodeOrdinates, axes: CoordinateAxes) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output Z has no source interval".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output M has no source interval".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

fn output_node_for_point(
    frame: crate::geographic::GeographicFrame,
    live: &[(EmittedNodeKey, QNodeId)],
    point: Point,
) -> Result<QNodeId> {
    let key = emitted_node_key(frame, point.xy())?;
    let index = live
        .binary_search_by_key(&key, |(candidate, _)| *candidate)
        .map_err(|_| {
            GeometryErrorKind::Invalid("Q output coordinate has no exact node identity".into())
        })?;
    live.get(index).map(|(_, node)| *node).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q output coordinate has no exact node identity".into()).into()
    })
}

fn apply_axes(
    frame: crate::geographic::GeographicFrame,
    ordinates: &[NodeOrdinates],
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
    points: &mut [Point],
    lines: &mut [CoordSeq],
    polygons: &mut [Polygon],
) -> Result<()> {
    for point in points {
        let node = output_node_for_point(frame, live, *point)?;
        let source = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q output point has no ordinates".into()))?;
        *point = with_axes(*point, *source, axes)?;
    }
    for line in lines {
        *line = line.try_map_points(|point| {
            let node = output_node_for_point(frame, live, point)?;
            let source = ordinates.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q output line has no ordinates".into())
            })?;
            with_axes(point, *source, axes)
        })?;
    }
    for polygon in polygons {
        *polygon = carry_polygon_axes(frame, polygon, ordinates, live, axes)?;
    }
    Ok(())
}

fn carry_polygon_axes(
    frame: crate::geographic::GeographicFrame,
    polygon: &Polygon,
    ordinates: &[NodeOrdinates],
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
) -> Result<Polygon> {
    let shell = carry_ring_axes(frame, &polygon.shell, ordinates, live, axes)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(carry_ring_axes(frame, hole, ordinates, live, axes)?);
    }
    Ok(Polygon::new(shell, holes))
}

fn carry_ring_axes(
    frame: crate::geographic::GeographicFrame,
    ring: &Ring,
    ordinates: &[NodeOrdinates],
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
) -> Result<Ring> {
    let mut coordinates = Vec::with_capacity(ring.coord_count());
    for point in ring.coords().iter_coords() {
        let node = output_node_for_point(frame, live, point)?;
        let source = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q output ring has no ordinates".into()))?;
        coordinates.push(with_axes(point, *source, axes)?);
    }
    Ring::try_from_closed(CoordSeq::try_from_points(&coordinates)?)
}

fn canonicalize_ring_vertices(
    frame: crate::geographic::GeographicFrame,
    vertices: &[EmittedVertex],
    chart_points: &mut [Point],
) -> Result<()> {
    if vertices.len() != chart_points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q chart ring vertices do not match their emitted nodes".into(),
        )
        .into());
    }
    let first_vertex = vertices
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an empty polygon ring".into()))?;
    let first_point = chart_points
        .first_mut()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an empty polygon ring".into()))?;
    let latitude = Latitude::try_new(frame, first_vertex.point.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    first_point.x = first_vertex.point.x;
    first_point.y = latitude.get();
    let mut reference = first_vertex.point.x;
    for (vertex, point) in vertices.iter().zip(chart_points.iter_mut()).skip(1) {
        let latitude = Latitude::try_new(frame, vertex.point.y)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        point.x = frame
            .longitude()
            .nearest_longitude_alias(reference, vertex.point.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        point.y = latitude.get();
        reference = point.x;
    }
    let first_vertex = vertices
        .first()
        .expect("an admitted ring has a first vertex");
    let last_vertex = vertices.last().expect("an admitted ring has a last vertex");
    if first_vertex.node != last_vertex.node {
        return Err(GeometryErrorKind::Invalid(
            "Q emitted a polygon ring with distinct endpoint nodes".into(),
        )
        .into());
    }
    let first_point = *chart_points
        .first()
        .expect("an admitted ring has a first chart point");
    *chart_points
        .last_mut()
        .expect("an admitted ring has a last chart point") = first_point;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::{AngularFactor, GeographicFrame, GeographicInputs};

    #[test]
    fn canonical_full_band_ring_uses_its_chart_orientation() {
        let Shape::Polygon(polygon) = crate::io::parse_wkt(
            "POLYGON ((-180 -80, -60 -80, 60 -80, -180 -80, \
             -180 80, 60 80, -60 80, -180 80, -180 -80))",
        )
        .unwrap() else {
            unreachable!()
        };
        let frame = GeographicFrame::crs84();
        assert!(!chart_ring_is_hole(frame, &polygon.shell).unwrap());
        assert!(chart_ring_is_hole(frame, &polygon.shell.reversed().unwrap()).unwrap());
    }

    fn exact_boundary(shape: &Shape) -> Shape {
        let frame = GeographicFrame::crs84();
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
            .expect("source admits");
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
        exact_geographic_boundary::<crate::crs::GeodesicMetric>(&topology, shape.axes(), None)
            .unwrap()
    }

    #[test]
    fn q_node_ordinates_return_xy_defaults_before_source_metric_access() {
        let frame = GeographicFrame::crs84();
        let seam_crossing = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(
                        170.0,
                        -10.0,
                        ZOrdinate(Some(-30.0)),
                        MOrdinate(Some(-300.0)),
                    )
                    .unwrap(),
                    Point::new_axes(-170.0, -10.0, ZOrdinate(Some(30.0)), MOrdinate(Some(300.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let inputs =
            GeographicInputs::try_from_shapes(frame, std::iter::once(&seam_crossing)).unwrap();
        assert_eq!(
            crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
            crate::geometry::GeographicTopologyRoute::Quotient
        );
        let topology =
            ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
                .unwrap();
        let metric_accesses = std::cell::Cell::new(0_usize);
        let no_metric = || -> Result<&crate::crs::GeodesicMetric> {
            metric_accesses.set(metric_accesses.get().wrapping_add(1));
            Err(crate::crs::CrsError::invalid("unexpected source-arc metric request").into())
        };

        let xy = node_ordinates(&topology, CoordinateAxes::XY, Some(&no_metric)).unwrap();
        assert!(
            xy.iter()
                .all(|ordinates| ordinates.z.value().is_none() && ordinates.m.value().is_none())
        );
        assert_eq!(xy.len(), topology.q.nodes.len());
        assert_eq!(metric_accesses.get(), 0);
    }

    type TestPoint = (u64, u64);
    type TestEdge = (TestPoint, TestPoint);

    fn edge(left: (f64, f64), right: (f64, f64)) -> TestEdge {
        let left = (left.0.to_bits(), left.1.to_bits());
        let right = (right.0.to_bits(), right.1.to_bits());
        if left <= right {
            (left, right)
        } else {
            (right, left)
        }
    }

    fn line_edges(shape: &Shape) -> Vec<TestEdge> {
        let mut edges = Vec::new();
        let mut append = |line: &CoordSeq| {
            for [start, end] in line.segment_pairs() {
                edges.push(edge((start.x, start.y), (end.x, end.y)));
            }
        };
        match shape {
            Shape::LineString(line) => append(line.as_coords()),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    append(line.as_coords());
                }
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => panic!("area boundary emits linework, got {shape:?}"),
        }
        edges.sort_unstable();
        edges
    }

    fn q_ring_chart_points(
        frame: GeographicFrame,
        coordinates: &[(f64, f64)],
        repeated_start_nodes: &[usize],
    ) -> Result<Vec<Point>> {
        let last = coordinates.len().wrapping_sub(1);
        let vertices = coordinates
            .iter()
            .enumerate()
            .map(|(index, &(longitude, latitude))| {
                let node = if index == 0 || index == last || repeated_start_nodes.contains(&index) {
                    QNodeId(0)
                } else {
                    QNodeId(u32::try_from(index).expect("fixed Q cycle fits its node IDs"))
                };
                EmittedVertex {
                    node,
                    port: QPortId(u32::try_from(index).expect("fixed Q cycle fits its port IDs")),
                    point: Point::new_raw_xy(longitude, latitude),
                    chart: ExactChartPoint::from_rationals(
                        ExactRational::from_f64(longitude),
                        ExactRational::from_f64(latitude),
                    ),
                }
            })
            .collect::<Vec<_>>();
        let mut chart_points = vertices
            .iter()
            .map(|vertex| vertex.point)
            .collect::<Vec<_>>();
        canonicalize_ring_vertices(frame, &vertices, &mut chart_points)?;
        Ok(chart_points)
    }

    #[test]
    fn emitted_longitude_reduces_chart_period_before_rounding() {
        let frame = GeographicFrame::crs84();
        let source = 122.427_194_f64;
        let period = ExactRational::from_f64(frame.longitude().period());
        let shifted = ExactRational::from_f64(source).try_add(&period).unwrap();
        let rounded_chart_alias = source + frame.longitude().period();

        assert_eq!(
            frame
                .longitude()
                .canonicalize(rounded_chart_alias)
                .unwrap()
                .get()
                .to_bits(),
            source.next_down().to_bits()
        );
        assert_eq!(
            round_canonical_longitude(frame, &shifted)
                .unwrap()
                .to_bits(),
            source.to_bits()
        );
    }

    #[test]
    fn subdivision_publication_rejects_distinct_exact_points_with_one_rounded_key() {
        let frame = GeographicFrame::crs84();
        let third = ExactRational::from_f64(1.0)
            .try_quotient(&ExactRational::from_f64(3.0))
            .unwrap();
        let original = ExactChartPoint::from_rationals(
            ExactRational::from_f64(1.0 / 3.0),
            ExactRational::zero(),
        );
        let inserted = ExactChartPoint::from_rationals(third, ExactRational::zero());
        assert_eq!(
            round_canonical_longitude(frame, &original.x).unwrap(),
            round_canonical_longitude(frame, &inserted.x).unwrap()
        );
        let error =
            certify_publication_identity(frame, &original, &inserted, QNodeId(7), QNodeId(8))
                .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactNodeCollision {
                first: 7,
                second: 8,
            })
        ));

        let shifted = ExactChartPoint::from_rationals(
            original
                .x
                .try_add(&ExactRational::from_f64(frame.longitude().period()))
                .unwrap(),
            original.y.clone(),
        );
        certify_publication_identity(frame, &original, &shifted, QNodeId(7), QNodeId(8)).unwrap();
    }

    #[test]
    fn boundary_keeps_the_valid_seam_component_in_a_multirow_union() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (\
             ((20 20,40 20,40 30,20 30,20 20)),\
             ((0 10,20 10,20 20,0 20,0 10)),\
             ((170 20,190 20,190 40,170 40,170 20))\
             )",
        )
        .expect("three-box multipolygon parses");
        let mut expected = vec![
            edge((0.0, 10.0), (20.0, 10.0)),
            edge((20.0, 10.0), (20.0, 20.0)),
            edge((20.0, 20.0), (0.0, 20.0)),
            edge((0.0, 20.0), (0.0, 10.0)),
            edge((20.0, 20.0), (40.0, 20.0)),
            edge((40.0, 20.0), (40.0, 30.0)),
            edge((40.0, 30.0), (20.0, 30.0)),
            edge((20.0, 30.0), (20.0, 20.0)),
            edge((170.0, 40.0), (170.0, 20.0)),
            edge((170.0, 20.0), (-180.0, 20.0)),
            edge((-180.0, 20.0), (-170.0, 20.0)),
            edge((-170.0, 20.0), (-170.0, 40.0)),
            edge((-170.0, 40.0), (-180.0, 40.0)),
            edge((-180.0, 40.0), (170.0, 40.0)),
        ];
        expected.sort_unstable();
        let actual = line_edges(&exact_boundary(&shape));
        assert_eq!(actual, expected);
        assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
    }

    #[test]
    fn boundary_keeps_a_pentagon_when_its_host_ray_hits_a_seam_box_node() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (\
             ((0 10,20 10,20 20,40 20,0 30,0 10)),\
             ((170 20,190 20,190 40,170 40,170 20))\
             )",
        )
        .expect("pentagon and seam box parse");
        let mut expected = vec![
            edge((0.0, 30.0), (0.0, 10.0)),
            edge((0.0, 10.0), (20.0, 10.0)),
            edge((20.0, 10.0), (20.0, 20.0)),
            edge((20.0, 20.0), (40.0, 20.0)),
            edge((40.0, 20.0), (0.0, 30.0)),
            edge((170.0, 40.0), (170.0, 20.0)),
            edge((170.0, 20.0), (-180.0, 20.0)),
            edge((-180.0, 20.0), (-170.0, 20.0)),
            edge((-170.0, 20.0), (-170.0, 40.0)),
            edge((-170.0, 40.0), (-180.0, 40.0)),
            edge((-180.0, 40.0), (170.0, 40.0)),
        ];
        expected.sort_unstable();
        let actual = line_edges(&exact_boundary(&shape));
        assert_eq!(actual, expected);
        assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
    }

    #[test]
    fn seam_only_collapsed_support_publishes_its_odd_endpoints() {
        let shape = crate::io::parse_wkt("POLYGON ((-180 -85,180 -85,180 85,-180 85,-180 -85))")
            .expect("seam-only polygon parses");
        let Shape::MultiPoint(points) = exact_boundary(&shape) else {
            panic!("seam-only boundary is multipoint");
        };
        let frame = GeographicFrame::crs84();
        let mut actual = points
            .iter_coords()
            .map(|point| {
                (
                    frame
                        .longitude()
                        .canonicalize(point.x)
                        .expect("emitted longitude is in the frame")
                        .get()
                        .to_bits(),
                    point.y.to_bits(),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = [
            ((-180.0_f64).to_bits(), (-85.0_f64).to_bits()),
            ((-180.0_f64).to_bits(), 85.0_f64.to_bits()),
        ];
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn q_ring_cycles_keep_the_lifted_alias_after_two_native_periods() {
        let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let cases: [(&str, GeographicFrame, &[f64], f64, f64); 3] = [
            (
                "degree",
                GeographicFrame::crs84(),
                [
                    122.427_194,
                    -177.572_806,
                    -117.572_806,
                    -57.572_806,
                    2.427_194,
                    62.427_194,
                ]
                .as_slice(),
                842.427_194,
                -597.572_806,
            ),
            (
                "grad",
                GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
                [
                    25.0,
                    75.0,
                    124.999_999_999_999_99,
                    174.999_999_999_999_97,
                    -174.999_999_999_999_97,
                    -124.999_999_999_999_97,
                    -74.999_999_999_999_99,
                    -24.999_999_999_999_993,
                ]
                .as_slice(),
                825.0,
                -775.0,
            ),
            (
                "radian",
                GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
                [
                    0.25,
                    1.035_398_163_397_448_3,
                    1.820_796_326_794_896_6,
                    2.606_194_490_192_345,
                    -2.891_592_653_589_793,
                    -2.106_194_490_192_345,
                    -1.320_796_326_794_896_6,
                    -0.535_398_163_397_448_3,
                ]
                .as_slice(),
                12.816_370_614_359_172,
                -12.316_370_614_359_172,
            ),
        ];

        for (name, frame, lap, positive, negative) in cases {
            for (direction, expected) in [(1.0, positive), (-1.0, negative)] {
                let path = if direction > 0.0 {
                    lap.to_vec()
                } else {
                    std::iter::once(lap[0])
                        .chain(lap[1..].iter().rev().copied())
                        .collect()
                };
                let mut coordinates = path
                    .iter()
                    .map(|&longitude| (longitude, 0.1))
                    .collect::<Vec<_>>();
                let repeated_start = coordinates.len();
                coordinates.push((lap[0], 0.1));
                coordinates.extend(path.iter().skip(1).map(|&longitude| (longitude, 0.1)));
                let high_latitude_vertex = coordinates.len();
                coordinates.push((lap[0], 0.2));
                coordinates.push((lap[0], 0.1));

                let chart_points =
                    q_ring_chart_points(frame, &coordinates, &[repeated_start]).unwrap();

                assert_eq!(
                    chart_points[high_latitude_vertex].x.to_bits(),
                    expected.to_bits(),
                    "{name} frame, direction {direction}"
                );
                assert_eq!(
                    chart_points.last().unwrap().x.to_bits(),
                    chart_points.first().unwrap().x.to_bits(),
                    "{name} frame closure"
                );
            }
        }
    }

    #[test]
    fn q_ring_half_period_ties_keep_each_native_seam_spelling() {
        let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let cases = [
            ("degree", GeographicFrame::crs84(), -180.0),
            (
                "grad positive seam",
                GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
                200.0,
            ),
            (
                "grad negative seam",
                GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
                -200.0,
            ),
            (
                "radian",
                GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
                -std::f64::consts::PI,
            ),
        ];

        for (name, frame, seam) in cases {
            let chart_points =
                q_ring_chart_points(frame, &[(0.0, 0.1), (seam, 0.1), (0.0, 0.1)], &[]).unwrap();
            assert_eq!(chart_points[1].x.to_bits(), seam.to_bits(), "{name} seam");
            assert_eq!(
                chart_points.last().unwrap().x.to_bits(),
                chart_points.first().unwrap().x.to_bits(),
                "{name} closure"
            );
        }
    }
}
