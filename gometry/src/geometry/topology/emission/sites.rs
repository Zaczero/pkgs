use std::cmp::Ordering;

use super::super::exact::ExactSourceRing;
use super::super::q::QPort;
use super::*;
use crate::geographic::SeamIdentity;
#[cfg(test)]
use crate::geometry::Unmetered;
use crate::geometry::XY;
use crate::geometry::exact_arith::rounded_native_neighbors_with_work;

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct QCoordinates {
    pub(in crate::geometry::topology) node: Vec<XY>,
    pub(in crate::geometry::topology) longitude_periods: Vec<i64>,
    pub(in crate::geometry::topology) pole_port_longitude: Vec<Option<f64>>,
    pub(in crate::geometry::topology) pole_port_periods: Vec<Option<i64>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::geometry::topology) struct QCandidate {
    pub(in crate::geometry::topology) native: XY,
    pub(in crate::geometry::topology) periods: i64,
}

impl QCoordinates {
    #[cfg(test)]
    pub(in crate::geometry::topology) fn build(topology: &ExactGeographicTopology) -> Result<Self> {
        let mut work = Unmetered;
        Self::build_with_work(topology, &mut work)
    }

    pub(in crate::geometry::topology) fn build_with_work(
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<Self> {
        let frame = topology.sources.frame;
        let q = &topology.q;
        work.charge_into_error(
            u64::try_from(
                q.ports
                    .len()
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut node = Vec::with_capacity(q.nodes.len());
        let mut longitude_periods = Vec::with_capacity(q.nodes.len());
        let mut pole_port_longitude = vec![None; q.ports.len()];
        let mut pole_port_periods = vec![None; q.ports.len()];
        for (node_index, q_node) in q.nodes.iter().enumerate() {
            work.charge_into_error(1)?;
            if q_node.id.0 as usize != node_index {
                return Err(GeometryErrorKind::Invalid(
                    "Q node coordinates are not indexed by QNodeId".into(),
                )
                .into());
            }
            let (longitude, periods, latitude) = match topology.canonical_node_key(q_node.id)? {
                super::super::q::CanonicalQNodeKey::Chart(key) => match key {
                    ExactQuotientKey::Coordinate(point) => {
                        let [_, nearest, _] =
                            rounded_native_neighbors_with_work(frame.longitude(), &point.x, work)?;
                        (
                            nearest.native,
                            nearest.periods,
                            point.y.round_with_work(work)?,
                        )
                    },
                    ExactQuotientKey::Seam(latitude) => (
                        -frame.longitude().half_period(),
                        0,
                        latitude.round_with_work(work)?,
                    ),
                    ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => {
                        let first_port_id =
                            *q.node_ports.get(q_node.ports.start).ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole has no first port".into())
                            })?;
                        let first_port =
                            q.ports.get(first_port_id.0 as usize).ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole port is unknown".into())
                            })?;
                        let [_, nearest, _] = rounded_native_neighbors_with_work(
                            frame.longitude(),
                            &first_port.point.x,
                            work,
                        )?;
                        (
                            nearest.native,
                            nearest.periods,
                            first_port.point.y.round_with_work(work)?,
                        )
                    },
                },
                super::super::q::CanonicalQNodeKey::Cut {
                    longitude,
                    latitude,
                } => {
                    let [_, nearest, _] =
                        rounded_native_neighbors_with_work(frame.longitude(), longitude, work)?;
                    (
                        nearest.native,
                        nearest.periods,
                        latitude.round_with_work(work)?,
                    )
                },
            };
            let latitude = Latitude::try_new(frame, latitude)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            let xy = XY::new(longitude, latitude.get());
            let is_pole = matches!(
                emitted_node_key(frame, xy)?,
                EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
            );
            node.push(xy);
            longitude_periods.push(periods);
            if is_pole {
                for &port_id in q.node_ports.get(q_node.ports.clone()).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node ports are incomplete".into())
                })? {
                    work.charge_into_error(1)?;
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q pole port is unknown".into())
                    })?;
                    let [_, rounded, _] =
                        rounded_native_neighbors_with_work(frame.longitude(), &port.point.x, work)?;
                    *pole_port_longitude
                        .get_mut(port_id.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q pole port coordinate is unknown".into())
                        })? = Some(rounded.native);
                    *pole_port_periods
                        .get_mut(port_id.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q pole port lift is unknown".into())
                        })? = Some(rounded.periods);
                }
            }
        }
        Ok(Self {
            node,
            longitude_periods,
            pole_port_longitude,
            pole_port_periods,
        })
    }

    pub(in crate::geometry::topology) fn point(
        &self,
        topology: &ExactGeographicTopology,
        port_id: QPortId,
    ) -> Result<Point> {
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

    pub(in crate::geometry::topology) fn xy(&self, node: QNodeId) -> Result<XY> {
        self.node.get(node.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q emitted node has no coordinates".into()).into()
        })
    }

    pub(in crate::geometry::topology) fn candidate(&self, node: QNodeId) -> Result<QCandidate> {
        let index = node.0 as usize;
        Ok(QCandidate {
            native: self.xy(node)?,
            periods: *self.longitude_periods.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has no retained longitude lift".into())
            })?,
        })
    }

    #[cfg(test)]
    pub(in crate::geometry::topology) fn has_inexact_nodes(
        &self,
        topology: &ExactGeographicTopology,
        nodes: impl IntoIterator<Item = QNodeId>,
    ) -> Result<bool> {
        let mut work = Unmetered;
        self.has_inexact_nodes_with_work(topology, nodes, &mut work)
    }

    pub(in crate::geometry::topology) fn has_inexact_nodes_with_work(
        &self,
        topology: &ExactGeographicTopology,
        nodes: impl IntoIterator<Item = QNodeId>,
        work: &mut impl Work,
    ) -> Result<bool> {
        for node in nodes {
            work.charge_into_error(1)?;
            let nearest = self.xy(node)?;
            let key = match topology.canonical_node_key(node)? {
                super::super::q::CanonicalQNodeKey::Chart(key) => key,
                super::super::q::CanonicalQNodeKey::Cut { .. } => continue,
            };
            let matches = match key {
                ExactQuotientKey::Coordinate(point) => {
                    exact_coordinate_matches_with_work(&point.x, nearest.x, work)?
                        && exact_coordinate_matches_with_work(&point.y, nearest.y, work)?
                },
                ExactQuotientKey::Seam(latitude) => {
                    nearest.x.to_bits()
                        == (-topology.sources.frame.longitude().half_period()).to_bits()
                        && exact_coordinate_matches_with_work(latitude, nearest.y, work)?
                },
                ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => true,
            };
            if !matches {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(in crate::geometry::topology) fn exact_coordinate_matches_with_work(
    value: &ExactRational,
    coordinate: f64,
    work: &mut impl Work,
) -> Result<bool> {
    let (low, high) = value.outward_bounds_with_work(work)?;
    if low.partial_cmp(&high) == Some(Ordering::Equal) {
        Ok(low.partial_cmp(&coordinate) == Some(Ordering::Equal))
    } else {
        Ok(value
            .try_cmp_with_work(&ExactRational::from_f64(coordinate), work)?
            .is_eq())
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct EmittedVertex<'topology> {
    pub(in crate::geometry::topology) port: &'topology QPort,
    pub(in crate::geometry::topology) point: Point,
    pub(in crate::geometry::topology) literal_image: QSourceImage,
}

pub(in crate::geometry::topology) fn canonicalize_ring_vertices(
    frame: GeographicFrame,
    vertices: &[EmittedVertex<'_>],
    chart_points: &mut [Point],
    work: &mut impl Work,
) -> Result<bool> {
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
    let axis = frame.longitude();
    let mut reference = first_vertex.point.x;
    let mut previous = first_vertex.point.x;
    let mut has_circle = false;
    for (vertex, point) in vertices.iter().zip(chart_points.iter_mut()).skip(1) {
        work.charge_into_error(1)?;
        if (vertex.point.x - previous).abs() >= axis.period() {
            has_circle |= axis
                .chart_step(previous, vertex.point.x)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                .is_circle();
        }
        previous = vertex.point.x;
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
    if first_vertex.port.node != last_vertex.port.node {
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
    Ok(has_circle || (reference - first_point.x).abs() > axis.half_period())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) enum EmittedNodeKey {
    Coordinate(u64, u64),
    NorthPole,
    SouthPole,
}

pub(in crate::geometry::topology) const fn canonical_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

pub(in crate::geometry::topology) fn emitted_node_key(
    frame: GeographicFrame,
    point: XY,
) -> Result<EmittedNodeKey> {
    let longitude = frame
        .longitude()
        .canonicalize(point.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let latitude = Latitude::try_new(frame, point.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    match latitude.identity(frame) {
        SeamIdentity::Pole if latitude.get().is_sign_positive() => Ok(EmittedNodeKey::NorthPole),
        SeamIdentity::Pole => Ok(EmittedNodeKey::SouthPole),
        SeamIdentity::Ordinary | SeamIdentity::Seam => Ok(EmittedNodeKey::Coordinate(
            canonical_bits(longitude.get()),
            canonical_bits(latitude.get()),
        )),
    }
}

#[cfg(test)]
pub(in crate::geometry::topology) fn node_injectivity_collisions(
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Vec<QNodeId>> {
    let mut work = Unmetered;
    node_injectivity_collisions_with_work(live, &mut work)
}

pub(in crate::geometry::topology) fn node_injectivity_collisions_with_work(
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
    work: &mut impl Work,
) -> Result<Vec<QNodeId>> {
    try_sort_by_with_work(live, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
    })?;
    for pair in live.windows(2) {
        work.charge_into_error(1)?;
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
    try_sort_by_with_work(live, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.cmp(right))
    })?;
    let mut collisions = Vec::new();
    for pair in live.windows(2) {
        work.charge_into_error(1)?;
        let [(left_key, left_node), (right_key, right_node)] = pair else {
            unreachable!("windows(2) yields adjacent emitted nodes");
        };
        if left_key == right_key && left_node != right_node {
            collisions.extend([*left_node, *right_node]);
        }
    }
    try_sort_by_with_work(&mut collisions, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.cmp(right))
    })?;
    work.charge_into_error(
        u64::try_from(collisions.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    collisions.dedup();
    if collisions.is_empty() {
        work.charge_into_error(
            u64::try_from(live.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        live.dedup();
    }
    Ok(collisions)
}

#[cfg(test)]
pub(in crate::geometry::topology) fn certify_node_injectivity(
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    if !node_injectivity_collisions(live)?.is_empty() {
        for pair in live.windows(2) {
            let [(left_key, left_node), (right_key, right_node)] = pair else {
                unreachable!("windows(2) yields adjacent emitted nodes");
            };
            if left_key == right_key && left_node != right_node {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: left_node.0,
                    second: right_node.0,
                }
                .into());
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct QCycleVertex {
    pub(in crate::geometry::topology) port: QPortId,
    pub(in crate::geometry::topology) span: usize,
}

pub(in crate::geometry::topology) fn for_each_q_cycle_vertex<W: Work>(
    topology: &ExactGeographicTopology,
    darts: &[QDartId],
    mut visit: impl FnMut(QCycleVertex, &mut W) -> Result<()>,
    work: &mut W,
) -> Result<()> {
    let q = &topology.q;
    let mut pivot = 0;
    for (index, &dart_id) in darts.iter().enumerate() {
        work.charge_into_error(1)?;
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
    let (before_pivot, from_pivot) = darts.split_at(pivot);
    let mut first = None;
    let mut previous: Option<QCycleVertex> = None;
    let mut count = 0_usize;
    for &dart_id in from_pivot.iter().chain(before_pivot) {
        work.charge_into_error(1)?;
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        for port_id in [dart.from_port, dart.to_port] {
            work.charge_into_error(1)?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?;
            if !port.cut && previous.is_none_or(|previous| previous.port != port_id) {
                let vertex = QCycleVertex {
                    port: port_id,
                    span: dart.span.0 as usize,
                };
                visit(vertex, work)?;
                first.get_or_insert(vertex);
                previous = Some(vertex);
                count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
    }
    if count < 3 {
        return Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has fewer than three emitted vertices".into(),
        )
        .into());
    }
    let first = first.expect("the boundary has vertices");
    if previous.is_some_and(|last| last.port != first.port) {
        work.charge_into_error(1)?;
        visit(first, work)?;
    }
    Ok(())
}

pub(in crate::geometry::topology) fn source_ring_emitted_vertices<'topology>(
    topology: &'topology ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    reverse: bool,
    work: &mut impl Work,
) -> Result<Vec<EmittedVertex<'topology>>> {
    work.charge_into_error(
        u64::try_from(source_ring.source_edges.len().saturating_add(1))
            .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let capacity = source_ring.source_edges.len().saturating_add(1);
    let mut vertices = Vec::with_capacity(capacity);
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let point = edge.source[0].with_xy_raw(edge.source[0].x, edge.source[0].y);
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
        let [start_port, _] = *ports;
        let port =
            topology.q.ports.get(start_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring port is unknown".into())
            })?;
        vertices.push(EmittedVertex {
            port,
            point,
            literal_image: QSourceImage::SourceRingStart(edge.id),
        });
    }
    let last_edge = source_ring
        .source_edges
        .end
        .checked_sub(1)
        .and_then(|index| topology.sources.edges.get(index))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring has no edges".into()))?;
    let point = last_edge.source[1].with_xy_raw(last_edge.source[1].x, last_edge.source[1].y);
    let last_ports = topology
        .q
        .source_edge_ports
        .get(last_edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
    let [_, end_port] = *last_ports;
    let last_port = topology
        .q
        .ports
        .get(end_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring port is unknown".into()))?;
    vertices.push(EmittedVertex {
        port: last_port,
        point,
        literal_image: QSourceImage::SourceRingEnd(last_edge.id),
    });
    if reverse {
        vertices.reverse();
    }
    Ok(vertices)
}

pub(in crate::geometry::topology) fn record_emitted_node(
    frame: crate::geographic::GeographicFrame,
    node: QNodeId,
    point: Point,
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    live.push((emitted_node_key(frame, point.xy())?, node));
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

pub(in crate::geometry::topology) fn with_vertex_axes(
    vertex: EmittedVertex<'_>,
    ordinates: &[NodeOrdinates],
    axes: CoordinateAxes,
) -> Result<Point> {
    let source = ordinates
        .get(vertex.port.node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q output node has no ordinates".into()))?;
    with_axes(vertex.point, *source, axes)
}
