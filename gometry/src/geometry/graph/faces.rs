//! Generic graph face linking and component-host construction.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{IndexedTopology, compare_departures};
use crate::geometry::graph::{
    RayAxis, RayPoints, RayPointsWithWork, compare_departures_with_work, shoot_left,
    shoot_left_with_work,
};
use crate::geometry::{GeometryErrorKind, XY};
use crate::work::Work;

pub(in crate::geometry) struct GlobalFaces {
    pub(in crate::geometry) walk_faces: Vec<u32>,
    pub(in crate::geometry) component_hosts: Vec<u32>,
    pub(in crate::geometry) count: u32,
}

fn graph_edge_departure<E, W>(edge_nodes: &E, edge: usize, origin: u32, work: &mut W) -> Result<u32>
where
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    work.charge_into_error(1)?;
    let [start, end] = edge_nodes(edge)?;
    if start == origin {
        Ok(end)
    } else if end == origin {
        Ok(start)
    } else {
        Err(
            GeometryErrorKind::Invalid("graph departure edge is not incident to its origin".into())
                .into(),
        )
    }
}

fn graph_contact_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    edge: usize,
    work: &mut W,
) -> Result<u32>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    work.charge_into_error(1)?;
    let [start, end] = edge_nodes(edge)?;
    let upper = match points.compare_axis_with_work(start, end, RayAxis::Y, work)? {
        Ordering::Less => end,
        Ordering::Greater => start,
        Ordering::Equal => {
            return Err(
                GeometryErrorKind::Invalid("leftward ray contact is horizontal".into()).into(),
            );
        },
    };
    let start = *topology.starts.get(upper as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("leftward ray contact node has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (upper as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("leftward ray contact row is incomplete".into())
        })? as usize;
    for slot in start..end {
        work.charge_into_error(1)?;
        if edge_of_slot.get(slot).copied()
            == Some(u32::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?)
        {
            let face = *topology.face_of.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("leftward ray contact has no face walk".into())
            })?;
            return face
                .checked_add(1)
                .ok_or_else(|| ResourceError::SizeOverflow.into());
        }
    }
    Err(
        GeometryErrorKind::Invalid("leftward ray contact edge has no upper-end departure".into())
            .into(),
    )
}

fn graph_exterior_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    root: u32,
    work: &mut W,
) -> Result<Option<u32>>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work,
{
    let start = *topology.starts.get(root as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("graph component root has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (root as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component root row is incomplete".into())
        })? as usize;
    let mut chosen = None;
    let mut has_upper = false;
    for slot in start..end {
        work.charge_into_error(1)?;
        let target = *topology.targets.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component departure has no target".into())
        })?;
        work.charge_into_error(1)?;
        let upper = !points
            .compare_axis_with_work(target, root, RayAxis::Y, work)?
            .is_lt();
        if chosen.is_none() || (upper && !has_upper) {
            chosen = Some(slot);
            has_upper = upper;
            continue;
        }
        if upper != has_upper {
            continue;
        }
        let previous = chosen.ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan lost its chosen departure".into())
        })?;
        let previous_edge = *edge_of_slot.get(previous).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let current_edge = *edge_of_slot.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let previous_target = graph_edge_departure(edge_nodes, previous_edge, root, work)?;
        let current_target = graph_edge_departure(edge_nodes, current_edge, root, work)?;
        if compare_departures_with_work(points, root, previous_target, current_target, work)?
            == Ordering::Less
        {
            chosen = Some(slot);
        }
    }
    chosen
        .map(|slot| {
            topology.face_of.get(slot).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior departure has no face".into()).into()
            })
        })
        .transpose()
}

pub(in crate::geometry) fn graph_global_faces_with_work<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_count: usize,
    edge_nodes: E,
    edge_of_slot: &[u32],
    work: &mut W,
) -> Result<GlobalFaces>
where
    R: RayPointsWithWork + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]>,
    W: Work,
{
    let node_count = topology.component_of.len();
    let slots = edge_count
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    if edge_of_slot.len() != slots
        || topology.targets.len() != slots
        || topology.face_of.len() != slots
        || topology.starts.len()
            != node_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
    {
        return Err(GeometryErrorKind::Invalid(
            "graph face inputs do not share one indexed topology".into(),
        )
        .into());
    }
    work.charge_into_error(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut component_count = 0_usize;
    for &component in &topology.component_of {
        work.charge_into_error(1)?;
        component_count = component_count.max(
            usize::try_from(component)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut minima = vec![None; component_count];
    for (node, &component) in topology.component_of.iter().enumerate() {
        work.charge_into_error(1)?;
        let component = usize::try_from(component).map_err(|_| ResourceError::SizeOverflow)?;
        let minimum = minima.get_mut(component).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component ID is unknown".into())
        })?;
        let node = u32::try_from(node).map_err(|_| ResourceError::SizeOverflow)?;
        if let Some(previous) = *minimum {
            work.charge_into_error(1)?;
            let x = points.compare_axis_with_work(node, previous, RayAxis::X, work)?;
            let order = if x.is_eq() {
                work.charge_into_error(1)?;
                points.compare_axis_with_work(node, previous, RayAxis::Y, work)?
            } else {
                x
            };
            if order.is_lt() {
                *minimum = Some(node);
            }
        } else {
            *minimum = Some(node);
        }
    }
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut roots = Vec::with_capacity(component_count);
    for minimum in minima {
        work.charge_into_error(1)?;
        roots.push(minimum.ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component has no root node".into())
        })?);
    }

    let walks = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
        GeometryErrorKind::Invalid("indexed graph face starts have no initial sentinel".into())
    })?;
    let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let limit = u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut links: Vec<u32> = (0..limit).collect();
    work.charge_into_error(
        u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut component_hosts = Vec::with_capacity(component_count);
    let hits = if roots.len() == 1 {
        work.charge_into_error(1)?;
        vec![None]
    } else {
        shoot_left_with_work(points, edge_count, &edge_nodes, &roots, work)?
    };
    if hits.len() != roots.len() {
        return Err(GeometryErrorKind::Invalid(
            "leftward ray sweep returned an incomplete component-host table".into(),
        )
        .into());
    }
    for (&root, hit) in roots.iter().zip(hits) {
        work.charge_into_error(1)?;
        let host = if let Some(edge) = hit {
            graph_contact_walk(points, topology, edge_of_slot, &edge_nodes, edge, work)?
        } else {
            0
        };
        component_hosts.push(host);
        if let Some(exterior) =
            graph_exterior_walk(points, topology, edge_of_slot, &edge_nodes, root, work)?
        {
            let link = usize::try_from(exterior)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            *links.get_mut(link).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior face link is unknown".into())
            })? = host;
        }
    }

    for start in 1..size {
        work.charge_into_error(1)?;
        let mut root = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge_into_error(1)?;
            let parent = *links.get(root as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            root = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
        let mut node = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge_into_error(1)?;
            let parent = *links.get(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            *links.get_mut(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })? = root;
            node = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
    }

    work.charge_into_error(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut labels = vec![0_u32; size];
    let mut count = 1_u32;
    for node in 1..size {
        work.charge_into_error(1)?;
        let link = *links.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph face link is outside its table".into())
        })?;
        if link == node as u32 {
            *labels.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face label is outside its table".into())
            })? = count;
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for host in &mut component_hosts {
        work.charge_into_error(1)?;
        let root = *links.get(*host as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host is outside its link table".into())
        })?;
        *host = *labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host has no global label".into())
        })?;
    }
    work.charge_into_error(u64::try_from(walks).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut walk_faces = Vec::with_capacity(walks);
    for &root in links.iter().skip(1) {
        work.charge_into_error(1)?;
        walk_faces.push(*labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph local face has no global label".into())
        })?);
    }
    Ok(GlobalFaces {
        walk_faces,
        component_hosts,
        count,
    })
}

/// Compare each emitted component's local host with the face east of its
/// leftward ray hit. `sides(edge)` gives the exact face on the left and right
/// of the directed edge returned by `edge`.
#[expect(
    clippy::indexing_slicing,
    clippy::float_cmp,
    reason = "emitted node IDs index this face graph; its face-side labels and edge ordinals are paired"
)]
pub(in crate::geometry) fn visit_component_faces<
    R: RayPoints + ?Sized,
    E: Fn(usize) -> [u32; 2],
    F: Fn(usize) -> [u32; 2],
    V: FnMut(u32, Option<usize>) -> ControlFlow<()>,
>(
    points: &R,
    node_count: usize,
    edge_count: usize,
    edge: E,
    sides: F,
    mut visit: V,
) -> Result<ControlFlow<()>> {
    let incidence_rows = node_count.wrapping_add(1);
    let mut incident_starts = vec![0_usize; incidence_rows];
    let mut parents = Vec::with_capacity(node_count);
    parents.extend(0..node_count as u32);
    for index in 0..edge_count {
        let [start, end] = edge(index);
        let start_node = start as usize;
        let start_offset = start_node.wrapping_add(1);
        incident_starts[start_offset] = incident_starts[start_offset].wrapping_add(1);
        if end != start {
            let end_node = end as usize;
            let end_offset = end_node.wrapping_add(1);
            incident_starts[end_offset] = incident_starts[end_offset].wrapping_add(1);
        }
        let mut left = start;
        let mut right = end;
        while parents[left as usize] != left {
            parents[left as usize] = parents[parents[left as usize] as usize];
            left = parents[left as usize];
        }
        while parents[right as usize] != right {
            parents[right as usize] = parents[parents[right as usize] as usize];
            right = parents[right as usize];
        }
        parents[left.max(right) as usize] = left.min(right);
    }
    prefix_contact_offsets(&mut incident_starts);
    let incident_count = incident_starts[node_count];
    let mut incident = vec![0_usize; incident_count];
    {
        let mut cursor = Vec::with_capacity(node_count);
        cursor.extend(incident_starts.iter().take(node_count).copied());
        for index in 0..edge_count {
            let [start, end] = edge(index);
            for node in [start, end]
                .into_iter()
                .take(if end == start { 1 } else { 2 })
            {
                let node = node as usize;
                let position = cursor[node];
                incident[position] = index;
                cursor[node] = position.wrapping_add(1);
            }
        }
    }
    let mut roots: Vec<Option<(usize, XY)>> = Vec::with_capacity(node_count);
    roots.resize(node_count, None);
    for node in 0..node_count {
        if incident_starts[node] == incident_starts[node.wrapping_add(1)] {
            continue;
        }
        let mut root = node as u32;
        while parents[root as usize] != root {
            root = parents[root as usize];
        }
        let point = points.rounded(node as u32);
        let entry = &mut roots[root as usize];
        if entry.is_none_or(|(_, prior): (usize, XY)| {
            point.x < prior.x || (point.x == prior.x && point.y < prior.y)
        }) {
            *entry = Some((node, point));
        }
    }
    drop(parents);

    let mut queries = Vec::new();
    let mut exteriors = Vec::new();
    for &(node, point) in roots.iter().flatten() {
        let mut exterior = None;
        let mut chosen = None;
        let mut has_upper = false;
        let range = incident_starts[node]..incident_starts[node.wrapping_add(1)];
        for &index in &incident[range] {
            let [start, end] = edge(index);
            let (target, target_point, face) = if start as usize == node {
                (end, points.rounded(end), sides(index)[0])
            } else {
                (start, points.rounded(start), sides(index)[1])
            };
            if target as usize == node {
                exterior = Some(face);
                continue;
            }
            let upper = target_point.y >= point.y;
            if chosen.is_none()
                || (upper && !has_upper)
                || (upper == has_upper && {
                    let (prior_target, prior_point) =
                        chosen.expect("a selected departure exists before comparing the next one");
                    compare_departures(point, prior_point, prior_target, target_point, target)
                        == Ordering::Less
                })
            {
                chosen = Some((target, target_point));
                exterior = Some(face);
                has_upper = upper;
            }
        }
        queries.push(node as u32);
        exteriors.push(exterior.expect("a component root has an incident emitted edge"));
    }
    drop(roots);
    drop(incident);
    drop(incident_starts);

    let hits = shoot_left(points, edge_count, &edge, &queries)?;
    for ((&root, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
        let host = if let Some(index) = hit {
            let [start, end] = edge(index);
            if points.compare_axis(start, end, RayAxis::Y)?.is_lt() {
                sides(index)[1]
            } else {
                sides(index)[0]
            }
        } else {
            0
        };
        if host != exterior && visit(root, hit).is_break() {
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Convert per-segment contact counts into native-vector offsets in place.
pub(in crate::geometry) fn prefix_contact_offsets(offsets: &mut [usize]) {
    let Some((first, rest)) = offsets.split_first_mut() else {
        return;
    };
    let mut total = *first;
    for offset in rest {
        total = total.wrapping_add(*offset);
        *offset = total;
    }
}
