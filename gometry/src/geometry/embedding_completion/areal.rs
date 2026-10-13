use std::collections::BTreeSet;

use super::admission::{AdmittedGraph, indexed_topology};
use super::layout::LogicalLayout;
use super::points::ExactPoints;
use super::{ResourceError, Result, Work};
use crate::geometry::GeometryErrorKind;
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::graph::{GlobalFaces, graph_global_faces_with_work};
use crate::geometry::segments::ordering::try_sort_by_with_work;

/// Stable identities needed by a candidate proof survive the wall view. Its
/// indexed CSR and `GlobalFaces` are temporary admission/candidate data.
pub(super) struct ArealSignature {
    pub(super) face_keys: Vec<Vec<u32>>,
    pub(super) exterior_face: u32,
    pub(super) host_by_vertex: Vec<u32>,
    pub(super) fan_rows: Vec<(u32, Vec<u32>)>,
}

pub(in crate::geometry) struct ArealProof {
    /// Stable IDs in the producer's complete canonical atom inventory.
    pub(super) wall_atom_ids: Vec<u32>,
    pub(super) signature: ArealSignature,
}

pub(super) fn stable_areal_signatures_match(
    reference: &ArealSignature,
    candidate: &ArealSignature,
) -> bool {
    reference.face_keys == candidate.face_keys
        && reference.exterior_face == candidate.exterior_face
        && reference.host_by_vertex == candidate.host_by_vertex
        && reference.fan_rows == candidate.fan_rows
}

fn wall_covers_graph(wall_atom_ids: &[u32], atom_count: usize) -> bool {
    wall_atom_ids.len() == atom_count
        && wall_atom_ids
            .iter()
            .enumerate()
            .all(|(index, &atom)| usize::try_from(atom).ok() == Some(index))
}

/// Build global faces for a previously admitted areal-wall projection only.
fn global_faces(
    points: &ExactPoints,
    topology: &IndexedTopology,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    work: &mut impl Work,
) -> Result<GlobalFaces> {
    graph_global_faces_with_work(
        points,
        topology,
        edge_nodes.len(),
        |edge| {
            edge_nodes.get(edge).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph face edge is unknown".into()).into()
            })
        },
        edge_of_slot,
        work,
    )
}

fn areal_vertices(
    layout: &LogicalLayout,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<Vec<u32>> {
    let mut vertices = BTreeSet::new();
    for walk in &layout.areal_walks {
        for use_row in &walk.uses {
            work.charge_into_error(1)?;
            let [start, end] = *edge_nodes.get(use_row.atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("admitted areal use has an unknown atom".into())
            })?;
            vertices.insert(if use_row.reversed { end } else { start });
            vertices.insert(if use_row.reversed { start } else { end });
        }
    }
    Ok(vertices.into_iter().collect())
}

fn stable_fan_rows(
    topology: &IndexedTopology,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    vertices: &[u32],
    work: &mut impl Work,
) -> Result<Vec<(u32, Vec<u32>)>> {
    let vertex_count = topology.component_of.len();
    if topology.starts.len()
        != vertex_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?
        || edge_of_slot.len() != topology.targets.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "stable fan correspondence received inconsistent topology tables".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut rotations = Vec::with_capacity(vertices.len());
    for &vertex in vertices {
        work.charge_into_error(1)?;
        if vertex as usize >= vertex_count {
            return Err(GeometryErrorKind::Invalid(
                "areal fan references an unknown graph vertex".into(),
            )
            .into());
        }
        let start = *topology.starts.get(vertex as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable graph rotation start is missing".into())
        })? as usize;
        let end_offset = (vertex as usize)
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let end = *topology.starts.get(end_offset).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable graph rotation end is missing".into())
        })? as usize;
        let degree = end.checked_sub(start).ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(u64::try_from(degree).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut rotation = Vec::with_capacity(degree);
        for slot in start..end {
            work.charge_into_error(1)?;
            let edge = *edge_of_slot.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart has no atom identity".into())
            })?;
            let endpoints = edge_nodes.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart atom is unknown".into())
            })?;
            let origin = *topology.owners.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable graph dart has no origin".into())
            })?;
            let direction = if endpoints[0] == origin {
                0_u32
            } else if endpoints[1] == origin {
                1_u32
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "stable graph dart origin disagrees with its atom".into(),
                )
                .into());
            };
            let dart = edge
                .checked_mul(2)
                .and_then(|value| value.checked_add(direction))
                .ok_or(ResourceError::SizeOverflow)?;
            rotation.push(dart);
        }
        if !rotation.is_empty() {
            let anchor = rotation
                .iter()
                .enumerate()
                .min_by_key(|(_, dart)| *dart)
                .map(|(index, _)| index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("stable graph rotation has no anchor".into())
                })?;
            rotation.rotate_left(anchor);
        }
        rotations.push(rotation);
    }
    Ok(vertices.iter().copied().zip(rotations).collect())
}

fn stable_wall_face_keys(
    topology: &IndexedTopology,
    faces: &GlobalFaces,
    edge_nodes: &[[u32; 2]],
    edge_of_slot: &[u32],
    stable_edge_ids: &[u32],
    work: &mut impl Work,
) -> Result<(Vec<Vec<u32>>, Vec<u32>)> {
    let walk_count = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "stable areal wall correspondence has inconsistent face tables".into(),
        )
    })?;
    if faces.walk_faces.len() != walk_count {
        return Err(GeometryErrorKind::Invalid(
            "stable areal wall correspondence has inconsistent face tables".into(),
        )
        .into());
    }
    let face_count = usize::try_from(faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_keys = vec![Vec::<u32>::new(); face_count];
    for walk in 0..walk_count {
        let start =
            *topology.face_starts.get(walk).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk start is missing".into())
            })? as usize;
        let next_walk = walk.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let end =
            *topology.face_starts.get(next_walk).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk end is missing".into())
            })? as usize;
        let slots = topology.face_slots.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable face walk slot range is invalid".into())
        })?;
        if slots.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "stable face correspondence contains an empty boundary walk".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(slots.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut anchor = u32::MAX;
        for &slot in slots {
            let edge = *edge_of_slot.get(slot as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no atom identity".into())
            })?;
            let stable_edge = *stable_edge_ids.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no stable atom ID".into())
            })?;
            let origin = *topology.owners.get(slot as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart has no origin".into())
            })?;
            let endpoints = edge_nodes.get(edge as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face dart atom is unknown".into())
            })?;
            let direction = if endpoints[0] == origin {
                0_u32
            } else if endpoints[1] == origin {
                1_u32
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "stable face dart origin disagrees with its atom".into(),
                )
                .into());
            };
            anchor = anchor.min(
                stable_edge
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(direction))
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }
        let global_face = *faces.walk_faces.get(walk).ok_or_else(|| {
            GeometryErrorKind::Invalid("stable face walk has no global face".into())
        })? as usize;
        face_keys
            .get_mut(global_face)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("stable face walk names an unknown face".into())
            })?
            .push(anchor);
    }
    for key in &mut face_keys {
        try_sort_by_with_work(key, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
    }
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut face_order = (0..face_count).collect::<Vec<_>>();
    try_sort_by_with_work(&mut face_order, work, |left, right, work| {
        work.charge_into_error(1)?;
        let left = face_keys
            .get(*left)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable left face key is unknown".into()))?;
        let right = face_keys
            .get(*right)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable right face key is unknown".into()))?;
        Ok(left.cmp(right))
    })?;
    for pair in face_order.array_windows::<2>() {
        work.charge_into_error(1)?;
        let [left, right] = pair;
        let left_key = face_keys
            .get(*left)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable left face key is unknown".into()))?;
        let right_key = face_keys
            .get(*right)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable right face key is unknown".into()))?;
        if left_key == right_key {
            return Err(GeometryErrorKind::Invalid(
                "stable global faces do not have distinct boundary identities".into(),
            )
            .into());
        }
    }
    work.charge_into_error(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut stable_face_id = vec![0_u32; face_count];
    let mut stable_keys = Vec::with_capacity(face_count);
    for (stable, face) in face_order.into_iter().enumerate() {
        work.charge_into_error(1)?;
        *stable_face_id
            .get_mut(face)
            .ok_or_else(|| GeometryErrorKind::Invalid("stable face ordinal is unknown".into()))? =
            u32::try_from(stable).map_err(|_| ResourceError::SizeOverflow)?;
        stable_keys.push(
            face_keys
                .get(face)
                .cloned()
                .ok_or_else(|| GeometryErrorKind::Invalid("stable face key is unknown".into()))?,
        );
    }
    Ok((stable_keys, stable_face_id))
}

pub(super) fn admit_areal_proof(
    graph: &AdmittedGraph,
    layout: &LogicalLayout,
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<ArealProof> {
    let mut wall_atom_set = BTreeSet::new();
    for walk in &layout.areal_walks {
        for use_row in &walk.uses {
            work.charge_into_error(1)?;
            wall_atom_set.insert(use_row.atom);
        }
    }
    work.charge_into_error(
        u64::try_from(wall_atom_set.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let wall_atom_ids = wall_atom_set.into_iter().collect::<Vec<_>>();
    if wall_atom_ids.is_empty() {
        return Err(
            GeometryErrorKind::Invalid("areal logical layout has no wall atoms".into()).into(),
        );
    }
    let signature = build_areal_signature(
        graph,
        layout,
        &wall_atom_ids,
        &graph.points,
        &graph.topology,
        &graph.edge_of_slot,
        full_graph_faces,
        work,
    )?;
    Ok(ArealProof {
        wall_atom_ids,
        signature,
    })
}

pub(super) fn build_areal_signature(
    graph: &AdmittedGraph,
    layout: &LogicalLayout,
    wall_atom_ids: &[u32],
    points: &ExactPoints,
    full_topology: &IndexedTopology,
    full_edge_of_slot: &[u32],
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<ArealSignature> {
    let all_atoms_are_walls = wall_covers_graph(wall_atom_ids, graph.edge_nodes.len());
    let (projected_edges, projected_topology, projected_edge_of_slot) = if all_atoms_are_walls {
        (Vec::new(), None, Vec::new())
    } else {
        work.charge_into_error(
            u64::try_from(wall_atom_ids.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut projected_edges = Vec::with_capacity(wall_atom_ids.len());
        for &atom in wall_atom_ids {
            work.charge_into_error(1)?;
            projected_edges.push(*graph.edge_nodes.get(atom as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("areal logical wall names an unknown atom".into())
            })?);
        }
        let (topology, edge_of_slot) = indexed_topology(points, &projected_edges, work)?;
        (projected_edges, Some(topology), edge_of_slot)
    };
    let (wall_edges, wall_topology, wall_edge_of_slot) = if all_atoms_are_walls {
        (
            graph.edge_nodes.as_slice(),
            full_topology,
            full_edge_of_slot,
        )
    } else {
        (
            projected_edges.as_slice(),
            projected_topology.as_ref().ok_or_else(|| {
                GeometryErrorKind::Invalid("areal wall topology was not built".into())
            })?,
            projected_edge_of_slot.as_slice(),
        )
    };
    let computed_faces = if all_atoms_are_walls && full_graph_faces.is_some() {
        None
    } else {
        Some(global_faces(
            points,
            wall_topology,
            wall_edges,
            wall_edge_of_slot,
            work,
        )?)
    };
    let faces = full_graph_faces
        .filter(|_| all_atoms_are_walls)
        .or(computed_faces.as_ref())
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("required areal wall faces were not constructed".into())
        })?;
    let (face_keys, stable_face_id) = stable_wall_face_keys(
        wall_topology,
        faces,
        wall_edges,
        wall_edge_of_slot,
        wall_atom_ids,
        work,
    )?;
    // `graph_global_faces_with_work` reserves global face 0 for Exterior.
    let exterior_face = *stable_face_id.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("areal wall graph has no Exterior face".into())
    })?;

    let mut component_count = 0_usize;
    for &component in &wall_topology.component_of {
        work.charge_into_error(1)?;
        component_count = component_count.max(
            usize::try_from(component)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if faces.component_hosts.len() != component_count {
        return Err(GeometryErrorKind::Invalid(
            "areal wall hosts do not align with their components".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut host_by_vertex = Vec::with_capacity(points.exact.len());
    for &component in &wall_topology.component_of {
        work.charge_into_error(1)?;
        let face = *faces
            .component_hosts
            .get(component as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("areal wall component has no host face".into())
            })? as usize;
        host_by_vertex.push(*stable_face_id.get(face).ok_or_else(|| {
            GeometryErrorKind::Invalid("areal host face has no stable identity".into())
        })?);
    }

    let fan_vertices = areal_vertices(layout, &graph.edge_nodes, work)?;
    let fan_rows = stable_fan_rows(
        full_topology,
        &graph.edge_nodes,
        full_edge_of_slot,
        &fan_vertices,
        work,
    )?;
    Ok(ArealSignature {
        face_keys,
        exterior_face,
        host_by_vertex,
        fan_rows,
    })
}
