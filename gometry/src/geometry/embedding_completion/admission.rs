use std::collections::BTreeSet;

use super::areal::{
    ArealProof, admit_areal_proof, build_areal_signature, stable_areal_signatures_match,
};
use super::contacts::{edges_have_only_vertex_contacts, point_edge_contacts_preserved};
use super::layout::{LogicalArealWalk, LogicalLayout};
use super::points::ExactPoints;
use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{
    IndexedTopology, indexed_edge_slots_with_work, indexed_topology_from_edges_with_work,
};
use crate::geometry::overlay::{GlobalFaces, compare_departures_with_work};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::{GeometryErrorKind, Work};

pub(in crate::geometry) struct AdmittedLayout {
    layout: LogicalLayout,
    areal: Option<ArealProof>,
}

/// Admit identities and all-graph contacts first. Dimension-specific fan and
/// wall-face obligations are frozen only after `LogicalLayout` arrives.
pub(in crate::geometry) struct AdmittedGraph {
    pub(in crate::geometry) points: ExactPoints,
    pub(in crate::geometry) edge_nodes: Vec<[u32; 2]>,
    pub(in crate::geometry) edge_slots: Vec<[u32; 2]>,
    pub(in crate::geometry) edge_of_slot: Vec<u32>,
    pub(in crate::geometry) topology: IndexedTopology,
    pub(in crate::geometry) point_identities: Vec<u32>,
    pub(in crate::geometry) point_contact_queries: Vec<u32>,
}

/// A successful assignment retains the exact graph and layout that admitted
/// it. Its fields are issued only by `admit_candidate`.
pub(in crate::geometry) struct CheckedAssignment<'a> {
    reference: &'a AdmittedGraph,
    dimension: &'a AdmittedLayout,
    points: ExactPoints,
}

impl CheckedAssignment<'_> {
    pub(in crate::geometry) const fn points(&self) -> &ExactPoints {
        &self.points
    }

    pub(in crate::geometry) fn is_for(
        &self,
        reference: &AdmittedGraph,
        dimension: &AdmittedLayout,
    ) -> bool {
        std::ptr::eq(self.reference, reference) && std::ptr::eq(self.dimension, dimension)
    }
}

/// Admit a producer-known graph without assigning it a dimension's face law.
pub(in crate::geometry) fn admit_known_graph(
    points: ExactPoints,
    edge_nodes: Vec<[u32; 2]>,
    point_identities: Vec<u32>,
    work: &mut impl Work,
) -> Result<AdmittedGraph> {
    if point_identities.len() != points.exact.len() {
        return Err(GeometryErrorKind::Invalid(
            "known graph identity rows do not match its point inventory".into(),
        )
        .into());
    }
    validate_point_hints(&points, work)?;
    validate_atoms(&points, &edge_nodes, work)?;
    if !edges_have_only_vertex_contacts(&points, &edge_nodes, work)? {
        return Err(GeometryErrorKind::Invalid(
            "known graph edges contain an unrepresented contact or overlap".into(),
        )
        .into());
    }
    if !identities_are_injective(&points, &point_identities, work)? {
        return Err(GeometryErrorKind::Invalid(
            "distinct known graph identities share one exact point".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut degree = vec![0_usize; points.exact.len()];
    for &[start, end] in &edge_nodes {
        work.charge_into_error(2)?;
        for vertex in [start, end] {
            let count = degree.get_mut(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph edge endpoint is unknown".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    work.charge_into_error(u64::try_from(degree.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut point_contact_queries = Vec::new();
    for (vertex, &incident_edges) in degree.iter().enumerate() {
        work.charge_into_error(1)?;
        if incident_edges == 0 {
            point_contact_queries
                .push(u32::try_from(vertex).map_err(|_| ResourceError::SizeOverflow)?);
        }
    }
    let (topology, edge_of_slot) = indexed_topology(&points, &edge_nodes, work)?;
    let edge_slots = indexed_edge_slots_with_work(&edge_nodes, &topology, &edge_of_slot, work)?;
    Ok(AdmittedGraph {
        points,
        edge_nodes,
        edge_slots,
        edge_of_slot,
        topology,
        point_identities,
        point_contact_queries,
    })
}

impl AdmittedGraph {
    /// Extend the graph's degree-zero query set with the producer's actual
    /// output-point vertices before the reference is used for candidates.
    pub(in crate::geometry) fn with_output_point_contacts(
        mut self,
        vertices: &[u32],
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut requested = Vec::with_capacity(vertices.len());
        for &vertex in vertices {
            work.charge_into_error(1)?;
            if self.points.exact.get(vertex as usize).is_none() {
                return Err(GeometryErrorKind::Invalid(
                    "output point contact query is outside the admitted graph".into(),
                )
                .into());
            }
            requested.push(vertex);
        }
        if requested.is_empty() {
            return Ok(self);
        }
        try_sort_by_with_work(&mut requested, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(requested.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique = Vec::with_capacity(requested.len());
        for vertex in requested {
            work.charge_into_error(1)?;
            if unique.last() != Some(&vertex) {
                unique.push(vertex);
            }
        }
        self.point_contact_queries =
            merge_sorted_contact_queries(&self.point_contact_queries, &unique, work)?;
        Ok(self)
    }
}

/// Freeze dimension-specific obligations only after the producer's actual
/// logical output walks have been admitted against its canonical atoms.
/// A producer-owned full-graph face table is reused only when every admitted
/// atom is a wall; partial wall views build and drop their own face tables.
pub(in crate::geometry) fn admit_logical_layout(
    reference: &AdmittedGraph,
    walks: Vec<LogicalArealWalk>,
    full_graph_faces: Option<&GlobalFaces>,
    work: &mut impl Work,
) -> Result<AdmittedLayout> {
    let layout = LogicalLayout::admit(
        walks,
        reference.points.exact.len(),
        &reference.edge_nodes,
        work,
    )?;
    let areal = if layout.has_areal_walks() {
        Some(admit_areal_proof(
            reference,
            &layout,
            full_graph_faces,
            work,
        )?)
    } else {
        None
    };
    Ok(AdmittedLayout { layout, areal })
}

pub(in crate::geometry) fn admit_candidate<'a>(
    reference: &'a AdmittedGraph,
    dimension: &'a AdmittedLayout,
    points: ExactPoints,
    work: &mut impl Work,
) -> Result<Option<CheckedAssignment<'a>>> {
    if points.exact.len() != reference.points.exact.len()
        || points.rounded.len() != points.exact.len()
    {
        return Ok(None);
    }
    if !point_hints_match(&points, work)? {
        return Ok(None);
    }
    if !identities_are_injective(&points, &reference.point_identities, work)? {
        return Ok(None);
    }
    if !point_edge_contacts_preserved(
        &reference.points,
        &points,
        &reference.point_contact_queries,
        &reference.edge_nodes,
        work,
    )? {
        return Ok(None);
    }
    if !edges_have_only_vertex_contacts(&points, &reference.edge_nodes, work)? {
        return Ok(None);
    }
    let (topology, edge_of_slot) = match indexed_topology(&points, &reference.edge_nodes, work) {
        Ok(topology) => topology,
        Err(error) if is_tied_departure_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if let Some(areal_reference) = &dimension.areal {
        let areal = build_areal_signature(
            reference,
            &dimension.layout,
            &areal_reference.wall_atom_ids,
            &points,
            &topology,
            &edge_of_slot,
            None,
            work,
        )?;
        if !stable_areal_signatures_match(&areal_reference.signature, &areal) {
            return Ok(None);
        }
    }
    Ok(Some(CheckedAssignment {
        reference,
        dimension,
        points,
    }))
}

fn merge_sorted_contact_queries(
    left: &[u32],
    right: &[u32],
    work: &mut impl Work,
) -> Result<Vec<u32>> {
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut merged = Vec::with_capacity(capacity);
    let (mut left_index, mut right_index) = (0_usize, 0_usize);
    while left_index < left.len() && right_index < right.len() {
        work.charge_into_error(1)?;
        let (Some(&left_row), Some(&right_row)) = (left.get(left_index), right.get(right_index))
        else {
            return Err(GeometryErrorKind::Invalid(
                "contact query merge cursor is outside its rows".into(),
            )
            .into());
        };
        if left_row <= right_row {
            work.charge_into_error(1)?;
            if merged.last() != Some(&left_row) {
                work.charge_into_error(1)?;
                merged.push(left_row);
            }
            left_index = left_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        } else {
            work.charge_into_error(1)?;
            if merged.last() != Some(&right_row) {
                work.charge_into_error(1)?;
                merged.push(right_row);
            }
            right_index = right_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for &row in left.get(left_index..).ok_or_else(|| {
        GeometryErrorKind::Invalid("contact query merge left cursor is unknown".into())
    })? {
        work.charge_into_error(1)?;
        if merged.last() != Some(&row) {
            work.charge_into_error(1)?;
            merged.push(row);
        }
    }
    for &row in right.get(right_index..).ok_or_else(|| {
        GeometryErrorKind::Invalid("contact query merge right cursor is unknown".into())
    })? {
        work.charge_into_error(1)?;
        if merged.last() != Some(&row) {
            work.charge_into_error(1)?;
            merged.push(row);
        }
    }
    Ok(merged)
}

fn is_tied_departure_error(error: &crate::error::Error) -> bool {
    matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(message))
            if message.as_ref() == "distinct indexed graph edges share a departure ray"
    )
}

fn validate_atoms(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for &[start, end] in edge_nodes {
        work.charge_into_error(1)?;
        if start == end
            || points.exact.get(start as usize).is_none()
            || points.exact.get(end as usize).is_none()
        {
            return Err(GeometryErrorKind::Invalid(
                "known graph atom has a collapsed or unknown endpoint".into(),
            )
            .into());
        }
        if points
            .exact
            .get(start as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph start is unknown".into()))?
            .same_position(
                points.exact.get(end as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("known graph end is unknown".into())
                })?,
            )?
        {
            return Err(GeometryErrorKind::Invalid(
                "known graph atom has geometrically collapsed endpoints".into(),
            )
            .into());
        }
        let pair = if start < end {
            [start, end]
        } else {
            [end, start]
        };
        if !seen.insert(pair) {
            return Err(GeometryErrorKind::Invalid(
                "known graph contains duplicate geometric atoms".into(),
            )
            .into());
        }
    }
    Ok(())
}

fn validate_point_hints(points: &ExactPoints, work: &mut impl Work) -> Result<()> {
    if !point_hints_match(points, work)? {
        return Err(GeometryErrorKind::Invalid(
            "known graph rounded hints do not match their exact points".into(),
        )
        .into());
    }
    Ok(())
}

fn point_hints_match(points: &ExactPoints, work: &mut impl Work) -> Result<bool> {
    if points.rounded.len() != points.exact.len() {
        return Ok(false);
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    for (exact, &rounded) in points.exact.iter().zip(&points.rounded) {
        work.charge_into_error(1)?;
        let expected = XY::new(
            exact.x.round_with_work(work)?,
            exact.y.round_with_work(work)?,
        );
        if !expected.x.is_finite() || !expected.y.is_finite() || expected != rounded {
            return Ok(false);
        }
    }
    Ok(true)
}

fn identities_are_injective(
    points: &ExactPoints,
    identities: &[u32],
    work: &mut impl Work,
) -> Result<bool> {
    if points.exact.len() != identities.len() {
        return Ok(false);
    }
    work.charge_into_error(
        u64::try_from(points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut order = (0..points.exact.len())
        .map(|index| {
            Ok((
                index,
                *identities.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph point has no identity receipt".into())
                })?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    try_sort_by_with_work(&mut order, work, |left, right, work| {
        work.charge_into_error(1)?;
        let left_point = points.exact.get(left.0).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph identity point is outside its inventory".into())
        })?;
        let right_point = points.exact.get(right.0).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph identity point is outside its inventory".into())
        })?;
        let position = left_point.compare_lex(right_point)?;
        Ok(if position.is_eq() {
            left.1.cmp(&right.1)
        } else {
            position
        })
    })?;
    for pair in order.array_windows::<2>() {
        work.charge_into_error(1)?;
        let [left, right] = pair;
        if left.1 != right.1
            && points
                .exact
                .get(left.0)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph identity point is unknown".into())
                })?
                .same_position(points.exact.get(right.0).ok_or_else(|| {
                    GeometryErrorKind::Invalid("graph identity point is unknown".into())
                })?)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn indexed_topology(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<(IndexedTopology, Vec<u32>)> {
    indexed_topology_from_edges_with_work(
        points.exact.len(),
        edge_nodes,
        work,
        |origin, left, right, work| {
            let left_edge = edge_nodes.get(left).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph departure edge is unknown".into())
            })?;
            let right_edge = edge_nodes.get(right).ok_or_else(|| {
                GeometryErrorKind::Invalid("known graph departure edge is unknown".into())
            })?;
            let departure = |edge: &[u32; 2]| -> Result<u32> {
                if edge[0] == origin {
                    Ok(edge[1])
                } else if edge[1] == origin {
                    Ok(edge[0])
                } else {
                    Err(GeometryErrorKind::Invalid(
                        "known graph departure does not touch its vertex".into(),
                    )
                    .into())
                }
            };
            compare_departures_with_work(
                points,
                origin,
                departure(left_edge)?,
                departure(right_edge)?,
                work,
            )
        },
    )
}
