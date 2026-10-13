use std::cmp::Ordering;

use super::*;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct CutChordPair {
    pub(in crate::geometry::topology) west_far_port: QPortId,
    pub(in crate::geometry::topology) east_far_port: QPortId,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct CutNodePorts {
    pub(in crate::geometry::topology) node: QNodeId,
    pub(in crate::geometry::topology) west: QPortId,
    pub(in crate::geometry::topology) east: QPortId,
    pub(in crate::geometry::topology) chords: Vec<CutChordPair>,
}

#[derive(Clone)]
struct CutOwnerHalf {
    source_edge: SourceEdgeId,
    parameter: ExactRational,
    far_port: QPortId,
}

impl CutOwnerHalf {
    fn same_key(&self, other: &Self, work: &mut impl Work) -> Result<bool> {
        Ok(self.source_edge == other.source_edge
            && self
                .parameter
                .try_cmp_with_work(&other.parameter, work)?
                .is_eq())
    }
}

#[derive(Default)]
struct CutSides {
    west: Option<QPortId>,
    east: Option<QPortId>,
    west_halves: Vec<CutOwnerHalf>,
    east_halves: Vec<CutOwnerHalf>,
}

fn compare_cut_owner_half_key(
    left: &CutOwnerHalf,
    right: &CutOwnerHalf,
    work: &mut impl Work,
) -> Result<Ordering> {
    let edge = left.source_edge.cmp(&right.source_edge);
    Ok(if edge.is_eq() {
        left.parameter.try_cmp_with_work(&right.parameter, work)?
    } else {
        edge
    })
}

fn compare_cut_owner_half(
    left: &CutOwnerHalf,
    right: &CutOwnerHalf,
    work: &mut impl Work,
) -> Result<Ordering> {
    let key = compare_cut_owner_half_key(left, right, work)?;
    Ok(if key.is_eq() {
        left.far_port.cmp(&right.far_port)
    } else {
        key
    })
}

fn deduplicate_cut_owner_halves(
    halves: &mut Vec<CutOwnerHalf>,
    work: &mut impl Work,
) -> Result<()> {
    try_sort_by_with_work(halves, work, |left, right, work| {
        work.charge_into_error(1)?;
        compare_cut_owner_half(left, right, work)
    })?;
    let mut unique: Vec<CutOwnerHalf> = Vec::with_capacity(halves.len());
    for half in halves.drain(..) {
        work.charge_into_error(1)?;
        if let Some(previous) = unique.last()
            && previous.same_key(&half, work)?
            && previous.far_port == half.far_port
        {
            continue;
        }
        unique.push(half);
    }
    *halves = unique;
    Ok(())
}

pub(in crate::geometry::topology) fn build_cut_node_ports(
    topology: &ExactGeographicTopology,
    spans: &[usize],
    selected_ports: &[(QNodeId, QPortId)],
    work: &mut impl Work,
) -> Result<Vec<CutNodePorts>> {
    let q = &topology.q;
    let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
    let cut_east = q.cut.try_add_with_work(&period, work)?;
    work.charge_into_error(
        u64::try_from(
            q.nodes
                .len()
                .checked_add(q.ports.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut cuts = Vec::new();
    cuts.resize_with(q.nodes.len(), Option::<CutSides>::default);
    let mut selected_port = vec![false; q.ports.len()];
    for &(_, port_id) in selected_ports {
        work.charge_into_error(1)?;
        *selected_port
            .get_mut(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port is unknown".into()))? = true;
    }
    for &span_index in spans {
        work.charge_into_error(1)?;
        let span = q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut reconstruction has an unknown span".into())
        })?;
        let [first_port, second_port] = span.ports;
        for (at_first_endpoint, port_id, far_port) in [
            (true, first_port, second_port),
            (false, second_port, first_port),
        ] {
            work.charge_into_error(1)?;
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut reconstruction has an unknown port".into())
            })?;
            if !port.cut {
                continue;
            }
            let sides = cuts
                .get_mut(port.node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node ordinal is invalid".into()))?
                .get_or_insert_with(CutSides::default);
            let halves = if port.point.x.try_cmp_with_work(&q.cut, work)?.is_eq() {
                if sides
                    .west
                    .replace(port_id)
                    .is_some_and(|previous| previous != port_id)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "Q cut node has multiple west ports".into(),
                    )
                    .into());
                }
                &mut sides.west_halves
            } else if port.point.x.try_cmp_with_work(&cut_east, work)?.is_eq() {
                if sides
                    .east
                    .replace(port_id)
                    .is_some_and(|previous| previous != port_id)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "Q cut node has multiple east ports".into(),
                    )
                    .into());
                }
                &mut sides.east_halves
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "Q cut port is not on either chart boundary".into(),
                )
                .into());
            };
            for owner in &span.owners {
                work.charge_into_error(1)?;
                let at_source_start = at_first_endpoint != owner.reversed;
                let parameter = if at_source_start {
                    owner.source_range.start.try_clone_with_work(work)?
                } else {
                    owner.source_range.end.try_clone_with_work(work)?
                };
                halves.push(CutOwnerHalf {
                    source_edge: owner.source_edge,
                    parameter,
                    far_port,
                });
            }
        }
    }

    let mut output = Vec::new();
    for (node_index, sides) in cuts.into_iter().enumerate() {
        work.charge_into_error(1)?;
        let Some(mut sides) = sides else {
            continue;
        };
        let node_id = QNodeId(u32::try_from(node_index).map_err(|_| ResourceError::SizeOverflow)?);
        let west = sides
            .west
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node has no west port".into()))?;
        let east = sides
            .east
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node has no east port".into()))?;
        if !selected_port.get(west.0 as usize).copied().unwrap_or(false)
            || !selected_port.get(east.0 as usize).copied().unwrap_or(false)
        {
            return Err(GeometryErrorKind::Invalid(
                "Q selected subgraph has only one side of a cut node".into(),
            )
            .into());
        }

        deduplicate_cut_owner_halves(&mut sides.west_halves, work)?;
        deduplicate_cut_owner_halves(&mut sides.east_halves, work)?;
        let mut chords = Vec::new();
        let mut west_index = 0_usize;
        let mut east_index = 0_usize;
        while west_index < sides.west_halves.len() && east_index < sides.east_halves.len() {
            work.charge_into_error(1)?;
            let west_half = sides
                .west_halves
                .get(west_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q west cut owner is unknown".into()))?;
            let east_half = sides
                .east_halves
                .get(east_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q east cut owner is unknown".into()))?;
            match compare_cut_owner_half_key(west_half, east_half, work)? {
                Ordering::Less => {
                    west_index = west_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                },
                Ordering::Greater => {
                    east_index = east_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                },
                Ordering::Equal => {
                    let mut west_end = west_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    while let Some(next) = sides.west_halves.get(west_end) {
                        work.charge_into_error(1)?;
                        if !west_half.same_key(next, work)? {
                            break;
                        }
                        west_end = west_end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    }
                    let mut east_end = east_index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    while let Some(next) = sides.east_halves.get(east_end) {
                        work.charge_into_error(1)?;
                        if !east_half.same_key(next, work)? {
                            break;
                        }
                        east_end = east_end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    }
                    if west_end.checked_sub(west_index) != Some(1)
                        || east_end.checked_sub(east_index) != Some(1)
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "Q cut source endpoint has multiple chart continuations".into(),
                        )
                        .into());
                    }
                    chords.push(CutChordPair {
                        west_far_port: west_half.far_port,
                        east_far_port: east_half.far_port,
                    });
                    west_index = west_end;
                    east_index = east_end;
                },
            }
        }
        try_sort_by_with_work(&mut chords, work, |left, right, _work| Ok(left.cmp(right)))?;
        work.charge_into_error(
            u64::try_from(chords.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        chords.dedup();
        if chords.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q selected cut node has no paired emitted source chords".into(),
            )
            .into());
        }
        output.push(CutNodePorts {
            node: node_id,
            west,
            east,
            chords,
        });
    }
    Ok(output)
}
