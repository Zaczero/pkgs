use super::*;

#[derive(Debug)]
/// A row occupies half-open intervals in the dual walk's face preorder.
/// Crossing and rollback record only changed rows, so nesting does not copy
/// enclosing memberships into every descendant face.
pub(in crate::geometry::topology) struct GlobalFaceRowOccupancy {
    pub(in crate::geometry::topology::q) face_order: Box<[usize]>,
    pub(in crate::geometry::topology::q) face_counts: Box<[usize]>,
    pub(in crate::geometry::topology::q) row_offsets: Box<[usize]>,
    pub(in crate::geometry::topology::q) intervals: Box<[[usize; 2]]>,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct FaceRowIntervalEvent {
    pub(in crate::geometry::topology::q) order: usize,
    pub(in crate::geometry::topology::q) row: usize,
    pub(in crate::geometry::topology::q) starts: bool,
}

pub(in crate::geometry::topology::q) struct GlobalFaceRowProducts {
    pub(in crate::geometry::topology::q) face_rows: GlobalFaceRowOccupancy,
    pub(in crate::geometry::topology::q) span_collapsed_rows: GlobalSpanRowSupport,
    pub(in crate::geometry::topology::q) node_point_rows: GlobalNodeRowSupport,
    pub(in crate::geometry::topology::q) face_validity: Box<[QFaceValidity]>,
    pub(in crate::geometry::topology::q) seam_owner_sides: Box<[SeamOwnerSide]>,
}

pub(in crate::geometry::topology::q) fn seam_boundary_owner(
    sources: &ExactGeographicChartInventory,
    frame: GeographicFrame,
    owner: &QSpanOwner,
) -> Result<Option<SeamBoundaryOwner>> {
    if owner.ring_transition.is_none() {
        return Ok(None);
    }
    let edge = sources
        .edges
        .get(owner.source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q seam span has an unknown source edge".into())
        })?;
    let SourceEdgeKind::Ring {
        polygon,
        ring,
        hole,
        ..
    } = edge.kind
    else {
        return Ok(None);
    };
    let [start, end] = edge.source;
    if start.x.to_bits() != end.x.to_bits() || start.y.to_bits() == end.y.to_bits() {
        return Ok(None);
    }
    let axis = frame.longitude();
    let start_longitude = axis
        .canonicalize(start.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let end_longitude = axis
        .canonicalize(end.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    if start_longitude.identity(axis) != crate::geographic::SeamIdentity::Seam
        || end_longitude.identity(axis) != crate::geographic::SeamIdentity::Seam
    {
        return Ok(None);
    }
    Ok(Some(SeamBoundaryOwner {
        edge: owner.source_edge,
        polygon,
        ring,
        hole,
    }))
}

impl GlobalFaceRowOccupancy {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "GlobalFaceId arcs and CSR offsets are built from the checked global face count"
    )]
    pub(in crate::geometry::topology::q) fn build(
        face_count: usize,
        sources: &ExactGeographicChartInventory,
        topology: &QuotientFaces,
        global: &GlobalFaceTopology,
    ) -> Result<GlobalFaceRowProducts> {
        let offset_count = face_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let (mut collapsed_points, collapsed_point_faces) =
            collapsed_point_candidates(sources, topology, global, face_count)?;
        let mut node_point_memberships = Vec::with_capacity(sources.points.len());
        append_explicit_node_point_support(sources, topology, &mut node_point_memberships)?;
        let Some(seed_face) = global.seed_face else {
            if global.seed_winding.iter().any(|&winding| winding != 0) {
                return Err(GeometryErrorKind::Invalid(
                    "Q has seed winding without a global seed face".into(),
                )
                .into());
            }
            let mut shell_candidates = Vec::new();
            let mut hole_candidates = Vec::new();
            let mut collapsed_shells = Vec::new();
            let mut span_collapsed_memberships = Vec::new();
            for (span_index, span) in topology.spans.iter().enumerate() {
                collect_collapsed_shell_candidates(
                    span,
                    sources,
                    None,
                    &mut shell_candidates,
                    &mut hole_candidates,
                    &mut collapsed_shells,
                )?;
                span_collapsed_memberships.extend(
                    collapsed_shells
                        .iter()
                        .map(|candidate| (span_index, candidate.row)),
                );
            }
            node_point_memberships.extend(
                collapsed_points
                    .iter()
                    .filter(|candidate| !candidate.trimmed)
                    .map(|candidate| (candidate.node.0 as usize, candidate.row)),
            );
            return Ok(GlobalFaceRowProducts {
                face_rows: Self {
                    face_order: vec![0; face_count].into_boxed_slice(),
                    face_counts: vec![0; face_count].into_boxed_slice(),
                    row_offsets: vec![0; sources.row_count + 1].into_boxed_slice(),
                    intervals: Box::new([]),
                },
                span_collapsed_rows: GlobalSpanRowSupport::build(
                    topology.spans.len(),
                    &mut span_collapsed_memberships,
                )?,
                node_point_rows: GlobalNodeRowSupport::build(
                    topology.nodes.len(),
                    &mut node_point_memberships,
                )?,
                face_validity: vec![QFaceValidity::default(); face_count].into_boxed_slice(),
                seam_owner_sides: Box::new([]),
            });
        };
        if global.seed_winding.len() != sources.rings.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q seed winding does not cover every source ring".into(),
            )
            .into());
        }
        let seed_index = seed_face.0 as usize;
        if seed_index >= face_count {
            return Err(
                GeometryErrorKind::Invalid("Q seed has an unknown global face".into()).into(),
            );
        }

        let mut arc_count = 0_usize;
        for (span_index, faces) in global.span_global_faces.iter().enumerate() {
            let Some([left, right]) = faces else {
                continue;
            };
            let additional = if left == right {
                let span = topology.spans.get(span_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q face table has an unknown span".into())
                })?;
                usize::from(span_has_collapsed_shell(span, sources)?)
            } else {
                2
            };
            arc_count = arc_count
                .checked_add(additional)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut arcs = Vec::with_capacity(arc_count);
        for (span_index, faces) in global.span_global_faces.iter().enumerate() {
            let Some([left, right]) = faces else {
                continue;
            };
            let span = topology.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face table has an unknown span".into())
            })?;
            if left == right {
                if span_has_ring_transition(span)? {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "Q span {:?} nodes {:?}, ports {:?}, local faces {:?}, owners {:?}, has ring transitions {:?} on the same global face {left:?}",
                            span.id,
                            span.nodes,
                            span.ports,
                            topology.span_local_faces.get(span_index),
                            span.owners
                                .iter()
                                .filter_map(|owner| sources
                                    .edges
                                    .get(owner.source_edge.0 as usize)
                                    .map(|edge| (
                                        owner.source_edge,
                                        edge.source,
                                        owner.reversed,
                                        &owner.source_range,
                                    )))
                                .collect::<Vec<_>>(),
                            span.owners
                                .iter()
                                .filter_map(|owner| owner.ring_transition)
                                .collect::<Vec<_>>()
                        )
                        .into(),
                    )
                    .into());
                }
                if span_has_collapsed_shell(span, sources)? {
                    arcs.push(FaceArc {
                        from: *left,
                        to: *right,
                        span: span_index,
                        forward: true,
                    });
                }
                continue;
            }
            arcs.push(FaceArc {
                from: *left,
                to: *right,
                span: span_index,
                forward: true,
            });
            arcs.push(FaceArc {
                from: *right,
                to: *left,
                span: span_index,
                forward: false,
            });
        }
        if arcs.len() != arc_count {
            return Err(GeometryErrorKind::Invalid(
                "global face dual arc count changed during construction".into(),
            )
            .into());
        }
        let mut degrees = vec![0_usize; face_count];
        for arc in &arcs {
            let degree = degrees.get_mut(arc.from.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q dual arc references an unknown global face".into())
            })?;
            *degree = degree.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut offsets = Vec::with_capacity(offset_count);
        offsets.push(0_usize);
        for degree in degrees {
            let next = offsets
                .last()
                .copied()
                .expect("face offsets start at zero")
                .checked_add(degree)
                .ok_or(ResourceError::SizeOverflow)?;
            offsets.push(next);
        }
        if offsets.last().copied() != Some(arcs.len()) {
            return Err(GeometryErrorKind::Invalid(
                "global face dual offsets do not match the arc count".into(),
            )
            .into());
        }
        let mut positions = offsets[..face_count].to_vec();
        let mut ordered_arcs = arcs.clone();
        for arc in arcs {
            let position = &mut positions[arc.from.0 as usize];
            ordered_arcs[*position] = arc;
            *position += 1;
        }
        let arcs = ordered_arcs;

        let mut state = FaceOccupancyState::new(sources, &global.seed_winding)?;
        trim_collapsed_points_on_face(
            seed_face,
            &collapsed_point_faces,
            &mut collapsed_points,
            &state,
        )?;
        let mut seen = vec![false; face_count];
        let mut face_order = vec![0; face_count];
        let mut face_counts = vec![0; face_count];
        let mut face_validity = vec![QFaceValidity::default(); face_count];
        let mut span_collapsed_pending = vec![true; topology.spans.len()];
        let mut span_collapsed_memberships = Vec::<(usize, SourceRowId)>::new();
        let mut seam_owner_sides = Vec::new();
        let mut shell_candidates = Vec::new();
        let mut hole_candidates = Vec::new();
        let mut collapsed_shells = Vec::new();
        seen[seed_index] = true;
        state.append_face(
            seed_face,
            sources,
            &mut face_order,
            &mut face_counts,
            &mut face_validity,
        )?;
        let seed_start = offsets[seed_index];
        let seed_end = offsets[seed_index + 1];
        let mut stack = vec![FaceWalkFrame {
            next_arc: seed_start,
            end_arc: seed_end,
            undo: None,
        }];
        while !stack.is_empty() {
            let arc = {
                let frame = stack.last_mut().expect("the face walk stack is nonempty");
                if frame.next_arc == frame.end_arc {
                    None
                } else {
                    let arc = arcs.get(frame.next_arc).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q face walk has an invalid arc offset".into())
                    })?;
                    frame.next_arc = frame
                        .next_arc
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    Some(arc)
                }
            };
            if let Some(arc) = arc {
                let target = arc.to.0 as usize;
                let target_seen = seen.get(target).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has an unknown target face".into())
                })?;
                let span = topology.spans.get(arc.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has an unknown span".into())
                })?;
                let multiplier = if arc.forward { 1 } else { -1 };
                let pending = span_collapsed_pending.get_mut(arc.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has no collapsed-shell state".into())
                })?;
                if *pending {
                    let seam_owners = span
                        .owners
                        .iter()
                        .map(|owner| seam_boundary_owner(sources, sources.frame, owner))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    let seam_before = seam_owners
                        .iter()
                        .map(|&owner| state.seam_owner_interior(owner))
                        .collect::<Result<Vec<_>>>()?;
                    collect_collapsed_shell_candidates(
                        span,
                        sources,
                        Some(&state),
                        &mut shell_candidates,
                        &mut hole_candidates,
                        &mut collapsed_shells,
                    )?;
                    state.apply(&span.owners, multiplier, sources)?;
                    for (&owner, &before) in seam_owners.iter().zip(&seam_before) {
                        let after = state.seam_owner_interior(owner)?;
                        let (left_interior, right_interior) = if arc.forward {
                            (before, after)
                        } else {
                            (after, before)
                        };
                        let [first, second] = span.nodes;
                        seam_owner_sides.push(SeamOwnerSide {
                            nodes: if first <= second {
                                [first, second]
                            } else {
                                [second, first]
                            },
                            edge: owner.edge,
                            polygon: owner.polygon,
                            left_interior,
                            right_interior,
                        });
                    }
                    for candidate in &collapsed_shells {
                        let holes_after = state
                            .polygon_holes
                            .get(candidate.polygon.0 as usize)
                            .copied()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "collapsed shell has an unknown polygon occupancy".into(),
                                )
                            })?;
                        if candidate.holes_before == 0 && holes_after == 0 {
                            span_collapsed_memberships.push((arc.span, candidate.row));
                        }
                    }
                    *pending = false;
                    if target_seen {
                        state.apply(&span.owners, -multiplier, sources)?;
                        continue;
                    }
                } else {
                    if target_seen {
                        continue;
                    }
                    state.apply(&span.owners, multiplier, sources)?;
                }
                *seen.get_mut(target).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no visited state".into())
                })? = true;
                state.append_face(
                    arc.to,
                    sources,
                    &mut face_order,
                    &mut face_counts,
                    &mut face_validity,
                )?;
                trim_collapsed_points_on_face(
                    arc.to,
                    &collapsed_point_faces,
                    &mut collapsed_points,
                    &state,
                )?;
                let start = *offsets.get(target).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no adjacency start".into())
                })?;
                let end = *offsets.get(target + 1).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no adjacency end".into())
                })?;
                stack.push(FaceWalkFrame {
                    next_arc: start,
                    end_arc: end,
                    undo: Some((arc.span, -multiplier)),
                });
            } else {
                let frame = stack.pop().expect("the face walk stack is nonempty");
                if let Some((span_index, multiplier)) = frame.undo {
                    let span = topology.spans.get(span_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q face rollback has an unknown span".into())
                    })?;
                    state.apply(&span.owners, multiplier, sources)?;
                }
            }
        }
        if seen.iter().any(|&visited| !visited) {
            return Err(GeometryErrorKind::Invalid(
                "the global Q face dual is disconnected".into(),
            )
            .into());
        }
        node_point_memberships.extend(
            collapsed_points
                .iter()
                .filter(|candidate| !candidate.trimmed)
                .map(|candidate| (candidate.node.0 as usize, candidate.row)),
        );

        for (row, &start) in state.interval_starts.iter().enumerate() {
            if let Some(start) = start
                && start < state.cursor
            {
                state.intervals.push((
                    SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?),
                    [start, state.cursor],
                ));
            }
        }
        let mut row_offsets = vec![0_usize; sources.row_count + 1];
        for &(row, _) in &state.intervals {
            row_offsets[row.0 as usize + 1] += 1;
        }
        for index in 1..row_offsets.len() {
            row_offsets[index] += row_offsets[index - 1];
        }
        let mut positions = row_offsets[..sources.row_count].to_vec();
        let mut intervals = vec![[0; 2]; state.intervals.len()];
        for (row, interval) in state.intervals {
            let position = &mut positions[row.0 as usize];
            intervals[*position] = interval;
            *position += 1;
        }
        let mut write = 0;
        // Non-tree crossings can close and reopen a row without visiting a face.
        for row in 0..sources.row_count {
            let start = row_offsets[row];
            let end = row_offsets[row + 1];
            row_offsets[row] = write;
            let row_start = write;
            for read in start..end {
                let interval = intervals[read];
                if write > row_start && intervals[write - 1][1] == interval[0] {
                    intervals[write - 1][1] = interval[1];
                } else {
                    intervals[write] = interval;
                    write += 1;
                }
            }
        }
        row_offsets[sources.row_count] = write;
        intervals.truncate(write);
        let span_collapsed_rows =
            GlobalSpanRowSupport::build(topology.spans.len(), &mut span_collapsed_memberships)?;
        let node_point_rows =
            GlobalNodeRowSupport::build(topology.nodes.len(), &mut node_point_memberships)?;
        Ok(GlobalFaceRowProducts {
            face_rows: Self {
                face_order: face_order.into_boxed_slice(),
                face_counts: face_counts.into_boxed_slice(),
                row_offsets: row_offsets.into_boxed_slice(),
                intervals: intervals.into_boxed_slice(),
            },
            span_collapsed_rows,
            node_point_rows,
            face_validity: face_validity.into_boxed_slice(),
            seam_owner_sides: seam_owner_sides.into_boxed_slice(),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "face and source row IDs belong to the admitted topology; interval offsets cover every row"
    )]
    pub(in crate::geometry::topology) fn contains(
        &self,
        face: GlobalFaceId,
        row: SourceRowId,
    ) -> bool {
        let face = face.0 as usize;
        let row = row.0 as usize;
        if row + 1 >= self.row_offsets.len() {
            return false;
        }
        let count = self.face_counts[face];
        if count == 0 {
            return false;
        }
        if count + 1 == self.row_offsets.len() {
            return true;
        }
        let order = self.face_order[face];
        let intervals = &self.intervals[self.row_offsets[row]..self.row_offsets[row + 1]];
        let end = intervals.partition_point(|interval| interval[0] <= order);
        end != 0 && order < intervals[end - 1][1]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "global face IDs belong to the admitted topology"
    )]
    pub(in crate::geometry::topology) fn count(&self, face: GlobalFaceId) -> usize {
        self.face_counts[face.0 as usize]
    }

    /// Visit each global face once with its sparse active source-row set.
    ///
    /// Row occupancy is stored as preorder intervals by row. This sweep
    /// inverts those intervals without constructing a face-by-row bitmap.
    pub(in crate::geometry::topology) fn for_each_face_occupants(
        &self,
        work: &mut impl Work,
        mut visit: impl FnMut(GlobalFaceId, &[SourceRowId]) -> Result<()>,
    ) -> Result<()> {
        let face_count = self.face_order.len();
        let row_count = self
            .row_offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q face row offsets are empty".into()))?;
        let event_count = self
            .intervals
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(
                face_count
                    .checked_add(event_count)
                    .and_then(|count| count.checked_add(row_count))
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;

        let mut face_at_order = vec![None; face_count];
        for (face_index, &order) in self.face_order.iter().enumerate() {
            let face =
                GlobalFaceId(u32::try_from(face_index).map_err(|_| ResourceError::SizeOverflow)?);
            let slot = face_at_order.get_mut(order).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face preorder is out of range".into())
            })?;
            if slot.replace(face).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "Q face preorder contains a duplicate order".into(),
                )
                .into());
            }
        }

        let mut events = Vec::with_capacity(event_count);
        for (row, offsets) in self.row_offsets.windows(2).enumerate() {
            let [start, end] = offsets else {
                unreachable!("row CSR windows contain two offsets");
            };
            let intervals = self.intervals.get(*start..*end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face row intervals are out of range".into())
            })?;
            for &[low, high] in intervals {
                work.charge_into_error(1)?;
                if low > high || high > face_count {
                    return Err(GeometryErrorKind::Invalid(
                        "Q face row interval is outside the preorder".into(),
                    )
                    .into());
                }
                events.push(FaceRowIntervalEvent {
                    order: low,
                    row,
                    starts: true,
                });
                events.push(FaceRowIntervalEvent {
                    order: high,
                    row,
                    starts: false,
                });
            }
        }
        try_sort_by_with_work(&mut events, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left
                .order
                .cmp(&right.order)
                .then(left.starts.cmp(&right.starts))
                .then(left.row.cmp(&right.row)))
        })?;

        let mut active_counts = vec![0_usize; row_count];
        let mut active_set = BTreeSet::new();
        let mut active_rows = Vec::new();
        let mut event_cursor = 0_usize;
        for order in 0..face_count {
            while events
                .get(event_cursor)
                .is_some_and(|event| event.order <= order)
            {
                apply_face_row_interval_event(
                    *events
                        .get(event_cursor)
                        .expect("event cursor was checked against the event slice"),
                    &mut active_counts,
                    &mut active_set,
                    work,
                )?;
                event_cursor = event_cursor
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            let face = face_at_order.get(order).copied().flatten().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face preorder has a missing face".into())
            })?;
            work.charge_into_error(
                u64::try_from(
                    active_set
                        .len()
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?,
                )
                .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            active_rows.clear();
            active_rows.extend(active_set.iter().copied());
            visit(face, &active_rows)?;
        }
        while let Some(&event) = events.get(event_cursor) {
            apply_face_row_interval_event(event, &mut active_counts, &mut active_set, work)?;
            event_cursor = event_cursor
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        if !active_set.is_empty() || active_counts.iter().any(|&count| count != 0) {
            return Err(GeometryErrorKind::Invalid(
                "Q face row interval sweep ended with active owners".into(),
            )
            .into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::geometry::topology::q) fn rows_for(
        &self,
        face: GlobalFaceId,
    ) -> Vec<SourceRowId> {
        self.row_offsets
            .windows(2)
            .enumerate()
            .map(|(row, _)| SourceRowId(u32::try_from(row).unwrap()))
            .filter(|&row| self.contains(face, row))
            .collect()
    }
}

pub(in crate::geometry::topology::q) fn apply_face_row_interval_event(
    event: FaceRowIntervalEvent,
    active_counts: &mut [usize],
    active_rows: &mut BTreeSet<SourceRowId>,
    work: &mut impl Work,
) -> Result<()> {
    let tree_steps = active_rows
        .len()
        .max(1)
        .ilog2()
        .checked_mul(2)
        .and_then(|steps| steps.checked_add(1))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::from(tree_steps) + 1)?;
    let count = active_counts.get_mut(event.row).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q face interval references an unknown row".into())
    })?;
    let row = SourceRowId(u32::try_from(event.row).map_err(|_| ResourceError::SizeOverflow)?);
    if event.starts {
        let was_inactive = *count == 0;
        *count = (*count).checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        if was_inactive && !active_rows.insert(row) {
            return Err(GeometryErrorKind::Invalid(
                "Q active face row was inserted more than once".into(),
            )
            .into());
        }
    } else {
        if *count == 0 {
            return Err(GeometryErrorKind::Invalid(
                "Q face interval ended before it started".into(),
            )
            .into());
        }
        *count = (*count).checked_sub(1).ok_or(ResourceError::SizeOverflow)?;
        if *count == 0 && !active_rows.remove(&row) {
            return Err(GeometryErrorKind::Invalid(
                "Q active face row is missing from its set".into(),
            )
            .into());
        }
    }
    Ok(())
}

impl HeapSize for GlobalFaceRowOccupancy {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-published face order and row interval buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        (self.face_order.len() + self.face_counts.len() + self.row_offsets.len())
            * std::mem::size_of::<usize>()
            + self.intervals.len() * std::mem::size_of::<[usize; 2]>()
    }
}
