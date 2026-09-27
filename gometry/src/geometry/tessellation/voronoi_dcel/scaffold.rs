//! Building the scaffold: separators, atoms and their split events.
//!
//! The scaffold is the arrangement before it is labelled -- every clipped
//! separator noded against every other, with the owner transitions each
//! crossing implies recorded on its edges.

use super::{
    AtomSeed, AtomicEdge, CertifiedPrimalEdge, ClipSegmentId, ClippedSeparator, DelaunayComplex,
    ExactSign, IndexedTopology, NodedAtoms, NodedVertex, NormalizedClip, Ordering, OwnerTransition,
    PointKey, PrimitiveOwner, Result, ScaffoldPrimitive, Site, SiteAdjacency, SiteId, Transition,
    VertexId, XY, clip_hull_ray_to_rectangle, clip_segment_to_rectangle, exact, nearest_site_walk,
    orient_separator, sort_unique_along, try_dedup_by, try_sort_by, voronoi_error,
};

pub(super) fn coincident_frame_owner(
    pair: [SiteId; 2],
    ccw_start: &exact::ExactPoint,
    ccw_end: &exact::ExactPoint,
    sites: &[Site],
) -> Result<SiteId> {
    let mut inside = None;
    for &id in &pair {
        match exact::orient_points(
            ccw_start,
            ccw_end,
            &exact::ExactPoint::from_xy(sites[id.0 as usize].point.xy()),
        )? {
            ExactSign::Positive => {
                if inside.replace(id).is_some() {
                    return Err(voronoi_error(
                        "both separator sites lie inside a coincident frame side",
                    ));
                }
            },
            ExactSign::Negative => {},
            ExactSign::Zero => {
                return Err(voronoi_error("Voronoi separator contains its source site"));
            },
        }
    }
    inside.ok_or_else(|| voronoi_error("coincident frame separator has no interior site"))
}

pub(super) fn build_scaffold(
    rect: &[exact::ExactPoint; 4],
    clip: &NormalizedClip<'_>,
) -> Result<Vec<ScaffoldPrimitive>> {
    let mut outer_is_rect = clip.rings.first().is_some_and(|ring| ring.len() == 4);
    for side in 0..4 {
        outer_is_rect = outer_is_rect && clip.rings[0][side].same_position(&rect[side])?;
    }
    let clip_rings = if outer_is_rect {
        &clip.rings[1..]
    } else {
        clip.rings.as_slice()
    };
    let scaffold_segments = clip_rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let scaffold_count = 4_usize
        .checked_add(scaffold_segments)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let clip_segment_count = clip.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    u32::try_from(clip_segment_count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut scaffold = Vec::with_capacity(scaffold_count);
    for side in 0..4 {
        scaffold.push(ScaffoldPrimitive {
            line: exact::ExactLine::through_points(&rect[side], &rect[(side + 1) % 4])?,
            start: rect[side].clone(),
            end: rect[(side + 1) % 4].clone(),
            frame_delta: 1,
            clip_delta: i32::from(outer_is_rect),
            owner: PrimitiveOwner::FrameNearest,
            clip_source: outer_is_rect.then_some(ClipSegmentId(side as u32)),
            frame_side: Some(side as u8),
            events: Vec::new(),
        });
    }
    let mut clip_segment = if outer_is_rect { 4 } else { 0 };
    for ring in clip_rings {
        for index in 0..ring.len() {
            scaffold.push(ScaffoldPrimitive {
                line: exact::ExactLine::through_points(
                    &ring[index],
                    &ring[(index + 1) % ring.len()],
                )?,
                start: ring[index].clone(),
                end: ring[(index + 1) % ring.len()].clone(),
                frame_delta: 0,
                clip_delta: 1,
                owner: PrimitiveOwner::Preserve,
                clip_source: Some(ClipSegmentId(clip_segment)),
                frame_side: None,
                events: Vec::new(),
            });
            clip_segment = clip_segment
                .checked_add(1)
                .ok_or_else(|| voronoi_error("too many clip segments"))?;
        }
    }
    // Rectangular shells alias their four clip IDs with frame slots; other
    // clips place all ring segments after the four frame slots.
    let scaffold_clip_offset = if outer_is_rect { 0 } else { 4 };
    for contact in &clip.contacts {
        for segment in contact.segments {
            // Contact ordinals belong to the segment count admitted above.
            let clip_source = ClipSegmentId(segment as u32);
            let scaffold_index = segment
                .checked_add(scaffold_clip_offset)
                .ok_or_else(|| voronoi_error("validated clip contact has no source segment"))?;
            let primitive = scaffold
                .get_mut(scaffold_index)
                .filter(|primitive| primitive.clip_source == Some(clip_source))
                .ok_or_else(|| voronoi_error("validated clip contact has no source segment"))?;
            primitive.events.push(contact.point.clone());
        }
    }
    register_frame_clip_contacts(&mut scaffold)?;
    for primitive in &mut scaffold {
        if exact::signed_line_product(&primitive.line, &primitive.start)? != ExactSign::Zero
            || exact::signed_line_product(&primitive.line, &primitive.end)? != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        primitive.events.reserve(2);
        primitive
            .events
            .extend([primitive.start.clone(), primitive.end.clone()]);
    }
    Ok(scaffold)
}

pub(super) fn emit_clipped_separators(
    sites: &[Site],
    primal_edges: &[CertifiedPrimalEdge],
    complex: &DelaunayComplex,
    rect: &[exact::ExactPoint; 4],
) -> Result<Vec<ClippedSeparator>> {
    let mut separators = Vec::new();
    for &primal in primal_edges {
        let (pair, span, line) = match primal {
            CertifiedPrimalEdge::Interior { sites: pair, faces } => {
                let components = [
                    complex.component_of_face(faces[0]),
                    complex.component_of_face(faces[1]),
                ];
                if components[0] == components[1] {
                    continue;
                }
                let line = exact::ExactLine::perpendicular_bisector(
                    sites[pair[0]].point.xy(),
                    sites[pair[1]].point.xy(),
                )?;
                let Some(span) = clip_segment_to_rectangle(
                    complex.center(components[0]),
                    complex.center(components[1]),
                    [sites[pair[0]].point.xy(), sites[pair[1]].point.xy()],
                    &line,
                    rect,
                )?
                else {
                    continue;
                };
                (pair, span, line)
            },
            CertifiedPrimalEdge::Hull {
                sites: pair,
                face,
                opposite,
            } => {
                let line = exact::ExactLine::perpendicular_bisector(
                    sites[pair[0]].point.xy(),
                    sites[pair[1]].point.xy(),
                )?;
                let Some(span) = clip_hull_ray_to_rectangle(
                    complex.center(complex.component_of_face(face)),
                    pair,
                    opposite,
                    &line,
                    rect,
                    sites,
                )?
                else {
                    continue;
                };
                (pair, span, line)
            },
        };
        let ids = [SiteId(pair[0] as u32), SiteId(pair[1] as u32)];
        let (start, end) = orient_separator(span.0, span.1, ids, sites)?;
        if exact::signed_line_product(&line, &start)? != ExactSign::Zero
            || exact::signed_line_product(&line, &end)? != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        let mut events = Vec::with_capacity(2);
        events.extend([start.clone(), end.clone()]);
        separators.push(ClippedSeparator {
            pair: ids,
            start,
            end,
            events,
        });
    }
    Ok(separators)
}

pub(super) fn merge_owner(
    left: OwnerTransition,
    right: OwnerTransition,
) -> Result<OwnerTransition> {
    match (left, right) {
        (OwnerTransition::Preserve, value) | (value, OwnerTransition::Preserve) => Ok(value),
        (left, right) if left == right => Ok(left),
        _ => Err(voronoi_error(
            "conflicting owner transitions on one exact Voronoi atom",
        )),
    }
}

pub(super) fn fixed_coordinates(
    points: &[exact::ExactPoint],
    sites: &[Site],
    clip: &NormalizedClip<'_>,
) -> Result<Vec<Option<XY>>> {
    let mut fixed = Vec::with_capacity(points.len());
    fixed.resize(points.len(), None);
    for candidate in sites
        .iter()
        .map(|site| site.point.xy())
        .chain(clip.fixed.iter().flatten().copied())
    {
        let point = exact::ExactPoint::from_xy(candidate);
        if let Some(index) = lex_position(points, &point)? {
            if let Some(existing) = fixed[index]
                && PointKey::new(existing) != PointKey::new(candidate)
            {
                return Err(voronoi_error(
                    "one exact Voronoi vertex has conflicting source coordinates",
                ));
            }
            fixed[index] = Some(candidate);
        }
    }
    Ok(fixed)
}

/// The index of `point` in lexicographically sorted distinct `points`.
pub(super) fn lex_position(
    points: &[exact::ExactPoint],
    point: &exact::ExactPoint,
) -> Result<Option<usize>> {
    let (mut low, mut high) = (0, points.len());
    while low < high {
        let middle = low + (high - low) / 2;
        match points[middle].compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

pub(super) fn emit_atom_windows(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    events: &mut Vec<exact::ExactPoint>,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    separator: Option<[SiteId; 2]>,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<u8>,
    seeds: &mut Vec<AtomSeed>,
) -> Result<()> {
    sort_unique_along(start, end, events)?;
    let bounded = match events.as_slice() {
        [first, .., last] => first.same_position(start)? && last.same_position(end)?,
        [only] => only.same_position(start)? && only.same_position(end)?,
        [] => false,
    };
    if !bounded {
        return Err(voronoi_error(
            "exact primitive event order lost an endpoint",
        ));
    }
    for [left, right] in events.array_windows::<2>() {
        if exact::compare_along(start, end, left, right)? != Ordering::Less {
            return Err(voronoi_error(
                "exact primitive has unordered duplicate events",
            ));
        }
        seeds.push(AtomSeed {
            start: left.clone(),
            end: right.clone(),
            frame_delta,
            clip_delta,
            owner,
            separator,
            clip_source,
            frame_side,
        });
    }
    Ok(())
}

pub(super) fn register_frame_clip_contacts(scaffold: &mut [ScaffoldPrimitive]) -> Result<()> {
    let (frames, clips) = scaffold.split_at_mut(4);
    for clip_prim in clips {
        for frame_prim in frames.iter_mut() {
            match exact::segment_intersection(
                &frame_prim.start,
                &frame_prim.end,
                &clip_prim.start,
                &clip_prim.end,
            )? {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => {
                    frame_prim.events.push(point.clone());
                    clip_prim.events.push(point);
                },
                exact::SegmentIntersection::Overlap { start, end } => {
                    push_split_events(&mut frame_prim.events, &mut clip_prim.events, start, end);
                },
            }
        }
    }
    Ok(())
}

pub(super) fn push_split_events(
    left: &mut Vec<exact::ExactPoint>,
    right: &mut Vec<exact::ExactPoint>,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
) {
    left.reserve(2);
    left.extend([start.clone(), end.clone()]);
    right.reserve(2);
    right.extend([start, end]);
}

pub(super) fn node_scaffold_and_separators(
    mut scaffold: Vec<ScaffoldPrimitive>,
    mut separators: Vec<ClippedSeparator>,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    clip: &NormalizedClip<'_>,
    rect: &[exact::ExactPoint; 4],
) -> Result<NodedAtoms> {
    for separator in &mut separators {
        for primitive in &mut scaffold {
            match exact::segment_intersection(
                &separator.start,
                &separator.end,
                &primitive.start,
                &primitive.end,
            )? {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => {
                    separator.events.push(point.clone());
                    primitive.events.push(point);
                },
                exact::SegmentIntersection::Overlap { start, end } => {
                    push_split_events(&mut separator.events, &mut primitive.events, start, end);
                },
            }
        }
    }
    let mut seeds = Vec::new();
    for primitive in &mut scaffold {
        emit_atom_windows(
            &primitive.start,
            &primitive.end,
            &mut primitive.events,
            primitive.frame_delta,
            primitive.clip_delta,
            primitive.owner,
            None,
            primitive.clip_source,
            primitive.frame_side,
            &mut seeds,
        )?;
    }
    for separator in &mut separators {
        emit_atom_windows(
            &separator.start,
            &separator.end,
            &mut separator.events,
            0,
            0,
            PrimitiveOwner::Boundary {
                left: Some(separator.pair[0]),
                right: Some(separator.pair[1]),
            },
            Some(separator.pair),
            None,
            None,
            &mut seeds,
        )?;
    }
    let mut event_points = Vec::with_capacity(seeds.len().saturating_mul(2));
    for seed in &seeds {
        event_points.extend([seed.start.clone(), seed.end.clone()]);
    }
    try_sort_by(&mut event_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut event_points, exact::ExactPoint::same_position)?;
    IndexedTopology::admit_counts(event_points.len(), 0)?;
    let fixed = fixed_coordinates(&event_points, sites, clip)?;
    let vertex_id = |point: &exact::ExactPoint| -> Result<VertexId> {
        let index = lex_position(&event_points, point)?
            .ok_or_else(|| voronoi_error("exact event was not globally interned"))?;
        Ok(VertexId(index as u32))
    };
    #[derive(Clone)]
    struct MappedSeed {
        endpoints: [VertexId; 2],
        reversed: bool,
        frame_delta: i8,
        clip_delta: i32,
        owner: PrimitiveOwner,
        separator: Option<[SiteId; 2]>,
        clip_source: Option<ClipSegmentId>,
        frame_side: Option<u8>,
    }
    let mut mapped = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let mut endpoints = [vertex_id(&seed.start)?, vertex_id(&seed.end)?];
        if endpoints[0] == endpoints[1] {
            return Err(voronoi_error("exact primitive produced a zero-length atom"));
        }
        let reversed = endpoints[0] > endpoints[1];
        if reversed {
            endpoints.swap(0, 1);
        }
        mapped.push(MappedSeed {
            endpoints,
            reversed,
            frame_delta: if reversed {
                -seed.frame_delta
            } else {
                seed.frame_delta
            },
            clip_delta: if reversed {
                -seed.clip_delta
            } else {
                seed.clip_delta
            },
            owner: seed.owner,
            separator: seed.separator,
            clip_source: seed.clip_source,
            frame_side: seed.frame_side,
        });
    }
    try_sort_by(&mut mapped, |left, right| {
        Ok(left.endpoints.cmp(&right.endpoints))
    })?;
    let mut vertices = Vec::with_capacity(event_points.len());
    vertices.extend(
        event_points
            .into_iter()
            .zip(fixed)
            .map(|(point, fixed_xy)| NodedVertex { fixed_xy, point }),
    );
    let mut edges = Vec::new();
    let mut start = 0;
    let mut frame_owner = SiteId(0);
    while start < mapped.len() {
        let mut end = start + 1;
        while end < mapped.len() && mapped[end].endpoints == mapped[start].endpoints {
            end += 1;
        }
        let mut frame_delta = 0_i8;
        let mut clip_delta = 0_i32;
        let mut owner = OwnerTransition::Preserve;
        let mut separator = None;
        let mut clip_sources = Vec::new();
        let mut frame_side = None;
        for seed in &mapped[start..end] {
            frame_delta = frame_delta
                .checked_add(seed.frame_delta)
                .ok_or_else(|| voronoi_error("exact frame winding overflow"))?;
            clip_delta = clip_delta
                .checked_add(seed.clip_delta)
                .ok_or_else(|| voronoi_error("exact clip winding overflow"))?;
            if let Some(value) = seed.separator {
                if separator.is_some_and(|existing| existing != value) {
                    return Err(voronoi_error("two Voronoi separators share one open atom"));
                }
                separator = Some(value);
            }
            if let Some(value) = seed.clip_source {
                clip_sources.push(value);
            }
            if let Some(value) = seed.frame_side {
                if frame_side.is_some_and(|existing| existing != value) {
                    return Err(voronoi_error("two private frame sides share one atom"));
                }
                frame_side = Some(value);
            }
        }
        if let (Some(pair), Some(side)) = (separator, frame_side) {
            let ccw_start = &rect[side as usize];
            let ccw_end = &rect[(side as usize + 1) % 4];
            let inside = coincident_frame_owner(pair, ccw_start, ccw_end, sites)?;
            let mut coincident = OwnerTransition::Boundary {
                left: Some(inside),
                right: None,
            };
            let stored_start = &vertices[mapped[start].endpoints[0].0 as usize].point;
            let stored_end = &vertices[mapped[start].endpoints[1].0 as usize].point;
            if exact::compare_along(ccw_start, ccw_end, stored_start, stored_end)?
                == Ordering::Greater
            {
                coincident = Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: coincident,
                }
                .reversed()
                .owner;
            }
            owner = coincident;
        } else {
            for seed in &mapped[start..end] {
                let resolved = match seed.owner {
                    PrimitiveOwner::Preserve => OwnerTransition::Preserve,
                    PrimitiveOwner::Boundary { left, right } => {
                        let owner = OwnerTransition::Boundary { left, right };
                        if seed.reversed {
                            Transition {
                                frame_delta: 0,
                                clip_delta: 0,
                                owner,
                            }
                            .reversed()
                            .owner
                        } else {
                            owner
                        }
                    },
                    PrimitiveOwner::FrameNearest => {
                        let midpoint = vertices[seed.endpoints[0].0 as usize]
                            .point
                            .midpoint(&vertices[seed.endpoints[1].0 as usize].point)?;
                        frame_owner = nearest_site_walk(&midpoint, sites, adjacency, frame_owner)?;
                        let owner = OwnerTransition::Boundary {
                            left: Some(frame_owner),
                            right: None,
                        };
                        if seed.reversed {
                            Transition {
                                frame_delta: 0,
                                clip_delta: 0,
                                owner,
                            }
                            .reversed()
                            .owner
                        } else {
                            owner
                        }
                    },
                };
                owner = merge_owner(owner, resolved)?;
            }
        }
        clip_sources.sort_unstable();
        clip_sources.dedup();
        if frame_delta != 0 || clip_delta != 0 || separator.is_some() {
            edges.push(AtomicEdge {
                endpoints: mapped[start].endpoints,
                forward: Transition {
                    frame_delta,
                    clip_delta,
                    owner,
                },
                separator,
                clip_sources,
                frame_side,
            });
        }
        start = end;
    }
    Ok(NodedAtoms { vertices, edges })
}
