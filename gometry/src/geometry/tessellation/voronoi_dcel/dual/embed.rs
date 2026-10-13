//! Certified rounding and publication for direct-dual rings.

use super::{
    CoordSeq, DirectedDualEdge, DualEdgeDraft, DualEmbeddingError, DualRings, DualVertex, EdgeTag,
    EmbeddedEdgeData, EmbeddedEdges, EmbeddedPolygon, EmbeddedVoronoi, ExactSign, ExpansionBudget,
    FrameSide, NormalizedClip, Ordering, OverlayNodeReceipt, Point, PointKey, PointSelectionChart,
    Polygon, Ring, Site, VoronoiAssignmentReceipt, VoronoiCellReceipt, VoronoiClipAtomReceipt,
    VoronoiDartReceipt, VoronoiDirectedEdgeUse, VoronoiEdgeOrigin, VoronoiEdgeReceipt,
    VoronoiEdgeSourceUse, VoronoiProducer, VoronoiProduct, VoronoiRingEdgeRole,
    VoronoiSeparatorReceipt, VoronoiVertexOrigin, VoronoiVertexReceipt, XY, certify_convex_cell,
    chart_vertex_origins, clip_segment_receipts, dual_clip_segment_for_edge, dual_vertex_point,
    ensure_coordseq_vertex_capacity, exact, exact_xy_coordinates, lex_xy, polygon_is_valid,
    polygon_output_count, receipt_frame_side, same_undirected_dual_edge, site_receipts,
    stored_xy_cycle_matches_orientation, try_hash_map_with_capacity,
};

/// Round the dual once and run the certificates that survive the redesign.
///
/// What is UNCHANGED from `embed`: the sole nearest-even rounding, the global
/// injectivity check over distinct exact vertices, the per-ring orientation
/// check against the rounded ring, and `polygon_is_valid` per cell.
///
/// What is GONE, because the structure it certified no longer exists: the
/// arrangement's rounded no-crossing scan and the per-vertex exact-vs-rounded
/// fan-order comparison. The dual never builds that arrangement. Instead,
/// rounded edge cancellation proves that the valid cells have exactly the
/// rectangular clip boundary, and exact point membership checks each canonical
/// site's rounded cell. Polygon clips remain on the exact arrangement lane.
pub(in crate::geometry::tessellation::voronoi_dcel) fn dual_embed(
    rings: &DualRings,
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    product: VoronoiProduct,
    budget: &mut ExpansionBudget,
    chart: Option<&PointSelectionChart>,
) -> std::result::Result<EmbeddedVoronoi, DualEmbeddingError> {
    #[expect(
        clippy::collapsible_if,
        reason = "keep the center-only memo lookup on its measured base instruction path"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "center components index the admitted center map; dense IDs occupy its prefix or are assigned immediately before their coordinate is published, and the intern table only returns those IDs"
    )]
    fn resolve<'r>(
        vertex: &'r DualVertex,
        centers: &'r [exact::ExactPoint],
        center_rounded: &mut [Option<(XY, u32)>],
        exact_by_id: &mut Vec<exact::ExactPoint>,
        interned: &mut crate::collections::HashMap<PointKey, (&'r exact::ExactPoint, u32)>,
        vertex_xy: &mut Vec<XY>,
    ) -> std::result::Result<(XY, u32), DualEmbeddingError> {
        if let DualVertex::Center(component) = *vertex {
            if let Some(memo) = center_rounded[component] {
                return Ok(memo);
            }
        }
        let point = dual_vertex_point(vertex, centers);
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        let id = match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                let &(representative, id) = existing.get();
                if !representative.same_position(point)? {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                // Equal exact generated vertices can reach signed zero through
                // different algebraic expressions. Reuse the first admitted
                // representative so shared binary64 vertices are bit-identical.
                xy = representative.round_nearest_even()?;
                id
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                let id = u32::try_from(vertex_xy.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                vertex_xy.push(xy);
                exact_by_id.push(point.clone());
                slot.insert((point, id));
                id
            },
        };
        if let DualVertex::Center(component) = *vertex {
            center_rounded[component] = Some((xy, id));
        }
        Ok((xy, id))
    }

    if rings.rings.len() != sites.len() {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    // Vertex identity WITHOUT an exact positional sort.
    //
    // A `Center` vertex IS its component, so its rounding is memoised once per
    // component. Everything else is hashed on its ROUNDED key, and a key
    // collision is then resolved EXACTLY: two exact points that share a rounded
    // key are the same vertex only if `same_position` says so, and otherwise the
    // rounding is not injective and the diagram is refused. That is the same
    // verdict the sort produced -- rounding is a function, so equal exact points
    // always land in the same bucket -- reached in O(V) instead of O(V log V)
    // exact comparisons.
    // Every distinct exact vertex gets one dense id. Center component IDs
    // remain separate producer identities and map to these interned vertices;
    // a clip hit may be the first occurrence of a center's exact position.
    if let Some(last) = rings.centers.len().checked_sub(1) {
        u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    }
    let total = rings.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut center_rounded: Vec<Option<(XY, u32)>> = Vec::with_capacity(rings.centers.len());
    center_rounded.resize(rings.centers.len(), None);
    let mut exact_by_id = Vec::new();
    // Distinct vertices are the centers plus a boundary's worth of generated
    // points; sizing for every ring reference would triple the table.
    let mut interned = try_hash_map_with_capacity(rings.centers.len())?;
    let mut vertex_xy: Vec<XY> = Vec::with_capacity(rings.centers.len());
    let mut offsets = Vec::new();
    let mut rounded_all: Vec<XY> = Vec::new();
    let mut ids_all: Vec<u32> = Vec::new();
    let mut edge_tags = rings.edge_tags.clone();
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the offset column has one terminal sentinel in addition to one row per existing non-zero-sized ring"
    )]
    offsets.reserve(rings.rings.len() + 1);
    rounded_all.reserve(total);
    ids_all.reserve(total);
    offsets.push(0);
    for ring in &rings.rings {
        for vertex in ring {
            let (xy, id) = resolve(
                vertex,
                &rings.centers,
                &mut center_rounded,
                &mut exact_by_id,
                &mut interned,
                &mut vertex_xy,
            )?;
            rounded_all.push(xy);
            ids_all.push(id);
        }
        offsets.push(rounded_all.len());
    }
    for (ring_index, (window, site)) in offsets.array_windows::<2>().zip(sites).enumerate() {
        let [row_start, row_end] = *window;
        let rounded = rounded_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        // The ring starts at its lexicographically smallest rounded vertex,
        // the canonical form both lanes publish; rounding is injective here,
        // so the minimum is unique.
        let rotation = rounded
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| lex_xy(**left, **right))
            .map_or(0, |(index, _)| index);
        rounded.rotate_left(rotation);
        ids_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .rotate_left(rotation);
        let tags = edge_tags
            .get_mut(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        if tags.len() != rounded.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        tags.rotate_left(rotation);
        let site_xy = site.rounded_xy()?;
        let first = rounded
            .first()
            .expect("dual_build_rings admits only cells with at least three vertices");
        let closed = rounded.iter().chain(std::iter::once(first));
        if !certify_convex_cell(rounded, site_xy) {
            if !stored_xy_cycle_matches_orientation(rounded, ExactSign::Positive) {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let mut rounded_ring = Vec::with_capacity(rounded.len());
            rounded_ring.extend(rounded.iter().map(|&xy| exact::ExactPoint::from_xy(xy)));
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut points = Vec::with_capacity(capacity);
            points.extend(closed.clone().map(|xy| Point::new_raw_xy(xy.x, xy.y)));
            if !polygon_is_valid(&Polygon::new(Ring::closed(points)?, Vec::new()))? {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            if exact::incidence::point_in_cycle(&rounded_ring, &site.exact_point)?
                == exact::PointInCycle::Outside
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
        }
    }
    // Completeness by EDGE CANCELLATION rather than by a full overlay union.
    //
    // Every rounded cell edge is entered directed. An edge interior to the
    // partition is walked once in each direction by its two owners and cancels;
    // an edge on the outer boundary is walked exactly once. So: if every
    // undirected edge occurs either twice with opposite directions or once, and
    // the once-occurring edges tile the clip rectangle's four sides exactly with
    // no gap and no overlap, then the union of the cells IS the clip rectangle.
    // Combined with positive simple-cell validation and exact site ownership,
    // this is the same partition statement the union/difference pair made,
    // computed on stored binary64 with no overlay. Winding-number membership
    // alone cannot prove this: a gap and an overlap can leave every tested site
    // inside its own cell, while cancellation and directed boundary tiling
    // reject both.
    // Sort by the undirected endpoint pair, then direction, so the two owners
    // of each interior edge become an adjacent pair.
    let mut directed: Vec<DirectedDualEdge> = Vec::with_capacity(total);
    let mut by_dart: Vec<Option<DirectedDualEdge>> = Vec::with_capacity(total);
    by_dart.resize(total, None);
    let mut clip_uses_by_dart = Vec::with_capacity(total);
    let mut next_dart = 0_u32;
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        #[expect(
            clippy::indexing_slicing,
            reason = "each row offset was pushed around appending this same ring's dense vertex IDs"
        )]
        let ids = &ids_all[start..end];
        let tags = edge_tags
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        if ids.len() != tags.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        if ids.is_empty() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        for (edge_index, (&from, &to)) in ids.iter().zip(ids.iter().cycle().skip(1)).enumerate() {
            let from_xy = *vertex_xy
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let to_xy = *vertex_xy
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let (endpoints, forward) = if lex_xy(from_xy, to_xy).is_lt() {
                ([from, to], true)
            } else {
                ([to, from], false)
            };
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let exact_from = exact_by_id
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let exact_to = exact_by_id
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let clip_use = dual_clip_segment_for_edge(clip, exact_from, exact_to)?;
            let clip_segment = clip_use.as_ref().map(|use_| use_.segment);
            let tag = *tags
                .get(edge_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let origin = match tag {
                EdgeTag::Bisector(neighbor) => {
                    let mut pair = [owner, neighbor.0];
                    pair.sort_unstable();
                    VoronoiEdgeOrigin::Bisector { sites: pair }
                },
                EdgeTag::Frame(_) if clip.source.is_some() => {
                    let segment = clip_segment.ok_or(DualEmbeddingError::Unrepresentable)?;
                    VoronoiEdgeOrigin::SourceClip { segment }
                },
                EdgeTag::Frame(side) => VoronoiEdgeOrigin::Frame {
                    side: receipt_frame_side(side),
                },
            };
            let edge = DirectedDualEdge {
                low,
                high,
                endpoints,
                from,
                to,
                forward,
                owner,
                origin,
                clip_segment,
                dart: next_dart,
            };
            next_dart = next_dart
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            *by_dart
                .get_mut(edge.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(edge);
            clip_uses_by_dart.push(clip_use);
            directed.push(edge);
        }
    }
    if next_dart as usize != total {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    directed.sort_unstable();
    let [min_x, max_x, min_y, max_y] = clip.frame_bounds();
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let [bottom, right, top, left] = &mut sides;
    // Each interior edge takes two directed entries.
    let mut internal_edge_count = 0_usize;
    let mut edge_drafts = Vec::with_capacity(directed.len());
    for run in directed.chunk_by(|left, right| same_undirected_dual_edge(*left, *right)) {
        let (first, second) = match run {
            [left, right] => {
                if left.forward == right.forward
                    || left.owner == right.owner
                    || left.origin != right.origin
                    || left.clip_segment != right.clip_segment
                {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                internal_edge_count = internal_edge_count
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                (*left, Some(*right))
            },
            [edge] => (*edge, None),
            _ => return Err(DualEmbeddingError::Unrepresentable),
        };
        let coordinates = [
            *vertex_xy
                .get(first.endpoints[0] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
            *vertex_xy
                .get(first.endpoints[1] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        ];
        edge_drafts.push(DualEdgeDraft {
            endpoints: first.endpoints,
            coordinates,
            first,
            second,
        });
        if second.is_some() {
            continue;
        }
        let [a, b] = coordinates;
        let (a, b) = if first.forward { (a, b) } else { (b, a) };
        let side = if a.y == min_y && b.y == min_y {
            FrameSide::Bottom
        } else if a.x == max_x && b.x == max_x {
            FrameSide::Right
        } else if a.y == max_y && b.y == max_y {
            FrameSide::Top
        } else if a.x == min_x && b.x == min_x {
            FrameSide::Left
        } else {
            return Err(DualEmbeddingError::Unrepresentable);
        };
        let correctly_directed = match side {
            FrameSide::Bottom => a.x < b.x,
            FrameSide::Right => a.y < b.y,
            FrameSide::Top => a.x > b.x,
            FrameSide::Left => a.y > b.y,
        };
        if !correctly_directed {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        let (lo, hi) = match side {
            FrameSide::Bottom | FrameSide::Top => (a.x.min(b.x), a.x.max(b.x)),
            FrameSide::Right | FrameSide::Left => (a.y.min(b.y), a.y.max(b.y)),
        };
        match side {
            FrameSide::Bottom => &mut *bottom,
            FrameSide::Right => &mut *right,
            FrameSide::Top => &mut *top,
            FrameSide::Left => &mut *left,
        }
        .push((lo, hi));
    }
    for (frame_side, side) in FrameSide::ALL.into_iter().zip([bottom, right, top, left]) {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = match frame_side {
            FrameSide::Bottom | FrameSide::Top => (min_x, max_x),
            FrameSide::Right | FrameSide::Left => (min_y, max_y),
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            if lo != cursor {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            cursor = hi;
        }
        if cursor != end {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    edge_drafts.sort_unstable_by(|left, right| {
        lex_xy(left.coordinates[0], right.coordinates[0])
            .then(lex_xy(left.coordinates[1], right.coordinates[1]))
    });
    let mut edge_id_by_pair = try_hash_map_with_capacity(edge_drafts.len())?;
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge = u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if edge_id_by_pair
            .insert((draft.first.low, draft.first.high), edge)
            .is_some()
        {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    let mut published = 0_usize;
    if product.includes_polygons() {
        for ring in &rings.rings {
            let positions = ring
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let cell_count = polygon_output_count(positions, std::iter::empty())?;
            published = published
                .checked_add(cell_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    if product.includes_edges() {
        let edge_positions = internal_edge_count
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        published = published
            .checked_add(edge_positions)
            .and_then(|count| count.checked_add(internal_edge_count))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let baseline = if product.includes_polygons() {
        clip.polygon_baseline.unwrap_or(0)
    } else {
        0
    };
    budget.add(published.saturating_sub(baseline))?;

    let mut receipt = site_receipts(VoronoiProducer::Dual, sites, chart)?;
    receipt.clip_segments = clip_segment_receipts(clip, None)?;
    let mut vertex_origins: Vec<Vec<VoronoiVertexOrigin>> = std::iter::repeat_with(Vec::new)
        .take(vertex_xy.len())
        .collect();
    for (component, rounded) in center_rounded.iter().enumerate() {
        if let Some((_, id)) = rounded {
            let sites = *rings
                .center_sites
                .get(component)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            vertex_origins
                .get_mut(*id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::Circumcenter {
                    component: u32::try_from(component)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    sites,
                });
        }
    }
    let mut twin_by_dart = Vec::with_capacity(directed.len());
    twin_by_dart.resize(directed.len(), None);
    let mut internal_edges = Vec::with_capacity(if product.includes_edges() {
        internal_edge_count
    } else {
        0
    });
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge_id =
            u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if let Some(second) = draft.second {
            *twin_by_dart
                .get_mut(draft.first.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(second.dart);
            *twin_by_dart
                .get_mut(second.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(draft.first.dart);
        }
        let [start_id, end_id] = draft.endpoints;
        let [start, end] = draft.coordinates;
        let mut origins = vec![draft.first.origin];
        let source_segment = if clip.source.is_some() {
            draft.first.clip_segment
        } else {
            None
        };
        if let Some(segment) = source_segment {
            let source_origin = VoronoiEdgeOrigin::SourceClip { segment };
            if !origins.contains(&source_origin) {
                origins.push(source_origin);
            }
        }
        let separator = match draft.first.origin {
            VoronoiEdgeOrigin::Bisector { sites } => Some(sites),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Frame { .. } => None,
        };
        let frame_side = match draft.first.origin {
            VoronoiEdgeOrigin::Frame { side } => Some(side),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Bisector { .. } => None,
        };
        let mut owner_transition = [None, None];
        for use_edge in [Some(draft.first), draft.second].into_iter().flatten() {
            let [left_owner, right_owner] = &mut owner_transition;
            if use_edge.forward {
                *left_owner = Some(use_edge.owner);
            } else {
                *right_owner = Some(use_edge.owner);
            }
        }
        receipt.edges.push(VoronoiEdgeReceipt {
            id: edge_id,
            endpoints: [start_id, end_id],
            separator,
            clip_sources: source_segment.into_iter().collect(),
            frame_side,
            owner_transition,
            origins,
        });
        if let Some(sites) = separator {
            receipt.separators.push(VoronoiSeparatorReceipt {
                sites,
                vertices: [start_id, end_id].to_vec(),
                edges: vec![edge_id],
            });
            if product.includes_edges() {
                internal_edges.push([start, end]);
            }
        }
        if let Some(segment) = draft.first.clip_segment {
            let clip_use = clip_uses_by_dart
                .get(draft.first.dart as usize)
                .and_then(Option::as_ref)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if clip_use.segment != segment {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let (start_parameter, end_parameter) = if draft.first.forward {
                (
                    clip_use.start_parameter.clone(),
                    clip_use.end_parameter.clone(),
                )
            } else {
                (
                    clip_use.end_parameter.clone(),
                    clip_use.start_parameter.clone(),
                )
            };
            let reversed = start_parameter.try_cmp(&end_parameter)? == Ordering::Greater;
            let (low_parameter, high_parameter) = if reversed {
                (end_parameter.clone(), start_parameter.clone())
            } else {
                (start_parameter.clone(), end_parameter.clone())
            };
            let clip_segment = receipt
                .clip_segments
                .get_mut(segment as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            clip_segment.atoms.push(VoronoiClipAtomReceipt {
                edge: edge_id,
                reversed,
                start_parameter: low_parameter,
                end_parameter: high_parameter,
            });
            if clip.source.is_some() {
                for (vertex_id, parameter) in
                    [(start_id, &start_parameter), (end_id, &end_parameter)]
                {
                    for root in &clip_segment.source_roots {
                        let source_parameter = if root.reversed {
                            exact::ExactRational::one().try_subtract(parameter)?
                        } else {
                            parameter.clone()
                        };
                        vertex_origins
                            .get_mut(vertex_id as usize)
                            .ok_or(DualEmbeddingError::Unrepresentable)?
                            .push(VoronoiVertexOrigin::SourceClip {
                                segment,
                                parameter: source_parameter,
                                source_ring: Some(root.ring),
                                source_edge: Some(root.edge),
                            });
                    }
                }
            }
        }
        for vertex_id in [start_id, end_id] {
            if let Some(sites) = separator {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Bisector { sites });
            }
            if let Some(side) = frame_side {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Frame { side });
            }
        }
    }
    for site in sites {
        let site_id =
            u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let key = PointKey::new(site.rounded_xy()?);
        if let Some((representative, vertex_id)) = interned.get(&key)
            && representative.same_position(&site.exact_point)?
        {
            vertex_origins
                .get_mut(*vertex_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::SourceSite { site: site_id });
        }
    }
    for (id, exact_point) in exact_by_id.iter().enumerate() {
        vertex_origins
            .get_mut(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .extend(chart_vertex_origins(chart, exact_point)?);
        let embedded = *vertex_xy
            .get(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let origins = std::mem::take(
            vertex_origins
                .get_mut(id)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        );
        receipt.vertices.push(VoronoiVertexReceipt {
            id: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            exact_xy: exact_xy_coordinates(exact_point)?,
            origins,
            assignment: Some(embedded),
        });
        receipt.assignments.push(VoronoiAssignmentReceipt {
            vertex: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            embedded: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            point: embedded,
        });
    }
    let mut edge_roles_by_ring = Vec::with_capacity(rings.rings.len());
    let mut dart_receipts = Vec::with_capacity(directed.len());
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        let ids = ids_all
            .get(start..end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        let mut cell_uses = Vec::with_capacity(ids.len());
        let mut role_row = Vec::with_capacity(ids.len());
        for (&from, &to) in ids.iter().zip(ids.iter().cycle().skip(1)) {
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let edge_id = *edge_id_by_pair
                .get(&(low, high))
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let dart_id = u32::try_from(dart_receipts.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let directed_edge = by_dart
                .get(dart_id as usize)
                .and_then(|edge| *edge)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if directed_edge.from != from || directed_edge.to != to || directed_edge.owner != owner
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let forward = directed_edge.forward;
            let edge_record = receipt
                .edges
                .get(edge_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let mut source_uses = Vec::new();
            if let Some(segment) = directed_edge.clip_segment
                && clip.source.is_some()
            {
                let clip_use = clip_uses_by_dart
                    .get(dart_id as usize)
                    .and_then(Option::as_ref)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                if clip_use.segment != segment {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                let start_parameter = &clip_use.start_parameter;
                let end_parameter = &clip_use.end_parameter;
                let clip_segment = receipt
                    .clip_segments
                    .get(segment as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                for root in &clip_segment.source_roots {
                    let source_start = if root.reversed {
                        exact::ExactRational::one().try_subtract(start_parameter)?
                    } else {
                        start_parameter.clone()
                    };
                    let source_end = if root.reversed {
                        exact::ExactRational::one().try_subtract(end_parameter)?
                    } else {
                        end_parameter.clone()
                    };
                    source_uses.push(VoronoiEdgeSourceUse {
                        source_segment: segment,
                        source_operand: None,
                        source_ring: Some(
                            u32::try_from(root.ring)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        source_edge: Some(
                            u32::try_from(root.edge)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        forward: source_start.try_cmp(&source_end)? != Ordering::Greater,
                        start_parameter: source_start,
                        end_parameter: source_end,
                    });
                }
            }
            role_row.push(VoronoiRingEdgeRole {
                edge: Some(edge_id),
                forward,
                origins: edge_record.origins.clone(),
                source_uses,
            });
            cell_uses.push(VoronoiDirectedEdgeUse {
                edge: edge_id,
                forward,
            });
            dart_receipts.push(VoronoiDartReceipt {
                id: dart_id,
                origin: from,
                target: to,
                twin: twin_by_dart.get(dart_id as usize).copied().flatten(),
                edge: edge_id,
                face: None,
                owner: Some(owner),
            });
        }
        receipt.cells.push(VoronoiCellReceipt {
            owner,
            face: None,
            shell: cell_uses,
            holes: Vec::new(),
        });
        edge_roles_by_ring.push(role_row);
    }
    receipt.darts = dart_receipts;
    let edge_data = EmbeddedEdgeData::Segments(internal_edges);
    let mut polygons = Vec::new();
    if product.includes_polygons() {
        polygons.reserve(rings.rings.len());
        for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
            let [start, end] = *window;
            let rounded = rounded_all
                .get(start..end)
                .expect("row offsets bracket appends to this rounded vertex column");
            let first = rounded
                .first()
                .expect("dual_build_rings admits only cells with at least three vertices");
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut xs = Vec::with_capacity(capacity);
            let mut ys = Vec::with_capacity(capacity);
            xs.extend(rounded.iter().map(|xy| xy.x));
            ys.extend(rounded.iter().map(|xy| xy.y));
            xs.push(first.x);
            ys.push(first.y);
            let ring = Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None)?)?;
            let edge_roles = vec![
                edge_roles_by_ring
                    .get(ring_index)
                    .cloned()
                    .ok_or(DualEmbeddingError::Unrepresentable)?,
            ];
            let producer_ids = ids_all
                .get(start..end)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let producer_vertices = vec![
                producer_ids
                    .iter()
                    .map(|&id| {
                        Ok(OverlayNodeReceipt {
                            id,
                            exact_point: exact_by_id
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?
                                .clone(),
                            assignment: *vertex_xy
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?,
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, DualEmbeddingError>>()?,
            ];
            let owner = rings
                .owners
                .get(ring_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .0;
            polygons.push(EmbeddedPolygon {
                polygon: Polygon::new(ring, Vec::new()),
                owner: Some(owner),
                edge_roles,
                producer_vertices,
            });
        }
    }
    let receipt = std::sync::Arc::new(receipt);
    Ok(EmbeddedVoronoi {
        polygons,
        edges: EmbeddedEdges {
            data: edge_data,
            receipt: std::sync::Arc::clone(&receipt),
        },
        receipt,
    })
}
