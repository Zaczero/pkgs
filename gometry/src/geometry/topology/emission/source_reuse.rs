use std::collections::BTreeSet;

use super::super::exact::{ExactSourceEdge, ExactSourceRing, SourceEdgeKind, SourceRingId};
use super::plan::{
    BoundaryCycle, DrawnAreaOrigin, DrawnPolygons, PlannedAreaRing, PolygonPlan, QEmissionDrawing,
};
use super::sites::{EmittedVertex, canonical_bits};
use super::{
    BTreeMap, ExactGeographicTopology, ExactRational, ExactSourceRange, FragmentRecipe,
    GeometryErrorKind, PublicationInventory, QPortId, RecipeWalk, RecipeWalkOwner, ResourceError,
    Result, SelectedUse, SourceEdgeId, try_sort_by_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology) fn source_ring_reuse_matches(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    reverse: bool,
    vertices: &[EmittedVertex<'_>],
    recipes: &[FragmentRecipe],
    walk: &RecipeWalk,
) -> Result<bool> {
    let edge_count = source_ring.source_edges.len();
    if vertices.len()
        != edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?
        || walk.fragments.len() != edge_count
    {
        return Ok(false);
    }
    let expected_range = if reverse {
        (ExactRational::one(), ExactRational::zero())
    } else {
        (ExactRational::zero(), ExactRational::one())
    };
    for index in 0..edge_count {
        let edge_index = if reverse {
            source_ring
                .source_edges
                .end
                .checked_sub(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            source_ring
                .source_edges
                .start
                .checked_add(index)
                .ok_or(ResourceError::SizeOverflow)?
        };
        let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse edge is unknown".into())
        })?;
        let SourceEdgeKind::Ring { ring, .. } = edge.kind else {
            return Ok(false);
        };
        if ring != source_ring.id {
            return Ok(false);
        }
        let recipe_id = *walk.fragments.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse recipe is unknown".into())
        })?;
        let recipe = recipes.get(recipe_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse recipe row is unknown".into())
        })?;
        let Some(range) = recipe.ranges.first() else {
            return Ok(false);
        };
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring reuse edge has no Q ports".into())
            })?;
        let expected_ports = if reverse { [ports[1], ports[0]] } else { ports };
        let start = vertices.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse has no start vertex".into())
        })?;
        let end = vertices
            .get(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring reuse has no end vertex".into())
            })?;
        let (source_start, source_end) = if reverse {
            (edge.source[1], edge.source[0])
        } else {
            edge.source.into()
        };
        if !edge.source_interval_reads_as(
            topology.sources.frame,
            reverse,
            start.point,
            end.point,
        )? {
            return Ok(false);
        }
        if recipe.use_kind != SelectedUse::AreaBoundary
            || recipe.ranges.len() != 1
            || range.source_edge != edge.id
            || range.range.start != expected_range.0
            || range.range.end != expected_range.1
            || range.producer_start != ExactRational::zero()
            || range.producer_end != ExactRational::one()
            || range.endpoint_only
            || !range.emitted
            || recipe.endpoints != expected_ports
            || start.port.id != expected_ports[0]
            || end.port.id != expected_ports[1]
            || start.point != source_start
            || end.point != source_end
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(in crate::geometry::topology) fn source_line_reuse_is_complete(
    topology: &ExactGeographicTopology,
    recipes: &[FragmentRecipe],
    publication: Option<&PublicationInventory>,
    walk: &RecipeWalk,
    vertices: &[EmittedVertex<'_>],
    budget: &mut impl Work,
) -> Result<bool> {
    let Some(&first_recipe_id) = walk.fragments.first() else {
        return Ok(false);
    };
    let Some(first_recipe) = recipes.get(first_recipe_id.0 as usize) else {
        return Err(GeometryErrorKind::Invalid(
            "source line reuse references an unknown first recipe".into(),
        )
        .into());
    };
    let Some(first_range) = first_recipe.ranges.first() else {
        return Ok(false);
    };
    let first_edge = topology
        .sources
        .edges
        .get(first_range.source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse references an unknown edge".into())
        })?;
    let SourceEdgeKind::Line { line } = first_edge.kind else {
        return Ok(false);
    };
    let source_line = topology
        .sources
        .lines
        .get(line.0 as usize)
        .filter(|source_line| source_line.id == line)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse references an unknown line".into())
        })?;
    let edge_count = source_line.source_edges.len();
    if edge_count == 0
        || walk.fragments.len() != edge_count
        || vertices.len()
            != edge_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
    {
        return Ok(false);
    }
    let reverse = if first_range.range.start.is_zero() && first_range.range.end.is_one() {
        false
    } else if first_range.range.start.is_one() && first_range.range.end.is_zero() {
        true
    } else {
        return Ok(false);
    };
    for (index, &recipe_id) in walk.fragments.iter().enumerate() {
        budget.charge_into_error(1)?;
        let recipe = recipes.get(recipe_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse references an unknown recipe".into())
        })?;
        if recipe.use_kind != SelectedUse::Curve || recipe.ranges.len() != 1 {
            return Ok(false);
        }
        let range = recipe.ranges.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("source line recipe has no root interval".into())
        })?;
        let expected_edge = if reverse {
            source_line
                .source_edges
                .end
                .checked_sub(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            source_line
                .source_edges
                .start
                .checked_add(index)
                .ok_or(ResourceError::SizeOverflow)?
        };
        let edge = topology.sources.edges.get(expected_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse edge ordinal is unknown".into())
        })?;
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source line reuse edge has no Q ports".into())
            })?;
        let expected_range = if reverse {
            (ExactRational::one(), ExactRational::zero())
        } else {
            (ExactRational::zero(), ExactRational::one())
        };
        let expected_ports = if reverse { [ports[1], ports[0]] } else { ports };
        let start_vertex = vertices.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse has no start vertex".into())
        })?;
        let end_vertex = vertices
            .get(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source line reuse has no end vertex".into())
            })?;
        let (source_start, source_end) = if reverse {
            (edge.source[1], edge.source[0])
        } else {
            edge.source.into()
        };
        if !edge.source_interval_reads_as(
            topology.sources.frame,
            reverse,
            start_vertex.point,
            end_vertex.point,
        )? {
            return Ok(false);
        }
        let (fragment_matches, samples_have_source) = if let Some(publication) = publication {
            let fragment = publication
                .fragments
                .get(recipe_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line reuse has no published fragment".into())
                })?;
            let mut samples_have_source = true;
            for sample_index in fragment.sample_range.clone() {
                budget.charge_into_error(1)?;
                let sample = publication.samples.get(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line sample row is unknown".into())
                })?;
                samples_have_source &= sample.family.fragment_recipe() == Some(recipe_id)
                    && sample.source_trace.is_some();
            }
            (fragment.recipe == recipe_id, samples_have_source)
        } else {
            (true, true)
        };
        if range.source_edge != edge.id
            || range.range.start != expected_range.0
            || range.range.end != expected_range.1
            || range.producer_start != ExactRational::zero()
            || range.producer_end != ExactRational::one()
            || range.endpoint_only
            || !range.emitted
            || recipe.endpoints != expected_ports
            || !fragment_matches
            || !samples_have_source
            || start_vertex.port.id != expected_ports[0]
            || end_vertex.port.id != expected_ports[1]
            || start_vertex.point.x.to_bits() != source_start.x.to_bits()
            || start_vertex.point.y.to_bits() != source_start.y.to_bits()
            || end_vertex.point.x.to_bits() != source_end.x.to_bits()
            || end_vertex.point.y.to_bits() != source_end.y.to_bits()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(in crate::geometry::topology) fn cycle_belongs_to_source_ring(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    source_ring: SourceRingId,
    work: &mut impl Work,
) -> Result<bool> {
    let q = &topology.q;
    for &dart_id in &cycle.darts {
        work.charge_into_error(1)?;
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
            work.charge_into_error(1)?;
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

pub(in crate::geometry::topology) fn source_cycles_by_ring(
    topology: &ExactGeographicTopology,
    cycles: &[BoundaryCycle],
    work: &mut impl Work,
) -> Result<Vec<Vec<usize>>> {
    let mut output = vec![Vec::new(); topology.sources.rings.len()];
    for (cycle_index, cycle) in cycles.iter().enumerate() {
        work.charge_into_error(1)?;
        let mut common_rings: Option<BTreeSet<SourceRingId>> = None;
        for &dart_id in &cycle.darts {
            work.charge_into_error(1)?;
            let dart =
                topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary dart is unknown".into())
                })?;
            let span =
                topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary span is unknown".into())
                })?;
            let mut span_rings = BTreeSet::new();
            for owner in &span.owners {
                work.charge_into_error(1)?;
                if let SourceEdgeKind::Ring { ring, .. } =
                    source_edge_at(topology, owner.source_edge)?.kind
                {
                    span_rings.insert(ring);
                }
            }
            if let Some(common_rings) = &mut common_rings {
                work.charge_into_error(
                    u64::try_from(common_rings.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                common_rings.retain(|ring| span_rings.contains(ring));
            } else {
                common_rings = Some(span_rings);
            }
            if common_rings.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        if let Some(common_rings) = common_rings {
            for ring in common_rings {
                work.charge_into_error(1)?;
                let ring_index =
                    usize::try_from(ring.0).map_err(|_| ResourceError::SizeOverflow)?;
                output
                    .get_mut(ring_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary ring owner is unknown".into())
                    })?
                    .push(cycle_index);
            }
        }
    }
    Ok(output)
}

pub(in crate::geometry::topology) fn source_cycle_start_port(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    work: &mut impl Work,
) -> Result<QPortId> {
    for &dart_id in &cycle.darts {
        work.charge_into_error(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cycle start references an unknown dart".into())
        })?;
        let port = topology
            .q
            .ports
            .get(dart.from_port.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cycle start references an unknown port".into())
            })?;
        if !port.cut {
            return Ok(dart.from_port);
        }
    }
    for &dart_id in &cycle.darts {
        work.charge_into_error(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cycle start references an unknown dart".into())
        })?;
        let port = topology
            .q
            .ports
            .get(dart.to_port.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cycle start references an unknown port".into())
            })?;
        if !port.cut {
            return Ok(dart.to_port);
        }
    }
    Err(GeometryErrorKind::Invalid(
        "Q area cycle has no source node to anchor its native port order".into(),
    )
    .into())
}

pub(in crate::geometry::topology) fn source_ring_cycles_follow_source(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
    work: &mut impl Work,
) -> Result<Option<bool>> {
    let q = &topology.q;
    let mut follows_source = None;
    for &cycle_index in cycle_indices {
        work.charge_into_error(1)?;
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            work.charge_into_error(1)?;
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
                work.charge_into_error(1)?;
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

pub(in crate::geometry::topology) fn source_ring_cycles_cover_boundary(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
    work: &mut impl Work,
) -> Result<bool> {
    if source_ring.summary.exact_axis_band {
        let axis = topology.sources.frame.longitude();
        let south = canonical_bits(source_ring.summary.south.get());
        let north = canonical_bits(source_ring.summary.north.get());
        let mut parallel_edges = Vec::new();
        let mut parallel_mask = 0_u8;
        for edge_index in source_ring.source_edges.clone() {
            work.charge_into_error(1)?;
            let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
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
            work.charge_into_error(1)?;
            let mut ranges = Vec::new();
            for &cycle_index in cycle_indices {
                work.charge_into_error(1)?;
                let cycle = all_cycles.get(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into())
                })?;
                for &dart_id in &cycle.darts {
                    work.charge_into_error(1)?;
                    let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q axis-band dart is unknown".into())
                    })?;
                    let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q axis-band span is unknown".into())
                    })?;
                    for owner in &span.owners {
                        work.charge_into_error(1)?;
                        if owner.source_edge == edge_id {
                            work.charge_into_error(1)?;
                            ranges.push(owner.source_range.clone());
                        }
                    }
                }
            }
            if !source_ranges_cover_unit_interval(&mut ranges, work)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    let mut ranges_by_edge = BTreeMap::<SourceEdgeId, Vec<ExactSourceRange>>::new();
    for &cycle_index in cycle_indices {
        work.charge_into_error(1)?;
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            work.charge_into_error(1)?;
            let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-ring dart is unknown".into())
            })?;
            let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-ring span is unknown".into())
            })?;
            for owner in &span.owners {
                work.charge_into_error(1)?;
                let edge = source_edge_at(topology, owner.source_edge)?;
                if matches!(
                    edge.kind,
                    SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id
                ) {
                    ranges_by_edge
                        .entry(edge.id)
                        .or_default()
                        .push(owner.source_range.clone());
                }
            }
        }
    }
    for edge_index in source_ring.source_edges.clone() {
        work.charge_into_error(1)?;
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        if matches!(edge.kind, SourceEdgeKind::Ring {
            kind: crate::geometry::RingKind::Collapsed,
            ..
        }) {
            ranges_by_edge.remove(&edge.id);
            continue;
        }
        let Some(ranges) = ranges_by_edge.get_mut(&edge.id) else {
            return Ok(false);
        };
        if !source_ranges_cover_unit_interval(ranges, work)? {
            return Ok(false);
        }
        ranges_by_edge.remove(&edge.id);
    }
    Ok(ranges_by_edge.is_empty())
}

fn source_ranges_cover_unit_interval(
    ranges: &mut [ExactSourceRange],
    work: &mut impl Work,
) -> Result<bool> {
    try_sort_by_with_work(ranges, work, |left, right, work| {
        work.charge_into_error(1)?;
        left.start.try_cmp(&right.start)
    })?;
    let mut covered = ExactRational::zero();
    let one = ExactRational::one();
    for range in ranges {
        work.charge_into_error(1)?;
        if range.start.try_cmp(&covered)?.is_gt() {
            return Ok(false);
        }
        if range.end.try_cmp(&covered)?.is_gt() {
            covered = range.end.clone();
        }
    }
    Ok(covered.try_cmp(&one)?.is_ge())
}

pub(in crate::geometry::topology) fn source_edge_at(
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

impl super::plan::QEmissionPlan {
    pub(in crate::geometry::topology) fn source_reuse_is_proven(
        &self,
        topology: &ExactGeographicTopology,
        drawing: &QEmissionDrawing<'_>,
        source_points: &[Option<EmittedVertex<'_>>],
        work: &mut impl Work,
    ) -> Result<bool> {
        let walks = super::index_recipe_walks(self.fragment_recipes.walks(), work)?;
        let mut cursors = BTreeMap::new();
        match (&self.polygons, &drawing.polygons) {
            (PolygonPlan::WholeSphere(selected), DrawnPolygons::WholeSphere(polygons)) => {
                if selected.len() != polygons.len() {
                    return Ok(false);
                }
                for (&polygon_index, rings) in selected.iter().zip(polygons) {
                    work.charge_into_error(1)?;
                    let source_polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse polygon is unknown".into(),
                                )
                            })?;
                    if rings.len() != source_polygon.rings.len() {
                        return Ok(false);
                    }
                    for (ring_index, drawn) in source_polygon.rings.clone().zip(rings) {
                        work.charge_into_error(1)?;
                        let source_ring =
                            topology.sources.rings.get(ring_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse ring is unknown".into(),
                                )
                            })?;
                        if source_ring.polygon.0 as usize != polygon_index
                            || drawn.hole != source_ring.hole
                            || drawn.recipe_walk != RecipeWalkOwner::SourceRing(ring_index, None)
                            || !source_ring_reuse_matches(
                                topology,
                                source_ring,
                                false,
                                &drawn.vertices,
                                self.fragment_recipes.as_slice(),
                                super::next_recipe_walk(
                                    &walks,
                                    &mut cursors,
                                    drawn.recipe_walk,
                                    work,
                                )?,
                            )?
                        {
                            return Ok(false);
                        }
                    }
                }
            },
            (
                PolygonPlan::Regions { rings: planned, .. },
                DrawnPolygons::Regions { rings: drawn, .. },
            ) => {
                if planned.len() != drawn.len() {
                    return Ok(false);
                }
                for (planned, drawn) in planned.iter().zip(drawn) {
                    work.charge_into_error(1)?;
                    let walk =
                        super::next_recipe_walk(&walks, &mut cursors, drawn.recipe_walk, work)?;
                    let PlannedAreaRing::Source {
                        ring,
                        region,
                        reverse,
                        cycle_start,
                    } = *planned
                    else {
                        return Ok(false);
                    };
                    let source_ring = topology.sources.rings.get(ring).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source reuse ring is unknown".into())
                    })?;
                    let source_ring_matches = source_ring_reuse_matches(
                        topology,
                        source_ring,
                        reverse,
                        &drawn.vertices,
                        self.fragment_recipes.as_slice(),
                        walk,
                    )?;
                    if !matches!(drawn.origin, DrawnAreaOrigin::Source)
                        || drawn.region != region
                        || drawn.recipe_walk != RecipeWalkOwner::SourceRing(ring, Some(region))
                        || drawn.native_start_port != cycle_start
                        || !source_ring_matches
                    {
                        return Ok(false);
                    }
                }
            },
            _ => return Ok(false),
        }
        if self.line_paths.len() != drawing.lines.len()
            || self.point_nodes.len() != drawing.points.len()
        {
            return Ok(false);
        }
        for (index, (planned, drawn)) in self.line_paths.iter().zip(&drawing.lines).enumerate() {
            work.charge_into_error(1)?;
            let owner = RecipeWalkOwner::Line(index);
            if planned.recipe_walk != owner || drawn.recipe_walk != owner {
                return Ok(false);
            }
            let walk = super::next_recipe_walk(&walks, &mut cursors, owner, work)?;
            if !source_line_reuse_is_complete(
                topology,
                self.fragment_recipes.as_slice(),
                None,
                walk,
                &drawn.path,
                work,
            )? {
                return Ok(false);
            }
        }
        for (&node, drawn) in self.point_nodes.iter().zip(&drawing.points) {
            work.charge_into_error(1)?;
            let source = source_points.get(node.0 as usize).copied().flatten();
            if source.is_none_or(|source| {
                source.port.node != node
                    || source.port.id != drawn.port.id
                    || source.point != drawn.point
            }) {
                return Ok(false);
            }
        }
        for (owner, owner_walks) in &walks {
            work.charge_into_error(1)?;
            if cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
