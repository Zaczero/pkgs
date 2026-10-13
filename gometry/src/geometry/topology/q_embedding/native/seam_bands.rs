use super::super::{
    ChartPole, ExactChartPoint, ExactRational, ExactSign, GeographicFrame, GeometryErrorKind,
    NativeAreaEdge, NativeAreaEdgeKind, NativeRayTransform, NativeReferenceGraph,
    NativeReferenceGraphBuilder, NativeReferenceRayPoints, NativeSeamEvent, ResourceError, Result,
    SelectedNativeSeamBand, XY, point_on_exact_segment_with_work, try_sort_by_with_work,
    vector_cross_with_work, vector_difference_with_work,
};
use crate::geometry::graph::shoot_left_with_work;
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn native_region_at_bank(
    graph: &NativeReferenceGraphBuilder,
    area_edges: &[NativeAreaEdge],
    query: &ExactChartPoint,
    bank: super::super::super::emission::ChartSide,
    hit: Option<usize>,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<Option<usize>> {
    let Some(atom_index) = hit else {
        return Ok(None);
    };
    let atom = graph.atoms.get(atom_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("native seam ray hit an unknown graph atom".into())
    })?;
    let mut selected = None;
    for contributor in &atom.contributors {
        budget.charge_into_error(1)?;
        let Some(region) = contributor.region else {
            continue;
        };
        let edge = area_edges.get(contributor.written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native seam ray contributor is unknown".into())
        })?;
        let [start, end] = &edge.vertices;
        let direction = vector_difference_with_work(&start.point, &end.point, budget)?;
        let offset = vector_difference_with_work(&start.point, query, budget)?;
        let orientation = vector_cross_with_work(&direction, &offset, budget)?.sign();
        let bank_side = if orientation == ExactSign::Zero {
            if !point_on_exact_segment_with_work(query, &start.point, &end.point, budget)? {
                return Err(GeometryErrorKind::Invalid(
                    "native seam ray hit lies on an edge extension".into(),
                )
                .into());
            }
            let longitude = ExactRational::from_f64(match bank {
                super::super::super::emission::ChartSide::East => frame.longitude().half_period(),
                super::super::super::emission::ChartSide::West => -frame.longitude().half_period(),
            });
            if !start.point.x.try_cmp_with_work(&longitude, budget)?.is_eq()
                || !end.point.x.try_cmp_with_work(&longitude, budget)?.is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native seam band midpoint lies on an unrecorded producer boundary".into(),
                )
                .into());
            }
            let vertical = start.point.y.try_cmp_with_work(&end.point.y, budget)?;
            match bank {
                super::super::super::emission::ChartSide::East => vertical.is_lt(),
                super::super::super::emission::ChartSide::West => vertical.is_gt(),
            }
        } else {
            orientation == ExactSign::Positive
        };
        if bank_side {
            if selected.is_some_and(|previous| previous != region) {
                return Err(GeometryErrorKind::Invalid(
                    "native seam bank has conflicting selected producer regions".into(),
                )
                .into());
            }
            selected = Some(region);
        }
    }
    Ok(selected)
}
pub(in crate::geometry::topology::q_embedding) fn native_seam_wall_crosses_latitude(
    area_edges: &[NativeAreaEdge],
    latitude: &ExactRational,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<bool> {
    let east = ExactRational::from_f64(frame.longitude().half_period());
    let west = ExactRational::zero().try_subtract_with_work(&east, budget)?;
    for edge in area_edges {
        budget.charge_into_error(1)?;
        if !matches!(&edge.kind, NativeAreaEdgeKind::Producer { .. }) {
            continue;
        }
        let [start, end] = &edge.vertices;
        if !start
            .point
            .x
            .try_cmp_with_work(&end.point.x, budget)?
            .is_eq()
        {
            continue;
        }
        if !start.point.x.try_cmp_with_work(&east, budget)?.is_eq()
            && !start.point.x.try_cmp_with_work(&west, budget)?.is_eq()
        {
            continue;
        }
        let (low, high) = if start
            .point
            .y
            .try_cmp_with_work(&end.point.y, budget)?
            .is_le()
        {
            (&start.point.y, &end.point.y)
        } else {
            (&end.point.y, &start.point.y)
        };
        if latitude.try_cmp_with_work(low, budget)?.is_ge()
            && latitude.try_cmp_with_work(high, budget)?.is_le()
        {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(in crate::geometry::topology::q_embedding) fn native_seam_bands_from_assigned_graph(
    graph: &NativeReferenceGraphBuilder,
    area_edges: &[NativeAreaEdge],
    frame: GeographicFrame,
    events: &mut Vec<NativeSeamEvent>,
    budget: &mut impl Work,
) -> Result<Vec<SelectedNativeSeamBand>> {
    budget.charge_into_error(2)?;
    events.extend([
        NativeSeamEvent {
            latitude: ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South)),
            output_vertex: true,
        },
        NativeSeamEvent {
            latitude: ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
            output_vertex: true,
        },
    ]);
    try_sort_by_with_work(events, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        left.latitude.try_cmp_with_work(&right.latitude, budget)
    })?;
    budget
        .charge_into_error(u64::try_from(events.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut unique: Vec<NativeSeamEvent> = Vec::with_capacity(events.len());
    for event in events.drain(..) {
        budget.charge_into_error(1)?;
        if let Some(previous) = unique.last_mut()
            && previous
                .latitude
                .try_cmp_with_work(&event.latitude, budget)?
                .is_eq()
        {
            previous.output_vertex |= event.output_vertex;
        } else {
            unique.push(event);
        }
    }
    *events = unique;

    let band_count = events.len().saturating_sub(1);
    let double_band_count = band_count
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(double_band_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut bounds = Vec::with_capacity(band_count);
    let mut east_queries = Vec::with_capacity(band_count);
    let mut west_queries = Vec::with_capacity(band_count);
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    for pair in events.array_windows::<2>() {
        budget.charge_into_error(1)?;
        let [low, high] = pair;
        if !low
            .latitude
            .try_cmp_with_work(&high.latitude, budget)?
            .is_lt()
        {
            continue;
        }
        let latitude = low
            .latitude
            .try_midpoint_with_work(&high.latitude, budget)?;
        bounds.push((
            low.latitude.try_clone_with_work(budget)?,
            high.latitude.try_clone_with_work(budget)?,
            low.output_vertex,
        ));
        east_queries.push(ExactChartPoint::from_rationals(
            half.try_clone_with_work(budget)?,
            latitude.try_clone_with_work(budget)?,
        ));
        west_queries.push(ExactChartPoint::from_rationals(
            negative_half.try_clone_with_work(budget)?,
            latitude,
        ));
    }
    let base_len = graph.points.exact.len();
    let query_end = base_len
        .checked_add(bounds.len())
        .ok_or(ResourceError::SizeOverflow)?;
    u32::try_from(query_end).map_err(|_| ResourceError::SizeOverflow)?;
    let query_capacity = u64::try_from(bounds.len()).map_err(|_| ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        query_capacity
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    let mut east_rounded = Vec::with_capacity(bounds.len());
    let mut west_rounded = Vec::with_capacity(bounds.len());
    let mut east_ids = Vec::with_capacity(bounds.len());
    let mut west_ids = Vec::with_capacity(bounds.len());
    for (index, (east, west)) in east_queries.iter().zip(&west_queries).enumerate() {
        budget.charge_into_error(1)?;
        east_rounded.push(XY::new(
            east.x.round_with_work(budget)?,
            east.y.round_with_work(budget)?,
        ));
        west_rounded.push(XY::new(
            west.x.round_with_work(budget)?,
            west.y.round_with_work(budget)?,
        ));
        let query_id = base_len
            .checked_add(index)
            .ok_or(ResourceError::SizeOverflow)?;
        let query_id = u32::try_from(query_id).map_err(|_| ResourceError::SizeOverflow)?;
        east_ids.push(query_id);
        west_ids.push(query_id);
    }
    let east_points = NativeReferenceRayPoints {
        base: &graph.points,
        exact_queries: &east_queries,
        rounded_queries: &east_rounded,
        transform: NativeRayTransform::Identity,
    };
    let west_points = NativeReferenceRayPoints {
        base: &graph.points,
        exact_queries: &west_queries,
        rounded_queries: &west_rounded,
        transform: NativeRayTransform::ReflectX,
    };
    let edge = |index| {
        graph.edge_nodes.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("native seam query edge is unknown".into()).into()
        })
    };
    let mut east_hits = shoot_left_with_work(
        &east_points,
        graph.edge_nodes.len(),
        edge,
        &east_ids,
        budget,
    )?;
    let mut west_hits = shoot_left_with_work(
        &west_points,
        graph.edge_nodes.len(),
        edge,
        &west_ids,
        budget,
    )?;
    if east_hits.len() != bounds.len() || west_hits.len() != bounds.len() {
        return Err(GeometryErrorKind::Invalid(
            "native seam ray results do not match their band rows".into(),
        )
        .into());
    }
    // A horizontal no-hit can be tangent to a seam wall; retry on the orthogonal axis.
    let missing_capacity = bounds
        .len()
        .checked_mul(4)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(missing_capacity).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut missing_east = Vec::with_capacity(bounds.len());
    let mut missing_west = Vec::with_capacity(bounds.len());
    for index in 0..bounds.len() {
        budget.charge_into_error(1)?;
        if east_hits.get(index).is_some_and(Option::is_none) {
            missing_east.push((
                index,
                *east_ids.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native east seam query ID is unknown".into())
                })?,
            ));
        }
        if west_hits.get(index).is_some_and(Option::is_none) {
            missing_west.push((
                index,
                *west_ids.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native west seam query ID is unknown".into())
                })?,
            ));
        }
    }
    let vertical_east_points = NativeReferenceRayPoints {
        base: &graph.points,
        exact_queries: &east_queries,
        rounded_queries: &east_rounded,
        transform: NativeRayTransform::SwapAxes,
    };
    let vertical_west_points = NativeReferenceRayPoints {
        base: &graph.points,
        exact_queries: &west_queries,
        rounded_queries: &west_rounded,
        transform: NativeRayTransform::SwapAxes,
    };
    for (vertical_points, missing, hits) in [
        (&vertical_east_points, missing_east, &mut east_hits),
        (&vertical_west_points, missing_west, &mut west_hits),
    ] {
        if missing.is_empty() {
            continue;
        }
        budget.charge_into_error(
            u64::try_from(missing.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut query_ids = Vec::with_capacity(missing.len());
        for &(_, query_id) in &missing {
            budget.charge_into_error(1)?;
            query_ids.push(query_id);
        }
        let vertical_hits = shoot_left_with_work(
            vertical_points,
            graph.edge_nodes.len(),
            edge,
            &query_ids,
            budget,
        )?;
        if vertical_hits.len() != missing.len() {
            return Err(GeometryErrorKind::Invalid(
                "native vertical seam ray results do not match their query rows".into(),
            )
            .into());
        }
        for ((index, _), hit) in missing.into_iter().zip(vertical_hits) {
            budget.charge_into_error(1)?;
            let slot = hits.get_mut(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native seam ray result row is unknown".into())
            })?;
            *slot = hit;
        }
    }
    budget.charge_into_error(query_capacity)?;
    let mut output: Vec<SelectedNativeSeamBand> = Vec::with_capacity(bounds.len());
    for (index, (low, high, output_vertex)) in bounds.into_iter().enumerate() {
        budget.charge_into_error(1)?;
        let west_hit = *west_hits.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native west seam ray result is unknown".into())
        })?;
        let east_hit = *east_hits.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native east seam ray result is unknown".into())
        })?;
        let west_selected = native_region_at_bank(
            graph,
            area_edges,
            west_queries.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native west seam query is unknown".into())
            })?,
            super::super::super::emission::ChartSide::West,
            west_hit,
            frame,
            budget,
        )?;
        let east_selected = native_region_at_bank(
            graph,
            area_edges,
            east_queries.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native east seam query is unknown".into())
            })?,
            super::super::super::emission::ChartSide::East,
            east_hit,
            frame,
            budget,
        )?;
        let seam_query = west_queries.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native west seam query is unknown".into())
        })?;
        // Without a producer wall at this latitude, the sewn banks share one sector.
        let (west_selected, east_selected) =
            if native_seam_wall_crosses_latitude(area_edges, &seam_query.y, frame, budget)? {
                (west_selected, east_selected)
            } else {
                match (west_selected, east_selected) {
                    (Some(west), Some(east)) if west != east => {
                        return Err(GeometryErrorKind::Invalid(
                            "periodic seam banks disagree without a seam wall".into(),
                        )
                        .into());
                    },
                    (Some(region), None) | (None, Some(region)) => (Some(region), Some(region)),
                    (west, east) => (west, east),
                }
            };
        let band = SelectedNativeSeamBand {
            low,
            high,
            west_region: west_selected,
            east_region: east_selected,
        };
        let can_merge = output.last().is_some_and(|previous| {
            !output_vertex
                && previous.west_region == band.west_region
                && previous.east_region == band.east_region
        });
        if can_merge {
            let previous = output.last_mut().ok_or_else(|| {
                GeometryErrorKind::Invalid("native seam merge lost its previous band".into())
            })?;
            previous.high = band.high;
        } else {
            output.push(band);
        }
    }
    Ok(output)
}
pub(in crate::geometry::topology::q_embedding) fn native_graph_slot(
    graph: &NativeReferenceGraph,
    atom: u32,
    forward: bool,
    work: &mut impl Work,
) -> Result<usize> {
    work.charge_into_error(1)?;
    let slots = graph.edge_slots.get(atom as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native chart atom has no indexed slot pair".into())
    })?;
    let direction = usize::from(!forward);
    usize::try_from(*slots.get(direction).ok_or_else(|| {
        GeometryErrorKind::Invalid("native chart atom direction is unknown".into())
    })?)
    .map_err(|_| ResourceError::SizeOverflow.into())
}
pub(in crate::geometry::topology::q_embedding) fn assign_native_face_region(
    face_regions: &mut [Option<usize>],
    known: &mut [bool],
    face: u32,
    region: Option<usize>,
    budget: &mut impl Work,
) -> Result<()> {
    budget.charge_into_error(1)?;
    let face_index = face as usize;
    let is_known = known.get_mut(face_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("native face-region known bit is unknown".into())
    })?;
    let value = face_regions
        .get_mut(face_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("native face-region value is unknown".into()))?;
    if *is_known && *value != region {
        return Err(GeometryErrorKind::Invalid(
            "native graph face has conflicting source-region germs".into(),
        )
        .into());
    }
    *is_known = true;
    *value = region;
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_boundary_walks(
    graph: &NativeReferenceGraph,
    selected: &[bool],
    walks: &[Vec<u32>],
    budget: &mut impl Work,
) -> Result<()> {
    let topology = &graph.topology;
    let dart_count = topology.targets.len();
    if selected.len() != dart_count
        || topology.owners.len() != dart_count
        || topology.twins.len() != dart_count
    {
        return Err(GeometryErrorKind::Invalid(
            "native selected boundary and graph darts are misaligned".into(),
        )
        .into());
    }
    budget
        .charge_into_error(u64::try_from(dart_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut visited = vec![false; dart_count];
    for walk in walks {
        budget.charge_into_error(1)?;
        if walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "native selected boundary contains an empty walk".into(),
            )
            .into());
        }
        for index in 0..walk.len() {
            budget.charge_into_error(1)?;
            let slot = *walk.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native selected boundary slot is unknown".into())
            })? as usize;
            if selected.get(slot).copied() != Some(true) {
                return Err(GeometryErrorKind::Invalid(
                    "native boundary walk contains an unselected dart".into(),
                )
                .into());
            }
            let was_visited = visited.get_mut(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("native boundary walk dart is out of range".into())
            })?;
            if std::mem::replace(was_visited, true) {
                return Err(GeometryErrorKind::Invalid(
                    "native selected boundary dart appears more than once".into(),
                )
                .into());
            }
            let next_index = index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
                .checked_rem(walk.len())
                .ok_or(ResourceError::SizeOverflow)?;
            let next_slot = *walk.get(next_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native boundary successor is unknown".into())
            })? as usize;
            if topology.targets.get(slot) != topology.owners.get(next_slot) {
                return Err(GeometryErrorKind::Invalid(
                    "native selected boundary walk is not exactly closed".into(),
                )
                .into());
            }
        }
    }
    for (slot, &is_selected) in selected.iter().enumerate() {
        budget.charge_into_error(1)?;
        if visited.get(slot).copied() != Some(is_selected) {
            return Err(GeometryErrorKind::Invalid(
                "native selected boundary darts are not covered exactly once".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(in crate::geometry::topology::q_embedding) fn assign_selected_pole_seam_band(
    bands: &mut [SelectedNativeSeamBand],
    frame: GeographicFrame,
    region: usize,
    pole: ChartPole,
    budget: &mut impl Work,
) -> Result<()> {
    let band_index = match pole {
        ChartPole::South => 0,
        ChartPole::North => bands.len().checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("selected pole has no native seam band".into())
        })?,
    };
    let band = bands.get_mut(band_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("selected pole has no adjacent native seam band".into())
    })?;
    let latitude = ExactRational::from_f64(frame.chart_pole_latitude(pole));
    let adjacent_pole = match pole {
        ChartPole::South => &band.low,
        ChartPole::North => &band.high,
    };
    if !adjacent_pole.try_cmp_with_work(&latitude, budget)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "native seam band does not meet its selected chart pole".into(),
        )
        .into());
    }
    for selected in [&mut band.west_region, &mut band.east_region] {
        if let Some(existing) = *selected
            && existing != region
        {
            return Err(GeometryErrorKind::Invalid(
                "selected pole sector conflicts with its adjacent seam band".into(),
            )
            .into());
        }
        *selected = Some(region);
    }
    Ok(())
}
