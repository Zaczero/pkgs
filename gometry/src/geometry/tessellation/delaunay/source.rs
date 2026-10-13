use super::*;

/// Select the first full source point at each XY key, then order those points
/// by XY for the exact mesh.
fn selected_source_sites(points: &[Point]) -> Result<Vec<Point>> {
    let mut selected = crate::collections::try_hash_map_with_capacity(points.len())?;
    for &point in points {
        selected.entry(PointKey::new(point)).or_insert(point);
    }
    let mut selected: Vec<_> = selected.into_values().collect();
    selected.sort_unstable_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    Ok(selected)
}

pub(super) struct ConstrainedSourcePlan {
    pub(super) vertices: Vec<Point>,
    pub(super) sources: Vec<cdt::SourceConstraint>,
    pub(super) restrictions: Vec<cdt::RestrictionConstraint>,
    pub(super) source_rings: Vec<cdt::SourceRingStart>,
    pub(super) coordinates: cdt::CdtCoordinates,
    pub(super) chart_area: Option<exact::ExactDyadic>,
}

pub(super) fn constrained_source_plan(
    rings: &[(RingOwner, &Ring)],
    domain: crate::geometry::TopologyDomain,
    chart: Option<&PointSelectionChart>,
) -> Result<ConstrainedSourcePlan> {
    if let crate::geometry::TopologyDomain::Geographic(frame) = domain
        && let Some(plan) = retained_constrained_source_plan(frame, chart, rings)?
    {
        return Ok(plan);
    }
    raw_constrained_source_plan(rings)
}

fn retained_constrained_source_plan(
    frame: GeographicFrame,
    chart: Option<&PointSelectionChart>,
    rings: &[(RingOwner, &Ring)],
) -> Result<Option<ConstrainedSourcePlan>> {
    let capacity = rings.iter().try_fold(0_usize, |total, ring| {
        total
            .checked_add(ring.1.coords().coord_count())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut source_points = Vec::with_capacity(capacity);
    let mut retained_rings = Vec::with_capacity(rings.len());
    let mut polar_closures = Vec::new();
    for &(owner, ring) in rings {
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let reaches_pole = summary.south.identity(frame) == SeamIdentity::Pole
            || summary.north.identity(frame) == SeamIdentity::Pole;
        let planar_full_longitude_band = summary.chart_turn == 0 && summary.exact_axis_band;
        let polar_pole = if summary.chart_turn == 1 || summary.chart_turn == -1 {
            summary.bounded_pole()
        } else {
            None
        };
        let bounded_full_turn = polar_pole.is_some() && !reaches_pole && owner.ring() == 0;
        if (summary.domain != RingDomain::Ordinary
            && !planar_full_longitude_band
            && !bounded_full_turn)
            || reaches_pole
        {
            return Ok(None);
        }
        let coords = ring.coords();
        let first = coords.nth_coord(0);
        let initial_lift = chart
            .map(|chart| chart.longitude_site(first.x))
            .transpose()?
            .map_or(0, |longitude| longitude.periods());
        let mut lift = initial_lift;
        let mut sites = Vec::with_capacity(coords.coord_count());
        for index in 0..coords.coord_count() {
            let point = coords.nth_coord(index);
            let source_ordinal = source_points.len();
            source_points.push(point);
            sites.push(ChartPointSite::from_lifted_source(
                frame,
                point,
                source_ordinal,
                lift,
            )?);
            let next_index = index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            if next_index < coords.coord_count() {
                let next = coords.nth_coord(next_index);
                let transit = frame
                    .longitude()
                    .chart_connection(point.x, next.x)
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                    .seam_transit();
                lift = lift
                    .checked_add(i64::from(transit))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
        }
        if lift.checked_sub(initial_lift) != Some(summary.chart_turn) {
            return Err(GeometryErrorKind::Invalid(
                "retained polygon chart disagrees with its admitted ring turn".into(),
            )
            .into());
        }
        if let Some(pole) = polar_pole {
            let (Some(&first), Some(&last)) = (sites.first(), sites.last()) else {
                return Ok(None);
            };
            let (west, east) = if compare_retained_x(first, last).is_lt() {
                (first, last)
            } else {
                (last, first)
            };
            if sites
                .iter()
                .skip(1)
                .take(sites.len().saturating_sub(2))
                .any(|site| {
                    !compare_retained_x(west, *site).is_lt()
                        || !compare_retained_x(*site, east).is_lt()
                })
            {
                return Ok(None);
            }
            polar_closures.push(RetainedPolarClosure {
                owner,
                pole,
                first,
                last,
            });
        }
        retained_rings.push((owner, sites));
    }
    // Each polygon hole is translated by an integral period into its shell's
    // continuous lift. Exact containment chooses the chart; rounded X is never
    // used to admit or deduplicate a site.
    for hole_index in 0..retained_rings.len() {
        let Some((owner, hole)) = retained_rings.get(hole_index) else {
            continue;
        };
        let owner = *owner;
        if owner.ring() == 0 {
            continue;
        }
        let Some(shell_index) = retained_rings.iter().position(|(candidate, _)| {
            candidate.polygon() == owner.polygon() && candidate.ring() == 0
        }) else {
            continue;
        };
        let Some((_, shell)) = retained_rings.get(shell_index) else {
            return Err(GeometryErrorKind::triangulation(
                "polygon shell chart is absent from its retained ring table",
            ));
        };
        let polar_closure = polar_closures
            .iter()
            .find(|closure| closure.owner.polygon() == owner.polygon());
        let polar_cycle = polar_closure
            .map(|closure| -> Result<Vec<(RetainedLongitudeSite, f64)>> {
                let capacity = shell
                    .len()
                    .checked_add(3)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                let mut cycle = Vec::with_capacity(capacity);
                cycle.extend(shell.iter().map(|site| (site.longitude(), site.source_y())));
                let pole_y = frame.chart_pole_latitude(closure.pole);
                cycle.extend([
                    (closure.last.longitude(), pole_y),
                    (closure.first.longitude(), pole_y),
                    (closure.first.longitude(), closure.first.source_y()),
                ]);
                Ok(cycle)
            })
            .transpose()?;
        let first = *hole.first().ok_or_else(|| {
            GeometryErrorKind::triangulation("polygon hole has no retained source sites")
        })?;
        let mut containing = None;
        for shift in -1_i64..=1 {
            let candidate = first.shifted(shift)?;
            let location = if let Some(cycle) = &polar_cycle {
                retained_point_in_coordinate_cycle(frame, cycle, candidate)?
            } else {
                retained_point_in_cycle(frame, shell, candidate)?
            };
            if location == cdt::CdtPointLocation::Inside && containing.replace(shift).is_some() {
                containing = None;
                break;
            }
        }
        if let Some(shift) = containing {
            if let Some(closure) = polar_closure {
                let (west, east) = if compare_retained_x(closure.first, closure.last).is_lt() {
                    (closure.first, closure.last)
                } else {
                    (closure.last, closure.first)
                };
                for &site in hole {
                    let site = site.shifted(shift)?;
                    if !compare_retained_x(west, site).is_lt()
                        || !compare_retained_x(site, east).is_lt()
                    {
                        return Ok(None);
                    }
                }
            }
            let Some((_, hole_sites)) = retained_rings.get_mut(hole_index) else {
                return Err(GeometryErrorKind::triangulation(
                    "polygon hole chart disappeared during admission",
                ));
            };
            for site in hole_sites {
                *site = site.shifted(shift)?;
            }
        } else if polar_closure.is_some() {
            return Ok(None);
        }
    }

    let mut first_site_by_key = crate::collections::try_hash_map_with_capacity(capacity)?;
    for (_, sites) in &retained_rings {
        for &site in sites {
            first_site_by_key
                .entry(retained_site_key(site))
                .or_insert_with(|| (site.source_ordinal(), site));
        }
    }
    let mut selected_sites = first_site_by_key
        .into_iter()
        .map(|(key, (source_ordinal, site))| (key, source_ordinal, site))
        .collect::<Vec<_>>();
    selected_sites.sort_unstable_by(|left, right| {
        left.2
            .compare_chart_xy(right.2)
            .then_with(|| left.1.cmp(&right.1))
    });
    let generated_count = polar_closures
        .len()
        .checked_mul(2)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertex_capacity = selected_sites
        .len()
        .checked_add(generated_count)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut vertices = Vec::with_capacity(vertex_capacity);
    let mut site_locations = Vec::with_capacity(selected_sites.len());
    let mut site_indices = crate::collections::try_hash_map_with_capacity(selected_sites.len())?;
    for (site, (key, source_ordinal, location)) in selected_sites.into_iter().enumerate() {
        vertices.push(*source_points.get(source_ordinal).ok_or_else(|| {
            GeometryErrorKind::triangulation(
                "retained source site references an absent source record",
            )
        })?);
        site_locations.push(location);
        site_indices.insert(key, site);
    }

    let mut occurrence_locations = Vec::with_capacity(source_points.len());
    for (_, sites) in &retained_rings {
        occurrence_locations.extend_from_slice(sites);
    }
    let mut occurrence_to_site = Vec::with_capacity(occurrence_locations.len());
    for &location in &occurrence_locations {
        occurrence_to_site.push(*site_indices.get(&retained_site_key(location)).ok_or_else(
            || {
                GeometryErrorKind::triangulation(
                    "retained source occurrence has no admitted mesh site",
                )
            },
        )?);
    }

    let mut generated_sites = Vec::new();
    let mut restrictions = Vec::new();
    for closure in polar_closures.iter().copied() {
        let Some(&first_site) = site_indices.get(&retained_site_key(closure.first)) else {
            return Err(GeometryErrorKind::triangulation(
                "polar chart start has no admitted mesh site",
            ));
        };
        let Some(&last_site) = site_indices.get(&retained_site_key(closure.last)) else {
            return Err(GeometryErrorKind::triangulation(
                "polar chart end has no admitted mesh site",
            ));
        };
        if first_site == last_site {
            return Ok(None);
        }
        let (first_side, last_side) = if compare_retained_x(closure.first, closure.last).is_lt() {
            (ChartCutSide::West, ChartCutSide::East)
        } else {
            (ChartCutSide::East, ChartCutSide::West)
        };
        let pole_latitude = frame.chart_pole_latitude(closure.pole);
        let pole_node =
            u32::try_from(vertices.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let cut_node = u32::try_from(first_site.min(last_site))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let first_pole_site = vertices.len();
        let last_pole_site = first_pole_site
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let chart_longitude = |site: ChartPointSite| {
            site.longitude().native().get()
                + frame.longitude().period() * site.longitude().periods() as f64
        };
        let first_pole = Point::new_raw_xy(chart_longitude(closure.first), pole_latitude);
        let last_pole = Point::new_raw_xy(chart_longitude(closure.last), pole_latitude);
        if !first_pole.x.is_finite() || !last_pole.x.is_finite() {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        vertices.extend([first_pole, last_pole]);
        generated_sites.extend([
            (closure.first.longitude(), pole_latitude),
            (closure.last.longitude(), pole_latitude),
        ]);

        let first_cut = SyntheticChartVertex::Cut {
            node: cut_node,
            side: first_side,
        };
        let last_cut = SyntheticChartVertex::Cut {
            node: cut_node,
            side: last_side,
        };
        let first_pole_cut = SyntheticChartVertex::Cut {
            node: pole_node,
            side: first_side,
        };
        let last_pole_cut = SyntheticChartVertex::Cut {
            node: pole_node,
            side: last_side,
        };
        let pole_vertex = |native_longitude: f64| SyntheticChartVertex::Pole {
            node: pole_node,
            pole: closure.pole,
            native_longitude_bits: native_longitude.to_bits(),
        };
        restrictions.extend([
            cdt::RestrictionConstraint {
                id: restrictions.len(),
                owner: closure.owner,
                start_site: last_site,
                end_site: last_pole_site,
                use_: DirectedChartRestrictionUse {
                    restriction: ChartRestriction::CutChord,
                    start: last_cut,
                    end: last_pole_cut,
                },
            },
            cdt::RestrictionConstraint {
                id: restrictions
                    .len()
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
                owner: closure.owner,
                start_site: last_pole_site,
                end_site: first_pole_site,
                use_: DirectedChartRestrictionUse {
                    restriction: ChartRestriction::PolarCap { pole: closure.pole },
                    start: pole_vertex(last_pole.x),
                    end: pole_vertex(first_pole.x),
                },
            },
            cdt::RestrictionConstraint {
                id: restrictions
                    .len()
                    .checked_add(2)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
                owner: closure.owner,
                start_site: first_pole_site,
                end_site: first_site,
                use_: DirectedChartRestrictionUse {
                    restriction: ChartRestriction::CutChord,
                    start: first_pole_cut,
                    end: first_cut,
                },
            },
        ]);
    }

    let mut sources: Vec<cdt::SourceConstraint> = Vec::with_capacity(capacity);
    let mut source_rings = Vec::with_capacity(rings.len());
    let mut seen: crate::collections::HashMap<(usize, usize), usize> =
        crate::collections::try_hash_map_with_capacity(capacity)?;
    let mut occurrence_start = 0_usize;
    let mut source_root_start = 0_usize;
    for &(owner, ring) in rings {
        let coords = ring.coords();
        source_rings.push(cdt::SourceRingStart::new(sources.len(), owner));
        for index in 0..coords.coord_count().saturating_sub(1) {
            let start_occurrence = occurrence_start
                .checked_add(index)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let source_root = source_root_start
                .checked_add(index)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let end_occurrence = start_occurrence
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let start_site = occurrence_to_site
                .get(start_occurrence)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::triangulation("retained source edge has no start occurrence")
                })?;
            let end_site = occurrence_to_site
                .get(end_occurrence)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::triangulation("retained source edge has no end occurrence")
                })?;
            if start_site == end_site {
                continue;
            }
            let edge_key = (start_site.min(end_site), start_site.max(end_site));
            if let Some(&previous_id) = seen.get(&edge_key) {
                let previous = sources.get(previous_id).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "retained duplicate edge has no earlier source owner",
                    )
                })?;
                let previous_owner = cdt::source_ring_owner(previous.id, &source_rings)
                    .expect("an admitted source constraint belongs to a source ring");
                return Err(overlapping_rings_error(previous_owner, owner));
            }
            seen.insert(edge_key, sources.len());
            let end_index = index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            sources.push(cdt::SourceConstraint {
                id: sources.len(),
                source_root,
                start: coords.nth_coord(index),
                end: coords.nth_coord(end_index),
                start_site,
                end_site,
                start_occurrence,
                end_occurrence,
            });
        }
        occurrence_start = occurrence_start
            .checked_add(coords.coord_count())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        source_root_start = source_root_start
            .checked_add(coords.coord_count().saturating_sub(1))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let coordinates = cdt::CdtCoordinates::retained_with_generated(
        frame,
        &site_locations,
        &occurrence_locations,
        &generated_sites,
    )?;
    let chart_area = retained_chart_area(frame, &retained_rings, &polar_closures)?;
    Ok(Some(ConstrainedSourcePlan {
        vertices,
        sources,
        restrictions,
        source_rings,
        coordinates,
        chart_area: Some(chart_area),
    }))
}

#[expect(
    clippy::indexing_slicing,
    reason = "source endpoint keys come from the ring points collected above, and their site IDs index the selected vertex column used to build the map"
)]
fn raw_constrained_source_plan(rings: &[(RingOwner, &Ring)]) -> Result<ConstrainedSourcePlan> {
    let capacity = rings.iter().try_fold(0_usize, |total, ring| {
        total
            .checked_add(ring.1.coords().coord_count())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut source_points = Vec::with_capacity(capacity);
    for &(_, ring) in rings {
        for index in 0..ring.coords().coord_count() {
            source_points.push(ring.coords().nth_coord(index));
        }
    }
    let vertices = selected_source_sites(&source_points)?;
    let mut indices = crate::collections::try_hash_map_with_capacity(vertices.len())?;
    for (site, point) in vertices.iter().enumerate() {
        indices.insert(PointKey::new(*point), site);
    }
    let mut sources = Vec::with_capacity(capacity);
    let mut source_rings = Vec::with_capacity(rings.len());
    let mut seen = crate::collections::try_hash_set_with_capacity(capacity)?;
    let mut occurrence_start = 0_usize;
    let mut source_root_start = 0_usize;
    for &(owner, ring) in rings {
        let coords = ring.coords();
        source_rings.push(cdt::SourceRingStart::new(sources.len(), owner));
        for index in 0..coords.coord_count().saturating_sub(1) {
            let start = coords.nth_coord(index);
            let end_index = index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let end = coords.nth_coord(end_index);
            if same_point(start.xy(), end.xy()) {
                continue;
            }
            let key = ordered_constraint_key(start.xy(), end.xy());
            if !seen.insert(key) {
                let previous = sources
                    .iter()
                    .find(|source: &&cdt::SourceConstraint| {
                        ordered_constraint_key(source.start.xy(), source.end.xy()) == key
                    })
                    .expect("a seen source edge has an admitted source constraint");
                let previous_owner = cdt::source_ring_owner(previous.id, &source_rings)
                    .expect("an admitted source constraint belongs to a source ring");
                return Err(overlapping_rings_error(previous_owner, owner));
            }
            let start_site = indices[&PointKey::new(start)];
            let end_site = indices[&PointKey::new(end)];
            let id = sources.len();
            let start_occurrence = occurrence_start
                .checked_add(index)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let source_root = source_root_start
                .checked_add(index)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let end_occurrence = start_occurrence
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            sources.push(cdt::SourceConstraint {
                id,
                source_root,
                start: vertices[start_site],
                end: vertices[end_site],
                start_site,
                end_site,
                start_occurrence,
                end_occurrence,
            });
        }
        occurrence_start = occurrence_start
            .checked_add(coords.coord_count())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        source_root_start = source_root_start
            .checked_add(coords.coord_count().saturating_sub(1))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let coordinates = cdt::CdtCoordinates::raw(&vertices, &source_points);
    Ok(ConstrainedSourcePlan {
        vertices,
        sources,
        restrictions: Vec::new(),
        source_rings,
        coordinates,
        chart_area: None,
    })
}

fn ordered_constraint_key(a: XY, b: XY) -> (PointKey, PointKey) {
    let (a, b) = (PointKey::new(a), PointKey::new(b));
    if a <= b { (a, b) } else { (b, a) }
}

/// Prepare constraint segments: exact endpoint-key deduplication
/// (bit-identical XY only) and drop zero-length / duplicate undirected edges.
/// Absolute proximity snapping was deleted — a scale-free 1e-4 radius
/// annihilated valid small polygons (5e-5 square → zero triangles).
#[cfg(test)]
pub(crate) fn cleanup_constraint_lines(lines: Vec<[XY; 2]>) -> Vec<[XY; 2]> {
    let mut prepared: Vec<[XY; 2]> = Vec::with_capacity(lines.len());
    let mut seen = std::collections::HashSet::with_capacity(lines.len());
    for [start, end] in lines {
        if !same_point(start, end) && seen.insert(ordered_constraint_key(start, end)) {
            prepared.push([start, end]);
        }
    }
    prepared
}
