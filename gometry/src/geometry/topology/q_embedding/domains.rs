use super::{
    EmbeddingDomain, EmissionNodeId, EmissionNodeRegistry, ExactChartPoint, ExactDyadic,
    ExactGeographicTopology, ExactQuotientKey, ExactRational, GeographicFrame, GeometryErrorKind,
    Latitude, NodeAuthority, PublicationInventory, QCandidate, QCoordinates, QEmbeddingEmission,
    ResourceError, Result, XY, assigned_port_point, candidate_chart_point_with_work,
    exact_canonical_longitude_with_work, quotient_key_for_chart_position_with_work,
};
use crate::geographic::retained_exact::sites::{
    RoundedNativeSite, rounded_native_neighbors_with_work,
};
#[cfg(test)]
use crate::work::Unmetered;
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn exact_coordinate_has_binary64_value_with_work(
    value: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    let rounded = value.round_with_work(work)?;
    value.try_eq_with_work(&ExactRational::from_f64(rounded), work)
}
pub(in crate::geometry::topology::q_embedding) fn exact_node_position_has_binary64_value_with_work(
    position: &ExactNodePosition,
    work: &mut impl Work,
) -> Result<bool> {
    Ok(
        exact_coordinate_has_binary64_value_with_work(&position.longitude, work)?
            && exact_coordinate_has_binary64_value_with_work(&position.latitude, work)?,
    )
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct ExactNodePosition {
    pub(in crate::geometry::topology::q_embedding) longitude: ExactRational,
    pub(in crate::geometry::topology::q_embedding) latitude: ExactRational,
}
pub(in crate::geometry::topology::q_embedding) fn exact_node_position_from_key_with_work(
    frame: GeographicFrame,
    key: &ExactQuotientKey,
    work: &mut impl Work,
) -> Result<Option<ExactNodePosition>> {
    Ok(match key {
        ExactQuotientKey::Coordinate(point) => Some(ExactNodePosition {
            longitude: exact_canonical_longitude_with_work(frame, &point.x, work)?,
            latitude: point.y.try_clone_with_work(work)?,
        }),
        ExactQuotientKey::Seam(latitude) => Some(ExactNodePosition {
            longitude: ExactRational::from_f64(-frame.longitude().half_period()),
            latitude: latitude.try_clone_with_work(work)?,
        }),
        ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => None,
    })
}
#[cfg(test)]
pub(in crate::geometry::topology::q_embedding) fn inexact_axes(
    exact: &ExactNodePosition,
    nearest: XY,
) -> Result<[bool; 2]> {
    let mut work = Unmetered;
    inexact_axes_with_work(exact, nearest, &mut work)
}
pub(in crate::geometry::topology::q_embedding) fn inexact_axes_with_work(
    exact: &ExactNodePosition,
    nearest: XY,
    work: &mut impl Work,
) -> Result<[bool; 2]> {
    Ok([
        !exact
            .longitude
            .try_cmp_with_work(&ExactRational::from_f64(nearest.x), work)?
            .is_eq(),
        !exact
            .latitude
            .try_cmp_with_work(&ExactRational::from_f64(nearest.y), work)?
            .is_eq(),
    ])
}
pub(in crate::geometry::topology::q_embedding) fn inexact_published_axes_with_work(
    frame: GeographicFrame,
    exact: &ExactNodePosition,
    nearest: QCandidate,
    work: &mut impl Work,
) -> Result<[bool; 2]> {
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(nearest.periods));
    let nearest_longitude = ExactRational::from_f64(nearest.native.x).try_add_with_work(
        &ExactRational::from_f64(frame.longitude().period()).try_product_with_work(&lift, work)?,
        work,
    )?;
    Ok([
        !exact
            .longitude
            .try_cmp_with_work(&nearest_longitude, work)?
            .is_eq(),
        !exact
            .latitude
            .try_cmp_with_work(&ExactRational::from_f64(nearest.native.y), work)?
            .is_eq(),
    ])
}
pub(in crate::geometry::topology::q_embedding) struct PublishedDomains {
    pub(in crate::geometry::topology::q_embedding) exact_positions: Vec<Option<ExactNodePosition>>,
    pub(in crate::geometry::topology::q_embedding) nearest: Vec<Option<QCandidate>>,
    pub(in crate::geometry::topology::q_embedding) domains: Vec<Vec<QCandidate>>,
    pub(in crate::geometry::topology::q_embedding) variables: Vec<EmissionNodeId>,
}
pub(in crate::geometry::topology::q_embedding) fn published_domains(
    topology: &ExactGeographicTopology,
    publication: &PublicationInventory,
    nodes: &EmissionNodeRegistry,
    sample_nodes: &[EmissionNodeId],
    budget: &mut impl Work,
) -> Result<PublishedDomains> {
    if sample_nodes.len() != publication.samples.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q publication samples do not match their node IDs".into(),
        )
        .into());
    }
    let node_count = nodes.nodes().len();
    budget.charge_into_error(
        u64::try_from(
            node_count
                .checked_mul(4)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut representative_sample_for_node = vec![None; node_count];
    for (sample_index, &node_id) in sample_nodes.iter().enumerate() {
        budget.charge_into_error(1)?;
        let node = nodes.nodes().get(node_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication node ID is outside its registry".into())
        })?;
        if matches!(&node.authority, NodeAuthority::Computed(_)) {
            let slot = representative_sample_for_node
                .get_mut(node_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node sample slot is unknown".into())
                })?;
            if slot.is_none() {
                *slot = Some(sample_index);
            }
        }
    }

    let frame = topology.sources.frame;
    let mut exact_positions = vec![None; node_count];
    let mut nearest = vec![None; node_count];
    let mut domains = vec![Vec::new(); node_count];
    let mut variables = Vec::new();
    for (node_index, node) in nodes.nodes().iter().enumerate() {
        budget.charge_into_error(1)?;
        let NodeAuthority::Computed(key) = &node.authority else {
            continue;
        };
        let sample_index = *representative_sample_for_node
            .get(node_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication node has no sample index".into())
            })?;
        let sample = publication
            .samples
            .get(sample_index.ok_or_else(|| {
                GeometryErrorKind::Invalid("Q computed publication node has no sample".into())
            })?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q computed publication sample is unknown".into())
            })?;
        let exact = ExactNodePosition {
            longitude: sample.exact_point.x.clone(),
            latitude: sample.exact_point.y.clone(),
        };
        *exact_positions.get_mut(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication exact-position slot is unknown".into())
        })? = Some(exact.clone());
        let [_, longitude_site, _] =
            rounded_native_neighbors_with_work(frame.longitude(), &exact.longitude, budget)?;
        let nearest_candidate = QCandidate {
            native: XY::new(
                longitude_site.native,
                exact.latitude.round_with_work(budget)?,
            ),
            periods: longitude_site.periods,
        };
        let nearest_point = candidate_chart_point_with_work(frame, nearest_candidate, budget)?;
        if !nearest_point.try_eq_with_work(&sample.published_chart_point, budget)? {
            return Err(GeometryErrorKind::Invalid(
                "published nearest image differs from its exact source reference".into(),
            )
            .into());
        }
        *nearest.get_mut(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication nearest-site slot is unknown".into())
        })? = Some(nearest_candidate);

        if matches!(
            key,
            ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
        ) {
            *domains.get_mut(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication domain slot is unknown".into())
            })? = vec![nearest_candidate];
            continue;
        }
        let movable_axes =
            inexact_published_axes_with_work(frame, &exact, nearest_candidate, budget)?;
        let fixed_longitude = matches!(key, ExactQuotientKey::Seam(_));
        let longitude_candidates = if movable_axes[0] && !fixed_longitude {
            rounded_native_neighbors_with_work(frame.longitude(), &exact.longitude, budget)?
        } else {
            [RoundedNativeSite {
                native: nearest_candidate.native.x,
                periods: nearest_candidate.periods,
            }; 3]
        };
        let latitude_candidates = if movable_axes[1] {
            [
                nearest_candidate.native.y.next_down(),
                nearest_candidate.native.y,
                nearest_candidate.native.y.next_up(),
            ]
        } else {
            [nearest_candidate.native.y; 3]
        };
        let longitude_count: usize = if movable_axes[0] && !fixed_longitude {
            3
        } else {
            1
        };
        let latitude_count: usize = if movable_axes[1] { 3 } else { 1 };
        let domain_capacity = longitude_count
            .checked_mul(latitude_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut domain = Vec::with_capacity(domain_capacity);
        for longitude in longitude_candidates.iter().take(longitude_count) {
            for &latitude in latitude_candidates.iter().take(latitude_count) {
                budget.charge_into_error(1)?;
                if crate::geographic::Latitude::try_new(frame, latitude).is_err() {
                    continue;
                }
                let native = XY::new(longitude.native, latitude);
                if !EmbeddingDomain::Geographic(frame).admits(nearest_candidate.native, native) {
                    continue;
                }
                domain.push(QCandidate {
                    native,
                    periods: longitude.periods,
                });
            }
        }
        let mut contains_nearest = false;
        let mut has_choice = false;
        for &candidate in &domain {
            budget.charge_into_error(1)?;
            contains_nearest |= candidate == nearest_candidate;
            has_choice |= candidate != nearest_candidate;
        }
        if !contains_nearest {
            return Err(GeometryErrorKind::Invalid(
                "publication candidate domain excludes its written nearest sample".into(),
            )
            .into());
        }
        *domains.get_mut(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication domain slot is unknown".into())
        })? = domain;
        if has_choice {
            variables.push(EmissionNodeId(
                u32::try_from(node_index).map_err(|_| ResourceError::SizeOverflow)?,
            ));
        }
    }
    Ok(PublishedDomains {
        exact_positions,
        nearest,
        domains,
        variables,
    })
}
pub(in crate::geometry::topology::q_embedding) fn candidate_domains(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest: &QCoordinates,
    exact_positions: &[Option<ExactNodePosition>],
    movable_axes: &[[bool; 2]],
    budget: &mut impl Work,
) -> Result<Vec<Vec<QCandidate>>> {
    let frame = topology.sources.frame;
    let q = &topology.q;
    let cut_east = q
        .cut
        .try_add_with_work(&ExactRational::from_f64(frame.longitude().period()), budget)?;
    budget.charge_into_error(
        u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut domains = Vec::with_capacity(q.nodes.len());
    let mut selected_port_index = 0;
    for (node_index, node) in q.nodes.iter().enumerate() {
        budget.charge_into_error(1)?;
        let node_id = node.id;
        let nearest_xy = nearest.xy(node_id)?;
        let axes = movable_axes
            .get(node_index)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no movability state".into()))?;
        let nearest_periods = *nearest.longitude_periods.get(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node has no retained longitude lift".into())
        })?;
        let xs = if axes[0] {
            let exact = exact_positions
                .get(node_index)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("movable Q node has no exact position".into())
                })?;
            rounded_native_neighbors_with_work(frame.longitude(), &exact.longitude, budget)?
        } else {
            let site = RoundedNativeSite {
                native: nearest_xy.x,
                periods: nearest_periods,
            };
            [site; 3]
        };
        let ys = if axes[1] {
            [
                nearest_xy.y.next_down(),
                nearest_xy.y,
                nearest_xy.y.next_up(),
            ]
        } else {
            [nearest_xy.y; 3]
        };
        let x_count = if axes[0] { 3_usize } else { 1 };
        let y_count = if axes[1] { 3_usize } else { 1 };
        let selected_start = selected_port_index;
        while emission
            .selected_ports
            .get(selected_port_index)
            .is_some_and(|(selected_node, _)| *selected_node == node_id)
        {
            budget.charge_into_error(1)?;
            selected_port_index = selected_port_index.wrapping_add(1);
        }
        let selected_ports = emission
            .selected_ports
            .get(selected_start..selected_port_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port range is invalid".into()))?;
        let domain_capacity = x_count
            .checked_mul(y_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut domain = Vec::with_capacity(domain_capacity);
        for &x in xs
            .get(..x_count)
            .expect("x candidate count is one or three")
        {
            for &y in ys
                .get(..y_count)
                .expect("y candidate count is one or three")
            {
                budget.charge_into_error(1)?;
                let candidate = QCandidate {
                    native: XY::new(x.native, y),
                    periods: x.periods,
                };
                if !EmbeddingDomain::Geographic(frame).admits(nearest_xy, candidate.native) {
                    continue;
                }
                let mut admitted = true;
                for &(_, port_id) in selected_ports {
                    budget.charge_into_error(1)?;
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q candidate port is unknown".into())
                    })?;
                    if port.cut {
                        continue;
                    }
                    let pole_longitude = nearest
                        .pole_port_longitude
                        .get(port_id.0 as usize)
                        .copied()
                        .flatten();
                    if assigned_port_point(
                        frame,
                        q,
                        port_id,
                        nearest_xy,
                        candidate,
                        pole_longitude,
                        nearest
                            .pole_port_periods
                            .get(port_id.0 as usize)
                            .copied()
                            .flatten(),
                        &cut_east,
                        budget,
                    )?
                    .is_none()
                    {
                        admitted = false;
                        break;
                    }
                }
                if admitted {
                    domain.push(candidate);
                }
            }
        }
        domains.push(domain);
    }
    Ok(domains)
}
pub(in crate::geometry::topology::q_embedding) fn native_cut_candidate_domain(
    frame: GeographicFrame,
    exact: &ExactChartPoint,
    budget: &mut impl Work,
) -> Result<Vec<QCandidate>> {
    let [_, longitude, _] =
        rounded_native_neighbors_with_work(frame.longitude(), &exact.x, budget)?;
    let nearest_y = exact.y.round_with_work(budget)?;
    let fixed_y = matches!(
        quotient_key_for_chart_position_with_work(frame, exact, budget)?,
        ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
    ) || ExactRational::from_f64(nearest_y)
        .try_cmp_with_work(&exact.y, budget)?
        .is_eq();
    let latitudes = if fixed_y {
        [nearest_y; 3]
    } else {
        [nearest_y.next_down(), nearest_y, nearest_y.next_up()]
    };
    let nearest = QCandidate {
        native: XY::new(longitude.native, nearest_y),
        periods: longitude.periods,
    };
    let mut domain = Vec::with_capacity(if fixed_y { 1 } else { 3 });
    let latitude_count = if fixed_y { 1 } else { 3 };
    for &latitude in latitudes.iter().take(latitude_count) {
        budget.charge_into_error(1)?;
        if Latitude::try_new(frame, latitude).is_err() {
            continue;
        }
        let candidate = QCandidate {
            native: XY::new(longitude.native, latitude),
            periods: longitude.periods,
        };
        if EmbeddingDomain::Geographic(frame).admits(nearest.native, candidate.native) {
            domain.push(candidate);
        }
    }
    let mut contains_nearest = false;
    for &candidate in &domain {
        budget.charge_into_error(1)?;
        contains_nearest |= candidate == nearest;
    }
    if !contains_nearest {
        return Err(GeometryErrorKind::Invalid(
            "native cut candidate domain excludes its nearest site".into(),
        )
        .into());
    }
    Ok(domain)
}
