use super::super::*;

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "geographic build-area consumes exact cycle roles after F4 routing lands"
    )
)]
pub(in crate::geometry::topology::q_embedding) fn assigned_emitted_ring(
    topology: &ExactGeographicTopology,
    vertices: &[EmittedVertex<'_>],
    work: &mut impl Work,
) -> Result<(Ring, Ring, bool)> {
    work.charge_into_error(
        u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut chart_points = vertices
        .iter()
        .map(|vertex| vertex.point)
        .collect::<Vec<_>>();
    let quotient_closure =
        canonicalize_ring_vertices(topology.sources.frame, vertices, &mut chart_points, work)?;
    let chart_ring = Ring::try_from_closed(CoordSeq::try_from_points(&chart_points)?)?;
    work.charge_into_error(
        u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut published_points = vertices
        .iter()
        .map(|vertex| vertex.point)
        .collect::<Vec<_>>();
    let first = *published_points
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted ring has no vertices".into()))?;
    *published_points
        .last_mut()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted ring has no vertices".into()))? =
        first;
    let published_ring = Ring::try_from_closed(CoordSeq::try_from_points(&published_points)?)?;
    Ok((chart_ring, published_ring, quotient_closure))
}
pub(in crate::geometry::topology::q_embedding) enum QRingRoleFailure {
    CycleConstruction,
    CycleUnclassifiable,
    CycleChanged,
    SourceUnclassifiable,
    SourceChanged,
}
pub(in crate::geometry::topology::q_embedding) enum QRingRoleCheck {
    Pass(Vec<bool>),
    NativeRequired(Vec<QRingRoleStatus>),
    Reject {
        nodes: Vec<QNodeId>,
        failure: QRingRoleFailure,
    },
}
pub(in crate::geometry::topology::q_embedding) fn planned_area_rings(
    plan: &QEmissionPlan,
) -> &[PlannedAreaRing] {
    match &plan.polygons {
        PolygonPlan::WholeSphere(_) => &[],
        PolygonPlan::Regions { rings, .. } => rings,
    }
}
pub(in crate::geometry::topology::q_embedding) fn check_q_ring_roles(
    topology: &ExactGeographicTopology,
    drawing: &QEmissionDrawing<'_>,
    work: &mut impl Work,
) -> Result<QRingRoleCheck> {
    let DrawnPolygons::Regions {
        rings: area_rings, ..
    } = &drawing.polygons
    else {
        return Ok(QRingRoleCheck::Pass(Vec::new()));
    };
    let mut roles = Vec::with_capacity(area_rings.len());
    let mut region_roles: Option<Vec<QRingRoleStatus>> = None;
    for ring in area_rings {
        work.charge_into_error(1)?;
        let nodes = || {
            ring.vertices
                .iter()
                .map(|vertex| vertex.port.node)
                .collect()
        };
        let exact_ports = ring.vertices.iter().map(|vertex| &vertex.port.point);
        let is_source = matches!(ring.origin, DrawnAreaOrigin::Source);
        let (emitted_chart, published, quotient_closure) =
            match assigned_emitted_ring(topology, &ring.vertices, work) {
                Ok(rings) => rings,
                Err(_) if !is_source => {
                    return Ok(QRingRoleCheck::Reject {
                        nodes: nodes(),
                        failure: QRingRoleFailure::CycleConstruction,
                    });
                },
                Err(_) => {
                    return Ok(QRingRoleCheck::Reject {
                        nodes: nodes(),
                        failure: QRingRoleFailure::SourceUnclassifiable,
                    });
                },
            };
        let Some(role) = compare_q_ring_role_with_work(
            topology.sources.frame,
            &emitted_chart,
            &published,
            exact_ports,
            quotient_closure || is_source,
            work,
        )?
        else {
            if !is_source
                && matches!(
                    quotient_ring_role_status(topology.sources.frame, &emitted_chart)?,
                    QuotientRingRoleStatus::Unbounded
                )
            {
                let region_roles = region_roles.get_or_insert_with(|| {
                    std::mem::take(&mut roles)
                        .into_iter()
                        .map(QRingRoleStatus::Classified)
                        .collect()
                });
                // Final shell/hole roles belong to the selected region's assembled chart.
                region_roles.push(QRingRoleStatus::Unclassified);
                continue;
            }
            return Ok(QRingRoleCheck::Reject {
                nodes: nodes(),
                failure: if is_source {
                    QRingRoleFailure::SourceUnclassifiable
                } else {
                    QRingRoleFailure::CycleUnclassifiable
                },
            });
        };
        if !role.matches() {
            return Ok(QRingRoleCheck::Reject {
                nodes: nodes(),
                failure: if is_source {
                    QRingRoleFailure::SourceChanged
                } else {
                    QRingRoleFailure::CycleChanged
                },
            });
        }
        if let Some(region_roles) = &mut region_roles {
            region_roles.push(QRingRoleStatus::Classified(role.emitted_is_hole));
        } else {
            roles.push(role.emitted_is_hole);
        }
    }
    Ok(region_roles.map_or(QRingRoleCheck::Pass(roles), QRingRoleCheck::NativeRequired))
}
pub(in crate::geometry::topology) fn certify_nearest_ring_roles(
    topology: &ExactGeographicTopology,
    drawing: &QEmissionDrawing<'_>,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    match check_q_ring_roles(topology, drawing, work)? {
        QRingRoleCheck::Pass(roles) => Ok(roles),
        QRingRoleCheck::NativeRequired(_) => Err(GeometryErrorKind::Invalid(
            "selected Q cycle needs Native region assembly".into(),
        )
        .into()),
        QRingRoleCheck::Reject { failure, .. } => {
            let message = match failure {
                QRingRoleFailure::CycleConstruction => {
                    "selected Q area boundary cannot be constructed"
                },
                QRingRoleFailure::CycleUnclassifiable => {
                    "selected Q area boundary has no emitted chart role"
                },
                QRingRoleFailure::CycleChanged => {
                    "selected Q area boundary changes its exact shell/hole role"
                },
                QRingRoleFailure::SourceUnclassifiable => {
                    "selected Q source ring has no emitted chart role"
                },
                QRingRoleFailure::SourceChanged => {
                    "selected Q source ring changes its exact shell/hole role"
                },
            };
            Err(GeometryErrorKind::Invalid(message.into()).into())
        },
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology::q_embedding) enum QRingRoleStatus {
    Classified(bool),
    Unclassified,
}

impl QRingRoleCheck {
    pub(in crate::geometry::topology::q_embedding) const fn role_count(&self) -> Option<usize> {
        match self {
            Self::Pass(roles) => Some(roles.len()),
            Self::NativeRequired(roles) => Some(roles.len()),
            Self::Reject { .. } => None,
        }
    }

    pub(in crate::geometry::topology::q_embedding) fn role_at(
        &self,
        index: usize,
    ) -> Option<QRingRoleStatus> {
        match self {
            Self::Pass(roles) => roles.get(index).copied().map(QRingRoleStatus::Classified),
            Self::NativeRequired(roles) => roles.get(index).copied(),
            Self::Reject { .. } => None,
        }
    }
}
