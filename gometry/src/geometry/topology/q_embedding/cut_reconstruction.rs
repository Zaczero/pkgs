use super::super::emission::cut_ports::CutNodePorts;
use super::{
    ExactChartPoint, ExactGeographicQ, ExactRational, GeographicFrame, GeometryErrorKind,
    ResourceError, Result,
};
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn reconstruct_cut_ports(
    frame: GeographicFrame,
    q: &ExactGeographicQ,
    cut_nodes: &[CutNodePorts],
    port_points: &mut [Option<ExactChartPoint>],
    cut_east: &ExactRational,
    work: &mut impl Work,
) -> Result<Vec<Option<ExactRational>>> {
    let period = ExactRational::from_f64(frame.longitude().period());
    work.charge_into_error(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut cut_latitudes = vec![None; q.nodes.len()];
    for cut in cut_nodes {
        work.charge_into_error(1)?;
        let node = q
            .nodes
            .get(cut.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node is unknown".into()))?;

        let mut latitude: Option<ExactRational> = None;
        for chord in &cut.chords {
            work.charge_into_error(1)?;
            let west_point = port_points
                .get(chord.west_far_port.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut reconstruction has no assigned west endpoint".into(),
                    )
                })?;
            let east_point = port_points
                .get(chord.east_far_port.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut reconstruction has no assigned east endpoint".into(),
                    )
                })?;
            let east_x = east_point.x.try_subtract_with_work(&period, work)?;
            if !east_x.try_cmp_with_work(&q.cut, work)?.is_lt()
                || !west_point.x.try_cmp_with_work(&q.cut, work)?.is_gt()
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q emitted source chord no longer crosses its chart cut".into(),
                )
                .into());
            }
            let width = west_point.x.try_subtract_with_work(&east_x, work)?;
            let t = q
                .cut
                .try_subtract_with_work(&east_x, work)?
                .try_quotient_with_work(&width, work)?;
            if !super::super::exact::in_unit_interval_with_work(&t, work)? {
                return Err(GeometryErrorKind::Invalid(
                    "Q emitted source chord misses its chart cut".into(),
                )
                .into());
            }
            let candidate_y = east_point.y.try_add_with_work(
                &t.try_product_with_work(
                    &west_point.y.try_subtract_with_work(&east_point.y, work)?,
                    work,
                )?,
                work,
            )?;
            if let Some(previous) = &latitude {
                if !previous.try_cmp_with_work(&candidate_y, work)?.is_eq() {
                    return Err(GeometryErrorKind::Invalid(
                        "Q emitted chords disagree at a cut node".into(),
                    )
                    .into());
                }
            } else {
                latitude = Some(candidate_y);
            }
        }
        let latitude = latitude.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "Q selected cut node has no paired emitted source chords".into(),
            )
        })?;
        if node.pole.is_some() {
            let exact = q
                .ports
                .get(cut.west.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node has no west port".into()))?;
            if !latitude.try_cmp_with_work(&exact.point.y, work)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "Q emitted chord moves an exact pole cut node".into(),
                )
                .into());
            }
        }
        for (port_id, x) in [(cut.west, &q.cut), (cut.east, cut_east)] {
            work.charge_into_error(1)?;
            *port_points.get_mut(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut port ordinal is invalid".into())
            })? = Some(ExactChartPoint::from_rationals(x.clone(), latitude.clone()));
        }
        *cut_latitudes
            .get_mut(cut.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node ordinal is invalid".into()))? =
            Some(latitude);
    }
    Ok(cut_latitudes)
}
