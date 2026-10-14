use super::super::q::{ExactGeographicTopology, GlobalFaceId, QDartId};
use super::{GeographicCoverageAnalysis, GeographicCoverageFaceAssignment};
use crate::collections::{HashMap, try_hash_map};
use crate::error::{ResourceError, Result};
use crate::geographic::ChartPole;
use crate::geometry::coverage::{
    BorderTieBreak, CoverageOverlapRule, ScaledLength, ScaledLengthSum, longest_border_ranked_owner,
};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{ChartMetric, ExactRationalArea, GeometryErrorKind, XY, point_distance};
use crate::work::Work;

struct QFaceMetrics {
    chart_area: ExactRationalArea,
    chart_perimeter: f64,
    boundary_darts: Vec<QDartId>,
    surface_border_lengths: HashMap<usize, ScaledLengthSum>,
}

fn q_face_metrics<M: ChartMetric>(
    topology: &ExactGeographicTopology,
    span_rows: &[Vec<usize>],
    metric: &M,
    work: &mut impl Work,
) -> Result<Vec<QFaceMetrics>> {
    let q = &topology.q;
    work.charge_into_error(
        u64::try_from(q.global_face_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut metrics = std::iter::repeat_with(|| {
        Ok(QFaceMetrics {
            chart_area: ExactRationalArea::new(),
            chart_perimeter: 0.0,
            boundary_darts: Vec::new(),
            surface_border_lengths: try_hash_map()?,
        })
    })
    .take(q.global_face_count)
    .collect::<Result<Vec<_>>>()?;
    let mut face_poles = vec![[false; 2]; q.global_face_count];
    for pole in [ChartPole::North, ChartPole::South] {
        let pole_index = match pole {
            ChartPole::North => 0,
            ChartPole::South => 1,
        };
        q.for_each_pole_sector(pole, work, |sector| {
            let face_pole = face_poles
                .get_mut(sector.face().0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage pole sector references an unknown face".into(),
                    )
                })?;
            *face_pole.get_mut(pole_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage pole sector index is out of range".into())
            })? = true;
            Ok(())
        })?;
    }

    for (span_index, face_pair) in q.span_global_faces.iter().enumerate() {
        work.charge_into_error(1)?;
        if q.span_is_collapsed_pole(span_index)? {
            continue;
        }
        let Some([left, right]) = face_pair else {
            continue;
        };
        if left == right {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage face span has no Q dart".into()))?;
        let rows_on_span = span_rows.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face span has no source rows".into())
        })?;
        let border_length = if rows_on_span.is_empty() {
            ScaledLength::default()
        } else {
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face span is out of range".into())
            })?;
            let owner = span.owners.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face span has no source owner".into())
            })?;
            let source_edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage face span has an unknown source edge".into(),
                    )
                })?;
            let retained_edge = crate::crs::ChartEdge::from_retained(
                source_edge.source[0],
                source_edge.source[1],
                topology.sources.frame,
                source_edge.retained_longitude(),
            )?;
            let start_parameter = owner.source_range.start.round()?;
            let end_parameter = owner.source_range.end.round()?;
            ScaledLength::from_finite(metric.chart_edge_interval_length(
                &retained_edge,
                start_parameter,
                end_parameter,
            )?)
        };
        let dart = q.darts.get(forward.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face references an unknown Q dart".into())
        })?;
        let start = q.ports.get(dart.from_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face dart has no start port".into())
        })?;
        let end = q.ports.get(dart.to_port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face dart has no end port".into())
        })?;
        let start_xy = XY::new(start.point.x.round()?, start.point.y.round()?);
        let end_xy = XY::new(end.point.x.round()?, end.point.y.round()?);
        let chart_length = point_distance(start_xy, end_xy);
        // Measure the retained source interval so a Q cut or longitude lift
        // cannot select a different chart branch for the ellipsoidal length.
        let backward = QDartId(forward.0 ^ 1);
        for (face, dart_id) in [(*left, forward), (*right, backward)] {
            work.charge_into_error(1)?;
            let metric = metrics.get_mut(face.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face references an unknown Q face".into())
            })?;
            metric.boundary_darts.push(dart_id);
            metric.chart_perimeter += chart_length;
            for &row in rows_on_span {
                work.charge_into_error(1)?;
                metric
                    .surface_border_lengths
                    .entry(row)
                    .or_insert_with(ScaledLengthSum::default)
                    .add(border_length);
            }
        }
    }

    let mut visited = vec![false; q.darts.len()];
    for (face_index, metric) in metrics.iter_mut().enumerate() {
        let face =
            GlobalFaceId(u32::try_from(face_index).map_err(|_| ResourceError::SizeOverflow)?);
        for &seed in &metric.boundary_darts {
            let visited_seed = visited.get(seed.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face seed dart is out of range".into())
            })?;
            if visited_seed {
                continue;
            }
            let first = q.darts.get(seed.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face seed dart is out of range".into())
            })?;
            let first_start = q.ports.get(first.from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face seed has no start port".into())
            })?;
            let cycle_start_x = first_start.point.x.try_clone_with_work(work)?;
            let cycle_start_y = first_start.point.y.try_clone_with_work(work)?;
            let mut x_offset = ExactRational::from_f64(0.0);
            let mut cycle_end_x = None;
            let mut cycle_end_y = None;
            let mut current = seed;
            loop {
                work.charge_into_error(1)?;
                let slot = visited.get_mut(current.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage face dart is out of range".into())
                })?;
                if *slot {
                    if current == seed {
                        break;
                    }
                    return Err(GeometryErrorKind::Invalid(
                        "coverage face boundary cycles intersect".into(),
                    )
                    .into());
                }
                *slot = true;
                let dart = q.darts.get(current.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage face dart is out of range".into())
                })?;
                let start = q.ports.get(dart.from_port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage face dart has no start port".into())
                })?;
                let end = q.ports.get(dart.to_port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage face dart has no end port".into())
                })?;
                let start_x = start.point.x.try_add_with_work(&x_offset, work)?;
                let end_x = end.point.x.try_add_with_work(&x_offset, work)?;
                add_chart_cross(
                    &mut metric.chart_area,
                    &start_x,
                    &start.point.y,
                    &end_x,
                    &end.point.y,
                    work,
                )?;
                cycle_end_x = Some(end_x.try_clone_with_work(work)?);
                cycle_end_y = Some(end.point.y.try_clone_with_work(work)?);
                let next = next_face_boundary_dart(topology, face, current, work)?;
                let next_dart = q.darts.get(next.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage face successor dart is out of range".into(),
                    )
                })?;
                if dart.to_node != next_dart.from_node {
                    return Err(GeometryErrorKind::Invalid(
                        "coverage face boundary successor changes Q nodes".into(),
                    )
                    .into());
                }
                let next_start = q.ports.get(next_dart.from_port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage face successor has no start port".into())
                })?;
                if end.point.y != next_start.point.y {
                    return Err(GeometryErrorKind::Invalid(
                        "coverage face quotient joins different latitudes".into(),
                    )
                    .into());
                }
                // Ports at one Q node may differ by a longitude period;
                // translate the next dart instead of measuring a false edge.
                x_offset = end_x.try_subtract_with_work(&next_start.point.x, work)?;
                current = next;
                if current == seed {
                    break;
                }
            }
            let cycle_end_x = cycle_end_x.ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face boundary cycle is empty".into())
            })?;
            let cycle_end_y = cycle_end_y.ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face boundary cycle is empty".into())
            })?;
            if cycle_start_y != cycle_end_y {
                return Err(GeometryErrorKind::Invalid(
                    "coverage face boundary cycle does not close in latitude".into(),
                )
                .into());
            }
            let [north_pole, south_pole] = *face_poles.get(face_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face has no pole-sector entry".into())
            })?;
            // Longitude-winding cycles need a pole to close their chart area.
            // The unique incident sector identifies a cap; pole-free or
            // two-pole faces use north consistently so nested rings subtract.
            let pole = if south_pole && !north_pole {
                ChartPole::South
            } else {
                ChartPole::North
            };
            let pole_y = ExactRational::from_f64(topology.sources.frame.chart_pole_latitude(pole));
            add_cross_coordinates(
                &mut metric.chart_area,
                &cycle_end_x,
                &cycle_end_y,
                &cycle_end_x,
                &pole_y,
                work,
            )?;
            add_cross_coordinates(
                &mut metric.chart_area,
                &cycle_end_x,
                &pole_y,
                &cycle_start_x,
                &pole_y,
                work,
            )?;
            add_cross_coordinates(
                &mut metric.chart_area,
                &cycle_start_x,
                &pole_y,
                &cycle_start_x,
                &cycle_start_y,
                work,
            )?;
        }
    }
    Ok(metrics)
}

fn add_chart_cross(
    area: &mut ExactRationalArea,
    start_x: &ExactRational,
    start_y: &ExactRational,
    end_x: &ExactRational,
    end_y: &ExactRational,
    work: &mut impl Work,
) -> Result<()> {
    add_cross_coordinates(area, start_x, start_y, end_x, end_y, work)
}

fn add_cross_coordinates(
    area: &mut ExactRationalArea,
    start_x: &ExactRational,
    start_y: &ExactRational,
    end_x: &ExactRational,
    end_y: &ExactRational,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(1)?;
    let numerator = start_x
        .try_product_with_work(end_y, work)?
        .try_subtract_with_work(&start_y.try_product_with_work(end_x, work)?, work)?;
    let (numerator, denominator) = numerator.parts();
    area.add_fraction(numerator.clone(), denominator.clone())
}

fn face_of_dart(topology: &ExactGeographicTopology, dart: QDartId) -> Result<GlobalFaceId> {
    let local =
        *topology.q.dart_faces.get(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face dart has no local face".into())
        })?;
    topology
        .q
        .global_face_of_local
        .get(local.0 as usize)
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage face dart has no global face".into()).into()
        })
}

fn next_face_boundary_dart(
    topology: &ExactGeographicTopology,
    face: GlobalFaceId,
    incoming: QDartId,
    work: &mut impl Work,
) -> Result<QDartId> {
    let q = &topology.q;
    let dart = q.darts.get(incoming.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary references an unknown dart".into())
    })?;
    let twin = QDartId(incoming.0 ^ 1);
    let twin_position = *q
        .dart_positions
        .get(twin.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage boundary twin has no fan".into()))?;
    let start = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage boundary node has no fan".into()))?;
    let end = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize + 1)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage boundary node has no fan end".into())
        })?;
    let length = end.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary node has reversed fan offsets".into())
    })?;
    if length == 0 || twin_position >= length {
        return Err(GeometryErrorKind::Invalid(
            "coverage boundary twin is outside its node fan".into(),
        )
        .into());
    }
    let fan = q.outgoing.get(start..end).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary node fan is incomplete".into())
    })?;
    let (before_twin, twin_after) = fan.split_at(twin_position);
    let (_, after_twin) = twin_after.split_first().ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary twin is outside its node fan".into())
    })?;
    for candidate in std::iter::once(twin)
        .chain(before_twin.iter().rev().copied())
        .chain(after_twin.iter().rev().copied())
    {
        work.charge_into_error(1)?;
        let right = QDartId(candidate.0 ^ 1);
        if face_of_dart(topology, candidate)? == face && face_of_dart(topology, right)? != face {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("coverage face boundary has no successor dart".into()).into())
}

fn gap_is_enclosed_and_narrow(metric: &QFaceMetrics, gap_width: f64) -> Result<bool> {
    if metric.boundary_darts.is_empty()
        || metric.chart_area.sign() != ExactSign::Positive
        || metric.chart_perimeter == 0.0
    {
        return Ok(false);
    }
    let area = metric.chart_area.round_abs_half()?;
    Ok(2.0 * area / metric.chart_perimeter < gap_width)
}

fn select_overlap_owner(
    candidates: &[usize],
    row_surface_areas: &[f64],
    overlap_rule: CoverageOverlapRule,
    metric: Option<&QFaceMetrics>,
) -> Result<usize> {
    match overlap_rule {
        CoverageOverlapRule::MinIndex => candidates.first().copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage overlap face has no candidate rows".into()).into()
        }),
        CoverageOverlapRule::MaxArea => candidates
            .iter()
            .copied()
            .max_by(|left, right| {
                row_surface_areas
                    .get(*left)
                    .expect("row candidate comes from the admitted coverage")
                    .total_cmp(
                        row_surface_areas
                            .get(*right)
                            .expect("row candidate comes from the admitted coverage"),
                    )
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage overlap face has no candidate rows".into())
                    .into()
            }),
        CoverageOverlapRule::MinArea => candidates
            .iter()
            .copied()
            .min_by(|left, right| {
                row_surface_areas
                    .get(*left)
                    .expect("row candidate comes from the admitted coverage")
                    .total_cmp(
                        row_surface_areas
                            .get(*right)
                            .expect("row candidate comes from the admitted coverage"),
                    )
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage overlap face has no candidate rows".into())
                    .into()
            }),
        CoverageOverlapRule::LongestBorder => {
            let metric = metric.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage overlap face is missing border statistics".into(),
                )
            })?;
            let ranked = candidates.iter().copied().map(|row| {
                let rank = metric
                    .surface_border_lengths
                    .get(&row)
                    .copied()
                    .unwrap_or_default()
                    .rank();
                (row, rank)
            });
            longest_border_ranked_owner(ranked, BorderTieBreak::LowestRow, true).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage overlap face has no border owner".into())
                    .into()
            })
        },
    }
}

impl GeographicCoverageAnalysis {
    pub(crate) fn has_overlapping_faces(&self) -> bool {
        self.face_rows.iter().any(|occupants| occupants.len() > 1)
    }

    pub(crate) fn assign_cleaning_faces(
        &self,
        row_surface_areas: &[f64],
        gap_width: f64,
        overlap_rule: CoverageOverlapRule,
        metric: Option<&impl ChartMetric>,
        work: &mut impl Work,
    ) -> Result<GeographicCoverageFaceAssignment> {
        if !(gap_width.is_finite() && gap_width >= 0.0) {
            return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
        }
        let row_count = self.topology.sources.row_count;
        if row_surface_areas.len() != row_count {
            return Err(GeometryErrorKind::Invalid(
                "geographic coverage row surface areas are out of sync with the Q inventory".into(),
            )
            .into());
        }
        let metrics = if gap_width > 0.0
            || (self.has_overlapping_faces()
                && matches!(overlap_rule, CoverageOverlapRule::LongestBorder))
        {
            let metric = metric.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "geographic border ranking is missing the operation's CRS metric".into(),
                )
            })?;
            Some(q_face_metrics(
                &self.topology,
                &self.span_rows,
                metric,
                work,
            )?)
        } else {
            None
        };
        let mut face_owners = vec![None; self.face_rows.len()];
        let mut gap_assignments = vec![false; self.face_rows.len()];
        for (face, occupants) in self.face_rows.iter().enumerate() {
            work.charge_into_error(1)?;
            let owner = match occupants.as_slice() {
                [] if gap_width > 0.0 => {
                    let metric = metrics
                        .as_ref()
                        .and_then(|metrics| metrics.get(face))
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage face metric is out of range".into(),
                            )
                        })?;
                    if gap_is_enclosed_and_narrow(metric, gap_width)? {
                        let candidates = metric
                            .surface_border_lengths
                            .iter()
                            .map(|(&row, &length)| (row, length.rank()));
                        let owner = longest_border_ranked_owner(
                            candidates,
                            BorderTieBreak::HighestRow,
                            false,
                        );
                        if owner.is_some() {
                            *gap_assignments.get_mut(face).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "coverage gap assignment face is out of range".into(),
                                )
                            })? = true;
                        }
                        owner
                    } else {
                        None
                    }
                },
                [] => None,
                [only] => Some(*only),
                _ => {
                    let metric = metrics.as_ref().and_then(|metrics| metrics.get(face));
                    Some(select_overlap_owner(
                        occupants,
                        row_surface_areas,
                        overlap_rule,
                        metric,
                    )?)
                },
            };
            *face_owners.get_mut(face).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face owner slot is out of range".into())
            })? = owner;
        }
        Ok(GeographicCoverageFaceAssignment {
            owners: face_owners,
            gap_assignments,
        })
    }
}
