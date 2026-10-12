use std::borrow::Borrow;
use std::cmp::Ordering;

use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::exact::SourceRowId;
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, row_locations};
use crate::collections::{
    HashMap, HashSet, try_hash_map, try_hash_map_with_capacity, try_hash_set_with_capacity,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame, ScalarGeographicView};
use crate::geometry::coverage::{
    BorderTieBreak, CoverageOverlapRule, ScaledLengthSum, longest_border_ranked_owner,
    scaled_point_distance,
};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    BulkRTree, ExactRationalArea, ExpansionBudget, GeometryErrorKind, Loc, Segment, Shape,
    Unmetered, Work, XY, point_distance,
};

mod regions;

type RowIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;
type SpanIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;

#[derive(Clone, Copy)]
struct ChartBounds {
    min: [f64; 2],
    max: [f64; 2],
}

impl ChartBounds {
    fn include(&mut self, other: Self) {
        self.min[0] = self.min[0].min(other.min[0]);
        self.min[1] = self.min[1].min(other.min[1]);
        self.max[0] = self.max[0].max(other.max[0]);
        self.max[1] = self.max[1].max(other.max[1]);
    }

    fn rectangle(self) -> Rectangle<[f64; 2]> {
        Rectangle::from_corners(self.min, self.max)
    }
}

#[derive(Clone)]
struct ExactSegment {
    start: [ExactRational; 2],
    end: [ExactRational; 2],
}

struct QFaceMetrics {
    area: ExactRationalArea,
    perimeter: f64,
    boundary_darts: Vec<QDartId>,
    border_lengths: HashMap<usize, ScaledLengthSum>,
}

struct NodeEdge {
    row: usize,
    edge: usize,
    interior: bool,
}

pub(crate) struct GeographicCoverageAnalysis {
    pub(crate) topology: ExactGeographicTopology,
    pub(crate) face_rows: Vec<Vec<usize>>,
    pub(crate) span_rows: Vec<Vec<usize>>,
    pub(crate) invalid_source_segments: Vec<Vec<Segment>>,
}

impl GeographicCoverageAnalysis {
    pub(crate) fn is_valid(&self) -> bool {
        self.invalid_source_segments.iter().all(Vec::is_empty)
            && self.face_rows.iter().all(|rows| rows.len() <= 1)
    }

    pub(crate) fn has_shared_boundary(&self) -> bool {
        self.topology.sources.row_count > 1 && self.span_rows.iter().any(|rows| rows.len() > 1)
    }

    pub(crate) fn source_edges_match_shortest_chart(&self) -> Result<bool> {
        let frame = self.topology.sources.frame;
        for edge in &self.topology.sources.edges {
            let [start, end] = edge.source;
            if !edge.source_interval_reads_as(frame, false, start, end)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn source_rings_are_ordinary(&self) -> bool {
        self.topology.sources.rings.iter().all(|ring| {
            ring.summary.chart_turn == 0
                && ring.summary.domain == crate::geographic::RingDomain::Ordinary
                && !ring.summary.exact_axis_band
        })
    }

    pub(crate) fn assign_cleaning_faces(
        &self,
        row_areas: &[f64],
        gap_width: f64,
        overlap_rule: CoverageOverlapRule,
        work: &mut impl Work,
    ) -> Result<GeographicCoverageFaceAssignment> {
        if !(gap_width.is_finite() && gap_width >= 0.0) {
            return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
        }
        let row_count = self.topology.sources.row_count;
        if row_areas.len() != row_count {
            return Err(GeometryErrorKind::Invalid(
                "geographic coverage row areas are out of sync with the Q inventory".into(),
            )
            .into());
        }
        let metrics =
            if gap_width > 0.0 || matches!(overlap_rule, CoverageOverlapRule::LongestBorder) {
                Some(q_face_metrics(&self.topology, &self.span_rows, work)?)
            } else {
                None
            };
        let mut face_owners = vec![None; self.face_rows.len()];
        let mut gap_assignments = vec![false; self.face_rows.len()];
        for (face, occupants) in self.face_rows.iter().enumerate() {
            work.charge(1)?;
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
                            .border_lengths
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
                        row_areas,
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

pub(crate) struct GeographicCoverageFaceAssignment {
    pub(crate) owners: Vec<Option<usize>>,
    pub(crate) gap_assignments: Vec<bool>,
}

fn q_face_metrics(
    topology: &ExactGeographicTopology,
    span_rows: &[Vec<usize>],
    work: &mut impl Work,
) -> Result<Vec<QFaceMetrics>> {
    let q = &topology.q;
    work.charge(u64::try_from(q.global_face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut metrics = (0..q.global_face_count)
        .map(|_| {
            Ok(QFaceMetrics {
                area: ExactRationalArea::new(),
                perimeter: 0.0,
                boundary_darts: Vec::new(),
                border_lengths: try_hash_map()?,
            })
        })
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
        work.charge(1)?;
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
        let backward = QDartId(forward.0 ^ 1);
        for (face, dart_id) in [(*left, forward), (*right, backward)] {
            work.charge(1)?;
            let metric = metrics.get_mut(face.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face references an unknown Q face".into())
            })?;
            metric.boundary_darts.push(dart_id);
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
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
            let length = point_distance(start_xy, end_xy);
            metric.perimeter += length;
            let border_length = scaled_point_distance(start_xy, end_xy);
            for &row in span_rows.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage face span has no source rows".into())
            })? {
                work.charge(1)?;
                metric
                    .border_lengths
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
                work.charge(1)?;
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
                    &mut metric.area,
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
                &mut metric.area,
                &cycle_end_x,
                &cycle_end_y,
                &cycle_end_x,
                &pole_y,
                work,
            )?;
            add_cross_coordinates(
                &mut metric.area,
                &cycle_end_x,
                &pole_y,
                &cycle_start_x,
                &pole_y,
                work,
            )?;
            add_cross_coordinates(
                &mut metric.area,
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
    work.charge(1)?;
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
        work.charge(1)?;
        let right = QDartId(candidate.0 ^ 1);
        if face_of_dart(topology, candidate)? == face && face_of_dart(topology, right)? != face {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("coverage face boundary has no successor dart".into()).into())
}

fn gap_is_enclosed_and_narrow(metric: &QFaceMetrics, gap_width: f64) -> Result<bool> {
    if metric.boundary_darts.is_empty()
        || metric.area.sign() != ExactSign::Positive
        || metric.perimeter == 0.0
    {
        return Ok(false);
    }
    let area = metric.area.round_abs_half()?;
    Ok(2.0 * area / metric.perimeter < gap_width)
}

fn select_overlap_owner(
    candidates: &[usize],
    row_areas: &[f64],
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
                row_areas
                    .get(*left)
                    .expect("row candidate comes from the admitted coverage")
                    .total_cmp(
                        row_areas
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
                row_areas
                    .get(*left)
                    .expect("row candidate comes from the admitted coverage")
                    .total_cmp(
                        row_areas
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
                    .border_lengths
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
pub(crate) fn analyze_geographic_coverage<S: Borrow<Shape>>(
    rows: &[S],
    frame: GeographicFrame,
    gap_width: f64,
    operation: &'static str,
) -> Result<GeographicCoverageAnalysis> {
    let mut work = Unmetered;
    analyze_geographic_coverage_with_work(rows, frame, gap_width, operation, &mut work)
}

pub(crate) fn analyze_geographic_coverage_with_work<S: Borrow<Shape>>(
    rows: &[S],
    frame: GeographicFrame,
    gap_width: f64,
    operation: &'static str,
    work: &mut impl Work,
) -> Result<GeographicCoverageAnalysis> {
    if !(gap_width.is_finite() && gap_width >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
    }

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        work.charge(1)?;
        let shape = row.borrow();
        let view = ScalarGeographicView::try_new(frame, shape).map_err(|diagnostic| {
            crate::geographic::lonlat::geographic_admission_error(&diagnostic)
        })?;
        views.push(view);
    }
    let mut expansion_budget = ExpansionBudget::new(operation, "exact chart noding");
    let topology = ExactGeographicTopology::from_views_with_work_and_expansion_budget(
        frame,
        views.into_iter(),
        &mut expansion_budget,
        work,
    )?;
    let mut face_rows: Vec<Vec<usize>> = (0..topology.q.global_face_count)
        .map(|_| Vec::new())
        .collect();
    topology
        .q
        .face_rows
        .for_each_face_occupants(work, |face, occupants| {
            let rows = face_rows.get_mut(face.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face occupancy references an unknown face".into())
            })?;
            rows.extend(occupants.iter().map(|row| row.0 as usize));
            Ok(())
        })?;

    let source_rows = topology.sources.row_count;
    let source_edges = &topology.sources.edges;
    let mut locations = Vec::with_capacity(source_rows);
    for row in 0..source_rows {
        work.charge(1)?;
        let row = u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?;
        locations.push(row_locations(&topology, SourceRowId(row))?);
    }

    let mut span_rows = Vec::with_capacity(topology.q.spans.len());
    let mut span_edges = Vec::with_capacity(topology.q.spans.len());
    let mut row_span_counts = vec![0_usize; source_rows];
    let mut shared_span_counts = try_hash_map_with_capacity(topology.q.spans.len())?;
    let mut row_bounds: Vec<Option<ChartBounds>> = vec![None; source_rows];
    let mut span_bounds = Vec::with_capacity(topology.q.spans.len());
    let mut incident_spans = vec![Vec::new(); topology.q.nodes.len()];
    let mut node_edges: Vec<Vec<NodeEdge>> =
        (0..topology.q.nodes.len()).map(|_| Vec::new()).collect();

    for (span_index, span) in topology.q.spans.iter().enumerate() {
        work.charge(1)?;
        let start = topology
            .q
            .ports
            .get(span.ports[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q span has no start port".into())
            })?;
        let end = topology
            .q
            .ports
            .get(span.ports[1].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no end port".into()))?;
        let bounds = ChartBounds::from_points(&start.point, &end.point)?;
        span_bounds.push(bounds);
        for (endpoint, node) in span.nodes.iter().enumerate() {
            work.charge(1)?;
            if endpoint == 1 && span.nodes[0] == *node {
                continue;
            }
            incident_spans
                .get_mut(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q span has an unknown node".into())
                })?
                .push(span_index);
        }

        let mut rows_on_span = Vec::with_capacity(span.owners.len());
        let mut edges_on_span = Vec::with_capacity(span.owners.len());
        for owner in &span.owners {
            work.charge(1)?;
            let edge_index = owner.source_edge.0 as usize;
            let edge = source_edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q span has an unknown source edge".into())
            })?;
            let row = edge.row.0 as usize;
            rows_on_span.push(row);
            edges_on_span.push(edge_index);

            for endpoint in 0..2 {
                work.charge(1)?;
                let node = span.nodes[endpoint].0 as usize;
                let parameter = if (endpoint == 0) == owner.reversed {
                    &owner.source_range.end
                } else {
                    &owner.source_range.start
                };
                let interior = !parameter.is_zero()
                    && parameter.try_cmp(&ExactRational::from_f64(1.0))?.is_lt();
                let events = node_edges.get_mut(node).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q span has an unknown node".into())
                })?;
                if !events.iter().any(|event| {
                    event.row == row && event.edge == edge_index && event.interior == interior
                }) {
                    events.push(NodeEdge {
                        row,
                        edge: edge_index,
                        interior,
                    });
                }
            }
        }
        rows_on_span.sort_unstable();
        rows_on_span.dedup();
        edges_on_span.sort_unstable();
        edges_on_span.dedup();
        for &row in &rows_on_span {
            work.charge(1)?;
            let count = row_span_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q row is out of range".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if let Some(row_bounds) = row_bounds.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q row is out of range".into())
            })? {
                row_bounds.include(bounds);
            } else {
                *row_bounds.get_mut(row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q row is out of range".into())
                })? = Some(bounds);
            }
        }
        for left in 0..rows_on_span.len() {
            for right in left.wrapping_add(1)..rows_on_span.len() {
                work.charge(1)?;
                let pair = (
                    *rows_on_span
                        .get(left)
                        .expect("coverage row pair is within its span"),
                    *rows_on_span
                        .get(right)
                        .expect("coverage row pair is within its span"),
                );
                let count = shared_span_counts.entry(pair).or_insert(0_usize);
                *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        span_rows.push(rows_on_span);
        span_edges.push(edges_on_span);
    }

    expand_winding_row_bounds(&topology, &mut row_bounds, work)?;

    let mut coincident_pairs = try_hash_set_with_capacity(shared_span_counts.len())?;
    for (pair, shared_count) in shared_span_counts {
        work.charge(1)?;
        let [left, right] = [pair.0, pair.1];
        if shared_count != 0
            && row_span_counts.get(left) == Some(&shared_count)
            && row_span_counts.get(right) == Some(&shared_count)
        {
            coincident_pairs.insert(pair);
        }
    }

    work.charge_product(row_bounds.len(), 1)?;
    work.charge_product(span_bounds.len(), 1)?;
    let row_index = build_row_index(&row_bounds);
    let span_index = build_span_index(&span_bounds);
    let mut invalid_edges = vec![false; source_edges.len()];

    for (span_index, rows_on_span) in span_rows.iter().enumerate() {
        work.charge(1)?;
        if rows_on_span.is_empty() {
            continue;
        }
        let bounds = *span_bounds
            .get(span_index)
            .expect("coverage Q span has a retained chart envelope");
        let query = AABB::from_bounds(bounds.min, bounds.max);
        for candidate in row_index.locate_in_envelope_intersecting(query) {
            work.charge(1)?;
            let candidate_row = candidate.data;
            for &source_row in rows_on_span {
                work.charge(1)?;
                if source_row == candidate_row {
                    continue;
                }
                let ordinary_shared = rows_on_span.len() == 2
                    && rows_on_span
                        .first()
                        .zip(rows_on_span.get(1))
                        .is_some_and(|(&left, &right)| !coincident_pairs.contains(&(left, right)))
                    && rows_on_span.contains(&candidate_row);
                if ordinary_shared {
                    continue;
                }
                if locations
                    .get(candidate_row)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "coverage Q candidate row is out of range".into(),
                        )
                    })?
                    .span(span_index)?
                    .is_some_and(|location| location != Loc::Exterior)
                {
                    mark_edges(
                        &span_edges,
                        source_edges,
                        span_index,
                        source_row,
                        &mut invalid_edges,
                        work,
                    )?;
                }
            }
        }
    }

    mark_t_junction_edges(
        &node_edges,
        &incident_spans,
        &span_rows,
        &mut invalid_edges,
        work,
    )?;

    if gap_width > 0.0 {
        mark_near_gap_edges(
            frame,
            gap_width,
            &topology,
            &span_rows,
            &span_edges,
            &span_bounds,
            &span_index,
            &coincident_pairs,
            &mut invalid_edges,
            work,
        )?;
    }

    let mut output = vec![Vec::new(); source_rows];
    for (edge_index, edge) in source_edges.iter().enumerate() {
        work.charge(1)?;
        if !invalid_edges
            .get(edge_index)
            .copied()
            .expect("coverage invalid-edge flag matches its source inventory")
        {
            continue;
        }
        let row = edge.row.0 as usize;
        output
            .get_mut(row)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage edge row is out of range".into()))?
            .push(Segment {
                start: XY::new(edge.source[0].x, edge.source[0].y),
                end: XY::new(edge.source[1].x, edge.source[1].y),
            });
    }
    Ok(GeographicCoverageAnalysis {
        topology,
        face_rows,
        span_rows,
        invalid_source_segments: output,
    })
}

impl ChartBounds {
    fn from_points(
        start: &super::exact::ExactChartPoint,
        end: &super::exact::ExactChartPoint,
    ) -> Result<Self> {
        let (start_min_x, start_max_x) = start.x.outward_bounds()?;
        let (start_min_y, start_max_y) = start.y.outward_bounds()?;
        let (end_min_x, end_max_x) = end.x.outward_bounds()?;
        let (end_min_y, end_max_y) = end.y.outward_bounds()?;
        let [min_x, min_y] = [start_min_x.min(end_min_x), start_min_y.min(end_min_y)];
        let [max_x, max_y] = [start_max_x.max(end_max_x), start_max_y.max(end_max_y)];
        Ok(Self {
            min: [min_x, min_y],
            max: [max_x, max_y],
        })
    }
}

fn build_row_index(bounds: &[Option<ChartBounds>]) -> RowIndex {
    let entries = bounds
        .iter()
        .enumerate()
        .filter_map(|(row, bounds)| bounds.map(|bounds| GeomWithData::new(bounds.rectangle(), row)))
        .collect();
    BulkRTree::bulk_load_with_params(entries)
}

fn build_span_index(bounds: &[ChartBounds]) -> SpanIndex {
    let entries = bounds
        .iter()
        .enumerate()
        .map(|(span, bounds)| GeomWithData::new(bounds.rectangle(), span))
        .collect();
    BulkRTree::bulk_load_with_params(entries)
}

fn expand_winding_row_bounds(
    topology: &ExactGeographicTopology,
    row_bounds: &mut [Option<ChartBounds>],
    work: &mut impl Work,
) -> Result<()> {
    let frame = topology.sources.frame;
    let chart_min = topology.q.cut.outward_bounds()?.0;
    let chart_max = topology
        .q
        .cut
        .try_add(&ExactRational::from_f64(frame.longitude().period()))?
        .outward_bounds()?
        .1;
    let south_pole = frame.native_latitude_pole(crate::geographic::GeodeticPole::South);
    let north_pole = frame.native_latitude_pole(crate::geographic::GeodeticPole::North);
    let full_latitude = [south_pole.min(north_pole), south_pole.max(north_pole)];

    for ring in &topology.sources.rings {
        work.charge(1)?;
        if ring.summary.chart_turn == 0
            && !matches!(
                ring.summary.domain,
                crate::geographic::RingDomain::FullLongitude(_)
            )
        {
            continue;
        }
        let polygon = topology
            .sources
            .polygons
            .get(ring.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage winding ring has no source polygon".into())
            })?;
        let row = polygon.row.0 as usize;
        let bounds = row_bounds.get_mut(row).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage winding ring has an unknown source row".into())
        })?;
        let Some(bounds) = bounds else {
            continue;
        };
        bounds.min = [
            bounds.min[0].min(chart_min),
            bounds.min[1].min(full_latitude[0]),
        ];
        bounds.max = [
            bounds.max[0].max(chart_max),
            bounds.max[1].max(full_latitude[1]),
        ];
    }
    Ok(())
}

fn mark_edges(
    span_edges: &[Vec<usize>],
    source_edges: &[super::exact::ExactSourceEdge],
    span_index: usize,
    row: usize,
    invalid_edges: &mut [bool],
    work: &mut impl Work,
) -> Result<()> {
    let edges = span_edges.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage Q span edge map is incomplete".into())
    })?;
    for &edge in edges {
        work.charge(1)?;
        let source = source_edges.get(edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage source edge is out of range".into())
        })?;
        if source.row.0 as usize != row {
            continue;
        }
        let flag = invalid_edges.get_mut(edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage source edge is out of range".into())
        })?;
        *flag = true;
    }
    Ok(())
}

fn mark_t_junction_edges(
    node_edges: &[Vec<NodeEdge>],
    incident_spans: &[Vec<usize>],
    span_rows: &[Vec<usize>],
    invalid_edges: &mut [bool],
    work: &mut impl Work,
) -> Result<()> {
    for (node, events) in node_edges.iter().enumerate() {
        work.charge(1)?;
        let Some(incident) = incident_spans.get(node) else {
            continue;
        };
        for (left_index, left) in events.iter().enumerate() {
            for right in events.iter().skip(left_index.wrapping_add(1)) {
                work.charge(1)?;
                if left.row == right.row
                    || !(left.interior || right.interior)
                    || shared_incident_span_count(incident, span_rows, left.row, right.row, work)?
                        >= 2
                {
                    continue;
                }
                if let Some(flag) = invalid_edges.get_mut(left.edge) {
                    *flag = true;
                }
                if let Some(flag) = invalid_edges.get_mut(right.edge) {
                    *flag = true;
                }
            }
        }
    }
    Ok(())
}

fn shared_incident_span_count(
    incident: &[usize],
    span_rows: &[Vec<usize>],
    left: usize,
    right: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let mut count = 0_usize;
    for &span in incident {
        work.charge(1)?;
        if span_rows.get(span).is_some_and(|rows| {
            rows.binary_search(&left).is_ok() && rows.binary_search(&right).is_ok()
        }) {
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    Ok(count)
}

fn mark_near_gap_edges(
    frame: GeographicFrame,
    gap_width: f64,
    topology: &ExactGeographicTopology,
    span_rows: &[Vec<usize>],
    span_edges: &[Vec<usize>],
    span_bounds: &[ChartBounds],
    span_index: &SpanIndex,
    coincident_pairs: &HashSet<(usize, usize)>,
    invalid_edges: &mut [bool],
    work: &mut impl Work,
) -> Result<()> {
    let gap = ExactRational::from_f64(gap_width);
    let gap_squared = gap.try_square()?;
    let period = frame.longitude().period();
    let shifts = [
        (-period, ExactRational::from_f64(-period)),
        (0.0, ExactRational::from_f64(0.0)),
        (period, ExactRational::from_f64(period)),
    ];

    for (query_index, rows_on_query) in span_rows.iter().enumerate() {
        work.charge(1)?;
        let ordinary_shared = rows_on_query.len() == 2
            && rows_on_query
                .first()
                .zip(rows_on_query.get(1))
                .is_some_and(|(&left, &right)| !coincident_pairs.contains(&(left, right)));
        if rows_on_query.is_empty() || ordinary_shared {
            continue;
        }
        let bounds = *span_bounds
            .get(query_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage gap span has no bounds".into()))?;
        let query_segment = exact_span(topology, query_index)?;
        for &(shift_f64, ref shift) in &shifts {
            work.charge(1)?;
            let query = AABB::from_bounds(
                [
                    bounds.min[0] + shift_f64 - gap_width,
                    bounds.min[1] - gap_width,
                ],
                [
                    bounds.max[0] + shift_f64 + gap_width,
                    bounds.max[1] + gap_width,
                ],
            );
            for candidate in span_index.locate_in_envelope_intersecting(query) {
                work.charge(1)?;
                let candidate_index = candidate.data;
                if candidate_index == query_index {
                    continue;
                }
                let candidate_rows = span_rows.get(candidate_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage gap candidate span is out of range".into())
                })?;
                if candidate_rows.is_empty() {
                    continue;
                }
                let mut near_other_row = false;
                for &source_row in rows_on_query {
                    work.charge(1)?;
                    if candidate_rows
                        .iter()
                        .any(|&candidate_row| candidate_row != source_row)
                        && exact_gap_within(
                            &query_segment,
                            shift,
                            &exact_span(topology, candidate_index)?,
                            &gap_squared,
                            work,
                        )?
                    {
                        near_other_row = true;
                        break;
                    }
                }
                if near_other_row {
                    for &edge in span_edges.get(query_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "coverage gap span edge map is incomplete".into(),
                        )
                    })? {
                        work.charge(1)?;
                        *invalid_edges.get_mut(edge).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage gap source edge is out of range".into(),
                            )
                        })? = true;
                    }
                }
            }
        }
    }
    Ok(())
}

fn exact_span(topology: &ExactGeographicTopology, span_index: usize) -> Result<ExactSegment> {
    let span = topology.q.spans.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage exact span index is out of range".into())
    })?;
    let start = topology
        .q
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no start port".into()))?;
    let end = topology
        .q
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no end port".into()))?;
    Ok(ExactSegment {
        start: [start.point.x.clone(), start.point.y.clone()],
        end: [end.point.x.clone(), end.point.y.clone()],
    })
}

fn exact_gap_within(
    query: &ExactSegment,
    shift: &ExactRational,
    candidate: &ExactSegment,
    gap_squared: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    let query = translate_segment(query, shift, work)?;
    let query_midpoint = midpoint(&query.start, &query.end, work)?;
    let candidate_midpoint = midpoint(&candidate.start, &candidate.end, work)?;
    for point in [&query.start, &query.end, &query_midpoint] {
        if exact_point_segment_near(point, &candidate.start, &candidate.end, gap_squared, work)? {
            return Ok(true);
        }
    }
    for point in [&candidate.start, &candidate.end, &candidate_midpoint] {
        if exact_point_segment_near(point, &query.start, &query.end, gap_squared, work)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn translate_segment(
    segment: &ExactSegment,
    shift: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactSegment> {
    Ok(ExactSegment {
        start: [
            segment.start[0].try_add_with_work(shift, work)?,
            segment.start[1].try_clone_with_work(work)?,
        ],
        end: [
            segment.end[0].try_add_with_work(shift, work)?,
            segment.end[1].try_clone_with_work(work)?,
        ],
    })
}

fn midpoint(
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    work: &mut impl Work,
) -> Result<[ExactRational; 2]> {
    let two = ExactRational::from_f64(2.0);
    Ok([
        start[0]
            .try_add_with_work(&end[0], work)?
            .try_quotient_with_work(&two, work)?,
        start[1]
            .try_add_with_work(&end[1], work)?
            .try_quotient_with_work(&two, work)?,
    ])
}

fn exact_point_segment_near(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    gap_squared: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    let distance = point_segment_distance_squared(point, start, end, work)?;
    Ok(!distance.is_zero() && distance.try_cmp_with_work(gap_squared, work)? == Ordering::Less)
}

fn point_segment_distance_squared(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    work: &mut impl Work,
) -> Result<ExactRational> {
    let dx = end[0].try_subtract_with_work(&start[0], work)?;
    let dy = end[1].try_subtract_with_work(&start[1], work)?;
    let px = point[0].try_subtract_with_work(&start[0], work)?;
    let py = point[1].try_subtract_with_work(&start[1], work)?;
    let length_squared = dx
        .try_square_with_work(work)?
        .try_add_with_work(&dy.try_square_with_work(work)?, work)?;
    let zero = ExactRational::from_f64(0.0);
    if length_squared.is_zero() {
        return px
            .try_square_with_work(work)?
            .try_add_with_work(&py.try_square_with_work(work)?, work);
    }
    let projection = px
        .try_product_with_work(&dx, work)?
        .try_add_with_work(&py.try_product_with_work(&dy, work)?, work)?;
    if projection.try_cmp_with_work(&zero, work)? != Ordering::Greater {
        return px
            .try_square_with_work(work)?
            .try_add_with_work(&py.try_square_with_work(work)?, work);
    }
    if projection.try_cmp_with_work(&length_squared, work)? != Ordering::Less {
        let qx = point[0].try_subtract_with_work(&end[0], work)?;
        let qy = point[1].try_subtract_with_work(&end[1], work)?;
        return qx
            .try_square_with_work(work)?
            .try_add_with_work(&qy.try_square_with_work(work)?, work);
    }
    let cross = px
        .try_product_with_work(&dy, work)?
        .try_subtract_with_work(&py.try_product_with_work(&dx, work)?, work)?;
    cross
        .try_square_with_work(work)?
        .try_quotient_with_work(&length_squared, work)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology(rows: &[Shape]) -> ExactGeographicTopology {
        let frame = GeographicFrame::crs84();
        let views: Vec<_> = rows
            .iter()
            .map(|shape| {
                ScalarGeographicView::try_new(frame, shape).expect("geographic fixture admits")
            })
            .collect();
        ExactGeographicTopology::from_views(frame, views.into_iter()).expect("Q topology builds")
    }

    fn face_rows(topology: &ExactGeographicTopology) -> Vec<Vec<usize>> {
        let mut owners = Vec::new();
        let mut work = Unmetered;
        topology
            .q
            .face_rows
            .for_each_face_occupants(&mut work, |face, rows| {
                let actual: Vec<_> = rows.iter().map(|row| row.0 as usize).collect();
                for row in 0..topology.sources.row_count {
                    assert_eq!(
                        rows.binary_search(&SourceRowId(
                            u32::try_from(row).expect("test row ID fits")
                        ))
                        .is_ok(),
                        topology.q.face_rows.contains(
                            face,
                            SourceRowId(u32::try_from(row).expect("test row ID fits")),
                        ),
                    );
                }
                owners.push(actual);
                Ok(())
            })
            .expect("face occupancy sweep");
        owners.sort();
        owners
    }

    fn cleaning_assignments(
        rows: &[Shape],
        gap_width: f64,
        overlap_rule: CoverageOverlapRule,
    ) -> (Vec<Vec<usize>>, GeographicCoverageFaceAssignment) {
        let analysis =
            analyze_geographic_coverage(rows, GeographicFrame::crs84(), 0.0, "coverage_clean")
                .expect("geographic coverage admits");
        let face_rows = analysis.face_rows.clone();
        let row_areas: Vec<_> = rows.iter().map(Shape::area).collect();
        let mut work = Unmetered;
        let assignments = analysis
            .assign_cleaning_faces(&row_areas, gap_width, overlap_rule, &mut work)
            .expect("coverage face labels");
        (face_rows, assignments)
    }

    #[test]
    fn sparse_face_row_sweep_distinguishes_disjoint_regions_and_complement() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))").expect("first polygon"),
            crate::io::parse_wkt("POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))").expect("second polygon"),
        ];
        let owners = face_rows(&topology(&rows));
        assert!(owners.contains(&Vec::new()));
        assert!(owners.contains(&vec![0]));
        assert!(owners.contains(&vec![1]));
        assert!(owners.iter().all(|rows| rows.len() <= 1));
    }

    #[test]
    fn sparse_face_row_sweep_retains_overlap_owners_together() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").expect("outer polygon"),
            crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
                .expect("overlapping polygon"),
        ];
        let owners = face_rows(&topology(&rows));
        assert!(owners.contains(&vec![0, 1]));
        assert!(owners.contains(&vec![0]));
        assert!(owners.contains(&vec![1]));
        assert!(owners.contains(&Vec::new()));
    }

    #[test]
    fn geographic_face_labels_preserve_overlap_tie_rules() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").expect("larger polygon"),
            crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
                .expect("smaller polygon"),
        ];
        let rules = [
            (CoverageOverlapRule::MinIndex, 0),
            (CoverageOverlapRule::MaxArea, 0),
            (CoverageOverlapRule::MinArea, 1),
            (CoverageOverlapRule::LongestBorder, 1),
        ];
        for (rule, expected_row) in rules {
            let (face_rows, assignment) = cleaning_assignments(&rows, 0.0, rule);
            let overlap_face = face_rows
                .iter()
                .position(|owners| owners == &[0, 1])
                .expect("overlap has one worked Q face");
            assert_eq!(
                assignment.owners.get(overlap_face).copied().flatten(),
                Some(expected_row),
                "{rule:?}",
            );
        }

        let duplicate = rows[0].clone();
        let duplicate_rows = [rows[0].clone(), duplicate];
        let (face_rows, assignment) =
            cleaning_assignments(&duplicate_rows, 0.0, CoverageOverlapRule::LongestBorder);
        let overlap_face = face_rows
            .iter()
            .position(|owners| owners == &[0, 1])
            .expect("coincident rows share one Q face");
        assert_eq!(
            assignment.owners.get(overlap_face).copied().flatten(),
            Some(0),
        );
    }

    #[test]
    fn geographic_face_sweep_and_selector_charge_the_supplied_work_account() {
        let rows =
            [crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
                .expect("coverage polygon")];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_clean")
                .expect("geographic coverage admits");
        let row_areas = [rows[0].area()];

        let mut sweep_budget =
            crate::geometry::embedding_search::EvaluationBudget::with_remaining_ticks_for_test(
                "coverage_clean",
                0,
                0,
            )
            .expect("empty test budget");
        let sweep_error = analysis
            .topology
            .q
            .face_rows
            .for_each_face_occupants(&mut sweep_budget, |_, _| Ok(()))
            .expect_err("face sweep consumes work");
        assert!(matches!(
            sweep_error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                    operation: "coverage_clean",
                    ..
                }
            )
        ));

        let mut selector_budget =
            crate::geometry::embedding_search::EvaluationBudget::with_remaining_ticks_for_test(
                "coverage_clean",
                0,
                0,
            )
            .expect("empty test budget");
        let selector_error = match analysis.assign_cleaning_faces(
            &row_areas,
            1.0,
            CoverageOverlapRule::LongestBorder,
            &mut selector_budget,
        ) {
            Err(error) => error,
            Ok(_) => panic!("face metrics consume work"),
        };
        assert!(matches!(
            selector_error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                    operation: "coverage_clean",
                    ..
                }
            )
        ));
    }

    #[test]
    fn geographic_clean_labels_seam_overlap_without_absorbing_greenwich_rows() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))")
                .expect("seam coverage row"),
            crate::io::parse_wkt("POLYGON ((175 2, -175 2, -175 8, 175 8, 175 2))")
                .expect("overlapping seam row"),
            crate::io::parse_wkt("POLYGON ((-5 2, 5 2, 5 8, -5 8, -5 2))").expect("Greenwich row"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_clean")
                .expect("geographic source graph");
        let row_areas: Vec<_> = rows.iter().map(Shape::area).collect();
        let mut work = Unmetered;
        let assignment = analysis
            .assign_cleaning_faces(&row_areas, 0.0, CoverageOverlapRule::MinIndex, &mut work)
            .expect("geographic row labels");

        let overlap = analysis
            .face_rows
            .iter()
            .position(|owners| owners == &[0, 1])
            .expect("Q retains the overlapping seam face");
        let greenwich = analysis
            .face_rows
            .iter()
            .position(|owners| owners == &[2])
            .expect("Q retains the disjoint Greenwich face");
        assert_eq!(assignment.owners.get(overlap).copied().flatten(), Some(0));
        assert_eq!(assignment.owners.get(greenwich).copied().flatten(), Some(2));
    }

    #[test]
    fn geographic_face_labels_fill_only_the_enclosed_narrow_gap() {
        let rows: Vec<_> = (0..3)
            .flat_map(|x| {
                (0..3).filter_map(move |y| {
                    (x != 1 || y != 1).then(|| {
                        let (x, y) = (f64::from(x), f64::from(y));
                        crate::io::parse_wkt(&format!(
                            "POLYGON (({x} {y}, {} {y}, {} {}, {x} {}, {x} {y}))",
                            x + 1.0,
                            x + 1.0,
                            y + 1.0,
                            y + 1.0,
                        ))
                        .expect("coverage grid cell")
                    })
                })
            })
            .collect();
        let (face_rows, assignments) =
            cleaning_assignments(&rows, 1.0, CoverageOverlapRule::LongestBorder);
        let assigned_gaps: Vec<_> = assignments
            .gap_assignments
            .iter()
            .enumerate()
            .filter_map(|(face, &assigned)| assigned.then_some(face))
            .collect();
        assert_eq!(assigned_gaps.len(), 1);
        let gap_face = *assigned_gaps.first().expect("one selected gap");
        assert!(face_rows.get(gap_face).is_some_and(Vec::is_empty));
        assert_eq!(assignments.owners.get(gap_face).copied().flatten(), Some(6));
        assert_eq!(
            face_rows
                .iter()
                .zip(&assignments.owners)
                .filter(|(rows, owner)| rows.is_empty() && owner.is_none())
                .count(),
            1,
            "the exterior complement is not a gap candidate",
        );
    }

    #[test]
    fn geographic_face_labels_fill_a_seam_bordered_hole() {
        let holed = crate::io::parse_wkt(
            "POLYGON ((160 -10, -160 -10, -160 10, 160 10, 160 -10), \
             (178 -1, -178 -1, -178 1, 178 1, 178 -1))",
        )
        .expect("seam-bounded hole");
        let rows = [holed];
        let (face_rows, assignments) =
            cleaning_assignments(&rows, 2.0, CoverageOverlapRule::LongestBorder);
        let gap_face = assignments
            .gap_assignments
            .iter()
            .position(|&assigned| assigned)
            .expect("narrow seam-bordered hole is selected");
        assert!(face_rows.get(gap_face).is_some_and(Vec::is_empty));
        assert_eq!(assignments.owners.get(gap_face).copied().flatten(), Some(0));
        assert!(
            assignments.gap_assignments.len() == assignments.owners.len(),
            "face labels retain the global face inventory",
        );
    }

    #[test]
    fn geographic_face_labels_keep_a_polar_hole_distinct_from_the_complement() {
        for (shell_latitude, hole_latitude) in [(70.0, 89.0), (-70.0, -89.0)] {
            let holed = crate::io::parse_wkt(&format!(
                "POLYGON ((0 {shell_latitude}, 90 {shell_latitude}, \
                 180 {shell_latitude}, 270 {shell_latitude}, 0 {shell_latitude}), \
                 (0 {hole_latitude}, 90 {hole_latitude}, 180 {hole_latitude}, \
                 270 {hole_latitude}, 0 {hole_latitude}))",
            ))
            .expect("polar shell and hole");
            let rows = [holed];
            let (face_rows, assignments) =
                cleaning_assignments(&rows, 3.0, CoverageOverlapRule::LongestBorder);
            let selected_gaps: Vec<_> = assignments
                .gap_assignments
                .iter()
                .enumerate()
                .filter_map(|(face, &assigned)| assigned.then_some(face))
                .collect();
            assert_eq!(selected_gaps.len(), 1);
            let hole_face = *selected_gaps.first().expect("polar hole is selected");
            assert_eq!(
                face_rows.first(),
                Some(&vec![0]),
                "global face zero is the occupied band, not a reserved exterior",
            );
            assert_eq!(assignments.owners.first(), Some(&Some(0)));
            assert!(face_rows.get(hole_face).is_some_and(Vec::is_empty));
            assert_eq!(
                assignments.owners.get(hole_face).copied().flatten(),
                Some(0)
            );
            assert_eq!(
                face_rows
                    .iter()
                    .zip(&assignments.owners)
                    .filter(|(rows, owner)| rows.is_empty() && owner.is_none())
                    .count(),
                1,
                "the complementary face remains unassigned",
            );
        }
    }

    #[test]
    fn geographic_face_zero_keeps_the_whole_sphere_row() {
        let whole_sphere = crate::io::parse_wkt(
            "POLYGON ((-180 -90, -90 -90, 0 -90, 90 -90, 180 -90, \
             180 90, 90 90, 0 90, -90 90, -180 90, -180 0, -180 -90))",
        )
        .expect("whole-sphere source ring");
        let rows = [whole_sphere];
        let (face_rows, assignments) =
            cleaning_assignments(&rows, 3.0, CoverageOverlapRule::LongestBorder);

        assert_eq!(face_rows, [vec![0]]);
        assert_eq!(assignments.owners, [Some(0)]);
        assert_eq!(assignments.gap_assignments, [false]);
    }
}
