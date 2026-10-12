#![expect(
    dead_code,
    reason = "C2 exposes a complete Q producer artifact before the parent-owned C3 writer binds it"
)]

use super::super::exact::{ExactSourceRange, SourceEdgeId, SourceEdgeKind, SourceRowId};
use super::super::overlay::{self, BoundaryCycle, SelectedRegionPoles};
use super::super::q::{ChartSpanId, GlobalFaceId, QNodeId, QPortId};
use super::{GeographicCoverageAnalysis, GeographicCoverageFaceAssignment};
use crate::error::{ResourceError, Result};
use crate::geographic::ChartPole;
use crate::geometry::GeometryErrorKind;
use crate::geometry::embedding_search::Work;
use crate::geometry::segments::ordering::try_sort_by_with_work;

pub(in crate::geometry::topology) struct GeographicCoverageBoundaryPlans<'q> {
    pub(in crate::geometry::topology) topology: &'q super::super::q::ExactGeographicTopology,
    pub(in crate::geometry::topology) rows: Vec<GeographicCoverageRowBoundaryPlan>,
    pub(in crate::geometry::topology) face_regions: Vec<Option<GeographicCoverageRegionId>>,
    pub(in crate::geometry::topology) live_spans: Vec<ChartSpanId>,
    pub(in crate::geometry::topology) live_ports: Vec<QPortId>,
    pub(in crate::geometry::topology) live_nodes: Vec<QNodeId>,
}

pub(in crate::geometry::topology) struct GeographicCoverageRowBoundaryPlan {
    pub(in crate::geometry::topology) output_row: usize,
    pub(in crate::geometry::topology) whole_sphere: bool,
    pub(in crate::geometry::topology) north_pole: bool,
    pub(in crate::geometry::topology) south_pole: bool,
    pub(in crate::geometry::topology) source_polygons: Vec<usize>,
    pub(in crate::geometry::topology) regions: Vec<GeographicCoverageRegionBoundaryPlan>,
}

pub(in crate::geometry::topology) struct GeographicCoverageRegionBoundaryPlan {
    pub(in crate::geometry::topology) id: GeographicCoverageRegionId,
    pub(in crate::geometry::topology) poles: SelectedRegionPoles,
    pub(in crate::geometry::topology) cycles: Vec<GeographicCoverageBoundaryCycle>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct GeographicCoverageRegionId {
    pub(in crate::geometry::topology) output_row: usize,
    pub(in crate::geometry::topology) local_region_root: GlobalFaceId,
}

pub(in crate::geometry::topology) struct GeographicCoverageBoundaryCycle {
    pub(in crate::geometry::topology) cycle_index: usize,
    pub(in crate::geometry::topology) uses: Vec<GeographicCoverageDirectedSpanUse>,
}

pub(in crate::geometry::topology) struct GeographicCoverageDirectedSpanUse {
    pub(in crate::geometry::topology) span: ChartSpanId,
    pub(in crate::geometry::topology) from: QPortId,
    pub(in crate::geometry::topology) to: QPortId,
    pub(in crate::geometry::topology) from_presentation: GeographicCoveragePortPresentation,
    pub(in crate::geometry::topology) to_presentation: GeographicCoveragePortPresentation,
    pub(in crate::geometry::topology) root_uses: Vec<GeographicCoverageRootUse>,
}

pub(in crate::geometry::topology) struct GeographicCoveragePortPresentation {
    pub(in crate::geometry::topology) port: QPortId,
    pub(in crate::geometry::topology) node: QNodeId,
    pub(in crate::geometry::topology) seam_cut: bool,
    pub(in crate::geometry::topology) pole: Option<ChartPole>,
}

pub(in crate::geometry::topology) struct GeographicCoverageRootUse {
    pub(in crate::geometry::topology) owner_index: usize,
    pub(in crate::geometry::topology) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology) source_row: SourceRowId,
    /// The exact source interval is oriented in the directed boundary's walk order.
    pub(in crate::geometry::topology) source_range: ExactSourceRange,
    pub(in crate::geometry::topology) source_forward: bool,
    /// Longitude periods from this Q span to the source ring's continuous lift.
    pub(in crate::geometry::topology) lift: i64,
    /// One coowner supplies the emitted source record; all coowners remain in the recipe.
    pub(in crate::geometry::topology) emitted: bool,
}

struct PendingBoundaryCycle {
    output_row: usize,
    boundary: BoundaryCycle,
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry::topology) fn cleaning_boundary_plans<'q>(
        &'q self,
        assignment: &GeographicCoverageFaceAssignment,
        work: &mut impl Work,
    ) -> Result<GeographicCoverageBoundaryPlans<'q>> {
        let topology = &self.topology;
        let q = &topology.q;
        let row_count = topology.sources.row_count;
        if assignment.owners.len() != q.global_face_count
            || assignment.gap_assignments.len() != q.global_face_count
            || self.face_rows.len() != q.global_face_count
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage row labels do not match Q's global face table".into(),
            )
            .into());
        }

        work.charge_into_error(
            u64::try_from(assignment.owners.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut selected_faces = Vec::with_capacity(assignment.owners.len());
        let mut owned_face_counts = vec![0_usize; row_count];
        for (&owner, &is_gap) in assignment.owners.iter().zip(&assignment.gap_assignments) {
            let selected = owner.is_some();
            selected_faces.push(selected);
            if let Some(row) = owner {
                work.charge_into_error(1)?;
                let count = owned_face_counts.get_mut(row).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage face owner is outside the source row table".into(),
                    )
                })?;
                *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            } else if is_gap {
                return Err(GeometryErrorKind::Invalid(
                    "an assigned coverage gap has no output row".into(),
                )
                .into());
            }
        }

        let mut regions = overlay::coverage_face_regions(q, &assignment.owners, work)?;
        let mut boundary_cycles =
            overlay::coverage_area_boundaries(q, &assignment.owners, &mut regions, work)?
                .into_iter()
                .map(|(output_row, boundary)| PendingBoundaryCycle {
                    output_row,
                    boundary,
                })
                .collect::<Vec<_>>();
        try_sort_by_with_work(&mut boundary_cycles, work, |left, right, _work| {
            Ok(left
                .output_row
                .cmp(&right.output_row)
                .then(left.boundary.region.cmp(&right.boundary.region)))
        })?;

        let face_roots = overlay::selected_face_roots(q, &selected_faces, &mut regions, work)?;
        let root_capacity = face_roots
            .len()
            .checked_add(boundary_cycles.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut region_roots = Vec::with_capacity(root_capacity);
        for root in face_roots.iter().flatten() {
            work.charge_into_error(1)?;
            region_roots.push(*root);
        }
        for pending in &boundary_cycles {
            work.charge_into_error(1)?;
            region_roots.push(pending.boundary.region);
        }
        let region_facts =
            overlay::selected_region_facts(q, &selected_faces, &mut regions, &region_roots, work)?;
        let mut face_regions = Vec::with_capacity(face_roots.len());
        for (&owner, root) in assignment.owners.iter().zip(face_roots) {
            work.charge_into_error(1)?;
            match (owner, root) {
                (None, None) => face_regions.push(None),
                (Some(output_row), Some(local_root)) => {
                    face_regions.push(Some(GeographicCoverageRegionId {
                        output_row,
                        local_region_root: GlobalFaceId(
                            u32::try_from(local_root).map_err(|_| ResourceError::SizeOverflow)?,
                        ),
                    }));
                },
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "coverage face ownership and region roots disagree".into(),
                    )
                    .into());
                },
            }
        }

        let mut rows = (0..row_count)
            .map(|output_row| GeographicCoverageRowBoundaryPlan {
                output_row,
                whole_sphere: false,
                north_pole: false,
                south_pole: false,
                source_polygons: Vec::new(),
                regions: Vec::new(),
            })
            .collect::<Vec<_>>();
        for fact in &region_facts {
            work.charge_into_error(1)?;
            let owner = assignment
                .owners
                .get(fact.root)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "selected region root has no coverage output owner".into(),
                    )
                })?;
            let row = rows.get_mut(owner).ok_or_else(|| {
                GeometryErrorKind::Invalid("selected region output row is unknown".into())
            })?;
            row.north_pole |= fact.poles.includes(ChartPole::North);
            row.south_pole |= fact.poles.includes(ChartPole::South);
        }
        if q.global_face_count != 0 {
            for (output_row, &owned_faces) in owned_face_counts.iter().enumerate() {
                work.charge_into_error(1)?;
                if owned_faces != q.global_face_count {
                    continue;
                }
                let row = rows.get_mut(output_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere output row is unknown".into())
                })?;
                row.whole_sphere = true;
            }
        }
        for (polygon_index, polygon) in topology.sources.polygons.iter().enumerate() {
            work.charge_into_error(1)?;
            let row_index = polygon.row.0 as usize;
            rows.get_mut(row_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage source polygon row is unknown".into())
                })?
                .source_polygons
                .push(polygon_index);
        }

        let mut span_live = vec![false; q.spans.len()];
        let mut port_live = vec![false; q.ports.len()];
        let mut node_live = vec![false; q.nodes.len()];
        for (cycle_index, pending) in boundary_cycles.into_iter().enumerate() {
            work.charge_into_error(1)?;
            let row = rows.get_mut(pending.output_row).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage boundary row is unknown".into())
            })?;
            let region_id = GeographicCoverageRegionId {
                output_row: pending.output_row,
                local_region_root: GlobalFaceId(
                    u32::try_from(pending.boundary.region)
                        .map_err(|_| ResourceError::SizeOverflow)?,
                ),
            };
            let region_fact_index = region_facts
                .binary_search_by_key(&pending.boundary.region, |fact| fact.root)
                .map_err(|_| {
                    GeometryErrorKind::Invalid(
                        "coverage boundary has no selected-region pole facts".into(),
                    )
                })?;
            let poles = region_facts
                .get(region_fact_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage boundary region facts are incomplete".into(),
                    )
                })?
                .poles;
            if row
                .regions
                .last()
                .is_none_or(|region| region.id != region_id)
            {
                row.regions.push(GeographicCoverageRegionBoundaryPlan {
                    id: region_id,
                    poles,
                    cycles: Vec::new(),
                });
            }
            let region = row.regions.last_mut().ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage boundary region was not created".into())
            })?;
            let mut uses = Vec::with_capacity(pending.boundary.darts.len());
            for dart_id in pending.boundary.darts {
                work.charge_into_error(1)?;
                let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage boundary has an unknown dart".into())
                })?;
                let span = q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage boundary has an unknown span".into())
                })?;
                let from_presentation = coverage_port_presentation(topology, dart.from_port, work)?;
                let to_presentation = coverage_port_presentation(topology, dart.to_port, work)?;
                let emitted_owner = coverage_area_owner_index(
                    topology,
                    pending.output_row,
                    dart.span.0 as usize,
                    work,
                )?;
                let mut root_uses = Vec::with_capacity(span.owners.len());
                for (owner_index, owner) in span.owners.iter().enumerate() {
                    work.charge_into_error(1)?;
                    let source_forward = (dart.from_port == span.ports[0]) != owner.reversed;
                    let source_range = if source_forward {
                        ExactSourceRange {
                            start: owner.source_range.start.try_clone_with_work(work)?,
                            end: owner.source_range.end.try_clone_with_work(work)?,
                        }
                    } else {
                        ExactSourceRange {
                            start: owner.source_range.end.try_clone_with_work(work)?,
                            end: owner.source_range.start.try_clone_with_work(work)?,
                        }
                    };
                    let source = topology
                        .sources
                        .edges
                        .get(owner.source_edge.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage boundary owner has an unknown source edge".into(),
                            )
                        })?;
                    root_uses.push(GeographicCoverageRootUse {
                        owner_index,
                        source_edge: owner.source_edge,
                        source_row: source.row,
                        source_range,
                        source_forward,
                        lift: owner.lift,
                        emitted: owner_index == emitted_owner,
                    });
                }
                mark_live(
                    &mut span_live,
                    dart.span.0 as usize,
                    work,
                    "coverage boundary span",
                )?;
                for port_id in [dart.from_port, dart.to_port] {
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "coverage boundary has an unknown endpoint port".into(),
                        )
                    })?;
                    mark_live(
                        &mut port_live,
                        port_id.0 as usize,
                        work,
                        "coverage boundary port",
                    )?;
                    mark_live(
                        &mut node_live,
                        port.node.0 as usize,
                        work,
                        "coverage boundary node",
                    )?;
                }
                uses.push(GeographicCoverageDirectedSpanUse {
                    span: dart.span,
                    from: dart.from_port,
                    to: dart.to_port,
                    from_presentation,
                    to_presentation,
                    root_uses,
                });
            }
            region
                .cycles
                .push(GeographicCoverageBoundaryCycle { cycle_index, uses });
        }

        let live_spans = live_ids(&span_live, work)?
            .into_iter()
            .map(|index| {
                Ok(ChartSpanId(
                    u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let live_ports = live_ids(&port_live, work)?
            .into_iter()
            .map(|index| {
                Ok(QPortId(
                    u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let live_nodes = live_ids(&node_live, work)?
            .into_iter()
            .map(|index| {
                Ok(QNodeId(
                    u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(GeographicCoverageBoundaryPlans {
            topology: &self.topology,
            rows,
            face_regions,
            live_spans,
            live_ports,
            live_nodes,
        })
    }
}

fn coverage_port_presentation(
    topology: &super::super::q::ExactGeographicTopology,
    port_id: QPortId,
    work: &mut impl Work,
) -> Result<GeographicCoveragePortPresentation> {
    work.charge_into_error(1)?;
    let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage presentation port is unknown".into())
    })?;
    let node = topology.q.nodes.get(port.node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage presentation node is unknown".into())
    })?;
    Ok(GeographicCoveragePortPresentation {
        port: port_id,
        node: port.node,
        seam_cut: port.cut,
        pole: node.pole,
    })
}

fn coverage_area_owner_index(
    topology: &super::super::q::ExactGeographicTopology,
    output_row: usize,
    span_index: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let span =
        topology.q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage boundary span is unknown".into())
        })?;
    let output_row =
        SourceRowId(u32::try_from(output_row).map_err(|_| ResourceError::SizeOverflow)?);
    let mut first_ring_owner = None;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge_into_error(1)?;
        let source = topology
            .sources
            .edges
            .get(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage boundary owner edge is unknown".into())
            })?;
        if !matches!(source.kind, SourceEdgeKind::Ring { .. }) {
            continue;
        }
        first_ring_owner.get_or_insert(owner_index);
        if source.row == output_row {
            return Ok(owner_index);
        }
    }
    first_ring_owner.ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage boundary span has no original ring owner".into())
            .into()
    })
}

fn mark_live(live: &mut [bool], index: usize, work: &mut impl Work, label: &str) -> Result<()> {
    work.charge_into_error(1)?;
    *live.get_mut(index).ok_or_else(|| {
        GeometryErrorKind::Invalid(format!("coverage live {label} index is out of range").into())
    })? = true;
    Ok(())
}

fn live_ids(live: &[bool], work: &mut impl Work) -> Result<Vec<usize>> {
    work.charge_into_error(u64::try_from(live.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    Ok(live
        .iter()
        .enumerate()
        .filter_map(|(index, &is_live)| is_live.then_some(index))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::coverage::CoverageOverlapRule;
    use crate::geometry::{Shape, Unmetered};

    fn build_analysis(rows: &[Shape]) -> GeographicCoverageAnalysis {
        super::super::analyze_geographic_coverage(
            rows,
            GeographicFrame::crs84(),
            0.0,
            "coverage_clean",
        )
        .expect("geographic coverage topology")
    }

    fn labels(
        analysis: &GeographicCoverageAnalysis,
        rows: &[Shape],
        gap_width: f64,
    ) -> GeographicCoverageFaceAssignment {
        let row_areas = rows.iter().map(Shape::area).collect::<Vec<_>>();
        analysis
            .assign_cleaning_faces(
                &row_areas,
                gap_width,
                CoverageOverlapRule::MinIndex,
                &mut Unmetered,
            )
            .expect("geographic face labels")
    }

    #[test]
    fn row_boundary_plans_keep_both_directed_uses_of_a_shared_source_span() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))").expect("left coverage row"),
            crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")
                .expect("right coverage row"),
        ];
        let analysis = build_analysis(&rows);
        let assignment = labels(&analysis, &rows, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("row-region boundary plans");

        let shared_span = analysis
            .topology
            .q
            .spans
            .iter()
            .position(|span| {
                let owners = span
                    .owners
                    .iter()
                    .filter_map(|owner| {
                        analysis
                            .topology
                            .sources
                            .edges
                            .get(owner.source_edge.0 as usize)
                            .map(|edge| edge.row.0 as usize)
                    })
                    .collect::<std::collections::BTreeSet<_>>();
                owners.contains(&0) && owners.contains(&1)
            })
            .expect("one shared physical Q span");
        let shared_id =
            ChartSpanId(u32::try_from(shared_span).expect("shared span ID fits the admitted Q"));
        let row_use = |row: usize| {
            plans
                .rows
                .get(row)
                .into_iter()
                .flat_map(|row| &row.regions)
                .flat_map(|region| &region.cycles)
                .flat_map(|cycle| &cycle.uses)
                .find(|use_| use_.span == shared_id)
                .expect("row boundary retains the shared span")
        };
        let left_use = row_use(0);
        let right_use = row_use(1);
        assert_eq!(left_use.from, right_use.to);
        assert_eq!(left_use.to, right_use.from);
        assert_eq!(
            plans
                .live_spans
                .iter()
                .filter(|&&span| span == shared_id)
                .count(),
            1,
            "the union reference stores the physical span once",
        );

        for (output_row, directed) in [(0, left_use), (1, right_use)] {
            let mut emitted_rows = Vec::new();
            for root_use in &directed.root_uses {
                let span = analysis
                    .topology
                    .q
                    .spans
                    .get(directed.span.0 as usize)
                    .expect("planned span belongs to Q");
                let source = span
                    .owners
                    .get(root_use.owner_index)
                    .expect("root reference belongs to the span");
                let source_row = analysis
                    .topology
                    .sources
                    .edges
                    .get(source.source_edge.0 as usize)
                    .expect("Q owner resolves to its source root")
                    .row;
                assert_eq!(root_use.source_edge, source.source_edge);
                assert_eq!(root_use.source_row, source_row);
                assert_eq!(root_use.lift, source.lift);
                let source_forward = (directed.from == span.ports[0]) != source.reversed;
                assert_eq!(root_use.source_forward, source_forward);
                let expected_range = if source_forward {
                    source.source_range.clone()
                } else {
                    ExactSourceRange {
                        start: source.source_range.end.clone(),
                        end: source.source_range.start.clone(),
                    }
                };
                assert_eq!(root_use.source_range, expected_range);
                if root_use.emitted {
                    emitted_rows.push(source_row.0 as usize);
                }
            }
            assert_eq!(emitted_rows, [output_row]);
        }
    }

    #[test]
    fn row_boundary_plans_keep_disconnected_regions_and_hole_cycles() {
        let disconnected = [crate::io::parse_wkt(
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), \
             ((3 0, 4 0, 4 1, 3 1, 3 0)))",
        )
        .expect("disconnected coverage row")];
        let analysis = build_analysis(&disconnected);
        let assignment = labels(&analysis, &disconnected, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("disconnected row plan");
        assert_eq!(plans.rows.first().map(|row| row.regions.len()), Some(2));
        assert!(
            plans.rows.first().is_some_and(|row| {
                row.regions
                    .first()
                    .zip(row.regions.get(1))
                    .is_some_and(|(first, second)| first.id != second.id)
            }),
            "region identity includes the disconnected local root",
        );

        let holed = [crate::io::parse_wkt(
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), \
             (2 2, 2 8, 8 8, 8 2, 2 2))",
        )
        .expect("coverage row with a hole")];
        let analysis = build_analysis(&holed);
        let assignment = labels(&analysis, &holed, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("holed row plan");
        let region = plans
            .rows
            .first()
            .and_then(|row| row.regions.first())
            .expect("one region owns the shell and hole");
        assert_eq!(region.cycles.len(), 2);
        assert!(
            region
                .cycles
                .iter()
                .all(|cycle| cycle.uses.iter().all(|use_| !use_.root_uses.is_empty())),
            "both directed ring cycles retain original source roots",
        );
    }

    #[test]
    fn row_boundary_plans_preserve_pole_and_whole_sphere_authority() {
        let cap = [
            crate::io::parse_wkt("POLYGON ((0 80, 90 80, 180 80, 270 80, 0 80))")
                .expect("polar cap"),
        ];
        let analysis = build_analysis(&cap);
        let assignment = labels(&analysis, &cap, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("polar cap row plan");
        assert!(
            plans
                .rows
                .first()
                .and_then(|row| row.regions.first())
                .is_some_and(|region| region.poles.includes(crate::geographic::ChartPole::North)),
            "the selected region records its north-pole sector",
        );
        assert!(plans.rows.first().is_some_and(|row| row.north_pole));

        let full_longitude_band = [crate::io::parse_wkt(
            "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, \
             180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
        )
        .expect("full-longitude band")];
        let analysis = build_analysis(&full_longitude_band);
        let assignment = labels(&analysis, &full_longitude_band, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("full-longitude row plan");
        assert!(
            plans
                .rows
                .iter()
                .flat_map(|row| &row.regions)
                .flat_map(|region| &region.cycles)
                .flat_map(|cycle| &cycle.uses)
                .any(|use_| { use_.from_presentation.seam_cut || use_.to_presentation.seam_cut }),
            "directed uses retain the cut-port presentation roles",
        );

        let whole_sphere = [crate::io::parse_wkt(
            "POLYGON ((-180 -90, -90 -90, 0 -90, 90 -90, 180 -90, \
             180 90, 90 90, 0 90, -90 90, -180 90, -180 0, -180 -90))",
        )
        .expect("whole-sphere source ring")];
        let analysis = build_analysis(&whole_sphere);
        let assignment = labels(&analysis, &whole_sphere, 0.0);
        let plans = analysis
            .cleaning_boundary_plans(&assignment, &mut Unmetered)
            .expect("whole-sphere row plan");
        let row = plans.rows.first().expect("one whole-sphere row");
        assert!(row.whole_sphere);
        assert!(row.north_pole && row.south_pole);
        assert_eq!(row.source_polygons, [0]);
        assert!(row.regions.is_empty());
        assert!(plans.live_spans.is_empty());
    }
}
