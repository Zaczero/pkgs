use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use super::super::exact::SourceEdgeId;
use super::super::q::{ExactGeographicTopology, QSpanOwner};
use crate::array::StreamingShapes;
use crate::error::Result;
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerSourceRootUse,
};
use crate::geometry::storage::{StorageLayout, ValidityMask};
use crate::geometry::{
    CoordinateAxes, ExactRational, GeometryErrorKind, GeometryKind, LineSeq, PointKey, Segment,
    Shape, Work, XY,
};

type OwnersBySource<'a> = BTreeMap<SourceEdgeId, Vec<&'a QSpanOwner>>;
type SegmentOwners = BTreeMap<(usize, PointKey, PointKey), VecDeque<SourceEdgeId>>;

fn undirected_point_pair(start: crate::Point, end: crate::Point) -> (PointKey, PointKey) {
    let start = PointKey::new(start);
    let end = PointKey::new(end);
    if start <= end {
        (start, end)
    } else {
        (end, start)
    }
}

fn draw_line_sequence<W: Work>(
    line: &LineSeq,
    present_row: usize,
    logical_row: usize,
    output: u32,
    topology: &ExactGeographicTopology,
    owners_by_source: &OwnersBySource<'_>,
    segment_owners: &mut SegmentOwners,
    literals: &mut Vec<ProducerLiteral>,
    source_root_uses: &mut Vec<ProducerSourceRootUse>,
    work: &mut W,
) -> Result<LiteralSequence<ProducerImageId>> {
    work.charge_into_error(
        u64::try_from(line.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let points = line.iter().collect::<Vec<_>>();
    let mut images = Vec::with_capacity(points.len());
    for &point in &points {
        let image = ProducerImageId::new(literals.len());
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        literals.push(ProducerLiteral::from_point(address, image, point));
        images.push(image);
    }

    for (segment_index, pair) in points.windows(2).enumerate() {
        work.charge_into_error(1)?;
        let [start, end] = pair else {
            return Err(GeometryErrorKind::Invalid(
                "coverage invalid-edge line lost a source segment".into(),
            )
            .into());
        };
        let (first, second) = undirected_point_pair(*start, *end);
        let key = (present_row, first, second);
        let source_edge = segment_owners
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage invalid-edge output segment has no captured source edge".into(),
                )
            })?;
        let edge = topology
            .sources
            .edges
            .get(source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage invalid-edge source use is outside its inventory".into(),
                )
            })?;
        let source_start = PointKey::new(edge.source[0]);
        let source_end = PointKey::new(edge.source[1]);
        let output_start = PointKey::new(*start);
        let output_end = PointKey::new(*end);
        let reversed = if source_start == output_start && source_end == output_end {
            false
        } else if source_start == output_end && source_end == output_start {
            true
        } else {
            return Err(GeometryErrorKind::Invalid(
                "coverage invalid-edge merge changed a source segment".into(),
            )
            .into());
        };
        let image_index = if reversed {
            segment_index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        } else {
            segment_index
        };
        let image = *images.get(image_index).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage invalid-edge source use has no output literal".into(),
            )
        })?;
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output,
        };
        let owners = owners_by_source.get(&source_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "coverage invalid-edge segment has no captured Q source owner".into(),
            )
        })?;
        for owner in owners {
            work.charge_into_error(1)?;
            let producer_start = if reversed {
                ExactRational::one().try_subtract_with_work(&owner.source_range.start, work)?
            } else {
                owner.source_range.start.try_clone_with_work(work)?
            };
            let producer_end = if reversed {
                ExactRational::one().try_subtract_with_work(&owner.source_range.end, work)?
            } else {
                owner.source_range.end.try_clone_with_work(work)?
            };
            let range = RootRange::from_mapped_with_work(
                RootId::from_index(source_edge.0),
                owner.source_range.start.try_clone_with_work(work)?,
                owner.source_range.end.try_clone_with_work(work)?,
                producer_start,
                producer_end,
                work,
            )?;
            source_root_uses.push(ProducerSourceRootUse {
                address,
                image,
                range,
            });
        }
    }

    Ok(LiteralSequence {
        output,
        axes: line.as_coords().axes(),
        images,
    })
}

pub(super) fn producer_drawing_receipt(
    analysis: &super::GeographicCoverageAnalysis,
    missing: Option<ValidityMask>,
    work: &mut impl Work,
) -> Result<ProducerDrawingReceipt> {
    let shapes = project_shapes(
        &analysis.topology,
        &analysis.invalid_source_edge_flags,
        &analysis.invalid_source_owners,
        work,
    )?;
    if missing
        .as_ref()
        .is_some_and(|mask| mask.present_count() != shapes.len())
    {
        return Err(GeometryErrorKind::Invalid(
            "coverage invalid-edge source validity does not match its present rows".into(),
        )
        .into());
    }

    let mut logical_rows = Vec::with_capacity(shapes.len());
    match missing.as_ref() {
        Some(mask) => {
            for logical_row in 0..mask.len() {
                work.charge_into_error(1)?;
                if !mask.is_missing(logical_row) {
                    logical_rows.push(logical_row);
                }
            }
        },
        None => logical_rows.extend(0..shapes.len()),
    }
    if logical_rows.len() != shapes.len() {
        return Err(GeometryErrorKind::Invalid(
            "coverage invalid-edge logical rows do not match their source inventory".into(),
        )
        .into());
    }

    let mut owners_by_source: OwnersBySource<'_> = BTreeMap::new();
    for owner in &analysis.invalid_source_owners {
        work.charge_into_error(1)?;
        owners_by_source
            .entry(owner.source_edge)
            .or_default()
            .push(owner);
    }
    // Projection appends source edges in ID order, and these queues retain that
    // order. The shared line merger consumes input ordinals once, seeds open paths
    // and cycles by the first unused edge, and follows the unique continuation at
    // degree-two nodes; duplicate row/XY occurrences therefore leave FIFO, including
    // the two-edge cycle. PointKey is only the occurrence key, never provenance.
    let mut segment_owners = SegmentOwners::new();
    for &source_edge in owners_by_source.keys() {
        work.charge_into_error(1)?;
        let edge = analysis
            .topology
            .sources
            .edges
            .get(source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage invalid-edge source use is outside its inventory".into(),
                )
            })?;
        let (start, end) = undirected_point_pair(edge.source[0], edge.source[1]);
        segment_owners
            .entry((edge.row.0 as usize, start, end))
            .or_default()
            .push_back(source_edge);
    }

    let mut writer = StreamingShapes::with_capacity(0, shapes.len());
    let mut row_map = Vec::with_capacity(shapes.len());
    let mut literals = Vec::new();
    let mut source_root_uses = Vec::new();
    for (present_row, (shape, logical_row)) in shapes.into_iter().zip(logical_rows).enumerate() {
        let mut lines = Vec::new();
        match &shape {
            Shape::LineString(line) => lines.push(draw_line_sequence(
                line,
                present_row,
                logical_row,
                0,
                &analysis.topology,
                &owners_by_source,
                &mut segment_owners,
                &mut literals,
                &mut source_root_uses,
                work,
            )?),
            Shape::MultiLineString(multi_line) => {
                for (line_output, line) in multi_line.as_slice().iter().enumerate() {
                    lines.push(draw_line_sequence(
                        line,
                        present_row,
                        logical_row,
                        u32::try_from(line_output)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        &analysis.topology,
                        &owners_by_source,
                        &mut segment_owners,
                        &mut literals,
                        &mut source_root_uses,
                        work,
                    )?);
                }
            },
            Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {},
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => {
                return Err(GeometryErrorKind::Invalid(
                    "coverage invalid-edge projection produced a non-lineal row".into(),
                )
                .into());
            },
        }
        row_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                points: Vec::new(),
                lines,
                polygons: Vec::new(),
            },
        }));
        writer.try_push(shape)?;
    }
    for unwritten in segment_owners.values() {
        work.charge_into_error(1)?;
        if !unwritten.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "coverage invalid-edge source owner has no written line segment".into(),
            )
            .into());
        }
    }

    let (storage, ..) = writer.finish_parts()?;
    let buffers = FrozenLiteralBuffers::rows(
        Arc::new(storage),
        missing,
        StorageLayout::PresentOnly,
        row_map,
        work,
    )?;
    Ok(ProducerDrawingReceipt::new(
        buffers,
        literals,
        source_root_uses,
        Vec::new(),
    ))
}

pub(super) fn capture(
    topology: &ExactGeographicTopology,
    invalid_edges: &[bool],
    work: &mut impl Work,
) -> Result<Vec<QSpanOwner>> {
    let mut has_invalid_edge = false;
    for &invalid in invalid_edges {
        work.charge_into_error(1)?;
        if invalid {
            has_invalid_edge = true;
            break;
        }
    }
    if !has_invalid_edge {
        return Ok(Vec::new());
    }

    let mut owners = Vec::new();
    for span in &topology.q.spans {
        work.charge_into_error(1)?;
        for owner in &span.owners {
            work.charge_into_error(1)?;
            let invalid = invalid_edges
                .get(owner.source_edge.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage invalid-edge owner is outside the source inventory".into(),
                    )
                })?;
            if invalid {
                owners.push(owner.clone());
            }
        }
    }
    if owners.is_empty() {
        return Err(GeometryErrorKind::Invalid(
            "coverage invalid source edge has no Q source owner".into(),
        )
        .into());
    }
    Ok(owners)
}

pub(super) fn project_shapes(
    topology: &ExactGeographicTopology,
    invalid_source_edge_flags: &[bool],
    owners: &[QSpanOwner],
    work: &mut impl Work,
) -> Result<Vec<Shape>> {
    if invalid_source_edge_flags.len() != topology.sources.edges.len() {
        return Err(GeometryErrorKind::Invalid(
            "coverage invalid-edge flags are out of sync with source edges".into(),
        )
        .into());
    }
    if owners.is_empty() {
        for &invalid in invalid_source_edge_flags {
            work.charge_into_error(1)?;
            if invalid {
                return Err(GeometryErrorKind::Invalid(
                    "coverage invalid-edge source ownership is incomplete".into(),
                )
                .into());
            }
        }
        return std::iter::repeat_with(|| {
            Ok(Shape::typed_empty(
                GeometryKind::LineString,
                CoordinateAxes::XY,
            ))
        })
        .take(topology.sources.row_count)
        .collect();
    }

    let mut selected = vec![false; topology.sources.edges.len()];
    for owner in owners {
        work.charge_into_error(1)?;
        let selected = selected
            .get_mut(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "coverage invalid-edge projection has an unknown source edge".into(),
                )
            })?;
        *selected = true;
    }

    let mut output = vec![Vec::new(); topology.sources.row_count];
    for (edge_index, (is_selected, is_invalid)) in selected
        .into_iter()
        .zip(invalid_source_edge_flags.iter().copied())
        .enumerate()
    {
        work.charge_into_error(1)?;
        if is_selected != is_invalid {
            return Err(GeometryErrorKind::Invalid(
                "coverage invalid-edge source ownership is incomplete".into(),
            )
            .into());
        }
        if !is_selected {
            continue;
        }
        let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage invalid source edge is out of range".into())
        })?;
        output
            .get_mut(edge.row.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage invalid source row is out of range".into())
            })?
            .push(Segment {
                start: XY::new(edge.source[0].x, edge.source[0].y),
                end: XY::new(edge.source[1].x, edge.source[1].y),
            });
    }

    let mut shapes = Vec::with_capacity(output.len());
    for segments in output {
        if segments.is_empty() {
            shapes.push(Shape::typed_empty(
                GeometryKind::LineString,
                CoordinateAxes::XY,
            ));
            continue;
        }
        let lines = segments
            .into_iter()
            .map(|segment| LineSeq::segment(segment.start.point(), segment.end.point()))
            .collect::<Result<Vec<_>>>()?;
        shapes.push(Shape::multi_line_string(CoordinateAxes::XY, lines).line_merge()?);
    }
    Ok(shapes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::GeographicCoverageAnalysis;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{ExactRational, Shape};

    fn analyze(rows: &[Shape], gap_width: f64) -> GeographicCoverageAnalysis {
        crate::geometry::coverage_invalid_edges_geographic_analysis(
            rows,
            GeographicFrame::crs84(),
            gap_width,
            "coverage_invalid_edges",
        )
        .expect("coverage analysis succeeds")
        .0
    }

    fn selected_source_edges(analysis: &GeographicCoverageAnalysis) -> BTreeSet<(usize, [u64; 4])> {
        analysis
            .invalid_source_owners
            .iter()
            .map(|owner| {
                let edge = analysis
                    .topology
                    .sources
                    .edges
                    .get(owner.source_edge.0 as usize)
                    .expect("captured source edge is in its topology");
                (edge.row.0 as usize, [
                    edge.source[0].x.to_bits(),
                    edge.source[0].y.to_bits(),
                    edge.source[1].x.to_bits(),
                    edge.source[1].y.to_bits(),
                ])
            })
            .collect()
    }

    fn edge(row: usize, start: [f64; 2], end: [f64; 2]) -> (usize, [u64; 4]) {
        (row, [
            start[0].to_bits(),
            start[1].to_bits(),
            end[0].to_bits(),
            end[1].to_bits(),
        ])
    }

    fn assert_full_source_ranges(analysis: &GeographicCoverageAnalysis) {
        let mut ranges = Vec::<(u32, Vec<_>)>::new();
        for owner in &analysis.invalid_source_owners {
            let slot = owner.source_edge.0;
            if let Some((_, ranges)) = ranges.iter_mut().find(|(edge, _)| *edge == slot) {
                ranges.push(owner.source_range.clone());
            } else {
                ranges.push((slot, vec![owner.source_range.clone()]));
            }
        }
        let zero = ExactRational::from_f64(0.0);
        let one = ExactRational::from_f64(1.0);
        for (_, mut edge_ranges) in ranges {
            crate::geometry::try_sort_by(&mut edge_ranges, |left, right| {
                left.start.try_cmp(&right.start)
            })
            .expect("source range order is exact");
            assert_eq!(
                edge_ranges.first().map(|range| &range.start),
                Some(&zero),
                "the captured ranges begin at the original source endpoint",
            );
            assert_eq!(
                edge_ranges.last().map(|range| &range.end),
                Some(&one),
                "the captured ranges end at the original source endpoint",
            );
            for pair in edge_ranges.windows(2) {
                let [left, right] = pair else {
                    unreachable!("two ranges form a pair");
                };
                assert_eq!(
                    left.end, right.start,
                    "captured source ranges cover the edge without a gap",
                );
            }
        }
    }

    #[test]
    fn seam_t_join_retains_original_edge_ranges_and_rows() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((180 0, -170 0, -170 10, 180 10, 180 0))")
                .expect("seam-spanning row"),
            crate::io::parse_wkt("POLYGON ((170 0, 180 0, 180 5, 170 5, 170 0))")
                .expect("lower seam neighbor"),
            crate::io::parse_wkt("POLYGON ((170 5, 180 5, 180 10, 170 10, 170 5))")
                .expect("upper seam neighbor"),
        ];
        let analysis = analyze(&rows, 0.0);

        assert_eq!(
            selected_source_edges(&analysis),
            BTreeSet::from([
                edge(0, [180.0, 10.0], [180.0, 0.0]),
                edge(1, [180.0, 0.0], [180.0, 5.0]),
                edge(1, [180.0, 5.0], [170.0, 5.0]),
                edge(2, [170.0, 5.0], [180.0, 5.0]),
                edge(2, [180.0, 5.0], [180.0, 10.0]),
            ]),
        );
        assert_full_source_ranges(&analysis);
    }

    #[test]
    fn seam_gap_width_retains_both_original_boundary_ranges() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((170 0, 179.995 0, 179.995 1, 170 1, 170 0))")
                .expect("east seam row"),
            crate::io::parse_wkt("POLYGON ((-180 0, -170 0, -170 1, -180 1, -180 0))")
                .expect("west seam row"),
        ];
        let analysis = analyze(&rows, 0.01);

        let selected = selected_source_edges(&analysis);
        assert!(selected.contains(&edge(0, [179.995, 0.0], [179.995, 1.0])));
        assert!(selected.contains(&edge(1, [-180.0, 1.0], [-180.0, 0.0])));
        assert!(!selected.contains(&edge(0, [170.0, 0.0], [170.0, 1.0])));
        assert!(!selected.contains(&edge(1, [-170.0, 0.0], [-170.0, 1.0])));
        assert_full_source_ranges(&analysis);
    }

    #[test]
    fn seam_aliases_keep_distinct_original_edge_and_row_owners() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((170 0, 180 0, 180 10, 170 10, 170 0))")
                .expect("positive seam spelling"),
            crate::io::parse_wkt("POLYGON ((170 0, -180 0, -180 10, 170 10, 170 0))")
                .expect("negative seam spelling"),
        ];
        let analysis = analyze(&rows, 0.0);
        let selected = selected_source_edges(&analysis);

        assert_eq!(
            selected,
            BTreeSet::from([
                edge(0, [170.0, 0.0], [180.0, 0.0]),
                edge(0, [180.0, 0.0], [180.0, 10.0]),
                edge(0, [180.0, 10.0], [170.0, 10.0]),
                edge(0, [170.0, 10.0], [170.0, 0.0]),
                edge(1, [170.0, 0.0], [-180.0, 0.0]),
                edge(1, [-180.0, 0.0], [-180.0, 10.0]),
                edge(1, [-180.0, 10.0], [170.0, 10.0]),
                edge(1, [170.0, 10.0], [170.0, 0.0]),
            ]),
        );
        assert_full_source_ranges(&analysis);
    }

    #[test]
    fn captured_edge_id_mutation_changes_the_independent_source_projection() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
                .expect("first overlapping row"),
            crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
                .expect("second overlapping row"),
            crate::io::parse_wkt("POLYGON ((10 0, 11 0, 11 1, 10 1, 10 0))")
                .expect("disjoint clean row"),
        ];
        let mut analysis = analyze(&rows, 0.0);
        let shapes = analysis
            .invalid_source_shapes(&mut crate::geometry::Unmetered)
            .expect("typed source edges project");
        assert!(shapes.get(2).expect("clean row exists").is_empty());
        let clean_edge = analysis
            .topology
            .sources
            .edges
            .iter()
            .find(|edge| edge.row.0 == 2)
            .expect("clean row has source edges")
            .id;
        let corrupted_source_edge = analysis
            .invalid_source_owners
            .first()
            .expect("overlap has a captured source edge")
            .source_edge;
        for owner in analysis
            .invalid_source_owners
            .iter_mut()
            .filter(|owner| owner.source_edge == corrupted_source_edge)
        {
            owner.source_edge = clean_edge;
        }

        assert!(
            analysis
                .invalid_source_shapes(&mut crate::geometry::Unmetered)
                .is_err(),
            "a source-edge crosswire is rejected before row scatter",
        );
    }

    #[test]
    fn omitted_invalid_source_owners_cannot_project_as_clean_rows() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
                .expect("first overlapping row"),
            crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
                .expect("second overlapping row"),
        ];
        let mut analysis = analyze(&rows, 0.0);
        assert!(
            !analysis.invalid_source_owners.is_empty(),
            "the overlap has captured source-edge owners before the omission",
        );
        analysis.invalid_source_owners.clear();

        assert!(
            analysis
                .invalid_source_shapes(&mut crate::geometry::Unmetered)
                .is_err(),
            "missing captured owners must not turn invalid rows into empty output",
        );
    }

    #[test]
    fn captured_owner_rows_preserve_present_row_ordinals() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((10 0, 11 0, 11 1, 10 1, 10 0))")
                .expect("disjoint clean row"),
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
                .expect("first overlapping row"),
            crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
                .expect("second overlapping row"),
        ];
        let analysis = analyze(&rows, 0.0);
        let mut invalid_rows = analysis
            .invalid_source_owners
            .iter()
            .map(|owner| {
                analysis
                    .topology
                    .sources
                    .edges
                    .get(owner.source_edge.0 as usize)
                    .expect("captured edge is in topology")
                    .row
                    .0
            })
            .collect::<Vec<_>>();
        invalid_rows.sort_unstable();
        invalid_rows.dedup();

        assert_eq!(invalid_rows, [1, 2]);
        assert!(
            analysis
                .invalid_source_owners
                .iter()
                .all(|owner| owner.source_edge.0 < analysis.topology.sources.edges.len() as u32),
        );
    }
}
