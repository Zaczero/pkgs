use std::cmp::Ordering;

use rstar::primitives::{GeomWithData, Rectangle};
use rstar::{AABB, Envelope as _};

use crate::collections::{try_hash_map, try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::geometry::coverage::{HashMap, Result};
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::publication::{
    FinishedGeometry, ProducerReceipt, PublicationCall, PublicationOutcome, PublicationRejection,
    StorageTarget, finish_geographic,
};
use crate::geometry::storage::ValidityMask;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    Bounds, BulkRTree, Contact, CoordinateAxes, ExactPoint, GeometryErrorKind, GeometryKind,
    LineSeq, PointKey, Polygon, PreparedPlan, Segment, Shape, canonical_f64_bits, line_segments,
    point_segment_distance_squared_exact, same_point, segment_contact_exact,
    undirected_segment_edge_key,
};

/// One row's boundary segments plus its envelope and lazily prepared locator.
///
/// Coverage cleaning sends admitted source segments to `ExactPartition`; it
/// must not build a second per-row segment graph before that owner is reached.
pub(crate) struct CoverageRow<'a> {
    pub(crate) shape: &'a Shape,
    plan: std::sync::OnceLock<PreparedPlan>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) bounds: Option<Bounds>,
}

type CoverageRowIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;

pub(crate) struct PreparedCoverage<'a> {
    pub(crate) rows: Vec<CoverageRow<'a>>,
    index: Option<Box<CoverageRowIndex>>,
}

// Bulk loading costs more instructions than scanning through 64 rows. Keep
// that bounded scan without making large coverages pay for row-wide queries.
const ROW_INDEX_MIN_ROWS: usize = 65;

#[cfg(test)]
thread_local! {
    static ROW_CANDIDATE_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Collect each row's boundary segments (every ring of every polygon part).
/// Non-polygonal rows are a caller (binding) error; empties contribute none.
pub(crate) fn coverage_rows<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<PreparedCoverage<'_>> {
    validate_gap_width(gap_width)?;
    let mut prepared = Vec::with_capacity(rows.len());
    for row in rows {
        let shape = row.borrow();
        let mut segments = Vec::new();
        match shape {
            Shape::Polygon(polygon) => collect_polygon_segments(polygon, &mut segments),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    collect_polygon_segments(polygon, &mut segments);
                }
            },
            Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {},
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::GeometryCollection,
                _,
            ) => return Err(GeometryErrorKind::CoveragePolygonalRequired.into()),
        }
        prepared.push(CoverageRow {
            shape,
            plan: std::sync::OnceLock::new(),
            segments,
            bounds: shape.bounds(),
        });
    }
    let index = (prepared.len() >= ROW_INDEX_MIN_ROWS).then(|| {
        let entries = prepared
            .iter()
            .enumerate()
            .filter_map(|(index, row)| {
                row.bounds.map(|bounds| {
                    GeomWithData::new(
                        Rectangle::from_corners([bounds.minx(), bounds.miny()], [
                            bounds.maxx(),
                            bounds.maxy(),
                        ]),
                        index,
                    )
                })
            })
            .collect();
        Box::new(BulkRTree::bulk_load_with_params(entries))
    });
    Ok(PreparedCoverage {
        rows: prepared,
        index,
    })
}

fn validate_gap_width(gap_width: f64) -> Result<()> {
    if !(gap_width.is_finite() && gap_width >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
    }
    Ok(())
}

pub(crate) fn validate_polygonal_rows<S: std::borrow::Borrow<Shape>>(rows: &[S]) -> Result<()> {
    for row in rows {
        match row.borrow() {
            Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {},
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::GeometryCollection,
                _,
            ) => return Err(GeometryErrorKind::CoveragePolygonalRequired.into()),
        }
    }
    Ok(())
}

fn collect_polygon_segments(polygon: &Polygon, out: &mut Vec<Segment>) {
    out.reserve(
        polygon
            .rings()
            .map(|ring| ring.len().saturating_sub(1))
            .sum(),
    );
    for ring in polygon.rings() {
        // Elide zero-length stutter (repeated consecutive vertices): a
        // degenerate edge is removable redundancy, not a coverage boundary, so
        // it must not register as a coverage edge or it would surface as a
        // false invalid edge. Consistent with the validity model.
        out.extend(line_segments(&ring).filter(|segment| !same_point(segment.start, segment.end)));
    }
}

/// How many distinct rows carry each undirected segment (at most once per
/// row, even when a ring lists the same edge more than once).
pub(crate) fn edge_row_occurrences(
    rows: &[CoverageRow<'_>],
) -> Result<HashMap<(PointKey, PointKey), usize>> {
    let segment_capacity = rows.iter().map(|row| row.segments.len()).sum();
    let mut map = try_hash_map_with_capacity(segment_capacity)?;
    let mut seen = try_hash_set_with_capacity(rows.first().map_or(0, |row| row.segments.len()))?;
    for row in rows {
        seen.clear();
        seen.reserve(row.segments.len());
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            if seen.insert(key) {
                let count = map.entry(key).or_insert(0_usize);
                *count = count.wrapping_add(1);
            }
        }
    }
    Ok(map)
}

/// Successful validation product for check-and-do ops: prepared rows plus the
/// raw edge incidence map dissolve consumes (occurrence==1 = exterior).
pub(crate) struct ValidCoverage<'a> {
    pub(crate) rows: Vec<CoverageRow<'a>>,
    pub(crate) edge_incidences: HashMap<(PointKey, PointKey), (Segment, usize)>,
}

/// Per-row invalid boundary segments (empty everywhere on a valid coverage).
///
/// A row's segment is invalid when it is not exactly matched by another
/// row's segment and it still interacts with another row: its midpoint lies
/// in (or on the boundary of) that row, it properly crosses that row's
/// boundary, or — with `gap_width > 0` — it faces another row's unmatched
/// boundary across a gap narrower than `gap_width`.
pub(crate) fn coverage_invalid_segments<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Vec<Segment>>> {
    let rows = coverage_rows(rows, gap_width)?;
    let mut invalid = vec![Vec::new(); rows.rows.len()];
    visit_invalid_segments(&rows, gap_width, |index, segment| {
        invalid
            .get_mut(index)
            .expect("coverage row index is emitted from its row slice")
            .push(segment);
        true
    })?;
    Ok(invalid)
}

/// Validate already-prepared coverage rows, allowing check-and-do operations
/// to reuse the same extracted boundary graph for their kernel. The visitor
/// returns whether to continue; the result reports whether any segment is invalid.
pub(crate) fn visit_invalid_segments(
    rows: &PreparedCoverage<'_>,
    gap_width: f64,
    report_invalid: impl FnMut(usize, Segment) -> bool,
) -> Result<bool> {
    let occurrences = edge_row_occurrences(&rows.rows)?;
    coverage_invalid_with_count(
        rows,
        |key| {
            *occurrences
                .get(&key)
                .expect("every row segment was counted before validation")
        },
        gap_width,
        report_invalid,
    )
}

/// Core validator given a count lookup: matched interface when count==2.
///
/// `report_invalid` receives each offending segment in diagnostic order and
/// returns whether validation should continue. The result reports whether any
/// offending segment was found.
fn coverage_invalid_with_count(
    prepared: &PreparedCoverage<'_>,
    count_of: impl FnMut((PointKey, PointKey)) -> usize,
    gap_width: f64,
    report_invalid: impl FnMut(usize, Segment) -> bool,
) -> Result<bool> {
    let rows = &prepared.rows;
    if let Some(index) = &prepared.index {
        coverage_invalid_with_candidates(rows, count_of, gap_width, report_invalid, |query| {
            index.locate_in_envelope_intersecting(query).map(|entry| {
                (
                    entry.data,
                    rows.get(entry.data)
                        .expect("coverage index entries retain their source row"),
                )
            })
        })
    } else {
        coverage_invalid_with_candidates(rows, count_of, gap_width, report_invalid, |_| {
            rows.iter().enumerate()
        })
    }
}

fn coverage_invalid_with_candidates<'a, 'b, I>(
    rows: &'a [CoverageRow<'b>],
    mut count_of: impl FnMut((PointKey, PointKey)) -> usize,
    gap_width: f64,
    mut report_invalid: impl FnMut(usize, Segment) -> bool,
    mut candidates: impl FnMut(AABB<[f64; 2]>) -> I,
) -> Result<bool>
where
    I: Iterator<Item = (usize, &'a CoverageRow<'b>)>,
{
    // Two coincident polygon rows are an overlap, not a valid shared
    // interface. Their edges occur exactly twice, which otherwise looks just
    // like two adjacent polygons and would take the fast shared-edge path
    // below. Coincident pairs always share exact bounds, so group by bit-exact
    // bounds instead of an all-row geometry comparison — unique-bounds
    // coverages (parcels) pay one HashMap insert each and never call `equals`.
    let mut coincident = vec![false; rows.len()];
    // Group by canonical bounds bits so +0.0/−0.0 land in one bucket
    // (`f64::to_bits` alone splits them and misses coincident duplicates —
    // edges then all look like shared interfaces and coverage_union_all empties).
    let mut by_bounds = try_hash_map_with_capacity(rows.len())?;
    let mut duplicate_bounds = try_hash_map()?;
    for (index, row) in rows.iter().enumerate() {
        let Some(bounds) = row.bounds else { continue };
        let key = [
            canonical_f64_bits(bounds.minx()),
            canonical_f64_bits(bounds.miny()),
            canonical_f64_bits(bounds.maxx()),
            canonical_f64_bits(bounds.maxy()),
        ];
        match by_bounds.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(index);
            },
            std::collections::hash_map::Entry::Occupied(entry) => {
                let existing = *entry.get();
                match duplicate_bounds.entry(key) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(vec![existing, index]);
                    },
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(index);
                    },
                }
            },
        }
    }
    drop(by_bounds);
    // Within a bounds bucket, fingerprint each row by its sorted undirected
    // edge multiset (PointKey already canonicalizes ±0). Identical fingerprints
    // are coincident without O(g²) `equals` scans.
    for group in duplicate_bounds.into_values() {
        let mut by_edges = try_hash_map_with_capacity(group.len())?;
        for index in group {
            let mut edges: Vec<_> = rows
                .get(index)
                .expect("duplicate-bounds row index is within the coverage")
                .segments
                .iter()
                .copied()
                .map(undirected_segment_edge_key)
                .collect();
            edges.sort_unstable();
            match by_edges.entry(edges) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(index);
                },
                std::collections::hash_map::Entry::Occupied(entry) => {
                    *coincident
                        .get_mut(*entry.get())
                        .expect("duplicate edge row index is within the coverage") = true;
                    *coincident
                        .get_mut(index)
                        .expect("duplicate-bounds row index is within the coverage") = true;
                },
            }
        }
    }
    let mut any_invalid = false;
    for (index, row) in rows.iter().enumerate() {
        if *coincident
            .get(index)
            .expect("coverage row index is within the coincident flags")
        {
            for &segment in &row.segments {
                any_invalid = true;
                if !report_invalid(index, segment) {
                    return Ok(true);
                }
            }
            continue;
        }
        for &segment in &row.segments {
            if count_of(undirected_segment_edge_key(segment)) == 2 {
                continue; // exactly shared interface — the valid case
            }
            let query_min_x = segment.start.x.min(segment.end.x) - gap_width;
            let query_min_y = segment.start.y.min(segment.end.y) - gap_width;
            let query_max_x = segment.start.x.max(segment.end.x) + gap_width;
            let query_max_y = segment.start.y.max(segment.end.y) + gap_width;
            let mut offending = false;
            let query = AABB::from_bounds([query_min_x, query_min_y], [query_max_x, query_max_y]);
            for (candidate_row, candidate) in candidates(query) {
                #[cfg(test)]
                ROW_CANDIDATE_WORK.with(|work| work.set(work.get().saturating_add(1)));
                let Some(bounds) = candidate.bounds else {
                    continue;
                };
                if candidate_row != index
                    && bounds.maxx() >= query_min_x
                    && bounds.minx() <= query_max_x
                    && bounds.maxy() >= query_min_y
                    && bounds.miny() <= query_max_y
                    && segment_offends_row(segment, candidate, gap_width)?
                {
                    offending = true;
                    break;
                }
            }
            if offending {
                any_invalid = true;
                if !report_invalid(index, segment) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(any_invalid)
}

/// Prepare and require a valid coverage for a public check-and-do operation.
///
/// On success the edge incidence map is retained so dissolve can consume the
/// same prepared boundary product without rewalking polygons.
pub(crate) fn valid_coverage_rows<'a, S: std::borrow::Borrow<Shape>>(
    rows: &'a [S],
    operation: &'static str,
) -> Result<ValidCoverage<'a>> {
    let prepared = coverage_rows(rows, 0.0)?;
    // One raw incidence map serves both roles on the check-and-do path:
    // count==2 is the matched interface (same as distinct-row count on any
    // valid simple coverage; a within-row duplicate also lands at 2 and is a
    // safe skip — the edge cancels in dissolve too), and count==1 survivors
    // feed XOR dissolve. The is_valid path keeps the lighter distinct-row
    // map via `edge_row_occurrences` (no dissolve product needed).
    let segment_capacity = prepared.rows.iter().map(|row| row.segments.len()).sum();
    let mut incidences: HashMap<(PointKey, PointKey), (Segment, usize)> =
        try_hash_map_with_capacity(segment_capacity)?;
    for row in &prepared.rows {
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            incidences
                .entry(key)
                .and_modify(|entry| entry.1 = entry.1.wrapping_add(1))
                .or_insert((segment, 1));
        }
    }
    if coverage_invalid_with_count(
        &prepared,
        |key| {
            incidences
                .get(&key)
                .expect("every prepared segment has a retained incidence")
                .1
        },
        0.0,
        |_, _| false,
    )? {
        return Err(GeometryErrorKind::InvalidCoverage { operation }.into());
    }
    Ok(ValidCoverage {
        rows: prepared.rows,
        edge_incidences: incidences,
    })
}

/// Whether an unmatched boundary segment interacts with `other`'s area or
/// boundary in a way a valid coverage forbids.
pub(crate) fn segment_offends_row(
    segment: Segment,
    other: &CoverageRow<'_>,
    gap_width: f64,
) -> Result<bool> {
    let midpoint = exact_segment_midpoint(segment)?;
    // Overlap or inexact contact: the midpoint of a matched interface would
    // coincide with the neighbor's own segment — an unmatched one touching
    // (or entering) the neighbor is a T-join, sliver, or true overlap.
    if PreparedPlan::locate_exact_lazy(&other.plan, other.shape, other.bounds, &midpoint)?.location
        != crate::geometry::Loc::Exterior
    {
        return Ok(true);
    }
    // Proper crossing without containment (the bowtie-style overlap whose
    // midpoint happens to fall outside). The contact must be interior to
    // THIS segment: a shared corner vertex is legal, and a neighbor vertex
    // landing mid-segment (the T-join) is each side's own interior contact,
    // so both rows flag symmetrically.
    let envelope = AABB::from_corners([segment.start.x, segment.start.y], [
        segment.end.x,
        segment.end.y,
    ]);
    let crossing = other.segments.iter().any(|&other_seg| {
        if !envelope.intersects(&AABB::from_corners(
            [other_seg.start.x, other_seg.start.y],
            [other_seg.end.x, other_seg.end.y],
        )) {
            return false;
        }
        match segment_contact_exact(segment, other_seg) {
            // A proper crossing is interior to both segments.
            Contact::Cross => true,
            Contact::Touch(endpoint) => {
                let point = endpoint.point(segment, other_seg);
                !same_point(point, segment.start) && !same_point(point, segment.end)
            },
            Contact::Disjoint | Contact::Collinear => false,
        }
    });
    if crossing {
        return Ok(true);
    }
    // Narrow gap: the segment faces another row's unmatched boundary closer
    // than gap_width without touching it.
    if gap_width > 0.0 {
        let midpoint_point = ExactPoint::from_homogeneous_ref(&midpoint)?;
        let gap_squared = ExactRational::from_f64(gap_width).try_square()?;
        let near = |point: &ExactPoint| -> Result<bool> {
            for &candidate in &other.segments {
                let distance_squared = point_segment_distance_squared_exact(point, candidate)?;
                if !distance_squared.is_zero()
                    && distance_squared.try_cmp(&gap_squared)? == Ordering::Less
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        if near(&midpoint_point)?
            || near(&ExactPoint::from_xy(segment.start))?
            || near(&ExactPoint::from_xy(segment.end))?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exact_segment_midpoint(segment: Segment) -> Result<[ExactDyadic; 3]> {
    Ok([
        ExactDyadic::from_f64(segment.start.x).try_add(ExactDyadic::from_f64(segment.end.x))?,
        ExactDyadic::from_f64(segment.start.y).try_add(ExactDyadic::from_f64(segment.end.y))?,
        ExactDyadic::from_f64(2.0),
    ])
}

/// Whether the rows form a valid polygonal coverage under `gap_width`.
pub(crate) fn coverage_is_valid<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<bool> {
    let rows = coverage_rows(rows, gap_width)?;
    Ok(!visit_invalid_segments(&rows, gap_width, |_, _| false)?)
}

pub(crate) fn coverage_is_valid_with_domain<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
    domain: crate::geometry::TopologyDomain,
) -> Result<bool> {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => coverage_is_valid(rows, gap_width),
        crate::geometry::TopologyDomain::Geographic(frame) => {
            validate_gap_width(gap_width)?;
            validate_polygonal_rows(rows)?;
            let mut budget = EvaluationBudget::new("coverage_is_valid", 0)?;
            Ok(crate::geometry::analyze_geographic_coverage_with_work(
                rows,
                frame,
                gap_width,
                "coverage_is_valid",
                &mut budget,
            )?
            .is_valid())
        },
    }
}

/// Per-row invalid boundary linework: the row's offending segments merged
/// into lines (`LINESTRING EMPTY` where the row is clean).
#[cfg(test)]
pub(crate) fn coverage_invalid_edges<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Shape>> {
    let invalid = coverage_invalid_segments(rows, gap_width)?;
    coverage_invalid_edge_shapes(invalid)
}

pub(crate) fn coverage_invalid_edges_cartesian<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Shape>> {
    coverage_invalid_edge_shapes(coverage_invalid_segments(rows, gap_width)?)
}

#[cfg(test)]
pub(crate) fn coverage_invalid_edges_geographic_analysis<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    frame: crate::geographic::GeographicFrame,
    gap_width: f64,
    operation: &'static str,
) -> Result<(crate::geometry::GeographicCoverageAnalysis, Vec<Shape>)> {
    let mut budget = EvaluationBudget::new(operation, 0)?;
    let analysis =
        analyze_geographic_invalid_edges(rows, frame, gap_width, operation, &mut budget)?;
    let shapes = analysis.invalid_source_shapes(&mut budget)?;
    Ok((analysis, shapes))
}

pub(crate) fn coverage_invalid_edges_geographic_finished<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    frame: crate::geographic::GeographicFrame,
    gap_width: f64,
    operation: &'static str,
    output_frame: crate::Frame,
    missing: Option<ValidityMask>,
) -> Result<FinishedGeometry> {
    let mut budget = EvaluationBudget::new(operation, 0)?;
    let analysis =
        analyze_geographic_invalid_edges(rows, frame, gap_width, operation, &mut budget)?;
    let receipt = analysis.invalid_edges_drawing_receipt(missing, &mut budget)?;
    match finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            output_frame,
            &mut budget,
        ),
        StorageTarget::Array,
    )? {
        PublicationOutcome::Pass(finished) => Ok(finished),
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            Err(GeometryErrorKind::Invalid(
                format!(
                    "final coverage invalid-edge storage changed {} checked output records",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "coverage invalid-edge finisher returned a different producer rejection family".into(),
        )
        .into()),
    }
}

fn analyze_geographic_invalid_edges<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    frame: crate::geographic::GeographicFrame,
    gap_width: f64,
    operation: &'static str,
    work: &mut impl crate::geometry::Work,
) -> Result<crate::geometry::GeographicCoverageAnalysis> {
    validate_gap_width(gap_width)?;
    validate_polygonal_rows(rows)?;
    let mut analysis = crate::geometry::analyze_geographic_coverage_with_work(
        rows, frame, gap_width, operation, work,
    )?;
    analysis.capture_invalid_source_owners(work)?;
    Ok(analysis)
}

fn coverage_invalid_edge_shapes(invalid: Vec<Vec<Segment>>) -> Result<Vec<Shape>> {
    let mut output = Vec::with_capacity(invalid.len());
    for segments in invalid {
        if segments.is_empty() {
            output.push(Shape::typed_empty(
                GeometryKind::LineString,
                CoordinateAxes::XY,
            ));
            continue;
        }
        let mut lines = Vec::with_capacity(segments.len());
        for segment in segments {
            lines.push(LineSeq::segment(
                segment.start.point(),
                segment.end.point(),
            )?);
        }
        output.push(Shape::multi_line_string(CoordinateAxes::XY, lines).line_merge()?);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use pyo3::prelude::*;
    use pyo3::types::PyList;

    use super::{ROW_CANDIDATE_WORK, coverage_rows, exact_segment_midpoint, segment_offends_row};
    use crate::geometry::tessellation::exact::ExactRational;
    use crate::geometry::{
        CoordSeq, Point, Polygon, Ring, Segment, Shape, XY, coverage_invalid_edges,
        coverage_is_valid,
    };

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        let points = [
            Point::new_raw_xy(minx, miny),
            Point::new_raw_xy(maxx, miny),
            Point::new_raw_xy(maxx, maxy),
            Point::new_raw_xy(minx, maxy),
            Point::new_raw_xy(minx, miny),
        ];
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn one_ulp_segment_midpoint_does_not_alias_a_neighbor_corner() {
        let lower: f64 = -0.45;
        let upper = lower.next_up();
        assert_eq!(f64::midpoint(lower, upper), upper);

        let neighbor = [rectangle(1.0, upper, 2.0, 1.0)];
        let prepared = coverage_rows(&neighbor, 0.0).unwrap();
        let edge = Segment {
            start: XY::new(1.0, lower),
            end: XY::new(1.0, upper),
        };

        assert!(
            prepared.rows[0]
                .shape
                .covers_point(Point::new_raw_xy(1.0, f64::midpoint(lower, upper)))
        );
        assert!(!segment_offends_row(edge, &prepared.rows[0], 0.0).unwrap());
    }

    #[test]
    fn exact_midpoint_gap_contact_survives_rounding_down() {
        let step = f64::from_bits(1);
        let neighbor = [rectangle(step, 0.0, 1.0, 1.0)];
        let prepared = coverage_rows(&neighbor, step).unwrap();
        let edge = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(step, 0.0),
        };
        assert_eq!(f64::midpoint(0.0, step), 0.0);
        assert!(segment_offends_row(edge, &prepared.rows[0], step).unwrap());
    }

    #[test]
    fn exact_endpoint_gap_contact_survives_distance_rounding_up() {
        let x = 1.0_f64.next_down();
        let y = 2.0_f64.powi(-26).next_down();
        let neighbor = [rectangle(x, y, x + 0.125, 2.0 * y)];
        let prepared = coverage_rows(&neighbor, 1.0).unwrap();
        let edge = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(-10.0, 0.0),
        };
        let nearest_boundary = Segment {
            start: XY::new(x, y),
            end: XY::new(x + 0.125, y),
        };
        let exact_gap_squared = ExactRational::from_f64(x)
            .try_square()
            .unwrap()
            .try_add(&ExactRational::from_f64(y).try_square().unwrap())
            .unwrap();
        assert_eq!(
            exact_gap_squared
                .try_cmp(&ExactRational::from_f64(1.0).try_square().unwrap())
                .unwrap(),
            Ordering::Less
        );

        assert_eq!(
            crate::geometry::point_segment_distance(edge.start, nearest_boundary).unwrap(),
            1.0
        );
        assert!(segment_offends_row(edge, &prepared.rows[0], 1.0).unwrap());
    }

    #[test]
    fn exact_segment_midpoint_handles_subnormal_and_maximum_coordinates() {
        let cases = [(0.0, f64::from_bits(1)), (f64::MAX.next_down(), f64::MAX)];
        for (lower, upper) in cases {
            let midpoint = exact_segment_midpoint(Segment {
                start: XY::new(lower, 0.0),
                end: XY::new(upper, 0.0),
            })
            .unwrap();
            let x = ExactRational::new(midpoint[0].clone(), midpoint[2].clone()).unwrap();
            assert_eq!(
                x.try_cmp(&ExactRational::from_f64(lower)).unwrap(),
                Ordering::Greater
            );
            assert_eq!(
                x.try_cmp(&ExactRational::from_f64(upper)).unwrap(),
                Ordering::Less
            );
        }
    }

    #[test]
    fn coverage_midpoint_controls_preserve_overlap_and_contact_rules() {
        let t_join = [
            crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))").unwrap(),
            crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))").unwrap(),
            crate::io::parse_wkt("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))").unwrap(),
        ];
        let duplicate = [rectangle(0.0, 0.0, 1.0, 1.0), rectangle(0.0, 0.0, 1.0, 1.0)];
        let one_ulp_overlap = [
            rectangle(0.0, 0.0, 1.0, 1.0),
            rectangle(1.0_f64.next_down(), 0.0, 2.0, 1.0),
        ];

        for rows in [&t_join[..], &duplicate, &one_ulp_overlap] {
            assert!(!coverage_is_valid(rows, 0.0).unwrap());
            assert!(
                coverage_invalid_edges(rows, 0.0)
                    .unwrap()
                    .iter()
                    .any(|edges| !edges.is_empty())
            );
        }

        let corner_only = [rectangle(0.0, 0.0, 1.0, 1.0), rectangle(1.0, 1.0, 2.0, 2.0)];
        let hole_nested = [
            crate::io::parse_wkt(
                "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))",
            )
            .unwrap(),
            rectangle(3.0, 3.0, 4.0, 4.0),
        ];
        for rows in [&corner_only[..], &hole_nested] {
            assert!(coverage_is_valid(rows, 0.0).unwrap());
            assert!(
                coverage_invalid_edges(rows, 0.0)
                    .unwrap()
                    .iter()
                    .all(Shape::is_empty)
            );
        }
    }

    #[test]
    fn public_coverage_operations_visit_only_local_rows() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let make_box = module.getattr("box").unwrap();
            for count in [128_usize, 512] {
                let rows = PyList::empty(py);
                for index in 0..count {
                    rows.append(
                        make_box
                            .call1((index, 0, index.wrapping_add(1), 1))
                            .unwrap(),
                    )
                    .unwrap();
                }
                for operation in [
                    "coverage_is_valid",
                    "coverage_invalid_edges",
                    "coverage_simplify",
                    "coverage_clean",
                    "coverage_union_all",
                ] {
                    ROW_CANDIDATE_WORK.with(|work| work.set(0));
                    let function = module.getattr(operation).unwrap();
                    let output = if operation == "coverage_simplify" {
                        function.call1((&rows, 0.01)).unwrap()
                    } else {
                        function.call1((&rows,)).unwrap()
                    };
                    if operation == "coverage_is_valid" {
                        assert!(output.extract::<bool>().unwrap());
                    }
                    let work = ROW_CANDIDATE_WORK.with(std::cell::Cell::get);
                    assert!(work > 0, "{operation} did not reach coverage validation");
                    assert!(
                        work < count.saturating_mul(32),
                        "{operation} visited {work} candidate rows for {count} adjacent squares"
                    );
                }
            }
        });
    }
}
