use crate::collections::{try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::geometry::coverage::{HashMap, Result};
use crate::geometry::{
    Bounds, Contact, CoordinateAxes, GeometryErrorKind, GeometryKind, LineSeq, Point, PointKey,
    Polygon, Segment, Shape, XY, canonical_f64_bits, line_segments, same_point,
    segment_contact_exact, undirected_segment_edge_key,
};

/// One row's boundary segments plus its envelope.
///
/// The validator keeps the source walk as its only row product. Coverage
/// cleaning sends admitted source segments to `ExactPartition`; it must not
/// build a second per-row spatial graph before that owner is reached.
pub(crate) struct CoverageRow<'a> {
    pub(crate) shape: &'a Shape,
    pub(crate) segments: Vec<Segment>,
    pub(crate) bounds: Option<Bounds>,
}

/// Collect each row's boundary segments (every ring of every polygon part).
/// Non-polygonal rows are a caller (binding) error; empties contribute none.
pub(crate) fn coverage_rows<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<CoverageRow<'_>>> {
    if !(gap_width.is_finite() && gap_width >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
    }
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
            _ => return Err(GeometryErrorKind::CoveragePolygonalRequired.into()),
        }
        prepared.push(CoverageRow {
            shape,
            segments,
            bounds: shape.bounds(),
        });
    }
    Ok(prepared)
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
    for row in rows {
        let mut seen = try_hash_set_with_capacity(row.segments.len())?;
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
    let mut invalid = vec![Vec::new(); rows.len()];
    visit_invalid_segments(&rows, gap_width, |index, segment| {
        invalid[index].push(segment);
        true
    })?;
    Ok(invalid)
}

/// Validate already-prepared coverage rows, allowing check-and-do operations
/// to reuse the same extracted boundary graph for their kernel. The visitor
/// returns whether to continue; the result reports whether any segment is invalid.
pub(crate) fn visit_invalid_segments(
    rows: &[CoverageRow<'_>],
    gap_width: f64,
    report_invalid: impl FnMut(usize, Segment) -> bool,
) -> Result<bool> {
    let occurrences = edge_row_occurrences(rows)?;
    coverage_invalid_with_count(rows, |key| occurrences[&key], gap_width, report_invalid)
}

/// Core validator given a count lookup: matched interface when count==2.
///
/// `report_invalid` receives each offending segment in diagnostic order and
/// returns whether validation should continue. The result reports whether any
/// offending segment was found.
fn coverage_invalid_with_count(
    rows: &[CoverageRow<'_>],
    mut count_of: impl FnMut((PointKey, PointKey)) -> usize,
    gap_width: f64,
    mut report_invalid: impl FnMut(usize, Segment) -> bool,
) -> Result<bool> {
    // Two coincident polygon rows are an overlap, not a valid shared
    // interface. Their edges occur exactly twice, which otherwise looks just
    // like two adjacent polygons and would take the fast shared-edge path
    // below. Coincident pairs always share exact bounds, so group by bit-exact
    // bounds instead of an all-row geometry comparison — unique-bounds
    // coverages (parcels) pay one HashMap insert each and never call `equals`.
    let mut coincident = vec![false; rows.len()];
    // Group by canonical bounds bits so +0.0/−0.0 land in one bucket
    // (`f64::to_bits` alone splits them and misses coincident duplicates —
    // edges then all look like shared interfaces and coverage_union empties).
    let mut by_bounds = try_hash_map_with_capacity(rows.len())?;
    let mut duplicate_bounds = try_hash_map_with_capacity(rows.len())?;
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
            let mut edges: Vec<_> = rows[index]
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
                    coincident[*entry.get()] = true;
                    coincident[index] = true;
                },
            }
        }
    }
    let mut any_invalid = false;
    for (index, row) in rows.iter().enumerate() {
        if coincident[index] {
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
            for (candidate_row, candidate) in rows.iter().enumerate() {
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
    let segment_capacity = prepared.iter().map(|row| row.segments.len()).sum();
    let mut incidences: HashMap<(PointKey, PointKey), (Segment, usize)> =
        try_hash_map_with_capacity(segment_capacity)?;
    for row in &prepared {
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            incidences
                .entry(key)
                .and_modify(|entry| entry.1 = entry.1.wrapping_add(1))
                .or_insert((segment, 1));
        }
    }
    if coverage_invalid_with_count(&prepared, |key| incidences[&key].1, 0.0, |_, _| false)? {
        return Err(GeometryErrorKind::InvalidCoverage { operation }.into());
    }
    Ok(ValidCoverage {
        rows: prepared,
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
    let midpoint = Point::new_raw_xy(
        f64::midpoint(segment.start.x, segment.end.x),
        f64::midpoint(segment.start.y, segment.end.y),
    );
    // Overlap or inexact contact: the midpoint of a matched interface would
    // coincide with the neighbor's own segment — an unmatched one touching
    // (or entering) the neighbor is a T-join, sliver, or true overlap.
    if other.shape.covers_point(midpoint)? {
        return Ok(true);
    }
    // Proper crossing without containment (the bowtie-style overlap whose
    // midpoint happens to fall outside). The contact must be interior to
    // THIS segment: a shared corner vertex is legal, and a neighbor vertex
    // landing mid-segment (the T-join) is each side's own interior contact,
    // so both rows flag symmetrically.
    let crossing = other.segments.iter().any(|&other_seg| {
        match segment_contact_exact(segment, other_seg) {
            // A proper crossing is interior to both segments.
            Contact::Cross => true,
            Contact::Touch { point } => {
                !same_point(point, segment.start) && !same_point(point, segment.end)
            },
            Contact::Disjoint | Contact::Collinear { .. } => false,
        }
    });
    if crossing {
        return Ok(true);
    }
    // Narrow gap: the segment faces another row's unmatched boundary closer
    // than gap_width without touching it.
    if gap_width > 0.0 {
        let near = |point: XY| -> Result<bool> {
            // Distance space retains endpoint-governed gaps when a very small
            // or very large tolerance cannot be squared faithfully.
            for &candidate in &other.segments {
                let distance = crate::geometry::point_segment_distance(point, candidate)?;
                if distance > 0.0 && distance < gap_width {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        if near(midpoint.into())? || near(segment.start)? || near(segment.end)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether the rows form a valid polygonal coverage under `gap_width`.
pub(crate) fn coverage_is_valid<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<bool> {
    let rows = coverage_rows(rows, gap_width)?;
    Ok(!visit_invalid_segments(&rows, gap_width, |_, _| false)?)
}

/// Per-row invalid boundary linework: the row's offending segments merged
/// into lines (`LINESTRING EMPTY` where the row is clean).
pub(crate) fn coverage_invalid_edges<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Shape>> {
    let invalid = coverage_invalid_segments(rows, gap_width)?;
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
        output.push(Shape::multi_line_string(CoordinateAxes::XY, lines)?.line_merge()?);
    }
    Ok(output)
}
