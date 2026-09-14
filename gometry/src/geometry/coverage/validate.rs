use crate::collections::{try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::geometry::coverage::{HashMap, Result};
use crate::geometry::{
    Bounds, CoordinateAxes, GeometryErrorKind, GeometryKind, LineSeq, Point, PointKey, Polygon,
    Segment, Shape, XY, canonical_f64_bits, line_segments, same_point, segment_cross_point,
    undirected_segment_edge_key,
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
    let mut prepared = Vec::new();
    crate::try_reserve_vec(&mut prepared, rows.len())?;
    for row in rows {
        let shape = row.borrow();
        let mut segments = Vec::new();
        match shape {
            Shape::Polygon(polygon) => collect_polygon_segments(polygon, &mut segments)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    collect_polygon_segments(polygon, &mut segments)?;
                }
            },
            Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {},
            _ => return Err(GeometryErrorKind::CoveragePolygonalRequired.into()),
        }
        crate::try_push_vec(&mut prepared, CoverageRow {
            shape,
            segments,
            bounds: shape.bounds(),
        })?;
    }
    Ok(prepared)
}

pub(crate) fn collect_polygon_segments(polygon: &Polygon, out: &mut Vec<Segment>) -> Result<()> {
    for ring in polygon.rings() {
        // Elide zero-length stutter (repeated consecutive vertices): a
        // degenerate edge is removable redundancy, not a coverage boundary, so
        // it must not register as a coverage edge or it would surface as a
        // false invalid edge. Consistent with the validity model.
        for segment in
            line_segments(&ring).filter(|segment| !same_point(segment.start, segment.end))
        {
            crate::try_push_vec(out, segment)?;
        }
    }
    Ok(())
}

/// How many distinct rows carry each undirected segment (at most once per
/// row, even when a ring lists the same edge more than once).
pub(crate) fn edge_row_occurrences(
    rows: &[CoverageRow<'_>],
) -> Result<HashMap<(PointKey, PointKey), u32>> {
    let segment_capacity = rows.iter().try_fold(0_usize, |count, row| {
        count
            .checked_add(row.segments.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut map = try_hash_map_with_capacity(segment_capacity)?;
    for row in rows {
        let mut seen = try_hash_set_with_capacity(row.segments.len())?;
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            if seen.insert(key) {
                *map.entry(key).or_insert(0_u32) += 1;
            }
        }
    }
    Ok(map)
}

/// Successful validation product for check-and-do ops: prepared rows plus the
/// raw edge incidence map dissolve consumes (occurrence==1 = exterior).
pub(crate) struct ValidCoverage<'a> {
    pub(crate) rows: Vec<CoverageRow<'a>>,
    pub(crate) edge_incidences: HashMap<(PointKey, PointKey), (Segment, u32)>,
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
    coverage_invalid_segments_prepared(&rows, gap_width)
}

/// Validate already-prepared coverage rows, allowing check-and-do operations
/// to reuse the same extracted boundary graph for their kernel.
pub(crate) fn coverage_invalid_segments_prepared(
    rows: &[CoverageRow<'_>],
    gap_width: f64,
) -> Result<Vec<Vec<Segment>>> {
    let occurrences = edge_row_occurrences(rows)?;
    let mut invalid = Vec::new();
    crate::try_reserve_vec(&mut invalid, rows.len())?;
    for _ in rows {
        crate::try_push_vec(&mut invalid, Vec::new())?;
    }
    coverage_invalid_with_count(
        rows,
        |key| occurrences[&key],
        gap_width,
        |index, segment| crate::try_push_vec(&mut invalid[index], segment).map(|()| true),
    )?;
    Ok(invalid)
}

/// Core validator given a count lookup: matched interface when count==2.
///
/// `report_invalid` receives each offending segment in diagnostic order and
/// returns whether validation should continue. The result reports whether any
/// offending segment was found.
#[expect(
    clippy::iter_over_hash_type,
    reason = "hash buckets only carry coincidence groups; their order cannot affect validity"
)]
fn coverage_invalid_with_count(
    rows: &[CoverageRow<'_>],
    mut count_of: impl FnMut((PointKey, PointKey)) -> u32,
    gap_width: f64,
    mut report_invalid: impl FnMut(usize, Segment) -> Result<bool>,
) -> Result<bool> {
    // Two coincident polygon rows are an overlap, not a valid shared
    // interface. Their edges occur exactly twice, which otherwise looks just
    // like two adjacent polygons and would take the fast shared-edge path
    // below. Coincident pairs always share exact bounds, so group by bit-exact
    // bounds instead of an all-row geometry comparison — unique-bounds
    // coverages (parcels) pay one HashMap insert each and never call `equals`.
    let mut coincident = Vec::new();
    crate::try_reserve_vec(&mut coincident, rows.len())?;
    coincident.resize(rows.len(), false);
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
                        let mut members = Vec::new();
                        crate::try_reserve_vec(&mut members, 2)?;
                        crate::try_push_vec(&mut members, existing)?;
                        crate::try_push_vec(&mut members, index)?;
                        entry.insert(members);
                    },
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        crate::try_push_vec(entry.get_mut(), index)?;
                    },
                }
            },
        }
    }
    // Within a bounds bucket, fingerprint each row by its sorted undirected
    // edge multiset (PointKey already canonicalizes ±0). Identical fingerprints
    // are coincident without O(g²) `equals` scans.
    for group in duplicate_bounds.values() {
        if group.len() < 2 {
            continue;
        }
        let mut by_edges = try_hash_map_with_capacity(group.len())?;
        for &index in group {
            let mut edges = Vec::new();
            crate::try_reserve_vec(&mut edges, rows[index].segments.len())?;
            for &segment in &rows[index].segments {
                crate::try_push_vec(&mut edges, undirected_segment_edge_key(segment))?;
            }
            edges.sort_unstable();
            match by_edges.entry(edges) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let mut members = Vec::new();
                    crate::try_reserve_vec(&mut members, 1)?;
                    crate::try_push_vec(&mut members, index)?;
                    entry.insert(members);
                },
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    crate::try_push_vec(entry.get_mut(), index)?;
                },
            }
        }
        for members in by_edges.values() {
            if members.len() < 2 {
                continue;
            }
            for &index in members {
                coincident[index] = true;
            }
        }
    }
    let mut any_invalid = false;
    for (index, row) in rows.iter().enumerate() {
        if coincident[index] {
            for &segment in &row.segments {
                any_invalid = true;
                if !report_invalid(index, segment)? {
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
                if !report_invalid(index, segment)? {
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
    let segment_capacity = prepared.iter().try_fold(0_usize, |count, row| {
        count
            .checked_add(row.segments.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut incidences: HashMap<(PointKey, PointKey), (Segment, u32)> =
        try_hash_map_with_capacity(segment_capacity)?;
    for row in &prepared {
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            incidences
                .entry(key)
                .and_modify(|entry| entry.1 += 1)
                .or_insert((segment, 1));
        }
    }
    if coverage_invalid_with_count(&prepared, |key| incidences[&key].1, 0.0, |_, _| Ok(false))? {
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
        segment_cross_point(segment, other_seg).is_some_and(|point| {
            !same_point(point, segment.start) && !same_point(point, segment.end)
        })
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
    let occurrences = edge_row_occurrences(&rows)?;
    Ok(!coverage_invalid_with_count(
        &rows,
        |key| occurrences[&key],
        gap_width,
        |_, _| Ok(false),
    )?)
}

/// Per-row invalid boundary linework: the row's offending segments merged
/// into lines (`LINESTRING EMPTY` where the row is clean).
pub(crate) fn coverage_invalid_edges<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Shape>> {
    let invalid = coverage_invalid_segments(rows, gap_width)?;
    let mut output = Vec::new();
    crate::try_reserve_vec(&mut output, invalid.len())?;
    for segments in invalid {
        if segments.is_empty() {
            crate::try_push_vec(
                &mut output,
                Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XY),
            )?;
            continue;
        }
        let mut lines = Vec::new();
        crate::try_reserve_vec(&mut lines, segments.len())?;
        for segment in segments {
            crate::try_push_vec(
                &mut lines,
                LineSeq::segment(segment.start.point(), segment.end.point())?,
            )?;
        }
        crate::try_push_vec(
            &mut output,
            Shape::multi_line_string(CoordinateAxes::XY, lines)?.line_merge()?,
        )?;
    }
    Ok(output)
}
