use super::*;

/// One row's boundary segments plus the prepared per-row machinery the
/// validator probes (envelope filter, segment index, point membership).
/// The shape is borrowed and the segment index built lazily: only the
/// validator probes it, so `coverage_simplify`/`coverage_clean` never pay
/// for 400 R-trees (or clone 400 shapes) they would not use.
pub(crate) struct CoverageRow<'a> {
    pub(crate) shape: &'a Shape,
    pub(crate) segments: Vec<Segment>,
    index: std::cell::OnceCell<SegmentIndex>,
    pub(crate) bounds: Option<Bounds>,
}

impl CoverageRow<'_> {
    pub(crate) fn index(&self) -> &SegmentIndex {
        self.index
            .get_or_init(|| SegmentIndex::build(&self.segments))
    }
}

/// Envelope entry for the row broad-phase.
struct RowEnvelope {
    row: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for RowEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
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
    rows.iter()
        .map(std::borrow::Borrow::borrow)
        .map(|shape| {
            let mut segments = Vec::new();
            match shape {
                Shape::Polygon(polygon) => collect_polygon_segments(polygon, &mut segments),
                Shape::MultiPolygon(polygons) => {
                    for polygon in polygons {
                        collect_polygon_segments(polygon, &mut segments);
                    }
                },
                Shape::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => {},
                _ => return Err(GeometryErrorKind::CoveragePolygonalRequired.into()),
            }
            // The grown envelope makes the broad-phase see gap_width-near
            // rows too, so narrow-gap detection never misses a candidate.
            let bounds = shape.bounds();
            Ok(CoverageRow {
                shape,
                segments,
                index: std::cell::OnceCell::new(),
                bounds,
            })
        })
        .collect()
}

pub(crate) fn collect_polygon_segments(polygon: &Polygon, out: &mut Vec<Segment>) {
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
pub(crate) fn edge_row_occurrences(rows: &[CoverageRow<'_>]) -> HashMap<(PointKey, PointKey), u32> {
    let mut map = HashMap::new();
    for row in rows {
        let mut seen = HashSet::new();
        for &segment in &row.segments {
            let key = undirected_segment_edge_key(segment);
            if seen.insert(key) {
                *map.entry(key).or_insert(0_u32) += 1;
            }
        }
    }
    map
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
    Ok(coverage_invalid_segments_prepared(&rows, gap_width))
}

/// Validate already-prepared coverage rows, allowing check-and-do operations
/// to reuse the same extracted boundary graph for their kernel.
pub(crate) fn coverage_invalid_segments_prepared(
    rows: &[CoverageRow<'_>],
    gap_width: f64,
) -> Vec<Vec<Segment>> {
    let occurrences = edge_row_occurrences(rows);
    let envelope_tree = BulkRTree::bulk_load_with_params(
        rows.iter()
            .enumerate()
            .filter_map(|(row, data)| {
                data.bounds.map(|bounds| RowEnvelope {
                    row,
                    envelope: AABB::from_corners(
                        [bounds.minx() - gap_width, bounds.miny() - gap_width],
                        [bounds.maxx() + gap_width, bounds.maxy() + gap_width],
                    ),
                })
            })
            .collect(),
    );

    let mut invalid: Vec<Vec<Segment>> = vec![Vec::new(); rows.len()];
    // Two coincident polygon rows are an overlap, not a valid shared
    // interface. Their edges occur exactly twice, which otherwise looks just
    // like two adjacent polygons and would take the fast shared-edge path
    // below. Detect topologically-equal rows through the existing envelope
    // broad phase and mark both complete boundaries invalid. This also catches
    // equivalent rings with different start vertices or orientation.
    let mut coincident = vec![false; rows.len()];
    for (index, row) in rows.iter().enumerate() {
        let Some(bounds) = row.bounds else { continue };
        let envelope =
            AABB::from_corners([bounds.minx() - gap_width, bounds.miny() - gap_width], [
                bounds.maxx() + gap_width,
                bounds.maxy() + gap_width,
            ]);
        for candidate in envelope_tree.locate_in_envelope_intersecting(envelope) {
            if candidate.row <= index || rows[candidate.row].bounds != row.bounds {
                continue;
            }
            if row.shape.equals(rows[candidate.row].shape) {
                coincident[index] = true;
                coincident[candidate.row] = true;
            }
        }
    }
    for (index, row) in rows.iter().enumerate() {
        if coincident[index] {
            invalid[index].extend(row.segments.iter().copied());
            continue;
        }
        for &segment in &row.segments {
            if occurrences[&undirected_segment_edge_key(segment)] == 2 {
                continue; // exactly shared interface — the valid case
            }
            let query = AABB::from_corners(
                [
                    segment.start.x.min(segment.end.x) - gap_width,
                    segment.start.y.min(segment.end.y) - gap_width,
                ],
                [
                    segment.start.x.max(segment.end.x) + gap_width,
                    segment.start.y.max(segment.end.y) + gap_width,
                ],
            );
            let offending = envelope_tree
                .locate_in_envelope_intersecting(query)
                .any(|candidate| {
                    candidate.row != index
                        && segment_offends_row(segment, &rows[candidate.row], gap_width)
                });
            if offending {
                invalid[index].push(segment);
            }
        }
    }
    invalid
}

/// Prepare and require a valid coverage for a public check-and-do operation.
pub(crate) fn valid_coverage_rows<'a, S: std::borrow::Borrow<Shape>>(
    rows: &'a [S],
    operation: &'static str,
) -> Result<Vec<CoverageRow<'a>>> {
    let prepared = coverage_rows(rows, 0.0)?;
    if coverage_invalid_segments_prepared(&prepared, 0.0)
        .iter()
        .any(|segments| !segments.is_empty())
    {
        return Err(GeometryErrorKind::InvalidCoverage { operation }.into());
    }
    Ok(prepared)
}

/// Whether an unmatched boundary segment interacts with `other`'s area or
/// boundary in a way a valid coverage forbids.
pub(crate) fn segment_offends_row(
    segment: Segment,
    other: &CoverageRow<'_>,
    gap_width: f64,
) -> bool {
    let midpoint = Point::new_unchecked_xy(
        f64::midpoint(segment.start.x, segment.end.x),
        f64::midpoint(segment.start.y, segment.end.y),
    );
    // Overlap or inexact contact: the midpoint of a matched interface would
    // coincide with the neighbor's own segment — an unmatched one touching
    // (or entering) the neighbor is a T-join, sliver, or true overlap.
    if other.shape.covers_point(midpoint) {
        return true;
    }
    // Proper crossing without containment (the bowtie-style overlap whose
    // midpoint happens to fall outside). The contact must be interior to
    // THIS segment: a shared corner vertex is legal, and a neighbor vertex
    // landing mid-segment (the T-join) is each side's own interior contact,
    // so both rows flag symmetrically.
    let crossing = other.index().intersecting_candidates(segment).any(|entry| {
        segment_cross_point(segment, entry.segment).is_some_and(|point| {
            !same_point(point, segment.start) && !same_point(point, segment.end)
        })
    });
    if crossing {
        return true;
    }
    // Narrow gap: the segment faces another row's unmatched boundary closer
    // than gap_width without touching it.
    if gap_width > 0.0 {
        let near = |point: XY| {
            other
                .index()
                .nearest_segment_if(point.point(), gap_width * gap_width, |_, distance| {
                    distance > 0.0
                })
                .is_some()
        };
        if near(midpoint.into()) || near(segment.start) || near(segment.end) {
            return true;
        }
    }
    false
}

/// Whether the rows form a valid polygonal coverage under `gap_width`.
pub(crate) fn coverage_is_valid<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<bool> {
    Ok(coverage_invalid_segments(rows, gap_width)?
        .iter()
        .all(Vec::is_empty))
}

/// Per-row invalid boundary linework: the row's offending segments merged
/// into lines (`LINESTRING EMPTY` where the row is clean).
pub(crate) fn coverage_invalid_edges<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    gap_width: f64,
) -> Result<Vec<Shape>> {
    coverage_invalid_segments(rows, gap_width)?
        .into_iter()
        .map(|segments| {
            if segments.is_empty() {
                return Ok(Shape::LineString(LineSeq::empty(CoordinateAxes::XY)));
            }
            let lines: Vec<LineSeq> = segments
                .into_iter()
                .map(|segment| {
                    LineSeq::try_new(CoordSeq::from(vec![segment.start, segment.end]))
                        .expect("coverage invalid segment has two vertices")
                })
                .collect();
            Shape::MultiLineString(lines).line_merge()
        })
        .collect()
}
