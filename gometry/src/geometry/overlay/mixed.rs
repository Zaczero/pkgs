use super::*;
use crate::geometry::*;
pub(crate) fn overlay_points(
    left_shape: &Shape,
    right_shape: &Shape,
    left: &DimensionalParts<'_>,
    right: &DimensionalParts<'_>,
    op: OverlayOp,
) -> Vec<Point> {
    let mut points = match op {
        OverlayOp::Intersection => {
            let mut kept: Vec<Point> = left
                .points
                .iter()
                .copied()
                .filter(|point| right_shape.covers_point(*point))
                .collect();
            kept.extend(
                right
                    .points
                    .iter()
                    .copied()
                    .filter(|point| left_shape.covers_point(*point)),
            );
            kept
        },
        OverlayOp::Union => {
            let mut kept = left.points.clone();
            kept.extend(right.points.iter().copied());
            kept
        },
        OverlayOp::Difference => left
            .points
            .iter()
            .copied()
            .filter(|point| !right_shape.covers_point(*point))
            .collect(),
        OverlayOp::SymmetricDifference => {
            let mut kept: Vec<Point> = left
                .points
                .iter()
                .copied()
                .filter(|point| !right_shape.covers_point(*point))
                .collect();
            kept.extend(
                right
                    .points
                    .iter()
                    .copied()
                    .filter(|point| !left_shape.covers_point(*point)),
            );
            kept
        },
    };
    dedup_points(&mut points);
    points
}

/// Order-preserving topological dedup of overlay output points.
pub(crate) fn dedup_points(points: &mut Vec<Point>) {
    // `PointKey` canonicalizes ±0 exactly like `same_point`, so the hashed
    // first-seen filter keeps the identical set in the identical order — in
    // O(n) instead of the quadratic seen-scan.
    let mut seen: HashSet<PointKey> = HashSet::with_capacity(points.len());
    points.retain(|point| seen.insert(PointKey::new(*point)));
}

/// Evaluate the line bucket of any overlay via a noded midpoint-coverage
/// predicate — the general path covering line/line, line/polygon, and mixed
/// line+polygon collection operands (`GEOS`/JTS `OverlayNG` line extraction).
///
/// Every line segment is noded against all linework *and* polygon boundaries of
/// both operands, so each atomic sub-segment's midpoint has a stable coverage
/// classification: present in the left/right linework (`al`/`bl`), inside the
/// left/right polygons (`ap`/`bp`), and inside the result polygon (`rp`). The
/// per-operation predicate keeps exactly the segments Simple Features reports,
/// absorbing any covered by the result area (`rp`).
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "Polygon::bounds lives in a private module; closure keeps the call site readable"
)]
pub(crate) fn overlay_lines_general(
    left: &DimensionalParts<'_>,
    right: &DimensionalParts<'_>,
    result_polygons: &[Polygon],
    op: OverlayOp,
    boundary_contact: bool,
) -> Vec<CoordSeq> {
    let left_lines = collect_line_segments(&left.lines);
    let right_lines = collect_line_segments(&right.lines);
    // `boundary_contact` (Intersection only): the operands share a
    // positive-length polygon-boundary run, which degenerates to linework
    // (GEOS semantics) — the shared atomic segments become candidates. The
    // other set ops are areal-only, and the common no-shared-border case
    // skips this noding entirely (the envelope-indexed analysis already said
    // there is nothing to find).
    if left_lines.is_empty() && right_lines.is_empty() && !boundary_contact {
        return Vec::new();
    }
    let left_boundaries = polygon_boundary_segments(&left.polygons);
    let right_boundaries = polygon_boundary_segments(&right.polygons);

    // Node the WHOLE pool ONCE, exactly. `self_node_segments_sourced` runs the
    // collinear-overlap exactness pre-pass, so a shared collinear run — and any
    // third segment that crosses it — yields atoms that are BIT-IDENTICAL across
    // operands. Re-noding each operand separately against a shared index instead
    // recomputes those crossings per host extent and mints ulp-twin endpoints,
    // so the shared-edge match (`al && bl`) silently misses collinear linework
    // and drops it to a stray point. `sources[i]` indexes back into `combined`,
    // whose four source ranges are, in order: left lines, right lines, left
    // boundaries, right boundaries.
    let lines_end = left_lines.len() + right_lines.len();
    let left_lines_end = left_lines.len();
    let left_boundaries_end = lines_end + left_boundaries.len();
    let mut combined = left_lines;
    combined.extend(right_lines.iter().copied());
    combined.extend(left_boundaries.iter().copied());
    combined.extend(right_boundaries.iter().copied());
    let (atoms, sources) = self_node_segments_sourced(&combined);

    let mut left_keys: HashSet<(PointKey, PointKey)> = HashSet::new();
    let mut right_keys: HashSet<(PointKey, PointKey)> = HashSet::new();
    for (atom, &source) in atoms.iter().zip(&sources) {
        let source = source as usize;
        if source < left_lines_end {
            left_keys.insert(undirected_segment_edge_key(*atom));
        } else if source < lines_end {
            right_keys.insert(undirected_segment_edge_key(*atom));
        }
    }

    let left_polygon_bounds: Vec<Option<Bounds>> = left
        .polygons
        .iter()
        .map(|polygon| polygon.bounds())
        .collect();
    let right_polygon_bounds: Vec<Option<Bounds>> = right
        .polygons
        .iter()
        .map(|polygon| polygon.bounds())
        .collect();
    let result_polygon_bounds: Vec<Option<Bounds>> = result_polygons
        .iter()
        .map(|polygon| polygon.bounds())
        .collect();

    let mut seen: HashSet<(PointKey, PointKey)> = HashSet::new();
    let mut selected: Vec<CoordSeq> = Vec::new();
    for (atom, &source) in atoms.iter().zip(&sources) {
        if source as usize >= lines_end {
            continue; // boundary-sourced atoms are handled below
        }
        let key = undirected_segment_edge_key(*atom);
        if !seen.insert(key) {
            continue;
        }
        let midpoint = segment_midpoint(*atom);
        let al = left_keys.contains(&key);
        let bl = right_keys.contains(&key);
        let ap = left
            .polygons
            .iter()
            .zip(&left_polygon_bounds)
            .any(|(polygon, bounds)| {
                bounds.is_none_or(|bounds| bounds.contains_xy(midpoint.xy()))
                    && polygon.covers_point(midpoint)
            });
        let bp = right
            .polygons
            .iter()
            .zip(&right_polygon_bounds)
            .any(|(polygon, bounds)| {
                bounds.is_none_or(|bounds| bounds.contains_xy(midpoint.xy()))
                    && polygon.covers_point(midpoint)
            });
        let rp = result_polygons
            .iter()
            .zip(&result_polygon_bounds)
            .any(|(polygon, bounds)| {
                bounds.is_none_or(|bounds| bounds.contains_xy(midpoint.xy()))
                    && polygon.covers_point(midpoint)
            });
        let keep = match op {
            OverlayOp::Intersection => ((al && (bl || bp)) || (bl && ap)) && !rp,
            OverlayOp::Union => (al || bl) && !rp,
            OverlayOp::Difference => al && !bl && !bp && !rp,
            OverlayOp::SymmetricDifference => (al ^ bl) && !rp,
        };
        if keep {
            selected.push(CoordSeq::from(vec![atom.start, atom.end]));
        }
    }
    if boundary_contact {
        // Shared boundary∩boundary atomic segments that the areal result does
        // not absorb — the touching-borders linework. Both boundary sets come
        // from the same canonical noding, so a shared border keys identically.
        let right_boundary_keys: HashSet<(PointKey, PointKey)> = atoms
            .iter()
            .zip(&sources)
            .filter(|(_, source)| **source as usize >= left_boundaries_end)
            .map(|(atom, _)| undirected_segment_edge_key(*atom))
            .collect();
        for (atom, &source) in atoms.iter().zip(&sources) {
            let source = source as usize;
            if !(lines_end..left_boundaries_end).contains(&source) {
                continue;
            }
            let key = undirected_segment_edge_key(*atom);
            if !right_boundary_keys.contains(&key) || !seen.insert(key) {
                continue;
            }
            let midpoint = segment_midpoint(*atom);
            let absorbed = result_polygons
                .iter()
                .any(|polygon| polygon.covers_point(midpoint));
            if !absorbed {
                selected.push(CoordSeq::from(vec![atom.start, atom.end]));
            }
        }
    }
    selected
}

/// Whether two segments are collinear and overlap for positive length.
pub(crate) fn segments_share_collinear_run(a: Segment, b: Segment) -> bool {
    if orientation(a.start, a.end, b.start) != Orientation::Collinear
        || orientation(a.start, a.end, b.end) != Orientation::Collinear
    {
        return false;
    }
    // Collinear: positive-length overlap iff the extents overlap in more
    // than one point along the dominant axis.
    let horizontal = (a.end.x - a.start.x).abs() >= (a.end.y - a.start.y).abs();
    let (a_lo, a_hi, b_lo, b_hi) = if horizontal {
        (
            a.start.x.min(a.end.x),
            a.start.x.max(a.end.x),
            b.start.x.min(b.end.x),
            b.start.x.max(b.end.x),
        )
    } else {
        (
            a.start.y.min(a.end.y),
            a.start.y.max(a.end.y),
            b.start.y.min(b.end.y),
            b.start.y.max(b.end.y),
        )
    };
    a_lo.max(b_lo) < a_hi.min(b_hi)
}

/// All non-degenerate boundary (ring) segments of a polygon set.
/// Envelope-overlapping segment pairs between two segment sets, visited in
/// brute left-major/right-minor order. Past the pair-count crossover an
/// index over `right` generates candidates and the collected `(left, right)`
/// ordinal pairs are sorted lexicographically — exactly the brute order, so
/// observable outputs (cross points, shared paths) are byte-identical.
pub(crate) fn for_each_overlapping_pair<const ORDERED: bool>(
    left: &[Segment],
    right: &[Segment],
    mut visit: impl FnMut(Segment, Segment),
) {
    if left.len() * right.len() < SEGMENT_INDEX_MIN_PAIRS {
        for &left_segment in left {
            for &right_segment in right {
                visit(left_segment, right_segment);
            }
        }
        return;
    }
    // Find cross-operand candidate pairs through the L1-aware monotone-run
    // sweep over the combined pool — NOT an rstar `SegmentIndex` (whose
    // bulk_load dominated, measured) nor the flat sort that exceeds L1. Collect
    // and sort the (left, right) ordinals to keep the line-major visit order
    // callers rely on; the visitor filters non-sharing pairs as before.
    let split = left.len();
    let pool: Vec<Segment> = left.iter().chain(right).copied().collect();
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    let _ = for_each_candidate_pair::<RUN_NODING_MIN>(&pool, single_chain, |a, b| {
        match (a < split, b < split) {
            (true, false) => pairs.push((a, b - split)),
            (false, true) => pairs.push((b, a - split)),
            _ => {},
        }
        std::ops::ControlFlow::Continue(())
    });
    if ORDERED {
        pairs.sort_unstable();
    }
    for (left_ordinal, right_ordinal) in pairs {
        visit(left[left_ordinal], right[right_ordinal]);
    }
}

/// [`ArealArrangement::boundary_contact`] specialized for a RECTANGLE
/// operand: contact can only happen on the window boundary, so an
/// 8-compare columnar prefilter (segment envelope touches the CLOSED rect
/// but is not strictly inside the OPEN rect) drops the bulk before the
/// exact contact kernels run against the four window segments.
pub(crate) fn rect_boundary_contact<P: AsRef<Polygon>>(
    polygons: &[P],
    rect: Bounds,
) -> (Vec<XY>, bool) {
    let rect_ring = rect_polygon(rect);
    let rect_segments: Vec<Segment> = line_segments(rect_ring.shell.coords()).collect();
    let mut touch_points: Vec<XY> = Vec::new();
    let mut shares_boundary = false;
    for polygon in polygons {
        for ring in polygon.as_ref().rings() {
            let (xs, ys) = (ring.xs(), ring.ys());
            for index in 1..xs.len() {
                let (x0, y0, x1, y1) = (xs[index - 1], ys[index - 1], xs[index], ys[index]);
                let (lo_x, hi_x) = if x0 <= x1 { (x0, x1) } else { (x1, x0) };
                let (lo_y, hi_y) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
                if hi_x < rect.minx()
                    || lo_x > rect.maxx()
                    || hi_y < rect.miny()
                    || lo_y > rect.maxy()
                {
                    continue;
                }
                if lo_x > rect.minx()
                    && hi_x < rect.maxx()
                    && lo_y > rect.miny()
                    && hi_y < rect.maxy()
                {
                    continue;
                }
                let segment = Segment {
                    start: XY::new(x0, y0),
                    end: XY::new(x1, y1),
                };
                for &edge in &rect_segments {
                    for_each_segment_overlap_point(segment, edge, |point| {
                        touch_points.push(point);
                    });
                    shares_boundary =
                        shares_boundary || segments_share_collinear_run(segment, edge);
                }
            }
        }
    }
    (touch_points, shares_boundary)
}

pub(crate) fn polygon_boundary_segments<P: AsRef<Polygon>>(polygons: &[P]) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(
        polygons
            .iter()
            .map(|polygon| polygon.as_ref().segment_count())
            .sum(),
    );
    for polygon in polygons {
        let polygon = polygon.as_ref();
        for ring in polygon.rings() {
            for [start, end] in ring.segment_pairs() {
                if !same_point(start, end) {
                    segments.push(Segment {
                        start: start.xy(),
                        end: end.xy(),
                    });
                }
            }
        }
    }
    segments
}

/// Union every input line into one noded, polygon-clipped lineal result.
///
/// All segments are noded against each other and deduplicated, then the
/// portions covered by the dissolved polygon output are absorbed (clipped
/// away).
pub(crate) fn union_lines<C: AsRef<CoordSeq>, P: AsRef<Polygon>>(
    lines: &[C],
    polygons: &[P],
) -> Vec<CoordSeq> {
    if lines.is_empty() {
        return Vec::new();
    }
    let segments = collect_line_segments(lines);
    if polygons.is_empty() {
        return dedup_segments_to_coordseqs(self_node_segments(&segments));
    }
    // Node the lines against themselves AND the dissolved boundaries so
    // every piece is uniformly inside or outside, then keep the outside
    // pieces (boundary-riding linework is absorbed by the polygons).
    // Build the index straight from the line segments chained with the
    // dissolved boundaries — no intermediate `all` clone+extend (`segments`
    // stays owned for the `node_segments` walk below).
    let index = SegmentIndex::build_from_iter(
        segments
            .iter()
            .copied()
            .chain(polygon_boundary_segments(polygons)),
    );
    let covers = PointBatchTester::new(&Shape::MultiPolygon(
        polygons
            .iter()
            .map(|polygon| polygon.as_ref().clone())
            .collect(),
    ));
    let kept: Vec<Segment> = node_segments(&segments, &index)
        .into_iter()
        .filter(|piece| !covers.covers_point(segment_midpoint(*piece)))
        .collect();
    dedup_segments_to_coordseqs(kept)
}

/// Candidate isolated contact points where a line meets a polygon boundary.
///
/// Emits every line/polygon-boundary crossing from both operand orders;
/// `build_overlay_shape` keeps only those not absorbed by a retained line or
/// the result polygon. A line grazing a boundary at a single point (no interior
/// segment) surfaces here as a 0-D intersection component.
pub(crate) fn polygon_line_touch_points(
    left: &DimensionalParts<'_>,
    right: &DimensionalParts<'_>,
) -> Vec<Point> {
    let mut candidates: Vec<XY> = Vec::new();
    for (lines, polygons) in [
        (&left.lines, &right.polygons),
        (&right.lines, &left.polygons),
    ] {
        // No lines on this side — skip before flattening the (possibly
        // huge) polygon boundary.
        if lines.is_empty() || polygons.is_empty() {
            continue;
        }
        // Flattened in iteration order; the candidate engine preserves it.
        let line_segments_flat: Vec<Segment> = lines
            .iter()
            .flat_map(Coordinates::segment_pairs)
            .map(|[start, end]| Segment {
                start: start.xy(),
                end: end.xy(),
            })
            .collect();
        let ring_segments: Vec<Segment> = polygons
            .iter()
            .flat_map(|polygon| polygon.rings())
            .flat_map(Coordinates::segment_pairs)
            .map(|[start, end]| Segment {
                start: start.xy(),
                end: end.xy(),
            })
            .collect();
        for_each_overlapping_pair::<false>(
            &line_segments_flat,
            &ring_segments,
            |segment, ring_segment| {
                if let Some(point) = segment_cross_point(segment, ring_segment) {
                    candidates.push(point);
                }
            },
        );
    }
    candidates.into_iter().map(XY::point).collect()
}

/// Flatten coordinate sequences into non-degenerate directed segments.
pub(crate) fn collect_line_segments<C: AsRef<CoordSeq>>(lines: &[C]) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(
        lines
            .iter()
            .map(|line| line.as_ref().len().saturating_sub(1))
            .sum(),
    );
    for line in lines {
        for [start, end] in line.as_ref().segment_pairs() {
            if !same_point(start, end) {
                segments.push(Segment {
                    start: start.xy(),
                    end: end.xy(),
                });
            }
        }
    }
    segments
}
