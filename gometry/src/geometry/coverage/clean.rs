#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::error::Result;

// --- Cleaning ----------------------------------------------------------------

crate::tokens::token_enum! {
    /// How `coverage_clean` assigns a region covered by more than one row:
    /// to the candidate sharing the longest border with it, the largest or
    /// smallest candidate by area, or the lowest row index.
    pub enum CoverageOverlapRule("coverage_clean overlaps", param = "overlap_rule") {
        LongestBorder = "longest_border",
        MaxArea = "max_area",
        MinArea = "min_area",
        MinIndex = "min_index",
    }
}

crate::tokens::token_from_pyobject!(CoverageOverlapRule);

/// Clean a near-coverage into an exact one.
///
/// Optionally snaps vertices, then nodes every boundary and rebuilds each
/// row from the faces of the global arrangement: a face covered by several
/// rows goes to the `overlaps` winner, an enclosed gap face narrower than
/// `gap_width` merges into the neighbor with the longest shared border, and
/// rows are reassembled by exact edge cancellation — both sides of every
/// interface come from the SAME noded linework, so the output is an exact
/// coverage by construction.
pub(crate) fn coverage_clean<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    grid_size: f64,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
) -> Result<Vec<Shape>> {
    if !(grid_size.is_finite() && grid_size >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("grid_size", grid_size).into());
    }
    let snapped: Vec<Shape> = if grid_size > 0.0 {
        rows.iter()
            .map(std::borrow::Borrow::borrow)
            .map(|shape| shape.snap_to_grid_repaired((grid_size, grid_size), (0.0, 0.0), false))
            .collect::<Result<_, _>>()?
    } else {
        rows.iter()
            .map(std::borrow::Borrow::borrow)
            .cloned()
            .collect()
    };
    let prepared = coverage_rows(&snapped, gap_width)?;
    // Cleaning an already-valid coverage is an identity operation. Besides
    // avoiding needless arrangement work, this preserves coordinate bits,
    // ring presentation, and makes repeated cleaning exactly idempotent.
    if coverage_invalid_segments_prepared(&prepared, gap_width)
        .iter()
        .all(Vec::is_empty)
    {
        return Ok(snapped);
    }

    // The global arrangement: node every boundary segment, take its minimal
    // faces, and resolve nesting into regions (face minus direct children).
    let all_segments: Vec<Segment> = prepared
        .iter()
        .flat_map(|row| row.segments.iter().copied())
        .collect();
    let atomic = self_node_segments(&all_segments);
    let faces = minimal_positive_face_rings(&atomic);
    let regions = arrangement_regions(&faces);

    let per_row = assign_regions(&prepared, regions, gap_width, overlaps)?;
    Ok(per_row.into_iter().map(dissolve_regions).collect())
}

/// Assign every arrangement region to one row (see [`coverage_clean`]): the
/// single covering row, the `overlaps` winner among several, or — for an
/// enclosed narrow gap — the longest-bordering neighbor.
fn assign_regions(
    prepared: &[CoverageRow<'_>],
    regions: Vec<Polygon>,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
) -> Result<Vec<Vec<Polygon>>> {
    let row_edge_keys: Vec<HashSet<(PointKey, PointKey)>> = prepared
        .iter()
        .map(|row| {
            row.segments
                .iter()
                .map(|&s| undirected_segment_edge_key(s))
                .collect()
        })
        .collect();
    let row_areas: Vec<f64> = prepared.iter().map(|row| row.shape.area()).collect();
    // Point-stabbing index over row envelopes: each region probe asks which
    // rows could contain it. Scanning every row per region is O(regions ×
    // rows); the grid buckets rows by cell so a probe checks only its cell
    // (plus a small oversized list), turning the common spatially-local
    // coverage into ~O(regions + rows).
    let row_boxes: Vec<Option<Bounds>> = prepared.iter().map(|row| row.bounds).collect();
    let row_tree = bbox_rtree(&row_boxes);
    // One materialization of the region's segments backs every border
    // probe; the closure was previously re-collecting them per candidate
    // row (and max_by re-evaluates candidates), an O(rows x segments)
    // rebuild per region.
    let shared_border = |segments: &[Segment], row: usize| -> f64 {
        segments
            .iter()
            .filter(|&&segment| row_edge_keys[row].contains(&undirected_segment_edge_key(segment)))
            .map(|segment| point_distance(segment.start, segment.end))
            .sum()
    };
    let mut per_row: Vec<Vec<Polygon>> = vec![Vec::new(); prepared.len()];
    for region in regions {
        // Cheap probe first: the shell's interior-corner point works
        // whenever it dodges the region's holes (the overwhelmingly common
        // hole-free case); only then pay the geo interior-point machinery.
        let shell_points: Vec<XY> = region
            .shell
            .coords()
            .iter_coords()
            .map(|p| p.xy())
            .collect();
        let corner = face_interior_point(&shell_points).point();
        let probe = if region.holes.is_empty()
            || !region
                .holes
                .iter()
                .any(|hole| ring_contains_interior(hole.coords(), corner))
        {
            corner
        } else {
            // Degenerate regions enclose nothing and stay unassigned.
            let Shape::Point(probe) = Shape::Polygon(region.clone()).point_on_surface()? else {
                continue;
            };
            probe
        };
        // Candidate rows from the grid cell (a superset), then the exact bbox +
        // point-in-polygon test — same result set as scanning every row.
        let mut parents: Vec<usize> = boxes_at_point(&row_tree, probe.x, probe.y)
            .filter(|&row| prepared[row].shape.contains_point(probe))
            .collect();
        // The grid yields candidates in bucket order; restore ascending row
        // index so `MinIndex` (and the overlap tie-breaks) match the old scan.
        sort_row_ids(&mut parents, prepared.len());
        debug_assert!(parents.is_sorted());
        let winner = match parents.as_slice() {
            [] => {
                // An enclosed gap: merge when narrow, else leave unfilled.
                if gap_width <= 0.0 || !gap_is_narrow(&region, gap_width) {
                    continue;
                }
                let segments: Vec<Segment> = region.rings().flat_map(line_segments).collect();
                let neighbor = (0..prepared.len())
                    .map(|row| (row, shared_border(&segments, row)))
                    .filter(|&(_, border)| border > 0.0)
                    .max_by(|a, b| a.1.total_cmp(&b.1));
                match neighbor {
                    Some((row, _)) => row,
                    None => continue,
                }
            },
            [only] => *only,
            _ => match overlaps {
                CoverageOverlapRule::MinIndex => parents[0],
                CoverageOverlapRule::MaxArea => parents
                    .iter()
                    .copied()
                    .max_by(|&a, &b| row_areas[a].total_cmp(&row_areas[b]))
                    .expect("non-empty parents"),
                CoverageOverlapRule::MinArea => parents
                    .iter()
                    .copied()
                    .min_by(|&a, &b| row_areas[a].total_cmp(&row_areas[b]))
                    .expect("non-empty parents"),
                CoverageOverlapRule::LongestBorder => {
                    let segments: Vec<Segment> = region.rings().flat_map(line_segments).collect();
                    parents
                        .iter()
                        .map(|&row| (row, shared_border(&segments, row)))
                        .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
                        .map(|(row, _)| row)
                        .expect("non-empty parents")
                },
            },
        };
        per_row[winner].push(region);
    }
    Ok(per_row)
}

/// An indexed bounding box for the broad-phase R-tree (the same `rstar` index
/// the coverage validator uses). Backs the point-stabbing queries that replace
/// the O(n²) all-candidates scans in region assignment and nesting resolution.
struct IndexedEnvelope {
    index: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

/// Bulk-load a point-stabbing R-tree over the indexed bounding boxes (`None`
/// boxes — empty rings — are skipped).
fn bbox_rtree(boxes: &[Option<Bounds>]) -> BulkRTree<IndexedEnvelope> {
    BulkRTree::bulk_load_with_params(
        boxes
            .iter()
            .enumerate()
            .filter_map(|(index, bounds)| {
                bounds.map(|bounds| IndexedEnvelope {
                    index,
                    envelope: AABB::from_corners([bounds.minx(), bounds.miny()], [
                        bounds.maxx(),
                        bounds.maxy(),
                    ]),
                })
            })
            .collect(),
    )
}

/// The indices of every box whose envelope contains `(x, y)` (inclusive edges)
/// — the broad-phase candidate set; the caller applies the exact test.
fn boxes_at_point(
    tree: &BulkRTree<IndexedEnvelope>,
    x: f64,
    y: f64,
) -> impl Iterator<Item = usize> + '_ {
    let point = AABB::from_corners([x, y], [x, y]);
    tree.locate_in_envelope_intersecting(point)
        .map(|entry| entry.index)
}

/// Resolve the minimal CCW faces of a (possibly multi-component, nested)
/// arrangement into disjoint regions: each face becomes a polygon whose
/// holes are its DIRECT child faces. Components never partially overlap
/// (all crossings were noded), so containment is strict nesting.
fn arrangement_regions(faces: &[Vec<XY>]) -> Vec<Polygon> {
    // Sort by |area| descending so a child's direct parent is the smallest
    // containing face seen after it.
    let areas: Vec<f64> = faces
        .iter()
        .map(|face| ring_area_measure(face).get())
        .collect();
    let probes: Vec<XY> = faces.iter().map(|face| face_interior_point(face)).collect();
    // R-tree point-stabbing replaces the O(n²) all-larger-faces scan: a probe
    // only sees boxes covering it. The direct parent is the SMALLEST-area face
    // strictly containing the probe (nesting is strict, so the parent's area is
    // strictly larger).
    let boxes: Vec<Option<Bounds>> = faces.iter().map(|face| xy_bounds(face)).collect();
    let tree = bbox_rtree(&boxes);
    let mut direct_parent: Vec<Option<usize>> = vec![None; faces.len()];
    for face in 0..faces.len() {
        let probe = probes[face];
        let mut best: Option<usize> = None;
        for candidate in boxes_at_point(&tree, probe.x, probe.y) {
            if candidate == face
                || areas[candidate] <= areas[face]
                || !ring_contains_interior(&faces[candidate], probe)
            {
                continue;
            }
            // Smallest container wins; on an exact area tie keep the larger
            // index, matching the previous smallest-larger-first scan order.
            let better =
                best.is_none_or(
                    |current| match areas[candidate].total_cmp(&areas[current]) {
                        std::cmp::Ordering::Less => true,
                        std::cmp::Ordering::Equal => candidate > current,
                        std::cmp::Ordering::Greater => false,
                    },
                );
            if better {
                best = Some(candidate);
            }
        }
        direct_parent[face] = best;
    }
    let mut holes_of: Vec<Vec<usize>> = vec![Vec::new(); faces.len()];
    for (face, parent) in direct_parent.iter().enumerate() {
        if let Some(parent) = parent {
            holes_of[*parent].push(face);
        }
    }
    faces
        .iter()
        .enumerate()
        .map(|(index, face)| {
            let holes: Vec<Ring> = holes_of[index]
                .iter()
                .map(|&hole| Ring::from_trusted_closed(CoordSeq::from_xy(&faces[hole])))
                .collect();
            Polygon::new(Ring::from_trusted_closed(CoordSeq::from_xy(face)), holes)
        })
        .collect()
}

/// Dissolve one row's assigned regions into a single shape by exact edge
/// cancellation: interfaces between the row's own regions appear twice and
/// cancel; the survivors re-polygonize into the row's outline with the
/// arrangement's exact coordinates (no boolean-engine jitter, so neighbors
/// keep vector-identical interfaces).
fn dissolve_regions(regions: Vec<Polygon>) -> Shape {
    if regions.is_empty() {
        return Shape::empty_polygon();
    }
    if regions.len() == 1 {
        return Shape::Polygon(regions.into_iter().next().expect("one region"));
    }
    let mut counts: HashMap<(PointKey, PointKey), (Segment, u32)> = HashMap::new();
    for region in &regions {
        for ring in region.rings() {
            for segment in line_segments(&ring) {
                counts
                    .entry(undirected_segment_edge_key(segment))
                    .and_modify(|entry| entry.1 += 1)
                    .or_insert((segment, 1));
            }
        }
    }
    let mut survivors: Vec<Segment> = counts
        .into_values()
        .filter(|&(_, count)| count == 1)
        .map(|(segment, _)| segment)
        .collect();
    survivors.sort_by_key(|segment| undirected_segment_edge_key(*segment));
    let segments = survivors;
    let faces = minimal_positive_face_rings(&segments);
    let parts = top_level_regions(arrangement_regions(&faces));
    polygon_parts_to_shape(parts)
}

/// Union a polygonal coverage by dissolving its shared edges.
///
/// Assumes a VALID coverage — polygons meet edge-to-edge with no overlaps and
/// no T-junctions (`coverage_is_valid`) — so every interior edge appears in
/// exactly two
/// polygons and cancels, leaving the outer boundary, which the same
/// `arrangement` reassembly the cell-grid dissolve uses closes into the merged
/// polygon(s). This never nodes or classifies interior intersections, so it is
/// far cheaper than the general planar `union_all`. The public kernel validates
/// this precondition and rejects invalid coverage rather than returning a
/// plausible but incorrect dissolve.
pub(crate) fn coverage_union<S: std::borrow::Borrow<Shape>>(coverage: &[S]) -> Result<Shape> {
    // The fast dissolve is correct only for valid coverages. Validate at the
    // public kernel boundary instead of exposing an unchecked footgun.
    let _prepared = valid_coverage_rows(coverage, "coverage_union")?;
    let regions: Vec<Polygon> = coverage
        .iter()
        .filter_map(|shape| relate::polygon_parts(shape.borrow()))
        .flatten()
        .cloned()
        .collect();
    Ok(dissolve_regions(regions))
}

/// Drop regions that sit inside another region's hole-free interior twice
/// over: after edge cancellation, only depth-0 regions (and their holes) are
/// real — a region nested inside a HOLE is depth 2 and stays separate, which
/// `arrangement_regions` already encodes as its own polygon.
fn top_level_regions(parts: Vec<Polygon>) -> Vec<Polygon> {
    // `arrangement_regions` returns every face as a polygon with its direct
    // children as holes; a face that IS someone's hole is also returned
    // standalone. Keep faces at even nesting depth: those are real area.
    let shells: Vec<Vec<XY>> = parts
        .iter()
        .map(|part| part.shell.coords().iter_coords().map(|p| p.xy()).collect())
        .collect();
    let probes: Vec<XY> = shells
        .iter()
        .map(|shell| face_interior_point(shell))
        .collect();
    // R-tree point-stabbing (mirrors `arrangement_regions`): each region's
    // nesting depth = how many OTHER shells contain its probe. The tree yields
    // only shells whose envelope covers the probe, collapsing the O(n²)
    // all-shells scan to near-linear on flat (non-nested) coverages — the
    // common case, where every region's depth is 0.
    let boxes: Vec<Option<Bounds>> = shells.iter().map(|shell| xy_bounds(shell)).collect();
    let tree = bbox_rtree(&boxes);
    parts
        .into_iter()
        .enumerate()
        .filter(|&(index, _)| {
            let probe = probes[index];
            let depth = boxes_at_point(&tree, probe.x, probe.y)
                .filter(|&other| other != index && ring_contains_interior(&shells[other], probe))
                .count();
            depth % 2 == 0
        })
        .map(|(_, part)| part)
        .collect()
}

/// Axis-aligned envelope of a ring's XY coordinates (`None` if empty) — the
/// bbox pre-filter input shared by the coverage nesting scans.
fn xy_bounds(points: &[XY]) -> Option<Bounds> {
    let (first, rest) = points.split_first()?;
    let mut bounds = Bounds::from_xy(*first);
    for point in rest {
        bounds.include_xy(*point);
    }
    Some(bounds)
}

/// Whether a gap region is "narrow": its area-to-perimeter width estimate
/// (`2 * area / perimeter`, exact for long slivers) is below `gap_width`.
/// Holes participate (an annular sliver is narrow even though its shell
/// ring encloses a large area).
fn gap_is_narrow(region: &Polygon, gap_width: f64) -> bool {
    let area = ring_area_measure(region.shell.coords()).get()
        - region
            .holes
            .iter()
            .map(|hole| ring_area_measure(hole.coords()).get())
            .sum::<f64>();
    let perimeter: f64 = region
        .rings()
        .flat_map(line_segments)
        .map(|segment| point_distance(segment.start, segment.end))
        .sum();
    perimeter > 0.0 && 2.0 * area / perimeter < gap_width
}
