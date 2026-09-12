use ahash::HashSetExt as _;

use crate::error::Result;
#[cfg(test)]
use crate::geometry::Ring;
#[cfg(test)]
use crate::geometry::coverage::minimal_positive_face_rings;
use crate::geometry::coverage::{
    AABB, CoverageRow, HashMap, HashMapExt as _, HashSet, RTreeObject,
    coverage_invalid_segments_prepared, coverage_rows, sort_row_ids, valid_coverage_rows,
};
use crate::geometry::overlay::ExactSourceInventory;
#[cfg(test)]
use crate::geometry::overlay::self_node_segments;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeq, Coordinates as _, GeometryErrorKind, OverlayOp, PointKey, Polygon,
    Segment, SegmentIndex, Shape, XY, carry_overlay_ordinates, face_interior_point, line_segments,
    point_distance, polygon_parts_to_shape, ring_area_measure, ring_contains_interior,
    undirected_segment_edge_key,
};

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
            .map(|shape| {
                shape.snap_to_grid_repaired(
                    (grid_size, grid_size),
                    (0.0, 0.0),
                    crate::geometry::TopologyDomain::Cartesian,
                )
            })
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
    if coverage_invalid_segments_prepared(&prepared, gap_width)?
        .iter()
        .all(Vec::is_empty)
    {
        return Ok(snapped);
    }

    // The global arrangement: node every boundary segment, take its minimal
    // faces, and resolve nesting into regions (face minus direct children).
    let shape_refs: Vec<&Shape> = snapped.iter().collect();
    let inventory = ExactSourceInventory::from_shapes(&shape_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_clean", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    let regions = inventory.linework_polygons(&mut partition, false)?;

    let per_row = assign_regions(&prepared, regions, gap_width, overlaps)?;
    per_row.into_iter().map(dissolve_regions).collect()
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
    let mut row_edge_keys = Vec::with_capacity(prepared.len());
    let mut boundary_segments = Vec::new();
    let mut boundary_rows = Vec::new();
    for (row, prepared_row) in prepared.iter().enumerate() {
        let mut edge_keys = HashSet::new();
        for &segment in &prepared_row.segments {
            // The linear path counted a repeated row edge once per gap segment.
            if edge_keys.insert(undirected_segment_edge_key(segment)) {
                boundary_segments.push(segment);
                boundary_rows.push(row);
            }
        }
        row_edge_keys.push(edge_keys);
    }
    let boundary_index = SegmentIndex::build(&boundary_segments)?;
    assign_regions_with_gap_neighbor(
        prepared,
        regions,
        gap_width,
        overlaps,
        &row_edge_keys,
        |segments| {
            longest_border_neighbor(segments, &boundary_index, &boundary_rows, prepared.len())
        },
    )
}

/// Assign regions after the coverage boundary and gap-owner selection have been
/// prepared. Keeping the latter as a parameter lets the test-only linear path
/// exercise the complete cleaning pipeline as an oracle.
fn assign_regions_with_gap_neighbor(
    prepared: &[CoverageRow<'_>],
    regions: Vec<Polygon>,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
    row_edge_keys: &[HashSet<(PointKey, PointKey)>],
    mut gap_neighbor: impl FnMut(&[Segment]) -> Option<usize>,
) -> Result<Vec<Vec<Polygon>>> {
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
        let mut parents = Vec::new();
        for row in boxes_at_point(&row_tree, probe.x, probe.y) {
            if prepared[row].shape.contains_point(probe)? {
                crate::try_push_vec(&mut parents, row)?;
            }
        }
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
                match gap_neighbor(&segments) {
                    Some(row) => row,
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

/// Find the gap owner through the prepared boundary index. Segment ordinals
/// carry row IDs through `boundary_rows`; each row's duplicate edge spellings
/// were removed while building the index, matching the old per-row edge sets.
fn longest_border_neighbor(
    segments: &[Segment],
    boundary_index: &SegmentIndex,
    boundary_rows: &[usize],
    row_count: usize,
) -> Option<usize> {
    let mut border_by_row: HashMap<usize, f64> = HashMap::new();
    for &segment in segments {
        let key = undirected_segment_edge_key(segment);
        for entry in boundary_index.intersecting_candidates(segment) {
            if undirected_segment_edge_key(entry.segment) == key {
                // Every row has at most one indexed copy of an edge, so each
                // row's additions retain the gap-boundary traversal order.
                *border_by_row
                    .entry(boundary_rows[entry.ordinal])
                    .or_insert(0.0) += point_distance(segment.start, segment.end);
            }
        }
    }
    // `max_by` retains the later equal element, just like the prior ascending
    // all-row scan: equal positive borders therefore select the highest row ID.
    (0..row_count)
        .filter_map(|row| border_by_row.get(&row).map(|&border| (row, border)))
        .filter(|&(_, border)| border > 0.0)
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(row, _)| row)
}

#[cfg(test)]
fn assign_regions_linear(
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
                .map(|&segment| undirected_segment_edge_key(segment))
                .collect()
        })
        .collect();
    assign_regions_with_gap_neighbor(
        prepared,
        regions,
        gap_width,
        overlaps,
        &row_edge_keys,
        |segments| {
            (0..prepared.len())
                .map(|row| {
                    let border = segments
                        .iter()
                        .filter(|&&segment| {
                            row_edge_keys[row].contains(&undirected_segment_edge_key(segment))
                        })
                        .map(|segment| point_distance(segment.start, segment.end))
                        .sum::<f64>();
                    (row, border)
                })
                .filter(|&(_, border)| border > 0.0)
                .max_by(|a, b| a.1.total_cmp(&b.1))
                .map(|(row, _)| row)
        },
    )
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
#[cfg(test)]
fn arrangement_regions(faces: &[Vec<XY>]) -> Result<Vec<Polygon>> {
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
                || crate::geometry::cmp_area_keys_then_closed(
                    areas[candidate],
                    areas[face],
                    &faces[candidate][..],
                    &faces[face][..],
                ) != std::cmp::Ordering::Greater
                || !ring_contains_interior(&faces[candidate], probe)
            {
                continue;
            }
            // Smallest container wins; on an exact area tie keep the larger
            // index.
            let better =
                best.is_none_or(|current| {
                    match crate::geometry::cmp_area_keys_then_closed(
                        areas[candidate],
                        areas[current],
                        &faces[candidate][..],
                        &faces[current][..],
                    ) {
                        std::cmp::Ordering::Less => true,
                        std::cmp::Ordering::Equal => candidate > current,
                        std::cmp::Ordering::Greater => false,
                    }
                });
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
            let holes = holes_of[index]
                .iter()
                .map(|&hole| CoordSeq::try_from_xy(&faces[hole]).map(Ring::from_closed))
                .collect::<Result<Vec<_>>>()?;
            Polygon::new(Ring::from_closed(CoordSeq::try_from_xy(face)?), holes)
        })
        .collect()
}

/// Dissolve one row's assigned regions into a single shape by exact edge
/// cancellation: interfaces between the row's own regions appear twice and
/// cancel; the survivors re-polygonize into the row's outline with the
/// arrangement's exact coordinates (no boolean-engine jitter, so neighbors
/// keep vector-identical interfaces).
fn dissolve_regions(regions: Vec<Polygon>) -> Result<Shape> {
    if regions.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    if regions.len() == 1 {
        return Ok(Shape::Polygon(
            regions.into_iter().next().expect("one region"),
        ));
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
    dissolve_edge_incidences(counts)
}

/// XOR-dissolve from a raw undirected edge incidence map: occurrence==1
/// segments are exterior survivors; reassemble via the shared face pipeline.
/// Used by `coverage_union_inner` on the validation product (no second polygon walk)
/// and by `dissolve_regions` after building counts from assigned regions.
fn dissolve_edge_incidences(
    incidences: HashMap<(PointKey, PointKey), (Segment, u32)>,
) -> Result<Shape> {
    let mut survivors: Vec<Segment> = incidences
        .into_values()
        .filter(|&(_, count)| count == 1)
        .map(|(segment, _)| segment)
        .collect();
    if survivors.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    survivors.sort_by_key(|segment| undirected_segment_edge_key(*segment));
    let inventory = ExactSourceInventory::from_segments(&survivors)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_union", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    let parts = inventory.linework_polygons(&mut partition, true)?;
    polygon_parts_to_shape(parts)
}

/// Frame-aware coverage dissolve: rows whose raw boxes are unsound in the
/// geographic frame are split before the edge inventory sees them as planar.
pub(crate) fn coverage_union_topo<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    domain: crate::geometry::TopologyDomain,
) -> Result<Shape> {
    let crate::geometry::TopologyDomain::Geographic(frame) = domain else {
        return coverage_union_inner(coverage);
    };
    if coverage.iter().all(|row| {
        let shape = row.borrow();
        shape.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame)
        })
    }) {
        return coverage_union_inner(coverage);
    }
    let split_owned: Vec<Shape> = coverage
        .iter()
        .map(|row| {
            let shape = row.borrow();
            if shape.bounds().is_some_and(|bounds| {
                crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                    Some(shape),
                    bounds,
                    frame,
                )
            }) {
                Ok(shape.clone())
            } else {
                shape.split_antimeridian_in_frame(frame)
            }
        })
        .collect::<Result<_>>()?;
    coverage_union_inner(&split_owned)
}

fn coverage_union_inner<S: std::borrow::Borrow<Shape>>(coverage: &[S]) -> Result<Shape> {
    // The fast dissolve is correct only for valid coverages. Validate at the
    // public kernel boundary instead of exposing an unchecked footgun.
    let prepared = valid_coverage_rows(coverage, "coverage_union")?;
    let source_refs: Vec<&Shape> = coverage.iter().map(std::borrow::Borrow::borrow).collect();
    // Preserve dissolve_regions' single-region identity path: one polygon is
    // already the union (bit-identical presentation of the input).
    let mut sole: Option<&Polygon> = None;
    let mut multi = false;
    for row in &prepared.rows {
        match row.shape {
            Shape::Polygon(polygon) => {
                if sole.is_some() {
                    multi = true;
                    break;
                }
                sole = Some(polygon);
            },
            Shape::MultiPolygon(polygons) => {
                if polygons.len() > 1 || sole.is_some() {
                    multi = true;
                    break;
                }
                if let Some(polygon) = polygons.first() {
                    sole = Some(polygon);
                }
            },
            // Empties contribute no regions; non-polygonal rows are rejected by
            // coverage_rows before we get here.
            _ => {},
        }
    }
    if !multi {
        let shape = sole.map_or_else(Shape::empty_polygon, |polygon| {
            Shape::Polygon(polygon.clone())
        });
        return carry_overlay_ordinates(shape, &source_refs, OverlayOp::Union, false);
    }
    // If validation found no repeated edge, rows are pairwise disjoint except
    // for legal point touches. Their input polygon parts already are the
    // union, so skip sorting and rebuilding an arrangement for this common
    // sparse-coverage case.
    if prepared
        .edge_incidences
        .values()
        .all(|&(_, count)| count == 1)
        && prepared.rows.iter().try_fold(true, |ok, row| {
            Ok::<_, crate::error::Error>(ok && row.shape.validate()?.is_none())
        })?
    {
        let mut parts = Vec::new();
        for row in &prepared.rows {
            match row.shape {
                Shape::Polygon(polygon) => parts.push(polygon.clone()),
                Shape::MultiPolygon(polygons) => parts.extend(polygons.iter().cloned()),
                Shape::Empty(..) => {},
                _ => unreachable!("coverage rows are polygonal"),
            }
        }
        // The direct path already proves these parts are the complete union.
        // Make only its temporary payload homogeneous; keep source_refs intact
        // so carry_overlay_ordinates remains the owner of ordinate retention.
        let parts = if parts.first().is_some_and(|first| {
            let axes = first.axes();
            parts.iter().any(|polygon| polygon.axes() != axes)
        }) {
            parts
                .into_iter()
                .map(|polygon| polygon.map_ring_seqs(CoordSeq::force_2d))
                .collect::<Result<Vec<_>>>()?
        } else {
            parts
        };
        return carry_overlay_ordinates(
            polygon_parts_to_shape(parts)?,
            &source_refs,
            OverlayOp::Union,
            false,
        );
    }
    carry_overlay_ordinates(
        dissolve_edge_incidences(prepared.edge_incidences)?,
        &source_refs,
        OverlayOp::Union,
        false,
    )
}

/// Axis-aligned envelope of a ring's XY coordinates (`None` if empty) — the
/// bbox pre-filter input shared by the coverage nesting scans.
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_xy(&[
                        XY::new(minx, miny),
                        XY::new(maxx, miny),
                        XY::new(maxx, maxy),
                        XY::new(minx, maxy),
                        XY::new(minx, miny),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn narrow_gap_cleanup_matches_linear_border_oracle() {
        // Every pair of neighboring parcels leaves a narrow vertical gap. The
        // equal shared borders deliberately exercise the highest-row tie break.
        let rows: Vec<Shape> = (0..16)
            .map(|column| {
                let minx = f64::from(column) * 1.1;
                rectangle(minx, 0.0, minx + 1.0, 2.0)
            })
            .collect();
        let gap_width = 0.2;
        let prepared = coverage_rows(&rows, gap_width).expect("polygon rows prepare");
        let all_segments: Vec<Segment> = prepared
            .iter()
            .flat_map(|row| row.segments.iter().copied())
            .collect();
        let regions = arrangement_regions(&minimal_positive_face_rings(
            &self_node_segments(&all_segments).unwrap(),
        ))
        .unwrap();
        let expected: Vec<Shape> = assign_regions_linear(
            &prepared,
            regions,
            gap_width,
            CoverageOverlapRule::LongestBorder,
        )
        .expect("linear assignment succeeds")
        .into_iter()
        .map(dissolve_regions)
        .collect::<Result<Vec<_>>>()
        .expect("linear dissolve succeeds");

        let actual = coverage_clean(&rows, 0.0, gap_width, CoverageOverlapRule::LongestBorder)
            .expect("indexed cleanup succeeds");

        assert_eq!(actual, expected);
    }

    #[test]
    fn overflow_nested_faces_keep_strict_parent() {
        let width = 2.0_f64.powi(1023);
        let inset = 2.0_f64.powi(971);
        let outer = vec![
            XY::new(0.0, 0.0),
            XY::new(width, 0.0),
            XY::new(width, 8.0),
            XY::new(0.0, 8.0),
            XY::new(0.0, 0.0),
        ];
        let inner = vec![
            XY::new(inset, 1.0),
            XY::new(width - inset, 1.0),
            XY::new(width - inset, 7.0),
            XY::new(inset, 7.0),
            XY::new(inset, 1.0),
        ];
        let regions = arrangement_regions(&[outer, inner]).unwrap();
        assert_eq!(regions.len(), 2);
        assert_eq!(
            regions
                .iter()
                .filter(|region| !region.holes.is_empty())
                .count(),
            1,
            "the overflowing inner face must remain a hole of the outer"
        );
    }
}
