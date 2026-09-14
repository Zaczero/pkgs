use crate::error::Result;
use crate::geometry::coverage::{
    HashMap, coverage_invalid_segments_prepared, coverage_rows, valid_coverage_rows,
};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    CoordSeq, GeometryErrorKind, OverlayOp, Shape, carry_overlay_ordinates, polygon_parts_to_shape,
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
/// Optionally snaps vertices, then labels every face of one exact partition:
/// a face covered by several rows goes to the `overlaps` winner, and an
/// enclosed gap face narrower than `gap_width` merges into the neighbor with
/// the longest shared border. Every output row is published from that shared
/// partition, so interfaces use one exact node identity and one rounded
/// coordinate.
pub(crate) fn coverage_clean<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    grid_size: f64,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
) -> Result<Vec<Shape>> {
    if !(grid_size.is_finite() && grid_size >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("grid_size", grid_size).into());
    }
    #[cfg(test)]
    let _admission_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::Admission);
    let mut snapped = Vec::new();
    crate::try_reserve_vec(&mut snapped, rows.len())?;
    if grid_size > 0.0 {
        for row in rows {
            crate::try_push_vec(
                &mut snapped,
                row.borrow().snap_to_grid_repaired(
                    (grid_size, grid_size),
                    (0.0, 0.0),
                    crate::geometry::TopologyDomain::Cartesian,
                )?,
            )?;
        }
    } else {
        for row in rows {
            crate::try_push_vec(&mut snapped, row.borrow().clone())?;
        }
    }
    let prepared = coverage_rows(&snapped, gap_width)?;
    // Cleaning an already-valid coverage is an identity operation. Besides
    // avoiding needless partition work, this preserves coordinate bits, ring
    // presentation, and makes repeated cleaning exactly idempotent.
    if coverage_invalid_segments_prepared(&prepared, gap_width)?
        .iter()
        .all(Vec::is_empty)
    {
        return Ok(snapped);
    }

    #[cfg(test)]
    drop(_admission_phase);
    #[cfg(test)]
    let _source_staging_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::SourceStaging);
    let mut shape_refs = Vec::new();
    crate::try_reserve_vec(&mut shape_refs, snapped.len())?;
    for shape in &snapped {
        crate::try_push_vec(&mut shape_refs, shape)?;
    }
    let inventory = ExactSourceInventory::from_shapes(&shape_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_clean", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    #[cfg(test)]
    drop(_source_staging_phase);
    let mut row_areas = Vec::new();
    crate::try_reserve_vec(&mut row_areas, prepared.len())?;
    for row in &prepared {
        crate::try_push_vec(&mut row_areas, row.shape.area())?;
    }
    let declared_axes = snapped
        .iter()
        .fold(crate::geometry::CoordinateAxes::XY, |axes, shape| {
            axes.union(shape.axes())
        });
    inventory.coverage_clean_rows(
        &mut partition,
        &row_areas,
        gap_width,
        overlaps,
        declared_axes,
    )
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
    let mut split_owned = Vec::new();
    crate::try_reserve_vec(&mut split_owned, coverage.len())?;
    for row in coverage {
        let shape = row.borrow();
        let split = if shape.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame)
        }) {
            Ok(shape.clone())
        } else {
            shape.split_antimeridian_in_frame(frame)
        }?;
        crate::try_push_vec(&mut split_owned, split)?;
    }
    coverage_union_inner(&split_owned)
}

fn coverage_union_inner<S: std::borrow::Borrow<Shape>>(coverage: &[S]) -> Result<Shape> {
    // The fast dissolve is correct only for valid coverages. Validate at the
    // public kernel boundary instead of exposing an unchecked footgun.
    let prepared = valid_coverage_rows(coverage, "coverage_union")?;
    let mut source_refs = Vec::new();
    crate::try_reserve_vec(&mut source_refs, coverage.len())?;
    for shape in coverage {
        crate::try_push_vec(&mut source_refs, shape.borrow())?;
    }
    // Preserve the singleton identity path: one polygon is already the union
    // (bit-identical presentation of the input).
    let mut sole: Option<&crate::geometry::Polygon> = None;
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
        crate::try_reserve_vec(&mut parts, prepared.rows.len())?;
        for row in &prepared.rows {
            match row.shape {
                Shape::Polygon(polygon) => crate::try_push_vec(&mut parts, polygon.clone())?,
                Shape::MultiPolygon(polygons) => {
                    for polygon in polygons {
                        crate::try_push_vec(&mut parts, polygon.clone())?;
                    }
                },
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
            let mut homogeneous = Vec::new();
            crate::try_reserve_vec(&mut homogeneous, parts.len())?;
            for polygon in parts {
                crate::try_push_vec(&mut homogeneous, polygon.map_ring_seqs(CoordSeq::force_2d)?)?;
            }
            homogeneous
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

/// Dissolve the validation product's exterior incidences through one exact
/// partition. Coverage cleaning does not use this path: its row outputs are
/// published directly from the labeled source partition above.
fn dissolve_edge_incidences(
    incidences: HashMap<
        (crate::geometry::PointKey, crate::geometry::PointKey),
        (crate::geometry::Segment, u32),
    >,
) -> Result<Shape> {
    let mut survivors = Vec::new();
    crate::try_reserve_vec(&mut survivors, incidences.len())?;
    for (segment, count) in incidences.into_values() {
        if count == 1 {
            crate::try_push_vec(&mut survivors, segment)?;
        }
    }
    if survivors.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    survivors.sort_by_key(|segment| crate::geometry::undirected_segment_edge_key(*segment));
    let inventory = ExactSourceInventory::from_segments(&survivors)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_union", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    let parts = inventory.linework_polygons(&mut partition, true)?;
    polygon_parts_to_shape(parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, Point, Polygon, Ring, XY};
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn rectangle_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        let points = [
            Point::new_raw_xy(minx, miny),
            Point::new_raw_xy(maxx, miny),
            Point::new_raw_xy(maxx, maxy),
            Point::new_raw_xy(minx, maxy),
            Point::new_raw_xy(minx, miny),
        ];
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
            Vec::new(),
        )
        .unwrap()
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(rectangle_polygon(minx, miny, maxx, maxy))
    }

    fn dense_rectangle_with_subdivisions(
        minx: f64,
        miny: f64,
        maxx: f64,
        maxy: f64,
        subdivisions: usize,
    ) -> Shape {
        let mut points = Vec::new();
        points.reserve(subdivisions * 4 + 1);
        for index in 0..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(minx + (maxx - minx) * fraction, miny));
        }
        for index in 1..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(maxx, miny + (maxy - miny) * fraction));
        }
        for index in 1..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(maxx - (maxx - minx) * fraction, maxy));
        }
        for index in 1..subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(minx, maxy - (maxy - miny) * fraction));
        }
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn dense_rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        dense_rectangle_with_subdivisions(minx, miny, maxx, maxy, 256)
    }

    fn assert_allocation_prefixes(rows: &[Shape], grid_size: f64, gap_width: f64) {
        let run = || {
            coverage_clean(
                rows,
                grid_size,
                gap_width,
                CoverageOverlapRule::LongestBorder,
            )
        };
        let expected = run().expect("coverage clean warm-up");
        let (_, allocations) = allocation_count(|| run().expect("coverage clean count"));
        assert!(allocations > 0);
        for prefix in 0..allocations {
            let result = fail_allocations_after(prefix, &run);
            let Err(error) = result else {
                panic!("coverage clean unexpectedly succeeded at allocation prefix {prefix}");
            };
            assert!(
                matches!(error.kind(), ErrorKind::Resource(ResourceError::Allocation)),
                "coverage clean returned the wrong allocation error: {error:?}"
            );
            assert_eq!(run().expect("coverage clean retry"), expected);
        }
    }

    #[test]
    fn coverage_clean_allocation_prefixes_are_retryable() {
        let rows = [rectangle(0.0, 0.0, 1.2, 1.0), rectangle(1.0, 0.0, 2.0, 1.0)];
        assert_allocation_prefixes(&rows, 0.0, 0.0);
    }

    #[test]
    fn coverage_clean_builds_one_partition_after_admission() {
        let rows = [rectangle(0.0, 0.0, 1.2, 1.0), rectangle(1.0, 0.0, 2.0, 1.0)];
        crate::geometry::overlay::reset_partition_builds();
        coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::LongestBorder)
            .expect("coverage clean");
        assert_eq!(crate::geometry::overlay::partition_builds(), 1);
    }

    #[test]
    fn coverage_clean_gap_allocation_prefixes_are_retryable() {
        let shell = Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(3.0, 0.0),
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(0.0, 3.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        );
        let hole = Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(1.0, 1.0),
                Point::new_raw_xy(2.0, 1.0),
                Point::new_raw_xy(2.0, 2.0),
                Point::new_raw_xy(1.0, 2.0),
                Point::new_raw_xy(1.0, 1.0),
            ])
            .unwrap(),
        );
        let outer = Shape::Polygon(Polygon::new(shell, vec![hole]).unwrap());
        let rows = [outer, rectangle(1.005, 1.005, 1.995, 1.995)];
        assert_allocation_prefixes(&rows, 0.0, 0.05);
    }

    #[test]
    fn coverage_clean_grid_allocation_prefixes_are_retryable() {
        let rows = [
            rectangle(0.0, 0.0, 1.200_000_1, 1.0),
            rectangle(1.0, 0.0, 2.0, 1.0),
        ];
        assert_allocation_prefixes(&rows, 1.0e-6, 0.0);
    }

    #[test]
    fn coverage_clean_multipart_grid_allocation_prefixes_are_retryable() {
        let multipart = Shape::multi_polygon(crate::geometry::CoordinateAxes::XY, vec![
            rectangle_polygon(0.0, 0.0, 1.2, 1.0),
            rectangle_polygon(3.0, 0.0, 4.0, 1.0),
        ])
        .expect("multipart fixture");
        let rows = [multipart, rectangle(1.0, 0.0, 3.5, 1.0)];
        assert_allocation_prefixes(&rows, 1.0e-6, 0.0);
    }

    #[test]
    fn coverage_clean_keeps_one_shared_node_coordinate_for_both_rows() {
        let rows = [rectangle(0.0, 0.0, 1.2, 1.0), rectangle(1.0, 0.0, 2.0, 1.0)];
        let output = coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::LongestBorder)
            .expect("coverage clean");
        let shared = XY::new(1.2, 0.0);
        let mut rows_with_shared_node = Vec::new();
        for shape in &output {
            let mut found = false;
            shape
                .try_for_each_point(|point| {
                    if crate::geometry::same_point(point.xy(), shared) {
                        assert_eq!(point.x.to_bits(), shared.x.to_bits());
                        assert_eq!(point.y.to_bits(), shared.y.to_bits());
                        found = true;
                    }
                    Ok::<_, crate::error::Error>(())
                })
                .expect("coverage output points");
            rows_with_shared_node.push(found);
        }
        assert_eq!(rows_with_shared_node, [true, true]);
    }

    #[test]
    fn coverage_clean_collinear_overlap_work_stays_below_quadratic_materialization() {
        let rows = [
            dense_rectangle(0.0, 0.0, 10.2, 10.0),
            dense_rectangle(10.0, 0.0, 20.0, 10.0),
        ];
        crate::geometry::overlay::reset_partition_builds();
        let (output, allocations) = crate::test_support::allocation_count(|| {
            coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::LongestBorder)
                .expect("dense collinear coverage clean")
        });
        let metrics = crate::geometry::overlay::partition_metrics();
        let phase_allocations = crate::test_support::allocation_phase_counts();
        assert!(phase_allocations.iter().sum::<usize>() <= allocations);
        assert_eq!(metrics.partition_builds, 1);
        assert_eq!(metrics.source_segments, 2048);
        assert_eq!(metrics.unique_source_segments, 2048);
        assert_eq!(metrics.candidate_pool_items, 2048);
        assert_eq!(
            metrics.candidate_pairs_attempted,
            metrics.candidate_pairs_distinct
        );
        assert!(metrics.candidate_pairs_attempted < 100_000);
        assert!(metrics.overlap_contacts < 10_000);
        assert!(metrics.overlap_cuts < 40_000);
        assert!(metrics.source_cut_entries >= metrics.overlap_cuts);
        assert!(metrics.canonical_spans > 0);
        assert!(metrics.canonical_nodes > 0);
        assert!(metrics.global_faces > 0);
        assert_eq!(metrics.topology_halfedges, metrics.canonical_spans * 2);
        assert!(metrics.publication_nodes <= metrics.canonical_nodes);
        assert_eq!(metrics.published_rows, 2);
        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|shape| !shape.is_empty()));
        assert!(
            (output.iter().map(Shape::area).sum::<f64>() - 200.0).abs() < 1.0e-9,
            "dense coverage area changed: {output:?}"
        );
    }

    #[test]
    fn coverage_clean_dense_collinear_allocation_prefixes_are_retryable() {
        let rows = [
            dense_rectangle_with_subdivisions(0.0, 0.0, 10.2, 10.0, 8),
            dense_rectangle_with_subdivisions(10.0, 0.0, 20.0, 10.0, 8),
        ];
        assert_allocation_prefixes(&rows, 0.0, 0.0);
    }

    #[test]
    fn coverage_clean_preserves_reversed_duplicate_and_cancelled_collinear_support() {
        let other = rectangle(2.0, 0.0, 6.0, 4.0);
        for wkt in [
            "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
            "POLYGON ((0 0, 2 0, 4 0, 4 4, 0 4, 0 0))",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, -2 0, 0 0))",
        ] {
            let left = crate::io::parse_wkt(wkt).expect("collinear traversal fixture");
            let rows = [left, other.clone()];
            let output = coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::LongestBorder)
                .expect("coverage clean");
            assert_eq!(output.len(), 2);
            assert!(
                (output.iter().map(Shape::area).sum::<f64>() - 24.0).abs() < 1.0e-9,
                "collinear traversal changed support: {output:?}"
            );
            assert!(crate::geometry::coverage_is_valid(&output, 0.0).unwrap());
            for shape in &output {
                shape
                    .try_for_each_point(|point| {
                        assert!(point.x >= 0.0);
                        Ok::<_, crate::error::Error>(())
                    })
                    .expect("collinear support output points");
            }
        }
    }
}
